package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/purpshell/meowcaller"
	"github.com/purpshell/meowcaller/signaling"
	"github.com/rs/zerolog"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/calls"
	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// bridgedCall is the part of a meowcaller call this session drives. *meowcaller.Call is
// the one implementation outside the tests, which cannot build one: its fields are the
// library's own.
type bridgedCall interface {
	ID() string
	Answer() error
	Reject() error
	Hangup() error
	HangupAgain() error
	Receive(meowcaller.AudioSink)
	Play(meowcaller.AudioSource) *meowcaller.Player
	OnEnd(func(reason string))
	OnPeerAccept(func())
	State() meowcaller.CallPhase
	Discard()
}

// liveCall is one call this session carries the voice of, from the moment meowcaller
// engages it to the moment it ends.
type liveCall struct {
	call bridgedCall
	// leg is the browser's half. Nil for a received call until the offer is built, and
	// for one whose offer could not be built at all, which is then never accepted.
	leg *calls.Leg
	// outbound is a call the client placed; its answer waits for the callee.
	outbound bool
	accepted bool
	// accepting is a call.accept whose browser answer is applied and whose answer on
	// WhatsApp may still be on its way: a repeat has nothing left to do.
	accepting bool
	// creator is who rang a received call, which is what a rejection is addressed to.
	creator waTypes.JID
	// answered is closed once `call.answered` for a placed call has been queued, or
	// given up on. Nil until the callee picks up. The call's end waits for it, so the
	// client never reads the end of a call before its answer.
	answered chan struct{}
}

// bridgeState is what the session keeps about the calls it carries. Its own lock, because
// it is written from meowcaller's goroutines as well as from the dispatch and the
// executor, and none of those should wait on the session's.
type bridgeState struct {
	mu    sync.Mutex
	live  map[string]*liveCall
	ended ring
	// told is the calls whose end has been published, whichever path published it:
	// whatsmeow's terminate and meowcaller's end both report one call.
	told ring
	// engaged is the calls meowcaller's gate let through. The gate runs before meowcaller
	// decrypts the offer and preaccepts it, which can take a while, so this is what says
	// an `offer` is on its way to callOffered before the call is registered as live.
	engaged ring
	// closed is a session that is closing: a call registered after its calls were ended
	// would escape that, so none is.
	closed bool
	// pausing counts the connects turning calls off that are under way: from the moment
	// one ends the calls the session carries until its policy and route are in place, an
	// offer meowcaller engaged under the old policy would otherwise register after the
	// end, on the new route.
	pausing int
	// offline is a session an operator disconnected or logged out: no call registers on
	// it until the next connect, or one engaged as the socket went would outlive it.
	offline bool
	// unrejected is the received calls whose rejection did not go out, by who rang them.
	// meowcaller ends a call here before it writes the rejection, so a failed write leaves
	// nothing of the call to retry with, and the call ringing on the account's other
	// devices; a retried call.terminate rejects it again from this.
	unrejected map[string]waTypes.JID
	// unended is the answered and placed calls whose hangup did not go out, for the same
	// reason: the call is gone here and the other phone is still on it.
	unended map[string]bridgedCall
}

// callOfferPayload is `call.offer` as this session publishes it once it carries calls.
type callOfferPayload struct {
	callOffer
	SDP string `json:"sdp,omitempty"`
}

type callAnswered struct {
	CallID string `json:"call_id"`
	SDP    string `json:"sdp"`
}

type acceptRequest struct {
	CallID string `json:"call_id"`
	SDP    string `json:"sdp"`
}

type startRequest struct {
	To  *protocol.Address `json:"to"`
	SDP string            `json:"sdp"`
}

type terminateRequest struct {
	CallID string `json:"call_id"`
}

// answersCalls is whether this session carries the voice of calls: the deployment opened
// the media socket, and the last connect asked for `calls.answer` without
// `calls.auto_reject`, which wins, and without a proxy. meowcaller sends a call's media to
// WhatsApp's relays over UDP sockets of its own, which a proxy does not carry, so on a
// proxied session the call would leave from this host's own address, the one the proxy was
// asked to keep out of it.
func (s *Session) answersCalls() bool {
	if s.callMedia == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.answerCalls && !s.autoRejectCalls && s.proxy == ""
}

// engagesOffer is meowcaller's gate. Only a 1:1 voice call on a session that answers
// calls is engaged: a group call, a video call and every call on a session that did not
// ask are left to ring as they would without this, which is what the contract promises
// for them.
func (s *Session) engagesOffer(event *waEvents.CallOffer) bool {
	if !s.answersCalls() || !event.GroupJID.IsEmpty() {
		return false
	}
	if _, group, err := signaling.ParseGroupInviteSnapshot(event.Data); err != nil || group {
		return false
	}
	media := mediaOfOffer(event.Data)
	if !media.known || media.video {
		return false
	}
	s.bridge.mu.Lock()
	s.bridge.engaged.add(event.CallID)
	s.bridge.mu.Unlock()
	return true
}

// offerOnItsWay reports whether meowcaller engaged a call's `offer`, which then reaches
// callOffered with the browser's SDP to publish.
func (s *Session) offerOnItsWay(callID string) bool {
	s.bridge.mu.Lock()
	defer s.bridge.mu.Unlock()
	_, engaged := s.bridge.engaged.seen[callID]
	return engaged
}

// currentRetirement is the fence of the client this session runs on now, which a rebuild
// sets as it retires that client.
func (s *Session) currentRetirement() *atomic.Bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.retiredHandler
}

// engagesOfferFrom is the gate as the client it was installed on asks it: nothing is
// engaged once that client is retired.
func (s *Session) engagesOfferFrom(retired *atomic.Bool, event *waEvents.CallOffer) bool {
	return !retired.Load() && s.engagesOffer(event)
}

// ringingFrom is meowcaller handing over a call it engaged. It runs on the dispatch,
// before this session's own handler sees the same offer, so by the time callOffered runs
// the call is here to build the browser's offer for. A call from a client already retired
// belongs to an account this session no longer runs on that socket, and is dropped there
// alone.
//
// The fence is read again where the call is registered, under the lock the rebuild's
// cleanup takes its list of calls under: read only on the way in, a call handed over just
// as the client was retired could register after that list was taken, and outlive it.
func (s *Session) ringingFrom(retired *atomic.Bool, call bridgedCall) {
	if retired.Load() {
		call.Discard()
		return
	}
	s.ringingOn(retired, call)
}

func (s *Session) ringing(call bridgedCall) { s.ringingOn(s.currentRetirement(), call) }

func (s *Session) ringingOn(retired *atomic.Bool, call bridgedCall) {
	if !s.register(call.ID(), &liveCall{call: call}, retired) {
		// Engaged as the session closed or stopped carrying calls: dropped here alone,
		// so meowcaller starts no media for it, and left to ring on the account's other
		// devices, as it would on a session that does not carry them. Refusing it from
		// here would end it for every device.
		call.Discard()
		return
	}
	id := call.ID()
	call.OnEnd(func(reason string) { s.callFinished(id, reason) })
	if call.State() == meowcaller.CallPhaseEnded {
		// Ended between the registration and OnEnd above -- a connect turning calls off
		// hangs up what is registered -- which meowcaller does not replay.
		s.callFinished(id, "")
	}
}

// register records a call this session carries, unless the session is closing, a
// connect is turning calls off, the policy it was engaged under has changed since, or the
// client it came on has been retired.
func (s *Session) register(id string, live *liveCall, retired *atomic.Bool) bool {
	s.bridge.mu.Lock()
	defer s.bridge.mu.Unlock()
	if s.bridge.closed || s.bridge.offline || s.bridge.pausing > 0 || !s.answersCalls() || retired.Load() {
		return false
	}
	if s.bridge.live == nil {
		s.bridge.live = make(map[string]*liveCall)
	}
	s.bridge.live[id] = live
	return true
}

// offerToBrowser builds the browser leg of a received call meowcaller engaged, and
// returns the SDP the `call.offer` carries. Empty when there is no such call or the leg
// could not be built: the offer is then published without one, and the call is left to
// ring, which is what a client that cannot answer it sees anyway.
func (s *Session) offerToBrowser(callID string, creator waTypes.JID) string {
	s.bridge.mu.Lock()
	live := s.bridge.live[callID]
	if live != nil {
		live.creator = creator
	}
	s.bridge.mu.Unlock()
	if live == nil || live.outbound {
		return ""
	}
	log := s.log.With().Str("call_id", callID).Logger()
	leg, sdp, err := s.callMedia.Offer(s.ctx, log)
	if err != nil {
		log.Warn().Err(err).Msg("could not build the browser's half of a call; offering it without sdp")
		return ""
	}
	leg.OnLost(func() { s.browserLost(callID) })
	s.bridge.mu.Lock()
	if current := s.bridge.live[callID]; current == live {
		live.leg = leg
		s.bridge.mu.Unlock()
		return sdp
	}
	// Ended while the offer was being built.
	s.bridge.mu.Unlock()
	_ = leg.Close()
	return ""
}

// browserLost hangs up a call whose browser peer is gone for good. Nobody is listening on
// the connector's side of it any more, and a call left up would keep the person on the
// phone talking to silence.
func (s *Session) browserLost(callID string) {
	s.bridge.mu.Lock()
	live := s.bridge.live[callID]
	s.bridge.mu.Unlock()
	if live == nil {
		return
	}
	s.log.Warn().Str("call_id", callID).Msg("the browser peer of a call was lost; hanging up")
	if err := live.call.Hangup(); err != nil {
		s.log.Warn().Err(err).Str("call_id", callID).Msg("could not hang up a call whose browser was lost")
		s.rememberUnended(callID, live.call)
	}
}

// callFinished is meowcaller reporting a call over, for whatever reason and whoever ended
// it. The browser leg goes with it, and the client is told unless whatsmeow's own
// terminate already did.
func (s *Session) callFinished(callID, reason string) {
	s.bridge.mu.Lock()
	live := s.bridge.live[callID]
	first, answered := s.claimEndLocked(callID)
	delete(s.bridge.live, callID)
	s.bridge.ended.add(callID)
	s.bridge.mu.Unlock()
	if live != nil && live.leg != nil {
		_ = live.leg.Close()
	}
	if !first {
		return
	}
	payload := callTerminate{CallID: callID}
	if reason != "" {
		payload.Reason = &reason
	}
	s.publishEnd(payload, answered)
}

// claimEnd records that the end of a call is being published, and reports whether it
// was not already, with the barrier of a placed call's answer that the end has to follow.
func (s *Session) claimEnd(callID string) (first bool, answered chan struct{}) {
	s.bridge.mu.Lock()
	defer s.bridge.mu.Unlock()
	return s.claimEndLocked(callID)
}

func (s *Session) claimEndLocked(callID string) (first bool, answered chan struct{}) {
	if live := s.bridge.live[callID]; live != nil {
		answered = live.answered
	}
	if callID == "" {
		return true, answered
	}
	return s.bridge.told.add(callID), answered
}

// publishEnd publishes the end of a call, after the call's answer when one is still
// waiting for room. Off whatever goroutine ended the call, which can be whatsmeow's
// dispatch.
func (s *Session) publishEnd(payload callTerminate, answered chan struct{}) {
	if answered == nil {
		s.emitEnd(protocol.EventCallTerminate, payload)
		return
	}
	go func() {
		// The answer gives up after callWait on its own, and a session that closes
		// takes both with it.
		select {
		case <-answered:
		case <-s.done:
		}
		s.emitEnd(protocol.EventCallTerminate, payload)
	}()
}

// endPublished reports whether the end of a call has been published.
func (s *Session) endPublished(callID string) bool {
	s.bridge.mu.Lock()
	defer s.bridge.mu.Unlock()
	_, told := s.bridge.told.seen[callID]
	return told
}

// acceptCall carries out `call.accept`: the browser's answer is applied, and only then is
// the call answered on WhatsApp, so the person never hears a connected call with nobody
// behind it.
func (s *Session) acceptCall(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	if !s.answersCalls() {
		return nil, engine.ErrNotSupported
	}
	var req acceptRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil || req.CallID == "" || req.SDP == "" {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a call accept has to name the call and carry the browser's answer")
	}
	s.bridge.mu.Lock()
	live := s.bridge.live[req.CallID]
	_, wasEnded := s.bridge.ended.seen[req.CallID]
	// Read under the lock: an accept whose deadline passed may still be wiring this call
	// from its own goroutine.
	var accepted, outbound, hasLeg bool
	if live != nil {
		accepted, outbound, hasLeg = live.accepted || live.accepting, live.outbound, live.leg != nil
		if !accepted && !outbound && hasLeg {
			live.accepting = true
		}
	}
	s.bridge.mu.Unlock()
	switch {
	case live == nil && wasEnded:
		// Late: the call is over, and accepting it again is nothing new.
		return nil, nil
	case live == nil:
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "no call with that id is ringing on this session")
	case accepted:
		return nil, nil
	case outbound:
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a call the client placed is answered by the callee, not accepted")
	case !hasLeg:
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"this call was offered without sdp, so there is no offer to answer")
	}
	if err := live.leg.Accept(req.SDP); err != nil {
		s.bridge.mu.Lock()
		live.accepting = false
		s.bridge.mu.Unlock()
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "the browser's answer is not an SDP this connector can use")
	}
	// The voice is wired by whoever sees the answer go through, which is not this
	// command when its deadline passed first: the accept can still land after that.
	err := signal(ctx, func() error {
		if err := live.call.Answer(); err != nil {
			return err
		}
		s.wire(live)
		return nil
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("accept call %s: %w", req.CallID, ctx.Err())
		}
		return nil, protocol.NewError(protocol.ErrorWaError, "WhatsApp did not take the answer to the call")
	}
	return nil, nil
}

// wire starts the voice between the two halves of a call.
func (s *Session) wire(live *liveCall) {
	s.bridge.mu.Lock()
	live.accepted = true
	s.bridge.mu.Unlock()
	live.call.Receive(live.leg.Sink())
	live.call.Play(live.leg.Source())
	live.leg.Start()
}

// startCall carries out `call.start`. The callee is rung before this returns, so its
// result is the id the call is known by from then on; the browser's answer waits for the
// callee to pick up, and goes out as `call.answered`.
func (s *Session) startCall(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	if !s.answersCalls() {
		return nil, engine.ErrNotSupported
	}
	if command.IdempotencyKey == "" {
		// Required by the contract: ringing somebody's phone is not a side effect that
		// repeats harmlessly, and the key is what makes a redelivery answer the same
		// call instead of ringing again.
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "a call.start has to carry an idempotency_key")
	}
	var req startRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil || req.To == nil || req.SDP == "" {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a call.start has to name who to call and carry the browser's offer")
	}
	if req.To.Kind != protocol.AddressPhone {
		// meowcaller dials a number, and resolving the number from another namespace is
		// a lookup this would make on the caller's behalf with nothing to say it found
		// the right person.
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "a call is placed to a phone address")
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	// The number as WhatsApp registered it. A Brazilian mobile can be registered without
	// its ninth digit, meowcaller looks the number up exactly as given, and in the spike
	// a number written with the digit was not found. The JID can come back as a LID,
	// which meowcaller does not take as a target, so the phone number is read apart.
	found, err := s.onWhatsApp(ctx, s.current(), []string{"+" + req.To.ID})
	if err != nil {
		return nil, contactFailure(err, "call")
	}
	if len(found) != 1 || !found[0].IsIn {
		return nil, protocol.NewError(protocol.ErrorRecipientNotOnWhatsapp, "that number is not on WhatsApp")
	}
	target := resolvedNumber(&found[0])
	if target == "" {
		target = req.To.ID
	}

	log := s.log.With().Str("cmd_id", command.ID).Logger()
	leg, err := s.callMedia.Answer(req.SDP, log)
	if err != nil {
		if errors.Is(err, calls.ErrBadSDP) {
			return nil, protocol.NewError(protocol.ErrorInvalidPayload, "the browser's offer is not an SDP this connector can use")
		}
		return nil, protocol.NewError(protocol.ErrorInternal, "the connector could not prepare the call")
	}
	// The client the call is placed on, taken before the dial: a rebuild during it retires
	// that client, and the call is not registered on the one that replaced it.
	retired := s.currentRetirement()
	call, err := s.dialCall(ctx, target)
	if err != nil {
		_ = leg.Close()
		log.Warn().Err(err).Msg("meowcaller could not place the call")
		failure := protocol.NewError(protocol.ErrorWaError, "WhatsApp did not take the call")
		if ctx.Err() == nil && offerMayHaveRung(err) {
			// The callee's phone may be ringing, and a retry under the same key must
			// not ring it again.
			return nil, engine.MayHaveLanded(failure)
		}
		if ctx.Err() != nil {
			// Stopped by its own deadline, which can end it before the write as easily
			// as after: the contract has a resend run it again, so the attempt is not
			// held, and the answer is the deadline's.
			return nil, fmt.Errorf("call %s: %w", req.To.ID, ctx.Err())
		}
		// Failed before the offer was written: nothing rang, and the retry places
		// the call.
		return nil, failure
	}
	id := call.ID()
	live := &liveCall{call: call, leg: leg, outbound: true}
	if !s.register(id, live, retired) {
		// The session closed, or stopped carrying calls, while the callee was being
		// rung, after its calls were ended: this one is ended here, or it would ring
		// with nobody behind it.
		_ = leg.Close()
		go func() { _ = call.Hangup() }()
		return nil, engine.MayHaveLanded(protocol.NewError(protocol.ErrorWaError, "the session closed as the call was placed"))
	}
	leg.OnLost(func() { s.browserLost(id) })
	call.OnEnd(func(reason string) { s.callFinished(id, reason) })
	if call.State() == meowcaller.CallPhaseEnded {
		// Refused or ended between the offer going out and OnEnd above, which meowcaller
		// does not replay: the end already happened and nobody was listening for it.
		// Reported here, and once even if OnEnd did catch it after all.
		s.callFinished(id, "")
	}
	call.OnPeerAccept(func() { s.calleeAnswered(live, log) })
	log.Info().Str("call_id", id).Msg("placed a call")
	return json.Marshal(map[string]string{"call_id": id})
}

// calleeAnswered is the callee picking up a call the client placed: the browser's offer is
// answered only now, and the answer goes to the client as `call.answered`.
//
//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func (s *Session) calleeAnswered(live *liveCall, log zerolog.Logger) {
	id := live.call.ID()
	answer, err := live.leg.Answer(s.ctx)
	if err != nil {
		log.Warn().Err(err).Str("call_id", id).Msg("could not answer the browser once the callee picked up; hanging up")
		// Off the dispatch, like every other write this handler causes: the hang-up writes a
		// terminate on a context nothing here can cancel, and a socket that does not take it
		// would hold whatever WhatsApp sends next. The socket closing is what ends that wait.
		go func() {
			if err := live.call.Hangup(); err != nil {
				log.Warn().Err(err).Str("call_id", id).Msg("could not hang up a call the browser cannot carry")
				s.rememberUnended(id, live.call)
			}
		}()
		return
	}
	// A call that ended before this has its leg closed, and the answer above fails. One
	// that ends from here on may be wired for nothing, which costs nothing, and is checked
	// for, and published, under the lock callFinished ends the call under: it is not
	// reported answered after its end.
	s.wire(live)
	s.bridge.mu.Lock()
	defer s.bridge.mu.Unlock()
	if s.bridge.live[id] != live {
		return
	}
	// Off the dispatch: this runs inside meowcaller's handler of the callee's accept, and
	// an inbox with no room must not hold up whatever WhatsApp sends next, the end of
	// this same call included. The end waits on `answered` instead, which is what keeps
	// the two in order when both have to wait for room.
	sent := make(chan struct{})
	live.answered = sent
	go func() {
		defer close(sent)
		if !s.offer(&engine.Emission{Type: protocol.EventCallAnswered}, callAnswered{CallID: id, SDP: answer}, s.callWait) {
			log.Warn().Str("call_id", id).Msg("could not queue call.answered, with no room in the inbox")
		}
	}()
}

// offerMayHaveRung reports whether a failed dial got as far as writing the offer, which is
// the one write that rings the callee. A socket that was not there to write on sent nothing.
func offerMayHaveRung(err error) bool {
	return errors.Is(err, meowcaller.ErrSendOffer) && !sentNothing(err)
}

// dialOverCaller places a call through meowcaller on the current client.
func (s *Session) dialOverCaller(ctx context.Context, target string) (bridgedCall, error) {
	s.mu.Lock()
	caller := s.caller
	s.mu.Unlock()
	if caller == nil {
		// A client adopted before the media socket existed, which a session built by
		// this package never has: the socket is opened with the engine.
		return nil, errors.New("whatsmeow: meowcaller is not installed on this client")
	}
	return caller.Call(ctx, target) //nolint:wrapcheck // logged and turned into wa_error by the caller
}

// terminateCall carries out `call.terminate`: a call answered or placed is hung up, and
// one still ringing is refused. A call this session does not carry is nothing new.
func (s *Session) terminateCall(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	if !s.answersCalls() {
		return nil, engine.ErrNotSupported
	}
	var req terminateRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil || req.CallID == "" {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "a call terminate has to name the call")
	}
	s.bridge.mu.Lock()
	live := s.bridge.live[req.CallID]
	answered := live != nil && (live.outbound || live.accepted)
	var creator waTypes.JID
	if live != nil {
		creator = live.creator
	}
	unrejected, retry := s.bridge.unrejected[req.CallID]
	unended := s.bridge.unended[req.CallID]
	s.bridge.mu.Unlock()
	if live == nil && retry {
		return s.rejectAgain(ctx, req.CallID, unrejected)
	}
	if live == nil && unended != nil {
		return s.hangupAgain(ctx, req.CallID, unended)
	}
	if live == nil {
		return nil, nil
	}
	end := live.call.Reject
	if answered {
		end = live.call.Hangup
	}
	if err := signal(ctx, end); err != nil {
		switch {
		case answered:
			s.rememberUnended(req.CallID, live.call)
		case !creator.IsEmpty():
			s.rememberUnrejected(req.CallID, creator)
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("end call %s: %w", req.CallID, ctx.Err())
		}
		return nil, protocol.NewError(protocol.ErrorWaError, "WhatsApp did not take the end of the call")
	}
	return nil, nil
}

// rememberUnrejected keeps a received call whose rejection did not go out, for a retry.
// Bounded like the other per-call records: a session that runs for weeks would otherwise
// keep every call whose rejection ever failed.
func (s *Session) rememberUnrejected(callID string, creator waTypes.JID) {
	s.bridge.mu.Lock()
	defer s.bridge.mu.Unlock()
	if s.bridge.unrejected == nil {
		s.bridge.unrejected = make(map[string]waTypes.JID)
	}
	if len(s.bridge.unrejected) >= unrejectedLimit {
		clear(s.bridge.unrejected)
	}
	s.bridge.unrejected[callID] = creator
}

// rememberUnended keeps an answered or placed call whose hangup did not go out, for a
// retry, bounded like unrejected.
func (s *Session) rememberUnended(callID string, call bridgedCall) {
	s.bridge.mu.Lock()
	defer s.bridge.mu.Unlock()
	if s.bridge.unended == nil {
		s.bridge.unended = make(map[string]bridgedCall)
	}
	if len(s.bridge.unended) >= unrejectedLimit {
		clear(s.bridge.unended)
	}
	s.bridge.unended[callID] = call
}

// hangupAgain is a retried call.terminate of a call whose hangup did not go out: the same
// terminate is written again, and once it lands a further retry is nothing new.
func (s *Session) hangupAgain(ctx context.Context, callID string, call bridgedCall) (json.RawMessage, error) {
	if err := signal(ctx, call.HangupAgain); err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("end call %s: %w", callID, ctx.Err())
		}
		return nil, protocol.NewError(protocol.ErrorWaError, "WhatsApp did not take the end of the call")
	}
	s.bridge.mu.Lock()
	delete(s.bridge.unended, callID)
	s.bridge.mu.Unlock()
	return nil, nil
}

// unrejectedLimit bounds the calls kept for a retried rejection or hangup. A failed rejection is a
// socket going, and a handful is what a burst of those leaves.
const unrejectedLimit = 64

// rejectAgain is a retried call.terminate of a received call whose rejection did not go
// out the first time: meowcaller has nothing of it left, so the rejection is written the
// way calls.auto_reject writes one.
func (s *Session) rejectAgain(ctx context.Context, callID string, creator waTypes.JID) (json.RawMessage, error) {
	if err := s.declineCall(ctx, s.current(), creator, callID); err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("end call %s: %w", callID, ctx.Err())
		}
		return nil, protocol.NewError(protocol.ErrorWaError, "WhatsApp did not take the end of the call")
	}
	s.bridge.mu.Lock()
	delete(s.bridge.unrejected, callID)
	s.bridge.mu.Unlock()
	return nil, nil
}

// signal runs one of meowcaller's call signals, which write their node with a context of
// their own and cannot be cancelled, and stops waiting for it when ctx is done. The write
// itself goes on until the socket takes it or fails it.
func signal(ctx context.Context, send func() error) error {
	done := make(chan error, 1)
	go func() { done <- send() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// goOffline stops calls registering on this session until the next connect, and ends the
// ones it carries, for a session an operator is taking down.
func (s *Session) goOffline(ctx context.Context) {
	s.bridge.mu.Lock()
	s.bridge.offline = true
	s.bridge.mu.Unlock()
	s.endCalls(ctx)
}

// backOnline lets calls register again, on a session that connects or whose logout
// failed with the device untouched.
func (s *Session) backOnline() {
	s.bridge.mu.Lock()
	s.bridge.offline = false
	s.bridge.mu.Unlock()
}

// endCalls hangs up every call this session carries, for a socket that is going: the
// socket and the media are this instance's, and a call cannot follow the account to
// another one. It waits for the hang-ups to be written until ctx is done, and a context
// already done sends them without waiting at all. meowcaller ends each call locally before
// it writes the terminate, so the voice stops here whether or not the write lands.
func (s *Session) endCalls(ctx context.Context) {
	type carried struct {
		call bridgedCall
		leg  *calls.Leg
	}
	s.bridge.mu.Lock()
	live := make([]carried, 0, len(s.bridge.live))
	for _, call := range s.bridge.live {
		live = append(live, carried{call.call, call.leg})
	}
	s.bridge.mu.Unlock()
	if len(live) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, call := range live {
		wg.Go(func() { _ = signal(ctx, call.call.Hangup) })
	}
	wg.Wait()
	if ctx.Err() != nil {
		s.log.Debug().Int("calls", len(live)).Msg("hung up the calls of a session without waiting for the writes")
	}
	// A leg attached after the snapshot belongs to a call that is still live, and is
	// closed by callFinished once its hang-up lands, or by offerToBrowser if it already
	// has.
	for _, call := range live {
		if call.leg != nil {
			_ = call.leg.Close()
		}
	}
}
