package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/purpshell/meowcaller"
	"github.com/purpshell/meowcaller/signaling"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/calls"
	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// hangupWait bounds how long a session that is closing waits for the calls it carries to
// be hung up. The hang-up is a node on a socket that may already be going, and a close
// held behind it is a lease held by an instance that has stopped serving it.
const hangupWait = 2 * time.Second

// bridgedCall is the part of a meowcaller call this session drives. *meowcaller.Call is
// the one implementation outside the tests, which cannot build one: its fields are the
// library's own.
type bridgedCall interface {
	ID() string
	Answer() error
	Reject() error
	Hangup() error
	Receive(meowcaller.AudioSink)
	Play(meowcaller.AudioSource) *meowcaller.Player
	OnEnd(func(reason string))
	OnPeerAccept(func())
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
	answer   string
	accepted bool
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
// `calls.auto_reject`, which wins.
func (s *Session) answersCalls() bool {
	if s.callMedia == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.answerCalls && !s.autoRejectCalls
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
	return media.known && !media.video
}

// callRinging is meowcaller handing over a call it engaged. It runs on the dispatch,
// before this session's own handler sees the same offer, so by the time callOffered runs
// the call is here to build the browser's offer for.
func (s *Session) callRinging(call *meowcaller.Call) { s.ringing(call) }

func (s *Session) ringing(call bridgedCall) {
	s.bridge.mu.Lock()
	if s.bridge.live == nil {
		s.bridge.live = make(map[string]*liveCall)
	}
	s.bridge.live[call.ID()] = &liveCall{call: call}
	s.bridge.mu.Unlock()
	id := call.ID()
	call.OnEnd(func(reason string) { s.callFinished(id, reason) })
}

// offerToBrowser builds the browser leg of a received call meowcaller engaged, and
// returns the SDP the `call.offer` carries. Empty when there is no such call or the leg
// could not be built: the offer is then published without one, and the call is left to
// ring, which is what a client that cannot answer it sees anyway.
func (s *Session) offerToBrowser(callID string) string {
	s.bridge.mu.Lock()
	live := s.bridge.live[callID]
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
	leg.OnFailed(func() { s.browserLost(callID) })
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
	}
}

// callFinished is meowcaller reporting a call over, for whatever reason and whoever ended
// it. The browser leg goes with it, and the client is told unless whatsmeow's own
// terminate already did.
func (s *Session) callFinished(callID, reason string) {
	s.bridge.mu.Lock()
	live := s.bridge.live[callID]
	delete(s.bridge.live, callID)
	s.bridge.ended.add(callID)
	s.bridge.mu.Unlock()
	if live != nil && live.leg != nil {
		_ = live.leg.Close()
	}
	if !s.firstEndOf(callID) {
		return
	}
	payload := callTerminate{CallID: callID}
	if reason != "" {
		payload.Reason = &reason
	}
	s.emitEnd(protocol.EventCallTerminate, payload)
}

// firstEndOf reports whether the end of a call has not been published yet, and records
// it either way.
func (s *Session) firstEndOf(callID string) bool {
	if callID == "" {
		return true
	}
	s.bridge.mu.Lock()
	defer s.bridge.mu.Unlock()
	return s.bridge.told.add(callID)
}

// acceptCall carries out `call.accept`: the browser's answer is applied, and only then is
// the call answered on WhatsApp, so the person never hears a connected call with nobody
// behind it.
func (s *Session) acceptCall(_ context.Context, command *protocol.Command) (json.RawMessage, error) {
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
	s.bridge.mu.Unlock()
	switch {
	case live == nil && wasEnded:
		// Late: the call is over, and accepting it again is nothing new.
		return nil, nil
	case live == nil:
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "no call with that id is ringing on this session")
	case live.accepted:
		return nil, nil
	case live.outbound:
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a call the client placed is answered by the callee, not accepted")
	case live.leg == nil:
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"this call was offered without sdp, so there is no offer to answer")
	}
	if err := live.leg.Accept(req.SDP); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "the browser's answer is not an SDP this connector can use")
	}
	if err := live.call.Answer(); err != nil {
		return nil, protocol.NewError(protocol.ErrorWaError, "WhatsApp did not take the answer to the call")
	}
	s.wire(live)
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
	leg, answer, err := s.callMedia.Answer(ctx, req.SDP, log)
	if err != nil {
		if errors.Is(err, calls.ErrBadSDP) {
			return nil, protocol.NewError(protocol.ErrorInvalidPayload, "the browser's offer is not an SDP this connector can use")
		}
		return nil, protocol.NewError(protocol.ErrorInternal, "the connector could not prepare the call")
	}
	call, err := s.dialCall(ctx, target)
	if err != nil {
		_ = leg.Close()
		log.Warn().Err(err).Msg("meowcaller could not place the call")
		return nil, protocol.NewError(protocol.ErrorWaError, "WhatsApp did not take the call")
	}
	id := call.ID()
	live := &liveCall{call: call, leg: leg, outbound: true, answer: answer}
	s.bridge.mu.Lock()
	if s.bridge.live == nil {
		s.bridge.live = make(map[string]*liveCall)
	}
	s.bridge.live[id] = live
	s.bridge.mu.Unlock()
	leg.OnFailed(func() { s.browserLost(id) })
	call.OnEnd(func(reason string) { s.callFinished(id, reason) })
	call.OnPeerAccept(func() {
		s.wire(live)
		s.emit(protocol.EventCallAnswered, callAnswered{CallID: id, SDP: answer})
	})
	log.Info().Str("call_id", id).Msg("placed a call")
	return json.Marshal(map[string]string{"call_id": id})
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
func (s *Session) terminateCall(_ context.Context, command *protocol.Command) (json.RawMessage, error) {
	if !s.answersCalls() {
		return nil, engine.ErrNotSupported
	}
	var req terminateRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil || req.CallID == "" {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "a call terminate has to name the call")
	}
	s.bridge.mu.Lock()
	live := s.bridge.live[req.CallID]
	s.bridge.mu.Unlock()
	if live == nil {
		return nil, nil
	}
	var err error
	if live.outbound || live.accepted {
		err = live.call.Hangup()
	} else {
		err = live.call.Reject()
	}
	if err != nil {
		return nil, protocol.NewError(protocol.ErrorWaError, "WhatsApp did not take the end of the call")
	}
	return nil, nil
}

// hangUpEverything ends every call this session carries, for a session that is closing:
// the socket and the media are this instance's, and a call cannot follow the account to
// another one.
func (s *Session) hangUpEverything() {
	s.bridge.mu.Lock()
	live := make([]*liveCall, 0, len(s.bridge.live))
	for _, call := range s.bridge.live {
		live = append(live, call)
	}
	s.bridge.mu.Unlock()
	if len(live) == 0 {
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for _, call := range live {
			wg.Go(func() { _ = call.call.Hangup() })
		}
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(hangupWait):
		s.log.Warn().Int("calls", len(live)).Msg("calls were still being hung up when the session closed")
	}
	for _, call := range live {
		if call.leg != nil {
			_ = call.leg.Close()
		}
	}
}
