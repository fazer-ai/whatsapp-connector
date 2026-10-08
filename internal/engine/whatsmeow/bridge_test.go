package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/purpshell/meowcaller"
	"github.com/rs/zerolog"
	wm "go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/calls"
	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/testwait"
)

// newCallSession is a connected test session that carries calls: a media socket of its
// own, and a connect that asked for `calls.answer`.
func newCallSession(t *testing.T) *Session {
	t.Helper()
	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	media, err := calls.Open(calls.Config{}, zerolog.Nop())
	if err != nil {
		t.Fatalf("open the call media: %v", err)
	}
	t.Cleanup(func() { _ = media.Close() })
	session.callMedia = media
	session.setCallPolicy(false, true)
	return session
}

// fakeCall stands in for a meowcaller call, recording what the session asked of it.
type fakeCall struct {
	id string

	mu       sync.Mutex
	answered int
	rejected int
	hungUp   int
	sink     meowcaller.AudioSink
	source   meowcaller.AudioSource
	onEnd    func(string)
	onAccept func()
	fail     error
	// ended is a call meowcaller already ended, before anybody listened for it.
	ended bool
	// stall, when set, holds every hang-up and rejection until it is closed: a node
	// write the socket does not take.
	stall chan struct{}
	// signalled receives one value each time a hang-up or a rejection is attempted.
	signalled chan struct{}
	// answerStall, when set, holds Answer until it is closed.
	answerStall chan struct{}
	// onReceive runs when the voice is wired to the call.
	onReceive func()
}

func (c *fakeCall) ID() string { return c.id }

func (c *fakeCall) Answer() error {
	if c.answerStall != nil {
		<-c.answerStall
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.answered++
	return c.fail
}

func (c *fakeCall) Reject() error {
	c.mu.Lock()
	c.rejected++
	end := c.onEnd
	c.mu.Unlock()
	return c.signal(end, "rejected")
}

func (c *fakeCall) Hangup() error {
	c.mu.Lock()
	c.hungUp++
	end := c.onEnd
	c.mu.Unlock()
	return c.signal(end, "hangup")
}

func (c *fakeCall) signal(end func(string), reason string) error {
	if c.signalled != nil {
		c.signalled <- struct{}{}
	}
	if c.stall != nil {
		<-c.stall
	}
	if end != nil {
		end(reason)
	}
	return nil
}

func (c *fakeCall) State() meowcaller.CallPhase {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ended {
		return meowcaller.CallPhaseEnded
	}
	return meowcaller.CallPhaseRinging
}

func (c *fakeCall) Receive(sink meowcaller.AudioSink) {
	c.mu.Lock()
	c.sink = sink
	hook := c.onReceive
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (c *fakeCall) Play(source meowcaller.AudioSource) *meowcaller.Player {
	c.mu.Lock()
	c.source = source
	c.mu.Unlock()
	return nil
}

func (c *fakeCall) OnEnd(fn func(string)) {
	c.mu.Lock()
	c.onEnd = fn
	c.mu.Unlock()
}

func (c *fakeCall) OnPeerAccept(fn func()) {
	c.mu.Lock()
	c.onAccept = fn
	c.mu.Unlock()
}

func (c *fakeCall) counts() (answered, rejected, hungUp int, wired bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.answered, c.rejected, c.hungUp, c.sink != nil && c.source != nil
}

// peerAccepts is the callee picking up a call the client placed.
func (c *fakeCall) peerAccepts() {
	c.mu.Lock()
	accept := c.onAccept
	c.mu.Unlock()
	accept()
}

// remoteEnds is the other side ending the call.
func (c *fakeCall) remoteEnds(reason string) {
	c.mu.Lock()
	end := c.onEnd
	c.mu.Unlock()
	end(reason)
}

// browserPeer is a pion peer standing in for the agent's browser, offering or answering
// in the given codecs.
func browserPeer(t *testing.T, mimes ...string) *webrtc.PeerConnection {
	t.Helper()
	codecs := &webrtc.MediaEngine{}
	for _, mime := range mimes {
		pt := webrtc.PayloadType(9)
		if mime == webrtc.MimeTypePCMU {
			pt = 0
		}
		if err := codecs.RegisterCodec(webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: mime, ClockRate: 8000},
			PayloadType:        pt,
		}, webrtc.RTPCodecTypeAudio); err != nil {
			t.Fatal(err)
		}
	}
	pc, err := webrtc.NewAPI(webrtc.WithMediaEngine(codecs)).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio); err != nil {
		t.Fatal(err)
	}
	return pc
}

func gatheredSDP(t *testing.T, pc *webrtc.PeerConnection) string {
	t.Helper()
	select {
	case <-webrtc.GatheringCompletePromise(pc):
	case <-time.After(10 * time.Second):
		t.Fatal("the browser peer never finished gathering")
	}
	return pc.LocalDescription().SDP
}

// browserAnswer is the browser's answer to the connector's offer.
func browserAnswer(t *testing.T, offer string) string {
	t.Helper()
	pc := browserPeer(t, webrtc.MimeTypeG722, webrtc.MimeTypePCMU)
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}); err != nil {
		t.Fatalf("the browser applies the offer: %v", err)
	}
	answer, err := pc.CreateAnswer(nil)
	if err == nil {
		err = pc.SetLocalDescription(answer)
	}
	if err != nil {
		t.Fatal(err)
	}
	return gatheredSDP(t, pc)
}

// browserOffer is the browser's offer for a call the client places.
func browserOffer(t *testing.T) string {
	t.Helper()
	pc := browserPeer(t, webrtc.MimeTypeG722, webrtc.MimeTypePCMU)
	offer, err := pc.CreateOffer(nil)
	if err == nil {
		err = pc.SetLocalDescription(offer)
	}
	if err != nil {
		t.Fatal(err)
	}
	return gatheredSDP(t, pc)
}

func audioOffer(callID string) *waEvents.CallOffer {
	return &waEvents.CallOffer{BasicCallMeta: callMeta(callID), Data: offerNode("audio")}
}

// ringCall is a call meowcaller engaged, offered: what the dispatch does with one offer.
func ringCall(t *testing.T, session *Session, callID string) (call *fakeCall, sdp string) {
	t.Helper()
	call = &fakeCall{id: callID}
	session.ringing(call)
	session.handle(audioOffer(callID))
	payload := published(t, session, protocol.EventCallOffer, "event_call_offer")
	sdp, _ = payload["sdp"].(string)
	return call, sdp
}

func command(kind protocol.CommandType, payload string) *protocol.Command {
	return &protocol.Command{ID: "cmd-" + string(kind), Type: kind, Payload: json.RawMessage(payload)}
}

// The offer of a call the session carries has the connector's SDP in it, with both
// codecs and G.722 first.
func TestAReceivedCallIsOfferedWithTheConnectorsSDP(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)

	_, sdp := ringCall(t, session, "call-1")
	if !strings.HasPrefix(sdp, "v=0") || !strings.Contains(sdp, "m=audio") {
		t.Fatalf("the offer carries no usable SDP: %q", sdp)
	}
	line := ""
	for l := range strings.SplitSeq(sdp, "\n") {
		if strings.HasPrefix(l, "m=audio") {
			line = strings.TrimSpace(l)
		}
	}
	if fields := strings.Fields(line); len(fields) < 5 || fields[3] != "9" || fields[4] != "0" {
		t.Fatalf("the audio line is %q, want G.722 (9) offered before PCMU (0)", line)
	}
}

// A call meowcaller did not engage -- a session that does not answer, a deployment with
// no media port -- is offered the way it always was.
func TestACallTheSessionDoesNotCarryIsOfferedWithoutSDP(t *testing.T) {
	t.Parallel()

	for name, build := range map[string]func(t *testing.T) *Session{
		"a session that did not ask": func(t *testing.T) *Session {
			s := newCallSession(t)
			s.setCallPolicy(false, false)
			return s
		},
		"a deployment with no media port": func(t *testing.T) *Session {
			s, _ := callSession(t, false)
			s.setCallPolicy(false, true)
			return s
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session := build(t)
			session.handle(audioOffer("call-1"))
			payload := published(t, session, protocol.EventCallOffer, "event_call_offer")
			if _, has := payload["sdp"]; has {
				t.Fatalf("the offer carries sdp: %v", payload)
			}
		})
	}
}

// meowcaller's gate: only a 1:1 voice call on a session that answers calls is engaged.
func TestOnlyADirectVoiceCallIsEngaged(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)

	if !session.engagesOffer(audioOffer("call-1")) {
		t.Fatal("a direct voice call on a session that answers was not engaged")
	}
	video := &waEvents.CallOffer{BasicCallMeta: callMeta("call-2"), Data: offerNode("video")}
	if session.engagesOffer(video) {
		t.Error("a video call was engaged")
	}
	group := audioOffer("call-3")
	group.GroupJID = waTypes.NewJID("120363000000000000", waTypes.GroupServer)
	if session.engagesOffer(group) {
		t.Error("a group call was engaged")
	}
	bare := &waEvents.CallOffer{BasicCallMeta: callMeta("call-4"), Data: &waBinary.Node{Tag: "offer"}}
	if session.engagesOffer(bare) {
		t.Error("an offer that does not say it is a voice call was engaged")
	}

	session.setCallPolicy(true, true)
	if session.engagesOffer(audioOffer("call-5")) {
		t.Error("auto_reject did not win over answer")
	}
	session.setCallPolicy(false, false)
	if session.engagesOffer(audioOffer("call-6")) {
		t.Error("a session that did not ask for calls.answer engaged a call")
	}
	plain, _ := callSession(t, false)
	plain.setCallPolicy(false, true)
	if plain.engagesOffer(audioOffer("call-7")) {
		t.Error("a deployment with no media port engaged a call")
	}
}

// The browser's answer is applied before WhatsApp is told, and then the voice is wired.
// A second accept for the same call is nothing new.
func TestAcceptAnswersOnWhatsAppOnceTheBrowserAnswerIsApplied(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call, offer := ringCall(t, session, "call-1")

	accept := `{"call_id":"call-1","sdp":` + mustJSON(t, browserAnswer(t, offer)) + `}`
	if _, err := session.Execute(t.Context(), command(protocol.CommandCallAccept, accept)); err != nil {
		t.Fatalf("call.accept: %v", err)
	}
	answered, _, _, wired := call.counts()
	if answered != 1 || !wired {
		t.Fatalf("answered %d times, wired %v; want once, wired", answered, wired)
	}

	if _, err := session.Execute(t.Context(), command(protocol.CommandCallAccept, accept)); err != nil {
		t.Fatalf("a repeated call.accept: %v", err)
	}
	if answered, _, _, _ := call.counts(); answered != 1 {
		t.Fatalf("a repeated accept answered the call again: %d", answered)
	}
}

// An answer the connector cannot use fails the command and leaves the call ringing.
func TestAnAcceptWithABadSDPDoesNotAnswer(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call, _ := ringCall(t, session, "call-1")

	_, err := session.Execute(t.Context(), command(protocol.CommandCallAccept, `{"call_id":"call-1","sdp":"this is not an sdp"}`))
	assertCode(t, err, protocol.ErrorInvalidPayload)
	if answered, _, _, _ := call.counts(); answered != 0 {
		t.Fatal("the call was answered on WhatsApp with no browser behind it")
	}
}

func TestAcceptOfACallThatIsNotRinging(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)

	_, err := session.Execute(t.Context(), command(protocol.CommandCallAccept, `{"call_id":"nobody","sdp":"v=0"}`))
	assertCode(t, err, protocol.ErrorInvalidPayload)

	call, _ := ringCall(t, session, "call-1")
	call.remoteEnds("timeout")
	published(t, session, protocol.EventCallTerminate, "event_call_terminate")
	if _, err := session.Execute(t.Context(), command(protocol.CommandCallAccept, `{"call_id":"call-1","sdp":"v=0"}`)); err != nil {
		t.Fatalf("accepting a call that already ended = %v, want nothing new", err)
	}
	if answered, _, _, _ := call.counts(); answered != 0 {
		t.Fatal("a call that had ended was answered")
	}
}

// A call that rang and was offered without SDP cannot be accepted: there is no offer to
// answer.
func TestACallOfferedWithoutSDPCannotBeAccepted(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call := &fakeCall{id: "call-1"}
	session.ringing(call)

	_, err := session.Execute(t.Context(), command(protocol.CommandCallAccept, `{"call_id":"call-1","sdp":"v=0"}`))
	assertCode(t, err, protocol.ErrorInvalidPayload)
	if answered, _, _, _ := call.counts(); answered != 0 {
		t.Fatal("a call with no browser leg was answered")
	}
}

// call.terminate refuses a call still ringing, hangs up one that was answered, and is
// nothing new for one that is over or was never here. The end is published once.
func TestTerminateEndsTheCallAndIsPublishedOnce(t *testing.T) {
	t.Parallel()

	t.Run("ringing", func(t *testing.T) {
		t.Parallel()
		session := newCallSession(t)
		call, _ := ringCall(t, session, "call-1")
		if _, err := session.Execute(t.Context(), command(protocol.CommandCallTerminate, `{"call_id":"call-1"}`)); err != nil {
			t.Fatal(err)
		}
		if _, rejected, hungUp, _ := call.counts(); rejected != 1 || hungUp != 0 {
			t.Fatalf("rejected %d, hung up %d; want a rejection", rejected, hungUp)
		}
		published(t, session, protocol.EventCallTerminate, "event_call_terminate")
	})

	t.Run("answered", func(t *testing.T) {
		t.Parallel()
		session := newCallSession(t)
		call, offer := ringCall(t, session, "call-1")
		accept := `{"call_id":"call-1","sdp":` + mustJSON(t, browserAnswer(t, offer)) + `}`
		if _, err := session.Execute(t.Context(), command(protocol.CommandCallAccept, accept)); err != nil {
			t.Fatal(err)
		}
		if _, err := session.Execute(t.Context(), command(protocol.CommandCallTerminate, `{"call_id":"call-1"}`)); err != nil {
			t.Fatal(err)
		}
		if _, rejected, hungUp, _ := call.counts(); rejected != 0 || hungUp != 1 {
			t.Fatalf("rejected %d, hung up %d; want a hang-up", rejected, hungUp)
		}
		end := published(t, session, protocol.EventCallTerminate, "event_call_terminate")
		if end["call_id"] != "call-1" {
			t.Fatalf("the end names %v", end["call_id"])
		}

		// WhatsApp's own terminate for the same call, and a second command: neither
		// publishes the end again.
		session.handle(&waEvents.CallTerminate{BasicCallMeta: callMeta("call-1"), Reason: "hangup"})
		if _, err := session.Execute(t.Context(), command(protocol.CommandCallTerminate, `{"call_id":"call-1"}`)); err != nil {
			t.Fatal(err)
		}
		if _, _, hungUp, _ := call.counts(); hungUp != 1 {
			t.Fatalf("a call that ended was hung up again: %d", hungUp)
		}
		select {
		case emission := <-session.Events():
			t.Fatalf("published %s after the end was already out", emission.Type)
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("never here", func(t *testing.T) {
		t.Parallel()
		session := newCallSession(t)
		if _, err := session.Execute(t.Context(), command(protocol.CommandCallTerminate, `{"call_id":"nobody"}`)); err != nil {
			t.Fatalf("ending a call that is not here = %v, want nothing new", err)
		}
	})
}

// A call the client places: the number as WhatsApp registered it is dialled, the id comes
// back at once, and the browser's answer goes out once the callee picks up.
func TestStartDialsTheRegisteredNumberAndAnswersWhenTheCalleePicksUp(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	session.onWhatsApp = func(_ context.Context, _ *wm.Client, phones []string) ([]waTypes.IsOnWhatsAppResponse, error) {
		if len(phones) != 1 || phones[0] != "+5541999990000" {
			t.Errorf("asked WhatsApp about %v", phones)
		}
		// Registered without the ninth digit, and with a LID for a JID.
		return []waTypes.IsOnWhatsAppResponse{{
			Query: phones[0], IsIn: true,
			JID:         waTypes.NewJID("182736451928374", waTypes.HiddenUserServer),
			PhoneNumber: waTypes.NewJID("554199990000", waTypes.DefaultUserServer),
		}}, nil
	}
	call := &fakeCall{id: "CALLOUT1"}
	var dialled string
	session.dialCall = func(_ context.Context, target string) (bridgedCall, error) {
		dialled = target
		return call, nil
	}

	start := &protocol.Command{
		ID: "c1", Type: protocol.CommandCallStart, IdempotencyKey: "k1",
		Payload: json.RawMessage(`{"to":{"kind":"phone","id":"5541999990000"},"sdp":` + mustJSON(t, browserOffer(t)) + `}`),
	}
	result, err := session.Execute(t.Context(), start)
	if err != nil {
		t.Fatalf("call.start: %v", err)
	}
	if dialled != "554199990000" {
		t.Fatalf("dialled %q, want the number WhatsApp registered", dialled)
	}
	if got := decode(t, result)["call_id"]; got != "CALLOUT1" {
		t.Fatalf("call_id is %v", got)
	}

	call.peerAccepts()
	answered := published(t, session, protocol.EventCallAnswered, "event_call_answered")
	if answered["call_id"] != "CALLOUT1" {
		t.Fatalf("call.answered names %v", answered["call_id"])
	}
	if sdp, _ := answered["sdp"].(string); !strings.HasPrefix(sdp, "v=0") || !strings.Contains(sdp, "G722") {
		t.Fatalf("call.answered carries %q, want the connector's G.722 answer", sdp)
	}
	if _, _, _, wired := call.counts(); !wired {
		t.Fatal("the voice was not wired once the callee picked up")
	}
}

func TestStartRefusesWhatItCannotDial(t *testing.T) {
	t.Parallel()

	offerless := `{"to":{"kind":"phone","id":"5541999990000"},"sdp":"v=0"}`
	for name, test := range map[string]struct {
		command *protocol.Command
		found   []waTypes.IsOnWhatsAppResponse
		want    protocol.ErrorCode
	}{
		"no idempotency key": {
			command: &protocol.Command{Type: protocol.CommandCallStart, Payload: json.RawMessage(offerless)},
			want:    protocol.ErrorInvalidPayload,
		},
		"a LID": {
			command: &protocol.Command{Type: protocol.CommandCallStart, IdempotencyKey: "k",
				Payload: json.RawMessage(`{"to":{"kind":"lid","id":"182736451928374"},"sdp":"v=0"}`)},
			want: protocol.ErrorInvalidPayload,
		},
		"a number not on WhatsApp": {
			command: &protocol.Command{Type: protocol.CommandCallStart, IdempotencyKey: "k", Payload: json.RawMessage(offerless)},
			found:   []waTypes.IsOnWhatsAppResponse{{Query: "+5541999990000", IsIn: false}},
			want:    protocol.ErrorRecipientNotOnWhatsapp,
		},
		"an offer that is not an SDP": {
			command: &protocol.Command{Type: protocol.CommandCallStart, IdempotencyKey: "k", Payload: json.RawMessage(offerless)},
			found: []waTypes.IsOnWhatsAppResponse{{Query: "+5541999990000", IsIn: true,
				JID: waTypes.NewJID("5541999990000", waTypes.DefaultUserServer)}},
			want: protocol.ErrorInvalidPayload,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session := newCallSession(t)
			session.onWhatsApp = func(context.Context, *wm.Client, []string) ([]waTypes.IsOnWhatsAppResponse, error) {
				return test.found, nil
			}
			session.dialCall = func(context.Context, string) (bridgedCall, error) {
				t.Error("a call was dialled")
				return nil, errors.New("no")
			}
			_, err := session.Execute(t.Context(), test.command)
			assertCode(t, err, test.want)
		})
	}
}

// Every call command is refused on a session that does not carry calls, including one
// that asked for them on a deployment with no media port.
func TestTheCallCommandsAreRefusedWithoutCalls(t *testing.T) {
	t.Parallel()
	session, _ := callSession(t, false)
	session.setCallPolicy(false, true)
	for _, kind := range []protocol.CommandType{protocol.CommandCallAccept, protocol.CommandCallStart, protocol.CommandCallTerminate} {
		if _, err := session.Execute(t.Context(), command(kind, `{"call_id":"c","sdp":"v=0"}`)); !errors.Is(err, engine.ErrNotSupported) {
			t.Errorf("%s answered %v, want ErrNotSupported", kind, err)
		}
	}
}

// answeredCall is a received call the browser answered, whose hang-up and rejection stall
// until the returned function is called.
func answeredCall(t *testing.T, session *Session, callID string) (call *fakeCall, release func()) {
	t.Helper()
	call = &fakeCall{id: callID, stall: make(chan struct{}), signalled: make(chan struct{}, 4)}
	session.ringing(call)
	session.handle(audioOffer(callID))
	offer, _ := published(t, session, protocol.EventCallOffer, "event_call_offer")["sdp"].(string)
	accept := `{"call_id":"` + callID + `","sdp":` + mustJSON(t, browserAnswer(t, offer)) + `}`
	if _, err := session.Execute(t.Context(), command(protocol.CommandCallAccept, accept)); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release = func() { once.Do(func() { close(call.stall) }) }
	t.Cleanup(release)
	return call, release
}

func hungUpWithin(t *testing.T, call *fakeCall) {
	t.Helper()
	select {
	case <-call.signalled:
	case <-time.After(10 * time.Second):
		t.Fatal("the call was never hung up")
	}
}

// A session that closes hangs up every call it carries, since the media is this
// instance's and cannot follow the account, and does not wait for the hang-up to be
// written: a close is also the lease being lost, and that socket has to go now.
func TestClosingTheSessionHangsUpItsCallsWithoutWaiting(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call, release := answeredCall(t, session, "call-1")

	closed := make(chan error, 1)
	go func() { closed <- session.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(5 * time.Second):
		release()
		t.Fatal("close waited on a hang-up the socket did not take")
	}
	hungUpWithin(t, call)
}

// An operator's disconnect ends the calls too, before the socket goes, and waits for the
// hang-ups as long as the command may.
func TestDisconnectingTheSessionHangsUpItsCalls(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call, release := answeredCall(t, session, "call-1")
	release()

	if err := session.Disconnect(t.Context()); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	hungUpWithin(t, call)
	if _, _, hungUp, _ := call.counts(); hungUp != 1 {
		t.Fatalf("the call was hung up %d times on disconnect, want once", hungUp)
	}
}

// A call.terminate whose hang-up the socket does not take gives the executor back when
// the command's deadline passes, rather than holding every command behind it.
func TestATerminateThatStallsHonoursItsDeadline(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call, _ := answeredCall(t, session, "call-1")

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	returned := make(chan error, 1)
	go func() {
		_, err := session.Execute(ctx, command(protocol.CommandCallTerminate, `{"call_id":"call-1"}`))
		returned <- err
	}()
	select {
	case err := <-returned:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("a stalled terminate returned %v, want the deadline", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a stalled terminate held the executor past its deadline")
	}
	hungUpWithin(t, call)
}

// meowcaller does not replay an end that happened before OnEnd was set. A placed call
// refused between the offer going out and the listener going on is reported over, and
// nothing of it is left behind.
func TestAPlacedCallThatEndedBeforeItsListenerIsReportedOver(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	session.onWhatsApp = func(_ context.Context, _ *wm.Client, phones []string) ([]waTypes.IsOnWhatsAppResponse, error) {
		return []waTypes.IsOnWhatsAppResponse{{Query: phones[0], IsIn: true,
			JID: waTypes.NewJID("5541999990000", waTypes.DefaultUserServer)}}, nil
	}
	call := &fakeCall{id: "CALLOUT1", ended: true}
	session.dialCall = func(context.Context, string) (bridgedCall, error) { return call, nil }
	start := &protocol.Command{
		ID: "c1", Type: protocol.CommandCallStart, IdempotencyKey: "k1",
		Payload: json.RawMessage(`{"to":{"kind":"phone","id":"5541999990000"},"sdp":` + mustJSON(t, browserOffer(t)) + `}`),
	}
	if _, err := session.Execute(t.Context(), start); err != nil {
		t.Fatalf("call.start: %v", err)
	}
	if end := published(t, session, protocol.EventCallTerminate, "event_call_terminate"); end["call_id"] != "CALLOUT1" {
		t.Fatalf("the end names %v", end["call_id"])
	}
	session.bridge.mu.Lock()
	left := len(session.bridge.live)
	session.bridge.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d calls are still carried after the call ended", left)
	}
}

// The callee picks up a placed call only after the browser's offer has waited however
// long the phone rang, and the answer is made then, not when the call was placed.
func TestAPlacedCallIsAnsweredOnlyWhenTheCalleePicksUp(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	session.onWhatsApp = func(_ context.Context, _ *wm.Client, phones []string) ([]waTypes.IsOnWhatsAppResponse, error) {
		return []waTypes.IsOnWhatsAppResponse{{Query: phones[0], IsIn: true,
			JID: waTypes.NewJID("5541999990000", waTypes.DefaultUserServer)}}, nil
	}
	call := &fakeCall{id: "CALLOUT1"}
	session.dialCall = func(context.Context, string) (bridgedCall, error) { return call, nil }
	start := &protocol.Command{
		ID: "c1", Type: protocol.CommandCallStart, IdempotencyKey: "k1",
		Payload: json.RawMessage(`{"to":{"kind":"phone","id":"5541999990000"},"sdp":` + mustJSON(t, browserOffer(t)) + `}`),
	}
	if _, err := session.Execute(t.Context(), start); err != nil {
		t.Fatalf("call.start: %v", err)
	}
	session.bridge.mu.Lock()
	leg := session.bridge.live["CALLOUT1"].leg
	session.bridge.mu.Unlock()
	if leg.Codec() != "" {
		t.Fatal("the browser's offer was answered while the callee was still being rung")
	}
	call.peerAccepts()
	published(t, session, protocol.EventCallAnswered, "event_call_answered")
}

func callNotice(callID string) *waEvents.CallOfferNotice {
	return &waEvents.CallOfferNotice{BasicCallMeta: callMeta(callID), Media: "audio", Type: "1:1"}
}

// WhatsApp sends `offer_notice` and `offer` for one call in either order, and only the
// offer is engaged. A notice first does not take the call's one `call.offer` from the
// offer that can carry the SDP; a notice whose offer never comes still publishes the call.
func TestANoticeBeforeTheOfferLeavesTheSDPToTheOffer(t *testing.T) {
	t.Parallel()

	t.Run("the offer follows", func(t *testing.T) {
		t.Parallel()
		session := newCallSession(t)
		session.offerWait = 200 * time.Millisecond
		session.handle(callNotice("call-1"))
		session.ringing(&fakeCall{id: "call-1"})
		session.handle(audioOffer("call-1"))
		offer := published(t, session, protocol.EventCallOffer, "event_call_offer")
		if sdp, _ := offer["sdp"].(string); !strings.HasPrefix(sdp, "v=0") {
			t.Fatalf("the offer that followed a notice carries %q, want the connector's SDP", sdp)
		}
		select {
		case emission := <-session.Events():
			t.Fatalf("published %s for a call already offered", emission.Type)
		case <-time.After(600 * time.Millisecond):
		}
	})

	t.Run("no offer comes", func(t *testing.T) {
		t.Parallel()
		session := newCallSession(t)
		session.offerWait = 10 * time.Millisecond
		session.handle(callNotice("call-1"))
		offer := published(t, session, protocol.EventCallOffer, "event_call_offer")
		if offer["call_id"] != "call-1" {
			t.Fatalf("the offer names %v", offer["call_id"])
		}
		if _, carries := offer["sdp"]; carries {
			t.Fatal("a call announced only by its notice was offered with SDP")
		}
	})

	t.Run("the call ends first", func(t *testing.T) {
		t.Parallel()
		session := newCallSession(t)
		session.offerWait = 50 * time.Millisecond
		session.handle(callNotice("call-1"))
		session.handle(&waEvents.CallTerminate{BasicCallMeta: callMeta("call-1"), Reason: "timeout"})
		published(t, session, protocol.EventCallTerminate, "event_call_terminate")
		select {
		case emission := <-session.Events():
			t.Fatalf("published %s for a call whose end was already out", emission.Type)
		case <-time.After(400 * time.Millisecond):
		}
	})

	t.Run("a session that does not answer", func(t *testing.T) {
		t.Parallel()
		session := newCallSession(t)
		session.setCallPolicy(false, false)
		session.offerWait = time.Hour
		session.handle(callNotice("call-1"))
		published(t, session, protocol.EventCallOffer, "event_call_offer")
	})
}

// placeCall starts a call to a number WhatsApp knows, over the given fake.
func placeCall(t *testing.T, session *Session, call *fakeCall) {
	t.Helper()
	session.onWhatsApp = func(_ context.Context, _ *wm.Client, phones []string) ([]waTypes.IsOnWhatsAppResponse, error) {
		return []waTypes.IsOnWhatsAppResponse{{Query: phones[0], IsIn: true,
			JID: waTypes.NewJID("5541999990000", waTypes.DefaultUserServer)}}, nil
	}
	session.dialCall = func(context.Context, string) (bridgedCall, error) { return call, nil }
	start := &protocol.Command{
		ID: "c1", Type: protocol.CommandCallStart, IdempotencyKey: "k1",
		Payload: json.RawMessage(`{"to":{"kind":"phone","id":"5541999990000"},"sdp":` + mustJSON(t, browserOffer(t)) + `}`),
	}
	if _, err := session.Execute(t.Context(), start); err != nil {
		t.Fatalf("call.start: %v", err)
	}
}

// A dial that fails may have rung the phone already: the failure carries the mark that
// keeps the command's attempt standing, so a redelivery under the same key does not ring
// it again.
func TestAFailedDialMayHaveRungThePhone(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	session.onWhatsApp = func(_ context.Context, _ *wm.Client, phones []string) ([]waTypes.IsOnWhatsAppResponse, error) {
		return []waTypes.IsOnWhatsAppResponse{{Query: phones[0], IsIn: true,
			JID: waTypes.NewJID("5541999990000", waTypes.DefaultUserServer)}}, nil
	}
	session.dialCall = func(context.Context, string) (bridgedCall, error) { return nil, errors.New("socket went") }
	start := &protocol.Command{
		ID: "c1", Type: protocol.CommandCallStart, IdempotencyKey: "k1",
		Payload: json.RawMessage(`{"to":{"kind":"phone","id":"5541999990000"},"sdp":` + mustJSON(t, browserOffer(t)) + `}`),
	}
	_, err := session.Execute(t.Context(), start)
	assertCode(t, err, protocol.ErrorWaError)
	if !errors.Is(err, engine.ErrMayHaveLanded) {
		t.Fatalf("a failed dial = %v, without the mark that keeps its attempt", err)
	}
}

// A call.accept whose answer the socket does not take gives the executor back at its
// deadline, and the voice is still wired if the answer lands afterwards.
func TestAnAcceptThatStallsHonoursItsDeadline(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call := &fakeCall{id: "call-1", answerStall: make(chan struct{})}
	session.ringing(call)
	session.handle(audioOffer("call-1"))
	offer, _ := published(t, session, protocol.EventCallOffer, "event_call_offer")["sdp"].(string)
	accept := `{"call_id":"call-1","sdp":` + mustJSON(t, browserAnswer(t, offer)) + `}`

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	returned := make(chan error, 1)
	go func() {
		_, err := session.Execute(ctx, command(protocol.CommandCallAccept, accept))
		returned <- err
	}()
	select {
	case err := <-returned:
		if !errors.Is(err, context.DeadlineExceeded) {
			close(call.answerStall)
			t.Fatalf("a stalled accept returned %v, want the deadline", err)
		}
	case <-time.After(5 * time.Second):
		close(call.answerStall)
		t.Fatal("a stalled accept held the executor past its deadline")
	}
	close(call.answerStall)
	deadline := time.Now().Add(testwait.Budget)
	for {
		if _, _, _, wired := call.counts(); wired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the answer landed after the deadline and the voice was never wired")
		}
		time.Sleep(testwait.Poll)
	}
}

// call.answered is published off the dispatch: the callee's accept arrives on it, and an
// inbox with no room must not hold it up.
func TestTheAnswerOfAPlacedCallDoesNotWaitForRoom(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call := &fakeCall{id: "CALLOUT1"}
	placeCall(t, session, call)
	drain(t, session)
	filled := 0
fill:
	for {
		select {
		case session.inbox <- pending{}:
			filled++
		default:
			break fill
		}
	}

	accepted := make(chan struct{})
	go func() { call.peerAccepts(); close(accepted) }()
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("the callee's accept waited for room in a full inbox")
	}
	for range filled {
		<-session.Events()
	}
	published(t, session, protocol.EventCallAnswered, "event_call_answered")
}

// A placed call that ends while the callee's pick-up is being handled is not reported
// answered after its end, whether it ended before the voice was wired or during it.
func TestAPlacedCallThatEndsAsItIsAnsweredIsNotReportedAnswered(t *testing.T) {
	t.Parallel()
	for _, when := range []string{"before", "during"} {
		t.Run(when, func(t *testing.T) {
			t.Parallel()
			session := newCallSession(t)
			call := &fakeCall{id: "CALLOUT1"}
			placeCall(t, session, call)
			if when == "before" {
				call.remoteEnds("rejected")
			} else {
				call.onReceive = func() { call.remoteEnds("rejected") }
			}
			call.peerAccepts()
			published(t, session, protocol.EventCallTerminate, "event_call_terminate")
			select {
			case emission := <-session.Events():
				t.Fatalf("published %s after the call ended", emission.Type)
			case <-time.After(300 * time.Millisecond):
			}
			if _, _, _, wired := call.counts(); when == "before" && wired {
				t.Fatal("the voice was wired to a call that had already ended")
			}
		})
	}
}

// meowcaller can take a while between engaging an offer and handing the call over. A
// notice whose wait runs out in that window leaves the call's one call.offer to the offer.
func TestANoticeDoesNotTakeTheOfferMeowcallerIsStillWorkingThrough(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	session.offerWait = 10 * time.Millisecond
	if !session.engagesOffer(audioOffer("call-1")) {
		t.Fatal("the gate did not engage a voice call")
	}
	session.handle(callNotice("call-1"))
	select {
	case emission := <-session.Events():
		t.Fatalf("published %s while meowcaller was still working through the offer", emission.Type)
	case <-time.After(300 * time.Millisecond):
	}
	session.ringing(&fakeCall{id: "call-1"})
	session.handle(audioOffer("call-1"))
	if sdp, _ := published(t, session, protocol.EventCallOffer, "event_call_offer")["sdp"].(string); !strings.HasPrefix(sdp, "v=0") {
		t.Fatalf("the offer carries %q, want the connector's SDP", sdp)
	}
}

// A client replaced -- WhatsApp logging the device out, a rebuild after a drop -- takes its
// calls with it: meowcaller does not see that, and a call left to it would outlive the
// account it belonged to.
func TestReplacingTheClientEndsItsCalls(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call, release := answeredCall(t, session, "call-1")
	release()
	if !session.adopt(t.Context(), session.current()) {
		t.Fatal("the session would not take a client")
	}
	hungUpWithin(t, call)
	session.bridge.mu.Lock()
	left := len(session.bridge.live)
	session.bridge.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d calls are still carried by a client that was replaced", left)
	}
}

// With the inbox full, the callee's answer and then the call's end both wait for room, and
// they reach the client in that order.
func TestTheAnswerOfAPlacedCallIsPublishedBeforeItsEnd(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call := &fakeCall{id: "CALLOUT1"}
	placeCall(t, session, call)
	drain(t, session)
	filled := 0
fill:
	for {
		select {
		case session.inbox <- pending{}:
			filled++
		default:
			break fill
		}
	}
	call.peerAccepts()
	call.remoteEnds("hangup")

	var order []protocol.EventType
	deadline := time.After(testwait.Budget)
	for len(order) < 2 {
		select {
		case emission := <-session.Events():
			if emission.Type == protocol.EventCallAnswered || emission.Type == protocol.EventCallTerminate {
				order = append(order, emission.Type)
			}
		case <-deadline:
			t.Fatalf("published %v and nothing more", order)
		}
	}
	if order[0] != protocol.EventCallAnswered {
		t.Fatalf("published %v, want the answer before the end", order)
	}
}

// An accept whose deadline passed goes on wiring the call from its own goroutine while a
// call.terminate for it runs on the executor; the two read and write the call's state
// under one lock, which is what -race checks here, and the terminate hangs up the call
// that the accept answered.
func TestATerminateRacingALateAcceptHangsUp(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call := &fakeCall{id: "call-1", answerStall: make(chan struct{})}
	session.ringing(call)
	session.handle(audioOffer("call-1"))
	offer, _ := published(t, session, protocol.EventCallOffer, "event_call_offer")["sdp"].(string)
	accept := `{"call_id":"call-1","sdp":` + mustJSON(t, browserAnswer(t, offer)) + `}`
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := session.Execute(ctx, command(protocol.CommandCallAccept, accept)); !errors.Is(err, context.DeadlineExceeded) {
		close(call.answerStall)
		t.Fatalf("a stalled accept returned %v, want the deadline", err)
	}
	close(call.answerStall)
	if _, err := session.Execute(t.Context(), command(protocol.CommandCallTerminate, `{"call_id":"call-1"}`)); err != nil {
		t.Fatal(err)
	}
	answered, rejected, hungUp, _ := call.counts()
	if answered != 1 || rejected+hungUp != 1 {
		t.Fatalf("answered %d, rejected %d, hung up %d", answered, rejected, hungUp)
	}
}
