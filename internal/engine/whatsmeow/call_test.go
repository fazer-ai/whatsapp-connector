package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	wm "go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

const theCaller = "5511988887777"

// refusals is what a session was asked to decline, in order, for the cases that assert
// the policy fired rather than just that an event came out.
//
// The address is kept beside the call id, and not only the id, because who the refusal
// names is a decision this package makes -- the client's `call.reject` carries no device
// and the session fills one in from the offer -- and a seam that dropped it would let that
// decision be reversed by a green suite.
type refusals struct {
	mu     sync.Mutex
	calls  []string
	named  []waTypes.JID
	answer error
}

func (r *refusals) record(caller waTypes.JID, callID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, callID)
	r.named = append(r.named, caller)
	return r.answer
}

func (r *refusals) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// addressed is who each refusal named, in the same order as seen().
func (r *refusals) addressed() []waTypes.JID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]waTypes.JID(nil), r.named...)
}

// callSession is a connected session with the call seam watched.
func callSession(t *testing.T, autoReject bool) (*Session, *refusals) {
	t.Helper()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.setGroups(true)
	session.setCallPolicy(autoReject)

	watched := &refusals{}
	session.declineCall = func(_ context.Context, _ *wm.Client, caller waTypes.JID, callID string) error {
		return watched.record(caller, callID)
	}
	return session, watched
}

// codeOf is the word the contract would carry for an error, or the empty string when the
// error is not one of ours.
func codeOf(err error) protocol.ErrorCode {
	var coded *protocol.Error
	if !errors.As(err, &coded) {
		return ""
	}
	return coded.Code
}

// offerNode is the offer whatsmeow hands through as `Data`, carrying the stream WhatsApp
// put in it.
func offerNode(media string) *waBinary.Node {
	return &waBinary.Node{Tag: "offer", Content: []waBinary.Node{{Tag: media}}}
}

func callMeta(callID string) waTypes.BasicCallMeta {
	return waTypes.BasicCallMeta{
		From:        someone(theCaller),
		CallCreator: someone(theCaller),
		CallID:      callID,
		Timestamp:   time.UnixMilli(1755440000123),
	}
}

// refusedWithin waits for the policy's rejection, which is written off the dispatch
// goroutine and so is not on the caller's clock.
//
// Polled rather than slept on: the write is a seam call with nothing between it and the
// handler, so it lands immediately on an idle machine and this only exists for a loaded
// one.
func refusedWithin(t *testing.T, watched *refusals, want string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if seen := watched.seen(); len(seen) > 0 {
			if seen[0] != want {
				t.Fatalf("refused call %q, want %q", seen[0], want)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the call was never refused")
}

// The event a client needs to show that somebody rang: who, which call, and whether it
// was video. Held to the contract's own definition rather than to what this test expects,
// because the client validates against that.
func TestACallThatArrivesIsPublished(t *testing.T) {
	t.Parallel()

	for name, event := range map[string]any{
		"an offer":                  &waEvents.CallOffer{BasicCallMeta: callMeta("call-1")},
		"a notice":                  &waEvents.CallOfferNotice{BasicCallMeta: callMeta("call-1"), Media: "audio"},
		"an offer naming the media": &waEvents.CallOffer{BasicCallMeta: callMeta("call-1"), Data: offerNode("audio")},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session, _ := callSession(t, false)

			if !session.handle(event) {
				t.Fatal("a call offer must be acknowledged: WhatsApp does not send it again")
			}
			payload := published(t, session, protocol.EventCallOffer, "event_call_offer")

			if got := payload["call_id"]; got != "call-1" {
				t.Errorf("call_id is %v, want call-1", got)
			}
			from, ok := payload["from"].(map[string]any)
			if !ok {
				t.Fatalf("the offer names nobody: %v", payload)
			}
			if got := from["phone"]; got != theCaller {
				t.Errorf("the caller is %v, want %s", got, theCaller)
			}
			if got := payload["video"]; got != false {
				t.Errorf("video is %v, want false", got)
			}
			if got := payload["timestamp"]; got != float64(1755440000123) {
				t.Errorf("timestamp is %v, want the one WhatsApp dated the call", got)
			}
		})
	}
}

// Only `offer_notice` says what kind of call it is, so it is the one that can answer the
// question. A client that got `video: false` for a video call shows the wrong icon and,
// worse, the wrong thing in a conversation the agent reads back later.
func TestAVideoCallSaysSo(t *testing.T) {
	t.Parallel()
	session, _ := callSession(t, false)

	session.handle(&waEvents.CallOfferNotice{BasicCallMeta: callMeta("call-1"), Media: "video"})

	payload := published(t, session, protocol.EventCallOffer, "event_call_offer")
	if got := payload["video"]; got != true {
		t.Errorf("video is %v, want true", got)
	}
}

// A 1:1 video call has no notice to name it: whatsmeow hands the offer node through as
// `Data` and the media is a child of it. Read from the event's fields alone, every direct
// video call reaches the client as a voice call -- and the notice that would have said so
// arrives for a call this session has already published, so nothing corrects it.
func TestADirectVideoCallSaysSoFromItsOwnNode(t *testing.T) {
	t.Parallel()
	session, _ := callSession(t, false)

	session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta("call-1"), Data: offerNode("video")})

	if got := published(t, session, protocol.EventCallOffer, "event_call_offer")["video"]; got != true {
		t.Errorf("video is %v, want true", got)
	}
}

// A notice says it is a group call in its `type`, and the `group-jid` beside it is
// optional. Read from the id alone, a group call announced without one is published to an
// inbox that asked for direct chats only, as a direct call from whoever started it.
func TestAGroupCallIsRecognisedFromTheNoticeType(t *testing.T) {
	t.Parallel()
	session := silentSession(t, false)
	session.setCallPolicy(false)

	// No GroupJID on purpose: the attribute is the only thing saying this is a group.
	if !session.handle(&waEvents.CallOfferNotice{BasicCallMeta: callMeta("call-1"), Media: "audio", Type: "group"}) {
		t.Fatal("a group call must be acknowledged even when it is not published")
	}
	nothingPublished(t, session)
}

// WhatsApp announces one call twice, as `offer` and as `offer_notice`, and both reach the
// handler. Two events for one ring is two conversations' worth of noise in an inbox, and
// a client with no way to tell they are the same call.
func TestOneCallAnnouncedTwiceIsPublishedOnce(t *testing.T) {
	t.Parallel()

	t.Run("the notice second", func(t *testing.T) {
		t.Parallel()
		session, _ := callSession(t, false)

		session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta("call-1")})
		session.handle(&waEvents.CallOfferNotice{BasicCallMeta: callMeta("call-1"), Media: "video"})

		published(t, session, protocol.EventCallOffer, "event_call_offer")
		if queued := len(session.inbox); queued != 0 {
			t.Fatalf("queued %d more emissions for the same call", queued)
		}
	})

	t.Run("a second call still arrives", func(t *testing.T) {
		t.Parallel()
		session, _ := callSession(t, false)

		session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta("call-1")})
		session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta("call-2")})

		first := published(t, session, protocol.EventCallOffer, "event_call_offer")
		second := published(t, session, protocol.EventCallOffer, "event_call_offer")
		if first["call_id"] == second["call_id"] {
			t.Fatalf("two calls were published as one: %v", first["call_id"])
		}
	})
}

// The policy the connect carried, and the whole point of it: the account does not ring.
// The offer is published either way, because "somebody called and we declined" is what
// an inbox has to show.
func TestAutoRejectRefusesTheCallAndStillPublishesIt(t *testing.T) {
	t.Parallel()
	session, watched := callSession(t, true)

	session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta("call-1")})

	published(t, session, protocol.EventCallOffer, "event_call_offer")
	refusedWithin(t, watched, "call-1")
}

// The other half of the switch. A session whose client did not ask for calls to be
// refused must not refuse them: the operator's phone is supposed to ring.
func TestACallIsNotRefusedWhenNobodyAskedForThat(t *testing.T) {
	t.Parallel()
	session, watched := callSession(t, false)

	session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta("call-1")})
	published(t, session, protocol.EventCallOffer, "event_call_offer")

	// The rejection is written off the dispatch goroutine, so the only honest way to
	// assert it did not happen is to give it the room it would have needed.
	time.Sleep(50 * time.Millisecond)
	if seen := watched.seen(); len(seen) != 0 {
		t.Fatalf("refused %v on a session whose client never asked for that", seen)
	}
}

// A rejection that WhatsApp would not take leaves the call ringing, and there is nobody
// to answer: the client asked for a policy, not for a command. What it must not do is
// take the session down with it.
func TestARefusalThatFailsDoesNotStopTheSession(t *testing.T) {
	t.Parallel()
	session, watched := callSession(t, true)
	watched.answer = errors.New("whatsmeow: the socket went")

	session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta("call-1")})

	published(t, session, protocol.EventCallOffer, "event_call_offer")
	refusedWithin(t, watched, "call-1")

	// Still serving: the next call is published like any other.
	session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta("call-2")})
	if got := published(t, session, protocol.EventCallOffer, "event_call_offer")["call_id"]; got != "call-2" {
		t.Fatalf("the session published %v after a failed refusal, want call-2", got)
	}
}

// The end of a call, whoever ended it. `reason` is nullable in the contract precisely so
// a client can tell "ended, cause unknown" from a frame that forgot to carry it.
func TestACallThatEndsIsPublished(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		reason string
		want   any
	}{
		"with the reason WhatsApp gave": {reason: "rejected", want: "rejected"},
		"with none":                     {reason: "", want: nil},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session, _ := callSession(t, false)

			if !session.handle(&waEvents.CallTerminate{BasicCallMeta: callMeta("call-1"), Reason: test.reason}) {
				t.Fatal("the end of a call must be acknowledged")
			}
			payload := published(t, session, protocol.EventCallTerminate, "event_call_terminate")

			if got := payload["call_id"]; got != "call-1" {
				t.Errorf("call_id is %v, want call-1", got)
			}
			if got := payload["reason"]; got != test.want {
				t.Errorf("reason is %v, want %v", got, test.want)
			}
		})
	}
}

// A terminate is published whether or not this instance saw the offer. An instance that
// took the account over mid-ring never saw it, and its client still has a ringing
// conversation to close.
func TestACallEndsEvenWhenThisInstanceNeverSawItStart(t *testing.T) {
	t.Parallel()
	session, _ := callSession(t, false)

	session.handle(&waEvents.CallTerminate{BasicCallMeta: callMeta("call-nobody-saw"), Reason: "timeout"})

	if got := published(t, session, protocol.EventCallTerminate, "event_call_terminate")["call_id"]; got != "call-nobody-saw" {
		t.Fatalf("call_id is %v", got)
	}
}

// Group calls are group traffic, like the messages and the receipts beside them. An inbox
// that asked for direct chats only has no group conversation to show one in, and the
// contract's `call.offer` carries no group, so publishing it would put a call from a
// group the client does not have into the direct chat with whoever started it.
func TestAGroupCallIsNotPublishedToASessionThatDidNotAskForGroups(t *testing.T) {
	t.Parallel()

	meta := callMeta("call-1")
	meta.GroupJID = groupJID()

	for name, event := range map[string]any{
		"an offer":    &waEvents.CallOfferNotice{BasicCallMeta: meta, Media: "audio", Type: "group"},
		"a terminate": &waEvents.CallTerminate{BasicCallMeta: meta, Reason: "timeout"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session := silentSession(t, false)
			session.setCallPolicy(false)

			if !session.handle(event) {
				t.Fatal("a group call must be acknowledged even when it is not published")
			}
			nothingPublished(t, session)
		})
	}
}

// `call.reject` is the client refusing one call, as opposed to the standing policy.
func TestRejectCallRefusesTheCallTheClientNamed(t *testing.T) {
	t.Parallel()
	session, watched := callSession(t, false)

	payload, err := json.Marshal(map[string]any{
		"call_id": "call-1",
		"from":    map[string]string{"kind": "phone", "id": theCaller},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := session.rejectCall(t.Context(), &protocol.Command{
		Type: protocol.CommandCallReject, Payload: payload,
	}); err != nil {
		t.Fatalf("rejectCall: %v", err)
	}
	if seen := watched.seen(); len(seen) != 1 || seen[0] != "call-1" {
		t.Fatalf("refused %v, want [call-1]", seen)
	}
}

// What the command refuses, and why each one is refused rather than guessed at. A
// rejection WhatsApp answers by doing nothing is worse than one the client is told about:
// the call keeps ringing and the agent is told it was declined.
func TestRejectCallRefusesWhatItCannotCarryOut(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		payload any
		want    protocol.ErrorCode
	}{
		"with no call named": {
			payload: map[string]any{"from": map[string]string{"kind": "phone", "id": theCaller}},
			want:    protocol.ErrorInvalidPayload,
		},
		"with nobody to refuse": {
			payload: map[string]any{"call_id": "call-1"},
			want:    protocol.ErrorInvalidPayload,
		},
		"addressed to the group instead of the caller": {
			payload: map[string]any{
				"call_id": "call-1",
				"from":    map[string]string{"kind": "group", "id": theGroup},
			},
			want: protocol.ErrorInvalidPayload,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session, watched := callSession(t, false)

			payload, err := json.Marshal(test.payload)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			_, err = session.rejectCall(t.Context(), &protocol.Command{
				Type: protocol.CommandCallReject, Payload: payload,
			})
			if code := codeOf(err); code != test.want {
				t.Fatalf("answered %q, want %q (err: %v)", code, test.want, err)
			}
			if seen := watched.seen(); len(seen) != 0 {
				t.Fatalf("wrote a rejection for a command it refused: %v", seen)
			}
		})
	}
}

// The bound on what a session remembers. Without it a session that runs for weeks keeps a
// call id for every call the account has ever been offered, and the set is the session's
// for as long as it lives.
func TestTheCallsASessionRemembersAreBounded(t *testing.T) {
	t.Parallel()

	rang := func(i int) waTypes.JID {
		return waTypes.JID{User: theCaller, Server: waTypes.DefaultUserServer, Device: uint16(i % 60)}
	}

	var remembered ring
	for i := range answeredCalls * 3 {
		if !remembered.add("call-"+strconv.Itoa(i), rang(i)) {
			t.Fatalf("call-%d was reported as one already seen", i)
		}
	}
	if got := len(remembered.seen); got != answeredCalls {
		t.Fatalf("remembers %d calls, want %d", got, answeredCalls)
	}

	// The most recent are the ones kept, which is what the deduplication needs: the
	// second half of a call announced a moment ago.
	last := answeredCalls*3 - 1
	if remembered.add("call-"+strconv.Itoa(last), rang(last)) {
		t.Fatal("the call it was just told about was forgotten")
	}
	// And the oldest is gone, which is the point of the bound.
	if !remembered.add("call-0", rang(0)) {
		t.Fatal("the oldest call is still remembered, so the set is not bounded")
	}

	// What is remembered is who rang, not just that somebody did: it is the device the
	// refusal addresses its join to, and a client naming the call has no device to give.
	who, known := remembered.of("call-" + strconv.Itoa(last))
	if !known {
		t.Fatal("the most recent call is not in the set")
	}
	if who != rang(last) {
		t.Fatalf("remembers %v as the device that rang, want %v", who, rang(last))
	}
	if _, stale := remembered.of("call-1"); stale {
		t.Fatal("an evicted call still answers with a device")
	}
}

// The policy decides whether the account rings; the subscription decides what the client
// is told about. Refusing a group call only when the client also wanted group
// conversation is an account ringing on the operator's phone in exactly the case they
// asked it not to. The offer is still not published, for the reason above.
func TestAGroupCallIsRefusedEvenWhenGroupsAreOff(t *testing.T) {
	t.Parallel()
	session, watched := callSession(t, true)
	session.setGroups(false)

	meta := callMeta("call-1")
	meta.GroupJID = groupJID()
	if !session.handle(&waEvents.CallOfferNotice{BasicCallMeta: meta, Media: "audio", Type: "group"}) {
		t.Fatal("a group call must be acknowledged")
	}

	refusedWithin(t, watched, "call-1")
	if queued := len(session.inbox); queued != 0 {
		t.Fatalf("queued %d emissions for a group call on a session that wanted direct chats only", queued)
	}
}

// A rejection has seconds to be written and `emit` has no deadline: it waits on the
// session's inbox for as long as the publisher is stalled, and `callWait` does not bound
// that wait. Queued behind a stalled publisher, the refusal arrives after the caller has
// given up and the account rang the whole time.
func TestACallIsRefusedEvenWhileThePublisherIsStalled(t *testing.T) {
	t.Parallel()
	session, watched := callSession(t, true)
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)

	// Full, which is the state this is about: every further emit parks until the
	// publisher moves, and in the case being reproduced it never does.
	for len(session.inbox) < cap(session.inbox) {
		session.inbox <- pending{event: engine.Emission{Type: protocol.EventSessionState, Payload: []byte(`{}`)}}
	}

	// On its own goroutine, because the handler is the thing that is expected to park.
	go session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta("call-1")})

	refusedWithin(t, watched, "call-1")
}

// A call with no id is still somebody ringing. It cannot be deduplicated, and publishing
// it twice is better than swallowing it: the alternative silences every call WhatsApp
// announces without one.
func TestACallWithNoIDIsStillPublished(t *testing.T) {
	t.Parallel()
	session, _ := callSession(t, false)

	session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta("")})
	session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta("")})

	for i := range 2 {
		if got := published(t, session, protocol.EventCallOffer, "event_call_offer")["call_id"]; got != "" {
			t.Fatalf("offer %d carries call_id %v", i, got)
		}
	}
}

// `pairing.request_code` is a connect this build synthesises, and what it leaves out is
// not defaulted, it is absent. A client asking for a pairing code is not asking for its
// call policy to be dropped -- and the connect records what it carries, so the drop would
// outlive the pairing and be put back by the next resume.
func TestAskingForAPairingCodeKeepsTheCallPolicy(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, "5511999990001")
	// Connected before the connect, which is what keeps this off the network: a resume
	// on a session whatsmeow is already holding open returns without dialling, and what
	// is being asserted here is decided well before the dial either way.
	session.setConnected(true)
	session.setCallPolicy(true)
	session.setGroups(true)

	payload, err := json.Marshal(map[string]string{"phone": "5511999990001"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// The pairing that follows has nothing to dial in a unit test; what is asserted is
	// what the session was left carrying, which is decided before any of that.
	_ = session.requestCode(t.Context(), &protocol.Command{
		Type: protocol.CommandPairingRequestCode, Payload: payload,
	})

	if !session.rejectsCalls() {
		t.Fatal("asking for a pairing code turned the call policy off")
	}
	if !session.wantsGroups() {
		t.Fatal("asking for a pairing code turned the group subscription off")
	}
}

// A connect asking for the policy used to be refused outright, which is what kept the
// Chatwoot side from declaring the capability at all. And what it carries has to outlive
// the command: the sweep brings an account back with its own synthesised connect, so a
// policy that is not on the desired row is a session that comes back letting calls ring
// after the operator asked for the opposite, with nothing saying it changed its mind.
func TestAConnectMayAskForCallsToBeRefusedAndIsRememberedThatWay(t *testing.T) {
	t.Parallel()
	session, container := newTestSession(t, "5511999990001")
	// Connected first, so the resume this asks for returns without dialling. A unit test
	// here must never reach a real socket, and an account paired with fabricated
	// credentials would try.
	session.setConnected(true)

	err := session.Connect(t.Context(), engine.ConnectRequest{
		Pairing: "resume", Groups: true, Calls: &engine.CallsRequest{AutoReject: true},
	})
	if code := codeOf(err); code == protocol.ErrorUnsupported {
		t.Fatalf("a connect asking for auto-reject was refused: %v", err)
	}
	if !session.rejectsCalls() {
		t.Fatal("the session did not take the call policy the connect carried")
	}

	wanted, err := container.Wanted(t.Context())
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(wanted) != 1 {
		t.Fatalf("the sweep has %d sessions to bring back, want 1", len(wanted))
	}
	if !wanted[0].CallAutoReject {
		t.Fatal("the call policy is not on the desired row, so a resume would not put it back")
	}
}

// TestARefusalJoinsTheCallBeforeItRefusesIt holds the shape of both nodes, which is the
// whole of what separates a refusal WhatsApp honours from the one this connector used to
// write. A bare `<reject>` was measured five times against a paired account, across five
// addressings, and never ended a call; with the `<preaccept>` in front of it the call
// ends in under a second. Nothing but a test keeps the pair together, because the socket
// takes either one without complaining.
func TestARefusalJoinsTheCallBeforeItRefusesIt(t *testing.T) {
	t.Parallel()

	own := waTypes.JID{User: "5511999998888", Server: waTypes.DefaultUserServer, Device: 42}
	caller := waTypes.JID{User: theCaller, Server: waTypes.DefaultUserServer, Device: 58}
	join, refusal := refusalNodes(own, caller, "CALL-1")

	for _, outer := range []waBinary.Node{join, refusal} {
		if outer.Tag != "call" {
			t.Fatalf("both nodes are <call>, this one is <%s>", outer.Tag)
		}
		if got := outer.Attrs["from"]; got != own.ToNonAD() {
			t.Fatalf("the refusal is signed by this account without its device, got %v", got)
		}
		if _, carried := outer.Attrs["id"]; carried {
			t.Fatal("the stanza id is the writer's to fill, so that two nodes of one call differ")
		}
		if len(outer.GetChildren()) != 1 {
			t.Fatalf("one child per node, got %d", len(outer.GetChildren()))
		}
	}

	// Both halves carry the caller exactly as the offer named it. Neither flattened nor
	// elaborated: no refusal measured to work involved a caller that carried a device at
	// all, so an address WhatsApp did not name would be invention.
	for _, outer := range []waBinary.Node{join, refusal} {
		if got := outer.Attrs["to"]; got != caller {
			t.Fatalf("addressed to the caller as the offer named them, got %v", got)
		}
	}

	if tag := join.GetChildren()[0].Tag; tag != "preaccept" {
		t.Fatalf("the first node joins the call, got <%s>", tag)
	}
	if tag := refusal.GetChildren()[0].Tag; tag != "reject" {
		t.Fatalf("the second node refuses it, got <%s>", tag)
	}

	for _, child := range []waBinary.Node{join.GetChildren()[0], refusal.GetChildren()[0]} {
		if got := child.Attrs["call-id"]; got != "CALL-1" {
			t.Fatalf("<%s> names the call, got %v", child.Tag, got)
		}
		if got := child.Attrs["call-creator"]; got != caller {
			t.Fatalf("<%s> names whoever started it as the offer did, got %v", child.Tag, got)
		}
	}
	if got := refusal.GetChildren()[0].Attrs["count"]; got != "0" {
		t.Fatalf(`<reject> carries count="0", got %v`, got)
	}
}

// TestTheTwoHalvesOfARefusalAreTheSameCall guards the pairing itself: a preaccept for one
// call and a reject for another would leave the account inside a call it never left, which
// is the one state this change creates that did not exist before.
func TestTheTwoHalvesOfARefusalAreTheSameCall(t *testing.T) {
	t.Parallel()

	own := waTypes.JID{User: "5511999998888", Server: waTypes.DefaultUserServer}
	caller := waTypes.JID{User: theCaller, Server: waTypes.HiddenUserServer, Device: 58}
	join, refusal := refusalNodes(own, caller, "CALL-2")

	if join.GetChildren()[0].Attrs["call-id"] != refusal.GetChildren()[0].Attrs["call-id"] {
		t.Fatal("the call joined and the call refused must be the same one")
	}
	if join.Attrs["to"] != refusal.Attrs["to"] {
		t.Fatal("both halves go to the same caller")
	}
}

// TestNothingCallsTheLibrarysOwnRejection is a fence, and it is the one assertion that
// fails on the code this change replaces. whatsmeow's RejectCall writes the `<reject>`
// alone, which WhatsApp acks and does not act on, and which leaves the account's phone
// holding a call notification it cannot dismiss. Reaching for it again from anywhere in
// this package would put that back without any other test noticing, because every unit
// test swaps the seam and never sees the node.
func TestNothingCallsTheLibrarysOwnRejection(t *testing.T) {
	t.Parallel()

	// The needle is a real method, and the compiler is what says so. A fence searching
	// for a string that can no longer appear passes because there is nothing left to
	// find, and whatsmeow renaming or removing RejectCall is the way that happens here
	// -- silently, in a dependency bump, with this test still green. The reference costs
	// a line and turns that into a build failure on the bump that caused it. The name is
	// all it pins: what this fence looks for is the call, whatever shape it has.
	var _ = (&wm.Client{}).RejectCall

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package: %v", err)
	}
	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		checked++
		if strings.Contains(string(source), ".RejectCall(") {
			t.Errorf("%s calls whatsmeow's RejectCall, which is refused by nobody and honoured by nobody: "+
				"a refusal has to join the call first, which is what declineOverClient does", name)
		}
	}
	if checked == 0 {
		t.Fatal("the fence read no production files, so it proves nothing")
	}
}

// writtenNodes is a refusalSocket that writes nowhere and remembers everything, so the
// sequence a refusal is -- and not just the shape of its two nodes -- is reachable from a
// test.
type writtenNodes struct {
	own    waTypes.JID
	nodes  []waBinary.Node
	ids    int
	failOn int // 1 refuses the join, 2 refuses the refusal, 0 accepts both
	err    error
}

func (w *writtenNodes) ownID() waTypes.JID { return w.own }

func (w *writtenNodes) stanzaID() string {
	w.ids++
	return fmt.Sprintf("STANZA-%d", w.ids)
}

func (w *writtenNodes) send(_ context.Context, node waBinary.Node) error {
	w.nodes = append(w.nodes, node)
	if len(w.nodes) == w.failOn {
		return w.err
	}
	return nil
}

// tags is the tag of each node's one child, in the order they were written.
func (w *writtenNodes) tags() []string {
	out := make([]string, 0, len(w.nodes))
	for _, node := range w.nodes {
		out = append(out, node.GetChildren()[0].Tag)
	}
	return out
}

func loggedInSocket() *writtenNodes {
	return &writtenNodes{own: waTypes.JID{User: "5511999998888", Server: waTypes.DefaultUserServer, Device: 12}}
}

// TestARefusalWritesTheJoinAndThenTheRefusal holds the order, which is the whole of the
// fix: the `<reject>` on its own is what this connector already wrote, and WhatsApp acked
// it and did nothing.
func TestARefusalWritesTheJoinAndThenTheRefusal(t *testing.T) {
	t.Parallel()

	socket := loggedInSocket()
	if err := decline(t.Context(), socket, someone(theCaller), "CALL-ORDER"); err != nil {
		t.Fatalf("a refusal both writes accept: %v", err)
	}
	if got := socket.tags(); !slices.Equal(got, []string{"preaccept", "reject"}) {
		t.Fatalf("a refusal joins the call and then refuses it, wrote %v", got)
	}
}

// TestTheTwoNodesOfARefusalCarryDifferentStanzaIDs is the one a mutation battery found
// alive: both nodes went out under one id and every assertion about their shape still
// passed. Two nodes of a call sharing an id is a single answer addressed to both.
func TestTheTwoNodesOfARefusalCarryDifferentStanzaIDs(t *testing.T) {
	t.Parallel()

	socket := loggedInSocket()
	if err := decline(t.Context(), socket, someone(theCaller), "CALL-IDS"); err != nil {
		t.Fatalf("a refusal both writes accept: %v", err)
	}
	if len(socket.nodes) != 2 {
		t.Fatalf("a refusal is two nodes, wrote %d", len(socket.nodes))
	}
	first, second := socket.nodes[0].Attrs["id"], socket.nodes[1].Attrs["id"]
	if first == "" || second == "" {
		t.Fatalf("both nodes carry a stanza id, got %q and %q", first, second)
	}
	if first == second {
		t.Fatalf("the two nodes of one refusal need different stanza ids, both were %q", first)
	}
}

// TestARefusalThatCannotJoinWritesNoRefusal is the other survivor: swallowing the join's
// error left the `<reject>` going out alone, which is exactly the node this change exists
// to stop writing, and no test noticed.
func TestARefusalThatCannotJoinWritesNoRefusal(t *testing.T) {
	t.Parallel()

	socket := loggedInSocket()
	socket.failOn, socket.err = 1, errors.New("socket gone")

	err := decline(t.Context(), socket, someone(theCaller), "CALL-HALF")
	if err == nil {
		t.Fatal("a refusal whose join did not go out has to say so")
	}
	if !strings.Contains(err.Error(), "CALL-HALF") {
		t.Fatalf("the failure names the call it was refusing, said %q", err)
	}
	if got := socket.tags(); !slices.Equal(got, []string{"preaccept"}) {
		t.Fatalf("a refusal that could not join writes nothing after it, wrote %v", got)
	}
}

// TestARefusalFromAnAccountThatIsNotLoggedInWritesNothing keeps the guard in front of both
// writes rather than letting an empty `from` reach the socket.
func TestARefusalFromAnAccountThatIsNotLoggedInWritesNothing(t *testing.T) {
	t.Parallel()

	socket := &writtenNodes{}

	err := decline(t.Context(), socket, someone(theCaller), "CALL-OUT")
	if !errors.Is(err, wm.ErrNotLoggedIn) {
		t.Fatalf("an account that is not logged in cannot refuse, said %v", err)
	}
	if len(socket.nodes) != 0 {
		t.Fatalf("nothing is written when there is no account to write it from, wrote %d", len(socket.nodes))
	}
}

// TestACommandRefusesTheDeviceThatRang covers what the contract cannot carry: `call.reject`
// names a person, WhatsApp answers to a device, and the session is the only thing that
// saw which one rang. Without this the session could go back to refusing whatever address
// the client handed it and nothing would fail.
func TestACommandRefusesTheDeviceThatRang(t *testing.T) {
	t.Parallel()
	session, watched := callSession(t, false)

	rang := waTypes.JID{User: theCaller, Server: waTypes.DefaultUserServer, Device: 58}
	meta := callMeta("call-device")
	meta.CallCreator = rang
	session.callOffered(&meta, callMedia{}, false)

	payload, err := json.Marshal(map[string]any{
		"call_id": "call-device",
		"from":    map[string]string{"kind": "phone", "id": theCaller},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := session.rejectCall(t.Context(), &protocol.Command{
		Type: protocol.CommandCallReject, Payload: payload,
	}); err != nil {
		t.Fatalf("rejectCall: %v", err)
	}
	named := watched.addressed()
	if len(named) != 1 {
		t.Fatalf("one refusal, got %d", len(named))
	}
	if named[0] != rang {
		t.Fatalf("the refusal names the device that rang (%s), named %s", rang, named[0])
	}
}

// TestACommandForACallThisSessionNeverSawNamesWhatTheClientGave is the other side, and it
// is a behaviour and not a fallback: the node still goes out, addressed to the account,
// because a refusal WhatsApp takes and ignores is better than no node at all for a client
// that is waiting on one.
func TestACommandForACallThisSessionNeverSawNamesWhatTheClientGave(t *testing.T) {
	t.Parallel()
	session, watched := callSession(t, false)

	payload, err := json.Marshal(map[string]any{
		"call_id": "call-unseen",
		"from":    map[string]string{"kind": "phone", "id": theCaller},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := session.rejectCall(t.Context(), &protocol.Command{
		Type: protocol.CommandCallReject, Payload: payload,
	}); err != nil {
		t.Fatalf("rejectCall: %v", err)
	}
	named := watched.addressed()
	if len(named) != 1 {
		t.Fatalf("one refusal, got %d", len(named))
	}
	if named[0].Device != 0 || named[0].User != theCaller {
		t.Fatalf("a call this session never saw is refused at the address the client gave, named %s", named[0])
	}
}

// TestOneCallAnnouncedTwiceIsRefusedOnce fences the other half of the deduplication, which
// the publishing test cannot reach: it runs with the policy off, so the gate in front of
// the refusal is invisible to it.
//
// WhatsApp announces one call as `offer` and `offer_notice`, in either order. Refusing on
// both writes two `<preaccept>` and two `<reject>` into a call with one of each, which is
// what the ring exists to prevent and what nothing else here would notice.
//
// Under synctest, because the refusal is written from a goroutine `refuse` starts and the
// question is whether a second one exists. `synctest.Wait` returns once every goroutine in
// the bubble is durably blocked, so "no second refusal" becomes something the test knows.
// Waiting a while and looking is the weaker version of this and is what AGENTS.md rules
// out: it passes whenever the duplicate is merely slow.
func TestOneCallAnnouncedTwiceIsRefusedOnce(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		session, watched := callSession(t, true)

		session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta("call-twice")})
		session.handle(&waEvents.CallOfferNotice{BasicCallMeta: callMeta("call-twice"), Media: "video"})

		synctest.Wait()

		if seen := watched.seen(); len(seen) != 1 || seen[0] != "call-twice" {
			t.Fatalf("one call announced twice is refused once, refused %v", seen)
		}
	})
}
