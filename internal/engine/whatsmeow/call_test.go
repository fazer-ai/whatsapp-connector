package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/rs/zerolog"
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
type refusals struct {
	mu     sync.Mutex
	calls  []string
	answer error
}

func (r *refusals) record(callID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, callID)
	return r.answer
}

func (r *refusals) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// callSession is a connected session with the call seam watched.
func callSession(t *testing.T, autoReject bool) (*Session, *refusals) {
	t.Helper()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.setGroups(true)
	session.setCallPolicy(autoReject)

	watched := &refusals{}
	session.declineCall = func(_ context.Context, _ *wm.Client, _ waTypes.JID, callID string) error {
		return watched.record(callID)
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
// refusalOnlySession is a Session built by hand, holding only what the refusal path reads
// and deliberately without a store.
//
// `newTestSession` opens one, and under `WAC_TEST_DATABASE_URL` that pulls `database/sql`
// into whatever goroutine touches it. A connection pool is not safe inside a synctest
// bubble in either direction: the process-wide pool it builds on first use outlives the
// callback, and a per-test pool created inside has its connections handed back from
// outside, which aborts the whole test binary. Announced as a group call to a session that
// did not ask for groups, `callOffered` decides the refusal and returns before it would
// look anybody up, so none of that is needed here.
func refusalOnlySession(t *testing.T) (*Session, *refusals) {
	t.Helper()

	watched := &refusals{}
	session := &Session{
		sid:      "sid-" + t.Name(),
		ctx:      t.Context(),
		log:      zerolog.Nop(),
		callWait: callWriteTimeout,
	}
	session.setCallPolicy(true)
	session.declineCall = func(_ context.Context, _ *wm.Client, _ waTypes.JID, callID string) error {
		return watched.record(callID)
	}
	return session, watched
}

// TestOneCallAnnouncedTwiceIsRefusedOnce fences the other half of the deduplication, which
// the publishing test below cannot reach: it runs with the policy off, so the gate in front
// of the refusal is invisible to it, and every test that does turn the policy on sends a
// single announcement.
//
// WhatsApp announces one call as `offer` and `offer_notice`, in either order, and both
// reach the handler. Refusing on both writes two refusals into a call that has one, which
// is what the ring exists to prevent and what nothing else here would notice.
//
// Under synctest, because the refusal is written from a goroutine `refuse` starts and the
// question asked is whether a second one exists. `synctest.Wait` returns once every
// goroutine in the bubble is durably blocked, so "there is no second refusal" is something
// the test knows. Waiting a while and looking is the weaker version of this and is what
// AGENTS.md rules out: it passes whenever the duplicate is merely slow.
func TestOneCallAnnouncedTwiceIsRefusedOnce(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		session, watched := refusalOnlySession(t)

		meta := callMeta("call-twice")
		session.callOffered(&meta, callMedia{}, true)
		session.callOffered(&meta, callMedia{known: true, video: true}, true)

		synctest.Wait()

		if seen := watched.seen(); len(seen) != 1 || seen[0] != "call-twice" {
			t.Fatalf("one call announced twice is refused once, refused %v", seen)
		}
	})
}

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

	var remembered ring
	for i := range answeredCalls * 3 {
		if !remembered.add("call-" + strconv.Itoa(i)) {
			t.Fatalf("call-%d was reported as one already seen", i)
		}
	}
	if got := len(remembered.seen); got != answeredCalls {
		t.Fatalf("remembers %d calls, want %d", got, answeredCalls)
	}

	// The most recent are the ones kept, which is what the deduplication needs: the
	// second half of a call announced a moment ago.
	if remembered.add("call-" + strconv.Itoa(answeredCalls*3-1)) {
		t.Fatal("the call it was just told about was forgotten")
	}
	// And the oldest is gone, which is the point of the bound.
	if !remembered.add("call-0") {
		t.Fatal("the oldest call is still remembered, so the set is not bounded")
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
// Chatwoot side from declaring the capability at all.
//
// That what it carries outlives the command is the other half, and it is no longer this
// engine's half: since #266 the desired row is written by the session layer, where the
// request arrives, and `TestAConnectIsRememberedWithWhatItAskedFor` in `internal/session`
// is what holds it. Kept apart rather than deleted, because the two can fail separately:
// a session that takes the policy and a row that carries it are different promises.
func TestAConnectMayAskForCallsToBeRefused(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, "5511999990001")
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
}
