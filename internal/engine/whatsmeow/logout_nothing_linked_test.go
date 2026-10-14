package whatsmeow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// A logout for a session whose store holds no device -- one unlinked from the phone, or one
// never paired -- has nothing to remove: the state it asks for is the state it is already
// in. Answered as a failure, the client records one that did not happen and every retry of
// the teardown meets the same error (#398). And WhatsApp is not asked: there are no
// credentials to sign the request with, and whatsmeow refuses it with ErrNotLoggedIn.
func TestALogoutWithNothingLinkedSucceedsWithoutAskingWhatsApp(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "")
	// What the client asked for before it logged out: a connect for a pairing nobody
	// finished, or the account the phone has since unlinked.
	if err := session.store.PutDesiredConnected(t.Context(), store.Wants{}); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
	}
	var asked atomic.Int32
	session.logout = func(context.Context, *wm.Client) error {
		asked.Add(1)
		return wm.ErrNotLoggedIn
	}

	if err := session.Logout(t.Context()); err != nil {
		t.Fatalf("Logout with nothing linked failed with %v, want success", err)
	}
	if n := asked.Load(); n != 0 {
		t.Fatalf("WhatsApp was asked %d time(s) to unlink a device the store does not hold", n)
	}
	// Nothing was revoked, and the client is the one the next pairing reuses: marked
	// revoked, it would go on answering not_paired after that pairing succeeds, since
	// pairing does not rebuild it and nothing else clears the mark.
	if session.isRevoked() {
		t.Fatal("a session with nothing linked was marked revoked, and the next pairing would inherit the mark")
	}
	// The contract has a logout delete the record of what the client asked for, and an
	// answer of success says the logout happened: left behind, the row outlives a session
	// the client has ended.
	var desired int
	if err := container.DB().QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM wac_session_desired WHERE sid = '`+session.sid+`'`).Scan(&desired); err != nil {
		t.Fatalf("read the desired state back: %v", err)
	}
	if desired != 0 {
		t.Fatal("the desired state outlived a logout answered as done")
	}
}

// A scan the phone was finishing while the logout ran must not land after it: the store
// was read empty, the logout answered done, and a pairing that binds afterwards would
// leave the account linked under a session its client has just ended. Refused here,
// whatsmeow cancels that pairing. The next connect puts the session on a fresh client
// whose pairing binds normally, and the client the logout put down stays refused: its
// pairing runs on whatsmeow's goroutine, which nothing waits for, and a callback arriving
// after the replacement paired would overwrite the replacement's device.
func TestAPairingThatLandsAfterALogoutWithNothingLinkedIsRefused(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "")
	session.logout = func(context.Context, *wm.Client) error { return wm.ErrNotLoggedIn }
	putDown := session.current()
	if err := session.Logout(t.Context()); err != nil {
		t.Fatalf("Logout with nothing linked failed with %v, want success", err)
	}

	jid, err := waTypes.ParseJID("5511999990002:12@" + waTypes.DefaultUserServer)
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	if putDown.PrePairCallback(jid, "", "") {
		t.Fatal("a pairing finishing after the logout was let bind the device")
	}
	if _, bound, err := container.For(session.sid).JID(t.Context()); err != nil || bound {
		t.Fatalf("the refused pairing left a device mapped (bound=%v, err=%v)", bound, err)
	}

	// A resume, which refuses on a session that never paired without dialling: what is
	// under test is the connect replacing the client on its way in, not the pairing.
	_ = session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"})
	fresh := session.current()
	if fresh == putDown {
		t.Fatal("the next connect kept the client the logout put down")
	}
	if putDown.PrePairCallback(jid, "", "") {
		t.Fatal("the pairing of the client the logout put down bound after the next connect")
	}
	if !fresh.PrePairCallback(jid, "", "") {
		t.Fatal("the pairing of the next connect was refused")
	}
}

// The other order: a pairing recording its device while the logout reads the store. The read
// waits for the record, and the logout then has a device to unlink and asks WhatsApp to.
// Read first, it would find the store empty, answer done, and leave the device the pairing
// was writing linked under a session its client has ended.
func TestALogoutWaitsForAPairingThatIsRecordingItsDevice(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	var asked atomic.Int32
	session.logout = func(context.Context, *wm.Client) error {
		asked.Add(1)
		return nil
	}
	jid, err := waTypes.ParseJID("5511999990003:12@" + waTypes.DefaultUserServer)
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}

	// Held the way bind holds it, around the write.
	if !session.holdBinding(t.Context()) {
		t.Fatal("could not take the binding slot")
	}
	answered := make(chan error, 1)
	go func() { answered <- session.Logout(t.Context()) }()
	select {
	case err := <-answered:
		session.releaseBinding()
		t.Fatalf("the logout answered (%v) while a pairing was still recording its device", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := session.store.Bind(t.Context(), jid); err != nil {
		session.releaseBinding()
		t.Fatalf("Bind: %v", err)
	}
	session.releaseBinding()

	select {
	case err := <-answered:
		if err != nil {
			t.Fatalf("the logout of the device the pairing recorded: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the logout never answered after the pairing finished recording")
	}
	if n := asked.Load(); n != 1 {
		t.Fatalf("WhatsApp was asked %d time(s) to unlink the device the pairing recorded, want once", n)
	}
}

// The wait for a pairing to finish recording is the caller's to bound. A store that stalls
// under a bind holds it for the bind's whole limit, and a logout that sat that out past its
// own deadline would hold the session's command queue behind a reply nobody is reading.
func TestALogoutStopsWaitingForABindAtItsDeadline(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	if !session.holdBinding(t.Context()) {
		t.Fatal("could not take the binding slot")
	}
	t.Cleanup(session.releaseBinding)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	answered := make(chan error, 1)
	go func() { answered <- session.Logout(ctx) }()
	select {
	case err := <-answered:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("the logout answered %v, want the caller's deadline", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the logout went on waiting for a bind past its deadline")
	}
}

// A logout puts the session down, paired or not: a pairing it interrupted had published
// connecting, and nothing else will take that down, since a disconnect whatsmeow is told to
// make emits no event of its own.
func TestALogoutWithNothingLinkedSettlesTheStateAtClose(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	if err := session.Logout(t.Context()); err != nil {
		t.Fatalf("Logout with nothing linked failed with %v, want success", err)
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case emission := <-session.Events():
			if emission.Type != protocol.EventSessionState {
				continue
			}
			if state := decode(t, emission.Payload)["state"]; state != "close" {
				t.Fatalf("published session.state %v after the logout, want close", state)
			}
			if !session.hangUpStanding() {
				t.Fatal("the logout left no guard against a Connected the pairing socket had queued")
			}
			return
		case <-deadline:
			t.Fatalf("no session.state after the logout; the session reports %q", session.state())
		}
	}
}

// An empty mapping is not the same as an unpaired client. When the same phone pairs under
// another session, the store moves the account's mapping there and leaves this session
// running on credentials it still holds in memory, linked on the phone. Its logout is a
// real one: WhatsApp is asked to unlink, or the companion stays listed with nothing left
// that could ever remove it once the next connect discards those credentials.
func TestALogoutOfADisplacedSessionStillAsksWhatsAppToUnlink(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "")
	jid, err := waTypes.ParseJID("5511999990004:12@" + waTypes.DefaultUserServer)
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	// The live client's identity, with no mapping behind it: what a displaced session
	// holds.
	session.current().Store.ID = &jid
	if _, bound, err := container.For(session.sid).JID(t.Context()); err != nil || bound {
		t.Fatalf("the bench has a mapping (bound=%v, err=%v); the case needs none", bound, err)
	}
	var asked atomic.Int32
	session.logout = func(context.Context, *wm.Client) error {
		asked.Add(1)
		return nil
	}

	if err := session.Logout(t.Context()); err != nil {
		t.Fatalf("Logout of a displaced session: %v", err)
	}
	if n := asked.Load(); n != 1 {
		t.Fatalf("WhatsApp was asked %d time(s) to unlink a device the live client still holds, want once", n)
	}
}
