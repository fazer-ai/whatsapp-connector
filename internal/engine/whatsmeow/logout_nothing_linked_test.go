package whatsmeow

import (
	"context"
	"sync/atomic"
	"testing"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

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
// whatsmeow cancels that pairing; a later connect is the operator asking again, and
// pairs normally.
func TestAPairingThatLandsAfterALogoutWithNothingLinkedIsRefused(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "")
	session.logout = func(context.Context, *wm.Client) error { return wm.ErrNotLoggedIn }
	if err := session.Logout(t.Context()); err != nil {
		t.Fatalf("Logout with nothing linked failed with %v, want success", err)
	}

	jid, err := waTypes.ParseJID("5511999990002:12@" + waTypes.DefaultUserServer)
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	if session.bind(jid, "", "") {
		t.Fatal("a pairing finishing after the logout was let bind the device")
	}
	if _, bound, err := container.For(session.sid).JID(t.Context()); err != nil || bound {
		t.Fatalf("the refused pairing left a device mapped (bound=%v, err=%v)", bound, err)
	}

	session.allowBind()
	if !session.bind(jid, "", "") {
		t.Fatal("the pairing of the next connect was refused as well")
	}
}
