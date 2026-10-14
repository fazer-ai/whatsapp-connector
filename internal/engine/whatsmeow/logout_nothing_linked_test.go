package whatsmeow

import (
	"context"
	"sync/atomic"
	"testing"

	wm "go.mau.fi/whatsmeow"
)

// A logout for a session whose store holds no device -- one unlinked from the phone, or one
// never paired -- has nothing to remove: the state it asks for is the state it is already
// in. Answered as a failure, the client records one that did not happen and every retry of
// the teardown meets the same error (#398). And WhatsApp is not asked: there are no
// credentials to sign the request with, and whatsmeow refuses it with ErrNotLoggedIn.
func TestALogoutWithNothingLinkedSucceedsWithoutAskingWhatsApp(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
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
}
