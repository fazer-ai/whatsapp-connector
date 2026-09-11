package store_test

import (
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// What the sweep that brings sessions back reads, and the three answers it has to get
// right. A session a client asked to connect is in it; one the operator turned off is
// not, or the connector would undo a disconnect on its own; and one that never finished
// pairing is not, because there is nothing to resume -- brought back, it would publish a
// QR into an empty room on every pass.
func TestWantedIsThePairedSessionsSomebodyAskedToConnect(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	pair(t, container, "sid-up", "5511999990001")
	pair(t, container, "sid-off", "5511999990002")
	if err := container.For("sid-up").PutDesired(ctx, store.DesiredConnected); err != nil {
		t.Fatalf("PutDesired: %v", err)
	}
	if err := container.For("sid-off").PutDesired(ctx, store.DesiredDisconnected); err != nil {
		t.Fatalf("PutDesired: %v", err)
	}
	// Asked for and never paired, which is a QR somebody walked away from.
	if err := container.For("sid-never").PutDesired(ctx, store.DesiredConnected); err != nil {
		t.Fatalf("PutDesired: %v", err)
	}

	wanted, err := container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(wanted) != 1 || wanted[0] != "sid-up" {
		t.Fatalf("the sweep would bring back %v, want only sid-up", wanted)
	}
}

// A disconnect is the operator saying the account stays down, and it has to outlive the
// instance that heard it: written only in memory, the next instance to come up would read
// a session that should be connected and dial the socket the operator just closed.
func TestADisconnectIsRememberedOverAConnect(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	pair(t, container, "sid-1", "5511999990001")
	if err := container.For("sid-1").PutDesired(ctx, store.DesiredConnected); err != nil {
		t.Fatalf("PutDesired: %v", err)
	}
	if err := container.For("sid-1").PutDesired(ctx, store.DesiredDisconnected); err != nil {
		t.Fatalf("PutDesired: %v", err)
	}

	wanted, err := container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(wanted) != 0 {
		t.Fatalf("a session the operator turned off is still wanted (%v); a sweep would dial it again", wanted)
	}
}

// The row has no foreign key to cascade from -- it is written before a session has a
// device -- so the forget has to delete it by hand. Left behind, the sweep would go on
// bringing back an account whose credentials the logout or the teardown just deleted.
func TestForgettingASessionForgetsThatItShouldBeConnected(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	pair(t, container, "sid-1", "5511999990001")
	if err := container.For("sid-1").PutDesired(ctx, store.DesiredConnected); err != nil {
		t.Fatalf("PutDesired: %v", err)
	}
	if err := container.For("sid-1").Forget(ctx); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	// Paired again under the same id, which is what a fresh inbox reusing a session id
	// would be: what it should do is wait to be asked, not resume on the old record.
	pair(t, container, "sid-1", "5511999990003")
	wanted, err := container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(wanted) != 0 {
		t.Fatalf("a session that was torn down is still wanted (%v)", wanted)
	}
}
