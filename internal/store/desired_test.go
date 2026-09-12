package store_test

import (
	"testing"
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
	if err := container.For("sid-up").PutDesiredConnected(ctx, false); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
	}
	if err := container.For("sid-off").PutDesiredDisconnected(ctx); err != nil {
		t.Fatalf("PutDesiredDisconnected: %v", err)
	}
	// Asked for and never paired, which is a QR somebody walked away from.
	if err := container.For("sid-never").PutDesiredConnected(ctx, false); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
	}

	wanted, err := container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(wanted) != 1 || wanted[0].SID != "sid-up" {
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
	if err := container.For("sid-1").PutDesiredConnected(ctx, false); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
	}
	if err := container.For("sid-1").PutDesiredDisconnected(ctx); err != nil {
		t.Fatalf("PutDesiredDisconnected: %v", err)
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
	if err := container.For("sid-1").PutDesiredConnected(ctx, false); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
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

// The whole of #190 at this layer. A client says "connect, and send me groups" in one
// command, and the sweep that brings the account back after the instance running it goes
// away reads this row and nothing else: what the row does not carry is what the resumed
// session does not have. It came back open and, for groups, deaf.
func TestWhatWasAskedForComesBackWithTheSession(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	pair(t, container, "sid-groups", "5511999990001")
	pair(t, container, "sid-direct", "5511999990002")
	if err := container.For("sid-groups").PutDesiredConnected(ctx, true); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
	}
	if err := container.For("sid-direct").PutDesiredConnected(ctx, false); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
	}

	wanted, err := container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	subscription := map[string]bool{}
	for _, session := range wanted {
		subscription[session.SID] = session.Groups
	}
	if got, ok := subscription["sid-groups"]; !ok || !got {
		t.Fatalf("the session that asked for groups would come back with groups=%v (present=%v)", got, ok)
	}
	if got, ok := subscription["sid-direct"]; !ok || got {
		t.Fatalf("the session that asked for direct chats only would come back with groups=%v (present=%v)", got, ok)
	}
}

// A client turns groups off by connecting again without them, which is the only way it
// can: there is no command that says "keep the connection and stop the groups". Recorded
// once and never overwritten, an account would go on receiving group conversation after
// every restart, on the strength of a request its client has replaced.
func TestTurningGroupsOffIsRemembered(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	pair(t, container, "sid-1", "5511999990001")
	if err := container.For("sid-1").PutDesiredConnected(ctx, true); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
	}
	if err := container.For("sid-1").PutDesiredConnected(ctx, false); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
	}

	wanted, err := container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(wanted) != 1 {
		t.Fatalf("the session is not wanted at all (%v)", wanted)
	}
	if wanted[0].Groups {
		t.Fatal("a client that reconnected without groups would still be resumed with them")
	}
}

// The two doors are fenced, because what they record is what brings an account back. An
// instance that has lost the session must not be the one saying it should be running --
// it would be telling its successor to put back what the successor has already been
// asked to leave down, or to leave down what a client has asked for since.
func TestTheDesiredStateIsNotWrittenByASessionThatWasHandedOn(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	pair(t, container, "sid-1", "5511999990001")

	losing := container.For("sid-1")
	losing.Drop()

	if err := losing.PutDesiredConnected(ctx, true); err == nil {
		t.Fatal("a session that no longer owns this one asked for it to be brought back")
	}
	if err := losing.PutDesiredDisconnected(ctx); err == nil {
		t.Fatal("a session that no longer owns this one asked for it to stay down")
	}
	// And nothing reached the table: a fence that answered the error after writing would
	// pass the two checks above and still hand the successor the wrong instruction.
	wanted, err := container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(wanted) != 0 {
		t.Fatalf("the refused write landed anyway: the sweep would bring back %v", wanted)
	}
}

// A disconnect does not carry a subscription, so it must not write one. The value it
// would write is not read while the session is down -- Wanted selects on the state --
// which is exactly why writing it is the kind of falsehood that survives until somebody
// adds the reader.
func TestADisconnectLeavesTheSubscriptionAlone(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	pair(t, container, "sid-1", "5511999990001")
	if err := container.For("sid-1").PutDesiredConnected(ctx, true); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
	}
	if err := container.For("sid-1").PutDesiredDisconnected(ctx); err != nil {
		t.Fatalf("PutDesiredDisconnected: %v", err)
	}

	var groups int64
	if err := container.DB().QueryRowContext(ctx,
		`SELECT wants_groups FROM wac_session_desired WHERE sid = 'sid-1'`).Scan(&groups); err != nil {
		t.Fatalf("read the row back: %v", err)
	}
	if groups == 0 {
		t.Fatal("the disconnect answered a question it was not asked, and cleared the subscription")
	}
}
