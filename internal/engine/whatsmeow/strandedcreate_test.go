package whatsmeow

import (
	"context"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"
)

// A stranded intent, end to end (#277): the request never left, so WhatsApp names no group,
// and every delivery is refused while pushing the intent's clock forward. Once the sweep's
// ceiling has passed, counted from the first delivery, the intent is gone, and the next
// delivery of the same key makes the group exactly once; the one after that is answered
// with that group rather than making another.
func TestAStrandedCreationIsMadeOnceTheCeilingHasTakenItsIntent(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	session.setConnected(true)
	impatient(session)
	self := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
	made := aMadeGroup("120363041234567890", "Obras", self)

	const ceiling = 24 * time.Hour
	// Written a day and a bit ago by a delivery that died before sending anything.
	if _, _, err := session.store.BeginGroupCreate(
		t.Context(), "idem:once", "WACSTRANDED", "Obras", time.Now().Add(-ceiling-time.Hour)); err != nil {
		t.Fatalf("begin the attempt that never left: %v", err)
	}

	asked := 0
	session.createTheGroup = func(context.Context, *wm.Client, keyedCreate) (*waTypes.GroupInfo, error) {
		asked++
		return made, nil
	}
	session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
		return made, nil
	}
	payload := `{"subject":"Obras","participants":[]}`

	// The client still asking, which is what used to keep the intent alive for good.
	if _, err := session.Execute(t.Context(), namedCreate("c1", "once", payload)); err == nil {
		t.Fatal("a delivery over an intent nobody sent was answered as if it had made a group")
	}
	if asked != 0 {
		t.Fatal("a group was made while the intent was still on record")
	}

	now := time.Now()
	if _, err := container.SweepGroupCreations(t.Context(), now.Add(-2*ceiling), now.Add(-ceiling)); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	first, err := session.Execute(t.Context(), namedCreate("c2", "once", payload))
	if err != nil {
		t.Fatalf("the delivery after the ceiling: %v", err)
	}
	again, err := session.Execute(t.Context(), namedCreate("c3", "once", payload))
	if err != nil {
		t.Fatalf("the delivery after that: %v", err)
	}
	if asked != 1 {
		t.Fatalf("WhatsApp was asked to make %d groups, want 1", asked)
	}
	if namedGroup(t, first) != made.JID.String() || namedGroup(t, again) != made.JID.String() {
		t.Fatalf("the two deliveries answered %s and %s, want %s both times",
			namedGroup(t, first), namedGroup(t, again), made.JID)
	}
}
