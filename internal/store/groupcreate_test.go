package store_test

import (
	"testing"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/store/storetest"
)

// The intent is what a later delivery goes on, so a later delivery must not rewrite it.
// The key above all: it is what the creation was sent under and what WhatsApp echoes back,
// so a redelivery that overwrote it would be waiting under a name nothing will ever answer.
func TestALaterDeliveryDoesNotMoveTheIntentItFound(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	scoped := container.For("sid-1")

	began := time.Now().Add(-time.Hour)
	if _, found, err := scoped.BeginGroupCreate(ctx, "idem:k", "WACFIRST", "Obras", began); err != nil {
		t.Fatalf("the first delivery: %v", err)
	} else if found {
		t.Fatal("the first delivery was reported as one that had been here before")
	}

	again, found, err := scoped.BeginGroupCreate(ctx, "idem:k", "WACSECOND", "Obras", time.Now())
	if err != nil {
		t.Fatalf("the second delivery: %v", err)
	}
	if !found {
		t.Fatal("the second delivery was not reported as a redelivery")
	}
	if again.Key != "WACFIRST" {
		t.Fatalf("the attempt now reads %s, want the key its creation was sent under, WACFIRST", again.Key)
	}
	if got := again.StartedAt.UnixMilli(); got != began.UnixMilli() {
		t.Fatalf("the intent now reads %d, want the first delivery's %d", got, began.UnixMilli())
	}
}

// Two answers to one command have to be one answer. WhatsApp's notification and the
// creation's own reply both say which group was made, and they can arrive in either order.
func TestNamingAGroupTwiceKeepsTheFirstName(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	scoped := container.For("sid-1")

	if _, _, err := scoped.BeginGroupCreate(ctx, "idem:k", "WACK", "Obras", time.Now()); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := scoped.FinishGroupCreate(ctx, "idem:k", "120363041234567890@g.us"); err != nil {
		t.Fatalf("the first naming: %v", err)
	}
	if named, err := scoped.FinishGroupCreateByKey(ctx, "WACK", "120363099999999999@g.us"); err != nil {
		t.Fatalf("the second naming: %v", err)
	} else if named {
		t.Fatal("the second naming reported that it settled an attempt that was already answered")
	}

	found, ok, err := scoped.GroupCreation(ctx, "idem:k")
	if err != nil || !ok {
		t.Fatalf("read it back: %v, found=%v", err, ok)
	}
	if found.JID != "120363041234567890@g.us" {
		t.Fatalf("the attempt now names %s, want the group it was first answered with", found.JID)
	}
}

// The key is how WhatsApp's own notification finds the attempt that sent it, and that is
// the whole mechanism: the instance that wrote the intent may be gone, so the pairing has
// to be findable from nothing but what comes back on the wire.
func TestANotificationNamesTheGroupByTheKeyItsCreationWasSentUnder(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	scoped := container.For("sid-1")

	if _, _, err := scoped.BeginGroupCreate(ctx, "idem:k", "WACK", "Obras", time.Now()); err != nil {
		t.Fatalf("begin: %v", err)
	}
	named, err := scoped.FinishGroupCreateByKey(ctx, "WACK", "120363041234567890@g.us")
	if err != nil {
		t.Fatalf("name it by its key: %v", err)
	}
	if !named {
		t.Fatal("the notification settled nothing, so the command it answers waits for good")
	}
	found, _, err := scoped.GroupCreation(ctx, "idem:k")
	if err != nil {
		t.Fatalf("read it back: %v", err)
	}
	if found.JID != "120363041234567890@g.us" {
		t.Fatalf("the attempt names %q, want the group the notification carried", found.JID)
	}

	// A key nothing is waiting under settles nothing, and says so: most groups an account
	// joins were made by somebody else.
	if named, err := scoped.FinishGroupCreateByKey(ctx, "WACSTRANGER", "120363099999999999@g.us"); err != nil {
		t.Fatalf("a key nothing is waiting under: %v", err)
	} else if named {
		t.Fatal("a key no attempt was filed under was reported as having settled one")
	}
}

// The key is this session's, and a notification is about this account's group. Another
// session's attempt under the same key is another account's business, and settling it from
// here would answer one client's command with another's group.
func TestAKeyIsOnlyLookedForInItsOwnSession(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	mine, theirs := container.For("sid-1"), container.For("sid-2")

	began := time.Now()
	if _, _, err := mine.BeginGroupCreate(ctx, "idem:k", "WACSHARED", "Obras", began); err != nil {
		t.Fatalf("begin this session's: %v", err)
	}
	if _, _, err := theirs.BeginGroupCreate(ctx, "idem:k", "WACSHARED", "Obras", began); err != nil {
		t.Fatalf("begin the other session's: %v", err)
	}

	if _, err := mine.FinishGroupCreateByKey(ctx, "WACSHARED", "120363041234567890@g.us"); err != nil {
		t.Fatalf("name this session's group: %v", err)
	}
	other, _, err := theirs.GroupCreation(ctx, "idem:k")
	if err != nil {
		t.Fatalf("read the other session's attempt: %v", err)
	}
	if other.Done() {
		t.Fatalf("the other session's attempt was settled with %s, a group of this session's", other.JID)
	}
}

// A session that no longer owns this one may not write for it, here as everywhere: the
// successor is the one making the group now, and a write landing behind it would file its
// creation under an owner that is gone.
func TestASessionThatLostTheLeaseCannotRecordACreation(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	losing := container.For("sid-1")
	if _, _, err := losing.BeginGroupCreate(ctx, "idem:k", "WACK", "Obras", time.Now()); err != nil {
		t.Fatalf("begin while it still owned the session: %v", err)
	}
	losing.Drop()

	if _, _, err := losing.BeginGroupCreate(ctx, "idem:other", "WACOTHER", "Obras", time.Now()); err == nil {
		t.Fatal("a session that lost the lease wrote the intent to make a group")
	}
	if err := losing.FinishGroupCreate(ctx, "idem:k", "120363041234567890@g.us"); err == nil {
		t.Fatal("a session that lost the lease recorded the group a creation made")
	}
	if _, err := losing.FinishGroupCreateByKey(ctx, "WACK", "120363041234567890@g.us"); err == nil {
		t.Fatal("a session that lost the lease recorded a group from a notification")
	}
	// And nothing reached the table: a fence that answers after writing passes the checks
	// above and still leaves the row behind.
	found, ok, err := container.For("sid-1").GroupCreation(ctx, "idem:other")
	if err != nil {
		t.Fatalf("read it back: %v", err)
	}
	if ok {
		t.Fatalf("the refused write landed anyway: %v", found)
	}
}

// The sweep exists so the row count is not the number of groups the deployment has ever
// made, and it must not take a record a delivery still needs. It goes by when the row was
// last written, not by when it began: an attempt still waiting to hear which group it made
// is the one row here that cannot be reconstructed.
func TestTheSweepTakesOnlyWhatWasLastWrittenLongAgo(t *testing.T) {
	t.Parallel()
	// The address is kept because this test writes one statement of its own, and the two
	// dialects spell a placeholder differently: `?` reaches Postgres verbatim through
	// Container.DB, which takes no rebinding with it.
	target := storetest.New(t)
	container := openAt(t, target)
	ctx := t.Context()
	scoped := container.For("sid-1")

	long := time.Now().Add(-72 * time.Hour)
	for attempt, key := range map[string]string{
		"idem:old": "WACOLD", "idem:fresh": "WACFRESH", "idem:open": "WACOPEN",
	} {
		if _, _, err := scoped.BeginGroupCreate(ctx, attempt, key, "Obras", long); err != nil {
			t.Fatalf("begin %s: %v", attempt, err)
		}
	}
	if err := scoped.FinishGroupCreate(ctx, "idem:old", "120363041111111111@g.us"); err != nil {
		t.Fatalf("settle the old one: %v", err)
	}
	if err := scoped.FinishGroupCreate(ctx, "idem:fresh", "120363042222222222@g.us"); err != nil {
		t.Fatalf("settle the fresh one: %v", err)
	}
	// The old one was last written long ago; the fresh one just now. Both began long ago,
	// and the open one has not been written since it began, so the cutoff takes it too.
	if _, err := container.DB().ExecContext(ctx, target.Rebind(
		`UPDATE wac_group_create SET touched_at = ? WHERE sid = ? AND attempt = ?`),
		long.UnixMilli(), "sid-1", "idem:old"); err != nil {
		t.Fatalf("date the old settlement: %v", err)
	}

	swept, err := container.SweepGroupCreations(ctx, time.Now().Add(-48*time.Hour))
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if swept != 2 {
		t.Fatalf("the sweep took %d rows, want the two last written before the cutoff", swept)
	}
	for attempt, want := range map[string]bool{"idem:old": false, "idem:fresh": true, "idem:open": false} {
		_, found, err := scoped.GroupCreation(ctx, attempt)
		if err != nil {
			t.Fatalf("read %s: %v", attempt, err)
		}
		if found != want {
			t.Fatalf("%s present=%v, want %v", attempt, found, want)
		}
	}
}

// An attempt that made nothing says nothing, and left on record it would have every later
// request under that name wait on a notification about a group that does not exist.
func TestAnAbandonedAttemptIsGone(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	scoped := container.For("sid-1")

	if _, _, err := scoped.BeginGroupCreate(ctx, "idem:refused", "WACNO", "Obras", time.Now()); err != nil {
		t.Fatalf("begin the one WhatsApp refused: %v", err)
	}
	if err := scoped.AbandonGroupCreate(ctx, "idem:refused"); err != nil {
		t.Fatalf("abandon it: %v", err)
	}
	if _, found, err := scoped.GroupCreation(ctx, "idem:refused"); err != nil {
		t.Fatalf("read it back: %v", err)
	} else if found {
		t.Fatal("the abandoned attempt is still on record, where every retry of that name waits on it")
	}
}

// An attempt that already named a group is the answer a redelivery is owed, and forgetting
// it is not this call's to do: the next delivery would find nothing, create a second group
// and answer with that one.
func TestAnAttemptThatNamedAGroupIsNotAbandoned(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	scoped := container.For("sid-1")

	if _, _, err := scoped.BeginGroupCreate(ctx, "idem:k", "WACK", "Obras", time.Now()); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := scoped.FinishGroupCreate(ctx, "idem:k", "120363041234567890@g.us"); err != nil {
		t.Fatalf("name the group: %v", err)
	}
	if err := scoped.AbandonGroupCreate(ctx, "idem:k"); err != nil {
		t.Fatalf("abandon: %v", err)
	}
	found, ok, err := scoped.GroupCreation(ctx, "idem:k")
	if err != nil || !ok {
		t.Fatalf("read it back: %v, found=%v", err, ok)
	}
	if found.JID != "120363041234567890@g.us" {
		t.Fatalf("the answer a redelivery is owed was thrown away: %+v", found)
	}
}
