package store_test

import (
	"testing"
	"time"
)

// The intent is what a later delivery goes on, so a later delivery must not rewrite it.
// Moving the instant forward would put the group the first attempt made before its own
// intent, and the search would miss it: the duplicate, caused by the record meant to
// prevent it.
func TestALaterDeliveryDoesNotMoveTheIntentItFound(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	scoped := container.For("sid-1")

	began := time.Now().Add(-time.Hour)
	if _, found, err := scoped.BeginGroupCreate(ctx, "idem:k", "Obras", began); err != nil {
		t.Fatalf("the first delivery: %v", err)
	} else if found {
		t.Fatal("the first delivery was reported as one that had been here before")
	}

	again, found, err := scoped.BeginGroupCreate(ctx, "idem:k", "Obras", time.Now())
	if err != nil {
		t.Fatalf("the second delivery: %v", err)
	}
	if !found {
		t.Fatal("the second delivery was not reported as a redelivery")
	}
	if got := again.StartedAt.UnixMilli(); got != began.UnixMilli() {
		t.Fatalf("the intent now reads %d, want the first delivery's %d: a search from here would miss the group",
			got, began.UnixMilli())
	}
}

// Two answers to one command have to be one answer. A later attempt that reconciled its way
// to a group and a first attempt that came back slowly both write what they believe, and the
// first naming is the one every delivery after it has already been answered with.
func TestNamingAGroupTwiceKeepsTheFirstName(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	scoped := container.For("sid-1")

	if _, _, err := scoped.BeginGroupCreate(ctx, "idem:k", "Obras", time.Now()); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := scoped.FinishGroupCreate(ctx, "idem:k", "120363041234567890@g.us"); err != nil {
		t.Fatalf("the first naming: %v", err)
	}
	if err := scoped.FinishGroupCreate(ctx, "idem:k", "120363099999999999@g.us"); err != nil {
		t.Fatalf("the second naming: %v", err)
	}

	found, ok, err := scoped.GroupCreation(ctx, "idem:k")
	if err != nil || !ok {
		t.Fatalf("read it back: %v, found=%v", err, ok)
	}
	if found.JID != "120363041234567890@g.us" {
		t.Fatalf("the attempt now names %s, want the group it was first answered with", found.JID)
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
	if _, _, err := losing.BeginGroupCreate(ctx, "idem:k", "Obras", time.Now()); err != nil {
		t.Fatalf("begin while it still owned the session: %v", err)
	}
	losing.Drop()

	if _, _, err := losing.BeginGroupCreate(ctx, "idem:other", "Obras", time.Now()); err == nil {
		t.Fatal("a session that lost the lease wrote the intent to make a group")
	}
	if err := losing.FinishGroupCreate(ctx, "idem:k", "120363041234567890@g.us"); err == nil {
		t.Fatal("a session that lost the lease recorded the group a creation made")
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
// made, and it must not take a record a delivery still needs. Two things follow: an attempt
// that settled inside the window stays, and one that never settled stays whatever its age --
// it is the only row here that cannot be reconstructed.
func TestTheSweepTakesOnlyWhatSettledLongAgo(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	scoped := container.For("sid-1")

	for _, attempt := range []string{"idem:old", "idem:fresh", "idem:open"} {
		if _, _, err := scoped.BeginGroupCreate(ctx, attempt, "Obras", time.Now().Add(-72*time.Hour)); err != nil {
			t.Fatalf("begin %s: %v", attempt, err)
		}
	}
	if err := scoped.FinishGroupCreate(ctx, "idem:old", "120363041111111111@g.us"); err != nil {
		t.Fatalf("settle the old one: %v", err)
	}
	if err := scoped.FinishGroupCreate(ctx, "idem:fresh", "120363042222222222@g.us"); err != nil {
		t.Fatalf("settle the fresh one: %v", err)
	}
	// The old one settled long ago; the fresh one settled just now. Both began long ago,
	// which is exactly what the sweep must not go by.
	if _, err := container.DB().ExecContext(ctx,
		`UPDATE wac_group_create SET settled_at = ? WHERE sid = ? AND attempt = ?`,
		time.Now().Add(-72*time.Hour).UnixMilli(), "sid-1", "idem:old"); err != nil {
		t.Fatalf("date the old settlement: %v", err)
	}

	swept, err := container.SweepGroupCreations(ctx, time.Now().Add(-48*time.Hour))
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if swept != 1 {
		t.Fatalf("the sweep took %d rows, want the one that settled before the cutoff", swept)
	}
	for attempt, want := range map[string]bool{"idem:old": false, "idem:fresh": true, "idem:open": true} {
		_, found, err := scoped.GroupCreation(ctx, attempt)
		if err != nil {
			t.Fatalf("read %s: %v", attempt, err)
		}
		if found != want {
			t.Fatalf("%s present=%v, want %v", attempt, found, want)
		}
	}
}

// An attempt at a group by another name is not competition, and one that settled is not
// either: what makes a search untrustworthy is another attempt at the same name that has
// not finished, because a group that matches one matches both.
func TestOnlyAnUnsettledAttemptAtTheSameNameContestsASearch(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	scoped := container.For("sid-1")

	began := time.Now()
	for _, attempt := range []string{"idem:mine", "idem:other-name", "idem:settled", "idem:open"} {
		subject := "Obras"
		if attempt == "idem:other-name" {
			subject = "Churrasco"
		}
		if _, _, err := scoped.BeginGroupCreate(ctx, attempt, subject, began); err != nil {
			t.Fatalf("begin %s: %v", attempt, err)
		}
	}
	if err := scoped.FinishGroupCreate(ctx, "idem:settled", "120363041111111111@g.us"); err != nil {
		t.Fatalf("settle one: %v", err)
	}

	contested, err := scoped.OtherAttemptsStillOpen(ctx, "idem:mine", "Obras")
	if err != nil {
		t.Fatalf("OtherAttemptsStillOpen: %v", err)
	}
	if !contested {
		t.Fatal("an open attempt at a group by the same name was not counted as competition")
	}

	// With the open one settled, nothing is in doubt any more.
	if err := scoped.FinishGroupCreate(ctx, "idem:open", "120363042222222222@g.us"); err != nil {
		t.Fatalf("settle the open one: %v", err)
	}
	contested, err = scoped.OtherAttemptsStillOpen(ctx, "idem:mine", "Obras")
	if err != nil {
		t.Fatalf("OtherAttemptsStillOpen: %v", err)
	}
	if contested {
		t.Fatal("a search was called contested with every competing attempt settled")
	}

	// And another session's open attempt is not this one's competition.
	if _, _, err := container.For("sid-2").BeginGroupCreate(ctx, "idem:theirs", "Obras", began); err != nil {
		t.Fatalf("begin another session's attempt: %v", err)
	}
	contested, err = scoped.OtherAttemptsStillOpen(ctx, "idem:mine", "Obras")
	if err != nil {
		t.Fatalf("OtherAttemptsStillOpen: %v", err)
	}
	if contested {
		t.Fatal("another session's open attempt was counted against this one")
	}
}

// The groups a search rules out are the ones this session has already filed under another
// name, and only those: an attempt that has not named a group rules nothing out, and another
// session's records are not this one's.
func TestOnlyThisSessionsNamedGroupsAreRuledOut(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()
	scoped := container.For("sid-1")

	began := time.Now()
	if _, _, err := scoped.BeginGroupCreate(ctx, "idem:mine", "Obras", began); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, _, err := scoped.BeginGroupCreate(ctx, "idem:sibling", "Obras", began); err != nil {
		t.Fatalf("begin the sibling: %v", err)
	}
	if err := scoped.FinishGroupCreate(ctx, "idem:sibling", "120363041111111111@g.us"); err != nil {
		t.Fatalf("name the sibling's group: %v", err)
	}
	if _, _, err := container.For("sid-2").BeginGroupCreate(ctx, "idem:theirs", "Obras", began); err != nil {
		t.Fatalf("begin another session's attempt: %v", err)
	}
	if err := container.For("sid-2").FinishGroupCreate(ctx, "idem:theirs", "120363042222222222@g.us"); err != nil {
		t.Fatalf("name another session's group: %v", err)
	}

	claimed, err := scoped.GroupsClaimedByOtherAttempts(ctx, "idem:mine")
	if err != nil {
		t.Fatalf("GroupsClaimedByOtherAttempts: %v", err)
	}
	if _, ruled := claimed["120363041111111111@g.us"]; !ruled {
		t.Fatal("a group this session already filed under another name was not ruled out")
	}
	if _, ruled := claimed["120363042222222222@g.us"]; ruled {
		t.Fatal("another session's group was ruled out of this session's search")
	}
	if len(claimed) != 1 {
		t.Fatalf("the search rules out %v, want only this session's other attempt", claimed)
	}
}
