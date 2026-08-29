package store_test

import (
	"reflect"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

func sampleHold(messageID string, learnedAt, dueAt int64) store.Placeholder {
	return store.Placeholder{
		MessageID: messageID,
		Message:   `{"id":"` + messageID + `","content":{"type":"unsupported","reason":"unavailable"}}`,
		LearnedAt: learnedAt,
		DueAt:     dueAt,
	}
}

// The row is the bubble. It is published unchanged if nothing better arrives, so a
// round trip that alters it alters what somebody reads in a chat.
func TestWhatIsHeldForAnUndecidedBubbleComesBackExactly(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")

	held := sampleHold("3EB0HELD", 1755000000000, 1755000045000)
	if err := container.For("sid-1").PutPlaceholder(t.Context(), &held); err != nil {
		t.Fatalf("PutPlaceholder: %v", err)
	}
	back, err := container.For("sid-1").Placeholders(t.Context())
	if err != nil {
		t.Fatalf("Placeholders: %v", err)
	}
	want := held
	want.SID = "sid-1"
	if len(back) != 1 || !reflect.DeepEqual(back[0], want) {
		t.Fatalf("what came back is %+v, and what went in was %+v", back, want)
	}
}

// whatsmeow raises the same failure again when a resend will not decrypt either, and
// the first hold is the one that stands. The engine's waiting list keeps the first
// arming and ignores the second, so a row that took the second would be due after the
// timer that is going to fire, and a handoff in between would serve out a window nobody
// granted.
func TestHoldingTheSameMessageTwiceKeepsTheDeadlineOfTheFirst(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")

	first := sampleHold("3EB0AGAIN", 1755000000000, 1755000045000)
	if err := container.For("sid-1").PutPlaceholder(t.Context(), &first); err != nil {
		t.Fatalf("PutPlaceholder: %v", err)
	}
	second := sampleHold("3EB0AGAIN", 1755000010000, 1755000055000)
	if err := container.For("sid-1").PutPlaceholder(t.Context(), &second); err != nil {
		t.Fatalf("PutPlaceholder the second time: %v", err)
	}

	back, err := container.For("sid-1").Placeholders(t.Context())
	if err != nil {
		t.Fatalf("Placeholders: %v", err)
	}
	if len(back) != 1 {
		t.Fatalf("one message is waiting under %d rows, and each of them is a bubble", len(back))
	}
	if back[0].DueAt != first.DueAt {
		t.Errorf("the row is due at %d, and the window that was actually armed ends at %d",
			back[0].DueAt, first.DueAt)
	}
}

// Oldest deadline first, so an owner picking several up arms the most overdue before it
// spends its store budget on the rest.
func TestHeldPlaceholdersComeBackOldestDeadlineFirst(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")

	for _, held := range []store.Placeholder{
		sampleHold("3EB0LATER", 1755000020000, 1755000065000),
		sampleHold("3EB0SOONER", 1755000000000, 1755000045000),
	} {
		if err := container.For("sid-1").PutPlaceholder(t.Context(), &held); err != nil {
			t.Fatalf("PutPlaceholder: %v", err)
		}
	}

	back, err := container.For("sid-1").Placeholders(t.Context())
	if err != nil {
		t.Fatalf("Placeholders: %v", err)
	}
	if len(back) != 2 {
		t.Fatalf("%d rows came back, and two went in", len(back))
	}
	if back[0].MessageID != "3EB0SOONER" {
		t.Errorf("the first row back is %s, and the sooner deadline is 3EB0SOONER's", back[0].MessageID)
	}
}

// One session's undecided bubbles are not another's. A message id is WhatsApp's to
// reuse across accounts, and a bubble published into the wrong chat is permanent.
func TestWhatIsHeldForOneSessionIsNotReadByAnother(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")
	pair(t, container, "sid-2", "5511999990002")

	held := sampleHold("3EB0SHARED", 1755000000000, 1755000045000)
	if err := container.For("sid-1").PutPlaceholder(t.Context(), &held); err != nil {
		t.Fatalf("PutPlaceholder: %v", err)
	}
	if back, err := container.For("sid-2").Placeholders(t.Context()); err != nil || len(back) != 0 {
		t.Fatalf("the other session is holding %d of these (err=%v)", len(back), err)
	}
}

// The write is fenced because the row is read by whoever owns the session next: an
// instance that has lost it would otherwise hand its successor a bubble for a message
// already answered somewhere else, and the client's deduplication makes that permanent.
func TestAHeldPlaceholderFromASessionThatWasHandedOnIsRefused(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")

	losing := container.For("sid-1")
	losing.Drop()

	held := sampleHold("3EB0STALE", 1755000000000, 1755000045000)
	if err := losing.PutPlaceholder(t.Context(), &held); err == nil {
		t.Fatal("a session that no longer owns this one wrote a bubble for its successor to publish")
	}
	if back, _ := container.For("sid-1").Placeholders(t.Context()); len(back) != 0 {
		t.Fatalf("%d bubble(s) were left behind by a session that had been handed on", len(back))
	}
}

// And the delete with it: taking somebody else's row is how a bubble goes missing on
// the very handoff the row exists to survive.
func TestDroppingAHeldPlaceholderFromASessionThatWasHandedOnIsRefused(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")

	owner := container.For("sid-1")
	held := sampleHold("3EB0KEPT", 1755000000000, 1755000045000)
	if err := owner.PutPlaceholder(t.Context(), &held); err != nil {
		t.Fatalf("PutPlaceholder: %v", err)
	}
	owner.Drop()

	if err := owner.DropPlaceholder(t.Context(), "3EB0KEPT"); err == nil {
		t.Fatal("a session that no longer owns this one took its successor's bubble away")
	}
	if back, _ := container.For("sid-1").Placeholders(t.Context()); len(back) != 1 {
		t.Fatalf("the bubble the next owner should publish is gone (%d rows)", len(back))
	}
}

// A bubble is decided once, and the row goes when it is.
func TestDroppingAHeldPlaceholderLeavesTheRest(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")

	for _, held := range []store.Placeholder{
		sampleHold("3EB0DONE", 1755000000000, 1755000045000),
		sampleHold("3EB0WAITING", 1755000010000, 1755000055000),
	} {
		if err := container.For("sid-1").PutPlaceholder(t.Context(), &held); err != nil {
			t.Fatalf("PutPlaceholder: %v", err)
		}
	}
	if err := container.For("sid-1").DropPlaceholder(t.Context(), "3EB0DONE"); err != nil {
		t.Fatalf("DropPlaceholder: %v", err)
	}

	back, err := container.For("sid-1").Placeholders(t.Context())
	if err != nil {
		t.Fatalf("Placeholders: %v", err)
	}
	if len(back) != 1 || back[0].MessageID != "3EB0WAITING" {
		t.Fatalf("what is left waiting is %+v, and only 3EB0DONE was decided", back)
	}
}

// The table refuses a row whose session is not paired, the way the media table does.
// A bubble exists to be published into an account's chat, so one that names no account
// is one nothing could ever do anything with.
func TestAHoldForASessionThatIsNotPairedIsRefused(t *testing.T) {
	t.Parallel()
	container := open(t)

	held := sampleHold("3EB0NOWHERE", 1755000000000, 1755000045000)
	if err := container.For("sid-unpaired").PutPlaceholder(t.Context(), &held); err == nil {
		t.Fatal("a bubble was held for a session with no account to publish it into")
	}
}
