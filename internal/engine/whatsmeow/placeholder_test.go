package whatsmeow

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/zerolog"
	wm "go.mau.fi/whatsmeow"

	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// scheduledAt is the moment the first owner receives the unreadable message. Fixed, so
// every deadline in these tests is arithmetic rather than a race with the clock.
var scheduledAt = time.UnixMilli(1755000000000)

// The bubble for a message that could not be read lives in a timer, and a timer lives
// exactly as long as the process holding it. A deploy, a crash or a lease moving inside
// the window used to take it: whatsmeow acknowledged the stanza before the connector
// ever saw the failure, so WhatsApp has no copy left to send and the chat is left
// showing nothing at all.
func TestAPlaceholderOutlivesTheProcessThatScheduledIt(t *testing.T) {
	t.Parallel()

	first, container := newTestSession(t, "5511999990001")
	// Long enough that this session never fires it. What is under test is what the row
	// holds, not what the process that wrote it publishes.
	first.rerequestWait = time.Minute
	first.wallClock = func() time.Time { return scheduledAt }

	publishedNothingUnreadable(t, first, unavailableMessage("3EB0HANDOFF", "view_once"))

	held := placeholdersOf(t, container, first.sid)
	if len(held) != 1 {
		t.Fatalf("the undecided bubble was not written down (%d rows)", len(held))
	}
	if want := scheduledAt.Add(time.Minute).UnixMilli(); held[0].DueAt != want {
		t.Errorf("the row is due at %d, and the window it was given ends at %d", held[0].DueAt, want)
	}
	if held[0].LearnedAt != scheduledAt.UnixMilli() {
		t.Errorf("the row learned at %d, and the message reached the connector at %d",
			held[0].LearnedAt, scheduledAt.UnixMilli())
	}

	if err := first.Close(); err != nil {
		t.Fatalf("Close the first owner: %v", err)
	}

	// A minute past the deadline the first owner set, which is what an instance picking
	// the session up after a deploy finds.
	second := nextOwner(t, container, first.sid)
	second.wallClock = func() time.Time { return scheduledAt.Add(2 * time.Minute) }
	second.rearm(t.Context())

	emission := next(t, second)
	emission.Settle(nil)
	message := messageOf(t, emission)
	if message.ID != "3EB0HANDOFF" {
		t.Errorf("the bubble names %q, and the message the first owner held was 3EB0HANDOFF", message.ID)
	}
	// The moment the message reached the connector, not the moment the second owner
	// started. A bubble stamped with the handover lands at the top of the thread instead
	// of where the message it stands for belongs.
	if emission.At != scheduledAt.UnixMilli() {
		t.Errorf("the bubble goes out at %d, and the message arrived at %d", emission.At, scheduledAt.UnixMilli())
	}
	validateInboundAgainstContract(t, &message)

	// And the row goes with it, or the owner after this one publishes it again.
	waitForNoPlaceholders(t, container, first.sid)
}

// The row is the good ending staying possible, not the bad one being scheduled. A
// message that turns up at whoever owns the session next is the recovery working, and
// the bubble has to be called off there exactly as it would have been at the owner that
// wrote it -- otherwise carrying it across the handoff would have made things worse,
// since a client deduplicates on the id and keeps whichever arrived first.
func TestAMessageArrivingAtTheNextOwnerCallsOffTheHeldPlaceholder(t *testing.T) {
	t.Parallel()

	first, container := newTestSession(t, "5511999990001")
	first.rerequestWait = time.Minute
	first.wallClock = func() time.Time { return scheduledAt }

	publishedNothingUnreadable(t, first, unavailableMessage("3EB0RECOVERAFTER", "view_once"))
	if len(placeholdersOf(t, container, first.sid)) != 1 {
		t.Fatal("nothing was written down, so nothing here could be picked up")
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close the first owner: %v", err)
	}

	// A second into the window rather than past it, so the placeholder is armed and
	// waiting when the real message arrives.
	second := nextOwner(t, container, first.sid)
	second.wallClock = func() time.Time { return scheduledAt.Add(time.Second) }
	second.rearm(t.Context())
	if !waitingOn(second, "3EB0RECOVERAFTER") {
		t.Fatal("the second owner did not pick the placeholder up, so there was nothing to call off")
	}

	emission := publishedBy(t, second, textMessage("3EB0RECOVERAFTER", "bom dia"))
	message := messageOf(t, emission)
	var content struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(mustMarshal(t, message.Content), &content); err != nil {
		t.Fatalf("unmarshal the content: %v", err)
	}
	if content.Type != "text" {
		t.Fatalf("what reached the chat is %s, and the message itself arrived", content.Type)
	}
	if waitingOn(second, "3EB0RECOVERAFTER") {
		t.Fatal("the placeholder is still waiting behind the message it was standing in for")
	}
	// And it is gone from the store too, or the owner after this one arms it again for a
	// message that has already been published.
	waitForNoPlaceholders(t, container, first.sid)
}

// A row this build cannot read would otherwise be armed, fail, and be armed again by
// every owner the session ever has. Dropped instead, and said so.
func TestAHeldPlaceholderThatCannotBeReadIsDroppedRatherThanRetriedForever(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	if err := container.For(session.sid).PutPlaceholder(t.Context(), &store.Placeholder{
		MessageID: "3EB0GARBAGE", Message: "{not json", LearnedAt: scheduledAt.UnixMilli(),
		DueAt: scheduledAt.UnixMilli(),
	}); err != nil {
		t.Fatalf("PutPlaceholder: %v", err)
	}

	session.rearm(t.Context())
	if waitingOn(session, "3EB0GARBAGE") {
		t.Fatal("a row nothing can render was put on the clock, where it can only fail")
	}
	waitForNoPlaceholders(t, container, session.sid)
}

// nextOwner is the session the lease moves to: a new handle on the same store and the
// same session id, the way Open builds one. A fence of its own, because the handle the
// first owner held was dropped when it closed.
func nextOwner(t *testing.T, container *store.Container, sid string) *Session {
	t.Helper()

	scoped := container.For(sid)
	device, err := scoped.Device(t.Context())
	if err != nil {
		t.Fatalf("Device for the next owner: %v", err)
	}
	session := newSession(
		sid, wm.NewClient(device, nil), scoped, MediaOptions{}, zerolog.Nop(),
		newLibraryLogger(zerolog.Nop(), sid))
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func placeholdersOf(t *testing.T, container *store.Container, sid string) []store.Placeholder {
	t.Helper()

	held, err := container.For(sid).Placeholders(t.Context())
	if err != nil {
		t.Fatalf("Placeholders: %v", err)
	}
	return held
}

// waitForNoPlaceholders polls, because the row is dropped after the publish settles and
// on the goroutine that published it: asserting straight away would be asserting on the
// scheduler rather than on the code.
func waitForNoPlaceholders(t *testing.T, container *store.Container, sid string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if len(placeholdersOf(t, container, sid)) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the row for a decided placeholder is still there, and the next owner would publish it again")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A placeholder held for a session that is unpaired goes with it: the row exists to put
// a bubble in that account's chat, and the account is gone.
func TestForgettingASessionTakesThePlaceholdersItWasHolding(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	if err := container.For(session.sid).PutPlaceholder(t.Context(), &store.Placeholder{
		MessageID: "3EB0UNPAIRED", Message: `{"id":"3EB0UNPAIRED"}`,
		LearnedAt: scheduledAt.UnixMilli(), DueAt: scheduledAt.UnixMilli(),
	}); err != nil {
		t.Fatalf("PutPlaceholder: %v", err)
	}
	if err := container.For(session.sid).Forget(t.Context()); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if held := placeholdersOf(t, container, session.sid); len(held) != 0 {
		t.Fatalf("%d placeholder(s) outlived the pairing they were held for", len(held))
	}
}
