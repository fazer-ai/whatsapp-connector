package store_test

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// storedAt is a fixed clock: every test that cares about retention says how old a row
// is by writing it at an offset from this rather than by sleeping.
var storedAt = time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

func samplePart(sid, messageID string) store.MediaPart {
	return store.MediaPart{
		SID: sid, MessageID: messageID, Kind: "image",
		DirectPath:    "/v/t62.7118-24/file.enc",
		MediaKey:      bytes.Repeat([]byte{1}, 32),
		FileEncSHA256: bytes.Repeat([]byte{2}, 32),
		FileSHA256:    bytes.Repeat([]byte{3}, 32),
		FileLength:    111743, Mime: "image/jpeg", Filename: "recibo.pdf",
	}
}

// The whole point of the table: what goes in comes back out byte for byte, because what
// is in it is the address of a file on WhatsApp's servers and one wrong byte fetches
// nothing.
func TestWhatIsKeptToFetchAFileComesBackExactly(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")

	want := samplePart("sid-1", "3EB0IMAGE")
	if err := container.PutMediaPart(t.Context(), &want, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	got, found, err := container.MediaPart(t.Context(), "sid-1", "3EB0IMAGE")
	if err != nil {
		t.Fatalf("MediaPart: %v", err)
	}
	if !found {
		t.Fatal("a message that was recorded is not kept")
	}
	want.StoredAt = storedAt.UnixMilli()
	if !reflect.DeepEqual(got, want) {
		// Compared whole rather than field by field: a field added to the struct and
		// forgotten in the SQL is exactly what this has to catch.
		t.Fatalf("what came back is\n %+v\nwant\n %+v", got, want)
	}
}

// A message id belongs to an account, and two accounts can be handed the same one.
// Reading across sessions would fetch a file with somebody else's key.
func TestWhatIsKeptForOneSessionIsNotReadByAnother(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")
	pair(t, container, "sid-2", "5511999990002")

	part := samplePart("sid-1", "3EB0SAME")
	if err := container.PutMediaPart(t.Context(), &part, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	if _, found, err := container.MediaPart(t.Context(), "sid-2", "3EB0SAME"); err != nil || found {
		t.Fatalf("another session read the part (found=%v, err=%v)", found, err)
	}
}

// A message that arrives twice is the same file both times, and the second delivery
// carries metadata at least as fresh as the first.
func TestARedeliveredMessageReplacesWhatWasKeptForIt(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")

	first := samplePart("sid-1", "3EB0AGAIN")
	if err := container.PutMediaPart(t.Context(), &first, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	second := first
	second.DirectPath = "/v/t62.7118-24/refreshed.enc"
	later := storedAt.Add(time.Hour)
	if err := container.PutMediaPart(t.Context(), &second, later); err != nil {
		t.Fatalf("PutMediaPart again: %v", err)
	}

	got, _, err := container.MediaPart(t.Context(), "sid-1", "3EB0AGAIN")
	if err != nil {
		t.Fatalf("MediaPart: %v", err)
	}
	if got.DirectPath != second.DirectPath {
		t.Fatalf("the direct path is %q, want the one the redelivery carried", got.DirectPath)
	}
	if got.StoredAt != later.UnixMilli() {
		// The clock moves with it, or a message that keeps arriving would age out on the
		// strength of the first time it was seen.
		t.Fatalf("the row is stamped %d, want the redelivery's %d", got.StoredAt, later.UnixMilli())
	}
}

// Ownership of a session moves between instances, and the old owner's handler can still
// be inside this call when the new owner has already written. The older write has nothing
// to add and can only take something away: a stale direct path installed over a fresh one
// is a download answered with a 404, which is the failure this whole table exists to
// prevent.
//
// This is not a fence against a lost lease -- there is no epoch in the schema and adding
// one is an architecture change -- it is the write being monotonic in time, which is what
// removes the harm reordering does here.
func TestAWriteThatArrivesLateDoesNotOverwriteANewerOne(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")

	fresh := samplePart("sid-1", "3EB0RACE")
	fresh.DirectPath = "/v/t62.7118-24/fresh.enc"
	if err := container.PutMediaPart(t.Context(), &fresh, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	// The old owner's write, made before the fresh one and landing after it.
	stale := samplePart("sid-1", "3EB0RACE")
	stale.DirectPath = "/v/t62.7118-24/stale.enc"
	if err := container.PutMediaPart(t.Context(), &stale, storedAt.Add(-time.Minute)); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	got, _, err := container.MediaPart(t.Context(), "sid-1", "3EB0RACE")
	if err != nil {
		t.Fatalf("MediaPart: %v", err)
	}
	if got.DirectPath != fresh.DirectPath {
		t.Fatalf("the direct path is %q, want the one the newer write left", got.DirectPath)
	}
	if got.StoredAt != storedAt.UnixMilli() {
		t.Fatalf("the row is stamped %d, want the newer write's %d", got.StoredAt, storedAt.UnixMilli())
	}
}

// A message nobody kept anything for is not an error: it is the ordinary answer for a
// text message, and for one whose retention has run out.
func TestAMessageNothingWasKeptForIsNotAnError(t *testing.T) {
	t.Parallel()
	container := open(t)

	part, found, err := container.MediaPart(t.Context(), "sid-1", "3EB0NEVER")
	if err != nil {
		t.Fatalf("MediaPart: %v", err)
	}
	if found {
		t.Fatalf("a message nothing was kept for came back as %+v", part)
	}
}

// The table holds a row per media message the deployment ever received, each one the key
// to somebody's file. Without the sweep it grows for the life of the deployment.
func TestWhatOutlivedItsRetentionIsSweptAndTheRestIsLeft(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")

	old := samplePart("sid-1", "3EB0OLD")
	fresh := samplePart("sid-1", "3EB0FRESH")
	if err := container.PutMediaPart(t.Context(), &old, storedAt.Add(-8*24*time.Hour)); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	if err := container.PutMediaPart(t.Context(), &fresh, storedAt.Add(-time.Hour)); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	dropped, err := container.SweepMediaParts(t.Context(), storedAt.Add(-7*24*time.Hour))
	if err != nil {
		t.Fatalf("SweepMediaParts: %v", err)
	}
	if dropped != 1 {
		t.Fatalf("the sweep dropped %d rows, want only the one past its retention", dropped)
	}
	if _, found, _ := container.MediaPart(t.Context(), "sid-1", "3EB0OLD"); found {
		t.Fatal("a row past its retention survived the sweep")
	}
	if _, found, _ := container.MediaPart(t.Context(), "sid-1", "3EB0FRESH"); !found {
		t.Fatal("the sweep took a row that was still within its retention")
	}
}

// Unpairing takes the device's keys with it. What is in this table is the key to
// somebody's files, and a session that no longer exists is not going to be asked for
// them.
func TestForgettingASessionTakesWhatWasKeptToFetchItsFiles(t *testing.T) {
	t.Parallel()
	container := open(t)

	pair(t, container, "sid-1", "5511999990001")
	pair(t, container, "sid-2", "5511999990002")
	for _, sid := range []string{"sid-1", "sid-2"} {
		part := samplePart(sid, "3EB0KEEP")
		if err := container.PutMediaPart(t.Context(), &part, storedAt); err != nil {
			t.Fatalf("PutMediaPart: %v", err)
		}
	}

	if err := container.Forget(t.Context(), "sid-1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if _, found, _ := container.MediaPart(t.Context(), "sid-1", "3EB0KEEP"); found {
		t.Fatal("unpairing left the keys to the session's files behind")
	}
	if _, found, _ := container.MediaPart(t.Context(), "sid-2", "3EB0KEEP"); !found {
		t.Fatal("unpairing one session took another session's keys with it")
	}
}

// The window a delete cannot close: the old owner's write is already on its way when the
// session is unpaired here, so the delete finds nothing to take and the write lands
// afterwards as an insert. Left standing, that is a session that no longer exists holding
// the key to somebody's file until the retention sweep.
func TestAWriteThatOutlivesTheSessionItBelongsToIsRefused(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")

	part := samplePart("sid-1", "3EB0LATE")
	if err := container.PutMediaPart(t.Context(), &part, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	if err := container.Forget(t.Context(), "sid-1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	later := samplePart("sid-1", "3EB0LATE")
	if err := container.PutMediaPart(t.Context(), &later, storedAt.Add(time.Minute)); err == nil {
		t.Fatal("a write that arrived after its session was unpaired put the key to a file back")
	}
	if _, found, _ := container.MediaPart(t.Context(), "sid-1", "3EB0LATE"); found {
		t.Fatal("a session that no longer exists is holding the key to a file")
	}
}

// A row with no session or no message is one nothing can ever look up, and writing it
// would be a silent no-op somebody debugs later.
func TestAPartThatNamesNoMessageIsRefused(t *testing.T) {
	t.Parallel()
	container := open(t)

	for _, part := range []store.MediaPart{
		{SID: "", MessageID: "3EB0X"},
		{SID: "sid-1", MessageID: ""},
	} {
		if err := container.PutMediaPart(t.Context(), &part, storedAt); err == nil {
			t.Fatalf("a part with sid %q and message %q was written", part.SID, part.MessageID)
		}
	}
}
