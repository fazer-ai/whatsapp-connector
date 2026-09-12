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
		ChatKind: "phone", ChatID: "5511999990002",
		DirectPath:    "/v/t62.7118-24/file.enc",
		MediaKey:      bytes.Repeat([]byte{1}, 32),
		FileEncSHA256: bytes.Repeat([]byte{2}, 32),
		FileSHA256:    bytes.Repeat([]byte{3}, 32),
		FileLength:    111743, Mime: "image/jpeg", Filename: "recibo.pdf",
		BlobID: "blob_0123456789abcdef01234567",
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
	if err := container.For(want.SID).PutMediaPart(t.Context(), &want, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	got, found, err := container.For("sid-1").MediaPart(t.Context(), "3EB0IMAGE")
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
	if err := container.For(part.SID).PutMediaPart(t.Context(), &part, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	if _, found, err := container.For("sid-2").MediaPart(t.Context(), "3EB0SAME"); err != nil || found {
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
	if err := container.For(first.SID).PutMediaPart(t.Context(), &first, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	second := first
	second.DirectPath = "/v/t62.7118-24/refreshed.enc"
	later := storedAt.Add(time.Hour)
	if err := container.For(second.SID).PutMediaPart(t.Context(), &second, later); err != nil {
		t.Fatalf("PutMediaPart again: %v", err)
	}

	got, _, err := container.For("sid-1").MediaPart(t.Context(), "3EB0AGAIN")
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
	if err := container.For(fresh.SID).PutMediaPart(t.Context(), &fresh, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	// The old owner's write, made before the fresh one and landing after it.
	stale := samplePart("sid-1", "3EB0RACE")
	stale.DirectPath = "/v/t62.7118-24/stale.enc"
	if err := container.For(stale.SID).PutMediaPart(t.Context(), &stale, storedAt.Add(-time.Minute)); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	got, _, err := container.For("sid-1").MediaPart(t.Context(), "3EB0RACE")
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

// The stamp has millisecond resolution, so two writes inside one millisecond are not
// ordered by it. Refusing the second there discards a write nothing showed to be older --
// silently, with no error to read -- and the caller that loses most is the ordinary one:
// a row written and then corrected, where both calls land in the same millisecond and the
// correction is the half that disappears. The later call wins instead, which is what an
// upsert with no guard would do and the only tie-break this stamp supports.
func TestAWriteStampedTheSameMillisecondAsTheRowItReplacesWins(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")

	first := samplePart("sid-1", "3EB0TIE")
	first.DirectPath = "/v/t62.7118-24/first.enc"
	if err := container.For(first.SID).PutMediaPart(t.Context(), &first, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	// The correction, close enough behind it to share the stamp.
	corrected := samplePart("sid-1", "3EB0TIE")
	corrected.DirectPath = "/v/t62.7118-24/corrected.enc"
	corrected.ReceiptChat, corrected.Sender = "", ""
	if err := container.For(corrected.SID).PutMediaPart(t.Context(), &corrected, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	got, _, err := container.For("sid-1").MediaPart(t.Context(), "3EB0TIE")
	if err != nil {
		t.Fatalf("MediaPart: %v", err)
	}
	if got.DirectPath != corrected.DirectPath {
		t.Fatalf("the direct path is %q, want the correction's %q", got.DirectPath, corrected.DirectPath)
	}
	if got.ReceiptChat != "" || got.Sender != "" {
		t.Fatalf("the row still names %q/%q, want the correction to have cleared both", got.ReceiptChat, got.Sender)
	}
}

// A message nobody kept anything for is not an error: it is the ordinary answer for a
// text message, and for one whose retention has run out.
func TestAMessageNothingWasKeptForIsNotAnError(t *testing.T) {
	t.Parallel()
	container := open(t)

	part, found, err := container.For("sid-1").MediaPart(t.Context(), "3EB0NEVER")
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
	if err := container.For(old.SID).PutMediaPart(t.Context(), &old, storedAt.Add(-8*24*time.Hour)); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	if err := container.For(fresh.SID).PutMediaPart(t.Context(), &fresh, storedAt.Add(-time.Hour)); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	dropped, err := container.SweepMediaParts(t.Context(), storedAt.Add(-7*24*time.Hour))
	if err != nil {
		t.Fatalf("SweepMediaParts: %v", err)
	}
	if dropped != 1 {
		t.Fatalf("the sweep dropped %d rows, want only the one past its retention", dropped)
	}
	if _, found, _ := container.For("sid-1").MediaPart(t.Context(), "3EB0OLD"); found {
		t.Fatal("a row past its retention survived the sweep")
	}
	if _, found, _ := container.For("sid-1").MediaPart(t.Context(), "3EB0FRESH"); !found {
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
		if err := container.For(part.SID).PutMediaPart(t.Context(), &part, storedAt); err != nil {
			t.Fatalf("PutMediaPart: %v", err)
		}
	}

	if err := container.For("sid-1").Forget(t.Context()); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if _, found, _ := container.For("sid-1").MediaPart(t.Context(), "3EB0KEEP"); found {
		t.Fatal("unpairing left the keys to the session's files behind")
	}
	if _, found, _ := container.For("sid-2").MediaPart(t.Context(), "3EB0KEEP"); !found {
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
	if err := container.For(part.SID).PutMediaPart(t.Context(), &part, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	if err := container.For("sid-1").Forget(t.Context()); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	later := samplePart("sid-1", "3EB0LATE")
	if err := container.For(later.SID).PutMediaPart(t.Context(), &later, storedAt.Add(time.Minute)); err == nil {
		t.Fatal("a write that arrived after its session was unpaired put the key to a file back")
	}
	if _, found, _ := container.For("sid-1").MediaPart(t.Context(), "3EB0LATE"); found {
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
		if err := container.For(part.SID).PutMediaPart(t.Context(), &part, storedAt); err == nil {
			t.Fatalf("a part with sid %q and message %q was written", part.SID, part.MessageID)
		}
	}
}

// RememberBlob is what points a row at the file this instance downloaded, and the
// condition on it is the whole of it. The caller has been inside a download for as long
// as the media timeout allows, and an inbound handler can have replaced the row in that
// time -- a redelivery, or a sender reusing a message id in another chat, which puts a
// different file under the same key. Written on the key alone, one chat's file would be
// filed as another's, where the chat guard in front of a later download cannot catch it.
//
// The row is stamped ahead so the two behaviours separate without racing a clock.
func TestABlobIsOnlyRememberedOnTheRowTheCallerRead(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")

	read := samplePart("sid-1", "3EB0RACE")
	if err := scoped.PutMediaPart(t.Context(), &read, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	// And then the row changes hands while the download the caller started is still out.
	replaced := samplePart("sid-1", "3EB0RACE")
	replaced.ChatID = "5511888880002"
	replaced.BlobID = "blob_ffffffffffffffffffffffff"
	if err := scoped.PutMediaPart(t.Context(), &replaced, storedAt.Add(time.Second)); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	read.StoredAt = storedAt.UnixMilli()
	if err := scoped.RememberBlob(t.Context(), "blob_aaaaaaaaaaaaaaaaaaaaaaaa", &read); err != nil {
		t.Fatalf("RememberBlob: %v", err)
	}

	got, found, err := container.For("sid-1").MediaPart(t.Context(), "3EB0RACE")
	if err != nil || !found {
		t.Fatalf("MediaPart: %v (found %v)", err, found)
	}
	if got.BlobID != replaced.BlobID {
		t.Errorf("the row points at %q, want the file of the row that replaced it, %q", got.BlobID, replaced.BlobID)
	}
	if got.ChatID != replaced.ChatID {
		t.Errorf("the row is under chat %q, want %q", got.ChatID, replaced.ChatID)
	}
}

// The other half: on the row the caller did read, it writes -- and it writes that and
// nothing else. `stored_at` is what the retention sweep goes by, and which file is on
// this instance's disk is not a message arriving again.
func TestRememberingABlobLeavesTheRowAndItsRetentionAlone(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")

	want := samplePart("sid-1", "3EB0KEPT")
	if err := scoped.PutMediaPart(t.Context(), &want, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	read := want
	read.StoredAt = storedAt.UnixMilli()
	if err := scoped.RememberBlob(t.Context(), "blob_aaaaaaaaaaaaaaaaaaaaaaaa", &read); err != nil {
		t.Fatalf("RememberBlob: %v", err)
	}

	got, found, err := container.For("sid-1").MediaPart(t.Context(), "3EB0KEPT")
	if err != nil || !found {
		t.Fatalf("MediaPart: %v (found %v)", err, found)
	}
	want.BlobID = "blob_aaaaaaaaaaaaaaaaaaaaaaaa"
	want.StoredAt = storedAt.UnixMilli()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("what came back is\n %+v\nwant\n %+v", got, want)
	}
}

// An instance that has lost the account must not be writing to its rows, and a write-back
// arriving from one is the same hazard as any other: it lands after the new owner's.
func TestABlobRememberedByASessionThatWasHandedOnIsRefused(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")

	part := samplePart("sid-1", "3EB0LOST")
	if err := scoped.PutMediaPart(t.Context(), &part, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	read := part
	read.StoredAt = storedAt.UnixMilli()
	scoped.Drop()
	if err := scoped.RememberBlob(t.Context(), "blob_aaaaaaaaaaaaaaaaaaaaaaaa", &read); err == nil {
		t.Fatal("a session that no longer owns the account recorded a file against it")
	}

	got, _, err := container.For("sid-1").MediaPart(t.Context(), "3EB0LOST")
	if err != nil {
		t.Fatalf("MediaPart: %v", err)
	}
	if got.BlobID != part.BlobID {
		t.Errorf("the row points at %q, want the file it was written with, %q", got.BlobID, part.BlobID)
	}
}

// `stored_at` is not a row identity, and treating it as one is how this write lands on
// somebody else's row. putMediaPart admits a write stamped the same millisecond as the
// one it replaces -- on purpose, because a stamp of that resolution does not order two
// writes inside one millisecond at all -- so a replacement that arrives inside the
// millisecond the caller read is invisible to the stamp alone. What is left to tell them
// apart is what the row says, and the chat is the half that matters: a blob id landing on
// another chat's row puts one conversation's attachment where the chat guard in front of
// a later download cannot see it, because the row it checks is the other chat's row.
func TestABlobIsNotRememberedOnARowThatChangedChatsInTheSameMillisecond(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")

	read := samplePart("sid-1", "3EB0TIEBREAK")
	if err := scoped.PutMediaPart(t.Context(), &read, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	// The same millisecond, another chat, another file: the stamp cannot separate them.
	replaced := samplePart("sid-1", "3EB0TIEBREAK")
	replaced.ChatID = "5511888880002"
	replaced.BlobID = "blob_ffffffffffffffffffffffff"
	if err := scoped.PutMediaPart(t.Context(), &replaced, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	read.StoredAt = storedAt.UnixMilli()
	if err := scoped.RememberBlob(t.Context(), "blob_aaaaaaaaaaaaaaaaaaaaaaaa", &read); err != nil {
		t.Fatalf("RememberBlob: %v", err)
	}

	got, _, err := container.For("sid-1").MediaPart(t.Context(), "3EB0TIEBREAK")
	if err != nil {
		t.Fatalf("MediaPart: %v", err)
	}
	if got.BlobID != replaced.BlobID {
		t.Errorf("the row of chat %s points at %q, want %q", got.ChatID, got.BlobID, replaced.BlobID)
	}
}

// And the same for the file itself: a row whose coordinates were replaced describes a
// different file, whatever chat it is under, and the blob the caller downloaded is not
// that file.
func TestABlobIsNotRememberedOnARowThatNowDescribesAnotherFile(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")

	read := samplePart("sid-1", "3EB0OTHERFILE")
	if err := scoped.PutMediaPart(t.Context(), &read, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	replaced := samplePart("sid-1", "3EB0OTHERFILE")
	replaced.FileEncSHA256 = bytes.Repeat([]byte{9}, 32)
	replaced.BlobID = "blob_ffffffffffffffffffffffff"
	if err := scoped.PutMediaPart(t.Context(), &replaced, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	read.StoredAt = storedAt.UnixMilli()
	if err := scoped.RememberBlob(t.Context(), "blob_aaaaaaaaaaaaaaaaaaaaaaaa", &read); err != nil {
		t.Fatalf("RememberBlob: %v", err)
	}

	got, _, err := container.For("sid-1").MediaPart(t.Context(), "3EB0OTHERFILE")
	if err != nil {
		t.Fatalf("MediaPart: %v", err)
	}
	if got.BlobID != replaced.BlobID {
		t.Errorf("the row points at %q, want the file it now describes, %q", got.BlobID, replaced.BlobID)
	}
}
