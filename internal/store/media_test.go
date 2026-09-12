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
// condition on it is the whole of it. The caller read the row, went off to download a
// file on its coordinates for as long as the media timeout allows, and an inbound handler
// can have replaced the row in that time: a redelivery, or a sender reusing a message id,
// which puts a different file under the same key. Landing the write-back on the
// replacement files one message's file as another's, where the chat guard in front of a
// later download cannot catch it -- the row it checks is the replacement's, and it is the
// blob on it that is wrong.
//
// A table, because what the replacement changes is the point: the condition has to hold
// for a replacement that changes the chat, one that changes only the file, and one that
// changes neither in any way a comparison can see.
func TestABlobIsOnlyRememberedOnTheRowTheCallerRead(t *testing.T) {
	t.Parallel()

	// Media that is not encrypted: whatsmeow draws the line at a nil ciphertext digest
	// and drops the key to match, so both digest columns come back empty and every
	// comparison that would tell two of these apart compares nothing.
	digestless := func(sid, messageID string) store.MediaPart {
		part := samplePart(sid, messageID)
		part.MediaKey, part.FileEncSHA256, part.FileSHA256 = nil, nil, nil
		return part
	}

	for _, tc := range []struct {
		name string
		// read is the row the caller reads before its download; replacing is what lands
		// on top of it while the download is out, and at is when that write is stamped.
		read      func(sid, messageID string) store.MediaPart
		replacing func(part *store.MediaPart)
		at        time.Duration
	}{
		{
			name: "another chat, a second later",
			read: samplePart,
			replacing: func(part *store.MediaPart) {
				part.ChatID = "5511888880002"
			},
			at: time.Second,
		},
		{
			// The stamp is no help here: putMediaPart admits a write from inside the
			// same millisecond on purpose, because at that resolution the two are not
			// ordered at all.
			name: "another chat, inside the same millisecond",
			read: samplePart,
			replacing: func(part *store.MediaPart) {
				part.ChatID = "5511888880002"
			},
		},
		{
			name: "the same chat, another file",
			read: samplePart,
			replacing: func(part *store.MediaPart) {
				part.FileEncSHA256 = bytes.Repeat([]byte{9}, 32)
				part.DirectPath = "/v/another"
			},
			at: time.Second,
		},
		{
			// Nothing on this row distinguishes it from the one before it: the same
			// chat, both digests empty, and the same millisecond. It is also the shape
			// where nothing downstream can help, because the digest check on the way
			// back out has nothing to compare either.
			name: "the same chat, no digests either side, inside the same millisecond",
			read: digestless,
			replacing: func(part *store.MediaPart) {
				part.DirectPath = "/v/another"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			container := open(t)
			pair(t, container, "sid-1", "5511999990001")
			scoped := container.For("sid-1")

			first := tc.read("sid-1", "3EB0RACE")
			if err := scoped.PutMediaPart(t.Context(), &first, storedAt); err != nil {
				t.Fatalf("PutMediaPart: %v", err)
			}
			// As the caller holds it: read back, not filled in.
			read, found, err := scoped.MediaPart(t.Context(), "3EB0RACE")
			if err != nil || !found {
				t.Fatalf("MediaPart: %v (found %v)", err, found)
			}

			replaced := tc.read("sid-1", "3EB0RACE")
			replaced.BlobID = "blob_ffffffffffffffffffffffff"
			tc.replacing(&replaced)
			if err := scoped.PutMediaPart(t.Context(), &replaced, storedAt.Add(tc.at)); err != nil {
				t.Fatalf("PutMediaPart: %v", err)
			}

			if err := scoped.RememberBlob(t.Context(), "blob_aaaaaaaaaaaaaaaaaaaaaaaa", &read); err != nil {
				t.Fatalf("RememberBlob: %v", err)
			}

			got, _, err := container.For("sid-1").MediaPart(t.Context(), "3EB0RACE")
			if err != nil {
				t.Fatalf("MediaPart: %v", err)
			}
			if got.BlobID != replaced.BlobID {
				t.Errorf("the row points at %q, want the file of the row that replaced it, %q",
					got.BlobID, replaced.BlobID)
			}
			if got.ChatID != replaced.ChatID || got.DirectPath != replaced.DirectPath {
				t.Errorf("the row is chat %q at %q, want %q at %q",
					got.ChatID, got.DirectPath, replaced.ChatID, replaced.DirectPath)
			}
		})
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
	// As the caller holds it: read back, not filled in.
	read, found, err := scoped.MediaPart(t.Context(), "3EB0KEPT")
	if err != nil || !found {
		t.Fatalf("MediaPart: %v (found %v)", err, found)
	}
	if err := scoped.RememberBlob(t.Context(), "blob_aaaaaaaaaaaaaaaaaaaaaaaa", &read); err != nil {
		t.Fatalf("RememberBlob: %v", err)
	}

	got, found, err := container.For("sid-1").MediaPart(t.Context(), "3EB0KEPT")
	if err != nil || !found {
		t.Fatalf("MediaPart after the write-back: %v (found %v)", err, found)
	}
	want.BlobID = "blob_aaaaaaaaaaaaaaaaaaaaaaaa"
	want.StoredAt = storedAt.UnixMilli()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("what came back is\n %+v\nwant\n %+v", got, want)
	}
}

// A row that was swept and a row an inbound message wrote in its place are two different
// rows wearing the same key, and `rev` cannot tell them apart: it starts at nought, and
// both of these are fresh inserts. The retention sweep drops what is older than the
// deployment keeps, and a download can be in flight across that -- a client asking for the
// file of an old message at the moment the sweep reaches it -- so a write-back arriving
// afterwards would file its file on a message it knows nothing about, in whatever chat
// that message is in.
//
// What separates them is when each was written, and by the whole retention window, since
// being older than that window is the reason the first one was swept at all.
func TestABlobIsNotRememberedOnARowThatTookThePlaceOfASweptOne(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")

	// Old enough for the sweep to reach, and read by a caller that is about to download.
	old := samplePart("sid-1", "3EB0SWEPT")
	if err := scoped.PutMediaPart(t.Context(), &old, storedAt.Add(-30*24*time.Hour)); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	read, found, err := scoped.MediaPart(t.Context(), "3EB0SWEPT")
	if err != nil || !found {
		t.Fatalf("MediaPart: %v (found %v)", err, found)
	}

	dropped, err := container.SweepMediaParts(t.Context(), storedAt.Add(-7*24*time.Hour))
	if err != nil {
		t.Fatalf("SweepMediaParts: %v", err)
	}
	if dropped != 1 {
		t.Fatalf("the sweep dropped %d rows, so this is not the gap it is meant to be", dropped)
	}

	// And the same id arrives again, in another chat, while that download is still out.
	fresh := samplePart("sid-1", "3EB0SWEPT")
	fresh.ChatID = "5511888880002"
	fresh.BlobID = "blob_ffffffffffffffffffffffff"
	if err := scoped.PutMediaPart(t.Context(), &fresh, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	if read.Rev != 0 {
		t.Fatalf("the row the caller read is at rev %d, so this is not the collision it is meant to be", read.Rev)
	}

	if err := scoped.RememberBlob(t.Context(), "blob_aaaaaaaaaaaaaaaaaaaaaaaa", &read); err != nil {
		t.Fatalf("RememberBlob: %v", err)
	}

	got, _, err := container.For("sid-1").MediaPart(t.Context(), "3EB0SWEPT")
	if err != nil {
		t.Fatalf("MediaPart after the write-back: %v", err)
	}
	if got.BlobID != fresh.BlobID {
		t.Errorf("the row of chat %s points at %q, want %q", got.ChatID, got.BlobID, fresh.BlobID)
	}
}

// A path refreshed is not a row replaced, and the write-back that follows one has to
// land. `refetchReuploaded` rewrites `direct_path` on the row it read and then records
// the blob it fetched, so a row identity that moved when a path was refreshed would have
// this refuse its own caller -- and every file a sender's phone uploaded again would go
// unfiled, with every later download paying the 404 and the wait on the phone once more.
func TestRefreshingAPathDoesNotMakeTheRowAnotherRow(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")

	part := samplePart("sid-1", "3EB0REFRESHED")
	if err := scoped.PutMediaPart(t.Context(), &part, storedAt); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
	read, found, err := scoped.MediaPart(t.Context(), "3EB0REFRESHED")
	if err != nil || !found {
		t.Fatalf("MediaPart: %v (found %v)", err, found)
	}

	if err := scoped.RefreshDirectPath(t.Context(), "3EB0REFRESHED", "/v/fresh", read.StoredAt); err != nil {
		t.Fatalf("RefreshDirectPath: %v", err)
	}
	if err := scoped.RememberBlob(t.Context(), "blob_aaaaaaaaaaaaaaaaaaaaaaaa", &read); err != nil {
		t.Fatalf("RememberBlob: %v", err)
	}

	got, _, err := container.For("sid-1").MediaPart(t.Context(), "3EB0REFRESHED")
	if err != nil {
		t.Fatalf("MediaPart: %v", err)
	}
	if got.BlobID != "blob_aaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Errorf("the row points at %q, want the file the caller downloaded", got.BlobID)
	}
	if got.DirectPath != "/v/fresh" {
		t.Errorf("the row is fetched from %q, want the path the phone answered with", got.DirectPath)
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
	read, found, err := scoped.MediaPart(t.Context(), "3EB0LOST")
	if err != nil || !found {
		t.Fatalf("MediaPart: %v (found %v)", err, found)
	}

	scoped.Drop()
	if err := scoped.RememberBlob(t.Context(), "blob_aaaaaaaaaaaaaaaaaaaaaaaa", &read); err == nil {
		t.Fatal("a session that no longer owns the account recorded a file against it")
	}

	got, _, err := container.For("sid-1").MediaPart(t.Context(), "3EB0LOST")
	if err != nil {
		t.Fatalf("MediaPart after the refusal: %v", err)
	}
	if got.BlobID != part.BlobID {
		t.Errorf("the row points at %q, want the file it was written with, %q", got.BlobID, part.BlobID)
	}
}
