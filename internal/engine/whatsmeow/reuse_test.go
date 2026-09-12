package whatsmeow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// Issue #24: a `message.download_media` for a message whose file is already on this
// instance's disk downloads it again and writes a second copy of the same bytes. What
// the tests here are about is the instance consulting its own store before it spends a
// download, and every way that consultation can be wrong.

// The cost the issue is about, counted. Measured at ef0e24f, one event plus two commands
// cost three downloads and three blobs of the same file -- 12288 bytes for a 4096-byte
// picture -- all three alive until the TTL drops them.
func TestTwoDownloadsOfOneMessagePayForTheFileOnce(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	session, downloads, _ := reuseSession(t, root, media.Options{})
	file := bytes.Repeat([]byte("x"), 4096)
	downloads.answer(file, nil)
	connect(session)

	emissions, acknowledged := deliver(t, session, imageEvent("3EB0COST"), 1)
	if !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}
	published := mediaContentOf(t, emissions[0]).Ref
	if published == nil {
		t.Fatal("a message whose file was downloaded was published with no reference")
	}

	first := refetch(t, session, "3EB0COST", nil)
	second := refetch(t, session, "3EB0COST", nil)

	if first.ID != published.ID || second.ID != published.ID {
		t.Errorf("the event published %s and the commands answered %s and %s, want one blob for the three",
			published.ID, first.ID, second.ID)
	}
	if first.URL != published.URL || second.URL != published.URL {
		t.Errorf("the commands published %q and %q, want the event's own %q", first.URL, second.URL, published.URL)
	}
	if downloads.count() != 1 {
		t.Errorf("the file was downloaded %d times, want once for the event and nothing for the two commands",
			downloads.count())
	}
	if blobs, held := countBlobs(t, root); blobs != 1 || held != int64(len(file)) {
		t.Errorf("the store holds %d blobs and %d bytes, want 1 and %d", blobs, held, len(file))
	}
}

// A message id belongs to whoever sent it, so the same one can arrive in two chats and
// the second row replaces the first. Handing the first chat's file back for the second
// chat's row is the worst thing this change can do: it is the leak the chat guard exists
// to stop, coming back in through the column the reuse reads.
func TestAMessageIdReusedInAnotherChatNeverServesTheFirstChatsFile(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	session, downloads, _ := reuseSession(t, root, media.Options{})
	inChatA := []byte("o arquivo do chat A")
	inChatB := []byte("o arquivo do chat B")
	connect(session)

	downloads.answer(inChatA, nil)
	if _, acknowledged := deliver(t, session, imageEvent("3EB0TWOCHATS"), 1); !acknowledged {
		t.Fatal("the first delivery was left unacknowledged")
	}
	downloads.answer(inChatB, nil)
	if _, acknowledged := deliver(t, session, inAnotherChat(imageEvent("3EB0TWOCHATS")), 1); !acknowledged {
		t.Fatal("the second delivery was left unacknowledged")
	}

	served := refetch(t, session, "3EB0TWOCHATS", &protocol.Address{Kind: protocol.AddressPhone, ID: otherChat})
	if got := servedBytes(t, session, served); !bytes.Equal(got, inChatB) {
		t.Errorf("the download served %q, want the file of the chat that asked for it, %q", got, inChatB)
	}
	if served.Size != int64(len(inChatB)) {
		t.Errorf("the reference says %d bytes and the chat's file is %d", served.Size, len(inChatB))
	}

	// The chat the row no longer belongs to is refused, and refusing costs nothing: no
	// download, and nothing written to the disk.
	blobsBefore, _ := countBlobs(t, root)
	spent := downloads.count()
	_, err := refetchErr(session, "3EB0TWOCHATS", &protocol.Address{Kind: protocol.AddressPhone, ID: "5511999990001"})
	assertCode(t, err, protocol.ErrorMediaUnavailable)
	if blobsAfter, _ := countBlobs(t, root); blobsAfter != blobsBefore || downloads.count() != spent {
		t.Errorf("a refused download went from %d blobs and %d downloads to %d and %d",
			blobsBefore, spent, blobsAfter, downloads.count())
	}

	// The contract makes the chat optional, and what is served without one has to be what
	// is served with the chat the row is under. Answering the older chat's file here
	// would be the same leak, reached by omission.
	blind := refetch(t, session, "3EB0TWOCHATS", nil)
	if got := servedBytes(t, session, blind); !bytes.Equal(got, inChatB) {
		t.Errorf("a download with no chat served %q, want what naming the chat serves, %q", got, inChatB)
	}
	if blind.ID != served.ID || blind.SHA256 != served.SHA256 {
		t.Errorf("a download with no chat answered %s (%s) and one naming the chat answered %s (%s)",
			blind.ID, blind.SHA256, served.ID, served.SHA256)
	}
}

// The window is reachable, and it is measured rather than argued: the session executor
// serialises commands against each other and the inbound pump runs beside it. A blob id
// written back on `(sid, message_id)` alone lands on whatever row is there when the
// download finishes, which is how one chat's file gets attached to another chat's row.
func TestABlobIsRememberedOnlyWhileTheRowIsTheOneTheDownloadRead(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	session, _, _ := reuseSession(t, root, media.Options{})
	inChatA := []byte("o arquivo do chat A")
	inChatB := []byte("o arquivo do chat B")

	// Deterministic by call number, because the order is: the first delivery, then the
	// command that is held, then the delivery that replaces its row.
	var mu sync.Mutex
	var calls int
	release := make(chan struct{})
	inside := make(chan struct{}, 4)
	session.download = func(_ context.Context, _ *wm.Client, _ wm.DownloadableMessage, file media.File) error {
		mu.Lock()
		calls++
		nth := calls
		mu.Unlock()
		inside <- struct{}{}
		if nth == 2 {
			<-release
		}
		body := inChatA
		if nth >= 3 {
			body = inChatB
		}
		_, err := file.Write(body)
		return err
	}
	connect(session)

	emissions, acknowledged := deliver(t, session, imageEvent("3EB0HELD"), 1)
	if !acknowledged {
		t.Fatal("the first delivery was left unacknowledged")
	}
	<-inside
	// The blob goes, so the command below has to download -- which is what puts it inside
	// the transfer, where the window is.
	removeBlob(t, root, mediaContentOf(t, emissions[0]).Ref.ID)

	answered := make(chan protocol.MediaRef, 1)
	go func() {
		ref, err := refetchErr(session, "3EB0HELD", nil)
		if err != nil {
			t.Errorf("the held download failed: %v", err)
		}
		answered <- ref
	}()
	<-inside

	// While it is held, the same id arrives in another chat and replaces the row.
	if _, acknowledged := deliver(t, session, inAnotherChat(imageEvent("3EB0HELD")), 1); !acknowledged {
		t.Fatal("the delivery that replaces the row was left unacknowledged")
	}
	<-inside
	replaced, found, err := session.store.MediaPart(t.Context(), "3EB0HELD")
	if err != nil || !found {
		t.Fatalf("MediaPart: %v (found %v)", err, found)
	}
	if replaced.ChatID != otherChat {
		t.Fatalf("the row is still under %s, so this is not the window it is meant to be", replaced.ChatID)
	}

	close(release)
	stale := <-answered

	kept, _, err := session.store.MediaPart(t.Context(), "3EB0HELD")
	if err != nil {
		t.Fatalf("MediaPart: %v", err)
	}
	if kept.BlobID == stale.ID {
		t.Errorf("the second chat's row now points at %s, which holds the first chat's file", stale.ID)
	}
	// And the observable the guard is there for: the next caller still gets its own file.
	next := refetch(t, session, "3EB0HELD", &protocol.Address{Kind: protocol.AddressPhone, ID: otherChat})
	if got := servedBytes(t, session, next); !bytes.Equal(got, inChatB) {
		t.Errorf("the download after the race served %q, want %q", got, inChatB)
	}
}

// The row carries the digest WhatsApp put on the message and the store carries the digest
// of the bytes it wrote, and for an honest message those are one value. Comparing them
// costs nothing and closes the whole family of "the id on this row is not this row's
// file" by construction instead of by getting an ordering right.
func TestARowAndABlobThatDisagreeAboutTheFileAreNotReused(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	session, downloads, _ := reuseSession(t, root, media.Options{})
	theirs := []byte("os bytes que a mensagem descreve")
	somebodyElses := []byte("os bytes de outro arquivo")
	connect(session)

	// A blob of the wrong file, under a row that names it while describing the right one.
	downloads.answer(somebodyElses, nil)
	if _, acknowledged := deliver(t, session, imageEvent("3EB0MISMATCH"), 1); !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}
	kept, found, err := session.store.MediaPart(t.Context(), "3EB0MISMATCH")
	if err != nil || !found {
		t.Fatalf("MediaPart: %v (found %v)", err, found)
	}
	digest := sha256.Sum256(theirs)
	kept.FileSHA256 = digest[:]
	if err := session.store.PutMediaPart(t.Context(), &kept, time.Now()); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	downloads.answer(theirs, nil)
	spent := downloads.count()
	ref := refetch(t, session, "3EB0MISMATCH", nil)
	if downloads.count() == spent {
		t.Error("a blob whose digest contradicts its row was handed back without a download")
	}
	if ref.SHA256 != hex.EncodeToString(digest[:]) {
		t.Errorf("the download answered %s, want the digest the message describes, %s",
			ref.SHA256, hex.EncodeToString(digest[:]))
	}
	if got := servedBytes(t, session, ref); !bytes.Equal(got, theirs) {
		t.Errorf("the download served %q, want %q", got, theirs)
	}
}

// Every way the blob behind a row can be gone. All three are ordinary -- the sweep runs
// on a tick, instances are replaced on every deploy -- and each has to cost one download
// and never a broken answer: `media_unavailable` is final on the client, which is what
// issue #19 is about.
func TestABlobThatIsNoLongerThereCostsADownloadAndNeverABrokenAnswer(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		gone func(t *testing.T, session *Session, root string, clock *movable, published protocol.MediaRef)
	}{
		{"swept", func(t *testing.T, session *Session, _ string, clock *movable, _ protocol.MediaRef) {
			t.Helper()
			clock.advance(2 * media.DefaultTTL)
			blobs, ok := session.blobs.(*media.Store)
			if !ok {
				t.Fatalf("the session keeps its blobs in a %T", session.blobs)
			}
			if _, _, err := blobs.Sweep(t.Context()); err != nil {
				t.Fatalf("Sweep: %v", err)
			}
		}},
		{"instance replaced", func(t *testing.T, session *Session, _ string, _ *movable, _ protocol.MediaRef) {
			t.Helper()
			replaceInstance(t, session)
		}},
		{"file removed under the store", func(t *testing.T, _ *Session, root string, _ *movable, published protocol.MediaRef) {
			t.Helper()
			removeBlob(t, root, published.ID)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			session, downloads, clock := reuseSession(t, root, media.Options{})
			file := []byte("os mesmos bytes")
			downloads.answer(file, nil)
			connect(session)

			emissions, acknowledged := deliver(t, session, imageEvent("3EB0GONE"), 1)
			if !acknowledged {
				t.Fatal("a media message with a file was left unacknowledged")
			}
			published := mediaContentOf(t, emissions[0]).Ref
			tc.gone(t, session, root, clock, *published)

			again := refetch(t, session, "3EB0GONE", nil)
			if again.ID == published.ID {
				t.Errorf("the download answered %s, which is the blob that is gone", again.ID)
			}
			if again.Size != published.Size || again.SHA256 != published.SHA256 {
				t.Errorf("the download served %d bytes (%s), want the same file as %d (%s)",
					again.Size, again.SHA256, published.Size, published.SHA256)
			}
			if !strings.HasPrefix(again.URL, session.blobBase+"/") {
				t.Errorf("the download published under %q, want the address of the instance answering, %q",
					again.URL, session.blobBase)
			}
			if got := servedBytes(t, session, again); !bytes.Equal(got, file) {
				t.Errorf("the download served %q, want %q", got, file)
			}
			if downloads.count() != 2 {
				t.Errorf("the file was downloaded %d times, want one for the event and one for the blob that went",
					downloads.count())
			}
		})
	}
}

// The two ways a blob's life is counted do not agree, and only a freshly written blob
// makes them look as if they do. The sweep drops on the modification time, which being
// handed out renews; `expires_at` is the description's `StoredAt` plus the TTL, which
// nothing renews. Measured at ef0e24f: a blob written at T0 and collected at T0+20h is
// still served at T0+30h with the expiry it was published under six hours in the past.
//
// A reference reused out of that gap is worse than today's second download. A client
// whose answer to a lapsed reference is to ask for the file again gets the same lapsed
// reference back, in a loop, on the path that exists to recover the attachment.
func TestAReusedReferenceNeverLapsesBeforeItIsAnswered(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	session, downloads, clock := reuseSession(t, root, media.Options{TTL: 24 * time.Hour})
	downloads.answer([]byte("os mesmos bytes"), nil)
	connect(session)

	emissions, acknowledged := deliver(t, session, imageEvent("3EB0LAPSED"), 1)
	if !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}
	published := mediaContentOf(t, emissions[0]).Ref

	// Twenty hours later a client collects it, which is what puts the two clocks out of
	// step: the sweep now keeps it for a day from here, and `StoredAt` has not moved.
	clock.advance(20 * time.Hour)
	blobs, ok := session.blobs.(*media.Store)
	if !ok {
		t.Fatalf("the session keeps its blobs in a %T", session.blobs)
	}
	body, _, err := blobs.Open(published.ID)
	if err != nil {
		t.Fatalf("the blob was not there to collect: %v", err)
	}
	_ = body.Close()

	// And ten hours after the expiry it was published under, it is still being served.
	clock.advance(10 * time.Hour)
	now := clock.now().UnixMilli()
	if published.ExpiresAt > now {
		t.Fatalf("the published expiry %d has not passed at %d, so this is not the gap it is meant to be",
			published.ExpiresAt, now)
	}

	ref := refetch(t, session, "3EB0LAPSED", nil)
	if ref.ExpiresAt <= now {
		t.Errorf("the download answered a reference that lapsed at %d, and it is %d", ref.ExpiresAt, now)
	}
	if got := servedBytes(t, session, ref); !bytes.Equal(got, []byte("os mesmos bytes")) {
		t.Errorf("the download served %q, want the file that is on the disk", got)
	}
}

// The download goes out over this session's own socket, so a session that is down fetches
// nothing. Reading a file this instance already has goes out over no socket at all, and
// refusing it would make a reconnection cost an attachment for no reason.
func TestAFileOnThisDiskIsServedWhileTheSessionIsDown(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	session, downloads, _ := reuseSession(t, root, media.Options{})
	file := []byte("os mesmos bytes")
	downloads.answer(file, nil)
	connect(session)

	emissions, acknowledged := deliver(t, session, imageEvent("3EB0DISCONNECTED"), 1)
	if !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}
	published := mediaContentOf(t, emissions[0]).Ref

	disconnect(session)
	ref := refetch(t, session, "3EB0DISCONNECTED", nil)
	if ref.ID != published.ID {
		t.Errorf("the download answered %s, want the blob that is on this disk, %s", ref.ID, published.ID)
	}
	if downloads.count() != 1 {
		t.Errorf("a session that is down spent %d downloads", downloads.count()-1)
	}
	if got := servedBytes(t, session, ref); !bytes.Equal(got, file) {
		t.Errorf("the download served %q, want %q", got, file)
	}
}

// What the quota counts is what a deployment's disk holds. Ten commands for one message
// used to be eleven copies of it; the floor this has to reach is one.
func TestAskingForOneFileTenTimesKeepsOneCopyOfIt(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	session, downloads, _ := reuseSession(t, root, media.Options{})
	file := bytes.Repeat([]byte("y"), 2048)
	downloads.answer(file, nil)
	connect(session)

	if _, acknowledged := deliver(t, session, imageEvent("3EB0TENTIMES"), 1); !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}
	for range 10 {
		refetch(t, session, "3EB0TENTIMES", nil)
	}

	blobs, held := countBlobs(t, root)
	if blobs != 1 || held != int64(len(file)) {
		t.Errorf("ten downloads left %d blobs and %d bytes, want 1 and %d", blobs, held, len(file))
	}
}

// Being handed out puts a blob at the back of the eviction queue, which is what the LRU
// is: what the sweep drops is what nobody collected. A blob that is reused is collected
// like any other, and it has to stay as droppable as any other when the quota is what is
// pressing rather than the age.
func TestAReusedBlobIsStillDroppedWhenTheQuotaIsWhatIsPressing(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	file := bytes.Repeat([]byte("z"), 4096)
	// Room for one of these files and not for two, so writing the next one has to cost
	// this one whatever the reuse did to its age. Counted in blocks, and the description
	// beside a blob costs one of its own.
	session, downloads, clock := reuseSession(t, root, media.Options{
		MaxBlob: int64(len(file)), Quota: 3 * media.DefaultBlockSize,
	})
	downloads.answer(file, nil)
	connect(session)

	if _, acknowledged := deliver(t, session, imageEvent("3EB0EVICTED"), 1); !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}
	reused := refetch(t, session, "3EB0EVICTED", nil)

	// Something newer arrives and the quota is over.
	clock.advance(time.Minute)
	if _, acknowledged := deliver(t, session, imageEvent("3EB0NEWER"), 1); !acknowledged {
		t.Fatal("the second media message was left unacknowledged")
	}
	blobs, ok := session.blobs.(*media.Store)
	if !ok {
		t.Fatalf("the session keeps its blobs in a %T", session.blobs)
	}
	if _, _, err := blobs.Sweep(t.Context()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if _, _, err := blobs.Open(reused.ID); err == nil {
		t.Error("the blob a download reused survived a quota sweep it was the oldest in")
	}
}

// `mediaBody` records the length it measured rather than the one the sender announced,
// so that a cap lowered afterwards stops the file before the download rather than after
// it. A reuse in front of the cap would quietly undo that for every file already on the
// disk, and the two paths would then disagree about the same file.
func TestALoweredCapStopsAFileWhicheverPathPutItOnTheDisk(t *testing.T) {
	t.Parallel()

	file := bytes.Repeat([]byte("w"), 8192)
	for _, tc := range []struct {
		name string
		// put leaves the file on the disk under the id it returns, by one path or the
		// other, and answers with the message it is filed under.
		put func(t *testing.T, session *Session, root string) string
	}{
		{"written on the way in", func(t *testing.T, session *Session, _ string) string {
			t.Helper()
			if _, acknowledged := deliver(t, session, imageEvent("3EB0CAPIN"), 1); !acknowledged {
				t.Fatal("a media message with a file was left unacknowledged")
			}
			return "3EB0CAPIN"
		}},
		{"written by an earlier download", func(t *testing.T, session *Session, root string) string {
			t.Helper()
			emissions, acknowledged := deliver(t, session, imageEvent("3EB0CAPOUT"), 1)
			if !acknowledged {
				t.Fatal("a media message with a file was left unacknowledged")
			}
			removeBlob(t, root, mediaContentOf(t, emissions[0]).Ref.ID)
			refetch(t, session, "3EB0CAPOUT", nil)
			return "3EB0CAPOUT"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			session, downloads, _ := reuseSession(t, root, media.Options{MaxBlob: int64(len(file)) * 2})
			downloads.answer(file, nil)
			connect(session)
			messageID := tc.put(t, session, root)

			// The operator lowers the cap under a file that is already here.
			lowered, err := media.New(media.Options{
				Root: root, MaxBlob: int64(len(file)) / 2, Now: func() time.Time { return storedAt },
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = lowered.Close() })
			session.blobs = lowered

			spent := downloads.count()
			_, err = refetchErr(session, messageID, nil)
			if err == nil {
				t.Fatalf("a file of %d bytes was served under a cap of %d", len(file), len(file)/2)
			}
			assertCode(t, err, protocol.ErrorMediaTooLarge)
			if downloads.count() != spent {
				t.Errorf("a file past the cap was downloaded %d times before being refused",
					downloads.count()-spent)
			}
		})
	}
}

// The row is shared and the blob is not: the id on it names a file on the disk of
// whichever instance wrote it. Publishing that id under this instance's address is the
// defect the issue itself uses to throw out its second proposal, and it comes straight
// back if the answer trusts the row instead of asking its own store.
func TestAnInstanceThatNeverHadTheFileNeverPublishesAnotherInstancesBlob(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	session, downloads, _ := reuseSession(t, root, media.Options{})
	file := []byte("os mesmos bytes")
	downloads.answer(file, nil)
	connect(session)

	emissions, acknowledged := deliver(t, session, imageEvent("3EB0HANDEDON"), 1)
	if !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}
	elsewhere := mediaContentOf(t, emissions[0]).Ref

	// The account moves, and the disk does not move with it.
	replaceInstance(t, session)
	ref := refetch(t, session, "3EB0HANDEDON", nil)
	if ref.ID == elsewhere.ID {
		t.Fatalf("the new instance answered %s, which is a file on the old one's disk", ref.ID)
	}
	if !strings.HasPrefix(ref.URL, session.blobBase+"/") {
		t.Errorf("the new instance published under %q, want its own address %q", ref.URL, session.blobBase)
	}

	// And it keeps what it downloaded, so its own next caller is answered from its disk.
	kept, _, err := session.store.MediaPart(t.Context(), "3EB0HANDEDON")
	if err != nil {
		t.Fatalf("MediaPart: %v", err)
	}
	if kept.BlobID != ref.ID {
		t.Errorf("the row points at %q and the instance answering wrote %q", kept.BlobID, ref.ID)
	}
	spent := downloads.count()
	if second := refetch(t, session, "3EB0HANDEDON", nil); second.ID != ref.ID {
		t.Errorf("the next download answered %s, want the blob this instance just wrote, %s", second.ID, ref.ID)
	}
	if downloads.count() != spent {
		t.Error("the instance that had just downloaded the file downloaded it again")
	}
}

// A row written before this column existed carries no blob id, and there is nothing to
// consult for it. It has to behave exactly as it did -- download -- and it has to come
// out of that download carrying the id, so the deployment's cost falls off as its
// messages are asked for rather than all at once or never.
func TestARowFromBeforeTheBlobWasRememberedStillDownloadsAndThenRemembers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	session, downloads, _ := reuseSession(t, root, media.Options{})
	file := []byte("os mesmos bytes")
	downloads.answer(file, nil)
	connect(session)

	if _, acknowledged := deliver(t, session, imageEvent("3EB0OLDROW"), 1); !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}
	// What such a row looks like: everything the download needs, and no blob id.
	kept, found, err := session.store.MediaPart(t.Context(), "3EB0OLDROW")
	if err != nil || !found {
		t.Fatalf("MediaPart: %v (found %v)", err, found)
	}
	kept.BlobID = ""
	if err := session.store.PutMediaPart(t.Context(), &kept, time.Now()); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	spent := downloads.count()
	first := refetch(t, session, "3EB0OLDROW", nil)
	if downloads.count() != spent+1 {
		t.Errorf("a row with nothing to consult spent %d downloads, want one", downloads.count()-spent)
	}
	remembered, _, err := session.store.MediaPart(t.Context(), "3EB0OLDROW")
	if err != nil {
		t.Fatalf("MediaPart: %v", err)
	}
	if remembered.BlobID != first.ID {
		t.Errorf("the row came out of the download pointing at %q, want %q", remembered.BlobID, first.ID)
	}
	if second := refetch(t, session, "3EB0OLDROW", nil); second.ID != first.ID || downloads.count() != spent+1 {
		t.Errorf("the download after it answered %s at a cost of %d downloads, want %s and none",
			second.ID, downloads.count()-spent-1, first.ID)
	}
}

// A reuse that only wants to know whether the blob is there is exactly the shape that
// forgets to close what it opened, and a connector holds its sessions for weeks.
func TestReusingABlobDoesNotLeaveADescriptorBehind(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	session, downloads, _ := reuseSession(t, root, media.Options{})
	downloads.answer([]byte("os mesmos bytes"), nil)
	connect(session)

	if _, acknowledged := deliver(t, session, imageEvent("3EB0DESCRIPTORS"), 1); !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}
	// One first, so whatever the path opens once and keeps is already open when the
	// count is taken.
	refetch(t, session, "3EB0DESCRIPTORS", nil)
	before := openDescriptors(t)
	for range 50 {
		refetch(t, session, "3EB0DESCRIPTORS", nil)
	}
	if after := openDescriptors(t); after > before+8 {
		t.Errorf("fifty reuses went from %d open descriptors to %d", before, after)
	}
}

// --- helpers ------------------------------------------------------------------------

// otherChat is the second chat a reused message id arrives in. It is a different party
// from the one `textMessage` builds, which is what makes the row change hands.
const otherChat = "5511888880002"

// reuseSession is a session whose media root the test names, so it can count and remove
// what is on the disk, and whose store clock it can move.
func reuseSession(t *testing.T, root string, opts media.Options) (*Session, *downloads, *movable) {
	t.Helper()

	session, _ := newTestSession(t, "5511999990001")
	clock := &movable{at: storedAt}
	opts.Root = root
	opts.Now = clock.now
	blobs, err := media.New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = blobs.Close() })

	handing := &downloads{}
	session.blobs = blobs
	session.blobBase = "http://connector-a1b2c3:8080"
	session.download = handing.hand
	return session, handing, clock
}

// movable is the store's clock as a test winds it. The store's own is a function, and a
// test that has to reach past a day of TTL cannot wait for one.
type movable struct {
	mu sync.Mutex
	at time.Time
}

func (m *movable) now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.at
}

func (m *movable) advance(by time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.at = m.at.Add(by)
}

// inAnotherChat is the same message from another party, which is what a sender reusing a
// message id looks like from here.
func inAnotherChat(event *waEvents.Message) *waEvents.Message {
	event.Info.Chat = waTypes.NewJID(otherChat, waTypes.DefaultUserServer)
	event.Info.Sender = event.Info.Chat
	event.Info.Timestamp = event.Info.Timestamp.Add(time.Second)
	return event
}

// servedBytes fetches a reference the way a client does, over the handler and against
// the token, rather than reading the file off the disk. What a client gets is the
// question, and the id and the URL are only half of it.
func servedBytes(t *testing.T, session *Session, ref protocol.MediaRef) []byte {
	t.Helper()

	blobs, ok := session.blobs.(*media.Store)
	if !ok {
		t.Fatalf("the session keeps its blobs in a %T", session.blobs)
	}
	const token = "reuse-check"
	mux := http.NewServeMux()
	mux.Handle("GET /media/{id}", media.Handler(media.HandlerOptions{Blobs: blobs, Token: token}))
	endpoint := httptest.NewServer(mux)
	defer endpoint.Close()

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, endpoint.URL+"/media/"+ref.ID, nil)
	if err != nil {
		t.Fatalf("build the request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("fetch %s: %v", ref.ID, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("fetching %s answered %d", ref.ID, response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read %s: %v", ref.ID, err)
	}
	return body
}

// countBlobs reports how many files the store holds and how many bytes of them, the
// descriptions beside them excluded.
func countBlobs(t *testing.T, root string) (blobs int, held int64) {
	t.Helper()

	err := filepath.WalkDir(root, func(_ string, entry fs.DirEntry, err error) error {
		switch {
		case err != nil:
			return err
		case entry.IsDir(), !strings.HasPrefix(entry.Name(), "blob_"), strings.HasSuffix(entry.Name(), ".json"):
			return nil
		}
		about, err := entry.Info()
		if err != nil {
			return err
		}
		blobs++
		held += about.Size()
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return blobs, held
}

// removeBlob takes a blob's bytes out from under the store, leaving its description and
// the row that names it. It is the third way a blob goes: a disk that was cleaned, a
// volume that was replaced.
func removeBlob(t *testing.T, root, id string) {
	t.Helper()

	var found bool
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		switch {
		case err != nil:
			return err
		case entry.IsDir(), entry.Name() != id:
			return nil
		}
		found = true
		return os.Remove(path)
	})
	if err != nil {
		t.Fatalf("remove %s from %s: %v", id, root, err)
	}
	if !found {
		t.Fatalf("%s was not under %s to remove", id, root)
	}
}

// openDescriptors counts what this process has open. Read off the filesystem rather than
// asked of the runtime, because a descriptor leaked by a path that forgot to close is
// not something the runtime knows it is holding.
func openDescriptors(t *testing.T) int {
	t.Helper()

	entries, err := os.ReadDir("/dev/fd")
	if err != nil {
		t.Skipf("this platform does not count descriptors at /dev/fd: %v", err)
	}
	return len(entries)
}
