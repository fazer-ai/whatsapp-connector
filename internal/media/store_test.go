package media_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/media"
)

func newStore(t *testing.T, opts media.Options) (store *media.Store, root string) {
	t.Helper()
	if opts.Root == "" {
		opts.Root = t.TempDir()
	}
	store, err := media.New(opts)
	if err != nil {
		t.Fatalf("media.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, opts.Root
}

func put(t *testing.T, store *media.Store, body string, about *media.Blob) media.Blob {
	t.Helper()
	stored, err := store.Put(strings.NewReader(body), about)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	return stored
}

func read(t *testing.T, store *media.Store, id string) (string, media.Blob) {
	t.Helper()
	body, about, err := store.Open(id)
	if err != nil {
		t.Fatalf("Open %s: %v", id, err)
	}
	defer func() { _ = body.Close() }()
	raw, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read %s: %v", id, err)
	}
	return string(raw), about
}

// What goes in comes back, described as it was put in and measured by the store rather
// than by whoever handed it over: the size and the digest are what the client checks the
// download against, so taking them on trust from the caller would have them agree with
// each other and with nothing else.
func TestABlobComesBackAsItWentIn(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t, media.Options{})
	stored := put(t, store, "hello media", &media.Blob{Mime: "image/jpeg", Filename: "photo.jpg", Size: 999})

	if !strings.HasPrefix(stored.ID, "blob_") {
		t.Fatalf("the blob is named %q, which is not the shape the contract's refs carry", stored.ID)
	}
	if stored.Size != int64(len("hello media")) {
		t.Fatalf("the blob measures %d, want what was actually written", stored.Size)
	}
	sum := sha256.Sum256([]byte("hello media"))
	if stored.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("the digest is %s, want the one over the stored bytes", stored.SHA256)
	}

	body, about := read(t, store, stored.ID)
	if body != "hello media" {
		t.Fatalf("the blob reads back as %q", body)
	}
	if about.Mime != "image/jpeg" || about.Filename != "photo.jpg" {
		t.Fatalf("the description came back as %+v", about)
	}
}

// A blob that is not here is the ordinary case, not an exceptional one: the quota drops
// what nobody collected, and the client's answer is to ask the session for the media
// again. It has to be told apart from a store that is broken, which is not.
func TestAMissingBlobSaysSoRatherThanFailing(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t, media.Options{})
	if _, _, err := store.Open("blob_000102030405060708090a0b"); !errors.Is(err, media.ErrNotFound) {
		t.Fatalf("a blob that was never stored answered with %v, want ErrNotFound", err)
	}
}

// The id reaches this off an HTTP request, so anything that is not the shape the store
// issues is refused before it becomes a path. Otherwise a request can name a file
// outside the root and be handed whatever is there.
func TestAnIdThatIsNotOneIsRefusedRatherThanLookedUp(t *testing.T) {
	t.Parallel()

	store, root := newStore(t, media.Options{})
	secret := filepath.Join(filepath.Dir(root), "secret")
	if err := os.WriteFile(secret, []byte("not yours"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	for _, id := range []string{
		"../secret", "blob_../../secret", "", "blob_", "blob_zzzz", "secret",
		"blob_000102030405060708090a0b/../../secret",
	} {
		if _, _, err := store.Open(id); !errors.Is(err, media.ErrNotFound) {
			t.Fatalf("Open(%q) answered with %v, want ErrNotFound", id, err)
		}
	}
}

// The cap exists because the engine downloads into memory before it gets here, so a
// file past it is refused rather than half-kept. Refused at the byte after the cap, so a
// file exactly at it still goes in.
func TestABlobPastTheCapIsRefusedAndLeavesNothingBehind(t *testing.T) {
	t.Parallel()

	store, root := newStore(t, media.Options{MaxBlob: 8, Quota: 1 << 20})

	if _, err := store.Put(bytes.NewReader(bytes.Repeat([]byte("x"), 8)), &media.Blob{}); err != nil {
		t.Fatalf("a blob exactly at the cap was refused: %v", err)
	}
	_, err := store.Put(bytes.NewReader(bytes.Repeat([]byte("x"), 9)), &media.Blob{})
	if !errors.Is(err, media.ErrTooLarge) {
		t.Fatalf("a blob past the cap answered with %v, want ErrTooLarge", err)
	}

	var files int
	_ = filepath.WalkDir(root, func(_ string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			files++
		}
		return nil
	})
	// The one that fit, and its description.
	if files != 2 {
		t.Fatalf("the root holds %d files, want only the blob that fit and its description", files)
	}
}

// A cap larger than the whole quota is a store where one blob evicts everything else and
// then itself, which is a deployment that looks configured and caches nothing.
func TestAStoreThatCannotHoldOneBlobIsRefused(t *testing.T) {
	t.Parallel()

	if _, err := media.New(media.Options{Root: t.TempDir(), MaxBlob: 1 << 20, Quota: 1 << 10}); err == nil {
		t.Fatal("a store whose per-blob cap is larger than its whole quota was accepted")
	}
}

// Age is counted from the last time somebody came for it, not from when it arrived: a
// blob that is being read is one the client has not finished with.
func TestTheSweepDropsWhatNobodyCameFor(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	store, _ := newStore(t, media.Options{TTL: time.Hour, Now: clock})

	old := put(t, store, "forgotten", &media.Blob{})
	fresh := put(t, store, "collected", &media.Blob{})

	// Half an hour on, only one of them is asked for.
	now = now.Add(30 * time.Minute)
	read(t, store, fresh.ID)

	// And now the first one is past its hour while the second is half an hour into a
	// new one.
	now = now.Add(31 * time.Minute)
	dropped, freed, err := store.Sweep()
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if dropped != 1 || freed != int64(len("forgotten")) {
		t.Fatalf("the sweep dropped %d blobs and %d bytes, want the one nobody came for", dropped, freed)
	}
	if _, _, err := store.Open(old.ID); !errors.Is(err, media.ErrNotFound) {
		t.Fatalf("the blob nobody came for is still here: %v", err)
	}
	if body, _ := read(t, store, fresh.ID); body != "collected" {
		t.Fatalf("the blob that was collected reads back as %q", body)
	}
}

// Over quota, the least recently collected go first. Dropping the newest instead would
// throw away exactly the blob whose client is on its way.
func TestTheSweepDropsTheLeastRecentlyCollectedFirst(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	// Room for two of the three, so the sweep has to choose.
	store, _ := newStore(t, media.Options{TTL: time.Hour, Quota: 20, MaxBlob: 10, Now: clock})

	first := put(t, store, "0123456789", &media.Blob{})
	now = now.Add(time.Minute)
	second := put(t, store, "0123456789", &media.Blob{})
	now = now.Add(time.Minute)
	third := put(t, store, "0123456789", &media.Blob{})

	// The oldest is collected, which puts it at the back of the queue and leaves the
	// second as the one nobody has come for in longest.
	now = now.Add(time.Minute)
	read(t, store, first.ID)

	dropped, _, err := store.Sweep()
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if dropped != 1 {
		t.Fatalf("the sweep dropped %d blobs, want the one over quota", dropped)
	}
	if _, _, err := store.Open(second.ID); !errors.Is(err, media.ErrNotFound) {
		t.Fatalf("the sweep kept the blob nobody had come for in longest: %v", err)
	}
	for _, kept := range []string{first.ID, third.ID} {
		if _, _, err := store.Open(kept); err != nil {
			t.Fatalf("the sweep dropped %s, which was not the least recently collected: %v", kept, err)
		}
	}
}

// A crash mid-write leaves a temporary file named after nothing. Nobody can ask for it,
// so the sweep is the only thing that will ever collect it.
func TestTheSweepCollectsAnInterruptedWrite(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	store, root := newStore(t, media.Options{TTL: time.Hour, Now: clock})

	shard := filepath.Join(root, "de")
	if err := os.MkdirAll(shard, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	orphan := filepath.Join(shard, ".blob_deadbeef0011223344556677")
	if err := os.WriteFile(orphan, []byte("half a file"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	now = now.Add(2 * time.Hour)
	if _, _, err := store.Sweep(); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the interrupted write is still on disk: %v", err)
	}
}

// Every id starts `blob_`, so a shard taken off the front of the id is the same two
// characters for every blob: the sharding would be in the layout and nowhere on disk,
// and one directory would hold the whole cache.
func TestBlobsAreSpreadAcrossShards(t *testing.T) {
	t.Parallel()

	store, root := newStore(t, media.Options{})
	for range 64 {
		put(t, store, "x", &media.Blob{})
	}

	shards := map[string]struct{}{}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			shards[entry.Name()] = struct{}{}
		}
	}
	if len(shards) < 8 {
		t.Fatalf("64 blobs landed in %d directories (%v), which is one directory for the whole cache", len(shards), shards)
	}
	for name := range shards {
		if strings.HasPrefix(name, "bl") {
			t.Fatalf("the shard %q is the id's prefix rather than anything that varies", name)
		}
	}
}

// A crash between the description and the bytes, or between the two removals in a drop,
// leaves a description on its own. Nothing will ever ask for it and no blob will ever be
// dropped that takes it along, so the sweep is the only thing that can collect it.
func TestTheSweepCollectsADescriptionWithNoBlob(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	store, root := newStore(t, media.Options{TTL: time.Hour, Now: clock})
	kept := put(t, store, "still here", &media.Blob{})

	// The blob goes, its description stays, which is the state a crash between the two
	// removals leaves.
	orphanID := put(t, store, "gone", &media.Blob{}).ID
	blobPath := filepath.Join(root, orphanID[5:7], orphanID)
	if err := os.Remove(blobPath); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	orphanAbout := blobPath + ".json"
	if _, err := os.Stat(orphanAbout); err != nil {
		t.Fatalf("the description should still be here: %v", err)
	}

	// Not while it is new: a Put that has written the description and not yet named the
	// bytes looks exactly like this, and collecting it there loses a blob that is about
	// to exist.
	if _, _, err := store.Sweep(); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, err := os.Stat(orphanAbout); err != nil {
		t.Fatalf("the sweep took a description a Put may still be finishing: %v", err)
	}

	// Two hours on, with the blob that is describing something collected in between so
	// it is not simply aged out along with the orphan.
	now = now.Add(2 * time.Hour)
	read(t, store, kept.ID)
	if _, _, err := store.Sweep(); err != nil {
		t.Fatalf("the second Sweep: %v", err)
	}
	if _, err := os.Stat(orphanAbout); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a description with no blob survived the sweep: %v", err)
	}
	if _, _, err := store.Open(kept.ID); err != nil {
		t.Fatalf("the sweep took a description that was describing something: %v", err)
	}
}

// The root is a directory an operator points at, and one that turns out to hold
// something else is a misconfiguration to leave alone. A store that tidies a directory
// it was pointed at by mistake deletes somebody's files, and a name that is not a blob
// id is not one this can take apart into a path either.
func TestTheSweepLeavesFilesItDidNotWriteAlone(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	store, root := newStore(t, media.Options{TTL: time.Hour, Now: clock})

	foreign := []string{"notes.txt", ".hidden", "x", "blob_short", ".blob_short", "config.json"}
	for _, name := range foreign {
		if err := os.WriteFile(filepath.Join(root, name), []byte("somebody else's"), 0o600); err != nil {
			t.Fatalf("WriteFile %s: %v", name, err)
		}
	}

	now = now.Add(2 * time.Hour)
	if _, _, err := store.Sweep(); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	for _, name := range foreign {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("the sweep removed %s, which it did not write: %v", name, err)
		}
	}
}
