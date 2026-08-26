package media_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
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
	stored, err := store.Put(t.Context(), strings.NewReader(body), about)
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

	if _, err := store.Put(t.Context(), bytes.NewReader(bytes.Repeat([]byte("x"), 8)), &media.Blob{}); err != nil {
		t.Fatalf("a blob exactly at the cap was refused: %v", err)
	}
	_, err := store.Put(t.Context(), bytes.NewReader(bytes.Repeat([]byte("x"), 9)), &media.Blob{})
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
	dropped, freed, err := store.Sweep(t.Context())
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
	// Room for two of the three, so the sweep has to choose. A blob costs a block for
	// its bytes and a block for its description however short it is, so the quota is
	// written in those rather than in the lengths.
	const oneBlob = 8 << 10
	store, _ := newStore(t, media.Options{TTL: time.Hour, Quota: 2 * oneBlob, MaxBlob: 4 << 10, Now: clock})

	first := put(t, store, "0123456789", &media.Blob{})
	now = now.Add(time.Minute)
	second := put(t, store, "0123456789", &media.Blob{})
	now = now.Add(time.Minute)
	third := put(t, store, "0123456789", &media.Blob{})

	// The oldest is collected, which puts it at the back of the queue and leaves the
	// second as the one nobody has come for in longest.
	now = now.Add(time.Minute)
	read(t, store, first.ID)

	dropped, _, err := store.Sweep(t.Context())
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

	// No time is allowed to pass. The in-flight set is the whole answer — only this
	// process writes to this root, so a temporary file it is not writing is one a crash
	// or a restart left — and waiting a day would leave up to one MaxBlob of each of
	// them on the disk while every sweep reported the cache within budget.
	if _, _, err := store.Sweep(t.Context()); err != nil {
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

	// No time is allowed to pass: what tells a description a Put is still finishing from
	// one a crash left behind is the in-flight set, not the clock.
	if _, _, err := store.Sweep(t.Context()); err != nil {
		t.Fatalf("Sweep: %v", err)
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
	if _, _, err := store.Sweep(t.Context()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	for _, name := range foreign {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("the sweep removed %s, which it did not write: %v", name, err)
		}
	}
}

// A volume that has gone read-only refuses every removal. Counting those as freed would
// have the sweep report a cache it had emptied while the disk stayed full, every minute,
// with nothing said.
func TestASweepThatCannotDropSaysSoRatherThanCountingIt(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	store, root := newStore(t, media.Options{TTL: time.Hour, Now: clock})
	stored := put(t, store, "cannot go", &media.Blob{})

	// The shard is made unwritable, which is what a read-only volume looks like to one
	// removal.
	shard := filepath.Join(root, stored.ID[5:7])
	if err := os.Chmod(shard, 0o500); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(shard, 0o700) })

	now = now.Add(2 * time.Hour)
	dropped, freed, err := store.Sweep(t.Context())
	if err == nil {
		t.Fatal("a sweep that could not remove anything reported success")
	}
	if dropped != 0 || freed != 0 {
		t.Fatalf("the sweep counted %d blobs and %d bytes it did not actually free", dropped, freed)
	}
}

// A shutdown must not wait out a walk of a large cache on a slow disk, and what the pass
// has found by then is not worth acting on: the process is stopping either way.
func TestASweepStopsWhenTheProcessIs(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	store, _ := newStore(t, media.Options{TTL: time.Hour, Now: clock})
	stored := put(t, store, "would have gone", &media.Blob{})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	now = now.Add(2 * time.Hour)
	if _, _, err := store.Sweep(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("a sweep asked to stop answered with %v, want the cancellation", err)
	}
	if _, _, err := store.Open(stored.ID); err != nil {
		t.Fatalf("a sweep that was asked to stop dropped a blob anyway: %v", err)
	}
}

// A file with a blob's name somewhere else in the tree is not that blob. Dropping goes
// by the id, so the sweep would remove whatever sits at the canonical path — nothing, or
// the real blob of the same name — and count this one's bytes as freed either way.
func TestABlobIsOnlyItselfInItsOwnShard(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	store, root := newStore(t, media.Options{TTL: time.Hour, Now: clock})
	genuine := put(t, store, "the real one", &media.Blob{})

	// The same name, in the wrong place.
	impostor := filepath.Join(root, "zz", genuine.ID)
	if err := os.MkdirAll(filepath.Dir(impostor), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(impostor, []byte("not the real one"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// The real blob is collected, so only the impostor is old enough to tempt the sweep.
	now = now.Add(2 * time.Hour)
	read(t, store, genuine.ID)
	if _, _, err := store.Sweep(t.Context()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if _, _, err := store.Open(genuine.ID); err != nil {
		t.Fatalf("the sweep dropped the real blob on account of a file elsewhere: %v", err)
	}
	if _, err := os.Stat(impostor); err != nil {
		t.Fatalf("the sweep removed a file that is not in the layout it writes: %v", err)
	}
}

// A cap at the top of the range makes a one-byte lookahead wrap: the limit goes
// negative, the reader answers EOF straight away, and every file is stored empty.
func TestAnEnormousCapDoesNotStoreEveryBlobEmpty(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t, media.Options{MaxBlob: math.MaxInt64, Quota: math.MaxInt64})
	stored := put(t, store, "the bytes are here", &media.Blob{})

	if stored.Size != int64(len("the bytes are here")) {
		t.Fatalf("the blob measures %d, want what was written", stored.Size)
	}
	if body, _ := read(t, store, stored.ID); body != "the bytes are here" {
		t.Fatalf("the blob reads back as %q", body)
	}
}

// The store holds a directory handle for the life of the process. Closing it is what
// releases the descriptor, and a store that has been closed refuses rather than reaching
// through a handle that is gone.
func TestAClosedStoreLetsGoOfTheDirectory(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t, media.Options{})
	stored := put(t, store, "the bytes", &media.Blob{})

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := store.Open(stored.ID); err == nil {
		t.Fatal("a closed store served a blob through a handle it had let go of")
	}
	if _, _, err := store.Sweep(t.Context()); err == nil {
		t.Fatal("a closed store swept through a handle it had let go of")
	}
}

// The quota is a disk budget, and a filesystem hands out blocks rather than bytes. A
// cache of many tiny files measured by the sum of their lengths reads as almost nothing
// while the volume it sits on is much fuller, and the eviction that should have started
// never does.
func TestTinyBlobsAreChargedWhatTheyCostOnDisk(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	// Eight kibibytes: room for one blob and its description, and no more.
	store, _ := newStore(t, media.Options{TTL: time.Hour, Quota: 8 << 10, MaxBlob: 4 << 10, Now: clock})

	first := put(t, store, "x", &media.Blob{})
	now = now.Add(time.Minute)
	second := put(t, store, "y", &media.Blob{})

	// Two one-byte files are two bytes by their lengths and two whole blocks plus two
	// descriptions on the disk.
	dropped, _, err := store.Sweep(t.Context())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if dropped != 1 {
		t.Fatalf("the sweep dropped %d blobs, want the one over quota: two one-byte files are not two bytes of disk", dropped)
	}
	if _, _, err := store.Open(first.ID); !errors.Is(err, media.ErrNotFound) {
		t.Fatalf("the sweep kept the older of the two: %v", err)
	}
	if _, _, err := store.Open(second.ID); err != nil {
		t.Fatalf("the sweep dropped the newer of the two: %v", err)
	}
}

// The snapshot the sweep works from is minutes old by the time it evicts, and Open
// touches a blob it hands out. A blob collected in between is one somebody is using, and
// dropping it on the stale time turns a HEAD that answered 200 into a GET that answers
// 404.
func TestABlobCollectedSinceTheWalkIsNotEvicted(t *testing.T) {
	t.Parallel()

	var (
		now   = time.Now()
		root  string
		blob  string
		reads int
	)
	clock := func() time.Time {
		reads++
		// The sweep reads the clock once, for the cutoff, and it does that after the
		// walk has taken its snapshot: this is the window between the snapshot and the
		// eviction, which is where a GET lands in production and the only place a test
		// can put one without a hook that exists for tests.
		if reads == 1 && blob != "" {
			touched := now.Add(time.Second)
			if err := os.Chtimes(filepath.Join(root, blob[5:7], blob), touched, touched); err != nil {
				t.Errorf("Chtimes: %v", err)
			}
		}
		return now
	}
	store, dir := newStore(t, media.Options{TTL: time.Hour, Now: clock})
	root = dir
	stored := put(t, store, "in use", &media.Blob{})

	// Past the TTL, so the walk sees it as aged out and the sweep sets out to drop it.
	now = now.Add(2 * time.Hour)
	reads, blob = 0, stored.ID

	if _, _, err := store.Sweep(t.Context()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, _, err := store.Open(stored.ID); err != nil {
		t.Fatalf("a blob collected between the walk and the eviction was dropped anyway: %v", err)
	}
}

// An interrupted write that will not go is up to a whole blob of disk that no accounting
// covers: it is not a blob, so nothing charges it against the quota, and a sweep that
// swallowed the failure reports a clean pass every minute while it sits there.
func TestASweepThatCannotCollectALeftoverSaysSo(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	store, root := newStore(t, media.Options{TTL: time.Hour, Now: clock})

	shard := filepath.Join(root, "de")
	if err := os.MkdirAll(shard, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(shard, ".blob_deadbeef0011223344556677"), []byte("half"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Chmod(shard, 0o500); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(shard, 0o700) })

	now = now.Add(2 * time.Hour)
	if _, _, err := store.Sweep(t.Context()); err == nil {
		t.Fatal("a sweep that could not collect an interrupted write reported a clean pass")
	}
}

// A leftover is only a leftover in the place this store would have written it. A root
// pointed at a shared directory has subdirectories of its own, and a name that looks
// like one of this store's temporary files or descriptions somewhere else in that tree
// belongs to whoever put it there.
func TestLeftoversAreOnlyCollectedWhereTheyWouldHaveBeenWritten(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	store, root := newStore(t, media.Options{TTL: time.Hour, Now: clock})

	const id = "blob_deadbeef0011223344556677"
	elsewhere := filepath.Join(root, "somebody-elses-cache")
	if err := os.MkdirAll(elsewhere, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	foreign := []string{
		filepath.Join(elsewhere, "."+id),
		filepath.Join(elsewhere, id+".json"),
		// The right shard name, the wrong depth.
		filepath.Join(elsewhere, "."+id+".json"),
	}
	for _, path := range foreign {
		if err := os.WriteFile(path, []byte("not this store's"), 0o600); err != nil {
			t.Fatalf("WriteFile %s: %v", path, err)
		}
	}

	now = now.Add(2 * time.Hour)
	if _, _, err := store.Sweep(t.Context()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	for _, path := range foreign {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("the sweep removed %s, which is not where it writes: %v", path, err)
		}
	}
}

// A shard that cannot be read takes every blob in it out of the accounting, so the quota
// is measured against a fraction of the disk. Silently, and for as long as the
// permissions stay that way.
func TestAnUnreadableShardIsReported(t *testing.T) {
	t.Parallel()

	store, root := newStore(t, media.Options{TTL: time.Hour})
	stored := put(t, store, "in an unreadable shard", &media.Blob{})

	shard := filepath.Join(root, stored.ID[5:7])
	if err := os.Chmod(shard, 0o000); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(shard, 0o700) })

	if _, _, err := store.Sweep(t.Context()); err == nil {
		t.Fatal("a sweep that could not read a shard reported a clean pass")
	}
}

// The description carries a filename off a message somebody else wrote, so it is not a
// fixed cost. One large enough to matter sits entirely outside a quota that assumes a
// block for it.
func TestALargeDescriptionIsChargedForWhatItTakes(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	// Room for one small blob and one small description, twice over.
	store, _ := newStore(t, media.Options{TTL: time.Hour, Quota: 16 << 10, MaxBlob: 4 << 10, Now: clock})

	small := put(t, store, "x", &media.Blob{})
	now = now.Add(time.Minute)
	// A filename of six kibibytes, so the description takes two blocks rather than the
	// one a fixed charge assumes. The pair then costs 20 KiB against a 16 KiB quota;
	// charged a block each they would come to exactly 16 and nothing would be dropped.
	put(t, store, "y", &media.Blob{Filename: strings.Repeat("n", 6<<10)})

	dropped, _, err := store.Sweep(t.Context())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if dropped != 1 {
		t.Fatalf("the sweep dropped %d blobs: a description is being charged a block whatever it takes", dropped)
	}
	if _, _, err := store.Open(small.ID); !errors.Is(err, media.ErrNotFound) {
		t.Fatalf("the sweep kept the older blob: %v", err)
	}
}

// A reader may answer no bytes and no error, which means nothing happened rather than
// there is nothing left. Taking one of those for the end of the file renames the first
// MaxBlob bytes and reports a whole one.
func TestASourceThatPausesIsNotMistakenForOneThatEnded(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t, media.Options{MaxBlob: 4, Quota: 1 << 20})

	// Four bytes, then one pause, then more: exactly the shape that reads as an exact
	// fit to a single lookahead.
	source := io.MultiReader(strings.NewReader("0123"), &pause{}, strings.NewReader("456"))
	if _, err := store.Put(t.Context(), source, &media.Blob{}); !errors.Is(err, media.ErrTooLarge) {
		t.Fatalf("a source with more bytes after a pause answered with %v, want ErrTooLarge", err)
	}
}

// pause answers once with nothing at all, which io.Reader permits and which means
// nothing happened.
type pause struct{ done bool }

func (p *pause) Read([]byte) (int, error) {
	if p.done {
		return 0, io.EOF
	}
	p.done = true
	return 0, nil
}

// The source is a download off somebody else's network, so it can stall for as long as
// that network feels like it. The session that owns the write has to be able to let go
// of it when its lease moves or the process stops.
func TestAStalledWriteGivesUpWithTheContext(t *testing.T) {
	t.Parallel()

	store, _ := newStore(t, media.Options{})
	ctx, cancel := context.WithCancel(t.Context())

	// Answers a little and then never again, which is a socket that has gone quiet.
	stalled := io.MultiReader(strings.NewReader("the first bytes"), blockUntil(ctx.Done()))
	done := make(chan error, 1)
	go func() {
		_, err := store.Put(ctx, stalled, &media.Blob{})
		done <- err
	}()

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("a stalled write answered with %v, want the cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a stalled write did not come back: the session cannot let go of it")
	}
}

// blockUntil is a reader that answers nothing until the channel closes, then reports the
// end of the file. It is a socket waiting on bytes that are not coming.
type blockUntil <-chan struct{}

func (b blockUntil) Read([]byte) (int, error) {
	<-b
	return 0, io.EOF
}

// The sweep subtracts the whole cost of an entry, and the description is part of that. A
// description left behind after its bytes went is disk the accounting has stopped
// counting, which is how the cache sits over quota with nothing said.
func TestADescriptionThatWillNotGoIsReported(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	store, root := newStore(t, media.Options{TTL: time.Hour, Now: clock})
	stored := put(t, store, "the bytes", &media.Blob{})

	// The description is replaced by a directory with something in it, which is a file
	// the blob beside it can be removed without and this one cannot: a removal that
	// fails for the description alone, which permissions on the shard cannot produce
	// because they stop both.
	about := filepath.Join(root, stored.ID[5:7], stored.ID+".json")
	if err := os.Remove(about); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(about, "in the way"), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	now = now.Add(2 * time.Hour)
	if _, _, err := store.Sweep(t.Context()); err == nil {
		t.Fatal("a sweep that could not remove a description reported a clean pass")
	}
	if _, err := os.Stat(about); err != nil {
		t.Fatalf("the description went after all, so the test proves nothing: %v", err)
	}
}

// A download can take longer than the TTL. Its temporary file then reads to the sweep as
// one an interrupted write abandoned, and collecting it unlinks a file that is still
// being written into: the copy carries on into an inode with no name and the rename at
// the end fails on a blob that was on its way.
func TestAWriteInFlightIsNotCollectedForBeingSlow(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	// A minute of TTL against a write that takes longer than that.
	store, _ := newStore(t, media.Options{TTL: time.Minute, Now: clock})

	arrived := make(chan struct{})
	release := make(chan struct{})
	slow := io.MultiReader(strings.NewReader("the first bytes"), &announce{arrived: arrived, wait: release})

	done := make(chan error, 1)
	go func() {
		_, err := store.Put(t.Context(), slow, &media.Blob{})
		done <- err
	}()

	<-arrived
	// Well past the TTL, with the write still going.
	now = now.Add(time.Hour)
	if _, _, err := store.Sweep(t.Context()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	close(release)

	if err := <-done; err != nil {
		t.Fatalf("a write that outlasted the TTL was collected from under itself: %v", err)
	}
}

// announce says when it has been reached and then waits to be let go.
type announce struct {
	arrived chan struct{}
	wait    chan struct{}
	done    bool
}

func (a *announce) Read([]byte) (int, error) {
	if !a.done {
		a.done = true
		close(a.arrived)
		<-a.wait
	}
	return 0, io.EOF
}

// The age is how long since anybody wanted a blob. Left with the time its temporary file
// picked up when the first byte landed, a download that took longer than the TTL arrives
// already expired and the next sweep drops it before anyone can collect it.
func TestABlobIsAsOldAsItsArrivalRatherThanItsFirstByte(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	store, _ := newStore(t, media.Options{TTL: time.Minute, Now: clock})

	arrived := make(chan struct{})
	release := make(chan struct{})
	slow := io.MultiReader(strings.NewReader("the first bytes"), &announce{arrived: arrived, wait: release})

	stored := make(chan media.Blob, 1)
	go func() {
		blob, err := store.Put(t.Context(), slow, &media.Blob{})
		if err != nil {
			t.Errorf("Put: %v", err)
		}
		stored <- blob
	}()

	<-arrived
	now = now.Add(time.Hour)
	close(release)
	blob := <-stored

	if _, _, err := store.Sweep(t.Context()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, _, err := store.Open(blob.ID); err != nil {
		t.Fatalf("a blob whose download outlasted the TTL was dropped on arrival: %v", err)
	}
}

// The bytes going and the description staying leaves the payload's disk free. Charging
// the whole entry anyway has the sweep evict other blobs to make room for something that
// is not there any more.
func TestBytesThatWentAreNotStillChargedForWhenTheDescriptionStays(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	// Twenty-two kibibytes: the two that matter cost sixteen, and the stuck one costs
	// four once its bytes are accounted as gone and twelve if they are not. Only the
	// second reading puts the cache over the quota and starts evicting.
	store, root := newStore(t, media.Options{TTL: time.Hour, Quota: 22 << 10, MaxBlob: 4 << 10, Now: clock})

	stuck := put(t, store, "will half go", &media.Blob{})
	// The description is replaced by a directory with something in it, so the bytes can
	// go and it cannot.
	about := filepath.Join(root, stuck.ID[5:7], stuck.ID+".json")
	if err := os.Remove(about); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(about, "in the way"), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	// Two more, well within the quota once the stuck one's bytes are accounted as gone.
	now = now.Add(time.Minute)
	wanted := []string{put(t, store, "keep me", &media.Blob{}).ID, put(t, store, "and me", &media.Blob{}).ID}

	// Only the first is past the TTL.
	now = now.Add(90 * time.Minute)
	for _, id := range wanted {
		read(t, store, id)
	}
	if _, _, err := store.Sweep(t.Context()); err == nil {
		t.Fatal("a sweep that could not remove a description reported a clean pass")
	}

	for _, id := range wanted {
		if _, _, err := store.Open(id); err != nil {
			t.Fatalf("%s was evicted to make room for bytes that had already gone: %v", id, err)
		}
	}
}

// A file is never both canonical and unstamped. Until it is stamped it carries whatever
// time the temporary file had, which for a slow download is old enough to evict, so a
// stamp landing after the rename leaves a window in which a sweep deletes a blob the
// caller is about to be handed. Stamping before the rename closes it: what appears under
// the canonical name is already carrying the store's own clock.
func TestABlobIsStampedBeforeItIsNamed(t *testing.T) {
	t.Parallel()

	// Nowhere near the wall clock, so a file left with the time its bytes were written
	// cannot pass for one the store stamped.
	committed := time.Unix(1_700_000_000, 0)
	store, root := newStore(t, media.Options{Now: func() time.Time { return committed }})

	stored, err := store.Put(t.Context(), strings.NewReader("arriving"), &media.Blob{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	about, err := os.Stat(filepath.Join(root, stored.ID[5:7], stored.ID))
	if err != nil {
		t.Fatalf("stat the blob: %v", err)
	}
	if !about.ModTime().Equal(committed) {
		t.Fatalf("the named blob carries %s, want the store's own clock %s", about.ModTime(), committed)
	}
	// And the description agrees with it, because a caller publishing an expiry reads
	// the description and a sweep reads the file.
	if stored.StoredAt != committed.UnixMilli() {
		t.Fatalf("the description says the blob was stored at %d and the file says %d",
			stored.StoredAt, committed.UnixMilli())
	}
}

// A write that cannot be committed hands back nothing and leaves nothing behind.
//
// The description is written before the bytes are stamped and named, so a failure past
// that point has already put a file on disk. Left there it is an orphan: disk the
// accounting counts against the quota with no blob under it, which is how a cache sits
// over its limit with nothing to show for it.
func TestAWriteThatCannotBeCommittedLeavesNoDescriptionBehind(t *testing.T) {
	t.Parallel()

	var (
		now   = time.Now()
		root  string
		reads int
	)
	// Put reads the clock once, after the bytes are all in and before it describes,
	// stamps and names them. The arriving file is taken away there, which is what the
	// commit then fails on.
	clock := func() time.Time {
		reads++
		if reads == 1 {
			removeArriving(t, root)
		}
		return now
	}
	store, dir := newStore(t, media.Options{Now: clock})
	root = dir

	stored, err := store.Put(t.Context(), strings.NewReader("arriving"), &media.Blob{})
	if err == nil {
		t.Fatalf("a blob that could not be committed was published as %s", stored.ID)
	}
	if stored.ID != "" {
		t.Fatalf("a failed write handed back an id: %+v", stored)
	}
	if left := descriptions(t, root); len(left) != 0 {
		t.Fatalf("a failed write left %v behind, which the quota goes on counting", left)
	}
}

// descriptions is every blob description under root, which after a failed write should
// be none.
func descriptions(t *testing.T, root string) []string {
	t.Helper()

	var found []string
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			return nil //nolint:nilerr // best effort in a test helper
		}
		found = append(found, path)
		return nil
	})
	return found
}

// removeArriving takes away every blob whose bytes are still arriving, which is what the
// stamp above fails on.
func removeArriving(t *testing.T, root string) {
	t.Helper()
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasPrefix(entry.Name(), ".blob_") {
			return nil //nolint:nilerr // best effort in a test helper
		}
		_ = os.Remove(path)
		return nil
	})
}

// A volume formatted with larger allocation units than the default undercharges every
// file by the difference, so a cache of tiny blobs consumes much more than the quota
// while the sweep calculates a total below it and evicts nothing.
func TestTheQuotaIsCountedInTheVolumesAllocationUnit(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	// Sixty-four kibibytes to a unit, so one blob and its description take 128 KiB and
	// two of them do not fit in the 160 KiB budget. Counted in the four-kibibyte
	// default the pair would come to 16 KiB and nothing would be dropped.
	store, _ := newStore(t, media.Options{
		TTL: time.Hour, Quota: 160 << 10, MaxBlob: 32 << 10, BlockSize: 64 << 10, Now: clock,
	})

	first := put(t, store, "x", &media.Blob{})
	now = now.Add(time.Minute)
	second := put(t, store, "y", &media.Blob{})

	dropped, _, err := store.Sweep(t.Context())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if dropped != 1 {
		t.Fatalf("the sweep dropped %d blobs, want the one over quota: the volume hands out 64 KiB at a time", dropped)
	}
	if _, _, err := store.Open(first.ID); !errors.Is(err, media.ErrNotFound) {
		t.Fatalf("the sweep kept the older of the two: %v", err)
	}
	if _, _, err := store.Open(second.ID); err != nil {
		t.Fatalf("the sweep dropped the newer of the two: %v", err)
	}
}

// The walk lists a directory in one go and then works through it, and a Put can finish in
// between: the listing has the description and not the blob, which by the time the
// removal runs is renamed into place and out of the in-flight set. Removing on the walk's
// answer there takes the description of a blob that was just handed to a caller.
func TestADescriptionIsNotCollectedFromAPutThatFinishedMeanwhile(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }
	store, root := newStore(t, media.Options{TTL: time.Hour, Now: clock})

	// The state the walk would have snapshotted: a description whose blob is not there
	// yet. The blob then appears, which is the rename landing between the listing and
	// the removal.
	stored := put(t, store, "arrived meanwhile", &media.Blob{})
	blob := filepath.Join(root, stored.ID[5:7], stored.ID)
	moved := blob + ".moved"
	if err := os.Rename(blob, moved); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// A sweep whose listing is taken with the blob away and whose removal runs with it
	// back. The clock is read once, for the cutoff, which is after the walk.
	var restored bool
	store2, _ := newStore(t, media.Options{TTL: time.Hour, Root: root, Now: func() time.Time {
		if !restored {
			restored = true
			if err := os.Rename(moved, blob); err != nil {
				t.Errorf("Rename back: %v", err)
			}
		}
		return now
	}})

	if _, _, err := store2.Sweep(t.Context()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, _, err := store2.Open(stored.ID); err != nil {
		t.Fatalf("the description of a blob that arrived mid-sweep was collected: %v", err)
	}
}
