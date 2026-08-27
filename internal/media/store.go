// Package media keeps the bytes of a media message on this instance's disk and hands
// them back over HTTP.
//
// Bytes never travel inside a frame, so an inbound media message is published with a
// reference to a blob here and the client fetches it separately. That makes this a
// cache with a promise attached rather than storage: the client is told where the bytes
// are, and if they are gone by the time it asks, it asks the session for them again and
// this fills up anew.
//
// The filesystem is the index. A blob is a file named by its id with its description
// beside it, its modification time is when it was last handed out, and the sweep reads
// both off the directory. Keeping a separate index would mean keeping it true across
// crashes, and the one thing a cache must never do is believe it holds something it
// does not.
//
// Every path here is opened through an os.Root on the media directory. Blob ids arrive
// on HTTP requests, and a root confines them at the system call rather than at a check:
// a name that climbs out is refused by the kernel, and so is one that only climbs out
// once a symlink is swapped in between the check and the open.
package media

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Defaults for the store's bounds. They are fields on Options so a deployment can trade
// disk for how long a client has to come and collect.
const (
	// DefaultTTL is how long a blob is kept without being asked for. It only has to
	// outlive the gap between the event and the client's fetch, which is one job queue
	// deep, and a client that arrives later asks the session to download it again.
	DefaultTTL = 24 * time.Hour
	// DefaultQuota is how much disk the blobs may take between sweeps. Reached, the
	// least recently handed out go first.
	DefaultQuota int64 = 2 << 30
	// DefaultMaxBlob is the largest single blob this instance will keep. It matches the
	// cap the Chatwoot side downloads with, so a file this refuses is one that would
	// have been refused on arrival anyway.
	DefaultMaxBlob int64 = 100 << 20
	// DefaultSendMax is the largest file this connector will send. It is not a property
	// of the cache and nothing here reads it: it lives beside the blob caps because it
	// is the same decision seen from the other direction, and a deployment that raises
	// one and forgets the other ends up accepting a file it cannot send back.
	//
	// WhatsApp's own ceiling is per type and moves on its own schedule. A file past it
	// is refused by WhatsApp with an answer the caller is told, which is a better place
	// to learn it than a table in here that goes stale.
	DefaultSendMax int64 = 100 << 20
)

// ErrNotFound is what a blob that is not here answers with. It is the ordinary case
// rather than an exceptional one: the quota drops blobs nobody collected, and the
// client's answer to it is to ask for the media again.
var ErrNotFound = errors.New("media: no such blob")

// ErrTooLarge is a blob past the per-blob cap. It is permanent for that file: a retry
// downloads the same bytes and is refused again.
var ErrTooLarge = errors.New("media: blob is larger than this instance keeps")

// Blob is what the store holds about one file. It is written beside the bytes, so a
// restart finds it rather than having to infer it.
type Blob struct {
	ID       string `json:"id"`
	Mime     string `json:"mime,omitempty"`
	Filename string `json:"filename,omitempty"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256,omitempty"`
	// StoredAt is when this blob's clock started, in epoch milliseconds. It is the
	// moment the write was committed and not the moment it began: the age the sweep
	// reads is stamped at the end of Put, so a caller publishing an expiry from the
	// start would take a long download off the life of what it stored.
	StoredAt int64 `json:"stored_at"`
}

// Options configures the store. The zero value asks for the defaults above.
type Options struct {
	// Root is the directory the blobs live in. It is this instance's own: a blob is
	// served by whoever downloaded it, and an instance that is replaced takes its
	// cache with it.
	Root    string
	TTL     time.Duration
	Quota   int64
	MaxBlob int64
	// BlockSize is the volume's allocation unit, which is what the quota is counted in.
	// The zero value asks for DefaultBlockSize.
	BlockSize int64
	Now       func() time.Time
}

// Store is the blob cache.
type Store struct {
	opts Options
	root *os.Root

	// collecting is held for reading while a blob is being handed out and for writing
	// while one is being evicted. Reading the time and then removing on it is two
	// operations, and an Open that lands between them touches a blob the sweep is
	// already committed to deleting: the recheck sees the old time, the removal
	// succeeds because Unix lets it, and a HEAD that answered 200 is followed by a GET
	// that answers 404. Handing out is cheap and concurrent; evicting is neither, and
	// it is the only thing that has to be alone.
	collecting sync.RWMutex

	// writing is the ids this process has a Put in flight for. A download can take
	// longer than the TTL, and its temporary file then reads to the sweep as one an
	// interrupted write abandoned: collecting it unlinks a file that is still being
	// written into, the copy carries on into an inode with no name, and the rename at
	// the end fails on a blob that was on its way. Only this process writes to this
	// root, so knowing it here is knowing it.
	writingMu sync.Mutex
	writing   map[string]struct{}
}

// New prepares the store, creating the root if it is not there.
func New(opts Options) (*Store, error) {
	if opts.Root == "" {
		return nil, errors.New("media: a root directory is required")
	}
	if opts.TTL <= 0 {
		opts.TTL = DefaultTTL
	}
	if opts.Quota <= 0 {
		opts.Quota = DefaultQuota
	}
	if opts.MaxBlob <= 0 {
		opts.MaxBlob = DefaultMaxBlob
	}
	if opts.BlockSize <= 0 {
		opts.BlockSize = DefaultBlockSize
	}
	// Against what a blob actually costs, not its length: the description beside it and
	// the rounding to whole blocks are part of the budget, so a cap that only fits
	// under the quota before they are counted is one where a blob at the cap evicts
	// everything else and then itself.
	if cost := (held{size: opts.MaxBlob}).cost(opts.BlockSize); cost > opts.Quota {
		return nil, fmt.Errorf(
			"media: one blob of %d bytes takes %d on disk once its description and the block size are counted, "+
				"and the whole quota is %d, so a blob at the cap evicts everything else and then itself",
			opts.MaxBlob, cost, opts.Quota)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if err := os.MkdirAll(opts.Root, 0o700); err != nil {
		return nil, fmt.Errorf("media: prepare %s: %w", opts.Root, err)
	}
	root, err := os.OpenRoot(opts.Root)
	if err != nil {
		return nil, fmt.Errorf("media: open %s: %w", opts.Root, err)
	}
	return &Store{opts: opts, root: root, writing: map[string]struct{}{}}, nil
}

// Close lets go of the directory handle.
func (s *Store) Close() error {
	if err := s.root.Close(); err != nil {
		return fmt.Errorf("media: close the store: %w", err)
	}
	return nil
}

// MaxBlob is the largest single blob this store keeps, which a caller reading from the
// network wants before it starts rather than after.
func (s *Store) MaxBlob() int64 { return s.opts.MaxBlob }

// TTL is how long a blob is kept without being asked for. A caller publishing a
// reference to one needs it to say when the reference lapses, and being told sooner
// than the truth is the safe direction: every hand-out puts the clock back, so a blob
// somebody is still collecting outlives the expiry that was published for it.
func (s *Store) TTL() time.Duration { return s.opts.TTL }

// Put reads a blob in and returns what it stored.
//
// The bytes land in a temporary file first and are renamed into place once they are all
// there, so a reader never opens a partial blob and a crash mid-write leaves rubbish
// the sweep collects rather than a file that lies about its length. The description is
// written before the bytes are named, for the same reason in the other direction: a
// blob without one is unservable, so it must not be the state a crash can leave.
//
// The source has to answer to ctx itself, and this is a requirement on the caller rather
// than something the store can arrange. A read that has already entered a syscall cannot
// be interrupted from the outside: checking the context before each read, which is what
// happens below, returns promptly from a source that yields between reads and does
// nothing at all for one that is blocked inside one. The two callers this has both
// satisfy it — whatsmeow's download hands over bytes that are already in memory, and an
// HTTP body from a request carrying ctx unblocks when ctx does — and a source that does
// neither holds the session that owns the write for as long as its far end feels like it.
func (s *Store) Put(ctx context.Context, source io.Reader, about *Blob) (Blob, error) {
	id, err := newID()
	if err != nil {
		return Blob{}, err
	}
	stored := *about
	stored.ID = id

	s.writingMu.Lock()
	s.writing[id] = struct{}{}
	s.writingMu.Unlock()
	defer func() {
		s.writingMu.Lock()
		delete(s.writing, id)
		s.writingMu.Unlock()
	}()

	path := s.pathOf(id)
	if err := s.root.MkdirAll(shardOf(id), 0o700); err != nil {
		return Blob{}, fmt.Errorf("media: prepare the shard for %s: %w", id, err)
	}

	// Named rather than handed to CreateTemp, because the id is already unique: the
	// temporary name only has to be one the sweep recognises as an unfinished write,
	// and one it can read an id back out of so it never touches a file it did not
	// write.
	tempName := s.tempPath(id)
	temp, err := s.root.OpenFile(tempName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return Blob{}, fmt.Errorf("media: open a temporary file for %s: %w", id, err)
	}
	// Removed unless the rename below claims it. A failure past this point must not
	// leave the bytes behind under a name nothing will ever look up.
	defer func() {
		_ = temp.Close()
		_ = s.root.Remove(tempName)
	}()

	// Read through the context, so a source that yields between reads is given up on
	// rather than waited out. What this cannot do is interrupt a read already inside a
	// syscall, and no wrapper can: see the note on the requirement above.
	source = &reading{ctx: ctx, from: source}

	digest := sha256.New()
	written, err := io.Copy(io.MultiWriter(temp, digest), io.LimitReader(source, s.opts.MaxBlob))
	if err != nil {
		return Blob{}, fmt.Errorf("media: store %s: %w", id, err)
	}
	// Asked for one more byte rather than reading one past the cap, because a cap at
	// the top of the range makes that arithmetic wrap: the limit goes negative, the
	// reader answers EOF straight away, and every file is stored empty. A source with
	// nothing left is one that fitted exactly.
	//
	// Asked until it answers, because a reader is allowed to return no bytes and no
	// error, which means nothing happened rather than there is nothing left. Taking one
	// of those for EOF renames the first MaxBlob bytes and calls it a whole file.
	if err := s.refuseIfMore(source); err != nil {
		return Blob{}, err
	}
	if err := temp.Close(); err != nil {
		return Blob{}, fmt.Errorf("media: finish %s: %w", id, err)
	}

	// Read once and used for both, so the description says the same thing about this
	// blob's age as the stamp the sweep reads. A caller publishing an expiry from the
	// description is otherwise told a blob dies sooner than the store will drop it, by
	// however long the download took, and a client seeing a lapsed reference goes and
	// asks for the whole file again.
	now := s.opts.Now()
	stored.Size = written
	stored.SHA256 = hex.EncodeToString(digest.Sum(nil))
	stored.StoredAt = now.UnixMilli()
	if err := s.writeAbout(id, &stored); err != nil {
		return Blob{}, err
	}
	// Stamped rather than left with the time the temporary file picked up when the first
	// byte landed: the age is how long since anybody wanted this, and a download that
	// took longer than the TTL would otherwise arrive already expired.
	//
	// Stamped before it is named, and not after. A stamp that lands after the rename
	// leaves a window in which the canonical file carries the temporary file's time,
	// which for a slow download is old enough to evict, and a sweep landing in that
	// window deletes a blob the caller is about to be handed. Renaming does not touch
	// the file's own time, so stamping first and naming second closes the window rather
	// than racing inside it: a file is never both canonical and unstamped.
	//
	// A stamp that will not go on is a failed commit, not a detail: the blob would be
	// served under a time the next sweep reads as expired. Given up on instead, so what
	// the caller gets is an error it can retry.
	if err := s.root.Chtimes(tempName, now, now); err != nil {
		_ = s.root.Remove(s.aboutPath(id))
		return Blob{}, fmt.Errorf("media: stamp %s: %w", id, err)
	}
	if err := s.root.Rename(tempName, path); err != nil {
		_ = s.root.Remove(s.aboutPath(id))
		return Blob{}, fmt.Errorf("media: name %s: %w", id, err)
	}
	return stored, nil
}

// reading is a source that gives up when the context does. io.Copy has no way to be
// interrupted, so the check goes on the read it is already making.
type reading struct {
	ctx  context.Context
	from io.Reader
}

func (r *reading) Read(into []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.from.Read(into) //nolint:wrapcheck // a pass-through reader; wrapping would hide io.EOF
}

// refuseIfMore reports a source that still has something in it once the cap has been
// read, and nothing for one that fitted exactly.
func (s *Store) refuseIfMore(source io.Reader) error {
	one := make([]byte, 1)
	for {
		switch read, err := source.Read(one); {
		case read > 0:
			return fmt.Errorf("%w: more than %d bytes", ErrTooLarge, s.opts.MaxBlob)
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return fmt.Errorf("media: read past the cap: %w", err)
		}
		// No bytes and no error: nothing happened. Asked again.
	}
}

// Open hands back a blob's bytes and what is known about it. The caller closes the
// reader.
//
// Opening counts as being asked for, so the blob goes to the back of the eviction
// queue: what the sweep drops is what nobody has collected, not what arrived first.
func (s *Store) Open(id string) (io.ReadSeekCloser, Blob, error) {
	if !validID(id) {
		return nil, Blob{}, ErrNotFound
	}
	// Held across the whole hand-out, not only the touch. An eviction landing between
	// the open and the touch unlinks the blob, the touch then fails quietly, and this
	// returns a descriptor to a file that is no longer there: a HEAD that answers 200
	// and a GET that answers 404, which is the pair the lock exists to prevent.
	s.collecting.RLock()
	defer s.collecting.RUnlock()

	about, err := s.readAbout(id)
	if err != nil {
		return nil, Blob{}, err
	}
	file, err := s.root.Open(s.pathOf(id))
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil, Blob{}, ErrNotFound
	case err != nil:
		return nil, Blob{}, fmt.Errorf("media: open %s: %w", id, err)
	}
	now := s.opts.Now()
	// Best effort: a blob that could not be touched is one the sweep may drop sooner
	// than it should, which costs a download and not the answer being given here.
	_ = s.root.Chtimes(s.pathOf(id), now, now)
	return file, about, nil
}

// Sweep drops what has aged out and then, if the rest is still over quota, the blobs
// nobody has asked for in longest. It returns how many went and how many bytes came
// back.
//
// Called on a tick rather than from Put, so the cost lands on the loop that expects it
// instead of on the message that happened to fill the disk.
func (s *Store) Sweep(ctx context.Context) (dropped int, freed int64, err error) {
	blobs, leftovers, skipped, err := s.list(ctx)
	if err != nil {
		return 0, 0, err
	}

	// One reading for the whole pass, taken before anything is removed, so every
	// decision in it is made against the same notion of now.
	cutoff := s.opts.Now().Add(-s.opts.TTL)

	// One error is kept rather than the first one returned, so a single undeletable
	// file does not stop the sweep from clearing everything behind it. The shards that
	// could not be read start it off: what is in them is not in the accounting below.
	failed := skipped
	for _, entry := range leftovers {
		if err := ctx.Err(); err != nil {
			return dropped, freed, errors.Join(failed, err)
		}
		if s.stillWanted(entry) {
			continue
		}
		if err := s.root.Remove(entry.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			// Silence here reads as a clean sweep while the file is still on the disk,
			// and it is not counted against the quota either, so nothing would ever
			// notice up to one whole blob's worth of it.
			failed = errors.Join(failed, fmt.Errorf("media: collect %s: %w", entry.path, err))
		}
	}

	// take removes one entry and answers what it still costs: nothing when it went, its
	// whole cost when it did not, and the description alone when the bytes went and the
	// description would not follow.
	take := func(entry held) int64 {
		// Read again, immediately before removing, and with nothing able to touch it in
		// between. The snapshot is minutes old by now and Open touches a blob it hands
		// out, so a blob collected since the walk is one somebody is using: dropping it
		// on the stale time turns a HEAD that answered 200 into a GET that answers 404.
		s.collecting.Lock()
		defer s.collecting.Unlock()
		if s.isWriting(entry.id) {
			// A Put that has renamed its bytes into place and not yet stamped them.
			// The canonical file is there with the temporary file's time on it, which
			// is old enough to evict, and the caller is about to be handed a blob that
			// this would have deleted.
			return entry.charged
		}
		if info, err := s.root.Stat(s.pathOf(entry.id)); err == nil && info.ModTime().After(entry.touched) {
			return entry.charged
		}
		bytesGone, dropErr := s.drop(entry.id)
		if dropErr != nil {
			failed = errors.Join(failed, dropErr)
		}
		if !bytesGone {
			return entry.charged
		}
		dropped, freed = dropped+1, freed+entry.size
		if dropErr != nil {
			// The bytes went and the description would not follow. Charging the whole
			// entry would have the sweep evict other blobs to make room for a payload
			// that is not there any more.
			return blocks(entry.about, s.opts.BlockSize)
		}
		return 0
	}

	var total int64
	kept := blobs[:0]
	for _, entry := range blobs {
		if err := ctx.Err(); err != nil {
			return dropped, freed, errors.Join(failed, err)
		}
		// Counted for whatever is still here, including an entry that refused to go and
		// one that was collected between the walk and now: what the quota has to be
		// measured against is the disk, not the intention.
		entry.charged = entry.cost(s.opts.BlockSize)
		if entry.touched.Before(cutoff) {
			entry.charged = take(entry)
		}
		if entry.charged == 0 {
			continue
		}
		total += entry.charged
		kept = append(kept, entry)
	}

	if total <= s.opts.Quota {
		return dropped, freed, failed
	}
	// Least recently handed out first, which is the order they stop being worth the
	// disk in: a blob nobody has come for is one the client will ask the session for
	// again if it ever does.
	sort.Slice(kept, func(i, j int) bool { return kept[i].touched.Before(kept[j].touched) })
	for _, entry := range kept {
		if err := ctx.Err(); err != nil {
			return dropped, freed, errors.Join(failed, err)
		}
		if total <= s.opts.Quota {
			break
		}
		total -= entry.charged - take(entry)
	}
	return dropped, freed, failed
}

// leftover is a file that belongs to nobody: a write that was interrupted, or a
// description whose blob is not here. The id it names is kept so the state can be asked
// about again at the moment of removal rather than only at the moment of the walk.
type leftover struct {
	id   string
	path string
	// describes marks a description, which is the one of the two that can stop being a
	// leftover between the walk and the removal.
	describes bool
}

// held is one blob as the sweep sees it: what it costs and when it was last collected.
type held struct {
	id string
	// size is the bytes, and about the description beside them. The description is
	// measured rather than assumed: a filename comes off a message somebody else wrote,
	// so it is not a fixed cost and a long one would sit outside the quota entirely.
	size    int64
	about   int64
	touched time.Time

	// charged is what this entry still costs, which is its whole cost until something
	// removes part of it. Kept apart from size and about rather than written back over
	// them, because cost() is derived from those two and folding a remainder into them
	// makes the next reading of it wrong.
	charged int64
}

// DefaultBlockSize is what one file is charged at least, and what its length is rounded
// up to, unless the deployment says otherwise.
//
// The quota is a disk budget, and a filesystem does not hand out bytes: it hands out
// allocation units, and a file also costs an inode and a directory entry. A cache of ten
// thousand tiny stickers measured by the sum of their lengths reads as a few megabytes
// while the volume it sits on is much fuller, and the eviction that should have started
// never does.
//
// Four kibibytes because that is what ext4, xfs and apfs hand out by default. It is a
// setting rather than a constant because a volume formatted with larger units — 64 KiB
// is not unusual on a tuned filesystem — undercharges every file by the difference, and
// this cannot ask the filesystem without giving up on being portable. Raising the
// default instead would have every ordinary deployment evict far too eagerly.
const DefaultBlockSize int64 = 4 << 10

// cost is what a blob is charged against the quota: its bytes and its description's,
// each rounded up to an allocation unit.
func (b held) cost(unit int64) int64 {
	payload, about := blocks(b.size, unit), blocks(b.about, unit)
	if payload > math.MaxInt64-about {
		return math.MaxInt64
	}
	return payload + about
}

func blocks(size, unit int64) int64 {
	if size <= 0 {
		// An empty file still costs an inode and a directory entry.
		return unit
	}
	if size > math.MaxInt64-unit {
		// Rounding up would wrap, and a file this size is already every block there is.
		return size
	}
	return (size + unit - 1) / unit * unit
}

// list walks the store and reports what it found: the blobs, and the paths of the files
// that are nobody's — an interrupted write, or a description whose blob is not here.
//
// It removes nothing. Every removal the sweep makes goes through one place, so the
// accounting and the cancellation checks are written once rather than in each branch
// that happens to delete something.
func (s *Store) list(ctx context.Context) (blobs []held, leftovers []leftover, skipped, err error) {
	// The paths a description was found at, keyed by the id it describes, checked
	// against the blobs at the end: a description whose blob is not here describes
	// nothing.
	described := map[string]string{}
	// What each description actually takes, merged into its blob once the walk is done:
	// the two are separate files and the walk meets them in whatever order the
	// directory hands them over.
	sidecars := map[string]int64{}
	err = fs.WalkDir(s.root.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		if walkErr := ctx.Err(); walkErr != nil {
			// A walk of a large cache on a slow disk is exactly what a shutdown must not
			// have to wait out, and the process is stopping anyway: what this pass has
			// found so far is thrown away rather than acted on.
			return walkErr
		}
		switch {
		case err != nil && path == ".":
			// The root itself. Nothing is walked and nothing is swept, and swallowing it
			// makes an unreadable store look exactly like an empty one: the sweep would
			// report success every minute while the disk filled.
			return fmt.Errorf("media: read %s: %w", s.opts.Root, err)
		case err != nil:
			// One directory inside it, which is not a reason to abandon the sweep: what
			// is walked still gets swept. Reported all the same, because the blobs in an
			// unreadable shard vanish from the accounting and the quota is then measured
			// against a fraction of the disk, indefinitely and silently.
			skipped = errors.Join(skipped, fmt.Errorf("media: read %s: %w", path, err))
			return nil
		case entry.IsDir():
			return nil
		}
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			// Gone between the walk and the stat, which is what the sweep wanted anyway.
			return nil
		}
		if err != nil {
			// Anything else and the file is still there and unmeasured: it drops out of
			// the quota accounting while the sweep reports a clean pass, so the cache
			// can sit over budget for as long as the condition lasts.
			skipped = errors.Join(skipped, fmt.Errorf("media: measure %s: %w", path, err))
			return nil
		}
		switch name := entry.Name(); {
		case path == s.tempPath(strings.TrimPrefix(name, tempPrefix)):
			// A write that was interrupted. It is named after nothing and nobody can
			// ask for it, so the sweep is what collects it.
			//
			// Straight away rather than after the TTL, because the in-flight set is the
			// whole answer: only this process writes to this root, so a temporary file
			// it is not writing is one a crash or a restart left, and it is not charged
			// against the quota either — waiting a day would leave up to one MaxBlob of
			// each of them on the disk while every sweep reported the cache within
			// budget.
			if id := strings.TrimPrefix(name, tempPrefix); !s.isWriting(id) {
				leftovers = append(leftovers, leftover{id: id, path: path})
			}
		case path == s.aboutPath(strings.TrimSuffix(name, aboutSuffix)):
			// A description is normally accounted for with the blob it describes. One
			// on its own is what a crash between the two writes leaves behind, and
			// nothing else will ever collect it: no request names it and no blob drop
			// takes it along.
			//
			// Asked of the in-flight set for the same reason the temporary files are: a
			// Put that has written the description and not yet named the bytes looks
			// exactly like one that never will, and collecting that one leaves bytes
			// that every Open reports as missing.
			id := strings.TrimSuffix(name, aboutSuffix)
			sidecars[id] = info.Size()
			if !s.isWriting(id) {
				described[id] = path
			}
		case validID(name) && path == s.pathOf(name):
			// Its canonical place, and only there. A file with a blob's name somewhere
			// else in the tree is not that blob: dropping it goes by the id, so the
			// sweep would remove whatever is at the canonical path — nothing, or the
			// real blob of the same name — and count this one's bytes as freed either
			// way.
			blobs = append(blobs, held{id: name, size: info.Size(), touched: info.ModTime()})
		}
		// Anything else was not written by this store. The root is a directory an
		// operator points at, and one that turns out to hold something else is a
		// misconfiguration to leave alone rather than a directory to tidy.
		return nil
	})
	if err != nil {
		return nil, nil, skipped, fmt.Errorf("media: walk %s: %w", s.opts.Root, err)
	}

	present := make(map[string]struct{}, len(blobs))
	for i := range blobs {
		blobs[i].about = sidecars[blobs[i].id]
		present[blobs[i].id] = struct{}{}
	}
	for id, path := range described {
		if _, found := present[id]; !found {
			leftovers = append(leftovers, leftover{id: id, path: path, describes: true})
		}
	}
	return blobs, leftovers, skipped, nil
}

// stillWanted asks again, at the moment of removal, whether a leftover is one.
//
// The walk takes its listing of a directory in one go and then works through it, and a
// Put can finish in between: the listing has the description and not the blob, which by
// the time this reads it is renamed into place and out of the in-flight set. Removing on
// the walk's answer there takes the description of a blob that was just handed to a
// caller, and every Open of it afterwards reports it missing.
func (s *Store) stillWanted(entry leftover) bool {
	if s.isWriting(entry.id) {
		return true
	}
	if !entry.describes {
		return false
	}
	_, err := s.root.Stat(s.pathOf(entry.id))
	return err == nil
}

// isWriting reports whether a Put has this id in flight.
func (s *Store) isWriting(id string) bool {
	s.writingMu.Lock()
	defer s.writingMu.Unlock()
	_, found := s.writing[id]
	return found
}

// drop removes a blob and says whether the bytes actually went. The bytes go first: a
// description without them is unservable and the sweep collects it, while bytes without
// a description would be served as an unnamed file of unknown type.
//
// The answer is what the sweep counts on. A volume that has gone read-only refuses every
// removal, and a sweep that counted those as freed would report a cache it had emptied
// while the disk stayed full, every minute, with nothing said.
func (s *Store) drop(id string) (bytesGone bool, err error) {
	if removeErr := s.root.Remove(s.pathOf(id)); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return false, fmt.Errorf("media: drop %s: %w", id, removeErr)
	}
	// Reported as well, because the sweep subtracts the whole cost of the entry and the
	// description is part of that: a long one left behind is disk the accounting has
	// stopped counting, which is how the cache sits over quota with nothing said.
	// The bytes are gone either way by this point, and saying so is what keeps the sweep
	// from evicting other blobs to make room for a payload that is not there any more.
	if removeErr := s.root.Remove(s.aboutPath(id)); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return true, fmt.Errorf("media: drop the description of %s: %w", id, removeErr)
	}
	return true, nil
}

const (
	aboutSuffix = ".json"
	// tempPrefix marks a blob whose bytes are still arriving. A dot so an ordinary
	// listing does not show it, and the id after it so the sweep can tell a file this
	// store abandoned from one that was already in the directory.
	tempPrefix = "."
)

// Paths are relative to the root and always spelled with a forward slash, which is what
// os.Root takes on every platform.
//
// The shard is the first byte of the random half, not the first two characters of the
// id: every id starts `blob_`, so taking those would put every blob in one directory
// and the sharding would exist in the layout and nowhere on disk.
// All four answer the empty string for anything that is not an id this store issues,
// which is never a path the walk can be at: a name off the disk is compared against
// these rather than taken apart, so a foreign one matches nothing instead of being
// sliced into a directory that does not exist.
func shardOf(id string) string {
	if !validID(id) {
		return ""
	}
	return id[len(idPrefix) : len(idPrefix)+2]
}

func (s *Store) pathOf(id string) string {
	shard := shardOf(id)
	if shard == "" {
		return ""
	}
	return shard + "/" + id
}

func (s *Store) aboutPath(id string) string {
	path := s.pathOf(id)
	if path == "" {
		return ""
	}
	return path + aboutSuffix
}

func (s *Store) tempPath(id string) string {
	shard := shardOf(id)
	if shard == "" {
		return ""
	}
	return shard + "/" + tempPrefix + id
}

func (s *Store) writeAbout(id string, about *Blob) error {
	body, err := json.Marshal(about)
	if err != nil {
		return fmt.Errorf("media: describe %s: %w", id, err)
	}
	if err := s.root.WriteFile(s.aboutPath(id), body, 0o600); err != nil {
		return fmt.Errorf("media: write the description of %s: %w", id, err)
	}
	return nil
}

func (s *Store) readAbout(id string) (Blob, error) {
	body, err := s.root.ReadFile(s.aboutPath(id))
	switch {
	case errors.Is(err, os.ErrNotExist):
		return Blob{}, ErrNotFound
	case err != nil:
		return Blob{}, fmt.Errorf("media: read the description of %s: %w", id, err)
	}
	var about Blob
	if err := json.Unmarshal(body, &about); err != nil {
		return Blob{}, fmt.Errorf("media: the description of %s is unreadable: %w", id, err)
	}
	return about, nil
}

// idPrefix is what a blob id starts with, so a value that reaches the wrong side of the
// contract is recognisable for what it is.
const idPrefix = "blob_"

func newID() (string, error) {
	raw := make([]byte, 12)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("media: name a blob: %w", err)
	}
	return idPrefix + hex.EncodeToString(raw), nil
}

// validID is checked before an id becomes a path. The id comes off an HTTP request, so
// anything that is not the shape this issues is refused rather than joined onto the
// root and looked up.
func validID(id string) bool {
	if len(id) != len(idPrefix)+24 || !strings.HasPrefix(id, idPrefix) {
		return false
	}
	_, err := hex.DecodeString(id[len(idPrefix):])
	return err == nil
}
