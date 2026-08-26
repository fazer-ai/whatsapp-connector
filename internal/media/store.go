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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
	"strings"
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
	StoredAt int64  `json:"stored_at"`
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
	Now     func() time.Time
}

// Store is the blob cache.
type Store struct {
	opts Options
	root *os.Root
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
	if opts.MaxBlob > opts.Quota {
		return nil, fmt.Errorf("media: one blob may be %d bytes but all of them together only %d, so a blob at the cap evicts everything else and then itself",
			opts.MaxBlob, opts.Quota)
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
	return &Store{opts: opts, root: root}, nil
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

// Put reads a blob in and returns what it stored.
//
// The bytes land in a temporary file first and are renamed into place once they are all
// there, so a reader never opens a partial blob and a crash mid-write leaves rubbish
// the sweep collects rather than a file that lies about its length. The description is
// written before the bytes are named, for the same reason in the other direction: a
// blob without one is unservable, so it must not be the state a crash can leave.
func (s *Store) Put(source io.Reader, about *Blob) (Blob, error) {
	id, err := newID()
	if err != nil {
		return Blob{}, err
	}
	stored := *about
	stored.ID = id
	stored.StoredAt = s.opts.Now().UnixMilli()

	path := s.pathOf(id)
	if err := s.root.MkdirAll(shardOf(id), 0o700); err != nil {
		return Blob{}, fmt.Errorf("media: prepare the shard for %s: %w", id, err)
	}

	// Named rather than handed to CreateTemp, because the id is already unique: the
	// temporary name only has to be one the sweep recognises as an unfinished write,
	// and one it can read an id back out of so it never touches a file it did not
	// write.
	tempName := shardOf(id) + "/" + tempPrefix + id
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

	digest := sha256.New()
	// One byte past the cap, so a source that is exactly at it is kept and one over is
	// refused rather than silently truncated.
	written, err := io.Copy(io.MultiWriter(temp, digest), io.LimitReader(source, s.opts.MaxBlob+1))
	if err != nil {
		return Blob{}, fmt.Errorf("media: store %s: %w", id, err)
	}
	if written > s.opts.MaxBlob {
		return Blob{}, fmt.Errorf("%w: %d bytes", ErrTooLarge, written)
	}
	if err := temp.Close(); err != nil {
		return Blob{}, fmt.Errorf("media: finish %s: %w", id, err)
	}

	stored.Size = written
	stored.SHA256 = hex.EncodeToString(digest.Sum(nil))
	if err := s.writeAbout(id, &stored); err != nil {
		return Blob{}, err
	}
	if err := s.root.Rename(tempName, path); err != nil {
		_ = s.root.Remove(s.aboutPath(id))
		return Blob{}, fmt.Errorf("media: name %s: %w", id, err)
	}
	return stored, nil
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
func (s *Store) Sweep() (dropped int, freed int64, err error) {
	held, err := s.list()
	if err != nil {
		return 0, 0, err
	}

	cutoff := s.opts.Now().Add(-s.opts.TTL)
	var total int64
	kept := held[:0]
	for _, entry := range held {
		if entry.touched.Before(cutoff) {
			s.drop(entry.id)
			dropped, freed = dropped+1, freed+entry.size
			continue
		}
		total += entry.size
		kept = append(kept, entry)
	}

	if total <= s.opts.Quota {
		return dropped, freed, nil
	}
	// Least recently handed out first, which is the order they stop being worth the
	// disk in: a blob nobody has come for is one the client will ask the session for
	// again if it ever does.
	sort.Slice(kept, func(i, j int) bool { return kept[i].touched.Before(kept[j].touched) })
	for _, entry := range kept {
		if total <= s.opts.Quota {
			break
		}
		s.drop(entry.id)
		dropped, freed, total = dropped+1, freed+entry.size, total-entry.size
	}
	return dropped, freed, nil
}

// held is one blob as the sweep sees it: what it costs and when it was last collected.
type held struct {
	id      string
	size    int64
	touched time.Time
}

func (s *Store) list() ([]held, error) {
	var out []held
	// The ids a description was found for, checked against the blobs at the end: a
	// description whose blob is not here describes nothing.
	var described []string
	err := fs.WalkDir(s.root.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		switch {
		case err != nil:
			// A directory that cannot be read is not a reason to abandon the sweep:
			// what is walked still gets swept, and the next pass tries this again.
			return nil //nolint:nilerr // deliberate, see above
		case entry.IsDir():
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil //nolint:nilerr // gone between the walk and the stat, which the sweep wanted anyway
		}
		switch name, aged := entry.Name(), info.ModTime().Before(s.opts.Now().Add(-s.opts.TTL)); {
		case strings.HasPrefix(name, tempPrefix) && validID(strings.TrimPrefix(name, tempPrefix)):
			// A write that was interrupted. It is named after nothing and nobody can
			// ask for it, so the sweep is what collects it — but only once it is old
			// enough to be nobody's, since a write in progress right now looks exactly
			// like one that was abandoned.
			if aged {
				_ = s.root.Remove(path)
			}
		case strings.HasSuffix(name, aboutSuffix) && validID(strings.TrimSuffix(name, aboutSuffix)):
			// A description is normally accounted for with the blob it describes. One
			// on its own is what a crash between the two writes leaves behind, and
			// nothing else will ever collect it: no request names it and no blob drop
			// takes it along. Aged first for the same reason as above — a Put that has
			// written the description and not yet named the bytes is indistinguishable
			// from one that never will, and collecting that one loses a blob that is
			// about to exist.
			if aged {
				described = append(described, strings.TrimSuffix(name, aboutSuffix))
			}
		case validID(name):
			out = append(out, held{id: name, size: info.Size(), touched: info.ModTime()})
		}
		// Anything else was not written by this store. The root is a directory an
		// operator points at, and one that turns out to hold something else is a
		// misconfiguration to leave alone rather than a directory to tidy.
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("media: walk %s: %w", s.opts.Root, err)
	}

	present := make(map[string]struct{}, len(out))
	for _, entry := range out {
		present[entry.id] = struct{}{}
	}
	for _, id := range described {
		if _, found := present[id]; !found {
			_ = s.root.Remove(s.aboutPath(id))
		}
	}
	return out, nil
}

// drop removes a blob. The bytes go first: a description without them is unservable and
// the sweep collects it, while bytes without a description would be served as an
// unnamed file of unknown type.
func (s *Store) drop(id string) {
	_ = s.root.Remove(s.pathOf(id))
	_ = s.root.Remove(s.aboutPath(id))
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
func shardOf(id string) string              { return id[len(idPrefix) : len(idPrefix)+2] }
func (s *Store) pathOf(id string) string    { return shardOf(id) + "/" + id }
func (s *Store) aboutPath(id string) string { return s.pathOf(id) + aboutSuffix }

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
