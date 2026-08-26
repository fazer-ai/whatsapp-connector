// Package whatsmeow is the real WhatsApp side: one whatsmeow client per session,
// translated into the canonical events the contract names.
//
// Nothing above this package imports whatsmeow, and nothing in it decides ownership.
// A session runs here for as long as the layer above says this instance owns it, and
// that layer closes it the moment the lease is gone.
package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"

	"github.com/rs/zerolog"
	wm "go.mau.fi/whatsmeow"
	waStore "go.mau.fi/whatsmeow/store"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// deviceNameOnce guards the one write to whatsmeow's process-wide device properties.
var deviceNameOnce sync.Once

// Options is everything an engine needs beyond its device store.
type Options struct {
	// DeviceName is what the account's linked-devices list shows.
	DeviceName string
	// Media is where a session puts the file of an inbound message. The zero value is
	// an instance with nowhere to write, which publishes media messages with no file to
	// fetch.
	Media MediaOptions
}

// Engine hands out one session per account, backed by a shared device store.
type Engine struct {
	store *store.Container
	media MediaOptions
	log   zerolog.Logger

	mu       sync.Mutex
	sessions map[string]*Session
	closed   bool
}

// New returns an engine over an open store.
//
// The device name is set once, process-wide, because that is the only place whatsmeow
// keeps it: `store.DeviceProps` is a package-level value read during the pairing
// handshake, so a per-session name would mean mutating a global in the middle of every
// pairing and racing every other session doing the same. `session.connect` carries a
// `device_name` the contract promises, and this is why it cannot be honoured per
// session; the deployment's own name is what the account's linked-devices list shows.
//
// An engine that was given a blob store and no address to advertise it under is
// refused rather than run: the reference it would publish is a path with no host, and a
// client reading one has no way to tell it apart from a URL it simply cannot reach.
//
//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func New(container *store.Container, opts Options, log zerolog.Logger) (*Engine, error) {
	if opts.Media.Blobs != nil {
		if err := reachableAt(opts.Media.BaseURL); err != nil {
			return nil, err
		}
		if ttl := opts.Media.Blobs.TTL(); ttl <= deliverTimeout {
			// The clock starts when the file is stored and the reference is published
			// afterwards, with a message allowed to spend deliverTimeout waiting for the
			// publisher. A cache that keeps a blob for less than that hands out
			// references the sweeper has already collected, which the client reads as
			// media that is gone.
			return nil, fmt.Errorf(
				"whatsmeow: blobs are kept for %s and a message may spend %s waiting to be published, "+
					"so a reference could name a blob that was swept before the client was told about it",
				ttl, deliverTimeout)
		}
	}
	if deviceName := opts.DeviceName; deviceName != "" {
		// Written exactly once, because it is written to a package-level value whatsmeow
		// reads from its pairing handshake: a second engine assigning it is a write with
		// no lock against a read on another goroutine. The first name wins, which is the
		// deployment's own, since a process runs one engine.
		deviceNameOnce.Do(func() { waStore.DeviceProps.Os = proto.String(deviceName) })
	}
	return &Engine{
		store:    container,
		media:    opts.Media,
		log:      log,
		sessions: make(map[string]*Session),
	}, nil
}

// reachableAt reports whether blobs published under this address could be fetched.
//
// It is checked once, at startup, rather than found out per message: a reference is
// published after the message it belongs to has been acknowledged, so an address a
// client cannot fetch from costs the file rather than an error somebody sees. An
// operator who left the scheme off, which is the ordinary way to get this wrong, is told
// so by a container that will not start.
func reachableAt(base string) error {
	if base == "" {
		return errors.New("whatsmeow: a blob store needs the address this instance is reached at")
	}
	address, err := url.Parse(base)
	if err != nil {
		return fmt.Errorf("whatsmeow: %q is not an address blobs can be published under: %w", base, err)
	}
	if address.Scheme != "http" && address.Scheme != "https" {
		// Covers the bare `host:port`, which url.Parse reads as the scheme `host` with
		// an opaque body rather than refusing.
		return fmt.Errorf(
			"whatsmeow: blobs are published under %q, and a client fetches them over http or https", base)
	}
	host := address.Hostname()
	if host == "" {
		// Hostname rather than Host: `http://:8080` parses with an authority of
		// ":8080", which is a port and nobody to ask for it.
		return fmt.Errorf("whatsmeow: blobs are published under %q, which names no host", base)
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		// `0.0.0.0` and `::` say "every interface" to a listener and mean "this machine"
		// to whoever dials them, so a client elsewhere connects to itself. Loopback is
		// not refused with them: an instance sharing a network namespace with its client
		// is a deployment somebody really runs.
		return fmt.Errorf(
			"whatsmeow: blobs are published under %q, which is an address to listen on rather than one to reach",
			base)
	}
	if port := address.Port(); port != "" {
		// url.Parse only checks that a port is digits, so 99999 and 00 both come through
		// it. Range-checked rather than compared against "0": zero is what a listener
		// reads as "any free port", which is not a port anybody can be told to come back
		// to, and it is spelled more than one way.
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return fmt.Errorf("whatsmeow: blobs are published under %q, and %q is not a port to dial", base, port)
		}
	}
	if address.RawQuery != "" || address.Fragment != "" {
		// The id is appended as a path segment, so anything after it would end up in
		// front of the query rather than behind it.
		return fmt.Errorf(
			"whatsmeow: blobs are published under %q, and a blob's id is appended to it as a path", base)
	}
	return nil
}

// Open prepares a session with the device it paired, or a fresh one when it has not
// paired yet. It does not touch the network.
func (e *Engine) Open(ctx context.Context, sid string) (engine.Session, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, errors.New("whatsmeow: the engine is closed")
	}
	if existing, ok := e.sessions[sid]; ok {
		if !existing.Closed() {
			e.mu.Unlock()
			return existing, nil
		}
		// A closed session holds a socket that cannot be reopened, so the next Open
		// builds a new client rather than handing back one that will never connect.
		delete(e.sessions, sid)
	}
	e.mu.Unlock()

	device, err := e.store.Device(ctx, sid)
	if err != nil {
		return nil, fmt.Errorf("whatsmeow: open %s: %w", sid, err)
	}

	wa := newLibraryLogger(e.log, sid)
	session := newSession(sid, wm.NewClient(device, wa), e.store, e.media, e.log, wa)
	// Registered before the session can be handed out, so a close that happens while
	// this function is still running is not one nobody hears about.
	session.onClose(func() { e.forget(sid, session) })

	e.mu.Lock()
	// Unlocked before any Close below. A closing session calls back into forget, which
	// takes this lock, and holding it across the call would have the two wait on each
	// other.
	if e.closed {
		e.mu.Unlock()
		_ = session.Close()
		return nil, errors.New("whatsmeow: the engine is closed")
	}
	// Open can be reached twice for one session. The second caller must get the first
	// session rather than a second client on the same credentials.
	if winner, ok := e.sessions[sid]; ok && !winner.Closed() {
		e.mu.Unlock()
		_ = session.Close()
		return winner, nil
	}
	e.sessions[sid] = session
	e.mu.Unlock()

	// A close that landed before the entry existed found nothing to remove, so the
	// entry it should have taken is this one.
	if session.Closed() {
		e.forget(sid, session)
	}
	return session, nil
}

// forget drops a session from the cache once it is closed.
//
// Without it an entry lives until the same account is opened here again, which for a
// fleet member that has handed its sessions on is never: it would go on holding every
// whatsmeow client it ever ran, each with the device credentials and channels behind it.
//
// The session is compared and not just the id, because a closed session and the one
// that replaced it share an id, and the replacement is the one still running.
func (e *Engine) forget(sid string, session *Session) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.sessions[sid] == session {
		delete(e.sessions, sid)
	}
}

// Close shuts every session down.
func (e *Engine) Close() error {
	e.mu.Lock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, session := range e.sessions {
		sessions = append(sessions, session)
	}
	e.sessions = make(map[string]*Session)
	e.closed = true
	e.mu.Unlock()

	var errs []error
	for _, session := range sessions {
		errs = append(errs, session.Close())
	}
	return errors.Join(errs...)
}
