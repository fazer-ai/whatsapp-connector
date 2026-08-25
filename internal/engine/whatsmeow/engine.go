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
	"sync"

	"github.com/rs/zerolog"
	wm "go.mau.fi/whatsmeow"
	waStore "go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// Engine hands out one session per account, backed by a shared device store.
type Engine struct {
	store *store.Container
	log   zerolog.Logger
	waLog waLog.Logger

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
//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func New(container *store.Container, deviceName string, log zerolog.Logger) *Engine {
	if deviceName != "" {
		waStore.DeviceProps.Os = proto.String(deviceName)
	}
	return &Engine{
		store:    container,
		log:      log,
		waLog:    newLibraryLogger(log),
		sessions: make(map[string]*Session),
	}
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

	client := wm.NewClient(device, e.waLog.Sub(sid))
	session := newSession(sid, client, e.store, e.log, e.waLog.Sub(sid))

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		_ = session.Close()
		return nil, errors.New("whatsmeow: the engine is closed")
	}
	// Open can be reached twice for one session. The second caller must get the first
	// session rather than a second client on the same credentials.
	if winner, ok := e.sessions[sid]; ok && !winner.Closed() {
		_ = session.Close()
		return winner, nil
	}
	e.sessions[sid] = session
	return session, nil
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
