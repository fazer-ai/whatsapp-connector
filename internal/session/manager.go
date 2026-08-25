package session

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// Manager keeps the sessions this instance owns, routes commands to them, and keeps
// their leases alive.
//
// It is the layer that decides ownership: a session exists here only while the lease
// is held, and losing the lease tears it down rather than letting it publish under an
// epoch that has moved on.
type Manager struct {
	instance  string
	engine    engine.Engine
	leases    *cluster.Leases
	publisher transport.Publisher
	replier   transport.Replier
	newID     IDFunc
	now       func() time.Time
	log       zerolog.Logger

	mu       sync.RWMutex
	sessions map[string]*Session
}

// ManagerConfig is what a manager needs.
type ManagerConfig struct {
	Instance  string
	Engine    engine.Engine
	Leases    *cluster.Leases
	Publisher transport.Publisher
	Replier   transport.Replier
	NewID     IDFunc
	Now       func() time.Time
	Logger    zerolog.Logger
}

// NewManager returns a manager owning no sessions yet.
func NewManager(cfg *ManagerConfig) *Manager {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Manager{
		instance:  cfg.Instance,
		engine:    cfg.Engine,
		leases:    cfg.Leases,
		publisher: cfg.Publisher,
		replier:   cfg.Replier,
		newID:     cfg.NewID,
		now:       cfg.Now,
		log:       cfg.Logger,
		sessions:  make(map[string]*Session),
	}
}

// SIDs lists the sessions this instance is running, which is what the command reader
// subscribes to.
func (m *Manager) SIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sids := make([]string, 0, len(m.sessions))
	for sid := range m.sessions {
		sids = append(sids, sid)
	}
	return sids
}

// Count is how many sessions this instance runs, for the registry and `admin.ping`.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

// Adopt takes a session over: wins the lease, opens it on the engine, and starts it.
// It returns cluster.ErrNotOwner when another instance holds it, which is the ordinary
// answer in a fleet and not a failure.
func (m *Manager) Adopt(ctx context.Context, sid string) (*Session, error) {
	m.mu.RLock()
	existing, running := m.sessions[sid]
	m.mu.RUnlock()
	if running {
		return existing, nil
	}

	lease, err := m.leases.Acquire(ctx, sid)
	if err != nil {
		return nil, err
	}

	engineSession, err := m.engine.Open(ctx, sid)
	if err != nil {
		// The lease is held for a session that cannot run. Holding it would keep every
		// other instance from trying, so it goes back.
		_, _ = m.leases.Release(context.WithoutCancel(ctx), sid)
		return nil, err
	}

	session := New(ctx, &Config{
		Instance: m.instance, Lease: lease, Leases: m.leases, Engine: engineSession,
		Publisher: m.publisher, Replier: m.replier, NewID: m.newID, Now: m.now, Logger: m.log,
	})

	m.mu.Lock()
	// Adopt can be reached twice for one session (a wake plus a claim), and the second
	// caller must not replace a running session with a second one publishing under the
	// same epoch.
	if winner, ok := m.sessions[sid]; ok {
		m.mu.Unlock()
		session.Stop()
		return winner, nil
	}
	m.sessions[sid] = session
	m.mu.Unlock()

	m.log.Info().Str("sid", sid).Uint64("epoch", lease.Epoch).Msg("adopted a session")
	return session, nil
}

// Release stops a session and gives up its lease.
func (m *Manager) Release(ctx context.Context, sid string) {
	m.mu.Lock()
	session, ok := m.sessions[sid]
	delete(m.sessions, sid)
	m.mu.Unlock()
	if ok {
		session.Stop()
	}
	if _, err := m.leases.Release(ctx, sid); err != nil {
		m.log.Warn().Err(err).Str("sid", sid).Msg("failed to release a lease")
	}
}

// Dispatch routes one command. It answers by itself for the two it can answer without
// a session (`session.wake` and `admin.ping`) and hands the rest to the session.
func (m *Manager) Dispatch(ctx context.Context, delivery *transport.Delivery) {
	command := delivery.Command
	switch command.Type {
	case protocol.CommandSessionWake:
		m.wake(ctx, delivery)
		return
	case protocol.CommandAdminPing:
		m.pong(ctx, delivery)
		return
	}

	m.mu.RLock()
	session, running := m.sessions[command.SID]
	m.mu.RUnlock()

	if !running {
		// Not ours. Leaving it un-acknowledged is the point: the instance that does own
		// the session reads the same stream, and an instance that owns nothing must not
		// swallow a command on its way there.
		return
	}
	if !session.Offer(delivery) {
		m.refuse(ctx, delivery, protocol.NewError(protocol.ErrorRateLimited, "the session has too many commands waiting"))
	}
}

// wake asks this instance to pick a session up. Every instance reads the control
// stream, so the first one to win the lease runs it and the others find it taken,
// which is the fleet's whole scheduling algorithm.
func (m *Manager) wake(ctx context.Context, delivery *transport.Delivery) {
	sid := delivery.Command.SID
	if sid == "" {
		m.ack(ctx, delivery)
		return
	}

	_, err := m.Adopt(ctx, sid)
	switch {
	case err == nil, errors.Is(err, cluster.ErrNotOwner):
	default:
		// Left unacknowledged on purpose. Every instance reads this stream through one
		// consumer group, so acknowledging a wake nobody could act on retires it: the
		// session then stays unowned until a client happens to send another. Whatever
		// stopped the adoption — a database that was away, a store that could not be
		// read — may well be over by the time this is reclaimed.
		m.log.Error().Err(err).Str("sid", sid).Msg("failed to adopt a woken session; leaving the wake pending")
		return
	}
	m.ack(ctx, delivery)
}

func (m *Manager) pong(ctx context.Context, delivery *transport.Delivery) {
	command := delivery.Command
	if command.ReplyTo != "" {
		result, _ := json.Marshal(map[string]any{
			"inst": m.instance, "version": protocol.Version, "sessions": m.Count(),
		})
		if err := m.replier.Reply(ctx, command.ReplyTo, protocol.Reply{
			V: protocol.Version, ID: command.ID, OK: true, Result: result,
		}); err != nil {
			m.log.Error().Err(err).Msg("failed to answer admin.ping")
		}
	}
	m.ack(ctx, delivery)
}

func (m *Manager) refuse(ctx context.Context, delivery *transport.Delivery, failure *protocol.Error) {
	command := delivery.Command
	if command.ReplyTo != "" {
		_ = m.replier.Reply(ctx, command.ReplyTo, protocol.Reply{
			V: protocol.Version, ID: command.ID, OK: false, Error: failure,
		})
	}
	m.ack(ctx, delivery)
}

func (m *Manager) ack(ctx context.Context, delivery *transport.Delivery) {
	if err := delivery.Ack(ctx); err != nil && ctx.Err() == nil {
		m.log.Error().Err(err).Str("cmd_id", delivery.Command.ID).Msg("failed to acknowledge a command")
	}
}

// RenewAll keeps the leases of running sessions alive and tears down whatever this
// instance has lost. It is the loop that turns "the lease expired" into "the socket is
// closed", which is what keeps two instances off one account.
func (m *Manager) RenewAll(ctx context.Context) {
	for _, sid := range m.SIDs() {
		err := m.leases.Renew(ctx, sid)
		if err == nil {
			continue
		}
		if !errors.Is(err, cluster.ErrNotOwner) {
			if _, fresh := m.leases.Owned(sid); fresh {
				// A Redis blip is not proof the lease moved, and the lease has not run
				// out yet, so the right thing is to try again next tick rather than to
				// hand a live session away on one failed round trip.
				m.log.Warn().Err(err).Str("sid", sid).Msg("could not renew a lease")
				continue
			}
			// It has run out. Whatever Redis is doing, a peer is now free to take this
			// session, and a socket left open here would be the second one on the
			// account: WhatsApp answers that by replacing the stream, and both owners
			// write the same device meanwhile. Not knowing is the reason to let go, not
			// a reason to hold on.
			m.log.Warn().Err(err).Str("sid", sid).Msg("a lease went stale while unreachable; stopping the session")
		} else {
			m.log.Warn().Str("sid", sid).Msg("lost a lease; stopping the session")
		}
		m.mu.Lock()
		session, ok := m.sessions[sid]
		delete(m.sessions, sid)
		m.mu.Unlock()
		if ok {
			session.Stop()
		}
	}
}

// StopAll releases every session, which is what a SIGTERM does before the process
// exits: a released lease is one a peer can take immediately instead of waiting a full
// TTL for it to expire.
func (m *Manager) StopAll(ctx context.Context) {
	for _, sid := range m.SIDs() {
		m.Release(ctx, sid)
	}
}
