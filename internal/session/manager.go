package session

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
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

	// newly is the sessions adopted since the loop last asked, waiting to have what
	// their previous owner left pending drained before anything newer is read for them.
	newlyMu sync.Mutex
	newly   []string

	// orphans are leases of sessions this instance has stopped and could not hand back.
	// The Redis key still names this instance, and every wake for such a session is
	// answered "owned elsewhere" and acknowledged, so nobody runs it until the key
	// expires. Kept here so the next tick tries the release again.
	orphanMu sync.Mutex
	orphans  map[string]struct{}
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
		orphans:   make(map[string]struct{}),
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

// AdoptTimeout bounds the store and lease work one adoption does.
//
// Sized against the defaults it runs under: a 30s lease renewed every 5s. A single
// adoption may delay one heartbeat and no more, and an adoption that runs out is
// retried, because the wake it came from is left unacknowledged and reclaimed.
const AdoptTimeout = 5 * time.Second

// releaseTimeout bounds handing a lease back after an adoption that could not finish.
// Short, because nothing waits on it and the lease expires on its own anyway.
const releaseTimeout = 2 * time.Second

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

	// Bounded, and bounded around the I/O only. Commands are dispatched on the same
	// goroutine that renews every lease this instance holds, so a store that blocks here
	// stops all of them from being renewed: their leases expire, peers acquire the
	// accounts, and the sockets this instance still holds open go on talking to WhatsApp.
	// A session that fails to start is one wake; a renewal that never runs is every
	// session on the instance.
	//
	// The session itself is built on the caller's context, which is the instance's
	// lifetime and has to outlive this.
	io, cancelIO := context.WithTimeout(ctx, AdoptTimeout)
	defer cancelIO()

	lease, err := m.leases.Acquire(io, sid)
	if err != nil {
		return nil, err
	}
	// Won again, so a hand-back still queued for it is now about a live lease.
	m.forgetOrphan(sid)

	engineSession, err := m.engine.Open(io, sid)
	if err != nil {
		// The lease is held for a session that cannot run. Holding it would keep every
		// other instance from trying, so it goes back.
		//
		// Bounded on its own, and detached from the caller: the reason the adoption
		// failed is often the reason Redis is slow, and an unbounded cleanup would spend
		// the deadline this function just enforced on retries nobody is waiting for.
		//
		// Through abandon, so a hand-back that does not get through is tried again rather
		// than left as a key naming an instance that is running nothing.
		release, cancelRelease := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		m.abandon(release, sid)
		cancelRelease()
		return nil, err
	}

	// Detached from the caller on purpose. The context that reaches Adopt bounds the
	// work of adopting, and the caller may hand in one that expires in seconds to keep a
	// slow store off the goroutine that renews leases. A session built on that context
	// would be torn down along with it, which is a connector that pairs an account and
	// drops it. Sessions end when StopAll or a lost lease ends them, and nothing else.
	session := New(context.WithoutCancel(ctx), &Config{
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

	m.newlyMu.Lock()
	m.newly = append(m.newly, sid)
	m.newlyMu.Unlock()

	m.log.Info().Str("sid", sid).Uint64("epoch", lease.Epoch).Msg("adopted a session")
	return session, nil
}

// TakeNewlyAdopted returns the sessions adopted since it was last called, and forgets
// them.
//
// The caller uses it to drain what the previous owner left pending before it reads
// anything newer for those sessions. Commands for one session are ordered by being on
// one stream read by one consumer, and a command abandoned mid-flight is off that stream
// until it is reclaimed: without this it comes back after commands that arrived later,
// which for a disconnect landing behind the connect that replaced it is the account left
// in the state nobody asked for.
func (m *Manager) TakeNewlyAdopted() []string {
	m.newlyMu.Lock()
	defer m.newlyMu.Unlock()
	if len(m.newly) == 0 {
		return nil
	}
	taken := m.newly
	m.newly = nil
	return m.stillRunning(taken)
}

// stillRunning drops the sessions this instance has since let go of.
//
// A drain claims with no minimum idle, on the grounds that this instance holds the
// lease. Once it does not, that reasoning is gone: claiming would take the current
// owner's commands out from under it, dispatch them as belonging to nobody, and release
// them again on every pass.
func (m *Manager) stillRunning(sids []string) []string {
	if len(sids) == 0 {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	kept := sids[:0]
	for _, sid := range sids {
		if _, running := m.sessions[sid]; running {
			kept = append(kept, sid)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}

// ReturnAdopted puts sessions back among the ones waiting to be drained, for a caller
// that took them and could not finish. They go to the front: they were adopted before
// anything else on the list, and until their pending commands are taken over nothing
// newer for them can safely be read.
func (m *Manager) ReturnAdopted(sids []string) {
	if len(sids) == 0 {
		return
	}
	sids = m.stillRunning(sids)
	if len(sids) == 0 {
		return
	}
	m.newlyMu.Lock()
	m.newly = append(sids, m.newly...)
	m.newlyMu.Unlock()
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
	m.abandon(ctx, sid)
}

// abandon hands back the lease of a session this instance is no longer running.
//
// A release that does not reach Redis is kept and tried again on the next tick. The key
// still names this instance until it expires, and while it does every wake for that
// session finds the lease taken, is acknowledged, and retires: the session is then left
// unowned with nothing scheduled to pick it up.
func (m *Manager) abandon(ctx context.Context, sid string) {
	if _, err := m.leases.Release(ctx, sid); err != nil {
		m.log.Warn().Err(err).Str("sid", sid).Msg("could not hand a lease back; will try again")
		m.orphanMu.Lock()
		m.orphans[sid] = struct{}{}
		m.orphanMu.Unlock()
		return
	}
	m.forgetOrphan(sid)
}

// releaseOrphans retries the hand-backs that did not reach Redis.
//
// A session adopted again in the meantime is dropped from the list rather than
// released: the lease it holds now is a live one, and the release compares the instance
// and nothing else, so it would delete the lease of a session this instance is running.
func (m *Manager) releaseOrphans(ctx context.Context) {
	m.orphanMu.Lock()
	if len(m.orphans) == 0 {
		m.orphanMu.Unlock()
		return
	}
	sids := make([]string, 0, len(m.orphans))
	for sid := range m.orphans {
		sids = append(sids, sid)
	}
	m.orphanMu.Unlock()

	sort.Strings(sids)
	for _, sid := range sids {
		if ctx.Err() != nil {
			// Out of the window. What was not reached is still in the map, so the next
			// tick starts where this one stopped.
			return
		}
		m.mu.RLock()
		_, running := m.sessions[sid]
		m.mu.RUnlock()
		if running {
			m.forgetOrphan(sid)
			continue
		}
		m.abandon(ctx, sid)
	}
}

// handingBack reports whether a lease this instance failed to give up is still queued to
// be tried again.
func (m *Manager) handingBack(sid string) bool {
	m.orphanMu.Lock()
	defer m.orphanMu.Unlock()
	_, queued := m.orphans[sid]
	return queued
}

func (m *Manager) forgetOrphan(sid string) {
	m.orphanMu.Lock()
	delete(m.orphans, sid)
	m.orphanMu.Unlock()
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
		// swallow a command on its way there. Released, so it does not read as work this
		// process is still doing and become unclaimable.
		release(delivery)
		return
	}
	switch session.Offer(delivery) {
	case OfferAccepted:
	case OfferBusy:
		m.refuse(ctx, delivery, protocol.NewError(protocol.ErrorRateLimited, "the session has too many commands waiting"))
	case OfferStopped:
		// This instance is letting the account go. Refusing would answer for an owner
		// it is no longer, so the command stays pending for whoever takes it next.
		release(delivery)
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
	case err == nil:
	case errors.Is(err, cluster.ErrNotOwner):
		if m.handingBack(sid) {
			// Owned by this instance, which is running nothing and is still trying to
			// give the lease up. Acknowledging here on the grounds that somebody else has
			// it retires the only thing that would have started the session, and once the
			// stale key expires there is nothing left to start it at all.
			m.log.Warn().Str("sid", sid).
				Msg("a wake found a lease this instance is still handing back; leaving it pending")
			release(delivery)
			return
		}
	default:
		// Left unacknowledged on purpose. Every instance reads this stream through one
		// consumer group, so acknowledging a wake nobody could act on retires it: the
		// session then stays unowned until a client happens to send another. Whatever
		// stopped the adoption — a database that was away, a store that could not be
		// read — may well be over by the time this is reclaimed.
		m.log.Error().Err(err).Str("sid", sid).Msg("failed to adopt a woken session; leaving the wake pending")
		release(delivery)
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

// release says this instance is done with a delivery it did not carry out. A transport
// that does not track its own in-flight work leaves it nil, and there is nothing to say.
func release(delivery *transport.Delivery) {
	if delivery.Release != nil {
		delivery.Release()
	}
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
	// Renewals first, and nothing before them. Every hand-back is a Redis round trip
	// that can hang, and a lease left unrenewed because this goroutine was busy with
	// them is a session a peer takes while this instance still holds its socket open:
	// the cost of a hand-back arriving a tick late is one session unowned for a few
	// seconds, the cost of a renewal that never ran is every session on the instance.
	var released []string
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
		if errors.Is(err, cluster.ErrNotOwner) {
			// Somebody else's now, and Renew has already forgotten it locally. Nothing
			// to hand back, and the release only ever deletes a key naming this
			// instance, so asking would cost a round trip to be told no.
			continue
		}
		// The renewal may well have been applied and only the answer lost, which leaves
		// a key naming this instance for a full TTL after the session it named stopped.
		// Not knowing is the reason to hand it back explicitly rather than to wait the
		// key out. Done below, with everything else that is not a renewal.
		released = append(released, sid)
	}

	// What is left of the tick goes to the hand-backs, and only what a lease can spare
	// of it. One that does not fit is tried again on the next tick.
	window, cancel := context.WithTimeout(ctx, m.leases.TTL()/releaseShare)
	defer cancel()
	for _, sid := range released {
		m.abandon(window, sid)
	}
	m.releaseOrphans(window)
}

// releaseShare is the fraction of a lease one tick may spend handing leases back, which
// leaves the rest of it for the renewals that already ran and the ones on the next tick.
const releaseShare = 3

// StopAll releases every session, which is what a SIGTERM does before the process
// exits: a released lease is one a peer can take immediately instead of waiting a full
// TTL for it to expire.
func (m *Manager) StopAll(ctx context.Context) {
	for _, sid := range m.SIDs() {
		m.Release(ctx, sid)
	}
}
