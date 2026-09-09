package session

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
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
	ledger    Ledger
	newID     IDFunc
	now       func() time.Time
	// retireRetry is how long a session waits before saying again that it is finished
	// with. A field so the tests that drive that path do not wait on the real one.
	retireRetry time.Duration
	log         zerolog.Logger

	mu       sync.RWMutex
	sessions map[string]*Session

	// handing names the accounts an adoption or a hand-back is working on, and keeps the
	// two off each other. Both go [look at the map, talk to Redis, change the map], on
	// different goroutines -- adoptions on the answer loop, the sweep on the heartbeat --
	// and interleaved the release lands after the acquisition and deletes the lease the
	// session that just started is running under: `cluster.Release` matches on the
	// instance and nothing else, so it cannot tell one of this instance's leases from the
	// next one. A peer can then take an account whose socket is still open here, which is
	// the one thing the lease exists to prevent.
	//
	// By account and not one lock for the manager: an adoption holds its account for as
	// long as a store read takes, and a fleet coming up has a wake for every account it
	// owns. One lock, and a queue of wakes would keep the sweep from ever taking a turn --
	// retired sessions renewed for as long as the backlog lasts, which is this feature
	// not happening at all.
	//
	// The heartbeat never waits on it either way: a session the sweep finds busy is swept
	// on the next tick.
	handingMu   sync.Mutex
	handingBusy map[string]struct{}

	// newly is the sessions adopted since the loop last asked, waiting to have what
	// their previous owner left pending drained before anything newer is read for them.
	//
	// It is what triggers a drain, and not a predicate about a command in hand: a
	// session is on it both because it was just adopted and because something was left
	// pending for it, and those two want opposite answers for a command arriving now.
	// Asked as "may this one be carried out", it holds back every command for a session
	// adopted a moment ago.
	newlyMu sync.Mutex
	newly   []string

	// orphans are leases of sessions this instance has stopped and could not hand back.
	// The Redis key still names this instance, and every wake for such a session is
	// answered "owned elsewhere" and acknowledged, so nobody runs it until the key
	// expires. Kept here so the next tick tries the release again.
	//
	// The value says the hand-back has been marked in Redis for peers to read, which is
	// what keeps the mark at one round trip per hand-back rather than one per attempt --
	// and what makes a mark that did not land be tried again rather than assumed.
	orphanMu sync.Mutex
	orphans  map[string]bool

	// answers is the commands this manager carries out itself, waiting on the goroutine
	// that carries them out.
	//
	// A session's commands are queued on that session and run on its own goroutine, and
	// have been all along. These three -- a wake, a ping, and the refusal of a session
	// whose queue is full -- were the ones the manager answered inline, on whichever
	// goroutine dispatched. That goroutine is the one that renews every lease this
	// instance holds, and all three block on I/O: a wake reads the store and opens the
	// engine, and every one of them ends in a round trip to Redis to retire the command.
	answers chan answer
}

// answer is one command this manager owns, and the work that finishes it.
type answer struct {
	delivery *transport.Delivery
	give     func(context.Context, *transport.Delivery)
}

// DefaultAnswerDepth is how many commands may wait on the manager's own goroutine.
//
// A whole claim batch with room to spare, for the same reason DefaultQueueDepth is: a
// reclaim pass hands over what it took in one go, and the goroutine has had no turn
// while it did. Smaller than a session's queue because only three kinds of command
// reach here and a wake is the only slow one, and because what does not fit is not
// refused but left pending, which costs a claim delay rather than an answer.
const DefaultAnswerDepth = 64

// ManagerConfig is what a manager needs.
type ManagerConfig struct {
	Instance  string
	Engine    engine.Engine
	Leases    *cluster.Leases
	Publisher transport.Publisher
	Replier   transport.Replier
	// Ledger is where a command's outcome is remembered. Leaving it out turns the
	// idempotency invariant off, which only a test that is not exercising it should do.
	Ledger Ledger
	NewID  IDFunc
	Now    func() time.Time
	Logger zerolog.Logger
	// AnswerDepth bounds how many commands wait on the manager's own goroutine. The
	// zero value asks for DefaultAnswerDepth.
	AnswerDepth int
	// RetireRetry is how long a session waits before saying again that it is finished
	// with, when the first attempt did not reach the stream. Zero asks for the default.
	RetireRetry time.Duration
}

// NewManager returns a manager owning no sessions yet.
func NewManager(cfg *ManagerConfig) *Manager {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.AnswerDepth <= 0 {
		cfg.AnswerDepth = DefaultAnswerDepth
	}
	manager := &Manager{
		instance:    cfg.Instance,
		engine:      cfg.Engine,
		leases:      cfg.Leases,
		publisher:   cfg.Publisher,
		replier:     cfg.Replier,
		ledger:      cfg.Ledger,
		newID:       cfg.NewID,
		now:         cfg.Now,
		retireRetry: cfg.RetireRetry,
		log:         cfg.Logger,
		sessions:    make(map[string]*Session),
		orphans:     make(map[string]bool),
		handingBusy: make(map[string]struct{}),
		answers:     make(chan answer, cfg.AnswerDepth),
	}
	return manager
}

// running is every session this instance holds, by sid, as it stands now.
func (m *Manager) running() map[string]*Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sessions := make(map[string]*Session, len(m.sessions))
	for sid, session := range m.sessions {
		sessions[sid] = session
	}
	return sessions
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

// releaseTimeout bounds one round trip of hand-back work: the release after an adoption
// that could not finish, and the mark that goes in front of a stop. Short, because
// nothing waits on either and the lease expires on its own anyway.
const releaseTimeout = 2 * time.Second

// Adopt takes a session over: wins the lease, opens it on the engine, and starts it.
// It returns cluster.ErrNotOwner when another instance holds it, which is the ordinary
// answer in a fleet and not a failure.
func (m *Manager) Adopt(ctx context.Context, sid string) (*Session, error) {
	if !m.tryHoldHanding(sid) {
		// The heartbeat is handing this very account back. Waiting for it would hold every
		// wake and ping behind it on this goroutine, and what is being waited for is an
		// account this instance is giving up: the wake is left pending, and whoever reads
		// it next finds an account nobody owns.
		m.log.Info().Str("sid", sid).
			Msg("a wake found an account this instance is handing back; leaving it pending")
		return nil, errLeaving
	}
	defer m.dropHanding(sid)

	m.mu.RLock()
	existing, running := m.sessions[sid]
	m.mu.RUnlock()
	if running {
		if existing.leaving() {
			// On its way out, whether or not that is settled yet: the event saying the
			// engine finished with it may still be going out, a command it took before
			// the door shut may not have answered, or the heartbeat may simply not have
			// come round.
			//
			// Left pending rather than answered, and rather than handed back here to make
			// room for a fresh session. Answering with it acknowledges the wake, and the
			// commands behind it are then refused by a door this instance is about to
			// stop being the owner of. Handing it back and taking it again in one step is
			// worse: nothing keeps the instance that just let go from winning the account
			// straight back, and for a build WhatsApp will not talk to that is the whole
			// point -- the retry has to be free to land on a peer whose image can succeed,
			// and it cannot if the instance that cannot has already taken it. The sweep
			// hands the account back on the next tick, and the wake is then a wake for an
			// account nobody owns.
			m.log.Info().Str("sid", sid).
				Msg("a wake found an account this instance is finishing with; leaving it pending")
			return nil, errLeaving
		}
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
		Publisher: m.publisher, Replier: m.replier, Ledger: m.ledger,
		NewID: m.newID, Now: m.now, Logger: m.log,
		Undrained: func() { m.undrained(sid) }, RetireRetry: m.retireRetry,
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
	// Under the same lock as the insert, and this is the ordering the whole file turns
	// on: between the two there used to be a moment where the session was running and
	// not yet waiting to be drained. Nothing could observe it while adoption ran on the
	// reader's own goroutine; the moment it does not, a reader looking into that gap
	// would find the session in SIDs, miss it among the newly adopted, and read `>` for
	// it ahead of everything its previous owner left pending.
	m.newlyMu.Lock()
	m.newly = append(m.newly, sid)
	m.newlyMu.Unlock()
	m.mu.Unlock()

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
	// Deduplicated, and what is coming back still goes first: it is the older backlog.
	// A drain that gave a command back has already put that session among the newly
	// adopted while it ran, so appending blind leaves two copies of one sid -- and the
	// next failed drain a third. Each copy is another pending-list query every claim
	// makes, on the goroutine that renews the leases, for a stream already in the list.
	m.newly = slices.DeleteFunc(m.newly, func(sid string) bool {
		return slices.Contains(sids, sid)
	})
	m.newly = append(sids, m.newly...)
	m.newlyMu.Unlock()
}

// Release stops a session and gives up its lease.
func (m *Manager) Release(ctx context.Context, sid string) {
	// Before the session stops rather than with the release, and that ordering is the
	// whole of what the mark buys. Stopping is not instant -- it closes a socket and
	// drains what the session was holding -- and throughout it the lease still names this
	// instance while nothing here runs the account. A wake landing in that gap finds an
	// owner, is acknowledged as an account already running, and the release that follows
	// leaves the account owned by nobody.
	m.givingUpAhead(ctx, sid)
	m.stopSession(sid)
	m.abandon(ctx, sid)
}

// stopSession takes a session out of the map and stops it, and talks to nobody.
//
// Separate from the hand-back that follows because their costs are not alike: this one
// closes a socket and is local, and the hand-back is a round trip that can hang. A
// shutdown does all of these first, so no account's socket waits on another account's
// Redis. The mark that says the lease is on its way back is the caller's for the same
// reason: one for the whole list rather than one per socket.
func (m *Manager) stopSession(sid string) {
	m.mu.Lock()
	session, ok := m.sessions[sid]
	delete(m.sessions, sid)
	m.mu.Unlock()
	if ok {
		session.Stop()
	}
}

// abandon hands back the lease of a session this instance is no longer running.
//
// A release that does not reach Redis is kept and tried again on the next tick. The key
// still names this instance until it expires, and while it does every wake for that
// session finds the lease taken, is acknowledged, and retires: the session is then left
// unowned with nothing scheduled to pick it up.
func (m *Manager) abandon(ctx context.Context, sid string) {
	// Marked before the round trip and not after it fails. A wake that lands while this
	// is in flight finds the lease taken and this instance running nothing, and the mark
	// is the only thing that stops it from being acknowledged as somebody else's: the
	// release then lands, and the account is left owned by nobody with the one wake that
	// would have started it already retired.
	//
	// Asked again here even where the caller has already marked it, because the callers
	// that stop a session are not the only ones that reach this: an adoption that took a
	// lease and could not open the session comes through with nothing marked.
	m.givingUp(ctx, sid)
	if _, err := m.leases.Release(ctx, sid); err != nil {
		m.log.Warn().Err(err).Str("sid", sid).Msg("could not hand a lease back; will try again")
		return
	}
	m.forgetOrphan(sid)
}

// givingUp says this instance is letting a session's lease go, in the two places a wake
// is answered from.
//
// Twice, because the two readers are in different places. The local set answers for
// wakes this instance reads, without a round trip and even when Redis is the thing that
// is away; the key in Redis answers for the peers, which is most of them -- the wake goes
// to every instance and any one of them may be the one to take it.
//
// The round trip is spent once per hand-back and not once per attempt, which is what the
// value in the map records. A mark that did not land is not remembered as landed, so the
// next attempt at the release pays for it again.
func (m *Manager) givingUp(ctx context.Context, sid string) {
	m.mark(ctx, sid, m.marking())
}

// givingUpAhead is givingUp in front of the stop that takes the socket down, where what
// is left of the lease is the bound and not the TTL.
func (m *Manager) givingUpAhead(ctx context.Context, sid string) {
	m.mark(ctx, sid, m.markingAhead(sid))
}

func (m *Manager) mark(ctx context.Context, sid string, room time.Duration) {
	m.orphanMu.Lock()
	marked := m.orphans[sid]
	m.orphans[sid] = marked
	m.orphanMu.Unlock()
	if marked {
		return
	}

	// Bounded, and this is what lets the mark go in front of a stop at all. The stop is
	// what takes the socket down, and a socket still open on an account whose lease a
	// peer is free to take is the one thing the lease exists to prevent. Against the
	// lease and not only against a constant, because the TTL is configurable and the
	// point is a fraction of it: a mark that cannot be written in a fraction of a lease
	// is one the account is better off without, and the worst it costs is one wake
	// retired -- a session started late, against two live sockets on one account.
	//
	// Derived from the caller's context rather than detached from it, so a shutdown with
	// less than this left still gets the socket down inside its own grace.
	if room <= 0 {
		// Only reachable from ahead of a stop, and there the lease has nothing left to
		// spend: a peer may take the account from this moment, so the socket comes down
		// instead. The hand-back behind it marks again, on the bound that has nobody
		// waiting on it.
		return
	}
	marking, done := context.WithTimeout(ctx, room)
	err := m.leases.MarkHandingBack(marking, sid)
	done()
	if err != nil {
		// Not fatal to the hand-back, which is the part that matters and is attempted
		// anyway: an unmarked hand-back is the behaviour this instance had before the
		// mark existed, and a peer's wake can still be lost to it.
		m.log.Warn().Err(err).Str("sid", sid).
			Msg("could not mark a hand-back for peers to see; handing back anyway")
	}
	m.orphanMu.Lock()
	// Only where the session is still one this instance is giving up. A hand-back that
	// landed while this was in flight has already forgotten it, and an adoption that won
	// the account back has too: writing here would leave a live session carrying a mark
	// that says it is on its way out.
	if _, still := m.orphans[sid]; still {
		m.orphans[sid] = err == nil
	}
	m.orphanMu.Unlock()
}

// marking is how long a mark may take when nothing is waiting behind it.
func (m *Manager) marking() time.Duration {
	return min(releaseTimeout, m.leases.TTL()/ReleaseShare)
}

// markingAhead is the same for a mark that runs in front of a stop, and zero when the
// lease has no room for one at all.
//
// Against what is left of the lease and not only against its configured length. The stop
// behind the mark is what takes the socket down, and a lease near its end has less than a
// share of a TTL to give: a bound written from the TTL alone would run past the moment a
// peer may take the account, with this instance still talking to WhatsApp on it. Zero is
// the honest answer for a lease that has nothing left, and the caller stops the socket
// instead of shortening the mark into a round trip that cannot land.
//
// Only ahead of a stop. A mark that runs after the socket is already down has nobody
// waiting on it and is the only thing standing between a peer's wake and an account left
// unowned, so it is bounded by the constant and never skipped: local freshness reaching
// zero says this instance may not act on the lease, not that Redis has stopped naming it
// -- a renewal whose answer was lost leaves exactly that, and Release forgets the lease
// locally before its first attempt, so every retry would find zero.
func (m *Manager) markingAhead(sid string) time.Duration {
	return min(m.marking(), m.leases.Freshness(sid))
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
		if !m.tryHoldHanding(sid) {
			// An adoption or a hand-back of this account is under way, and the mark is
			// what a hand-back sets before its round trip: retrying alongside it is a
			// second release, which lands after the first one and after whatever lease
			// was won in between and deletes that one. Tried again on the next tick.
			continue
		}
		m.mu.RLock()
		_, running := m.sessions[sid]
		m.mu.RUnlock()
		if running {
			m.forgetOrphan(sid)
			m.dropHanding(sid)
			continue
		}
		m.abandon(ctx, sid)
		m.dropHanding(sid)
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

// GiveBack hands a delivery back without carrying it out, and remembers that the session
// it belongs to has an older entry pending again.
//
// What decides whether a site comes through here is not whether it knows which session
// the command belongs to -- that question is easy and it misleads. The queue behind `own`
// knows the sid of all three kinds it drains and only one of them may be marked. What
// decides is whether this instance goes on reading that session's stream by `>`: if it
// does, an older entry left pending there would be overtaken, and the mark is what stops
// it. A site where the answer is no releases directly and says so: an offer refused by a
// session being stopped is this instance letting the account go, and a wake rides the
// control stream, where there is no per-session turn to keep.
//
// What decides here is the delivery itself: a wake and a ping live on the control stream,
// so giving one back leaves nothing pending on a session's, and a command for a session
// this instance does not run is on its way to whoever does.
//
// The set is the one adoption uses, and reusing it is not a shortcut. Both mean the same
// thing -- there is an older command on this stream that has not been carried out -- and
// the drain already lets a session back into the `>` read only when a pass finds nothing
// left for it. A mark cleared on the first command accepted instead would let a second
// held one be overtaken by something newer.
func (m *Manager) GiveBack(delivery *transport.Delivery) {
	release(delivery)

	sid := delivery.Command.SID
	switch {
	case sid == "":
	case delivery.Command.Type == protocol.CommandSessionWake,
		delivery.Command.Type == protocol.CommandAdminPing:
	default:
		m.mu.RLock()
		_, running := m.sessions[sid]
		m.mu.RUnlock()
		if running {
			m.undrained(sid)
		}
	}
}

// undrained puts a session back among those whose stream has to be taken over before
// anything newer is read for it.
func (m *Manager) undrained(sid string) {
	m.newlyMu.Lock()
	if !slices.Contains(m.newly, sid) {
		m.newly = append(m.newly, sid)
	}
	m.newlyMu.Unlock()
}

// Dispatch routes one command. It answers by itself for the two it can answer without
// a session (`session.wake` and `admin.ping`) and hands the rest to the session.
//
// It takes no context and does no I/O, which is the whole of its contract to the caller:
// the goroutine that dispatches is the one that renews every lease this instance holds,
// and nothing routed here may hold it up. A session's command is offered to that
// session's queue, and the three this manager owns are queued on its own goroutine.
//
// It reports whether the command was left pending for somebody to run later, which is
// what a caller holding the rest of a batch needs: nothing newer for that session may be
// dispatched behind one that stayed pending.
func (m *Manager) Dispatch(delivery *transport.Delivery) (pending bool) {
	command := delivery.Command
	switch command.Type {
	case protocol.CommandSessionWake:
		return m.own(delivery, m.wake)
	case protocol.CommandAdminPing:
		return m.own(delivery, m.pong)
	}

	m.mu.RLock()
	session, running := m.sessions[command.SID]
	m.mu.RUnlock()

	if !running {
		// Not ours. Leaving it un-acknowledged is the point: the instance that does own
		// the session reads the same stream, and an instance that owns nothing must not
		// swallow a command on its way there. Released, so it does not read as work this
		// process is still doing and become unclaimable.
		//
		// Released rather than given back, and the distinction is the whole of it: the
		// mark says "this instance left something pending on a stream it reads", and this
		// instance does not read this one.
		release(delivery)
		return true
	}
	switch session.Offer(delivery) {
	case OfferAccepted:
	case OfferBusy:
		if !m.stillHeard(&command, delivery.Redelivered) {
			// Backpressure is an answer, and an answer only ends a command while somebody
			// is listening for it. Refusing into nowhere is not backpressure: it retires
			// the only copy of a command nobody ran, with no reply and no `command.failed`
			// to say so.
			//
			// Left pending instead, with its age, so the queue that is full now can take
			// it when it drains. Given back rather than released, unlike the stopping
			// session below: this instance still runs this one and goes on reading its
			// stream, so without the mark the first command the queue accepts after it
			// drains would overtake the one held here.
			m.GiveBack(delivery)
			return true
		}
		return m.own(delivery, func(ctx context.Context, busy *transport.Delivery) {
			m.refuse(ctx, busy, protocol.NewError(protocol.ErrorRateLimited, "the session has too many commands waiting"))
		})
	case OfferStopped:
		// This instance is letting the account go. Refusing would answer for an owner
		// it is no longer, so the command stays pending for whoever takes it next.
		//
		// Released rather than given back, and here it matters rather than merely reads
		// better: marking would schedule a drain for an account this instance is giving
		// up, and the drain claims the stream with no minimum idle time -- taking entries
		// the new owner already holds and handing them back at age zero, below the idle
		// floor its own reclaim watches. It stays pending for the owner instead.
		release(delivery)
		return true
	}
	return false
}

// own queues a command for the goroutine that carries out what this manager answers
// itself, and never blocks doing it. It reports whether the command was left pending.
//
// A queue with no room leaves the delivery pending rather than refusing it: released,
// age kept, so a later pass brings it back -- to this instance once it has caught up, or
// to a peer. That is the right trade for all three. A wake left pending is a session
// started a claim delay later; refused, it would be a session nobody starts at all. A
// ping left pending is answered on the redelivery; refused, its caller is told this
// instance is busy, which is true but is not what it asked. And the queue-full refusal
// is itself the answer to a session that is behind -- dropping it on a manager that is
// also behind would retire, unrun, a command the client never heard about.
func (m *Manager) own(delivery *transport.Delivery, give func(context.Context, *transport.Delivery)) bool {
	select {
	case m.answers <- answer{delivery: delivery, give: give}:
		return false
	default:
		m.log.Warn().Str("cmd_id", delivery.Command.ID).Str("type", string(delivery.Command.Type)).
			Msg("no room to carry out a command this instance answers itself; leaving it pending")
		// Given back rather than released, and this is the site that needs it: a wake and
		// a ping ride the control stream and carry no session's turn, but a refusal that
		// found no room is a command for a session this instance runs and goes on reading
		// by `>`. Released, the next command its queue accepts would overtake it.
		m.GiveBack(delivery)
		return true
	}
}

// Answer carries out the commands this manager owns, on a goroutine of its own. The
// returned channel closes once it has stopped.
//
// The context is the instance's lifetime, which is what gives the work back the bounds
// it is written against: an adoption gets AdoptTimeout rather than whatever was left of
// a tick window, and an acknowledgement gets its own two seconds without spending a
// renewal's. Neither bound moved; what moved is the goroutine they are measured on.
func (m *Manager) Answer(ctx context.Context) <-chan struct{} {
	stopped := make(chan struct{})
	// The work outlives the cancellation that stops the loop taking more on. Every step
	// it runs is bounded on its own -- AdoptTimeout for an adoption, ackTimeout for a
	// reply or a retirement -- so a command already being carried out when the instance
	// begins to stop is finished rather than torn in half, and whoever waits on the
	// channel below waits for exactly that. Under the caller's context instead, a
	// shutdown would cut an adoption between the store and the lease.
	carrying := context.WithoutCancel(ctx)
	go func() {
		defer close(stopped)
		for {
			// Asked before the select rather than inside it. A select with both arms
			// ready picks between them at random, so a command queued behind one being
			// carried out was sometimes carried out too and sometimes given back --
			// which is a session adopted onto an instance that is going away, on a coin
			// toss. Once the instance is stopping, nothing more is started.
			if ctx.Err() != nil {
				m.releaseQueued()
				return
			}
			select {
			case <-ctx.Done():
				// Given back rather than dropped. These are commands nobody has carried
				// out, and a delivery this process forgets about while still holding it
				// is one no reclaim can take: the instance is going away, so the sooner
				// they read as untouched the sooner a peer runs them.
				m.releaseQueued()
				return
			case work := <-m.answers:
				// Asked again here, and not only above: a cancellation landing while this
				// was blocked on the receive leaves both arms ready, and the random pick
				// is the same coin toss as before -- a command dequeued after the
				// instance began to stop, adopting a session onto one that is going away.
				if ctx.Err() != nil {
					m.GiveBack(work.delivery)
					m.releaseQueued()
					return
				}
				work.give(carrying, work.delivery)
			}
		}
	}()
	return stopped
}

// releaseQueued hands back everything still waiting, without carrying any of it out.
func (m *Manager) releaseQueued() {
	for {
		select {
		case work := <-m.answers:
			m.GiveBack(work.delivery)
		default:
			return
		}
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
	case errors.Is(err, errLeaving):
		// Not this instance's turn to answer: it is giving the account up, and the wake is
		// what starts it again once nobody owns it. Released rather than forfeited, so it
		// keeps its age -- the account has been unowned since it was sent.
		release(delivery)
		return
	case errors.Is(err, cluster.ErrNotOwner):
		if errors.Is(err, cluster.ErrHandingBack) || m.handingBack(sid) {
			// Owned by an instance that is running nothing and is still trying to give
			// the lease up, whether that is this one or a peer. Acknowledging here on the
			// grounds that somebody has it retires the only thing that would have started
			// the session, and once the stale key expires there is nothing left to start
			// it at all.
			m.log.Warn().Str("sid", sid).
				Msg("a wake found a lease that is still being handed back; leaving it pending")
			// A wake rides the control stream, not a session's, so there is no per-session
			// turn to keep and nothing to mark.
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
		// Forfeited rather than released: this instance took its turn at it. Keeping the
		// age would put it back at the head of the pending list, where this instance
		// takes it first again on the next pass, and again — and the wakes behind it,
		// which are the sessions nobody is running either, would never get a turn.
		forfeit(delivery)
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
		// Bounded here, the way a session bounds its own reply, and for a reason this
		// used to get for free: the caller's context was a tick window, so a Redis that
		// hung could not hold this past one. It is the manager's own goroutine now, and
		// its context is the instance's lifetime -- long enough that an unbounded reply
		// would stop every wake behind it, for as long as Redis stayed away.
		answered, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
		err := m.replier.Reply(answered, command.ReplyTo, protocol.Reply{
			V: protocol.Version, ID: command.ID, OK: true, Result: result,
		})
		cancel()
		if err != nil {
			m.log.Error().Err(err).Msg("failed to answer admin.ping")
		}
	}
	m.ack(ctx, delivery)
}

func (m *Manager) refuse(ctx context.Context, delivery *transport.Delivery, failure *protocol.Error) {
	command := delivery.Command
	if command.ReplyTo != "" {
		// Bounded for the same reason pong's is: see there.
		answered, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
		_ = m.replier.Reply(answered, command.ReplyTo, protocol.Reply{
			V: protocol.Version, ID: command.ID, OK: false, Error: failure,
		})
		cancel()
	}
	m.ack(ctx, delivery)
}

// stillHeard reports whether a refusal for this command would reach anybody.
//
// The caller's own deadline is the best evidence there is, and it outranks where the
// command came from: a handoff hands a command back within milliseconds (ClaimSessions
// claims with no minimum idle at all), and the caller that sent it is still blocked on
// its reply. Refusing that one is backpressure the caller acts on; leaving it pending
// would hold it until its queue drains or its own timeout runs out, having told it
// nothing.
//
// Where no deadline was declared, provenance is the only evidence left. A command read
// with `>` has just arrived, so somebody is waiting on the other end of it. A redelivered
// one has been round the pending list since its holder died or lost the session, and its
// reply list may not even exist any more.
func (m *Manager) stillHeard(command *protocol.Command, redelivered bool) bool {
	if command.ReplyTo == "" {
		return false
	}
	if command.Deadline > 0 {
		return !expired(command, m.now())
	}
	return !redelivered
}

// release says this instance is done with a delivery it did not carry out. A transport
// that does not track its own in-flight work leaves it nil, and there is nothing to say.
func release(delivery *transport.Delivery) {
	if delivery.Release != nil {
		delivery.Release()
	}
}

// forfeit gives a command back the way an instance that tried it and failed gives it
// back: still pending, but without the place in the queue that a command nobody has
// touched keeps.
func forfeit(delivery *transport.Delivery) {
	if delivery.Forfeit != nil {
		delivery.Forfeit()
		return
	}
	release(delivery)
}

// ackTimeout bounds retiring a command that has already been carried out. Short,
// because nothing waits on it.
const ackTimeout = 2 * time.Second

// ack retires a command, on a deadline of its own rather than the caller's.
//
// A batch is dispatched under a budget, so a command that finishes as that budget runs
// out would have its acknowledgement refused by the deadline rather than by Redis. That
// is not a retry: an entry whose ack did not land stays marked as being carried out here,
// on purpose, so that a reclaim does not run it a second time — and every later claim
// then skips it. In a fleet of one it is a command nothing retires until the process
// restarts. The work is already done by this point; what is left is bookkeeping that has
// to happen whether or not the batch had time for it.
func (m *Manager) ack(ctx context.Context, delivery *transport.Delivery) {
	retire, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()
	if err := delivery.Ack(retire); err != nil {
		m.log.Error().Err(err).Str("cmd_id", delivery.Command.ID).Msg("failed to acknowledge a command")
	}
}

// RenewAll keeps the leases of running sessions alive and tears down whatever this
// instance has lost. It is the loop that turns "the lease expired" into "the socket is
// closed", which is what keeps two instances off one account.
func (m *Manager) RenewAll(ctx context.Context, by time.Time) {
	// Renewals first, and nothing before them. Every hand-back is a Redis round trip
	// that can hang, and a lease left unrenewed because this goroutine was busy with
	// them is a session a peer takes while this instance still holds its socket open:
	// the cost of a hand-back arriving a tick late is one session unowned for a few
	// seconds, the cost of a renewal that never ran is every session on the instance.
	//
	// All of them in one round trip, so what the pass costs does not grow with how many
	// sessions this instance carries. The tearing down that follows is per session by
	// nature -- each one stops its own socket -- but it only touches the ones a renewal
	// refused, which on an ordinary tick is none.
	// By session and not by sid alone: an adoption replaces a retired session with a
	// fresh one under the same sid, on another goroutine, and an answer about the session
	// before it must not be carried out on the one that replaced it.
	running := m.running()
	sids := make([]string, 0, len(running))
	for sid := range running {
		sids = append(sids, sid)
	}
	renewals := m.leases.RenewMany(ctx, sids)

	var released []string
	for _, sid := range sids {
		err := renewals[sid]
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
		session, still := m.drop(sid, running[sid])
		if !still {
			// Adopted again since the renewal went out, which means a lease won after
			// this answer was already stale. Stopping that session would leave an account
			// nobody runs, and handing its lease back would delete a live one.
			m.log.Warn().Str("sid", sid).
				Msg("a session was adopted again while its renewal was in flight; leaving the new one alone")
			continue
		}
		if errors.Is(err, cluster.ErrNotOwner) {
			// Somebody else's now, and Renew has already forgotten it locally. Nothing
			// to hand back and nothing to mark: both keys only ever answer to this
			// instance's own name, so either would cost a round trip to be told no.
			session.Stop()
			continue
		}
		// The renewal may well have been applied and only the answer lost, which leaves
		// a key naming this instance for a full TTL after the session it named stopped.
		// Not knowing is the reason to hand it back explicitly rather than to wait the
		// key out. Done below, with everything else that is not a renewal.
		//
		// Stopped first, and this is the one place the mark does not go before the stop.
		// Everything that gets here failed to renew, so either the lease is gone or it is
		// past being fresh, and Redis is often the reason: a mark asked for here waits
		// out a network that is not answering, once per session, while the sockets those
		// leases were covering are still open and peers are free to take the accounts.
		// Not knowing is the reason to let go, not a reason to hold on, and the window
		// the mark covers is at most the margin's worth of lease that is left.
		session.Stop()
		released = append(released, sid)
	}

	// What is left of the tick goes to the hand-backs, and only what a lease can spare
	// of it. One that does not fit is tried again on the next tick.
	window, cancel := context.WithDeadline(ctx, by)
	defer cancel()
	for _, sid := range released {
		if !m.tryHoldHanding(sid) {
			// An adoption of this account is under way. Handing back alongside it deletes
			// the lease it wins; the mark `abandon` leaves is what brings this back on a
			// later tick.
			//
			// Put back rather than left alone, because the adoption in the way may be the
			// thing that cleared it. Marked as unwritten: what the adoption did to the key
			// in Redis is not knowable from here.
			m.orphanMu.Lock()
			m.orphans[sid] = false
			m.orphanMu.Unlock()
			continue
		}
		m.abandon(window, sid)
		m.dropHanding(sid)
	}
	m.releaseOrphans(window)
}

// HandBackBy is the moment every hand-back in one tick has to be done by, counted from
// the call rather than per pass.
//
// One deadline and not one per pass, because the tick has two: the renewals hand back
// what they lost, and the sweep hands back what the engine finished with. Two windows of
// a third of a lease each, back to back, is a renewal that can be two thirds of a lease
// late -- and the startup check that decides whether a lease TTL is configurable at all
// prices in exactly one of them (app.Config, "the lease hand-back tail"). A second one
// nobody priced is a lease lost under load, which is a peer running an account whose
// socket this instance still holds open.
func (m *Manager) HandBackBy() time.Time {
	return m.now().Add(m.leases.TTL() / ReleaseShare)
}

// ReleaseShare is the fraction of a lease one tick may spend handing leases back, which
// leaves the rest of it for the renewals that already ran and the ones on the next tick.
// Exported because the startup timing check is built on it, and the check has to be the
// same arithmetic as the wiring: a bound the check reads from a constant of its own is a
// bound that drifts the day either one is tuned.
const ReleaseShare = 3

// StopAll releases every session, which is what a SIGTERM does before the process
// exits: a released lease is one a peer can take immediately instead of waiting a full
// TTL for it to expire.
func (m *Manager) StopAll(ctx context.Context) {
	sids := m.SIDs()
	// All of them in one round trip, ahead of every stop, and this is the shape rather
	// than a mark per Release because the releases are serial: a Redis that answers
	// nothing would otherwise be waited out once per session, in front of each socket in
	// turn, and the last one on the list would still be talking to WhatsApp long after
	// the lease a peer can take its account on had expired.
	//
	// What a mark per session would have spent is then not spent at all: the stops below
	// go straight through, and the hand-backs behind them carry whatever retry the batch
	// still deserves.
	ahead, ending := m.roomToMark(sids)
	// The leases with nothing left to give come down before anything blocks on Redis at
	// all. No mark could outlive them, so waiting for one -- even a batch they are not in
	// -- buys nothing and spends the last of a lease a peer is about to be free to take,
	// with this instance still talking to WhatsApp on the account.
	for _, sid := range ending {
		m.stopSession(sid)
	}
	m.givingUpAll(ctx, ahead)
	for _, sid := range ahead {
		m.stopSession(sid)
	}
	// Only now, with every socket already down. A hand-back is a round trip that can
	// hang, and one interleaved with the stops is an account still talking to WhatsApp
	// because the account before it is waiting on a Redis that does not answer.
	for _, sid := range sids {
		m.abandon(ctx, sid)
	}
}

// roomToMark splits a shutdown's list into the leases worth marking before their stops
// and the ones that have to be stopped first.
//
// Local, and deliberately so: it decides the order everything below runs in, and a split
// that had to ask Redis would be one more thing in front of the sockets it is protecting.
func (m *Manager) roomToMark(sids []string) (ahead, ending []string) {
	room := m.marking()
	for _, sid := range sids {
		if m.leases.Freshness(sid) < room {
			ending = append(ending, sid)
			continue
		}
		ahead = append(ahead, sid)
	}
	return ahead, ending
}

// givingUpAll marks a whole batch of hand-backs in front of their stops, under one bound
// and one round trip. Every session in it has a whole bound of lease to spend, which is
// what roomToMark decided.
func (m *Manager) givingUpAll(ctx context.Context, sids []string) {
	unmarked := make([]string, 0, len(sids))
	m.orphanMu.Lock()
	for _, sid := range sids {
		if marked := m.orphans[sid]; !marked {
			unmarked = append(unmarked, sid)
			m.orphans[sid] = false
		}
	}
	m.orphanMu.Unlock()
	if len(unmarked) == 0 {
		return
	}

	// The whole bound, not one shrunk to fit the tightest lease in the batch: a deadline
	// sized by its most nearly expired member is one that can run out before the request
	// is even sent, and then nothing in it is marked. Which leases belong here is
	// roomToMark's answer, and every one of them has a bound of its own to spend.
	marking, done := context.WithTimeout(ctx, m.marking())
	err := m.leases.MarkManyHandingBack(marking, unmarked)
	done()
	if err != nil {
		m.log.Warn().Err(err).Int("sessions", len(unmarked)).
			Msg("could not mark hand-backs for peers to see; handing back anyway")
		return
	}
	m.orphanMu.Lock()
	for _, sid := range unmarked {
		// Only where the session is still one this instance is giving up, for the reason
		// givingUp gives: an adoption that won the account back must not be left carrying
		// a mark that says it is on its way out.
		if _, still := m.orphans[sid]; still {
			m.orphans[sid] = true
		}
	}
	m.orphanMu.Unlock()
}

// SweepRetired hands back the lease of every session the engine has finished with.
//
// On the heartbeat, beside the renewals, because it is the same bookkeeping: a session
// this instance has stopped working on is renewed forever otherwise, and while the lease
// stands no peer tries the account. `RenewAll` deliberately renews without looking at
// whether a session is connected -- a renewal skipped mid-reconnect hands an account to a
// peer while this instance still holds a socket -- and this is not that check. The engine
// has said the socket is down and staying down.
//
// Releasing stops the session and hands the lease back, so the account is owned by nobody
// until a command adopts it again. Nothing here reconnects on its own, so the next attempt
// is the client's to make, and it may land on any instance.
func (m *Manager) SweepRetired(ctx context.Context, by time.Time) {
	m.mu.RLock()
	retired := make(map[string]*Session, len(m.sessions))
	for sid, session := range m.sessions {
		if session.Retired() {
			retired[sid] = session
		}
	}
	m.mu.RUnlock()

	// The same deadline `RenewAll`'s own hand-backs ran under, and what they left of it:
	// each release is a Redis round trip that can hang, and a heartbeat that spends longer
	// than a lease here is every other session on this instance left unrenewed -- peers
	// take them while the sockets are still open. One that does not fit is swept on the
	// next tick, which costs an account a few more seconds of belonging to nobody.
	window, cancel := context.WithDeadline(ctx, by)
	defer cancel()
	for sid, session := range retired {
		if window.Err() != nil {
			m.log.Warn().Str("sid", sid).
				Msg("ran out of tick before handing back a retired session; will try again")
			return
		}
		m.log.Info().Str("sid", sid).
			Msg("handing back a session the engine will not bring back on its own")
		m.releaseThis(window, sid, session)
	}
}

// releaseThis hands a session back only while it is still the one that was found.
//
// Adoptions run on the answer goroutine and this runs on the heartbeat, so between the
// two lines above a wake can release this very session, win the lease again and put a
// fresh one in its place -- Adopt does exactly that for a retired session. Released by
// sid alone, this would then stop the session that just started and delete the lease it
// is running under.
//
// The pointer answers for a replacement that has already landed; `handing` answers for
// one that is still being built, because the release that overtakes an adoption deletes
// a live lease -- `cluster.Release` matches on the instance and nothing else.
// tryHoldHanding takes the turn for one account, or says it is taken.
//
// Nobody waits on it. The heartbeat has other sessions to renew and cannot spend a tick
// on one; an adoption runs on the goroutine that answers every wake and ping, and one
// waiting here would hold all of them behind an account it has been told is on its way
// out -- which is a wake to leave pending, not one to wait for. Two adoptions of the same
// account cannot contend, being the same goroutine.
func (m *Manager) tryHoldHanding(sid string) bool {
	m.handingMu.Lock()
	defer m.handingMu.Unlock()
	if _, busy := m.handingBusy[sid]; busy {
		return false
	}
	m.handingBusy[sid] = struct{}{}
	return true
}

func (m *Manager) dropHanding(sid string) {
	m.handingMu.Lock()
	delete(m.handingBusy, sid)
	m.handingMu.Unlock()
}

// forget takes a session out of the map and stops it, unless the map no longer holds the
// one the caller was looking at.
//
// Adoptions run on the answer goroutine, and one of them replaces a retired session with
// a fresh one under the same sid. A caller acting on an answer about the session before
// it -- a renewal that was refused, a sweep that found it finished with -- would then
// stop a session that is running and leave the lease it won behind.
func (m *Manager) forget(ctx context.Context, sid string, want *Session) bool {
	session, ok := m.drop(sid, want)
	if !ok {
		return false
	}
	// Here rather than at the callers, and here rather than before the lookup, for the
	// two reasons Release gives: the stop is what opens the window, and the session this
	// is about is only settled once the lookup has agreed it is the one asked for. A mark
	// written ahead of that names a session the instance turns out to be running.
	m.givingUpAhead(ctx, sid)
	session.Stop()
	return true
}

// drop takes a session out of the map, if it is still the one the caller means, and
// hands it back to be stopped.
//
// Taking it out and stopping it are separate because what belongs between them is not
// the same everywhere: an instance giving a lease up has to say so before the stop, and
// one whose lease is already somebody else's has nothing to say and no key that would
// answer to its name.
func (m *Manager) drop(sid string, want *Session) (*Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	session, ok := m.sessions[sid]
	if !ok || session != want {
		return nil, false
	}
	delete(m.sessions, sid)
	return session, true
}

func (m *Manager) releaseThis(ctx context.Context, sid string, want *Session) {
	if !m.tryHoldHanding(sid) {
		// An adoption of this account is under way. Tried and not taken: this runs on the
		// heartbeat, and an adoption reads a store.
		m.log.Debug().Str("sid", sid).
			Msg("an adoption is under way; leaving a retired session for the next tick")
		return
	}
	defer m.dropHanding(sid)

	// Asked again with the turn in hand, and the session locked shut in the same step,
	// because the sweep found it a step earlier: a command taken off its queue before the
	// door shut is one no door can call back, and a connect among them can put a socket up
	// in between. Stopping it then would close a socket the client has just been told is
	// open, and hand back the lease it is running under.
	if !want.claim() {
		m.log.Info().Str("sid", sid).
			Msg("a session finished with was not free to be handed over; leaving it for the next tick")
		return
	}
	if !m.forget(ctx, sid, want) {
		return
	}
	m.abandon(ctx, sid)
}
