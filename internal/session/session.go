// Package session runs one WhatsApp account: it holds the lease, pumps what the
// engine has to say onto the event stream, and carries out commands one at a time.
package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// IDFunc mints the unique id every frame carries. It is a field rather than a call to
// a uuid package so a test can read the ids it is asserting on.
type IDFunc func() string

// Session is one account, owned by this instance for as long as its lease is fresh.
type Session struct {
	sid       string
	instance  string
	lease     cluster.Lease
	leases    *cluster.Leases
	engine    engine.Session
	publisher transport.Publisher
	replier   transport.Replier
	ledger    Ledger
	store     *store.Scoped
	watch     Watch
	newID     IDFunc
	now       func() time.Time
	log       zerolog.Logger

	commands chan queued

	// seq is written by the pump goroutine alone, which is what keeps it monotonic
	// without a lock: two writers would have to agree on an order to be ordered, and
	// the order is exactly what seq is for.
	seq uint64

	stop     context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once

	// retiredOn is the giving-up this session was retired on, set by the pump once it has
	// published the emission that named it, and zero while the session is not retired at
	// all. Read by the instance's heartbeat, which is what hands the lease back, so it is
	// atomic rather than guarded: the two goroutines never touch anything else of each
	// other's.
	//
	// One word for both facts, because undoing a retirement has to be conditional on the
	// retirement it looked at. A retry can be given up on again while an undoing is in
	// flight, and a plain "not retired any more" written over that would leave a session
	// nothing will hand back and a door nothing will shut.
	retiredOn atomic.Uint64
	// finishing is the same news half a step earlier: the pump has the engine's last
	// emission in hand and has not published it yet. Publishing is a write to Redis and
	// can take a while, and the executor runs alongside the pump -- a connect waiting in
	// the queue would be carried out in that gap, dial an account this session is
	// finished with, and answer the client that it worked, moments before the heartbeat
	// stops the socket it opened.
	//
	// Kept apart from `retired` because they answer different questions. The door shuts
	// when the engine says its last word; the lease goes back only once that word is out,
	// so an emission that never landed puts the door back rather than handing an account
	// away with nothing published to say why.
	// Guarded by queueMu, with `stopping` and the queue itself: refusing a command and
	// reopening the door are each [read one, write the other], and interleaved they lose
	// the mark -- the door opens between the read and the write, and the command that was
	// turned away is left pending with no drain to take its stream back.
	//
	// It holds the giving-up the door was shut for rather than a flag, and zero when it
	// is open. Only an answer about that same giving-up opens it again: the engine can be
	// given up on afresh while an answer about the one before is on its way, and a door
	// opened by that older answer is a session marked retired and taking commands.
	shutFor uint64
	// accepted counts the deliveries this session has taken and not yet finished with,
	// from the instant `Offer` puts one in the channel to the instant it is answered,
	// released or abandoned.
	//
	// The queue's length and `running` answer that question everywhere except in the gap
	// between them: the executor takes a delivery out of the channel and then calls
	// `admit`, and a preemption in between leaves an accepted command that is in neither
	// -- an empty queue, nothing running, and a client waiting. That is the state the
	// hand-back must not read as idle.
	accepted int
	// refused says a command was turned away while the door was shut, which is what the
	// drain the reopening schedules is for.
	refused bool
	// running counts the commands the executor has taken off the queue and not answered
	// yet. A hand-back does not take a session with one in flight: its answer is not in,
	// and a connect among them may be about to tell the client that it worked.
	running int
	// owed is the emission finishing this session that did not reach the stream, kept for
	// another try. The pump's own, touched by nothing else.
	owed    *engine.Emission
	retryIn time.Duration
	// undrained marks this session as having something pending on its stream that was
	// not read by the loop's own `>`. Nil outside the manager.
	undrained func()
	// connected says the account is in the air, once per time it comes up, and resumeBad
	// says an attempt the connector made on its own did not work. Both are how the
	// backoff on a session that keeps failing is counted and cleared. Nil outside the
	// manager.
	//
	// wasOpen is the pump's own, and is what makes the first of them a transition rather
	// than an event: a session publishes its state on every reconnect whatsmeow makes,
	// and a Redis round trip per one of those is a cost with nothing behind it.
	connected func()
	resumeBad func()
	wasOpen   bool

	// afterCommand reports every command this session finished with, saying whether it
	// was a teardown and what it answered. Both halves matter to the one caller there is:
	// this session knows what it ran and why it said no, and only the manager knows
	// whether the account was opened to serve that very teardown.
	//
	// Every command and not only the refusals, so that the manager's bookkeeping is
	// ordered by the executor rather than by a count it polls: a client's command
	// arriving while an adoption waits to be undone is the thing that must cancel the
	// undoing, and this is where that is known first. Nil outside the manager.
	afterCommand func(refusal protocol.ErrorCode, teardown, happened bool)
	// carried counts the commands this session actually carried out, and exists for one
	// reader: the manager gives an account back only while nothing has been done on it
	// since the teardown it was adopted for was refused. A connect that landed behind that
	// refusal and worked makes this an account somebody wants up.
	//
	// Commands refused before they ran are left out on purpose. A late connect refused
	// `expired` is not a client using the account, and counting it would pin an account
	// nobody ever asked for to this instance for the life of the process.
	carried atomic.Int64

	// queueMu guards the door to commands rather than the channel itself: the executor
	// has to be able to say "nothing more comes in" and then empty what is left,
	// without a command slipping in between the two.
	queueMu  sync.Mutex
	stopping bool
}

// Config is what a session needs to run.
type Config struct {
	Instance  string
	Lease     cluster.Lease
	Leases    *cluster.Leases
	Engine    engine.Session
	Publisher transport.Publisher
	Replier   transport.Replier
	Watch     Watch
	Ledger    Ledger
	// Store is where this session records what its client asked for, behind the same
	// ownership arbiter every other write stands behind. Leaving it out turns that
	// record off, which only a test that is not exercising it should do: without it a
	// paired account that is in the air leaves nothing saying so, and the sweep that
	// brings accounts back reads an empty list. That was #266.
	//
	// It is a handle of this session's own rather than the engine's, and they are not
	// interchangeable by accident: both consult the same `Ownership` arbiter, so an
	// instance that lost the lease is refused on either, and the difference is the
	// manual drop the engine sets when it closes. That drop exists for device writes,
	// where two sessions writing one device is the hazard; the row this one writes is
	// not a device write and has no second writer to race.
	Store  *store.Scoped
	NewID  IDFunc
	Now    func() time.Time
	Logger zerolog.Logger
	// RetireRetry is how long to wait before saying again that this session is finished
	// with, when the first attempt did not reach the stream. Zero asks for the default.
	RetireRetry time.Duration
	// Undrained marks this session as having a command pending on its stream that the
	// loop's own read did not take. Called when a door that turned away commands opens
	// again, so the drain that keeps the session's turn is scheduled.
	Undrained func()
	// Connected is called when this session publishes a connection that is open, once
	// per time it comes up rather than once per event. It is what says the account is
	// working, which is the only thing that ends a quarantine.
	Connected func()
	// ResumeFailed is called when a command the connector sent itself failed. The only
	// one it sends is the connect that brings an account back, so this is "the attempt
	// to resume this session did not work", which is what the backoff counts.
	ResumeFailed func()
	// AfterCommand is called once this session has finished with a command and is no
	// longer running it, saying whether it was a teardown and what it answered. It is
	// what lets the manager undo an adoption that existed only to serve a teardown, and
	// what tells it when the account has become one somebody else is using.
	AfterCommand func(refusal protocol.ErrorCode, teardown, happened bool)
	// QueueDepth bounds how many commands wait for this session. Beyond it a client
	// is told the session is busy rather than being queued behind a backlog whose
	// deadlines have all passed by the time it is reached.
	QueueDepth int
}

// DefaultQueueDepth is the command backlog one session accepts.
//
// A whole claim batch with room to spare, and the headroom is the point rather than
// generosity: adopting a session dispatches everything its previous owner left pending
// -- up to a full batch per pass -- into this queue before the executor has had a turn,
// and an offer the queue cannot take is refused `rate_limited` and acknowledged. With
// the two bounds equal, a session adopted with a full batch waiting refused whatever
// arrived behind the backlog unless the executor happened to win the race, and for a
// redelivered fire-and-forget command that refusal retires it unrun.
const DefaultQueueDepth = 128

// New starts the session's two goroutines: the pump, which publishes what the engine
// emits, and the executor, which runs commands in the order they arrived.
func New(ctx context.Context, cfg *Config) *Session {
	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = DefaultQueueDepth
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.RetireRetry <= 0 {
		cfg.RetireRetry = retireRetry
	}
	runCtx, cancel := context.WithCancel(ctx)
	s := &Session{
		sid:          cfg.Lease.SID,
		instance:     cfg.Instance,
		lease:        cfg.Lease,
		leases:       cfg.Leases,
		engine:       cfg.Engine,
		publisher:    cfg.Publisher,
		ledger:       cfg.Ledger,
		store:        cfg.Store,
		watch:        cfg.Watch,
		replier:      cfg.Replier,
		newID:        cfg.NewID,
		undrained:    cfg.Undrained,
		connected:    cfg.Connected,
		resumeBad:    cfg.ResumeFailed,
		afterCommand: cfg.AfterCommand,
		retryIn:      cfg.RetireRetry,
		now:          cfg.Now,
		log:          cfg.Logger.With().Str("sid", cfg.Lease.SID).Uint64("epoch", cfg.Lease.Epoch).Logger(),
		commands:     make(chan queued, cfg.QueueDepth),
		stop:         cancel,
		done:         make(chan struct{}),
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); s.pump(runCtx) }()
	go func() { defer wg.Done(); s.execute(runCtx) }()
	go func() { wg.Wait(); close(s.done) }()

	return s
}

// SID is the session this runs.
func (s *Session) SID() string { return s.sid }

// Epoch is the ownership this session publishes under.
func (s *Session) Epoch() uint64 { return s.lease.Epoch }

// Offer hands a command to the session. It reports false when the backlog is full, so
// the caller answers `rate_limited` instead of queueing work whose deadline will have
// passed before it is reached.
// Offer hands a command to this session's executor.
//
// The three answers are three different things for the caller to do, which is why this
// is not a bool: a busy session refuses the command, a stopping one has to leave it
// pending for whoever takes the account next, and only the first two are the same from
// the client's side.
type Offer int

const (
	// OfferAccepted means the command is queued and this session owns answering it.
	OfferAccepted Offer = iota
	// OfferBusy means the queue is full. What the caller does about it depends on
	// whether anybody is still listening: a client waiting on a reply is told so, and a
	// command whose sender has gone (a redelivery, or one sent without a reply address)
	// is left pending instead of being answered into nowhere.
	OfferBusy
	// OfferStopped means this session is going away. The command is not this
	// instance's to answer or to refuse.
	OfferStopped
)

// Offer queues a command, or says why it did not.
func (s *Session) Offer(delivery *transport.Delivery) Offer {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if s.stopping {
		return OfferStopped
	}
	if s.shutFor != 0 {
		// Finishing is stopping that has not happened yet: the engine has said its last
		// word and the heartbeat hands the lease back once it is out. A connect served in
		// that window dials an account this instance gives away moments later, and answers
		// the client that it worked -- the socket is then stopped with nothing published
		// to say so. Left pending for the owner that comes next, which is what
		// OfferStopped already means.
		s.refused = true
		return OfferStopped
	}
	select {
	// Stamped on the way in, not on the way out. The wait in this queue is the caller's
	// wait, and a timer started where the work starts reports a command that spent a
	// minute behind a backlog as having taken a millisecond. It is also the instant the
	// manager stamps for the commands it answers itself, so the two halves of the same
	// histogram measure the same span.
	case s.commands <- queued{delivery: delivery, at: s.now()}:
		s.accepted++
		return OfferAccepted
	default:
		return OfferBusy
	}
}

// abandonQueue lets go of everything still waiting when the executor stops.
//
// A queued delivery has been read from Redis and not acknowledged, so as far as the
// transport is concerned this process is still carrying it out and will not reclaim it.
// Dropping it silently means the command runs nowhere until another instance claims it
// or this one restarts. Releasing says out loud that nothing here is going to run it.
func (s *Session) abandonQueue() {
	s.queueMu.Lock()
	s.stopping = true
	s.queueMu.Unlock()

	for {
		select {
		case waiting := <-s.commands:
			s.finishedWith()
			if waiting.delivery.Release != nil {
				waiting.delivery.Release()
			}
		default:
			return
		}
	}
}

// Stop ends the session and waits for both goroutines. Safe to call twice, because
// both a lost lease and a shutdown reach it.
func (s *Session) Stop() {
	s.stopOnce.Do(func() {
		s.stop()
		_ = s.engine.Close()
	})
	<-s.done
}

// Retired reports whether the engine has said this session will not come back on its own.
//
// The lease goes back on the strength of it: an instance holding a session it has stopped
// working on is an account no peer will try, which is worse than an account nobody owns.
//
// The engine is asked again here rather than taken at the pump's word, because the pump's
// word is about the moment it published. A connect taken off the queue before the door
// shut is one no door can call back, and it can put a socket up after that moment: an
// account whose socket is back is not one to hand over. Asked at the point the answer is
// acted on, and the door opens again when the answer has changed.
func (s *Session) Retired() bool {
	on := s.retiredOn.Load()
	if on == 0 {
		return false
	}
	if s.engine.Finished() == on {
		return true
	}
	// Only the retirement this call looked at, or a newer one written meanwhile is
	// undone by an answer about the one before it. Losing the race here costs a tick:
	// the session reads as retired until the next one asks again.
	if s.retiredOn.CompareAndSwap(on, 0) {
		s.reopen(on)
	}
	return false
}

// Done is closed once both goroutines have returned.
func (s *Session) Done() <-chan struct{} { return s.done }

// pump publishes what the engine emits, in the order it emits it.
//
// It is the only writer of seq, and the only publisher for this session, which is
// what makes the client's ordering hold: one shard, one consumer, one writer.
func (s *Session) pump(ctx context.Context) {
	events := s.engine.Events()
	// Stopped until something is owed, which on nearly every session is never.
	again := time.NewTimer(s.retryIn)
	again.Stop()
	defer again.Stop()
	for {
		if ctx.Err() != nil {
			// Checked ahead of the select rather than inside it, because a select whose
			// cases are both ready picks at random: a pump that is being stopped would
			// publish an event under an epoch it is giving up, or not, depending on the
			// scheduler. Stopping means stopping.
			s.abandonPending(events)
			return
		}
		select {
		case <-ctx.Done():
			s.abandonPending(events)
			return
		case <-again.C:
			owed := s.owed
			s.owed = nil
			if owed == nil {
				continue
			}
			if s.engine.Finished() != owed.Attempt {
				// Asked before it is said again, and not after: a connect that succeeded
				// while this was waiting has already published `open`, and a giving-up
				// after that one has published its own. Said now, this would arrive after
				// both and describe neither -- and the door it shuts is a door the newer
				// one is holding.
				s.log.Info().Str("type", string(owed.Type)).
					Msg("the session moved on from the outcome that did not reach the stream; dropping it")
				s.reopen(owed.Attempt)
				continue
			}
			s.carry(ctx, owed, again)
		case emission, ok := <-events:
			if !ok {
				return
			}
			s.carry(ctx, &emission, again)
		}
	}
}

// retireRetry is how long the pump waits before saying again that a session is finished
// with, when the first attempt did not reach the stream.
//
// Nothing is waiting on it: the client hears when Redis comes back, and the account is
// handed over on the heartbeat after that. Short enough that an outage of a few seconds
// costs an account a few seconds of belonging to an instance that will not use it, long
// enough that a Redis that is away is not written to on every tick of a timer.
const retireRetry = 2 * time.Second

// carry publishes one emission and decides what it leaves behind.
func (s *Session) carry(ctx context.Context, emission *engine.Emission, again *time.Timer) {
	// Before the publish, because publishing is a write to Redis and the executor runs
	// alongside this: a connect waiting in the queue would otherwise be carried out in
	// that gap, on a session the engine has already said its last word about.
	if emission.Retires && emission.Attempt != 0 {
		s.shut(emission.Attempt)
	}
	landed := s.publish(ctx, emission)
	if landed {
		s.noticeState(emission)
	}
	if !emission.Retires {
		return
	}
	if emission.Attempt == 0 || s.engine.Finished() != emission.Attempt {
		// The session has moved on from the giving-up this is about: a connect ran between
		// the engine queueing it and the pump taking it, and either put a socket back up or
		// ran into a giving-up of its own. Handing the account over on this one would tear
		// down a retry that worked, or stop the session with the newer outcome and
		// everything before it still queued.
		//
		// A mark carrying no giving-up at all is the same answer for the same reason: the
		// engine had already been taken back by a connect when the emission was made, and
		// nothing about an active session is finished.
		s.log.Info().Str("type", string(emission.Type)).
			Msg("a connect answered an outcome the engine had already given up on; keeping the session")
		s.reopen(emission.Attempt)
		return
	}
	if !landed {
		// Nobody heard it, and the engine has nothing more to say: whatsmeow publishes
		// these from the branch that keeps the socket down, so this emission is the only
		// trigger there will ever be. Dropped, the account stays owned by an instance that
		// will not use it and the client is never told why, which is the whole of what
		// this feature exists to stop.
		//
		// Kept and tried again instead, with the door still shut: nothing is carried out
		// for a session the engine has finished with, and a Redis that is away is a Redis
		// no command reaches this session through either.
		//
		// Without the callback, which has already been answered with the failure. Whoever
		// was waiting on this emission was waiting for one attempt, and telling them twice
		// is worse than not telling them again.
		kept := *emission
		kept.Settle = nil
		s.owed = &kept
		again.Reset(s.retryIn)
		s.log.Warn().Str("type", string(emission.Type)).
			Msg("the event finishing this session did not reach the stream; will say it again")
		return
	}
	// The lease goes back only now. The event says why the session is finished: handing it
	// back before the event is out lets another instance adopt the account and publish
	// under a newer epoch, which is a client dropping the explanation as stale.
	s.retiredOn.Store(emission.Attempt)
}

// noticeState says the account is working, on the transition into an open connection and
// not on every event that reports one.
//
// Read from what was published rather than asked of the engine, because what matters is
// the moment a client can act on: an account that came up and was announced. Only a state
// that reached the stream counts, which is why the caller asks after the publish.
//
// Runs on the pump, so the callback is not allowed to block on anything slow. What is
// behind it is one Redis round trip, and only on the transition.
func (s *Session) noticeState(emission *engine.Emission) {
	if emission.Type != protocol.EventSessionState {
		return
	}
	var body struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(emission.Payload, &body); err != nil {
		return
	}
	if body.State != "open" {
		s.wasOpen = false
		return
	}
	if s.wasOpen {
		return
	}
	s.wasOpen = true
	if s.connected != nil {
		s.connected()
	}
}

// reopen takes the door off a session that turned out not to be finished with.
//
// A command refused while it was shut was released rather than given back -- a session on
// its way out must not schedule a drain, which would claim its stream from under the owner
// taking it over. Released, it keeps no turn, so the newest command for this session can
// be read and run ahead of it. That is only true of a door that opens again, which is why
// the mark is put back here and nowhere else.
func (s *Session) reopen(undoing uint64) {
	s.queueMu.Lock()
	if s.shutFor != undoing {
		// Shut again since, for a giving-up this answer is not about.
		s.queueMu.Unlock()
		return
	}
	s.shutFor = 0
	refused := s.refused
	s.refused = false
	s.queueMu.Unlock()

	if refused && s.undrained != nil {
		s.undrained()
	}
}

// shut closes the door for one giving-up: the engine has said its last word and this
// session is not to carry out anything else while it goes out.
func (s *Session) shut(attempt uint64) {
	s.queueMu.Lock()
	s.shutFor = attempt
	s.queueMu.Unlock()
}

// admit takes a command in and counts it as running, or refuses it and marks the refusal.
//
// One step for all of it. A check that passed and a count that has not happened yet is a
// session that reads as having nothing running: a hand-back takes it there, and the
// command runs on a session being stopped -- a connect answering the client that it
// worked, over a socket that is closed a moment later. The refusal is marked in the same
// step for the same reason, so a door reopening cannot come between the two and lose it.
func (s *Session) admit() bool {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if s.stopping {
		s.accepted--
		return false
	}
	if s.shutFor != 0 {
		s.refused = true
		s.accepted--
		return false
	}
	s.running++
	return true
}

// finishedWith counts out a delivery this session took and will not be answering: the
// executor released it at the door, or the queue was abandoned with it still waiting.
func (s *Session) finishedWith() {
	s.queueMu.Lock()
	s.accepted--
	s.queueMu.Unlock()
}

// doneWith is the other end of `admit`: the command has been answered, and the session is
// free to be handed over.
func (s *Session) doneWith() {
	s.queueMu.Lock()
	s.running--
	s.accepted--
	s.queueMu.Unlock()
}

// leaving reports that this session's door is shut: the engine has said its last word and
// what happens next is not decided yet -- the event saying so may still be going out, or a
// command taken off the queue before the door shut may not have answered.
//
// Neither is a session to answer a wake with. Answering with it acknowledges the wake, and
// the commands behind it are then refused by the shut door and left pending for an owner
// that the heartbeat is about to stop being, with the one wake that would have started the
// account somewhere else already retired.
func (s *Session) leaving() bool {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	return s.shutFor != 0
}

// claimIdle takes a session nothing is using, for the hand-back that is about to stop it.
//
// The sibling of `claim` below, for an account adopted to serve a teardown that was
// refused before it ran: nothing about the engine ended, so that session was never
// retired and `claim` would always say no. What has to be true is the same in spirit --
// the door is not already shut, and nothing this session took is unfinished -- with one
// clause `claim` has no use for: the count the caller decided on has to still be the
// count. A command that was answered between the caller's look and this one leaves no
// trace in what is outstanding at all -- it is finished -- so the count is the only place
// it shows.
//
// Outstanding and not "running, or on the queue", and the difference is a state those two
// cannot see between them. The executor takes a delivery out of the channel and calls
// `admit` after, so a preemption in between leaves a command that has left the queue and
// not yet been counted as running: an empty channel, nothing running, a count that has not
// moved, and a client waiting on a session about to be stopped underneath it. Counted from
// the instant `Offer` puts the delivery in the channel, under the same lock, there is no
// in-between to be preempted in.
//
// Dropping the clause would not lose the command: it is released and comes back pending.
// What it buys is a client's command not taking a claim delay on the way to an account
// this instance was about to keep for it, and a teardown still on the queue not being
// handed back to the fleet that just adopted the account for it. Since #259 it buys more
// than that -- an account handed back is one a connect cannot reach until a wake -- which
// is why the state above is worth closing rather than arguing away.
func (s *Session) claimIdle(carried int64) bool {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()

	if s.stopping || s.shutFor != 0 || s.accepted > 0 {
		return false
	}
	// Asked here and not only by the caller, and this is the clause that makes the other
	// three safe rather than nearly safe. The count is read by the heartbeat a step
	// earlier, and a command can be answered in between: it leaves nothing running and
	// nothing queued, so every other clause says yes about a session that has just told a
	// client its connect worked. Under the same lock as the door, the count cannot move
	// between the question and the answer.
	if s.carried.Load() != carried {
		return false
	}
	s.stopping = true
	return true
}

// claim takes a retired session for the hand-back that is about to stop it, and says
// whether it may.
//
// The question the sweep asked a step earlier is asked once more here, with the door
// locked shut in the same step: a command taken off the queue before the door shut is one
// no door can call back, and a connect among them can put a socket up in between.
// Refused while one is still running, because its answer is not in yet and the client may
// be about to be told that it worked -- the account is swept on the next tick instead.
func (s *Session) claim() bool {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()

	if s.stopping || s.running > 0 {
		return false
	}
	on := s.retiredOn.Load()
	if on == 0 || s.engine.Finished() != on {
		return false
	}
	s.stopping = true
	return true
}

// publish writes one emission and says whether the client can be assumed to have it.
//
// False for every road that ends without a write on the stream -- a lease that moved, a
// moment that went stale, something the engine was waiting for answered first -- and for
// a write that failed. The one caller that reads it is the pump deciding whether the
// session is finished, and a session retired on an event nobody received is an account
// handed over with nothing saying why.
func (s *Session) publish(ctx context.Context, emission *engine.Emission) bool {
	if _, owned := s.leases.Owned(s.sid); !owned {
		// Publishing under a lease this instance no longer holds writes a lower epoch
		// after a higher one has already been seen, which is the one thing a client
		// cannot recover from. Dropping it costs an event; the new owner republishes
		// the state it finds.
		s.log.Warn().Str("type", string(emission.Type)).Msg("dropped an emission from a session owned elsewhere")
		settle(emission, errLostOwnership)
		return false
	}

	// outlives is the caller's own context, kept when a moment's remaining life is put
	// in front of it, so a deadline this function imposed can be told from one the caller
	// had. Nil when nothing was imposed.
	var outlives context.Context
	if emission.Expires != nil {
		// A moment, and this is the last place it can be stopped. The engine bounds what
		// it holds; everything after that -- a stream that is retrying, a Redis that is
		// coming back -- happens here, and it is exactly the delay that makes a typing
		// indicator wrong rather than late.
		//
		// The remaining life bounds the write rather than being checked before it. A
		// check alone would pass and then sit inside a Publish that outlasts the whole
		// event, which is the outage this is for.
		left := emission.Expires()
		if left <= 0 {
			s.log.Debug().Str("type", string(emission.Type)).
				Msg("dropped a transient emission that is no longer true")
			settle(emission, nil)
			return false
		}
		bounded, cancel := context.WithTimeout(ctx, left)
		defer cancel()
		outlives, ctx = ctx, bounded
	}

	if emission.Claim != nil && !emission.Claim() {
		// Something the engine was waiting for answered this first. Dropped before a
		// sequence number is spent on it, and settled as a success: the emission did what
		// it was for by not being made.
		s.log.Debug().Str("type", string(emission.Type)).
			Msg("dropped an emission that something else answered first")
		settle(emission, nil)
		return false
	}

	s.seq++
	event := protocol.Event{
		V:       protocol.Version,
		ID:      s.newID(),
		Type:    emission.Type,
		SID:     s.sid,
		Epoch:   s.lease.Epoch,
		Seq:     s.seq,
		TS:      stamped(emission, s.now()),
		Inst:    s.instance,
		Payload: emission.Payload,
	}
	err := s.publisher.Publish(ctx, &event)
	if err != nil && outlives != nil && outlives.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
		// The write was still going when the event stopped being worth making, and the
		// deadline that ended it was this function's own rather than the caller's.
		// Nothing failed, so nothing above should hear that anything did.
		s.log.Debug().Str("type", string(emission.Type)).
			Msg("gave up on a transient emission that went stale mid-write")
		settle(emission, nil)
		return false
	}
	if err != nil && ctx.Err() == nil {
		s.log.Error().Err(err).Str("type", string(emission.Type)).Msg("failed to publish an event")
	}
	if err == nil {
		err = s.stillOwned()
	}
	if err == nil && s.watch != nil && !emission.Decided.IsZero() {
		// After the write and not around it: what the issue asks for is the distance from
		// the decision to the frame being on the stream, and this is the first line from
		// which that is true. Guarded on the instant rather than on the event type, so an
		// arm that starts timing something else needs no change here and one that stops
		// leaves no silent zero behind.
		s.watch.StateDecided(s.now().Sub(emission.Decided))
	}
	settle(emission, err)
	return err == nil
}

// stamped is when the thing an event reports happened, which is what its `ts` carries.
// The engine's own reading where it gave one, and this moment where it did not: an event
// that spent time in a queue is not news from now, and a reader deciding whether a moment
// is still worth showing has only this to go on.
func stamped(emission *engine.Emission, published time.Time) int64 {
	if emission.At == 0 {
		return published.UnixMilli()
	}
	return emission.At
}

// stillOwned reports whether the epoch an event was just published under is the one
// this instance holds, and it is checked after the publish and not only before it.
//
// A lease can run out while the write is in flight, and a peer that takes the session
// publishes under a higher epoch straight away. The event then lands behind one the
// client has already seen from a newer owner, and the contract lets a client drop what
// comes from a stale owner: `wa:cursor:<sid>` is the last `epoch:seq` it processed. So
// a successful write is not proof the client has it, and an engine holding WhatsApp's
// acknowledgement on that answer would spend the redelivery that was the only way to
// get the message back. Saying so costs a redelivery, which the client deduplicates on
// the message id, and that is the trade this whole path is built on.
func (s *Session) stillOwned() error {
	lease, owned := s.leases.Owned(s.sid)
	if owned && lease.Epoch == s.lease.Epoch {
		return nil
	}
	s.log.Warn().Uint64("epoch", s.lease.Epoch).
		Msg("published an event under an epoch this instance no longer holds")
	return errLostOwnership
}

// abandonPending settles what the engine has already handed over and this pump is no
// longer going to publish. An engine waiting on a callback that never comes is one
// holding WhatsApp's acknowledgement for a message this instance is done with, and it
// would hold it until its own bound ran out rather than letting the account be
// redelivered to whoever takes the session next.
func (s *Session) abandonPending(events <-chan engine.Emission) {
	for {
		select {
		case emission, ok := <-events:
			if !ok {
				return
			}
			settle(&emission, errStopped)
		default:
			return
		}
	}
}

// Watch is what this layer reports its own work to, so a package that has no business
// knowing what Prometheus is can still be measured. Nil is nobody watching, which is
// what every test that is not about the numbers passes.
//
// It exists because the absence of it was the defect: three metrics were registered and
// never written because the packages that know those numbers had no way to reach the
// metric set (#226).
type Watch interface {
	// CommandDone is one command carried out, from being picked up to being answered.
	// `outcome` is "ok" or the contract error code it failed with, so a latency that
	// only got bad for refusals is separable from one that got bad for everything.
	CommandDone(kind protocol.CommandType, outcome string, took time.Duration)
	// LeaseLost is one session whose lease this instance no longer holds. Counted
	// where the loss is acted on, not where it is discovered, so it counts sessions
	// actually stopped rather than renewal attempts that came back unlucky.
	//
	// It names the session because a watcher may keep something per session, and this
	// is the moment that something stops being about a session this instance has. A
	// counter has no use for the name; anything labelled by session does.
	LeaseLost(sid string)
	// StateDecided is one session state this instance judged and then published, from the
	// judgement to the frame being on the stream.
	//
	// Reported only for a landing, because the question it answers is how long a client
	// waited to be told, and a client is told by a frame that arrived. The four ways
	// `publish` returns without landing are not slow publishes, they are absent ones, and
	// folding them in here would put a zero-cost drop in the same distribution as a
	// two-second wait.
	StateDecided(took time.Duration)
}

// queued is one command waiting its turn, with the instant this instance took it on.
//
// The instant travels with the delivery because the queue is where a caller's time
// actually goes: a session with a backlog answers the command in front of it first, and
// a latency measured from the far side of that queue reports the wait as nothing.
type queued struct {
	delivery *transport.Delivery
	at       time.Time
}

// Ledger remembers what a command did, so a redelivery is answered with the first
// run's result instead of carrying it out a second time. Invariant 5 in AGENTS.md is
// this and nothing else.
type Ledger interface {
	// Recall answers what a command with this key did, and whether it ran at all.
	Recall(ctx context.Context, sid, key string) (json.RawMessage, bool, error)
	// Remember records what a command did, without overwriting an earlier answer.
	Remember(ctx context.Context, sid, key string, result json.RawMessage) error
}

// errStopped is what an emission this pump stopped before publishing settles with.
var errStopped = errors.New("session: the pump stopped before the event was published")

// errLostOwnership is what an emission dropped for a session this instance no longer
// owns settles with. The engine waiting on it has to hear something: an inbound
// message whose callback never fires is one WhatsApp is never told about either way,
// and the session would sit there until it timed out rather than letting the account
// be redelivered to whoever owns it now.
var errLostOwnership = errors.New("session: dropped an emission from a session owned elsewhere")

// settle reports a publish outcome to an engine that asked for one. Most emissions do
// not: they are things the client is told about, not things WhatsApp is waiting on.
func settle(emission *engine.Emission, err error) {
	if emission.Settle != nil {
		emission.Settle(err)
	}
}

// execute runs one command at a time, which is what "commands for a session happen in
// the order they were sent" means. A slow send delays the next command of the same
// session and nothing else.
func (s *Session) execute(ctx context.Context) {
	defer s.abandonQueue()
	for {
		// Checked before the select, and not only inside it. With both cases ready the
		// select picks either one, so a session that is already stopping could start one
		// more command and answer it on behalf of an account this instance no longer
		// owns, instead of leaving it for whoever does.
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case waiting := <-s.commands:
			delivery := waiting.delivery
			if !s.admit() {
				// Queued before the engine finished with the session, which `Offer` can
				// no longer refuse because it was already taken. Carried out, a connect
				// waiting here dials an account the next tick hands away and answers the
				// client that it worked. Left pending for whoever takes the account, the
				// same answer an offer refused now gets.
				release(delivery)
				continue
			}
			refusal, teardown, happened := s.run(ctx, waiting)
			s.doneWith()
			if s.afterCommand != nil {
				// After `doneWith`, so an account adopted for a teardown that will not run
				// is free to be taken back by the very next tick rather than being refused
				// on the grounds that the refusal itself is still in flight.
				s.afterCommand(refusal, teardown, happened)
			}
		}
	}
}

func (s *Session) run(ctx context.Context, waiting queued) (refusal protocol.ErrorCode, teardown, happened bool) {
	delivery := waiting.delivery
	command := delivery.Command
	log := s.log.With().Str("cmd_id", command.ID).Str("type", string(command.Type)).Logger()

	began := waiting.at
	handedBack := false
	result, recalled, err := s.carryOut(ctx, &command)
	// Reported for the two endings that answer the caller, and not for the two below
	// that hand the command back: a command given back has not been carried out, and
	// timing it would put this instance's abandoned turn into the latency of a command
	// somebody else is about to run properly.
	defer func() {
		if !handedBack {
			s.reportCommand(&command, began, err)
		}
	}()
	if errors.Is(err, errUnknownWhetherItRan) {
		// Neither answered nor retired: nobody knows whether this already ran, and both of
		// the other choices are wrong. Handed back so whoever claims it next can ask again
		// once the record is readable.
		log.Warn().Err(err).Msg("gave a command back rather than risk carrying it out twice")
		forfeit(delivery)
		handedBack = true
		return "", false, false
	}
	if err != nil && ctx.Err() != nil {
		// The session went away underneath this command: the lease moved, or the process
		// is coming down. What failed is this instance's turn at it, not the command, and
		// the reply would go out on the same dead context and be dropped without a word,
		// leaving the caller with nothing and the entry retired.
		//
		// Handed back instead, which is the same trade invariant 4 makes for events: a
		// send that was cut off may already be in somebody's chat, and the next owner
		// resends under the id the caller picked, so the cost is a redelivery WhatsApp
		// discards rather than a message nobody ever hears about again.
		log.Warn().Err(err).Msg("gave a command back after the session ended under it")
		forfeit(delivery)
		handedBack = true
		return "", false, false
	}
	// The answer and the acknowledgement both go out detached from the session's
	// context, because this session is exactly what may have just ended. The work is
	// over by this point and what is left has to happen: a send can succeed in the same
	// instant the lease moves, and answering that on the dying context drops the reply
	// while the acknowledgement retires the command, leaving the caller with nothing
	// about a message that is already in somebody's chat.
	//
	// Acknowledged whether the command worked or not. A command that failed has been
	// answered, and leaving it pending would have another instance run it again after
	// this one restarts, which for a send is a duplicate message rather than a retry.
	// An acknowledgement refused anyway leaves the entry marked as being carried out
	// here, on purpose, so a reclaim does not run it twice: every later claim then skips
	// it, and if this same instance adopts the session again the marker is still
	// standing. The command is neither retired nor retried until the process restarts.
	if err != nil && delivery.Internal && s.resumeBad != nil {
		// Nobody sent it and nobody is waiting for it, so this is the only place its
		// failure is counted. What counts it is the backoff: an account the connector
		// cannot bring back must not be tried again at the same rate forever.
		s.resumeBad()
	}
	// Counted on the answering paths and not on the two that hand the command back, for
	// the same reason the timing is: a command given back was not carried out here, and
	// the count exists to say whether anything but the teardown ran on this session.
	// Counted only when the command was carried out. `carryOut` turns two away before any
	// work happens -- a deadline that had passed, a lease that is not fresh -- and neither
	// is somebody using this account: counting them would have a late `session.connect`,
	// refused without running, look exactly like a client who wanted the session up, and
	// pin an account nobody ever asked for to this instance forever. That is #249 by
	// another door, and the count is the door.
	if ran(err) {
		s.carried.Add(1)
	}
	retire, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()
	s.answer(retire, &command, delivery.Internal, result, err)
	if ackErr := delivery.Ack(retire); ackErr != nil {
		log.Error().Err(ackErr).Msg("failed to acknowledge a command")
	}
	// Reported rather than acted on here, and the caller fires it once this command is no
	// longer running. Called from inside, the manager would be told the teardown is over
	// while the executor still counts it as in flight, and the first tick after the client
	// saw its answer would refuse to take the account back on those grounds -- a hand-back
	// that needs a second tick for no reason anybody can see from outside.
	if err != nil {
		// `ran` and not a flat no: a teardown the engine refused reached the account and
		// is an attempt, which is a different thing from one turned away before it began.
		return asProtocolError(err).Code, command.Type == protocol.CommandSessionDelete, ran(err)
	}
	// A recall answered from the record rather than from the account: the command is
	// finished with, and nothing happened here.
	return "", command.Type == protocol.CommandSessionDelete, !recalled
}

// ran reports whether a command got as far as doing anything.
//
// The two refusals `carryOut` makes before the work begins are the whole of the list, and
// they are the two that say nothing about the account: a deadline that had already passed
// when the command was reached, and a lease this holder has not renewed recently enough to
// answer for. Everything else -- an engine that refused, a ledger that would not answer, a
// command that worked -- happened to this account.
func ran(err error) bool {
	if err == nil {
		return true
	}
	switch asProtocolError(err).Code {
	case protocol.ErrorExpired, protocol.ErrorOwnedElsewhere:
		return false
	default:
		return true
	}
}

// carriedSoFar is how many commands this session has answered.
//
// Its one reader is the manager deciding whether an account adopted to serve a teardown
// is still an account nothing else wants: the count standing where it was when the
// teardown was queued means the refusal is all that happened.
func (s *Session) carriedSoFar() int64 { return s.carried.Load() }

func (s *Session) carryOut(ctx context.Context, command *protocol.Command) (result json.RawMessage, recalled bool, err error) {
	if _, owned := s.leases.Owned(s.sid); !owned {
		return nil, false, protocol.NewError(protocol.ErrorOwnedElsewhere, "the session moved to another instance")
	}
	if expired(command, s.now()) {
		// The caller stopped waiting before this was reached. Running it anyway is a
		// side effect nobody is expecting the outcome of.
		return nil, false, protocol.NewError(protocol.ErrorExpired, "the command deadline passed before it was reached")
	}

	key := idempotencyKey(command)
	result, done, err := s.alreadyDid(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if done {
		// Only successes are remembered, so a recalled command is one that worked -- once,
		// and not now. Said out loud to the caller, because the two are different facts
		// about the account: the record knows something ran a day ago, not that the world
		// stayed that way.
		return result, true, nil
	}

	execCtx, releaseBound := bound(ctx, command)
	defer releaseBound()

	result, err = s.lifecycle(execCtx, command)
	if err == nil && key != "" && s.ledger != nil {
		// Only a success is remembered. A failure is the caller's to try again, and a
		// remembered one would answer every later attempt with the same refusal.
		//
		// Written after the fact and not reserved before it, because a reservation
		// cannot be resolved: an entry saying an attempt was made says nothing about
		// whether it landed, so an instance reclaiming the command would have to choose
		// between dropping a message that never went out and sending one that already
		// did. What covers that window is the caller naming the message, so a resend
		// carries the id the first attempt used, and every client downstream discards a
		// repeat of an id it already has. The discarding is theirs and not WhatsApp's:
		// WhatsApp delivers the second copy in full, whatever the gap (#215). The window
		// is real; what makes it survivable is the obligation `contract/PROTOCOL.md` puts
		// on a client, and the same one the inbound path already spends freely.
		// On a context of its own, because the command's deadline may have run out in
		// the same instant the work finished, and a record that is not written is a
		// command that gets carried out again.
		//
		// Tried more than once, because the side effect has already happened and this is
		// the only thing left that can stop it happening again. A Redis that refuses one
		// call and answers the next is the common shape of a failure here, and giving up
		// on the first refusal spends the whole window on it.
		//
		// A failure that outlasts the attempts is logged and the command is still
		// answered and retired, which is the one place this layer knowingly leaves a
		// window. Giving the delivery back instead would not close it: the side effect
		// has already happened, so the redelivery would find no record and do it a
		// second time, turning a risk into a certainty. What covers what is left is the
		// caller naming the message, and the client discarding the repeat.
		s.remember(ctx, command, key, result)
	}
	return result, false, err
}

// bound gives the work the ceiling its caller asked for, out of the two the contract has.
//
// They are different requests and a command may carry either, both or neither.
// `deadline` is an instant, and the reading that matters here is the second half of it:
// having refused to start a command that arrived after it, this stops one that is still
// running when it passes. `max_runtime_ms` is a duration, measured from now, which is the
// moment the work begins -- the ledger lookup above it is not the caller's to pay for.
//
// A command carrying both gets whichever runs out first, which is what each of them
// separately asked for. The zero value of both is no ceiling at all, and that is a real
// answer rather than an oversight: a teardown is published precisely so it can sit
// pending, and a group write has no ceiling because the ledger only remembers successes,
// so a write cut off mid-flight would be reported as failed and redone.
func bound(ctx context.Context, command *protocol.Command) (context.Context, context.CancelFunc) {
	deadline := time.Time{}
	if command.Deadline > 0 {
		deadline = time.UnixMilli(command.Deadline)
	}
	if command.MaxRuntimeMs > 0 {
		ends := time.Now().Add(time.Duration(command.MaxRuntimeMs) * time.Millisecond)
		if deadline.IsZero() || ends.Before(deadline) {
			deadline = ends
		}
	}
	if deadline.IsZero() {
		return ctx, func() {}
	}
	return context.WithDeadline(ctx, deadline)
}

// remember writes the record of a command that ran, and keeps trying inside a bounded
// window rather than giving up on the first refusal.
//
// Detached from the command's context, because its deadline may have run out in the
// same instant the work finished, and a record that is not written is a command that
// gets carried out again. Bounded all the same: this runs on the session's executor, so
// every attempt is a command behind it that is not running.
func (s *Session) remember(ctx context.Context, command *protocol.Command, key string, result json.RawMessage) {
	write, cancel := context.WithTimeout(context.WithoutCancel(ctx), ledgerWindow)
	defer cancel()

	var err error
	for attempt := range ledgerAttempts {
		if attempt > 0 {
			select {
			case <-write.Done():
				// Out of window. The error from the last attempt is what gets reported.
			case <-time.After(ledgerBackoff):
			}
			if write.Err() != nil {
				break
			}
		}
		if err = s.ledger.Remember(write, s.sid, key, result); err == nil {
			return
		}
	}
	s.log.Error().Err(err).Str("cmd_id", command.ID).Str("key", key).
		Msg("carried a command out and could not record it; a redelivery will do it again")
}

// ledgerWindow bounds the whole of that, and ledgerAttempts and ledgerBackoff divide it
// up. Short in total for the same reason a single read is: the record sits in the same
// Redis the command arrived through, so one that is not answering promptly is one
// nothing else is getting through either, and the window is time the session spends not
// running the commands behind this one.
const (
	ledgerWindow   = 5 * time.Second
	ledgerAttempts = 3
	ledgerBackoff  = 200 * time.Millisecond
)

// ledgerTimeout bounds a read or a write of the record. It is short because the record
// sits in the same Redis the command arrived through: one that is not answering
// promptly is one nothing else is getting through either.
const ledgerTimeout = 3 * time.Second

// errUnknownWhetherItRan is what a command whose record cannot be read answers with. It
// never reaches a caller: the delivery is handed back instead, so whoever claims it next
// can ask again once Redis is answering.
var errUnknownWhetherItRan = errors.New("session: cannot tell whether the command has already run")

// errLeaving is what an adoption answers with when this instance is in the middle of
// finishing with the account. Not a failure: the wake it came from is left pending, and
// whoever reads it next finds an account nobody owns and starts it.
var errLeaving = errors.New("session: this instance is finishing with the account")

// alreadyDid answers a command this session has already carried out.
//
// A record that cannot be read is not a record saying no. Running the command anyway
// would carry out a side effect whose first outcome is unknown, which is the one thing
// invariant 5 forbids, so the command is neither run nor refused: it is handed back for
// another claim. The store is the same Redis the command arrived through, so one that
// cannot answer this is one the reply and the acknowledgement would not reach either,
// and the delivery was coming round again regardless.
func (s *Session) alreadyDid(ctx context.Context, key string) (json.RawMessage, bool, error) {
	if key == "" || s.ledger == nil {
		return nil, false, nil
	}
	read, cancel := context.WithTimeout(ctx, ledgerTimeout)
	defer cancel()

	result, found, err := s.ledger.Recall(read, s.sid, key)
	if err != nil {
		return nil, false, fmt.Errorf("%w: %w", errUnknownWhetherItRan, err)
	}
	if !found {
		return nil, false, nil
	}
	s.log.Info().Str("key", key).Msg("answered a redelivered command with what the first one did")
	return result, true, nil
}

// idempotencyKey is what a command is remembered under, and the empty string for one
// that names nothing to be remembered by.
//
// A command that names the message it is about to put on the wire is keyed by it,
// which is the contract's `msg:<message_id>`: the caller picks the id, so every
// redelivery of that command names the same one, and so does a resend the caller makes
// of its own accord. Everything else is keyed by the `idempotency_key` the frame
// carries, which is a field of the command and not of its payload.
func idempotencyKey(command *protocol.Command) string {
	if !command.ChangesSomething() {
		// A question, and the answer is only worth having if it is current. Answering a
		// redelivered `session.status` from a record would report the state the session
		// was in when it was first asked.
		return ""
	}
	if protocol.RepeatableCommands[command.Type] {
		// Not a question, and still not worth remembering: what it set belongs to a
		// socket that may be gone by the time the redelivery lands, and the record would
		// report a success over a connection where nothing was done.
		return ""
	}
	// Only where the id is the command's own creation. `message.download_media` also
	// carries a `message_id`, and there it names a message somebody else's command
	// created, so keying by it would answer a download with the result of the send. It
	// no longer reaches this line -- it is a question, and returns above -- and it stays
	// out of that table anyway, because the two reasons are independent.
	if command.Type.NamesItsOwnMessage() {
		var body struct {
			MessageID string `json:"message_id"`
		}
		if err := json.Unmarshal(command.Payload, &body); err == nil && body.MessageID != "" {
			return "msg:" + body.MessageID
		}
	}
	if command.IdempotencyKey != "" {
		// Prefixed, because the caller picks this string and the schema takes any: one
		// reading `msg:m1` on a logout would be answered from the record of the send of
		// m1, and report an account unlinked that is still paired.
		return "idem:" + command.IdempotencyKey
	}
	// Neither, which the contract's own `session.logout` fixture is. The command's id
	// is what is left, and it is enough for the redelivery this exists to stop: the
	// transport hands back the same entry, so the same frame arrives carrying the same
	// id. A client that sends a second command of its own gets a new id and is not
	// covered, which is what `idempotency_key` is for. Prefixed for the same reason as
	// above: a command id is a caller's string too.
	return "cmd:" + command.ID
}

// The four lifecycle commands go to the engine's own methods rather than through
// Execute: they are not requests about a live session, they are what makes one live or
// ends it, and an engine that had to recognise them inside Execute would be answering
// two different kinds of question through one door.
func (s *Session) lifecycle(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	switch command.Type {
	case protocol.CommandSessionConnect:
		var request engine.ConnectRequest
		if err := json.Unmarshal(command.Payload, &request); err != nil {
			return nil, protocol.NewError(protocol.ErrorInvalidPayload, "session.connect payload is not readable")
		}
		s.recordAsked(ctx, request)
		if err := s.engine.Connect(ctx, request); err != nil {
			return nil, err
		}
		// The contract answers connect with a connection_state. Pairing is a
		// conversation that continues on the event stream; this is where it stands
		// when the call returns.
		return s.engine.Execute(ctx, &protocol.Command{
			V: protocol.Version, ID: command.ID, Type: protocol.CommandSessionStatus, SID: command.SID,
			Payload: json.RawMessage(`{}`),
		})
	case protocol.CommandSessionDisconnect:
		s.recordAskedDown(ctx)
		return nil, s.engine.Disconnect(ctx)
	case protocol.CommandSessionLogout:
		return nil, s.engine.Logout(ctx)
	case protocol.CommandSessionDelete:
		return nil, s.tearDown(ctx)
	default:
		return s.engine.Execute(ctx, command)
	}
}

// recordAsked remembers that a client asked for this session, and what it asked for.
//
// Here rather than inside an engine, which is where it was until #266: only one engine
// wrote it, so an account running on any other left nothing behind and the sweep that
// brings accounts back had nothing to read. What the row holds is not a fact about
// WhatsApp, it is the client's request, and this is the layer the request arrives at.
//
// Before the engine is called, and that is the ordering the engine's own comment defended
// when the write lived there: after the point the request stops being one this session
// might refuse -- the payload has parsed, which is the only refusal above this line -- and
// before anything that changes the session. A record written after the connect would be a
// record missing whenever the instance died in the middle of one, and the middle of a
// connect is a network handshake with no ceiling by default.
//
// The subscription goes with it, because a resume has no other way of learning it: the
// connect a sweep synthesises is not a frame a client sent, so what it does not carry is
// not defaulted, it is absent, and the session comes back acknowledging group traffic it
// publishes nowhere.
//
// Logged rather than returned, which is the engine's judgement kept: the connection is
// what the client asked for and it is happening, and a memory that could not be written
// is a session that will not come back by itself later, which is worse than it was but
// not a reason to refuse what is working now. The next connect writes it again.
func (s *Session) recordAsked(ctx context.Context, request engine.ConnectRequest) {
	if s.store == nil {
		return
	}
	if err := s.store.PutDesiredConnected(ctx, store.Wants{
		Groups: request.Groups, CallAutoReject: request.Calls != nil && request.Calls.AutoReject,
	}); err != nil {
		s.log.Warn().Err(err).Str("sid", s.sid).
			Msg("could not record that this session should be connected; it will not be resumed on its own")
	}
}

// recordAskedDown remembers that a client asked this session to stay down.
//
// Before the socket goes, for the same reason as above and for one more: a disconnect that
// lands and is not recorded leaves a row saying the account should be up, and the next
// sweep brings back a session whose client had just asked for it to stop.
func (s *Session) recordAskedDown(ctx context.Context) {
	if s.store == nil {
		return
	}
	if err := s.store.PutDesiredDisconnected(ctx); err != nil {
		s.log.Warn().Err(err).Str("sid", s.sid).
			Msg("could not record that this session should stay down; a sweep may bring it back")
	}
}

// tearDown deletes the account and the fleet state that exists only to address it.
//
// It runs here, on the session's own executor, and that is the whole of the ordering it
// needs: the lease that fences every store write is held, the engine is open, and the
// commands before and after this one are the ones the client sent before and after it.
// Nothing above has to stop the session first -- stopping closes the engine, and the
// whatsmeow one drops the store's fence when it does, so a teardown after that deletes
// nothing at all.
func (s *Session) tearDown(ctx context.Context) error {
	if err := s.engine.Delete(ctx); err != nil {
		return err
	}
	// On a context of its own, for the same reason the answer at the end of `run` is: the
	// ceiling the client put on this command is about how long to wait on WhatsApp, and by
	// here the account is already deleted. Left on the command's context, the last write of
	// a teardown would be skipped every time the unlink was slow rather than now and then,
	// which is the difference between a counter that leaks occasionally and one that leaks
	// on exactly the sessions hardest to delete.
	drop, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()
	if err := s.leases.ForgetEpoch(drop, s.sid); err != nil {
		// Logged rather than returned, for the same reason the refused unlink is: the
		// account is deleted by now, and answering a failure asks the client to send the
		// teardown again over an account that no longer exists. What is left behind is a
		// counter of a few bytes, which is what #159 is about.
		s.log.Warn().Err(err).Msg("could not delete the epoch counter of a session that was torn down")
	}
	return nil
}

// answer replies to an RPC command, and publishes a `command.failed` event for a
// fire-and-forget one that failed: nobody is blocked on it, so a failure with no event
// would be a command that silently did nothing.
func (s *Session) answer(ctx context.Context, command *protocol.Command, internal bool, result json.RawMessage, err error) {
	if command.ReplyTo != "" {
		s.reply(ctx, command, result, err)
		return
	}
	if err == nil {
		return
	}
	failure := asProtocolError(err)
	s.logFailure(command, failure, err)
	if internal {
		// Nobody sent it, so there is nobody to tell. The event names the command it is
		// about, and a client reading one for an id it never wrote learns that something
		// it does not know about failed. What a resume that failed publishes instead is
		// what the engine publishes for any connect that failed, addressed to the session.
		return
	}
	_ = s.publish(ctx, &engine.Emission{
		Type: protocol.EventCommandFailed,
		// `command_type`, which is what the schema requires and the fixture carries. It
		// was `type` here, so every `command.failed` this connector has ever published
		// named its command under a key nobody reads: the client's own handler destructures
		// `command_type` and has always been handed nil. The contract test compares
		// fixtures against the schema and never sees a payload built at runtime, which is
		// why nothing caught it.
		Payload: mustMarshal(map[string]any{
			"command_id": command.ID, "command_type": command.Type, "error": failure,
		}),
	})
}

func (s *Session) reply(ctx context.Context, command *protocol.Command, result json.RawMessage, err error) {
	reply := protocol.Reply{V: protocol.Version, ID: command.ID, OK: err == nil, Result: result}
	if err != nil {
		reply.Error = asProtocolError(err)
		s.logFailure(command, reply.Error, err)
	}
	if replyErr := s.replier.Reply(ctx, command.ReplyTo, reply); replyErr != nil && ctx.Err() == nil {
		s.log.Error().Err(replyErr).Str("cmd_id", command.ID).Msg("failed to answer a command")
	}
}

func expired(command *protocol.Command, now time.Time) bool {
	return command.Deadline > 0 && now.UnixMilli() > command.Deadline
}

// logFailure is the only place a command's real error is written down.
//
// What the caller gets is the closed set the contract names, and for anything without a
// code of its own that is one sentence: "the connector could not carry out the command".
// It is deliberately uninformative -- an internal error's text is not something to put in
// somebody's dashboard -- which leaves this the one record of what actually happened. An
// operator who could not connect after a logout was told exactly that sentence, and this
// log had nothing at all in it, so there was nowhere left to look.
//
// `internal` is the error, and everything else is the caller being told no: a payload the
// contract refuses, a command this build does not implement, a session that moved. Those
// are answers, not faults, and logging them at error would bury the one that is.
func (s *Session) logFailure(command *protocol.Command, failure *protocol.Error, err error) {
	event := s.log.Warn()
	if failure.Code == protocol.ErrorInternal {
		event = s.log.Error()
	}
	event.Err(err).
		Str("cmd_id", command.ID).
		Str("cmd_type", string(command.Type)).
		Str("code", string(failure.Code)).
		Msg("a command failed")
}

// asProtocolError maps whatever went wrong onto the closed set the client branches on.
// An error with no code of its own becomes `internal` rather than reaching a client's
// UI as raw text.
func asProtocolError(err error) *protocol.Error {
	var coded *protocol.Error
	if errors.As(err, &coded) {
		return coded
	}
	switch {
	case errors.Is(err, engine.ErrNotSupported):
		return protocol.NewError(protocol.ErrorUnsupported, "this connector does not implement that command")
	case errors.Is(err, cluster.ErrNotOwner):
		return protocol.NewError(protocol.ErrorOwnedElsewhere, "the session moved to another instance")
	case errors.Is(err, context.DeadlineExceeded):
		return protocol.NewError(protocol.ErrorTimeout, "the command did not finish in time")
	default:
		return protocol.NewError(protocol.ErrorInternal, "the connector could not carry out the command")
	}
}

func mustMarshal(payload any) json.RawMessage {
	body, err := json.Marshal(payload)
	if err != nil {
		// Everything reaching this is a literal built two lines above, so a failure is
		// a programming error rather than a runtime one.
		panic(fmt.Sprintf("session: marshal: %v", err))
	}
	return body
}

// reportCommand hands one finished command to whoever is measuring.
//
// The outcome is the contract's own error code rather than a bare "error", because the
// question an operator actually asks is which kind of failure got slower, and `internal`
// and `not_connected` are not the same incident.
func (s *Session) reportCommand(command *protocol.Command, began time.Time, err error) {
	if s.watch == nil {
		return
	}
	outcome := "ok"
	if err != nil {
		outcome = string(asProtocolError(err).Code)
	}
	s.watch.CommandDone(command.Type, outcome, s.now().Sub(began))
}
