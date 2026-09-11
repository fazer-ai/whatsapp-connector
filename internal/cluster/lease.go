// Package cluster decides which instance owns a session.
//
// Exactly one instance may hold a WhatsApp socket for a session at a time: two of them
// on one account is what WhatsApp answers with a stream replacement, and it is what
// makes two writers race over the same keystore rows. Ownership is a Redis lease with
// an epoch, and everything else here exists to make the answer to "do I still own it"
// cheap, local, and conservative.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// ErrNotOwner is returned by an operation that requires the lease when this instance
// does not hold it any more.
var ErrNotOwner = errors.New("cluster: session is owned elsewhere")

// ErrHandingBack is ErrNotOwner from a holder that has already said it is giving the
// session up, so the answer is "not yet" rather than "somebody else is running it".
//
// It wraps ErrNotOwner because every caller that branches on ownership wants the same
// thing here: the lease is not this instance's. Only the caller that has to decide
// whether an account will still be running a moment from now -- the one holding a wake
// it would otherwise retire -- looks past that.
var ErrHandingBack = fmt.Errorf("%w: the owner is handing it back", ErrNotOwner)

// DefaultTTL is how long a lease survives without a renewal. It has to outlast a
// stop-the-world pause plus a renewal round trip, and be short enough that a session
// on a killed instance moves within the DoD's 45 seconds.
const DefaultTTL = 30 * time.Second

// DefaultRenewMargin is how much of the TTL is treated as already spent when
// answering "do I still own this". Anything inside the margin is answered no: the
// cost of a false no is a disconnect and a reclaim, the cost of a false yes is two
// live sockets on one account.
const DefaultRenewMargin = 2 * time.Second

// renewScript extends the lease only while this instance still holds it. The compare
// and the expiry have to be one operation: between a GET and a PEXPIRE the lease can
// expire and be taken, and the PEXPIRE would then extend somebody else's.
var renewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

// releaseScript drops the lease only while this instance holds it, and takes the
// hand-back mark with it in the same step: the mark says an owner is on its way to
// letting go, and the moment it has, the account is free and a peer asking should be
// told so rather than told to wait for a hand-back that already happened.
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])
return 1
`)

// acquireScript takes the lease if it is free, and otherwise says whether the instance
// holding it has marked itself as handing it back.
//
// Both in one step, because the interesting case is exactly the race between them: a
// peer reading the mark after its own acquisition failed can be beaten to it by the
// release, find nothing, and conclude the account is somebody else's -- which is the bug
// the mark exists to close, only narrower. Inside the script the two cannot interleave:
// a release that lands first makes the SET succeed, and one that lands after leaves the
// mark for this read.
//
// The mark is compared against the holder rather than merely being present. One left by
// an instance that no longer holds the lease says nothing about the one that does, and a
// wake left pending on the strength of it would bounce until the mark expired.
//
// Winning clears it, for the case the comparison cannot see through: a hand-back that
// never landed leaves a mark outliving the lease it was about, and the instance that
// wrote it can win the account back under its own name. The mark would then equal the
// holder while that holder runs the session, and every wake for it would be left pending
// until the mark expired. A lease taken afresh is the moment nothing about the one
// before it is true any more.
var acquireScript = redis.NewScript(`
if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then
  redis.call("DEL", KEYS[2])
  return 1
end
local holder = redis.call("GET", KEYS[1])
if holder and redis.call("GET", KEYS[2]) == holder then
  return 2
end
return 0
`)

// Lease is one session's ownership, as held by this instance.
type Lease struct {
	SID   string
	Epoch uint64
}

// Leases hands out and renews leases for one instance.
type Leases struct {
	client   *redisx.Client
	instance string
	ttl      time.Duration
	margin   time.Duration

	mu sync.RWMutex
	// held by value, not by pointer: Owned answers from local state on every write, and
	// an entry that can escape the lock as a pointer is one a renewal can be rewriting
	// while a caller reads it.
	held  map[string]held
	clock Clock
}

// Clock is the monotonic reading used to decide whether a lease is still fresh. It is
// a field so tests can drive it; production uses the process clock.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

type held struct {
	epoch     uint64
	renewedAt time.Time
}

// Options configures Leases. The zero value asks for the defaults.
type Options struct {
	TTL    time.Duration
	Margin time.Duration
	Clock  Clock
}

// NewLeases returns the lease holder for one instance id.
func NewLeases(client *redisx.Client, instance string, opts Options) *Leases {
	if opts.TTL <= 0 {
		opts.TTL = DefaultTTL
	}
	if opts.Margin <= 0 {
		opts.Margin = DefaultRenewMargin
	}
	if opts.Clock == nil {
		opts.Clock = systemClock{}
	}
	return &Leases{
		client:   client,
		instance: instance,
		ttl:      opts.TTL,
		margin:   opts.Margin,
		held:     make(map[string]held),
		clock:    opts.Clock,
	}
}

// Instance is the id this holder claims leases under.
func (l *Leases) Instance() string { return l.instance }

// TTL is how long a lease lives without a renewal.
func (l *Leases) TTL() time.Duration { return l.ttl }

// Acquire takes the lease for a session, or reports that somebody else has it.
//
// The epoch is incremented on every successful acquisition and travels on every event
// the session publishes, which is how a client tells the state written by the previous
// owner from the state written by this one. It has to be read after the lease is won
// and never guessed: an epoch that repeats makes a stale write indistinguishable from
// a current one.
func (l *Leases) Acquire(ctx context.Context, sid string) (Lease, error) {
	keys := l.client.Keys()
	// Dated from here, for the reason applyRenew gives and with one more round trip's
	// worth of it: Redis starts the TTL when SETNX runs, and the epoch is read after
	// that, so a lease stamped once both have answered is dated later than Redis dates
	// it -- by however long an acquisition takes, which is exactly the moment Redis is
	// slow. Owned would then keep saying yes past the moment the key expires and a peer
	// can take it. Dating it earlier only ever gives it up sooner than necessary.
	on := []string{keys.Lease(sid), keys.HandBack(sid)}
	sent := l.clock.Now()
	won, err := acquireScript.EvalSha(ctx, l.client, on, l.instance, l.ttl.Milliseconds()).Int()
	if redis.HasErrorPrefix(err, "NOSCRIPT") {
		// Sent by hand rather than left to Run, which would send it too but date the
		// lease from before the request that came back unloaded. The digest is not
		// there on the first acquisition after a restart or a SCRIPT FLUSH, and an
		// EVALSHA answered NOSCRIPT started no TTL: Redis starts it here. Dated from
		// here for the same reason RenewMany re-dates the batch it has to resend.
		sent = l.clock.Now()
		won, err = acquireScript.Eval(ctx, l.client, on, l.instance, l.ttl.Milliseconds()).Int()
	}
	if err != nil {
		return Lease{}, fmt.Errorf("cluster: acquire %s: %w", sid, err)
	}
	switch won {
	case 1:
	case 2:
		return Lease{}, ErrHandingBack
	default:
		return Lease{}, ErrNotOwner
	}

	epoch, err := l.client.Incr(ctx, keys.LeaseEpoch(sid)).Result()
	if err != nil {
		// The lease is held but has no epoch to publish under, and publishing under a
		// stale one is worse than not owning the session: let it go and try again.
		_, _ = l.Release(context.WithoutCancel(ctx), sid)
		return Lease{}, fmt.Errorf("cluster: epoch for %s: %w", sid, err)
	}

	l.mu.Lock()
	l.held[sid] = held{epoch: uint64(epoch), renewedAt: sent} //nolint:gosec // INCR from 0 never returns a negative
	l.mu.Unlock()

	return Lease{SID: sid, Epoch: uint64(epoch)}, nil //nolint:gosec // same
}

// Renew extends a lease this instance holds. A renewal that finds the key owned by
// somebody else forgets the session locally, so Owned answers no from then on without
// another round trip.
func (l *Leases) Renew(ctx context.Context, sid string) error {
	keys := l.client.Keys()
	sent := l.clock.Now()
	about := l.epochOf(sid)
	ok, err := renewScript.Run(ctx, l.client, []string{keys.Lease(sid)}, l.instance, l.ttl.Milliseconds()).Int()
	if err != nil {
		return fmt.Errorf("cluster: renew %s: %w", sid, err)
	}
	return l.applyRenew(sid, ok, sent, about)
}

// RenewMany renews every session named in one round trip, and answers for each of them
// the way Renew would.
//
// One round trip and not one per session, because this runs on the goroutine that also
// reads commands, and its budget is written in fixed durations: a heartbeat, a share of
// a lease. A renewal per session puts a term proportional to how many sessions this
// instance holds inside a bound that names none of them, so an instance carrying a few
// hundred takes longer to renew than the arithmetic allows, and the sessions at the end
// of the list are the ones handed to a peer while this instance still holds their
// sockets. Pipelined, the pass costs what one renewal costs plus the bytes, whatever
// the count.
//
// The per-command errors are what is read, not the one Pipelined returns: it reports
// the first failure, and a lease lost by one session says nothing about the next.
func (l *Leases) RenewMany(ctx context.Context, sids []string) map[string]error {
	out := make(map[string]error, len(sids))
	if len(sids) == 0 {
		return out
	}
	renew := func(sids []string, send func(redis.Pipeliner, string) *redis.Cmd) []*redis.Cmd {
		cmds := make([]*redis.Cmd, len(sids))
		_, _ = l.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for i, sid := range sids {
				cmds[i] = send(pipe, sid)
			}
			return nil
		})
		return cmds
	}
	keys := l.client.Keys()
	byDigest := func(pipe redis.Pipeliner, sid string) *redis.Cmd {
		return renewScript.EvalSha(ctx, pipe, []string{keys.Lease(sid)}, l.instance, l.ttl.Milliseconds())
	}
	byBody := func(pipe redis.Pipeliner, sid string) *redis.Cmd {
		return renewScript.Eval(ctx, pipe, []string{keys.Lease(sid)}, l.instance, l.ttl.Milliseconds())
	}

	// The digest first, which is what Run would send, and the body only for what comes
	// back unloaded. Run's own fallback cannot help inside a pipeline -- it decides on
	// an error the command does not carry until the whole batch has been sent -- and
	// falling back one session at a time would spend, on the first pass after a restart
	// or a SCRIPT FLUSH, exactly the round trip per session this exists to avoid.
	// Read before the questions go out, and answered against afterwards: a session can be
	// given up and won again while the batch is in flight, and an answer about the lease
	// before that one must not be applied to the one after it.
	about := make(map[string]uint64, len(sids))
	for _, sid := range sids {
		about[sid] = l.epochOf(sid)
	}

	sent := l.clock.Now()
	cmds := renew(sids, byDigest)
	var unloaded []string
	for i, sid := range sids {
		ok, err := cmds[i].Int()
		if redis.HasErrorPrefix(err, "NOSCRIPT") {
			unloaded = append(unloaded, sid)
			continue
		}
		out[sid] = l.answerRenew(sid, ok, err, sent, about[sid])
	}
	if len(unloaded) > 0 {
		resent := l.clock.Now()
		retried := renew(unloaded, byBody)
		for i, sid := range unloaded {
			ok, err := retried[i].Int()
			out[sid] = l.answerRenew(sid, ok, err, resent, about[sid])
		}
	}
	return out
}

// answerRenew turns one command's outcome into the answer Renew gives for that session.
func (l *Leases) answerRenew(sid string, ok int, err error, sent time.Time, about uint64) error {
	if err != nil {
		return fmt.Errorf("cluster: renew %s: %w", sid, err)
	}
	return l.applyRenew(sid, ok, sent, about)
}

// applyRenew turns the script's answer into this instance's own record of the lease.
//
// Stamped with when the renewal was sent, not with when its answer came back. Redis
// starts the new TTL when the script runs, which is before the reply is read and, in a
// pipeline, can be a whole batch before it. Stamping on arrival would date the lease
// later than Redis does and let Owned keep saying yes past the moment the key actually
// expires, which is the one direction that puts two sockets on one account. Dating it
// earlier than Redis only ever gives up the lease sooner than necessary.
// applyRenew records what a renewal answered about the lease it was sent for.
//
// `about` is the epoch this holder had when the question went out, and every answer is
// checked against it: a session can be given up and won again while a renewal is in
// flight, and the lease that comes back from that is a different one. Answered without
// the check, a refusal about the lease before it forgets the lease after it -- the
// instance then runs a session it will not renew and whose events it drops as owned
// elsewhere, and the key expires under a socket that is still open.
func (l *Leases) applyRenew(sid string, ok int, sent time.Time, about uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, held := l.held[sid]
	if held && entry.epoch != about {
		// Won again since. Whatever this says, it is not about the lease being held now.
		return nil
	}
	if ok != 1 {
		delete(l.held, sid)
		return ErrNotOwner
	}
	if held {
		entry.renewedAt = sent
		l.held[sid] = entry
	}
	return nil
}

// epochOf is the lease this holder thinks it has for a session, or zero for none.
func (l *Leases) epochOf(sid string) uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.held[sid].epoch
}

// markHandingBackScript writes the mark only while this instance still holds the lease.
//
// Fenced, and not a plain SET, because a hand-back that failed is retried on a later
// tick and the account may have moved on by then: the lease expired, a peer took it, and
// that peer is now handing it back itself. An unfenced write would replace its mark with
// this instance's name, and the comparison every reader makes -- mark against holder --
// would then say nobody is handing anything back, on an account being handed back. The
// wake that follows is acknowledged into nothing, which is the bug the mark exists for.
var markHandingBackScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call("SET", KEYS[2], ARGV[1], "PX", ARGV[2])
return 1
`)

// MarkManyHandingBack marks a whole batch in one round trip.
//
// One and not one per session, for the reason RenewMany exists: what a shutdown spends
// here is spent in front of the stops that take the sockets down, and a wait that grows
// with how many sessions the instance carries is one where the last socket outlives the
// lease a peer can already take the account on.
//
// Answers are not read back one by one. The caller learns whether the batch reached
// Redis, which is the only thing it can act on: a mark refused because the lease moved
// on is an answer, not a failure, exactly as in MarkHandingBack.
func (l *Leases) MarkManyHandingBack(ctx context.Context, sids []string) error {
	if len(sids) == 0 {
		return nil
	}
	keys := l.client.Keys()
	_, err := l.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, sid := range sids {
			markHandingBackScript.EvalSha(
				ctx, pipe, []string{keys.Lease(sid), keys.HandBack(sid)}, l.instance, l.ttl.Milliseconds(),
			)
		}
		return nil
	})
	if err == nil {
		return nil
	}
	// The digest is not loaded on the first pass after a restart or a SCRIPT FLUSH, and
	// inside a pipeline Run's own fallback cannot help: it decides on an error the command
	// does not carry until the whole batch has been sent. Resent whole rather than per
	// session, which is the round trip this exists to avoid spending N times.
	if !redis.HasErrorPrefix(err, "NOSCRIPT") {
		return fmt.Errorf("cluster: mark handing back %d sessions: %w", len(sids), err)
	}
	_, err = l.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, sid := range sids {
			markHandingBackScript.Eval(
				ctx, pipe, []string{keys.Lease(sid), keys.HandBack(sid)}, l.instance, l.ttl.Milliseconds(),
			)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("cluster: mark handing back %d sessions: %w", len(sids), err)
	}
	return nil
}

// MarkHandingBack says that this instance holds a lease it has stopped running and is
// about to give up. Release clears it, and it expires on its own after one TTL, which
// outlasts the lease it is about.
//
// It is a separate round trip on purpose: the mark is only worth anything before the
// release, and there is no arrangement in which one call both writes it and acts on it
// having been written.
func (l *Leases) MarkHandingBack(ctx context.Context, sid string) error {
	keys := l.client.Keys()
	// The answer says whether the mark was written, and there is nothing for a caller to
	// do with it: a lease this instance no longer holds is a hand-back nobody is waiting
	// to hear about, which is an answer rather than a failure.
	if _, err := markHandingBackScript.Run(
		ctx, l.client, []string{keys.Lease(sid), keys.HandBack(sid)}, l.instance, l.ttl.Milliseconds(),
	).Int(); err != nil {
		return fmt.Errorf("cluster: mark handing back %s: %w", sid, err)
	}
	return nil
}

// Release gives up a lease and clears the hand-back mark. It reports whether this
// instance was the one holding it.
func (l *Leases) Release(ctx context.Context, sid string) (bool, error) {
	l.forget(sid)
	keys := l.client.Keys()
	released, err := releaseScript.Run(
		ctx, l.client, []string{keys.Lease(sid), keys.HandBack(sid)}, l.instance,
	).Int()
	if err != nil {
		return false, fmt.Errorf("cluster: release %s: %w", sid, err)
	}
	return released == 1, nil
}

// Unleased keeps the sessions no instance holds a lease for, in the order they came in.
//
// One pipelined batch rather than a call per session, because the caller is a sweep over
// everything a store says should be running: in a fleet of several instances most of that
// list is somebody else's at any moment, and an acquisition per entry to find that out is
// a Lua call per session per pass, on every instance.
//
// It is a filter and not a decision. A lease can be taken between this answer and the
// acquisition that follows it, and the acquisition is what settles ownership; what this
// removes is the traffic of asking for accounts that are plainly already running.
func (l *Leases) Unleased(ctx context.Context, sids []string) ([]string, error) {
	if len(sids) == 0 {
		return nil, nil
	}
	keys := l.client.Keys()
	pipeline := l.client.Pipeline()
	held := make([]*redis.IntCmd, len(sids))
	for i, sid := range sids {
		held[i] = pipeline.Exists(ctx, keys.Lease(sid))
	}
	if _, err := pipeline.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("cluster: read the leases of %d sessions: %w", len(sids), err)
	}

	free := make([]string, 0, len(sids))
	for i, sid := range sids {
		count, err := held[i].Result()
		if err != nil {
			// One unreadable answer is not a reason to drop the whole pass, and it is not
			// a reason to treat the account as free either: a session whose lease could
			// not be read is one this sweep says nothing about.
			continue
		}
		if count == 0 {
			free = append(free, sid)
		}
	}
	return free, nil
}

// Compared against the lease in one step, and that is the whole of why it is a script.
// A local lease can have expired in Redis without this instance knowing -- that is what
// the renew margin exists for -- and a bare DEL sent then deletes the counter of the
// peer that has since taken the account and started counting from its own epoch. The
// next handover would restart at one, under a client cursor that is already higher.
var forgetEpochScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call("DEL", KEYS[2])
return 1
`)

// ForgetEpoch deletes a session's epoch counter, and it is for a session being deleted
// rather than handed over.
//
// The epoch is a fencing token, not a statistic: a client drops events carrying a lower
// epoch than the one it has seen, which is what keeps a late event from a previous owner
// off a session another instance is running. So the counter must never go backwards
// while an account exists, and giving the key a TTL would do exactly that -- it would
// expire while nobody held the session, the next acquisition would count from one again,
// and a stale owner still holding eight would out-rank the live one and overwrite its
// state. Kilobytes of leak against silent corruption is not a trade.
//
// Deleting it here is safe for the one reason that does not generalise: the account
// itself is gone. The credentials are deleted, the mapping with them, and there is no
// inbox on the other side left for anybody's late event to corrupt. An account paired
// again is a new one, and a count starting over is the truth about it.
func (l *Leases) ForgetEpoch(ctx context.Context, sid string) error {
	keys := l.client.Keys()
	held, err := forgetEpochScript.Run(
		ctx, l.client, []string{keys.Lease(sid), keys.LeaseEpoch(sid)}, l.instance,
	).Int()
	if err != nil {
		return fmt.Errorf("cluster: forget the epoch of %s: %w", sid, err)
	}
	if held == 0 {
		// The lease moved on while this was in flight. Somebody else owns the account and
		// the counter is theirs now, so leaving it is the right answer rather than a
		// failure: this instance no longer has anything to say about the session.
		return ErrNotOwner
	}
	return nil
}

// Freshness is how much of a lease this instance may still act on, which is the same
// clock Owned answers from: the lifetime left before the margin, and zero once that is
// gone or the lease was never held.
//
// It exists for the work that has to happen before a socket comes down. A bound written
// against the configured TTL is the right size for a lease just renewed and the wrong
// one for a lease near its end: the work would run past the moment a peer can take the
// account, with this instance still talking to WhatsApp on it.
func (l *Leases) Freshness(sid string) time.Duration {
	l.mu.RLock()
	entry, ok := l.held[sid]
	l.mu.RUnlock()
	if !ok {
		return 0
	}
	return max(l.ttl-l.margin-l.clock.Now().Sub(entry.renewedAt), 0)
}

// Owned answers whether this instance may still act on a session, from local state
// alone.
//
// Asking Redis would be the wrong thing here even though it looks more correct: the
// answer is needed on every write, the network can be exactly what is broken, and an
// answer that arrives late is an answer about the past. A lease renewed less than one
// TTL ago (minus the margin) is one this instance may still act on; anything else is
// not, whatever Redis would say.
func (l *Leases) Owned(sid string) (Lease, bool) {
	l.mu.RLock()
	entry, ok := l.held[sid]
	l.mu.RUnlock()
	if !ok {
		return Lease{}, false
	}
	if l.clock.Now().Sub(entry.renewedAt) >= l.ttl-l.margin {
		return Lease{}, false
	}
	return Lease{SID: sid, Epoch: entry.epoch}, true
}

// Owns is Owned as a plain yes or no, for a caller that needs the answer and not the
// lease: the store fences every write on it.
func (l *Leases) Owns(sid string) bool {
	_, owned := l.Owned(sid)
	return owned
}

// Held lists the sessions this instance believes it owns, fresh or not. Used by the
// renew loop, which is what turns a stale entry into a released one.
func (l *Leases) Held() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	sids := make([]string, 0, len(l.held))
	for sid := range l.held {
		sids = append(sids, sid)
	}
	return sids
}

func (l *Leases) forget(sid string) {
	l.mu.Lock()
	delete(l.held, sid)
	l.mu.Unlock()
}
