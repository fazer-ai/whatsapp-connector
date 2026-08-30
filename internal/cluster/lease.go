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

// releaseScript drops the lease only while this instance holds it, and arms the
// cooldown in the same step so the instance that just let go does not immediately win
// the race to take it back.
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call("DEL", KEYS[1])
redis.call("SET", KEYS[2], ARGV[1], "PX", ARGV[2])
return 1
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
	cooldown time.Duration

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
	TTL      time.Duration
	Margin   time.Duration
	Cooldown time.Duration
	Clock    Clock
}

// NewLeases returns the lease holder for one instance id.
func NewLeases(client *redisx.Client, instance string, opts Options) *Leases {
	if opts.TTL <= 0 {
		opts.TTL = DefaultTTL
	}
	if opts.Margin <= 0 {
		opts.Margin = DefaultRenewMargin
	}
	if opts.Cooldown <= 0 {
		opts.Cooldown = opts.TTL / 3
	}
	if opts.Clock == nil {
		opts.Clock = systemClock{}
	}
	return &Leases{
		client:   client,
		instance: instance,
		ttl:      opts.TTL,
		margin:   opts.Margin,
		cooldown: opts.Cooldown,
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
	won, err := l.client.SetNX(ctx, keys.Lease(sid), l.instance, l.ttl).Result()
	if err != nil {
		return Lease{}, fmt.Errorf("cluster: acquire %s: %w", sid, err)
	}
	if !won {
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
	l.held[sid] = held{epoch: uint64(epoch), renewedAt: l.clock.Now()} //nolint:gosec // INCR from 0 never returns a negative
	l.mu.Unlock()

	return Lease{SID: sid, Epoch: uint64(epoch)}, nil //nolint:gosec // same
}

// Renew extends a lease this instance holds. A renewal that finds the key owned by
// somebody else forgets the session locally, so Owned answers no from then on without
// another round trip.
func (l *Leases) Renew(ctx context.Context, sid string) error {
	keys := l.client.Keys()
	ok, err := renewScript.Run(ctx, l.client, []string{keys.Lease(sid)}, l.instance, l.ttl.Milliseconds()).Int()
	if err != nil {
		return fmt.Errorf("cluster: renew %s: %w", sid, err)
	}
	return l.applyRenew(sid, ok)
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
	cmds := renew(sids, byDigest)
	var unloaded []string
	for i, sid := range sids {
		ok, err := cmds[i].Int()
		if redis.HasErrorPrefix(err, "NOSCRIPT") {
			unloaded = append(unloaded, sid)
			continue
		}
		out[sid] = l.answerRenew(sid, ok, err)
	}
	if len(unloaded) > 0 {
		retried := renew(unloaded, byBody)
		for i, sid := range unloaded {
			ok, err := retried[i].Int()
			out[sid] = l.answerRenew(sid, ok, err)
		}
	}
	return out
}

// answerRenew turns one command's outcome into the answer Renew gives for that session.
func (l *Leases) answerRenew(sid string, ok int, err error) error {
	if err != nil {
		return fmt.Errorf("cluster: renew %s: %w", sid, err)
	}
	return l.applyRenew(sid, ok)
}

// applyRenew turns the script's answer into this instance's own record of the lease.
func (l *Leases) applyRenew(sid string, ok int) error {
	if ok != 1 {
		l.forget(sid)
		return ErrNotOwner
	}
	l.mu.Lock()
	if entry, held := l.held[sid]; held {
		entry.renewedAt = l.clock.Now()
		l.held[sid] = entry
	}
	l.mu.Unlock()
	return nil
}

// Release gives up a lease and arms the cooldown. It reports whether this instance
// was the one holding it.
func (l *Leases) Release(ctx context.Context, sid string) (bool, error) {
	l.forget(sid)
	keys := l.client.Keys()
	released, err := releaseScript.Run(
		ctx, l.client, []string{keys.Lease(sid), keys.Cooldown(sid)}, l.instance, l.cooldown.Milliseconds(),
	).Int()
	if err != nil {
		return false, fmt.Errorf("cluster: release %s: %w", sid, err)
	}
	return released == 1, nil
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
