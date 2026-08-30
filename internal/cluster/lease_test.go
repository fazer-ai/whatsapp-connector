package cluster_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// fakeClock is driven by the test rather than by the wall clock: a lease expiring is
// the behaviour under test, and sleeping for it would make the suite slow and flaky.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Two instances against one Redis, which is the only configuration where the
// interesting bugs live.
func newFleet(t *testing.T, clock cluster.Clock) (server *miniredis.Miniredis, a, b *cluster.Leases) {
	t.Helper()
	server = miniredis.RunT(t)
	build := func(instance string) *cluster.Leases {
		rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), instance, cluster.Options{Clock: clock})
	}
	return server, build("inst-a"), build("inst-b")
}

func TestAcquireIsExclusive(t *testing.T) {
	t.Parallel()

	_, a, b := newFleet(t, newClock())
	ctx := context.Background()

	lease, err := a.Acquire(ctx, "s1")
	if err != nil {
		t.Fatalf("a.Acquire: %v", err)
	}
	if lease.Epoch != 1 {
		t.Errorf("first epoch = %d, want 1", lease.Epoch)
	}

	if _, err := b.Acquire(ctx, "s1"); !errors.Is(err, cluster.ErrNotOwner) {
		t.Fatalf("b.Acquire while a holds it = %v, want ErrNotOwner", err)
	}
	if _, owned := b.Owned("s1"); owned {
		t.Error("b believes it owns a session it never won")
	}
}

// The epoch is what tells a client "this state is from the new owner". It has to move
// on every handover, or a write from the previous owner is indistinguishable.
func TestEpochRisesOnEveryHandover(t *testing.T) {
	t.Parallel()

	_, a, b := newFleet(t, newClock())
	ctx := context.Background()

	first, err := a.Acquire(ctx, "s1")
	if err != nil {
		t.Fatalf("a.Acquire: %v", err)
	}
	if _, err := a.Release(ctx, "s1"); err != nil {
		t.Fatalf("a.Release: %v", err)
	}

	second, err := b.Acquire(ctx, "s1")
	if err != nil {
		t.Fatalf("b.Acquire: %v", err)
	}
	if second.Epoch <= first.Epoch {
		t.Fatalf("epoch went %d -> %d, want strictly increasing", first.Epoch, second.Epoch)
	}
}

func TestReleaseOnlyDropsYourOwnLease(t *testing.T) {
	t.Parallel()

	_, a, b := newFleet(t, newClock())
	ctx := context.Background()

	if _, err := a.Acquire(ctx, "s1"); err != nil {
		t.Fatalf("a.Acquire: %v", err)
	}

	released, err := b.Release(ctx, "s1")
	if err != nil {
		t.Fatalf("b.Release: %v", err)
	}
	if released {
		t.Fatal("b released a lease it does not hold")
	}
	if _, owned := a.Owned("s1"); !owned {
		t.Fatal("a lost its lease to another instance's release")
	}
}

func TestRenewKeepsTheLeaseAlive(t *testing.T) {
	t.Parallel()

	clock := newClock()
	server, a, _ := newFleet(t, clock)
	ctx := context.Background()

	if _, err := a.Acquire(ctx, "s1"); err != nil {
		t.Fatalf("a.Acquire: %v", err)
	}

	clock.advance(20 * time.Second)
	server.FastForward(20 * time.Second)
	if err := a.Renew(ctx, "s1"); err != nil {
		t.Fatalf("a.Renew: %v", err)
	}
	if _, owned := a.Owned("s1"); !owned {
		t.Fatal("a does not own a session it renewed a moment ago")
	}
}

// The dangerous direction: a renewal must not extend a lease that has already moved.
func TestRenewRefusesAfterTheLeaseMoved(t *testing.T) {
	t.Parallel()

	clock := newClock()
	server, a, b := newFleet(t, clock)
	ctx := context.Background()

	if _, err := a.Acquire(ctx, "s1"); err != nil {
		t.Fatalf("a.Acquire: %v", err)
	}

	server.FastForward(cluster.DefaultTTL + time.Second)
	if _, err := b.Acquire(ctx, "s1"); err != nil {
		t.Fatalf("b.Acquire after expiry: %v", err)
	}

	if err := a.Renew(ctx, "s1"); !errors.Is(err, cluster.ErrNotOwner) {
		t.Fatalf("a.Renew after b took over = %v, want ErrNotOwner", err)
	}
	if _, owned := a.Owned("s1"); owned {
		t.Fatal("a still believes it owns a session it was told it lost")
	}
	if _, owned := b.Owned("s1"); !owned {
		t.Fatal("b does not own the session it just took")
	}
}

// Ownership is answered locally, so a lease nobody renewed has to stop counting as
// owned on its own, without asking Redis. This is the guard the store writes sit
// behind.
func TestOwnedGoesStaleWithoutARenewal(t *testing.T) {
	t.Parallel()

	clock := newClock()
	_, a, _ := newFleet(t, clock)
	ctx := context.Background()

	if _, err := a.Acquire(ctx, "s1"); err != nil {
		t.Fatalf("a.Acquire: %v", err)
	}
	if _, owned := a.Owned("s1"); !owned {
		t.Fatal("a does not own the session it just acquired")
	}

	clock.advance(cluster.DefaultTTL - cluster.DefaultRenewMargin)
	if _, owned := a.Owned("s1"); owned {
		t.Fatal("a still acts on a lease it has not renewed within the margin")
	}
}

// Held is what the renew loop iterates, so a lease that has gone stale locally must
// still be in it: forgetting it there is what stops it from ever being renewed or
// released.
func TestHeldListsWhatWasAcquired(t *testing.T) {
	t.Parallel()

	clock := newClock()
	_, a, _ := newFleet(t, clock)
	ctx := context.Background()

	for _, sid := range []string{"s1", "s2"} {
		if _, err := a.Acquire(ctx, sid); err != nil {
			t.Fatalf("a.Acquire(%s): %v", sid, err)
		}
	}
	clock.advance(cluster.DefaultTTL)

	if got := len(a.Held()); got != 2 {
		t.Fatalf("Held() has %d entries, want 2", got)
	}
}

func TestReleaseArmsTheCooldown(t *testing.T) {
	t.Parallel()

	server, a, _ := newFleet(t, newClock())
	ctx := context.Background()

	if _, err := a.Acquire(ctx, "s1"); err != nil {
		t.Fatalf("a.Acquire: %v", err)
	}
	released, err := a.Release(ctx, "s1")
	if err != nil {
		t.Fatalf("a.Release: %v", err)
	}
	if !released {
		t.Fatal("a.Release reported it held nothing")
	}
	if !server.Exists("wa:cooldown:s1") {
		t.Fatal("release left no cooldown, so the same instance can win the reclaim immediately")
	}
	if server.Exists("wa:lease:s1") {
		t.Fatal("release left the lease in place")
	}
}

// Owned answers from local state on every write, and the renew loop rewrites that state
// on its own goroutine. The two have to be safe together, and an entry that escaped the
// lock as a pointer was not: the reader saw a renewal timestamp mid-write.
func TestOwnedAndRenewAreSafeTogether(t *testing.T) {
	t.Parallel()

	_, leases, _ := newFleet(t, newClock())
	ctx := context.Background()
	if _, err := leases.Acquire(ctx, "s1"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 200 {
			if err := leases.Renew(ctx, "s1"); err != nil {
				t.Errorf("Renew: %v", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range 200 {
			if _, owned := leases.Owned("s1"); !owned {
				t.Error("a lease being renewed reported itself lost")
				return
			}
		}
	}()
	wg.Wait()
}

// roundTrips counts what the client actually sends: one per command run on its own, and
// one per pipeline however many commands it carries.
type roundTrips struct{ n atomic.Int64 }

func (*roundTrips) DialHook(next redis.DialHook) redis.DialHook { return next }

func (r *roundTrips) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		r.n.Add(1)
		return next(ctx, cmd)
	}
}

func (r *roundTrips) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		r.n.Add(1)
		return next(ctx, cmds)
	}
}

// The renew pass runs on the goroutine that also reads commands, under a budget written
// in fixed durations. A round trip per session puts a term proportional to the number of
// sessions inside a bound that names none of them, and the sessions at the end of the
// list are the ones a peer takes while this instance still holds their sockets.
func TestRenewManyRenewsEverySessionInOneRoundTrip(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	clock := newClock()
	leases := cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{Clock: clock})
	ctx := context.Background()

	sids := []string{"s1", "s2", "s3", "s4", "s5"}
	for _, sid := range sids {
		if _, err := leases.Acquire(ctx, sid); err != nil {
			t.Fatalf("Acquire %s: %v", sid, err)
		}
	}

	// Counted from here, so the acquisitions above are not part of the measurement.
	var trips roundTrips
	rdb.AddHook(&trips)

	renew := func(when string) {
		t.Helper()
		clock.advance(20 * time.Second)
		server.FastForward(20 * time.Second)
		for sid, err := range leases.RenewMany(ctx, sids) {
			if err != nil {
				t.Fatalf("RenewMany %s (%s): %v", sid, when, err)
			}
		}
		for _, sid := range sids {
			if _, owned := leases.Owned(sid); !owned {
				t.Fatalf("%s is not owned after a renewal that reported success (%s)", sid, when)
			}
		}
	}

	// The first pass finds the script unloaded, which costs one more batch and not one
	// call per session: a restart or a SCRIPT FLUSH must not turn a tick into N trips.
	renew("cold")
	if got := trips.n.Load(); got > 2 {
		t.Fatalf("the first renewal of %d sessions took %d round trips, want at most 2", len(sids), got)
	}

	trips.n.Store(0)
	renew("warm")
	if got := trips.n.Load(); got != 1 {
		t.Fatalf("renewing %d sessions took %d round trips, want 1", len(sids), got)
	}
}

// One lease lost says nothing about the next, so the answer has to be per session: the
// pipeline reports the first failure and the caller stops the one session it names.
func TestRenewManyAnswersForEachSessionOnItsOwn(t *testing.T) {
	t.Parallel()

	clock := newClock()
	server, a, b := newFleet(t, clock)
	ctx := context.Background()

	for _, sid := range []string{"s1", "s2"} {
		if _, err := a.Acquire(ctx, sid); err != nil {
			t.Fatalf("a.Acquire %s: %v", sid, err)
		}
	}
	// s1 alone moves: expired, then taken by the peer.
	server.FastForward(cluster.DefaultTTL + time.Second)
	if _, err := b.Acquire(ctx, "s1"); err != nil {
		t.Fatalf("b.Acquire: %v", err)
	}
	if _, err := a.Acquire(ctx, "s2"); err != nil {
		t.Fatalf("a.Acquire s2 again: %v", err)
	}

	answers := a.RenewMany(ctx, []string{"s1", "s2"})
	if !errors.Is(answers["s1"], cluster.ErrNotOwner) {
		t.Errorf("RenewMany s1 = %v, want ErrNotOwner", answers["s1"])
	}
	if answers["s2"] != nil {
		t.Errorf("RenewMany s2 = %v, want nil: losing one lease is not losing the other", answers["s2"])
	}
	if _, owned := a.Owned("s1"); owned {
		t.Error("a still believes it owns the session it was told it lost")
	}
	if _, owned := a.Owned("s2"); !owned {
		t.Error("a dropped a session whose renewal succeeded")
	}
}
