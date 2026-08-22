package cluster_test

import (
	"context"
	"errors"
	"sync"
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
