package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/observability"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/session"
	"github.com/fazer-ai/whatsapp-connector/internal/testwait"
)

type leaseClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *leaseClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *leaseClock) step(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// The watcher is what takes a session down when its lease runs out with the loop busy
// elsewhere (#353). Nothing in this test runs a tick: the only thing that can stop the
// session is the watcher, and it has to without being woken by anything but time.
func TestTheLeaseWatcherStopsASessionWhoseLeaseRanOutWithNoTick(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", DefaultEventShards)

	clock := &leaseClock{now: time.Now()}
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{Clock: clock}),
		Publisher: quietPublisher{}, Replier: &orderedReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })
	if _, err := manager.Adopt(context.Background(), "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	connector := &Connector{
		metrics: observability.New(), log: zerolog.Nop(), manager: manager,
		// Short, so the watcher's cap is what wakes it after the clock jumps: the lease
		// clock is stepped, not slept through.
		cfg: Config{LeaseTTL: cluster.DefaultTTL, Heartbeat: 10 * time.Millisecond},
	}
	ctx, cancel := context.WithCancel(context.Background())
	watched := connector.watchLeases(ctx)
	t.Cleanup(func() { cancel(); <-watched })

	// Fresh: nothing to stop, for several of the watcher's wakes.
	time.Sleep(50 * time.Millisecond)
	if got := manager.Count(); got != 1 {
		t.Fatalf("the watcher stopped a session whose lease is fresh (running %d)", got)
	}
	if got := gathered(t, connector, "wac_sessions_running"); got != 1 {
		t.Fatalf("wac_sessions_running = %v before anything stopped, want 1", got)
	}

	clock.step(cluster.DefaultTTL)
	deadline := time.Now().Add(testwait.Budget)
	for manager.Count() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the watcher never stopped a session whose lease ran out")
		}
		time.Sleep(testwait.Poll)
	}
	// And says so, with no tick to do it: the gauge is what an operator reads during the
	// outage that stopped the tick.
	deadline = time.Now().Add(testwait.Budget)
	for gathered(t, connector, "wac_sessions_running") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("wac_sessions_running still counts a session the watcher stopped")
		}
		time.Sleep(testwait.Poll)
	}
}

// Stopped means returned: Run waits on the channel before the shutdown hands everything
// back, and a watcher still running then could stop a session the shutdown is releasing.
func TestTheLeaseWatcherReturnsWhenItsContextEnds(t *testing.T) {
	t.Parallel()

	connector, _, _, _ := adoptedSession(t, "2f1c6f0e-0000-4000-8000-0000000c3530")
	connector.cfg.Heartbeat = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	watched := connector.watchLeases(ctx)
	cancel()
	select {
	case <-watched:
	case <-time.After(testwait.Budget):
		t.Fatal("the lease watcher did not return after its context ended")
	}
}

// An account the lease watcher stopped is queued to be handed back, and the hand-back runs
// on the tick. A resume pass that lands first -- both wake up when Redis answers again --
// must not spend the fleet-wide turn on it: the adoption is refused while the hand-back is
// pending, and the turn would then keep every instance off the account for a whole
// cool-off. Measured on the bench at 70 s after Redis came back (#353).
func TestTheResumeSweepDoesNotSpendItsTurnOnAnAccountStillBeingHandedBack(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", DefaultEventShards)
	clock := &leaseClock{now: time.Now()}
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{Clock: clock})
	engine := fake.New()
	quarantine := cluster.NewQuarantine(client, nil)
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: engine, Leases: leases,
		Publisher: quietPublisher{}, Replier: quietReplier{}, Quarantine: quarantine,
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })
	answering(t, manager)
	container := openTestStore(t)
	connector := &Connector{
		metrics: observability.New(), cfg: Config{Instance: "inst-a"}, log: zerolog.Nop(),
		client: client, leases: leases, quarantine: quarantine, manager: manager, store: container,
	}
	wantConnected(t, container, "sid-1", "5511999990001", false)

	ctx := context.Background()
	if _, err := manager.Adopt(ctx, "sid-1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	// The lease runs out on this instance's clock, the watcher stops the session, and the
	// key has meanwhile expired in Redis -- the shape a long outage leaves.
	clock.step(cluster.DefaultTTL)
	manager.StopStale()
	keys := redisx.NewKeys("wa:", 0)
	server.Del(keys.Lease("sid-1"))
	if !manager.HandingBack("sid-1") {
		t.Fatal("the stopped account is not queued to be handed back, so this test would not see the case")
	}

	connector.resumeOnce(t.Context())
	if server.Exists(keys.Resume("sid-1")) {
		t.Fatal("the sweep took the fleet's turn on an account this instance is still handing back")
	}

	// The tick hands it back once the stop has finished, and the next pass brings it up.
	waitFor(t, "the tick to hand the account back", func() bool {
		manager.RenewAll(ctx, manager.HandBackBy())
		return !manager.HandingBack("sid-1")
	})
	connector.resumeOnce(t.Context())
	waitFor(t, "the account to come back on the next pass", func() bool { return manager.Count() == 1 })
}
