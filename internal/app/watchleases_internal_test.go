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

	clock.step(cluster.DefaultTTL)
	deadline := time.Now().Add(testwait.Budget)
	for manager.Count() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the watcher never stopped a session whose lease ran out")
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
