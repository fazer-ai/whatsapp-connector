package app

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/session"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
	"github.com/fazer-ai/whatsapp-connector/internal/transport/redisstream"
)

type quietReplier struct{}

func (quietReplier) Reply(context.Context, string, protocol.Reply) error { return nil }

// Reclaimed deliveries are mostly wakes, and a wake is the one command that blocks: it
// adopts a session, which reads the store. A batch of them runs on the goroutine that
// renews every lease this instance holds, so a batch that is not bounded delays those
// renewals and hands the accounts to peers while this instance still holds their
// sockets open.
func TestAReclaimBatchStopsWhenItRunsOutOfBudget(t *testing.T) {
	t.Parallel()

	connector := &Connector{
		cfg:     Config{LeaseTTL: time.Nanosecond},
		log:     zerolog.Nop(),
		manager: newTestManager(t),
	}

	acked, released := deliveryBatch(6)
	connector.dispatchWithin(context.Background(), acked.deliveries)

	if acked.count.Load() != 0 {
		t.Fatalf("%d commands were carried out past the budget", acked.count.Load())
	}
	if released.Load() != 6 {
		t.Fatalf("%d of 6 undispatched commands were released; the rest are held for good", released.Load())
	}
}

// And with room to work, everything in the batch is carried out.
func TestAReclaimBatchWithRoomCarriesEverythingOut(t *testing.T) {
	t.Parallel()

	connector := &Connector{
		cfg:     Config{LeaseTTL: time.Minute},
		log:     zerolog.Nop(),
		manager: newTestManager(t),
	}

	acked, released := deliveryBatch(6)
	connector.dispatchWithin(context.Background(), acked.deliveries)

	if acked.count.Load() != 6 {
		t.Fatalf("%d of 6 commands were carried out", acked.count.Load())
	}
	if released.Load() != 0 {
		t.Fatalf("%d commands were released despite the budget being ample", released.Load())
	}
}

// A drain that could not finish has to hand the sessions back, or they are read from
// with older commands still pending and are overtaken.
func TestADrainThatFailsGivesTheSessionsBack(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	streams, err := redisstream.New(client, redisstream.Options{
		Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("redisstream.New: %v", err)
	}
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	connector := &Connector{cfg: Config{LeaseTTL: time.Minute}, log: zerolog.Nop(), manager: manager, streams: streams}
	ctx := context.Background()
	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	// The session's command stream is replaced by something that is not a stream, which
	// is what a Redis answering wrongly looks like from here.
	if err := client.Set(ctx, client.Keys().Commands("s1"), "not a stream", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}

	undrained := connector.drainAdopted(ctx)
	if len(undrained) != 1 || undrained[0] != "s1" {
		t.Fatalf("the drain reported %v undrained, want [s1]", undrained)
	}
	if back := manager.TakeNewlyAdopted(); len(back) != 1 || back[0] != "s1" {
		t.Fatalf("the session was not put back to be drained again: %v", back)
	}
}

type quietPublisher struct{}

func (quietPublisher) Publish(context.Context, *protocol.Event) error { return nil }

func newTestManager(t *testing.T) *session.Manager {
	t.Helper()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })
	return manager
}

type batch struct {
	deliveries []transport.Delivery
	count      atomic.Int64
}

// deliveryBatch is admin pings, which the manager answers inline and acknowledges. That
// is what separates a command that was carried out from one the budget left behind:
// the first is acknowledged, the second released.
func deliveryBatch(n int) (*batch, *atomic.Int64) {
	b := &batch{}
	released := &atomic.Int64{}
	for i := range n {
		b.deliveries = append(b.deliveries, transport.Delivery{
			Command: protocol.Command{
				V: protocol.Version, ID: "ping-" + string(rune('a'+i)), Type: protocol.CommandAdminPing,
				TS: 1787000000000, Payload: json.RawMessage(`{}`),
			},
			Ack:     func(context.Context) error { b.count.Add(1); return nil },
			Release: func() { released.Add(1) },
		})
	}
	return b, released
}
