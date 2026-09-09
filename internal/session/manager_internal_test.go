package session

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// An offer refused by a session that is stopping must not mark it. The mark schedules a
// drain, and the drain claims the stream with no minimum idle time -- so an account this
// instance is deliberately giving up would have its stream taken from under the owner
// taking it over, and the entries handed back at age zero, below the idle floor that
// owner's own reclaim watches.
//
// Written against the map rather than through Release, and that is the point of it being
// here: Release deletes the session before it stops it, so today an offer can only be
// refused as stopped once the lookup in Dispatch already misses. Reorder those two lines
// -- stop the session, then forget it, which reads more naturally than what is there --
// and the window opens. This holds the rule instead of the accident that hides it.
func TestAnOfferRefusedByAStoppingSessionDoesNotMarkIt(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000a1"
	ctx := context.Background()
	session, err := manager.Adopt(ctx, sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	// The mark adoption leaves is not what this is about.
	manager.TakeNewlyAdopted()

	// Stopped while still in the map, which is the window the current ordering closes.
	session.Stop()

	var given bool
	manager.Dispatch(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "c1", Type: protocol.CommandMessageSend, SID: sid,
		},
		Ack:     func(context.Context) error { t.Error("a command nobody carried out was retired"); return nil },
		Release: func() { given = true },
	})

	if !given {
		t.Fatal("the command was not handed back, want it left pending for whoever takes the account")
	}
	if marked := manager.TakeNewlyAdopted(); len(marked) != 0 {
		t.Fatalf("the stopping session was marked for a drain (%v), want no mark", marked)
	}
}

// The refusal is answered on the manager's own goroutine now, so it can find no room
// there -- and a refusal left pending is a command for a session this instance runs and
// goes on reading by `>`. Released, the next command that session's queue accepts would
// overtake it, which is the overtaking #77 is about arriving through the door #76 opened.
//
// A wake and a ping pass through the same queue and must not be marked: they ride the
// control stream, where there is no per-session turn to keep. Both are asserted here, on
// the one call that cannot tell them apart by itself.
func TestARefusalWithNoRoomToBeSentKeepsItsSessionsTurn(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
		// One slot, filled below, so the next command finds the queue full.
		AnswerDepth: 1,
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000b2"
	if _, err := manager.Adopt(context.Background(), sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	manager.TakeNewlyAdopted()

	// Nothing is answering, so the one slot stays taken.
	manager.answers <- answer{delivery: &transport.Delivery{}, give: func(context.Context, *transport.Delivery) {}}

	cases := []struct {
		name    string
		command protocol.Command
		want    bool
	}{{
		name:    "a refusal for a session this instance runs",
		command: protocol.Command{V: protocol.Version, ID: "c1", Type: protocol.CommandMessageSend, SID: sid},
		want:    true,
	}, {
		name:    "a wake, which rides the control stream",
		command: protocol.Command{V: protocol.Version, ID: "c2", Type: protocol.CommandSessionWake, SID: sid},
		want:    false,
	}, {
		name:    "a ping, which rides the control stream",
		command: protocol.Command{V: protocol.Version, ID: "c3", Type: protocol.CommandAdminPing, SID: sid},
		want:    false,
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var given bool
			manager.own(&transport.Delivery{
				Command: tc.command,
				Ack:     func(context.Context) error { t.Error("a command nobody carried out was retired"); return nil },
				Release: func() { given = true },
			}, func(context.Context, *transport.Delivery) { t.Error("a command with no room was carried out") })

			if !given {
				t.Fatal("the command was not handed back, want it left pending")
			}
			if marked := len(manager.TakeNewlyAdopted()) > 0; marked != tc.want {
				t.Fatalf("marked for a drain = %v, want %v", marked, tc.want)
			}
		})
	}
}

type quietPublisher struct{}

func (quietPublisher) Publish(context.Context, *protocol.Event) error { return nil }

type quietReplier struct{}

func (quietReplier) Reply(context.Context, string, protocol.Reply) error { return nil }

// A session the engine has finished with keeps its lease otherwise, and while it does no
// peer tries the account: a fleet of three does not get three attempts at an account
// WhatsApp will not talk to, it gets one instance holding it and two that never see it.
//
// Two instances, because the whole point is the second one being able to take it. The
// account is owned by nobody in between, which is what a client's next connect adopts.
func TestASessionTheEngineFinishedWithHandsItsLeaseBack(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	holder := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { holder.StopAll(context.Background()) })
	peer := NewManager(&ManagerConfig{
		Instance: "inst-b", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-b", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { peer.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000a2"
	ctx := context.Background()
	if _, err := holder.Adopt(ctx, sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	if _, err := peer.Adopt(ctx, sid); err == nil {
		t.Fatal("a peer adopted a session another instance holds the lease for")
	}

	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}
	// The shape of a temporary ban: the event says why, and the engine says it has
	// nothing left to try.
	engineSession.EmitLast(protocol.EventSessionTemporaryBan, map[string]any{
		"ban": map[string]any{"kind": "temporary", "reason": "spam"},
	})

	waitFor(t, func() bool {
		holder.SweepRetired(ctx)
		_, held := holder.leases.Owned(sid)
		return !held
	}, "the lease of a session the engine finished with was never handed back")

	if _, err := peer.Adopt(ctx, sid); err != nil {
		t.Fatalf("the peer could not take an account nobody owns: %v", err)
	}
}

// waitFor polls a condition rather than sleeping to a deadline: the pump publishes on its
// own goroutine, so what is being waited for is a hand-off and not a duration.
func waitFor(t *testing.T, done func() bool, complaint string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if done() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(complaint)
}
