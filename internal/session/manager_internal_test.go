package session

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine"
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
		holder.SweepRetired(ctx, holder.HandBackBy())
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

// A client retrying between the terminal event and the next heartbeat must not be served
// by the session that is on its way out: a connect answered there would put the account
// back up on an instance that hands it away moments later. Nor is the account taken back
// in the same step, which is what handing it back here and acquiring again would be: the
// release arms a cooldown so that the instance letting go does not immediately win the
// account, and for a build WhatsApp will not talk to that is the whole point -- the retry
// has to be free to land on a peer whose image can succeed.
func TestAnAdoptionDoesNotHandBackASessionOnItsWayOut(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines, Leases: leases,
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000a3"
	ctx := context.Background()
	first, err := manager.Adopt(ctx, sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}
	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	waitFor(t, first.Retired, "the session was never finished with")

	// The retry, before any heartbeat has swept.
	second, err := manager.Adopt(ctx, sid)
	if !errors.Is(err, errLeaving) {
		t.Fatalf("the retry was answered with %v, want %v", err, errLeaving)
	}
	if second != nil {
		t.Fatal("the retry was answered with a session for an account this instance is finishing with")
	}
	if _, held := leases.Owned(sid); !held {
		t.Fatal("the retry handed the account back and took it again, which is the instance that cannot pair keeping it")
	}

	// And once the heartbeat has swept, the account is there for whoever wakes it next.
	manager.SweepRetired(ctx, manager.HandBackBy())
	if _, held := leases.Owned(sid); held {
		t.Fatal("the sweep never handed the account back")
	}
	if _, err := manager.Adopt(ctx, sid); err != nil {
		t.Fatalf("the account could not be adopted once it had been handed back: %v", err)
	}
}

// stalledRedis is a hop in front of a real server that can be told to stop carrying
// anything, which is what a Redis under load looks like from the caller: the connection
// is up, the request went out, and the answer never comes. Closing the server instead
// would prove nothing -- a refused connection fails at once, and it is the round trip
// that does not fail that a heartbeat has to be protected from.
type stalledRedis struct {
	listener net.Listener
	stalled  atomic.Bool
	held     chan struct{}
	// swallowed closes on the first request the hop takes and never answers, which is
	// what lets a test say "the round trip is in flight" without waiting on a clock.
	swallowed chan struct{}
	once      sync.Once

	// holdReply keeps the next answer coming back from the server, and `reached` says it
	// is being kept. One answer and not the connection, so everything the test does
	// meanwhile still reaches Redis: it is how a caller is held between asking and
	// hearing, which is where the interesting interleavings live.
	holdReply atomic.Bool
	reached   chan struct{}
	let       chan struct{}
	reachedOn sync.Once
}

func stallable(t *testing.T, backend string) *stalledRedis {
	t.Helper()
	var listen net.ListenConfig
	listener, err := listen.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	hop := &stalledRedis{
		listener: listener, held: make(chan struct{}), swallowed: make(chan struct{}),
		reached: make(chan struct{}), let: make(chan struct{}),
	}
	t.Cleanup(func() {
		close(hop.held)
		_ = listener.Close()
	})
	go func() {
		for {
			near, err := listener.Accept()
			if err != nil {
				return
			}
			var dial net.Dialer
			far, err := dial.DialContext(context.Background(), "tcp", backend)
			if err != nil {
				_ = near.Close()
				return
			}
			go hop.carry(far, near, false)
			go hop.carry(near, far, true)
		}
	}()
	return hop
}

func (s *stalledRedis) addr() string { return s.listener.Addr().String() }

func (s *stalledRedis) stall() { s.stalled.Store(true) }

// resume lets connections opened from here on through again. The one already swallowed
// stays swallowed: its request is lost, which is what the caller waiting on it is for.
func (s *stalledRedis) resume() { s.stalled.Store(false) }

// holdNextAnswer keeps the next answer the server sends until `let` is closed.
func (s *stalledRedis) holdNextAnswer() { s.holdReply.Store(true) }

func (s *stalledRedis) carry(dst, src net.Conn, answers bool) {
	defer func() { _, _ = dst.Close(), src.Close() }()
	buf := make([]byte, 4096)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if s.stalled.Load() {
				s.once.Do(func() { close(s.swallowed) })
				<-s.held
				return
			}
			if answers && s.holdReply.CompareAndSwap(true, false) {
				s.reachedOn.Do(func() { close(s.reached) })
				<-s.let
			}
			if _, err := dst.Write(buf[:n]); err != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// Every hand-back is a Redis round trip that can hang, and the goroutine that sweeps is
// the goroutine that renews every lease this instance holds. A sweep that spends longer
// than a lease is every other session left unrenewed while its socket is still open, so
// peers take those accounts from under a live connection. Retiring an account a tick
// later is the cheaper outcome by a wide margin.
func TestASweepGivesUpOnARedisThatStoppedAnswering(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	hop := stallable(t, server.Addr())
	// The same option production dials with, and the only one that matters here: without
	// it go-redis hands the connection a background context and its own read timeout, so
	// the window this measures would stop at the point the command reaches the socket.
	rdb := redis.NewClient(&redis.Options{Addr: hop.addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })

	// Short enough that the budget is unmistakable next to the read timeout a hand-back
	// that is not bounded waits out, which is what the reversion of this measures, and
	// long enough that the events retiring the sessions are still published under a
	// lease this holder counts as its own.
	const ttl = 1500 * time.Millisecond
	engines := fake.New()
	leases := cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{
		TTL: ttl, Margin: 100 * time.Millisecond,
	})
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines, Leases: leases,
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	// Bounded because this runs with the server still stalled, and a hand-back that
	// cannot reach it is not what is being measured here.
	t.Cleanup(func() {
		quick, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		manager.StopAll(quick)
	})

	sids := []string{"s1", "s2", "s3"}
	sessions := make([]*Session, 0, len(sids))
	for _, sid := range sids {
		session, err := manager.Adopt(context.Background(), sid)
		if err != nil {
			t.Fatalf("Adopt %s: %v", sid, err)
		}
		sessions = append(sessions, session)
		engineSession, running := engines.Session(sid)
		if !running {
			t.Fatalf("the engine has no session for %s, just adopted", sid)
		}
		engineSession.EmitLast(protocol.EventSessionState, map[string]any{
			"state": "close", "reason": "pairing_client_outdated",
		})
	}
	waitFor(t, func() bool {
		for _, session := range sessions {
			if !session.Retired() {
				return false
			}
		}
		return true
	}, "the sessions were never finished with")

	hop.stall()
	swept := make(chan struct{})
	go func() {
		defer close(swept)
		manager.SweepRetired(context.Background(), manager.HandBackBy())
	}()
	select {
	case <-swept:
	case <-time.After(2 * time.Second):
		t.Fatalf("the sweep is still handing leases back after 2s, on a lease worth %s", ttl)
	}
}

// Retirement is a stop the heartbeat has not performed yet, and until it does the
// session is still in the map and still answers. A connect carried out in that window
// dials an account this instance hands away moments later and tells the client it
// worked: the socket is then stopped by the sweep with nothing published to say so, and
// the inbox reads as connected until somebody tries to send on it.
func TestACommandForARetiredSessionIsLeftForItsNextOwner(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000a5"
	ctx := context.Background()
	session, err := manager.Adopt(ctx, sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	manager.TakeNewlyAdopted()

	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}
	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	waitFor(t, func() bool { return session.Retired() }, "the session was never finished with")

	var given bool
	manager.Dispatch(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "c1", Type: protocol.CommandSessionConnect, SID: sid,
		},
		Ack:     func(context.Context) error { t.Error("a command nobody carried out was retired"); return nil },
		Release: func() { given = true },
	})

	if !given {
		t.Fatal("a connect was taken by a session on its way out, want it left pending for whoever takes the account")
	}
	if marked := manager.TakeNewlyAdopted(); len(marked) != 0 {
		t.Fatalf("the retired session was marked for a drain (%v), want no mark", marked)
	}
}

// A hand-back acts on the session it was asked about and not on whatever the map holds
// when it gets there. The account can have been handed back and adopted again since --
// the sweep names one session, the wake that follows it names another -- and stopping the
// one that is running would take an account nobody asked to give up, with the lease it is
// running under deleted behind it.
//
// Written against the map rather than through a schedule that produces it, which is the
// point of it being here: what has to hold is that the answer names its session, and a
// rule that holds only because nothing currently interleaves is a rule that goes the day
// something does.
func TestAHandBackActsOnTheSessionItWasAskedAbout(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines, Leases: leases,
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000a4"
	ctx := context.Background()
	first, err := manager.Adopt(ctx, sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}
	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	waitFor(t, first.Retired, "the session was never finished with")

	// Handed back, and woken again: the account is running under a session of its own.
	manager.SweepRetired(ctx, manager.HandBackBy())
	second, err := manager.Adopt(ctx, sid)
	if err != nil {
		t.Fatalf("the account could not be adopted again: %v", err)
	}
	if second == first {
		t.Fatal("the account was adopted onto the session that had been handed back")
	}

	// And the sweep, coming to the one it found a moment ago.
	manager.releaseThis(ctx, sid, first)

	if !slices.Contains(manager.SIDs(), sid) {
		t.Fatal("a hand-back stopped the session that replaced the one it was asked about")
	}
	if _, held := leases.Owned(sid); !held {
		t.Fatal("a hand-back deleted the lease of the session that replaced the one it was asked about")
	}
}

// The tick hands leases back twice: the renewals give up what they lost, and the sweep
// gives up what the engine finished with. The budget is the tick's, not each pass's --
// the startup check that decides whether a lease TTL is configurable at all prices one
// hand-back tail into it (`app.Config`), and a second one nobody priced is a renewal that
// lands on a lease a peer has already taken while this instance holds the socket open.
func TestASweepTakesOnlyWhatTheRenewalsLeftOfTheTick(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	hop := stallable(t, server.Addr())
	rdb := redis.NewClient(&redis.Options{Addr: hop.addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })

	// A lease worth six seconds, so a pass that makes its own window would take two.
	const ttl = 6 * time.Second
	engines := fake.New()
	leases := cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{TTL: ttl})
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines, Leases: leases,
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() {
		quick, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		manager.StopAll(quick)
	})

	const sid = "9c2b7d1e-0000-4000-8000-0000000000a7"
	session, err := manager.Adopt(context.Background(), sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}
	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	waitFor(t, func() bool { return session.Retired() }, "the session was never finished with")

	hop.stall()
	// The renewals ran first and spent all but a moment of the tick's budget, which is
	// the deadline the sweep is handed rather than a fresh window of its own.
	nearlySpent := time.Now().Add(150 * time.Millisecond)
	swept := make(chan struct{})
	go func() {
		defer close(swept)
		manager.SweepRetired(context.Background(), nearlySpent)
	}()
	select {
	case <-swept:
	case <-time.After(time.Second):
		t.Fatal("the sweep made a window of its own instead of taking what was left of the tick")
	}
}

// A hand-back is a round trip, and until it answers the lease still names this instance
// while nothing here runs the session. A wake that lands in that gap finds the lease
// taken; acknowledged on those grounds it retires the only thing that would have started
// the account, and the release lands a moment later on an account with nothing left to
// pick it up.
func TestALeaseOnItsWayBackIsOneAWakeWaitsFor(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	hop := stallable(t, server.Addr())
	rdb := redis.NewClient(&redis.Options{Addr: hop.addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })

	engines := fake.New()
	leases := cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{})
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines, Leases: leases,
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() {
		quick, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		manager.StopAll(quick)
	})

	const sid = "9c2b7d1e-0000-4000-8000-0000000000a8"
	if _, err := manager.Adopt(context.Background(), sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	hop.stall()
	handing, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		manager.Release(handing, sid)
	}()

	// The release has reached the hop and will never be answered, which is exactly the
	// moment a wake has to be able to see.
	select {
	case <-hop.swallowed:
	case <-time.After(2 * time.Second):
		t.Fatal("the release never reached Redis")
	}
	if !manager.handingBack(sid) {
		t.Fatal("a lease in flight back to Redis reads as nobody's, and the wake that would have restarted the account is acknowledged as somebody else's")
	}
	cancel()
	<-done
}

// Offer closes the door on new commands, and the ones already through it are the point:
// a connect taken from the queue after the engine has finished with the session dials an
// account the next tick hands away, and answers the client that it worked.
func TestACommandAlreadyQueuedWhenTheSessionRetiresIsLeftForItsNextOwner(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000a9"
	session, err := manager.Adopt(context.Background(), sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}

	// One command in flight, so the queue behind it is a queue.
	release := engineSession.Hold()
	defer release()
	if offered := session.Offer(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "c1", Type: protocol.CommandMessageSend, SID: sid,
		},
		Ack:     func(context.Context) error { return nil },
		Release: func() {},
	}); offered != OfferAccepted {
		t.Fatalf("the session refused the first command: %v", offered)
	}
	waitFor(t, func() bool { return len(engineSession.Commands()) == 1 }, "the first command never reached the engine")

	// Written by the executor and read here, so not a plain bool.
	var given atomic.Bool
	if offered := session.Offer(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "c2", Type: protocol.CommandSessionConnect, SID: sid,
		},
		Ack:     func(context.Context) error { t.Error("a command nobody carried out was retired"); return nil },
		Release: func() { given.Store(true) },
	}); offered != OfferAccepted {
		t.Fatalf("the session refused the queued command: %v", offered)
	}

	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	waitFor(t, func() bool { return session.Retired() }, "the session was never finished with")

	release()
	waitFor(t, given.Load, "the queued connect was never handed back")
	if engineSession.Connected() {
		t.Fatal("a connect queued before the engine finished with the session dialled anyway")
	}
}

// A hand-back and an adoption of the same account are both [look at the map, talk to
// Redis, change the map], on two goroutines. Run through each other, the release lands
// after the acquisition and deletes the lease the session that just started is running
// under -- the release matches on the instance and nothing else -- and a peer takes an
// account whose socket is open here. Neither half may step into the other.
func TestAHandBackAndAnAdoptionOfTheSameAccountDoNotOverlap(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	hop := stallable(t, server.Addr())
	rdb := redis.NewClient(&redis.Options{Addr: hop.addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines, Leases: leases,
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() {
		quick, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		manager.StopAll(quick)
	})

	const sid = "9c2b7d1e-0000-4000-8000-0000000000b1"
	ctx := context.Background()
	first, err := manager.Adopt(ctx, sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}
	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	waitFor(t, first.Retired, "the session was never finished with")

	// The hand-back reaches Redis and is never answered, so it is still under way for as
	// long as the test needs it to be -- no clock decides that.
	hop.stall()
	handing, cancelHanding := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancelHanding()
	handed := make(chan struct{})
	go func() {
		defer close(handed)
		manager.releaseThis(handing, sid, first)
	}()
	<-hop.swallowed
	// Everything after the swallowed one gets through, so what the adoption waits for is
	// the hand-back and not a server that stopped answering.
	hop.resume()

	// On this goroutine, and answered rather than waited out: an adoption that waited here
	// would hold every other wake and ping behind an account this instance is giving up.
	_, err = manager.Adopt(ctx, sid)
	select {
	case <-handed:
		t.Fatal("the hand-back finished before the adoption was asked, so nothing was in the way of it")
	default:
	}
	if !errors.Is(err, errLeaving) {
		t.Fatalf("an adoption alongside a hand-back for the same account answered %v, want %v", err, errLeaving)
	}

	// The release never reached Redis, so the key still names this instance and the wake
	// stays pending on those grounds too: `handingBack` is what keeps it from being
	// acknowledged as somebody else's.
	<-handed
	if !manager.handingBack(sid) {
		t.Fatal("a hand-back that did not reach Redis was forgotten, so the wake that would restart the account is acknowledged as somebody else's")
	}
	if _, err := manager.Adopt(ctx, sid); !errors.Is(err, cluster.ErrNotOwner) {
		t.Fatalf("the adoption after a hand-back that never landed answered %v, want %v", err, cluster.ErrNotOwner)
	}
}

// And the other way round: a sweep does not take a session an adoption is working on. The
// adoption releases the retired session, wins a lease of its own and opens a socket on
// it; the sweep, acting on what it saw before any of that, would stop the session that
// just started and delete the lease it is running under.
func TestASweepDoesNotTakeASessionAnAdoptionIsWorkingOn(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines, Leases: leases,
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000b4"
	ctx := context.Background()
	first, err := manager.Adopt(ctx, sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}
	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	waitFor(t, first.Retired, "the session was never finished with")

	// An adoption in flight, which is all the answer goroutine holding this looks like
	// from the heartbeat.
	if !manager.tryHoldHanding(sid) {
		t.Fatal("the account's turn was already taken")
	}
	defer manager.dropHanding(sid)

	manager.releaseThis(ctx, sid, first)
	if !slices.Contains(manager.SIDs(), sid) {
		t.Fatal("the sweep handed a session back from under an adoption of the same account")
	}
	if _, held := leases.Owned(sid); !held {
		t.Fatal("the sweep released a lease an adoption of the same account was working on")
	}
}

// A renewal is a round trip, and the answer describes the session as it was when the
// question went out. An adoption on the answer goroutine can replace a retired session
// while it is in flight, winning a lease of its own, and a teardown that acts on the sid
// alone then stops the session that is running and leaves its lease where it is: Redis
// names this instance for an account it does not run, and the wakes that would restart it
// are acknowledged as somebody else's.
func TestARenewalThatWasRefusedDoesNotStopTheSessionThatReplacedIt(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	hop := stallable(t, server.Addr())
	rdb := redis.NewClient(&redis.Options{Addr: hop.addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines, Leases: leases,
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000b2"
	ctx := context.Background()
	first, err := manager.Adopt(ctx, sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}
	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	waitFor(t, func() bool { return first.Retired() }, "the session was never finished with")

	// One renewal that changes nothing, so the script is cached: the first EVALSHA of a
	// run answers NOSCRIPT and is retried as an EVAL, and holding that first answer would
	// hold the retry rather than the renewal.
	manager.RenewAll(ctx, manager.HandBackBy())

	// The lease is gone by the time the renewal runs, which is what makes the answer a
	// refusal and the account free for the retry to take.
	server.Del(client.Keys().Lease(sid))

	hop.holdNextAnswer()
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		manager.RenewAll(ctx, manager.HandBackBy())
	}()
	<-hop.reached

	// The account handed back and woken again, while the renewal is between asking and
	// hearing.
	manager.releaseThis(ctx, sid, first)
	second, err := manager.Adopt(ctx, sid)
	if err != nil {
		t.Fatalf("the retry could not be adopted: %v", err)
	}
	if second == first {
		t.Fatal("the retry was answered with the session that is on its way out")
	}
	close(hop.let)
	<-renewed

	if !slices.Contains(manager.SIDs(), sid) {
		t.Fatal("a refused renewal stopped the session that replaced the one it asked about")
	}
	if _, held := leases.Owned(sid); !held {
		t.Fatal("a refused renewal took the lease of the session that replaced the one it asked about")
	}
}

// Publishing is a write to Redis, and the executor runs alongside the pump. A connect
// waiting in the queue while the engine's last word is being written would dial an
// account this session is finished with and answer the client that it worked, moments
// before the heartbeat stops the socket it opened.
func TestASessionTakesNoCommandWhileItsLastWordIsGoingOut(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	publisher := &heldPublisher{
		holds:   protocol.EventSessionConnectFailure,
		entered: make(chan protocol.EventType, 8),
		let:     make(chan struct{}),
	}
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: publisher, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	// Registered after the stop and so run before it: cleanups run last-first, and a
	// publish still being held is a pump that never stops.
	t.Cleanup(func() { manager.StopAll(context.Background()) })
	t.Cleanup(publisher.release)

	const sid = "9c2b7d1e-0000-4000-8000-0000000000b3"
	session, err := manager.Adopt(context.Background(), sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}

	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	<-publisher.entered

	offered := session.Offer(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "c1", Type: protocol.CommandSessionConnect, SID: sid,
		},
		Ack:     func(context.Context) error { return nil },
		Release: func() {},
	})
	publisher.release()
	if offered != OfferStopped {
		t.Fatalf("the session answered %v to a connect while its last word was going out, want %v",
			offered, OfferStopped)
	}
	waitFor(t, session.Retired, "the session was never finished with")
	if engineSession.Connected() {
		t.Fatal("a connect taken while the last word was going out dialled anyway")
	}
}

// heldPublisher keeps one event type inside Publish until it is let go, which is what a
// Redis that is slow rather than away looks like from the pump.
type heldPublisher struct {
	holds protocol.EventType
	// entered takes one value per publish that is held, and let hands out one turn per
	// receive: a test that wants the pump stopped at a particular event asks for exactly
	// as many turns as the events before it.
	entered chan protocol.EventType
	let     chan struct{}
	letting sync.Once

	// fails is what the held publish answers, which is a Redis that took the write and
	// could not complete it.
	fails error

	mu     sync.Mutex
	events []protocol.EventType
}

func (p *heldPublisher) Publish(_ context.Context, event *protocol.Event) error {
	if event.Type == p.holds {
		p.entered <- event.Type
		<-p.let
		if p.fails != nil {
			return p.fails
		}
	}
	p.mu.Lock()
	p.events = append(p.events, event.Type)
	p.mu.Unlock()
	return nil
}

// release lets the held publish finish, once, however the test ends: a publish left
// waiting is a pump that never stops and a cleanup that never returns.
func (p *heldPublisher) release() { p.letting.Do(func() { close(p.let) }) }

func (p *heldPublisher) published() []protocol.EventType {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.events)
}

// The mark travels with the emission and cannot be taken off it. Between the engine
// queueing its last word and the pump taking it -- a gap as long as whatever the pump is
// publishing -- a connect can run and put the socket back up, and handing the account
// over on an answer about the attempt before that one tears down a retry that worked.
func TestAConnectThatWorkedCancelsAnOutcomeQueuedBeforeIt(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	publisher := &heldPublisher{
		holds:   protocol.EventSessionState,
		entered: make(chan protocol.EventType, 8),
		let:     make(chan struct{}),
	}
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: publisher, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	// Registered after the stop and so run before it: cleanups run last-first, and a
	// publish still being held is a pump that never stops.
	t.Cleanup(func() { manager.StopAll(context.Background()) })
	t.Cleanup(publisher.release)

	const sid = "9c2b7d1e-0000-4000-8000-0000000000b5"
	session, err := manager.Adopt(context.Background(), sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}

	// One event the pump is stuck publishing, and the engine's last word queued behind it.
	engineSession.Emit(protocol.EventSessionState, map[string]any{"state": "connecting"})
	<-publisher.entered
	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})

	// The retry, which the door cannot refuse: the pump has not reached the last word yet.
	if err := engineSession.Connect(context.Background(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("the retry could not connect: %v", err)
	}
	publisher.release()

	waitFor(t, func() bool { return len(publisher.published()) >= 2 }, "the queued outcome was never published")
	if session.Retired() {
		t.Fatal("a connect that worked was undone by an outcome the engine had already given up on")
	}
	if offered := session.Offer(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "c1", Type: protocol.CommandMessageSend, SID: sid,
		},
		Ack: func(context.Context) error { return nil }, Release: func() {},
	}); offered != OfferAccepted {
		t.Fatalf("the session answered %v after a retry that worked, want %v", offered, OfferAccepted)
	}
}

// A door that opens again has to put back what it turned away. A command refused while it
// was shut was released rather than given back -- a session on its way out must not
// schedule a drain, which would claim its stream from under the owner taking it over --
// and a released command keeps no turn: the newest command for the session would be read
// and run ahead of one that has been pending since before it.
func TestADoorThatOpensAgainSchedulesADrainForWhatItTurnedAway(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	publisher := &heldPublisher{
		holds:   protocol.EventSessionConnectFailure,
		entered: make(chan protocol.EventType, 8),
		let:     make(chan struct{}),
	}
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: publisher, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	// Registered after the stop and so run before it: cleanups run last-first, and a
	// publish still being held is a pump that never stops.
	t.Cleanup(func() { manager.StopAll(context.Background()) })
	t.Cleanup(publisher.release)

	const sid = "9c2b7d1e-0000-4000-8000-0000000000b6"
	if _, err := manager.Adopt(context.Background(), sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}
	// The mark adoption leaves is not what this is about.
	manager.TakeNewlyAdopted()

	// The door shuts when the pump takes the last word, and the pump is stopped there.
	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	<-publisher.entered

	var given atomic.Bool
	manager.Dispatch(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "c1", Type: protocol.CommandSessionConnect, SID: sid,
		},
		Ack:     func(context.Context) error { t.Error("a command nobody carried out was retired"); return nil },
		Release: func() { given.Store(true) },
	})
	if !given.Load() {
		t.Fatal("a connect was taken while the session's last word was going out")
	}

	// And the session turns out not to be finished with after all: a connect that was
	// already past the door put the socket back up while the word was going out.
	if err := engineSession.Connect(context.Background(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("the connect that was already past the door failed: %v", err)
	}
	publisher.release()

	// Taken rather than read, so a mark that arrives late is still seen: what is asserted
	// is that the drain is scheduled at all, not which poll finds it.
	waitFor(t, func() bool { return slices.Contains(manager.TakeNewlyAdopted(), sid) },
		"the door opened again with no drain scheduled, leaving a command pending and no claim to take its stream back")
}

// A retry can run into a giving-up of its own before the one it answered has even been
// published. Both emissions are marked, and the account is finished with -- but only the
// second of them is what the client is owed: retiring on the first stops the session with
// the retry's own state and its outcome still queued, and the client is left looking at a
// session that says it is connecting.
func TestASecondGivingUpIsNotAnsweredByTheFirstOnesEmission(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	publisher := &heldPublisher{
		holds:   protocol.EventSessionState,
		entered: make(chan protocol.EventType, 8),
		let:     make(chan struct{}),
	}
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: publisher, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	// Registered after the stop and so run before it: cleanups run last-first, and a
	// publish still being held is a pump that never stops.
	t.Cleanup(func() { manager.StopAll(context.Background()) })
	t.Cleanup(publisher.release)

	const sid = "9c2b7d1e-0000-4000-8000-0000000000b7"
	session, err := manager.Adopt(context.Background(), sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}

	// The pump is stopped on the first state, and everything else is queued behind it:
	// the connect WhatsApp refused, the retry that put a socket back up, and the build
	// WhatsApp would not talk to once it was up.
	engineSession.Emit(protocol.EventSessionState, map[string]any{"state": "connecting"})
	<-publisher.entered
	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	if err := engineSession.Connect(context.Background(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("the retry could not connect: %v", err)
	}
	engineSession.EmitLast(protocol.EventSessionClientOutdated, map[string]any{})

	// One turn, which carries the pump through the first state and the refused connect
	// behind it, and stops it on the state the retry published.
	publisher.let <- struct{}{}
	<-publisher.entered

	if session.Retired() {
		t.Fatal("the session was handed over on a giving-up it had already moved on from, with the retry's own outcome still queued")
	}
	published := publisher.published()
	if len(published) != 2 || published[1] != protocol.EventSessionConnectFailure {
		t.Fatalf("the pump published %v, want the state and then the refused connect", published)
	}

	publisher.release()
	waitFor(t, session.Retired, "the session was never finished with")
	if last := publisher.published(); last[len(last)-1] != protocol.EventSessionClientOutdated {
		t.Fatalf("the pump published %v last, want %s", last[len(last)-1], protocol.EventSessionClientOutdated)
	}
}

// A command taken off the queue before the door shut is one no door can call back, and a
// connect among them can put a socket up after the pump has already decided the session
// is finished with. The answer is asked of the engine again where it is acted on: an
// account whose socket is back is not one to hand over, and the heartbeat closing that
// socket would answer the client's connect with silence.
func TestAConnectTakenBeforeTheDoorShutUndoesTheRetirement(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines, Leases: leases,
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000b8"
	session, err := manager.Adopt(context.Background(), sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}

	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	waitFor(t, func() bool { return session.retiredOn.Load() != 0 }, "the session was never finished with")

	// The connect that was already past the door, landing after the pump had decided.
	if err := engineSession.Connect(context.Background(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("the connect that was already past the door failed: %v", err)
	}

	if session.Retired() {
		t.Fatal("a session whose socket is back up was still handed over")
	}
	manager.SweepRetired(context.Background(), manager.HandBackBy())
	if _, held := leases.Owned(sid); !held {
		t.Fatal("the sweep handed back the lease of an account whose socket is up")
	}
	if !engineSession.Connected() {
		t.Fatal("the sweep closed a socket the client had just been told was open")
	}

	// And the door is open again, or the account is one this instance owns and refuses to
	// carry anything out for.
	if offered := session.Offer(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "c1", Type: protocol.CommandMessageSend, SID: sid,
		},
		Ack: func(context.Context) error { return nil }, Release: func() {},
	}); offered != OfferAccepted {
		t.Fatalf("the session answered %v after the retirement was undone, want %v", offered, OfferAccepted)
	}
}

// The sweep runs on the goroutine that renews every lease this instance holds, and
// adoptions run on the answer loop with a store read inside them. Shared by the manager
// rather than by account, one wake in flight is every retired session left where it is --
// and a fleet coming up has a wake for every account it owns, so the backlog would keep
// the sweep from ever taking a turn and the accounts would be renewed for as long as it
// lasted.
func TestASweepIsNotHeldUpByAnAdoptionOfAnotherAccount(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines, Leases: leases,
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const retiring = "9c2b7d1e-0000-4000-8000-0000000000b9"
	const other = "9c2b7d1e-0000-4000-8000-0000000000ba"
	ctx := context.Background()
	session, err := manager.Adopt(ctx, retiring)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(retiring)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}
	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	waitFor(t, session.Retired, "the session was never finished with")

	// Another account being adopted, which is what a wake looks like from the heartbeat.
	if !manager.tryHoldHanding(other) {
		t.Fatal("the other account's turn was already taken")
	}
	defer manager.dropHanding(other)

	manager.SweepRetired(ctx, manager.HandBackBy())
	if _, held := leases.Owned(retiring); held {
		t.Fatal("an adoption of another account kept the sweep from handing a retired session back")
	}
}

// A mark can be made in the instant a connect has already taken the session back: the
// branch that gives up sets it, the connect clears it, and the emission that reports the
// giving-up is made after both. It names no giving-up at all, and an active session names
// none either -- so the two agree, and the session that is working is handed over.
func TestAMarkThatNamesNoGivingUpDoesNotHandTheAccountOver(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	published := make(chan protocol.EventType, 8)
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines, Leases: leases,
		Publisher: recordingPublisher{published}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000bb"
	session, err := manager.Adopt(context.Background(), sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}

	engineSession.EmitLastRaced(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	if got := <-published; got != protocol.EventSessionConnectFailure {
		t.Fatalf("the pump published %s, want %s", got, protocol.EventSessionConnectFailure)
	}

	if session.Retired() {
		t.Fatal("a mark naming no giving-up handed over a session with nothing wrong with it")
	}
	manager.SweepRetired(context.Background(), manager.HandBackBy())
	if _, held := leases.Owned(sid); !held {
		t.Fatal("the sweep handed back the lease of an account the engine had not given up on")
	}

	// And the door the mark shut is open again, or the account is one this instance owns,
	// never hands back, and refuses to carry anything out for.
	if offered := session.Offer(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "c1", Type: protocol.CommandMessageSend, SID: sid,
		},
		Ack: func(context.Context) error { return nil }, Release: func() {},
	}); offered != OfferAccepted {
		t.Fatalf("the session answered %v after a mark that named no giving-up, want %v", offered, OfferAccepted)
	}
}

// recordingPublisher says when a publish landed, which is what a test that wants the pump
// past one event and not the next waits on.
type recordingPublisher struct{ published chan protocol.EventType }

func (p recordingPublisher) Publish(_ context.Context, event *protocol.Event) error {
	p.published <- event.Type
	return nil
}

// The sweep finds a session a step before it hands it back, and a connect taken off the
// queue before the door shut can finish in between. Stopping it then closes a socket the
// client has just been told is open and hands back the lease it is running under, so the
// question is asked again with the account's turn in hand.
func TestAHandBackAsksAgainForTheSessionItWasAboutToTake(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines, Leases: leases,
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000bc"
	ctx := context.Background()
	session, err := manager.Adopt(ctx, sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}
	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	waitFor(t, session.Retired, "the session was never finished with")

	// What the sweep is holding when it comes to hand this one back, and the connect that
	// was already past the door landing in between.
	found := map[string]*Session{sid: session}
	if err := engineSession.Connect(ctx, engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("the connect that was already past the door failed: %v", err)
	}
	for swept, about := range found {
		manager.releaseThis(ctx, swept, about)
	}

	if _, held := leases.Owned(sid); !held {
		t.Fatal("the sweep handed back the lease of a session that came back before it got there")
	}
	if !engineSession.Connected() {
		t.Fatal("the sweep closed a socket the client had just been told was open")
	}
}

// whatsmeow publishes a terminal outcome from the branch that keeps the socket down, so
// the emission that carries it is the only trigger there will ever be: nothing reconnects
// and nothing says it again. Dropped on a Redis that was away, the account stays owned by
// an instance that will not use it and the client is never told why, which is the whole
// of what this feature exists to stop.
func TestTheEventFinishingASessionIsSaidAgainWhenItDoesNotLand(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	publisher := &flakyPublisher{fails: 1}
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: publisher, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
		RetireRetry: 20 * time.Millisecond,
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000bd"
	session, err := manager.Adopt(context.Background(), sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}

	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	waitFor(t, session.Retired, "the event finishing the session was never said again, so the account is held by an instance with nothing left to try")
	if attempts := publisher.attempts(); attempts < 2 {
		t.Fatalf("the pump published %d times, want the failed one and at least one more", attempts)
	}
}

// flakyPublisher fails the first few writes, which is a Redis that is away and comes back.
type flakyPublisher struct {
	fails int
	// failed closes on the first write that is refused, which is a test's signal that the
	// pump is now holding an event it could not say.
	failed  chan struct{}
	failing sync.Once

	mu    sync.Mutex
	seen  int
	types []protocol.EventType
}

func (p *flakyPublisher) Publish(_ context.Context, event *protocol.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen++
	p.types = append(p.types, event.Type)
	if p.seen <= p.fails {
		if p.failed != nil {
			p.failing.Do(func() { close(p.failed) })
		}
		return errors.New("redis is away")
	}
	return nil
}

func (p *flakyPublisher) attempts() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seen
}

// timesSaid is how often one event type was written, which is what tells a retry that
// went out from one that was dropped.
func (p *flakyPublisher) timesSaid(want protocol.EventType) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	said := 0
	for _, seen := range p.types {
		if seen == want {
			said++
		}
	}
	return said
}

// A connect taken off the queue before the door shut is one no door can call back, and it
// can be dialling while the sweep comes round. Its answer is not in: the client may be
// about to be told that it worked, so the account is left for the next tick rather than
// stopped from under it.
func TestAHandBackWaitsForACommandThatIsStillRunning(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines, Leases: leases,
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000be"
	ctx := context.Background()
	session, err := manager.Adopt(ctx, sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}

	// One command in flight, taken off the queue while the door was still open.
	release := engineSession.Hold()
	defer release()
	if offered := session.Offer(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "c1", Type: protocol.CommandMessageSend, SID: sid,
		},
		Ack: func(context.Context) error { return nil }, Release: func() {},
	}); offered != OfferAccepted {
		t.Fatalf("the session refused a command with the door open: %v", offered)
	}
	waitFor(t, func() bool { return len(engineSession.Commands()) == 1 }, "the command never reached the engine")

	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	waitFor(t, session.Retired, "the session was never finished with")

	manager.SweepRetired(ctx, manager.HandBackBy())
	if _, held := leases.Owned(sid); !held {
		t.Fatal("the sweep took a session with a command still running, whose answer the client is waiting for")
	}

	// And once the answer is in, the account goes back.
	release()
	waitFor(t, func() bool {
		manager.SweepRetired(ctx, manager.HandBackBy())
		_, held := leases.Owned(sid)
		return !held
	}, "the account was never handed back once the command it was waiting on answered")
}

// The mark a hand-back leaves before its round trip is what keeps a wake from being
// acknowledged as somebody else's, and it is also what brings a release that failed back
// on a later tick. The two readings are the same map, so a retry can go out alongside a
// release that is still in flight: it lands after that one and after whatever lease was
// won in between, and deletes the lease of a session this instance is running.
func TestAnOrphanIsNotRetriedWhileTheAccountIsBeingWorkedOn(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	hop := stallable(t, server.Addr())
	rdb := redis.NewClient(&redis.Options{Addr: hop.addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() {
		quick, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		manager.StopAll(quick)
	})

	const sid = "9c2b7d1e-0000-4000-8000-0000000000bf"
	ctx := context.Background()
	if _, err := manager.Adopt(ctx, sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	// A hand-back that did not reach Redis, which is what leaves the mark.
	hop.stall()
	handing, cancelHanding := context.WithTimeout(ctx, 300*time.Millisecond)
	manager.Release(handing, sid)
	cancelHanding()
	hop.resume()
	if !manager.handingBack(sid) {
		t.Fatal("a release that did not reach Redis left no mark, so nothing would try it again")
	}

	// An adoption of the same account under way, which is the release that must not have
	// a second one sent alongside it.
	if !manager.tryHoldHanding(sid) {
		t.Fatal("the account's turn was already taken")
	}
	defer manager.dropHanding(sid)

	manager.releaseOrphans(ctx)
	if !manager.handingBack(sid) {
		t.Fatal("an orphan was retried alongside work on the same account, and that release deletes whatever lease it wins")
	}
}

// The door holds the giving-up it was shut for, because a session can be given up on
// afresh while an answer about the one before is on its way. Opened by that older answer,
// it is a session marked retired and taking commands until the next sweep gets to it.
func TestADoorIsOnlyOpenedByAnAnswerAboutWhatShutIt(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000c0"
	session, err := manager.Adopt(context.Background(), sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	// Written against the door rather than through the pump, and that is the point of it
	// being here: the two shuttings are a retry given up on again while the answer about
	// the first is still on its way, which no ordering the pump can be driven through
	// reproduces on purpose.
	session.shut(1)
	session.shut(2)
	session.reopen(1)

	if offered := session.Offer(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "c1", Type: protocol.CommandSessionConnect, SID: sid,
		},
		Ack: func(context.Context) error { return nil }, Release: func() {},
	}); offered != OfferStopped {
		t.Fatalf("the session answered %v after an answer about a giving-up it had moved on from, want %v",
			offered, OfferStopped)
	}
}

// A wake for an account this instance is finishing with must not be acknowledged. The
// event saying the engine gave up may still be going out, so the session is not retired
// yet and reads as ordinary: answered with it, the wake is retired, the commands behind it
// are refused by the shut door, and the heartbeat then hands the account back -- unowned,
// with the one wake that would have started it somewhere else already consumed.
func TestAWakeForAnAccountOnItsWayOutIsLeftPending(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	publisher := &heldPublisher{
		holds:   protocol.EventSessionConnectFailure,
		entered: make(chan protocol.EventType, 8),
		let:     make(chan struct{}),
	}
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: publisher, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })
	t.Cleanup(publisher.release)

	const sid = "9c2b7d1e-0000-4000-8000-0000000000c1"
	ctx := context.Background()
	if _, err := manager.Adopt(ctx, sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}

	// The last word is going out: the door is shut and the session is not retired yet.
	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	<-publisher.entered

	if _, err := manager.Adopt(ctx, sid); !errors.Is(err, errLeaving) {
		t.Fatalf("a wake for an account on its way out was answered with %v, want %v", err, errLeaving)
	}
}

// The sweep waits for a command in flight; a wake reaching the same session has to wait
// for it too, or the hand-back it triggers cancels the command from under the client
// waiting for its answer -- and a redelivery of one whose side effect landed without its
// record is a side effect carried out twice.
func TestAWakeDoesNotHandBackASessionWithACommandInFlight(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines, Leases: leases,
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000c2"
	ctx := context.Background()
	session, err := manager.Adopt(ctx, sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}

	release := engineSession.Hold()
	defer release()
	if offered := session.Offer(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "c1", Type: protocol.CommandMessageSend, SID: sid,
		},
		Ack: func(context.Context) error { return nil }, Release: func() {},
	}); offered != OfferAccepted {
		t.Fatalf("the session refused a command with the door open: %v", offered)
	}
	waitFor(t, func() bool { return len(engineSession.Commands()) == 1 }, "the command never reached the engine")

	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	waitFor(t, session.Retired, "the session was never finished with")

	if _, err := manager.Adopt(ctx, sid); !errors.Is(err, errLeaving) {
		t.Fatalf("a wake took a session with a command still running, answering %v, want %v", err, errLeaving)
	}
	if len(engineSession.Commands()) != 1 {
		t.Fatal("the session the wake found was stopped from under the command it was carrying out")
	}
}

// An outcome kept for another try describes the session as it was. A connect that
// succeeded while it waited has already published `open`, and a giving-up after that one
// has published its own: said now, it arrives after both and describes neither, and the
// door it shuts is a door the newer one is holding.
func TestAnOutcomeTheSessionMovedOnFromIsNotSaidAgain(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	publisher := &flakyPublisher{fails: 1, failed: make(chan struct{})}
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: publisher, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
		RetireRetry: 200 * time.Millisecond,
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000c3"
	ctx := context.Background()
	session, err := manager.Adopt(ctx, sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}

	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	<-publisher.failed

	// The retry that answered it, well inside the wait before the outcome would be said
	// again.
	if err := engineSession.Connect(ctx, engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("the retry could not connect: %v", err)
	}

	// The door opening is the pump having reached the outcome it was keeping.
	waitFor(t, func() bool {
		return session.Offer(&transport.Delivery{
			Command: protocol.Command{
				V: protocol.Version, ID: "c1", Type: protocol.CommandMessageSend, SID: sid,
			},
			Ack: func(context.Context) error { return nil }, Release: func() {},
		}) == OfferAccepted
	}, "the door never opened again, so the outcome the session moved on from was never looked at")

	if session.Retired() {
		t.Fatal("an outcome the session had moved on from handed the account over")
	}
	if said := publisher.timesSaid(protocol.EventSessionConnectFailure); said != 1 {
		t.Fatalf("the outcome was published %d times, want the one attempt that failed and no more: said again, it lands after the open the retry published", said)
	}
}

// Taking a command in and counting it as running are one step. A session that has taken
// one and not counted it reads as having nothing running, and a hand-back takes it there:
// the command then runs on a session being stopped, which for a connect is the client
// told that it worked over a socket closed a moment later.
func TestASessionThatTookACommandIsNotFreeToBeHandedOver(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	engines := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000c4"
	session, err := manager.Adopt(context.Background(), sid)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for the account that was just adopted")
	}
	engineSession.EmitLast(protocol.EventSessionConnectFailure, map[string]any{"reason": "unavailable"})
	waitFor(t, session.Retired, "the session was never finished with")

	// Written against the two rather than through the executor, because what has to hold
	// is that no moment exists between them: a command is in or it is not, and a session
	// that has one is not free.
	if session.admit() {
		t.Fatal("a session whose door is shut took a command in")
	}
	session.reopen(session.retiredOn.Load())
	if !session.admit() {
		t.Fatal("a session with an open door refused a command")
	}
	if session.claim() {
		t.Fatal("a session that had just taken a command in was handed over as free")
	}
	session.doneWith()
}

// A hand-back is one instance's own business until it lands, and the wake that would put
// the account back to work is read by every instance in the fleet. The one giving the
// account up knows to leave that wake pending; a peer reading the same entry finds a
// lease naming somebody else and, with nothing else to go on, acknowledges it as an
// account already running. The release then lands, and the account is owned by nobody
// with the one wake that would have started it retired.
//
// Two instances, because that is the whole of it: the state the first one acts on is
// state the second one cannot see.
func TestAPeerLeavesAWakeForAnAccountBeingHandedBackPending(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	giving := redis.NewClient(&redis.Options{Addr: server.Addr()})
	taking := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = giving.Close(); _ = taking.Close() })
	givingClient := redisx.Wrap(giving, "wa:", 8)
	takingClient := redisx.Wrap(taking, "wa:", 8)
	keys := givingClient.Keys()

	const sid = "9c2b7d1e-0000-4000-8000-0000000000c1"
	// Dropped rather than answered with an error: the hand-back must not reach the
	// server, or there would be no lease left to find and nothing to be misread.
	var losing atomic.Bool
	giving.AddHook(dropped{when: func(cmd redis.Cmder) bool {
		return losing.Load() && handsBack(cmd, keys.HandBack(sid))
	}})

	holder := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(givingClient, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	peer := NewManager(&ManagerConfig{
		Instance: "inst-b", Engine: fake.New(),
		Leases:    cluster.NewLeases(takingClient, "inst-b", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()
	t.Cleanup(func() { holder.StopAll(ctx); peer.StopAll(ctx) })

	if _, err := holder.Adopt(ctx, sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	losing.Store(true)
	holder.Release(ctx, sid)
	if !holder.handingBack(sid) {
		t.Fatal("a hand-back that never reached Redis was forgotten, so nothing would try it again")
	}
	if !server.Exists(keys.HandBack(sid)) {
		t.Fatal("a hand-back left no mark for peers to read, so a wake for the account reads to them as somebody else's")
	}

	var acked, left bool
	peer.wake(ctx, &transport.Delivery{
		Command: protocol.Command{V: protocol.Version, ID: "wake", Type: protocol.CommandSessionWake, SID: sid},
		Ack:     func(context.Context) error { acked = true; return nil },
		Release: func() { left = true },
		Forfeit: func() {
			t.Error("a peer gave up its place in the pending list over a hand-back it only had to wait out")
		},
	})
	if acked {
		t.Fatal("a peer retired the wake for an account being handed back; once the release lands nothing is left to start it")
	}
	if !left {
		t.Fatal("the wake was neither acknowledged nor left pending, so nothing will read it again")
	}

	// And the mark goes when the lease does, or a wake for an account nobody owns is
	// left pending for as long as the mark outlives the lease it was about.
	losing.Store(false)
	holder.releaseOrphans(ctx)
	if server.Exists(keys.HandBack(sid)) {
		t.Fatal("a hand-back landed and left its mark behind, so wakes for a free account go on being left pending")
	}
	if _, err := peer.Adopt(ctx, sid); err != nil {
		t.Fatalf("adopting an account whose hand-back landed: %v", err)
	}
}

// dropped answers a command without sending it, which is what a round trip that never
// reaches Redis looks like from the instance that asked for it. Unlike a stalled hop it
// picks one command out of the traffic, so the rest of what a test does still lands.
type dropped struct{ when func(redis.Cmder) bool }

func (dropped) DialHook(next redis.DialHook) redis.DialHook { return next }

// Pipelines too, because the renewals go out as one batch: a hook that only covers the
// commands sent on their own leaves the very traffic a test about renewals is naming.
func (h dropped) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		sending := make([]redis.Cmder, 0, len(cmds))
		var dropped error
		for _, cmd := range cmds {
			if !h.when(cmd) {
				sending = append(sending, cmd)
				continue
			}
			dropped = errors.New("the answer never came back")
			cmd.SetErr(dropped)
		}
		if len(sending) > 0 {
			if err := next(ctx, sending); err != nil {
				return err
			}
		}
		return dropped
	}
}

func (h dropped) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if !h.when(cmd) {
			return next(ctx, cmd)
		}
		err := errors.New("the answer never came back")
		cmd.SetErr(err)
		return err
	}
}

// names reports whether a command carries a key.
func names(cmd redis.Cmder, key string) bool {
	for _, arg := range cmd.Args() {
		if text, ok := arg.(string); ok && text == key {
			return true
		}
	}
	return false
}

// handsBack names the hand-back of one session among the lease scripts.
//
// By what the script is told and not by the keys it takes, because the keys no longer
// separate them: renewing takes the lease alone, and acquiring, marking and handing back
// all take the lease and the mark. What is left is the arguments -- the hand-back needs
// only the instance's name, and the other two also carry a lifetime.
func handsBack(cmd redis.Cmder, handBackKey string) bool {
	return strings.HasPrefix(cmd.Name(), "eval") && names(cmd, handBackKey) && len(cmd.Args()) == 3+keysOf(cmd)+1
}

// The mark has to be in Redis before the session stops, not merely before the release.
// Stopping is not instant -- it closes a socket and drains what the session was holding
// -- and for all of it the lease still names this instance while nothing here runs the
// account. A wake landing in that window finds an owner and is acknowledged as an
// account already running, and the release that follows leaves the account owned by
// nobody. The window between the release going out and landing is the one the mark
// obviously covers; this is the wider one in front of it.
func TestAHandBackIsMarkedBeforeTheSessionStops(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	giving := redis.NewClient(&redis.Options{Addr: server.Addr()})
	taking := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = giving.Close(); _ = taking.Close() })
	givingClient := redisx.Wrap(giving, "wa:", 8)
	takingClient := redisx.Wrap(taking, "wa:", 8)
	keys := givingClient.Keys()

	// One event held parks the stop: Session.Stop closes the engine and then waits for
	// the pump, and the pump is in the publisher.
	held := &heldPublisher{
		holds:   protocol.EventSessionState,
		entered: make(chan protocol.EventType),
		let:     make(chan struct{}),
	}
	engines := fake.New()
	holder := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases:    cluster.NewLeases(givingClient, "inst-a", cluster.Options{}),
		Publisher: held, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	peer := NewManager(&ManagerConfig{
		Instance: "inst-b", Engine: fake.New(),
		Leases:    cluster.NewLeases(takingClient, "inst-b", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()

	const sid = "9c2b7d1e-0000-4000-8000-0000000000c2"
	if _, err := holder.Adopt(ctx, sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for an account that was adopted")
	}

	// Armed only now, so the acquisition above -- which reads the mark and so carries the
	// same two keys -- is not mistaken for the writing of one.
	var handing atomic.Bool
	marked := make(chan struct{})
	var announce sync.Once
	giving.AddHook(watching{after: func(cmd redis.Cmder) {
		if handing.Load() && names(cmd, keys.HandBack(sid)) {
			announce.Do(func() { close(marked) })
		}
	}})

	engineSession.Emit(protocol.EventSessionState, map[string]any{"state": "connected"})
	<-held.entered

	handing.Store(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		holder.Release(ctx, sid)
	}()
	defer func() {
		held.release()
		<-done
	}()

	select {
	case <-marked:
	case <-time.After(5 * time.Second):
		t.Fatal("the hand-back was not marked while the session was still stopping, so every wake in that window is acknowledged as an account somebody else runs")
	}

	var acked, left bool
	peer.wake(ctx, &transport.Delivery{
		Command: protocol.Command{V: protocol.Version, ID: "wake", Type: protocol.CommandSessionWake, SID: sid},
		Ack:     func(context.Context) error { acked = true; return nil },
		Release: func() { left = true },
	})
	if acked {
		t.Fatal("a peer retired the wake for an account that was still stopping here; the release that follows leaves it owned by nobody")
	}
	if !left {
		t.Fatal("the wake was neither acknowledged nor left pending, so nothing will read it again")
	}
}

// A hand-back that did not land is tried again on a later tick, and by then the account
// may have moved on: the lease expired, a peer took it, and that peer is now handing it
// back itself. An unfenced mark would put this instance's name over the peer's, and the
// comparison every reader makes -- mark against holder -- then says nobody is handing
// anything back, on an account that is.
func TestAStaleHandBackDoesNotOverwriteTheMarkOfTheInstanceThatHoldsTheLease(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	first := redis.NewClient(&redis.Options{Addr: server.Addr()})
	second := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = first.Close(); _ = second.Close() })
	firstClient := redisx.Wrap(first, "wa:", 8)
	secondClient := redisx.Wrap(second, "wa:", 8)
	keys := firstClient.Keys()

	const sid = "9c2b7d1e-0000-4000-8000-0000000000c3"
	// The second instance's hand-back reaches Redis to leave its mark and never gets to
	// delete the lease, which is the state a stale retry can land in the middle of.
	second.AddHook(dropped{when: func(cmd redis.Cmder) bool { return handsBack(cmd, keys.HandBack(sid)) }})

	stale := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(firstClient, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	holder := NewManager(&ManagerConfig{
		Instance: "inst-b", Engine: fake.New(),
		Leases:    cluster.NewLeases(secondClient, "inst-b", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()
	t.Cleanup(func() { stale.StopAll(ctx); holder.StopAll(ctx) })

	if _, err := stale.Adopt(ctx, sid); err != nil {
		t.Fatalf("Adopt on the first instance: %v", err)
	}

	// A hand-back that reached Redis with neither half: no mark, and a lease left naming
	// this instance. Through a context that is already over, which is what a Redis nobody
	// can reach looks like from here without a hop to stall.
	over, giveUp := context.WithCancel(ctx)
	giveUp()
	stale.Release(over, sid)
	if !stale.handingBack(sid) {
		t.Fatal("a hand-back that reached nothing was forgotten, so nothing would try it again")
	}

	// The lease runs out and the account moves. The second instance stops it in turn, and
	// its own hand-back gets as far as the mark.
	server.FastForward(cluster.DefaultTTL + time.Second)
	if _, err := holder.Adopt(ctx, sid); err != nil {
		t.Fatalf("Adopt on the second instance: %v", err)
	}
	holder.Release(ctx, sid)
	if got, _ := server.Get(keys.HandBack(sid)); got != "inst-b" {
		t.Fatalf("the instance holding the lease marked its hand-back as %q, want inst-b", got)
	}

	stale.releaseOrphans(ctx)
	if got, _ := server.Get(keys.HandBack(sid)); got != "inst-b" {
		t.Fatalf("a stale retry left the hand-back mark naming %q, want the instance that holds the lease", got)
	}

	var acked, left bool
	stale.wake(ctx, &transport.Delivery{
		Command: protocol.Command{V: protocol.Version, ID: "wake", Type: protocol.CommandSessionWake, SID: sid},
		Ack:     func(context.Context) error { acked = true; return nil },
		Release: func() { left = true },
	})
	if acked {
		t.Fatal("a wake was retired for an account whose owner is handing it back, because a stale retry had overwritten the mark")
	}
	if !left {
		t.Fatal("the wake was neither acknowledged nor left pending, so nothing will read it again")
	}
}

// watching runs after a command has been answered, which is where a test learns that a
// key is in Redis rather than merely on its way.
//
// `batch` sees a pipeline whole, which is the only way to ask what went out together: a
// shutdown's marks are one batch under one deadline, and which sessions are in it is a
// different question from which sessions were marked at all.
type watching struct {
	after func(redis.Cmder)
	batch func([]redis.Cmder)
}

func (watching) DialHook(next redis.DialHook) redis.DialHook { return next }

// Pipelines too, because a shutdown marks its whole list in one batch: a hook that only
// covers the commands sent on their own would miss exactly what it is watching for.
func (h watching) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		if h.batch != nil {
			h.batch(cmds)
		}
		if h.after != nil {
			for _, cmd := range cmds {
				h.after(cmd)
			}
		}
		return err
	}
}

func (h watching) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if h.after != nil {
			h.after(cmd)
		}
		return err
	}
}

// The one place the mark does not go before the stop. Everything the renewals give up on
// failed to renew, and Redis being unreachable is the common reason: a mark asked for
// there waits out a network that is not answering, once per session, while the sockets
// those leases were covering are still open and peers are free to take the accounts. Two
// live sockets on one account is what the lease exists to prevent, and it is a worse
// outcome by a wide margin than a wake retired inside the margin's worth of lease left.
func TestARenewalThatGaveUpStopsTheSocketBeforeTalkingToRedisAgain(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	hop := stallable(t, server.Addr())
	rdb := redis.NewClient(&redis.Options{Addr: hop.addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	clock := &steppingClock{now: time.Now()}
	engines := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{Clock: clock}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})

	const sid = "9c2b7d1e-0000-4000-8000-0000000000c4"
	ctx := context.Background()
	if _, err := manager.Adopt(ctx, sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for an account that was adopted")
	}

	// A renewal that fails without reaching Redis, so the answer is here at once and the
	// lease behind it is past being fresh: the branch that lets the session go.
	rdb.AddHook(dropped{when: func(cmd redis.Cmder) bool {
		return strings.HasPrefix(cmd.Name(), "eval") && keysOf(cmd) == 1
	}})
	clock.step(cluster.DefaultTTL)
	// And a Redis that answers nothing from here on, which is what the mark would wait
	// out. The hand-backs that follow are bounded by the tick's own budget; the stop in
	// front of them must not be behind anything at all.
	hop.stall()
	t.Cleanup(hop.resume)

	swept := make(chan struct{})
	go func() {
		defer close(swept)
		// Off the wall clock and not off the stepped one: the budget is a deadline a
		// context waits out for real, and the stepped clock is thirty seconds ahead.
		manager.RenewAll(ctx, time.Now().Add(300*time.Millisecond))
	}()
	t.Cleanup(func() { <-swept })

	// Closing the engine session is what closes this, so a receive means the socket is
	// down. Nothing is emitted, so there is nothing else a receive could be.
	select {
	case <-engineSession.Events():
	case <-time.After(2 * time.Second):
		t.Fatal("a session whose lease had gone stale was still holding its socket open while a mark waited on a Redis that was not answering")
	}
}

// A hand-back that never landed leaves a mark outliving the lease it was about, and the
// instance that wrote it can win the account back under its own name. The mark then
// equals the holder while that holder is running the session, and every wake for it is
// left pending on the strength of a hand-back that is over.
func TestWinningALeaseClearsTheMarkOfTheHandBackBeforeIt(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	giving := redis.NewClient(&redis.Options{Addr: server.Addr()})
	taking := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = giving.Close(); _ = taking.Close() })
	givingClient := redisx.Wrap(giving, "wa:", 8)
	takingClient := redisx.Wrap(taking, "wa:", 8)
	keys := givingClient.Keys()

	const sid = "9c2b7d1e-0000-4000-8000-0000000000c5"
	holder := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(givingClient, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	peer := NewManager(&ManagerConfig{
		Instance: "inst-b", Engine: fake.New(),
		Leases:    cluster.NewLeases(takingClient, "inst-b", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()
	t.Cleanup(func() { holder.StopAll(ctx); peer.StopAll(ctx) })

	if _, err := holder.Adopt(ctx, sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	// Half a lease in, so the mark the hand-back writes outlives the lease it is about by
	// the other half. That gap is what the account can be won back inside.
	server.FastForward(cluster.DefaultTTL / 2)

	giving.AddHook(dropped{when: func(cmd redis.Cmder) bool { return handsBack(cmd, keys.HandBack(sid)) }})
	holder.Release(ctx, sid)
	if !server.Exists(keys.HandBack(sid)) {
		t.Fatal("a hand-back that did not land left no mark, so there is nothing for the account to be won back under")
	}

	server.FastForward(cluster.DefaultTTL/2 + time.Second)
	if _, err := holder.Adopt(ctx, sid); err != nil {
		t.Fatalf("winning the account back: %v", err)
	}
	if server.Exists(keys.HandBack(sid)) {
		t.Fatal("a lease taken afresh kept the mark of the hand-back before it, so the account reads as being given up while it runs")
	}

	var acked, left bool
	peer.wake(ctx, &transport.Delivery{
		Command: protocol.Command{V: protocol.Version, ID: "wake", Type: protocol.CommandSessionWake, SID: sid},
		Ack:     func(context.Context) error { acked = true; return nil },
		Release: func() { left = true },
	})
	if left {
		t.Fatal("a wake for an account that is running was left pending, on the strength of a hand-back that is over")
	}
	if !acked {
		t.Fatal("a wake for an account somebody is running was neither acknowledged nor left pending")
	}
}

// steppingClock is a clock a test moves by hand, so a lease can go stale without the test
// waiting out its life.
type steppingClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *steppingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *steppingClock) step(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// keysOf reports how many keys a script command carries, counted where go-redis puts it:
// after the command and the script, ahead of the keys themselves. It is what tells the
// lease scripts apart regardless of which session they are about -- renewing takes the
// lease alone, acquiring takes the lease and the mark, handing back takes those and the
// cooldown.
func keysOf(cmd redis.Cmder) int {
	args := cmd.Args()
	if len(args) < 3 {
		return 0
	}
	count, ok := args[2].(int)
	if !ok {
		return 0
	}
	return count
}

// The mark goes in front of the stop, and the stop is what takes the socket down. A
// Redis that answers nothing would otherwise hold it there: the lease it names goes on
// expiring meanwhile, a peer with a working Redis takes the account, and this instance is
// still talking to WhatsApp on it. Two live sockets on one account is the one thing the
// lease exists to prevent, and it outweighs by a wide margin the wake the mark was for.
func TestAMarkThatCannotBeWrittenDoesNotHoldTheSocketOpen(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	hop := stallable(t, server.Addr())
	rdb := redis.NewClient(&redis.Options{Addr: hop.addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })

	// Short, because the bound is a fraction of it: the whole point is that the mark
	// cannot outlast the lease it is about, however the lease is configured.
	const ttl = 300 * time.Millisecond
	engines := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases: cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{
			TTL: ttl, Margin: ttl / 10,
		}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})

	const sid = "9c2b7d1e-0000-4000-8000-0000000000c6"
	ctx := context.Background()
	if _, err := manager.Adopt(ctx, sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for an account that was adopted")
	}

	hop.stall()
	released := make(chan struct{})
	// Registered before the resume, so the resume runs first: cleanups run last in, first
	// out, and the hand-back behind the stop is on the caller's own unbounded context. It
	// is not what this measures, and waiting it out against a stalled hop would put the
	// wait back into the test by the other door.
	t.Cleanup(func() { <-released })
	t.Cleanup(hop.resume)
	go func() {
		defer close(released)
		// The caller's own context has all the time in the world, which is what a
		// shutdown grace looks like next to a lease this short. The bound has to come
		// from the lease.
		manager.Release(ctx, sid)
	}()

	// Closing the engine session is what closes this, so a receive means the socket is
	// down. Nothing is emitted, so there is nothing else a receive could be.
	select {
	case <-engineSession.Events():
	case <-time.After(2 * time.Second):
		t.Fatal("a session held its socket open on a mark that was waiting out a Redis that does not answer, for longer than the lease a peer can take the account on")
	}
}

// A shutdown stops its sessions one after another, and the mark goes in front of each
// stop. Asked once per session against a Redis that answers nothing, the waits stack: the
// last account on the list is still talking to WhatsApp long after the lease a peer can
// take it on has expired, which is the one thing the lease exists to prevent. One batch,
// under one bound, is what keeps the shutdown's cost off the number of sessions.
func TestAShutdownDoesNotWaitOncePerSessionBeforeStoppingThem(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	hop := stallable(t, server.Addr())
	rdb := redis.NewClient(&redis.Options{Addr: hop.addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })

	// The bound on one mark is a third of this, so six sessions marked one at a time
	// spend six of them and the batch spends one.
	const ttl = 900 * time.Millisecond
	engines := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases: cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{
			TTL: ttl, Margin: ttl / 10,
		}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})

	ctx := context.Background()
	sessions := make([]*fake.Session, 0, 6)
	for i := range 6 {
		sid := fmt.Sprintf("9c2b7d1e-0000-4000-8000-00000000d0%02d", i)
		if _, err := manager.Adopt(ctx, sid); err != nil {
			t.Fatalf("Adopt %s: %v", sid, err)
		}
		engineSession, running := engines.Session(sid)
		if !running {
			t.Fatalf("the engine has no session for %s", sid)
		}
		sessions = append(sessions, engineSession)
	}

	hop.stall()
	stopped := make(chan struct{})
	// Before the resume, so the resume runs first: the hand-backs behind the stops are on
	// the caller's own context and are not what this measures.
	t.Cleanup(func() { <-stopped })
	t.Cleanup(hop.resume)
	go func() {
		defer close(stopped)
		manager.StopAll(ctx)
	}()

	// Closing the engine session is what closes these, so a receive on every one means
	// every socket is down. Three marks' worth of room, against the six a mark per
	// session would spend.
	deadline := time.After(ttl)
	for i, engineSession := range sessions {
		select {
		case <-engineSession.Events():
		case <-deadline:
			t.Fatalf("a shutdown was still holding sockets open at session %d of %d, waiting out a Redis that does not answer once per session", i, len(sessions))
		}
	}
}

// The mark before a stop is bounded, and the bound has to come from what is left of the
// lease rather than from how long a lease is configured to last. A hand-back that starts
// near the end of one has less than a share of a TTL to give: a wait sized from the TTL
// would run past the moment a peer may take the account, with this instance still
// talking to WhatsApp on it. With nothing left, there is no mark worth making at all.
func TestAMarkIsSkippedWhenTheLeaseHasNothingLeftToSpend(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	hop := stallable(t, server.Addr())
	rdb := redis.NewClient(&redis.Options{Addr: hop.addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })

	// A third of this is what the mark would be given if the lease's own life were not
	// asked about, which is what the reversion of this waits out.
	const ttl = 3 * time.Second
	clock := &steppingClock{now: time.Now()}
	engines := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases: cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{
			TTL: ttl, Margin: ttl / 10, Clock: clock,
		}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})

	const sid = "9c2b7d1e-0000-4000-8000-0000000000c7"
	ctx := context.Background()
	if _, err := manager.Adopt(ctx, sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, running := engines.Session(sid)
	if !running {
		t.Fatal("the engine has no session for an account that was adopted")
	}

	// Every bit of the lease this instance may act on is spent, which is the state a
	// hand-back can start in whenever a renewal was the last thing that went right.
	clock.step(ttl)
	hop.stall()
	released := make(chan struct{})
	t.Cleanup(func() { <-released })
	t.Cleanup(hop.resume)
	go func() {
		defer close(released)
		manager.Release(ctx, sid)
	}()

	// Closing the engine session is what closes this, so a receive means the socket is
	// down. Well inside the third of a lease a mark sized from the TTL alone would have
	// waited out.
	select {
	case <-engineSession.Events():
	case <-time.After(400 * time.Millisecond):
		t.Fatal("a session with no lease left to spend still held its socket open on a mark, past the moment a peer may take the account")
	}
}

// A mark that runs after the socket is already down has nobody waiting on it, and it is
// the only thing between a peer's wake and an account left unowned. Bounding it by what
// is left of the lease locally reads the wrong clock: freshness at zero says this
// instance may not act on the lease, not that Redis has stopped naming it -- and Release
// forgets the lease locally before its first attempt, so every retry finds zero.
func TestAHandBackRetriedAfterTheStopIsStillMarked(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	keys := client.Keys()

	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000c8"
	if _, err := manager.Adopt(ctx, sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	// A hand-back that reached Redis with neither half: no mark, and a lease left naming
	// this instance. It also forgets the lease locally, which is what leaves every retry
	// reading zero freshness for a lease Redis still holds.
	over, giveUp := context.WithCancel(ctx)
	giveUp()
	manager.Release(over, sid)
	if server.Exists(keys.HandBack(sid)) {
		t.Fatal("a hand-back that reached nothing left a mark, so this proves nothing about the retry")
	}
	if !manager.handingBack(sid) {
		t.Fatal("a hand-back that reached nothing was forgotten, so nothing would try it again")
	}

	// Watched rather than looked up afterwards: the retry marks and then releases, and a
	// release that lands takes the mark with it. What has to have happened is the mark
	// going out at all.
	//
	// The mark is the script that names the key and is not the hand-back: handing back
	// needs only the instance's name, and marking also carries a lifetime. Nothing
	// acquires during a retry, so nothing else answers to that.
	var marked atomic.Bool
	rdb.AddHook(watching{after: func(cmd redis.Cmder) {
		if strings.HasPrefix(cmd.Name(), "eval") && names(cmd, keys.HandBack(sid)) &&
			!handsBack(cmd, keys.HandBack(sid)) {
			marked.Store(true)
		}
	}})

	manager.releaseOrphans(ctx)
	if !marked.Load() {
		t.Fatal("a hand-back retried after the stop never marked, so a peer's wake for the account is acknowledged as somebody else's while the release is in flight")
	}
}

// A shutdown marks its whole list in one round trip, and one lease that has run out must
// not stand between every other socket and the mark its account needs: skipping the batch
// leaves fresh sessions stopped under live leases with nothing a peer can read, for the
// whole length of the loop that stops them.
func TestOneStaleLeaseDoesNotSuppressTheMarksOfTheRest(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	keys := client.Keys()

	const ttl = 3 * time.Second
	clock := &steppingClock{now: time.Now()}
	held := &heldPublisher{
		holds:   protocol.EventSessionState,
		entered: make(chan protocol.EventType),
		let:     make(chan struct{}),
	}
	engines := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases: cluster.NewLeases(client, "inst-a", cluster.Options{
			TTL: ttl, Margin: ttl / 10, Clock: clock,
		}),
		Publisher: held, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})

	const stale = "9c2b7d1e-0000-4000-8000-0000000000c9"
	const fresh = "9c2b7d1e-0000-4000-8000-0000000000ca"
	ctx := context.Background()
	if _, err := manager.Adopt(ctx, stale); err != nil {
		t.Fatalf("Adopt %s: %v", stale, err)
	}
	// Everything the older lease had to give is spent, and the newer one is taken now.
	clock.step(ttl)
	if _, err := manager.Adopt(ctx, fresh); err != nil {
		t.Fatalf("Adopt %s: %v", fresh, err)
	}

	// The fresh session's stop is parked in the pump, so nothing that happens after it
	// can be what wrote the mark: only the batch in front of the stops can.
	freshSession, running := engines.Session(fresh)
	if !running {
		t.Fatalf("the engine has no session for %s", fresh)
	}
	marked := make(chan struct{})
	var announce sync.Once
	rdb.AddHook(watching{after: func(cmd redis.Cmder) {
		if names(cmd, keys.HandBack(fresh)) {
			announce.Do(func() { close(marked) })
		}
	}})
	freshSession.Emit(protocol.EventSessionState, map[string]any{"state": "connected"})
	<-held.entered

	stopped := make(chan struct{})
	t.Cleanup(func() { <-stopped })
	t.Cleanup(held.release)
	go func() {
		defer close(stopped)
		manager.StopAll(ctx)
	}()

	select {
	case <-marked:
	case <-time.After(2 * time.Second):
		t.Fatal("one lease that had run out kept every other session in the shutdown from being marked, so a peer's wake for a live account is acknowledged as somebody else's")
	}
}

// A batch sized by its most nearly expired member is one that can run out before the
// request is even sent, and then nothing in it is marked: one lease near its end would
// cost every other account in the shutdown the mark that keeps it from being left
// unowned. Being near the end is not being over it, so the check cannot be for zero.
func TestANearlyExpiredLeaseIsLeftOutOfTheShutdownBatch(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	keys := client.Keys()

	const ttl = 3 * time.Second
	clock := &steppingClock{now: time.Now()}
	held := &heldPublisher{
		holds:   protocol.EventSessionState,
		entered: make(chan protocol.EventType),
		let:     make(chan struct{}),
	}
	engines := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases: cluster.NewLeases(client, "inst-a", cluster.Options{
			TTL: ttl, Margin: ttl / 10, Clock: clock,
		}),
		Publisher: held, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})

	const ending = "9c2b7d1e-0000-4000-8000-0000000000cb"
	const fresh = "9c2b7d1e-0000-4000-8000-0000000000cc"
	ctx := context.Background()
	if _, err := manager.Adopt(ctx, ending); err != nil {
		t.Fatalf("Adopt %s: %v", ending, err)
	}
	// Ten milliseconds of lease left: more than none, and less than the round trip a mark
	// would need, which is exactly the case a check for zero lets through.
	clock.step(ttl - ttl/10 - 10*time.Millisecond)
	if _, err := manager.Adopt(ctx, fresh); err != nil {
		t.Fatalf("Adopt %s: %v", fresh, err)
	}

	// The fresh session's stop is parked in the pump, so everything Redis has been asked
	// by then is the batch in front of the stops and nothing else.
	freshSession, running := engines.Session(fresh)
	if !running {
		t.Fatalf("the engine has no session for %s", fresh)
	}
	// The batch whole, not the marks one by one: the lease near its end is marked too,
	// with its own release, and what must not happen is it sharing the batch's deadline.
	var markedFresh, batchedEnding atomic.Bool
	rdb.AddHook(watching{batch: func(cmds []redis.Cmder) {
		for _, cmd := range cmds {
			if names(cmd, keys.HandBack(fresh)) {
				markedFresh.Store(true)
			}
			if names(cmd, keys.HandBack(ending)) {
				batchedEnding.Store(true)
			}
		}
	}})
	freshSession.Emit(protocol.EventSessionState, map[string]any{"state": "connected"})
	<-held.entered

	stopped := make(chan struct{})
	t.Cleanup(func() { <-stopped })
	t.Cleanup(held.release)
	go func() {
		defer close(stopped)
		manager.StopAll(ctx)
	}()

	waitFor(t, markedFresh.Load, "the shutdown never marked the lease that had room for it, so a peer wake for it is acknowledged as somebody else's")
	if batchedEnding.Load() {
		t.Fatal("a lease with less life than a mark needs went into the shared batch, where its deadline is every other account's too")
	}
}

// Being left out of the batch is not enough for a lease near its end: its socket has to
// come down before anything blocks on Redis at all. Waiting out a batch it is not even
// in spends the last of a lease a peer is about to be free to take, with this instance
// still talking to WhatsApp on the account.
func TestANearlyExpiredSocketComesDownBeforeTheBatchBlocks(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	hop := stallable(t, server.Addr())
	rdb := redis.NewClient(&redis.Options{Addr: hop.addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })

	// A third of this is the bound the batch waits out, which is what the reversion of
	// this holds the near-expiry socket open for.
	const ttl = 3 * time.Second
	clock := &steppingClock{now: time.Now()}
	engines := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases: cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{
			TTL: ttl, Margin: ttl / 10, Clock: clock,
		}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})

	const ending = "9c2b7d1e-0000-4000-8000-0000000000cd"
	const fresh = "9c2b7d1e-0000-4000-8000-0000000000ce"
	ctx := context.Background()
	if _, err := manager.Adopt(ctx, ending); err != nil {
		t.Fatalf("Adopt %s: %v", ending, err)
	}
	endingSession, running := engines.Session(ending)
	if !running {
		t.Fatalf("the engine has no session for %s", ending)
	}
	clock.step(ttl - ttl/10 - 10*time.Millisecond)
	if _, err := manager.Adopt(ctx, fresh); err != nil {
		t.Fatalf("Adopt %s: %v", fresh, err)
	}

	// From here the batch reaches nobody and waits out its whole bound.
	hop.stall()
	stopped := make(chan struct{})
	t.Cleanup(func() { <-stopped })
	t.Cleanup(hop.resume)
	go func() {
		defer close(stopped)
		manager.StopAll(ctx)
	}()

	// Closing the engine session is what closes this, so a receive means the socket is
	// down. Well inside the bound the batch is spending meanwhile.
	select {
	case <-endingSession.Events():
	case <-time.After(300 * time.Millisecond):
		t.Fatal("a socket whose lease was nearly out waited on a batch of marks it was not even in, past the moment a peer may take the account")
	}
}

// A socket taken down early still leaves a lease naming this instance, and a shutdown
// that defers its release until after the batch and every other stop leaves it live and
// unmarked for that whole stretch: a peer's wake in there is acknowledged into nothing,
// which is the bug this branch exists to close. The release goes out behind the stop,
// where the window is one round trip.
func TestALeaseTakenDownEarlyGoesBackBeforeTheBatch(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	keys := client.Keys()

	const ttl = 3 * time.Second
	clock := &steppingClock{now: time.Now()}
	engines := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases: cluster.NewLeases(client, "inst-a", cluster.Options{
			TTL: ttl, Margin: ttl / 10, Clock: clock,
		}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})

	const ending = "9c2b7d1e-0000-4000-8000-0000000000cf"
	const fresh = "9c2b7d1e-0000-4000-8000-0000000000d0"
	ctx := context.Background()
	if _, err := manager.Adopt(ctx, ending); err != nil {
		t.Fatalf("Adopt %s: %v", ending, err)
	}
	clock.step(ttl - ttl/10 - 10*time.Millisecond)
	if _, err := manager.Adopt(ctx, fresh); err != nil {
		t.Fatalf("Adopt %s: %v", fresh, err)
	}

	// The order the lease near its end is treated in, against the batch that follows it.
	var order []string
	var recorded sync.Mutex
	note := func(what string) {
		recorded.Lock()
		defer recorded.Unlock()
		if len(order) == 0 || order[len(order)-1] != what {
			order = append(order, what)
		}
	}
	rdb.AddHook(watching{
		after: func(cmd redis.Cmder) {
			if handsBack(cmd, keys.HandBack(ending)) {
				note("the lease taken down early went back")
			}
		},
		batch: func(cmds []redis.Cmder) {
			for _, cmd := range cmds {
				if names(cmd, keys.HandBack(fresh)) {
					note("the batch went out")
				}
			}
		},
	})

	manager.StopAll(ctx)

	recorded.Lock()
	defer recorded.Unlock()
	if len(order) == 0 || order[0] != "the lease taken down early went back" {
		t.Fatalf("a lease taken down early was left live and unmarked while the rest of the shutdown ran; order was %v", order)
	}
}

// The split between the leases worth marking and the ones that have to come down first
// is true when it is taken, and taking it once for a whole shutdown makes it a claim
// about the past: the stops and releases that follow spend exactly the room the rest were
// just measured to have. A lease that ran out meanwhile must not still be in the batch,
// under a deadline it no longer has the life to sit through.
func TestTheShutdownSplitIsTakenAgainAsTimePasses(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	keys := client.Keys()

	const ttl = 3 * time.Second
	clock := &steppingClock{now: time.Now()}
	engines := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases: cluster.NewLeases(client, "inst-a", cluster.Options{
			TTL: ttl, Margin: ttl / 10, Clock: clock,
		}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})

	const ending = "9c2b7d1e-0000-4000-8000-0000000000d1"
	const borderline = "9c2b7d1e-0000-4000-8000-0000000000d2"
	ctx := context.Background()
	if _, err := manager.Adopt(ctx, ending); err != nil {
		t.Fatalf("Adopt %s: %v", ending, err)
	}
	// Far enough in that the first lease has less than a mark's worth left, and the
	// second, taken now, has more.
	clock.step(ttl - ttl/10 - 900*time.Millisecond)
	if _, err := manager.Adopt(ctx, borderline); err != nil {
		t.Fatalf("Adopt %s: %v", borderline, err)
	}

	// Handing the first one back is what spends the second one's room, which is the whole
	// point: the shutdown's own work is what makes the split it took go stale.
	var batched atomic.Bool
	rdb.AddHook(watching{
		after: func(cmd redis.Cmder) {
			if handsBack(cmd, keys.HandBack(ending)) {
				clock.step(900 * time.Millisecond)
			}
		},
		batch: func(cmds []redis.Cmder) {
			for _, cmd := range cmds {
				if names(cmd, keys.HandBack(borderline)) {
					batched.Store(true)
				}
			}
		},
	})

	manager.StopAll(ctx)

	if batched.Load() {
		t.Fatal("a lease that ran out while the shutdown was working stayed in the batch, on a split taken before its room was spent")
	}
}

// The leases taken down early are handed back one after another, and a hand-back is a
// round trip that can hang. Interleaved with their stops, the second account is still
// talking to WhatsApp because the first one is waiting on a Redis that does not answer,
// and its own lease expires meanwhile.
func TestEveryExpiringSocketComesDownBeforeAnyOfTheirLeasesGoBack(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	hop := stallable(t, server.Addr())
	rdb := redis.NewClient(&redis.Options{Addr: hop.addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })

	const ttl = 3 * time.Second
	clock := &steppingClock{now: time.Now()}
	engines := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engines,
		Leases: cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{
			TTL: ttl, Margin: ttl / 10, Clock: clock,
		}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})

	ctx := context.Background()
	sessions := make([]*fake.Session, 0, 3)
	for i := range 3 {
		sid := fmt.Sprintf("9c2b7d1e-0000-4000-8000-00000000d3%02d", i)
		if _, err := manager.Adopt(ctx, sid); err != nil {
			t.Fatalf("Adopt %s: %v", sid, err)
		}
		engineSession, running := engines.Session(sid)
		if !running {
			t.Fatalf("the engine has no session for %s", sid)
		}
		sessions = append(sessions, engineSession)
	}
	// Every lease past the room a mark needs, which is the group this is about.
	clock.step(ttl)

	hop.stall()
	stopped := make(chan struct{})
	t.Cleanup(func() { <-stopped })
	t.Cleanup(hop.resume)
	go func() {
		defer close(stopped)
		manager.StopAll(ctx)
	}()

	// Closing the engine session is what closes these, so a receive on every one means
	// every socket is down. Nothing here is a round trip, so the only thing that could
	// take this long is a hand-back in front of a stop.
	deadline := time.After(400 * time.Millisecond)
	for i, engineSession := range sessions {
		select {
		case <-engineSession.Events():
		case <-deadline:
			t.Fatalf("a shutdown was still holding sockets open at session %d of %d, waiting out a hand-back for the account before it", i, len(sessions))
		}
	}
}

// The mark and the release behind it share one budget wherever a caller hands the
// hand-back a bound of its own. A mark that spends all of it against a slow Redis leaves
// the release running on a context that is already over, and the lease goes on naming an
// instance that is running nothing until a later tick or the TTL takes it away -- the
// opposite of what the mark is for.
func TestAMarkLeavesTheReleaseBehindItATurn(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	keys := client.Keys()

	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })

	const sid = "9c2b7d1e-0000-4000-8000-0000000000d4"
	if _, err := manager.Adopt(ctx, sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	manager.stopSession(sid)

	// A Redis that takes every lease script to the end of whatever context it is given,
	// which is what a hand-back looks like when the network is the thing that is broken.
	var attempts atomic.Int64
	rdb.AddHook(waiting{on: func(cmd redis.Cmder) bool {
		return strings.HasPrefix(cmd.Name(), "eval") && names(cmd, keys.HandBack(sid))
	}, count: &attempts})

	// The budget an adoption that could not open its session hands the whole hand-back.
	handing, cancel := context.WithTimeout(ctx, releaseTimeout)
	defer cancel()
	manager.abandon(handing, sid)

	if got := attempts.Load(); got != 2 {
		t.Fatalf("the hand-back reached Redis %d time(s), want 2: the mark and the release that has to follow it", got)
	}
}

// waiting holds a command until its own context is over, which is a Redis that answers
// nothing rather than one that refuses, and counts the ones it held.
type waiting struct {
	on    func(redis.Cmder) bool
	count *atomic.Int64
}

func (waiting) DialHook(next redis.DialHook) redis.DialHook { return next }

func (waiting) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h waiting) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if !h.on(cmd) {
			return next(ctx, cmd)
		}
		h.count.Add(1)
		<-ctx.Done()
		cmd.SetErr(ctx.Err())
		return ctx.Err()
	}
}
