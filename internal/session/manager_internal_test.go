package session

import (
	"context"
	"errors"
	"net"
	"slices"
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
		return losing.Load() && names(cmd, keys.Cooldown(sid))
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

func (dropped) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
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

// names reports whether a command carries a key, which is how a test picks one of the
// lease scripts out: only handing back reaches for the cooldown.
func names(cmd redis.Cmder, key string) bool {
	for _, arg := range cmd.Args() {
		if text, ok := arg.(string); ok && text == key {
			return true
		}
	}
	return false
}
