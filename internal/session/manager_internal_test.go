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
// back up on an instance whose next tick stops it and hands it away regardless. The
// adoption hands it back instead, and what serves the retry is the session built after.
func TestAnAdoptionDoesNotHandBackASessionOnItsWayOut(t *testing.T) {
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
	waitFor(t, func() bool { return first.Retired() }, "the session was never finished with")

	// The retry, before any heartbeat has swept.
	second, err := manager.Adopt(ctx, sid)
	if err != nil {
		t.Fatalf("the retry could not be adopted: %v", err)
	}
	if second == first {
		t.Fatal("the retry was answered with the session that is on its way out")
	}
	if second.Retired() {
		t.Fatal("the session built for the retry came back already finished with")
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

// The sweep runs on the heartbeat and adoptions run on the answer goroutine, so between
// finding a retired session and handing it back a wake can have replaced it -- Adopt
// does exactly that. Released by sid alone, the sweep then stops a session that is
// running and deletes the lease it is running under, and the cooldown that release arms
// keeps this instance from taking the account back.
func TestASweepDoesNotHandBackASessionAdoptedAgainSinceItLooked(t *testing.T) {
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

	const sid = "9c2b7d1e-0000-4000-8000-0000000000a6"
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

	// The interleaving, written out: the sweep has looked and has not released yet, and
	// the wake that replaces the session lands in between.
	retired := map[string]*Session{sid: first}
	second, err := manager.Adopt(ctx, sid)
	if err != nil {
		t.Fatalf("the retry could not be adopted: %v", err)
	}
	for swept, session := range retired {
		manager.releaseThis(ctx, swept, session)
	}

	if second.Retired() {
		t.Fatal("the session built for the retry came back already finished with")
	}
	if _, held := leases.Owned(sid); !held {
		t.Fatal("the sweep deleted the lease of the session that replaced the one it found")
	}
	// Forgotten and stopped are the same step, so the map answers for both.
	if !slices.Contains(manager.SIDs(), sid) {
		t.Fatal("the sweep stopped the session that replaced the one it found")
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

	// On this goroutine, so the answer is an ordering rather than a wait: the adoption
	// returns after the hand-back finished, or it ran through it.
	_, err = manager.Adopt(ctx, sid)
	select {
	case <-handed:
	default:
		t.Fatal("an adoption ran while a hand-back for the same account was still under way")
	}

	// The release never reached Redis, so the key still names this instance and the
	// adoption is told as much. `handingBack` is what keeps the wake behind it pending
	// rather than acknowledged as somebody else's.
	if !errors.Is(err, cluster.ErrNotOwner) {
		t.Fatalf("the adoption after a hand-back that never landed answered %v, want %v", err, cluster.ErrNotOwner)
	}
	if !manager.handingBack(sid) {
		t.Fatal("a hand-back that did not reach Redis was forgotten, so the wake that would restart the account is acknowledged as somebody else's")
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
	manager.holdHanding(sid)
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

	// The retry, while the renewal is between asking and hearing.
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
		fails:   errors.New("redis is away"),
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

	// The word never lands, so the session is not finished with after all.
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
	waitFor(t, func() bool { return session.retired.Load() }, "the session was never finished with")

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
	manager.holdHanding(other)
	defer manager.dropHanding(other)

	manager.SweepRetired(ctx, manager.HandBackBy())
	if _, held := leases.Owned(retiring); held {
		t.Fatal("an adoption of another account kept the sweep from handing a retired session back")
	}
}
