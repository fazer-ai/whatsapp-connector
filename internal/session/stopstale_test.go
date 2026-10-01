package session_test

import (
	"context"
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
	"github.com/fazer-ai/whatsapp-connector/internal/session"
	"github.com/fazer-ai/whatsapp-connector/internal/silentnet"
	"github.com/fazer-ai/whatsapp-connector/internal/testwait"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// staleSetup is one adopted session on inst-a, whose Redis can be muted and whose lease
// clock the test steps.
// lostWatch records which leases the manager reports lost, which is what
// wac_leases_lost_total counts.
type lostWatch struct {
	mu   sync.Mutex
	lost []string
}

func (w *lostWatch) CommandDone(protocol.CommandType, string, time.Duration) {}
func (w *lostWatch) StateDecided(time.Duration)                              {}
func (w *lostWatch) LeaseLost(sid string) {
	w.mu.Lock()
	w.lost = append(w.lost, sid)
	w.mu.Unlock()
}

func (w *lostWatch) seen() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.lost...)
}

// disconnected waits for a fake session to lose its socket: StopStale takes each stale
// session down on a goroutine of its own, so the close lands just after it returns.
func disconnected(t *testing.T, engineSession *fake.Session, what string) {
	t.Helper()
	deadline := time.Now().Add(testwait.Budget)
	for engineSession.Connected() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(testwait.Poll)
	}
}

// handedBack runs ticks until the lease key is gone. The hand-back waits for the stop it
// follows, which runs on its own goroutine, so the first tick may find it not done yet.
func handedBack(t *testing.T, s *staleSetup, key string) {
	t.Helper()
	deadline := time.Now().Add(testwait.Budget)
	for s.server.Exists(key) {
		if time.Now().After(deadline) {
			t.Fatalf("%s was never handed back", key)
		}
		s.manager.RenewAll(context.Background(), s.manager.HandBackBy())
		time.Sleep(testwait.Poll)
	}
}

type staleSetup struct {
	server  *miniredis.Miniredis
	mute    *silentnet.Relay
	clock   *steppingClock
	leases  *cluster.Leases
	engine  *fake.Engine
	manager *session.Manager
	watch   *lostWatch
}

func newStaleSetup(t *testing.T) *staleSetup {
	t.Helper()
	server := miniredis.RunT(t)
	mute := silentnet.New(t, server.Addr())
	rdb := redis.NewClient(&redis.Options{Addr: mute.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	clock := &steppingClock{now: time.Now()}
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{Clock: clock})
	fakeEngine := fake.New()
	watch := &lostWatch{}
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fakeEngine, Leases: leases,
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(), Watch: watch,
	})
	if _, err := manager.Adopt(context.Background(), "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	// Connected, so that "the socket is down" below is a change and not the state a
	// session nobody asked to connect is in anyway.
	engineSession, _ := fakeEngine.Session("s1")
	if err := engineSession.Connect(context.Background(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if !engineSession.Connected() {
		t.Fatal("the fake session did not connect, so nothing here could see it disconnect")
	}
	return &staleSetup{server: server, mute: mute, clock: clock, leases: leases, engine: fakeEngine, manager: manager, watch: watch}
}

// A lease that runs out while Redis says nothing takes its socket down at once, with no
// answer from Redis. Before #353 only a renewal coming back could do that, and with Redis
// silent none came back until Redis did: the store refused writes from the moment the
// lease ran out, the socket stayed open, and every message in between was decrypted into
// nothing and counted delivered.
func TestStopStaleTakesASessionDownWhenItsLeaseRunsOutWhileRedisIsSilent(t *testing.T) {
	t.Parallel()
	s := newStaleSetup(t)

	s.mute.Mute()
	s.clock.step(cluster.DefaultTTL - cluster.DefaultRenewMargin)

	stopped := make(chan struct{})
	go func() {
		s.manager.StopStale()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("StopStale waited on a silent Redis; it must not ask Redis anything")
	}

	if got := s.manager.Count(); got != 0 {
		t.Fatalf("the manager still runs %d sessions on a lease that ran out", got)
	}
	engineSession, _ := s.engine.Session("s1")
	disconnected(t, engineSession, "the engine session is still connected on a lease that ran out")
	// Counted, like every other lease this instance gives up: an operator reading
	// wac_leases_lost_total during a Redis outage would otherwise see nothing lost.
	if got := s.watch.seen(); len(got) != 1 || got[0] != "s1" {
		t.Fatalf("leases reported lost = %v, want [s1]", got)
	}
}

// The other side: a lease with life left keeps its session, and the answer says when to
// look again, which is when that life runs out.
func TestStopStaleKeepsAFreshLeaseAndSaysWhenItRunsOut(t *testing.T) {
	t.Parallel()
	s := newStaleSetup(t)

	s.clock.step(10 * time.Second)
	next := s.manager.StopStale()

	if got := s.manager.Count(); got != 1 {
		t.Fatalf("StopStale dropped a session whose lease is fresh (running %d)", got)
	}
	want := cluster.DefaultTTL - cluster.DefaultRenewMargin - 10*time.Second
	if next != want {
		t.Fatalf("StopStale said to look again in %s, want %s: the moment the lease runs out", next, want)
	}
}

// A session stopped this way still leaves a key naming this instance in Redis, and the
// tick is what gives it back once Redis answers: without that no peer could take the
// account until the key expired on its own.
func TestALeaseStoppedOnItsOwnClockIsHandedBackByTheNextTick(t *testing.T) {
	t.Parallel()
	s := newStaleSetup(t)

	s.clock.step(cluster.DefaultTTL - cluster.DefaultRenewMargin)
	s.manager.StopStale()
	if !s.server.Exists("wa:lease:s1") {
		t.Fatal("the key is already gone, so this test would not see a hand-back")
	}

	handedBack(t, s, "wa:lease:s1")
}

// Two instances: inst-a's socket has to be down before inst-b can take the account, and
// inst-a's Redis being silent cannot change that. inst-a stops on its own clock, which
// runs out a margin before the key does; inst-b can only win once the key is gone.
func TestAPeerNeverTakesAnAccountWhileTheSilentOwnerStillHoldsTheSocket(t *testing.T) {
	t.Parallel()
	s := newStaleSetup(t)

	direct := redis.NewClient(&redis.Options{Addr: s.server.Addr()})
	t.Cleanup(func() { _ = direct.Close() })
	peer := cluster.NewLeases(redisx.Wrap(direct, "wa:", 8), "inst-b", cluster.Options{})

	s.mute.Mute()
	ctx := context.Background()

	// inst-a's clock reaches the end of what it may act on; the key still has its margin.
	s.clock.step(cluster.DefaultTTL - cluster.DefaultRenewMargin)
	if _, err := peer.Acquire(ctx, "s1"); err == nil {
		t.Fatal("inst-b took the account while the key still named inst-a")
	}
	s.manager.StopStale()
	engineSession, _ := s.engine.Session("s1")
	disconnected(t, engineSession, "inst-a still holds the socket at the end of its lease")

	// Now the key runs out in Redis, and the peer is free to take it.
	s.server.FastForward(cluster.DefaultTTL)
	if _, err := peer.Acquire(ctx, "s1"); err != nil {
		t.Fatalf("inst-b could not take an account nobody holds: %v", err)
	}
	if engineSession.Connected() {
		t.Fatal("two sockets on one account: inst-a reconnected behind the peer")
	}
}

// hookedClock runs a hook, once, the first time the lease clock is read after it is
// armed. StopStale reads the clock between taking its list of sessions and dropping one,
// so the hook lands in that window.
type hookedClock struct {
	steppingClock
	armed atomic.Bool
	hook  func()
}

func (c *hookedClock) Now() time.Time {
	if c.armed.CompareAndSwap(true, false) {
		c.hook()
	}
	return c.steppingClock.Now()
}

// A session that something else stops between StopStale listing it and dropping it is
// not StopStale's to stop: the drop finds it gone and moves on. Without that check the
// stop would land on nothing -- or, after an adoption, on the session that replaced it.
func TestStopStaleLeavesASessionSomethingElseStoppedFirst(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	clock := &hookedClock{steppingClock: steppingClock{now: time.Now()}}
	leases := cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{Clock: clock})
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(), Leases: leases,
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	if _, err := manager.Adopt(context.Background(), "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	clock.step(cluster.DefaultTTL)
	clock.hook = func() { manager.StopAll(context.Background()) }
	clock.armed.Store(true)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("StopStale acted on a session it no longer had: %v", r)
		}
	}()
	manager.StopStale()

	if clock.armed.Load() {
		t.Fatal("the hook never ran, so the window this test is about was not reached")
	}
	if got := manager.Count(); got != 0 {
		t.Fatalf("running %d sessions, want 0", got)
	}
}

// One stale session that is slow to drain does not keep the others open. Stop closes the
// socket and then waits for the session's goroutines, and a command being answered is one
// of the things it waits for; stopped one after another, the first session's answer would
// hold every other stale socket on the instance past its lease.
//
// And the slow one's lease is not handed back while it is still coming down: the hand-back
// is what lets a peer take the account, and the socket being closed is not the session
// being done.
func TestOneSessionSlowToDrainDoesNotKeepTheOtherStaleSessionsOpen(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	clock := &steppingClock{now: time.Now()}
	replies := &gatedReplier{entered: make(chan struct{}), gate: make(chan struct{})}
	fakeEngine := fake.New()
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fakeEngine,
		Leases:    cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{Clock: clock}),
		Publisher: newRecorder(), Replier: replies,
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	answering, stopAnswering := context.WithCancel(context.Background())
	answered := manager.Answer(answering)
	var release sync.Once
	open := func() { release.Do(func() { close(replies.gate) }) }
	// A failure below must not leave the answer gated, or the cleanup waits on it forever.
	t.Cleanup(func() { open(); stopAnswering(); <-answered })

	ctx := context.Background()
	sockets := map[string]*fake.Session{}
	for _, sid := range []string{"s1", "s2"} {
		if _, err := manager.Adopt(ctx, sid); err != nil {
			t.Fatalf("Adopt(%s): %v", sid, err)
		}
		socket, _ := fakeEngine.Session(sid)
		if err := socket.Connect(ctx, engine.ConnectRequest{Pairing: "resume"}); err != nil {
			t.Fatalf("Connect(%s): %v", sid, err)
		}
		sockets[sid] = socket
	}

	// s1 is in the middle of answering a command, and stays there until the test says so.
	manager.Dispatch(&transport.Delivery{
		Command: protocol.Command{V: protocol.Version, ID: "c1", Type: protocol.CommandSessionStatus, SID: "s1", ReplyTo: "wa:reply:1"},
		Ack:     func(context.Context) error { return nil },
		Release: func() {},
	})
	<-replies.entered

	clock.step(cluster.DefaultTTL)
	returned := make(chan struct{})
	go func() {
		manager.StopStale()
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("StopStale waited on a session that was still answering a command")
	}
	disconnected(t, sockets["s2"], "s2 is still connected while s1 drains: one slow session held the others open")
	disconnected(t, sockets["s1"], "s1 kept its socket while it drained: closing it does not have to wait for the answer")

	// s1 is still answering, so its stop has not finished: its lease stays where it is.
	manager.RenewAll(ctx, manager.HandBackBy())
	if !server.Exists("wa:lease:s1") {
		t.Fatal("s1's lease was handed back while s1 was still coming down")
	}

	open()
	deadline := time.Now().Add(testwait.Budget)
	for server.Exists("wa:lease:s1") || server.Exists("wa:lease:s2") {
		if time.Now().After(deadline) {
			t.Fatal("the leases were not handed back once the sessions had stopped")
		}
		manager.RenewAll(ctx, manager.HandBackBy())
		time.Sleep(testwait.Poll)
	}
}
