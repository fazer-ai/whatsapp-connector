package session_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
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
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// recorder stands in for the transport: it keeps what was published and answered so a
// test asserts on frames rather than on Redis.
type recorder struct {
	mu      sync.Mutex
	events  []protocol.Event
	replies map[string]protocol.Reply
}

func newRecorder() *recorder {
	return &recorder{replies: make(map[string]protocol.Reply)}
}

func (r *recorder) Publish(_ context.Context, event *protocol.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, *event)
	return nil
}

func (r *recorder) Reply(_ context.Context, replyTo string, reply protocol.Reply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replies[replyTo] = reply
	return nil
}

func (r *recorder) Read(context.Context, []string) ([]transport.Delivery, error)  { return nil, nil }
func (r *recorder) Claim(context.Context, []string) ([]transport.Delivery, error) { return nil, nil }

func (r *recorder) published() []protocol.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]protocol.Event(nil), r.events...)
}

func (r *recorder) reply(replyTo string) (protocol.Reply, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply, ok := r.replies[replyTo]
	return reply, ok
}

type harness struct {
	leases   *cluster.Leases
	engine   *fake.Engine
	recorder *recorder
	manager  *session.Manager
}

func newHarness(t *testing.T) harness {
	t.Helper()
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	leases := cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{})
	fakeEngine := fake.New()
	rec := newRecorder()

	// Minted from the pump goroutine while the test reads what it published, so the
	// counter is atomic like every other cross-goroutine value here.
	var ids atomic.Int64
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fakeEngine, Leases: leases, Publisher: rec, Replier: rec,
		NewID:  func() string { return "evt-" + strconv.FormatInt(ids.Add(1), 10) },
		Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })
	return harness{leases: leases, engine: fakeEngine, recorder: rec, manager: manager}
}

func delivery(cmd *protocol.Command, acked *atomic.Bool) *transport.Delivery {
	return &transport.Delivery{Command: *cmd, Ack: func(context.Context) error { acked.Store(true); return nil }}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Every event of a session carries the epoch it was published under and a seq that
// only goes up. The client drops anything older, so a repeat or a gap here is a
// message that never reaches a conversation.
func TestPublishedEventsAreStampedAndOrdered(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()

	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, ok := h.engine.Session("s1")
	if !ok {
		t.Fatal("the engine has no session after Adopt")
	}

	for i := range 3 {
		engineSession.Emit(protocol.EventChatPresence, map[string]any{"state": "composing", "n": i})
	}
	waitFor(t, "three published events", func() bool { return len(h.recorder.published()) == 3 })

	for i, event := range h.recorder.published() {
		if event.SID != "s1" {
			t.Errorf("event %d has sid %q, want s1", i, event.SID)
		}
		if event.Epoch != 1 {
			t.Errorf("event %d has epoch %d, want 1", i, event.Epoch)
		}
		if event.Seq != uint64(i+1) {
			t.Errorf("event %d has seq %d, want %d", i, event.Seq, i+1)
		}
		if event.Inst != "inst-a" {
			t.Errorf("event %d has inst %q, want inst-a", i, event.Inst)
		}
		if event.V != protocol.Version {
			t.Errorf("event %d has v %d, want %d", i, event.V, protocol.Version)
		}
	}
}

func TestRPCCommandIsAnsweredAndAcknowledged(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()

	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	var acked atomic.Bool
	h.manager.Dispatch(ctx, delivery(&protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandSessionStatus, SID: "s1",
		ReplyTo: "c1", Payload: json.RawMessage(`{}`),
	}, &acked))

	waitFor(t, "the reply", func() bool { _, ok := h.recorder.reply("c1"); return ok })
	reply, _ := h.recorder.reply("c1")
	if !reply.OK {
		t.Fatalf("reply = %+v, want ok", reply)
	}
	waitFor(t, "the acknowledgement", acked.Load)
}

// An unknown command has to come back as `unsupported`, not as raw text: the client
// branches on the code.
func TestUnsupportedCommandIsRefusedWithItsCode(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()

	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	var acked atomic.Bool
	h.manager.Dispatch(ctx, delivery(&protocol.Command{
		V: protocol.Version, ID: "c2", Type: protocol.CommandGroupList, SID: "s1",
		ReplyTo: "c2", Payload: json.RawMessage(`{}`),
	}, &acked))

	waitFor(t, "the reply", func() bool { _, ok := h.recorder.reply("c2"); return ok })
	reply, _ := h.recorder.reply("c2")
	if reply.OK || reply.Error == nil || reply.Error.Code != protocol.ErrorUnsupported {
		t.Fatalf("reply = %+v, want an unsupported error", reply)
	}
}

// The caller stopped waiting, so running the command is a side effect nobody will read
// the outcome of.
func TestExpiredCommandIsRefusedWithoutRunning(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()

	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, _ := h.engine.Session("s1")

	var acked atomic.Bool
	h.manager.Dispatch(ctx, delivery(&protocol.Command{
		V: protocol.Version, ID: "c3", Type: protocol.CommandSessionStatus, SID: "s1",
		ReplyTo: "c3", Deadline: time.Now().Add(-time.Minute).UnixMilli(), Payload: json.RawMessage(`{}`),
	}, &acked))

	waitFor(t, "the reply", func() bool { _, ok := h.recorder.reply("c3"); return ok })
	reply, _ := h.recorder.reply("c3")
	if reply.OK || reply.Error == nil || reply.Error.Code != protocol.ErrorExpired {
		t.Fatalf("reply = %+v, want an expired error", reply)
	}
	if got := len(engineSession.Commands()); got != 0 {
		t.Fatalf("the engine ran %d commands, want none", got)
	}
}

// A fire-and-forget command has nobody blocked on it, so a failure that published
// nothing would be a command that silently did nothing.
func TestFireAndForgetFailurePublishesCommandFailed(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()

	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	var acked atomic.Bool
	h.manager.Dispatch(ctx, delivery(&protocol.Command{
		V: protocol.Version, ID: "c4", Type: protocol.CommandGroupList, SID: "s1",
		Payload: json.RawMessage(`{}`),
	}, &acked))

	waitFor(t, "a command.failed event", func() bool {
		for _, event := range h.recorder.published() {
			if event.Type == protocol.EventCommandFailed {
				return true
			}
		}
		return false
	})
}

// A command for a session this instance does not own must be left pending: the owner
// reads the same stream, and acknowledging it here is how it never gets there.
func TestCommandForAnotherInstanceIsLeftPending(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	var acked atomic.Bool

	h.manager.Dispatch(context.Background(), delivery(&protocol.Command{
		V: protocol.Version, ID: "c5", Type: protocol.CommandSessionStatus, SID: "not-ours",
		ReplyTo: "c5", Payload: json.RawMessage(`{}`),
	}, &acked))

	if acked.Load() {
		t.Fatal("a command for a session this instance does not own was acknowledged")
	}
	if _, answered := h.recorder.reply("c5"); answered {
		t.Fatal("a command for a session this instance does not own was answered")
	}
}

// The scheduling algorithm of the whole fleet: everybody hears the wake, the one that
// wins the lease runs it.
func TestWakeAdoptsTheSession(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	var acked atomic.Bool

	h.manager.Dispatch(context.Background(), delivery(&protocol.Command{
		V: protocol.Version, ID: "c6", Type: protocol.CommandSessionWake, SID: "s2",
		Payload: json.RawMessage(`{"desired":"connected"}`),
	}, &acked))

	if !acked.Load() {
		t.Fatal("the wake was not acknowledged")
	}
	if got := h.manager.SIDs(); len(got) != 1 || got[0] != "s2" {
		t.Fatalf("running sessions = %v, want [s2]", got)
	}
}

func TestAdoptRefusesASessionAnotherInstanceHolds(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	build := func(instance string) (*cluster.Leases, *session.Manager) {
		rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		leases := cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), instance, cluster.Options{})
		manager := session.NewManager(&session.ManagerConfig{
			Instance: instance, Engine: fake.New(), Leases: leases,
			Publisher: newRecorder(), Replier: newRecorder(),
			NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
		})
		t.Cleanup(func() { manager.StopAll(context.Background()) })
		return leases, manager
	}

	_, a := build("inst-a")
	_, b := build("inst-b")
	ctx := context.Background()

	if _, err := a.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("a.Adopt: %v", err)
	}
	if _, err := b.Adopt(ctx, "s1"); err == nil {
		t.Fatal("b adopted a session a already owns")
	}
	if got := b.Count(); got != 0 {
		t.Fatalf("b runs %d sessions, want none", got)
	}
}

// Losing the lease has to close the socket, not just stop the publishing: two live
// sockets on one WhatsApp account is what the whole lease exists to prevent.
func TestRenewAllDropsASessionWhoseLeaseMoved(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	fakeEngine := fake.New()
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fakeEngine, Leases: leases,
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()

	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	// The lease expires and another instance takes it, which is what a killed
	// instance's sessions look like from here.
	server.FastForward(cluster.DefaultTTL + time.Second)
	other := cluster.NewLeases(client, "inst-b", cluster.Options{})
	if _, err := other.Acquire(ctx, "s1"); err != nil {
		t.Fatalf("inst-b Acquire: %v", err)
	}

	manager.RenewAll(ctx)

	if got := manager.Count(); got != 0 {
		t.Fatalf("the manager still runs %d sessions after losing the lease", got)
	}
	engineSession, _ := fakeEngine.Session("s1")
	if engineSession.Connected() {
		t.Fatal("the engine session is still connected after the lease moved")
	}
}

// A Redis that stops answering is not proof the lease moved, so a renewal failure alone
// is worth another tick. What is not worth another tick is a lease that has run out: a
// peer is then free to take the session, and a socket still open here would be the
// second one on the account. WhatsApp answers that by replacing the stream, and both
// owners write the same device meanwhile.
func TestRenewAllDropsASessionWhoseLeaseWentStaleWhileRedisWasUnreachable(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	clock := &steppingClock{now: time.Now()}
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{Clock: clock})
	fakeEngine := fake.New()
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fakeEngine, Leases: leases,
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()

	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	// The lease runs out and Redis is unreachable, so the renewal fails with something
	// other than "you are not the owner": nobody can say who owns it now.
	clock.step(cluster.DefaultTTL + time.Second)
	server.Close()

	manager.RenewAll(ctx)

	if got := manager.Count(); got != 0 {
		t.Fatalf("the manager still runs %d sessions on a lease that ran out", got)
	}
	engineSession, _ := fakeEngine.Session("s1")
	if engineSession.Connected() {
		t.Fatal("the engine session is still connected on a lease that ran out")
	}
}

// The other half of the same rule: a blip that does not outlast the lease costs a tick,
// not a session.
func TestRenewAllKeepsASessionWhoseLeaseIsStillFresh(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	fakeEngine := fake.New()
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fakeEngine, Leases: leases,
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()

	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	server.Close()

	manager.RenewAll(ctx)

	if got := manager.Count(); got != 1 {
		t.Fatalf("the manager dropped a session on one failed round trip (running %d)", got)
	}
}

// steppingClock is the lease clock under a test's control.
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

// Every instance reads the control stream through one consumer group, so acknowledging
// a wake nobody could act on retires it: the session then stays unowned until a client
// happens to send another. Whatever stopped the adoption may well be over by the time
// the wake is reclaimed.
func TestAWakeThatCouldNotBeAdoptedStaysPending(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: refusingEngine{}, Leases: cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})

	acked := false
	delivery := &transport.Delivery{
		Command: protocol.Command{V: protocol.Version, ID: "c1", Type: protocol.CommandSessionWake, SID: "s1"},
		Ack:     func(context.Context) error { acked = true; return nil },
	}
	manager.Dispatch(context.Background(), delivery)

	if acked {
		t.Fatal("a wake nobody could act on was acknowledged, so nothing will retry it")
	}
	if got := manager.Count(); got != 0 {
		t.Fatalf("the manager runs %d sessions after a refused adoption", got)
	}
}

// refusingEngine cannot open anything, which is what a database that is away looks like
// from the manager.
type refusingEngine struct{}

func (refusingEngine) Open(context.Context, string) (engine.Session, error) {
	return nil, errors.New("the store is unreachable")
}
func (refusingEngine) Close() error { return nil }

// stalledEngine is a store that has stopped answering: Open blocks until whoever asked
// gives up. It is what a database in the middle of a failover looks like from here.
type stalledEngine struct {
	entered chan struct{}
	once    sync.Once
}

func (e *stalledEngine) Open(ctx context.Context, _ string) (engine.Session, error) {
	e.once.Do(func() { close(e.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (e *stalledEngine) Close() error { return nil }

// Commands are dispatched on the same goroutine that renews every lease this instance
// holds. An adoption that waits on a database therefore stops all of them from being
// renewed: their leases expire, peers acquire the accounts, and the sockets this
// instance still holds open go on talking to WhatsApp. One session failing to start is
// the cheaper outcome by a wide margin.
func TestAdoptGivesUpOnAStoreThatStoppedAnswering(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	leases := cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{})
	stalled := &stalledEngine{entered: make(chan struct{})}
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: stalled, Leases: leases,
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	started := time.Now()
	_, err := manager.Adopt(context.Background(), "s1")
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("Adopt returned successfully from a store that never answered")
	}
	if elapsed > 2*session.AdoptTimeout {
		t.Fatalf("Adopt waited %s on an unbounded context, want about %s", elapsed, session.AdoptTimeout)
	}
	select {
	case <-stalled.entered:
	default:
		t.Fatal("the engine was never asked, so the timeout proved nothing")
	}

	// The lease goes back, or every other instance is kept off an account nobody runs.
	if manager.Count() != 0 {
		t.Fatalf("the manager runs %d sessions after a failed adoption", manager.Count())
	}
	other := cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-b", cluster.Options{})
	if _, err := other.Acquire(context.Background(), "s1"); err != nil {
		t.Fatalf("a peer could not take the released lease: %v", err)
	}
}

// The bound belongs to the I/O and not to the session. A session built on the bounded
// context would be torn down a few seconds after it was adopted, which is a connector
// that pairs an account and drops it.
func TestAnAdoptedSessionOutlivesTheBoundOnItsAdoption(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	leases := cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{})
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(), Leases: leases,
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	// The context that adopts is the one the caller bounds to keep a slow store off the
	// goroutine that renews leases, so it can be over seconds later or cancelled outright.
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	cancel()

	time.Sleep(session.AdoptTimeout + 500*time.Millisecond)
	if got := manager.Count(); got != 1 {
		t.Fatalf("the manager runs %d sessions after the context that adopted them ended, want 1", got)
	}

	// Still running, not merely still counted.
	acked := atomic.Bool{}
	manager.Dispatch(context.Background(), status("c-after", "s1", &acked))
	waitUntil(t, "the session to answer a command after the adoption context ended", func() bool {
		return manager.Count() == 1 && !acked.Load()
	})
}

// The wake that reaches a peer after an instance died is only useful once the lease
// that instance left behind has run out. Before that the peer is told the session is
// already owned, which is true of a lease and false of anybody running it, and the
// whole point of the reclaim delay outlasting the lease is that this moment has already
// passed by the time a wake can be claimed.
func TestAPeerTakesOverOnceTheDeadOwnersLeaseRunsOut(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	const ttl = 30 * time.Second
	build := func(instance string) *session.Manager {
		rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		manager := session.NewManager(&session.ManagerConfig{
			Instance: instance, Engine: fake.New(),
			Leases:    cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), instance, cluster.Options{TTL: ttl}),
			Publisher: newRecorder(), Replier: newRecorder(),
			NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
		})
		t.Cleanup(func() { manager.StopAll(context.Background()) })
		return manager
	}

	dead, peer := build("inst-dead"), build("inst-peer")
	ctx := context.Background()

	if _, err := dead.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("the first instance could not adopt: %v", err)
	}
	// And now it is gone, without releasing anything. Its lease is still in Redis.
	if _, err := peer.Adopt(ctx, "s1"); !errors.Is(err, cluster.ErrNotOwner) {
		t.Fatalf("the peer adopted a leased session, err=%v", err)
	}

	server.FastForward(ttl + time.Second)

	if _, err := peer.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("the peer could not take over an expired lease: %v", err)
	}
	if got := peer.Count(); got != 1 {
		t.Fatalf("the peer runs %d sessions, want 1", got)
	}
}

// heldEngine is a session whose one command never finishes on its own, which is what
// lets a test put a second command in the queue behind it and keep it there.
type heldEngine struct {
	hold    chan struct{}
	events  chan engine.Emission
	entered atomic.Bool
}

func newHeldEngine() *heldEngine {
	return &heldEngine{hold: make(chan struct{}), events: make(chan engine.Emission)}
}

func (e *heldEngine) Open(context.Context, string) (engine.Session, error) { return e, nil }
func (e *heldEngine) Events() <-chan engine.Emission                       { return e.events }
func (e *heldEngine) Connect(context.Context, engine.ConnectRequest) error { return nil }
func (e *heldEngine) Disconnect(context.Context) error                     { return nil }
func (e *heldEngine) Logout(context.Context) error                         { return nil }

func (e *heldEngine) Execute(ctx context.Context, _ *protocol.Command) (json.RawMessage, error) {
	e.entered.Store(true)
	select {
	case <-e.hold:
	case <-ctx.Done():
	}
	return json.RawMessage(`{}`), nil
}

func (e *heldEngine) Close() error {
	close(e.events)
	return nil
}

// A command sitting in a session's queue has been read from Redis and not acknowledged,
// so the transport counts it as work this process is still doing and will not reclaim
// it. Dropping it when the session stops means it runs nowhere at all until another
// instance claims it or this one restarts, which is the failure the reclaim exists to
// prevent, reintroduced one layer up.
func TestStoppingASessionLetsGoOfWhatWasStillQueued(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	held := newHeldEngine()
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: held,
		Leases:    cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{}),
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()
	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	var releasedFirst, releasedSecond atomic.Bool
	running := status("c1", "s1", &releasedFirst)
	queued := status("c2", "s1", &releasedSecond)

	manager.Dispatch(ctx, running)
	// The executor has to have taken the first one, or the second is not behind it.
	waitUntil(t, "the first command to reach the engine", func() bool {
		return manager.Count() == 1 && held.taken()
	})
	manager.Dispatch(ctx, queued)

	manager.StopAll(ctx)

	if !releasedSecond.Load() {
		t.Fatal("a command left in the queue was dropped, so nothing will ever reclaim it")
	}
}

// taken reports whether the engine has been asked to carry a command out.
func (e *heldEngine) taken() bool { return e.entered.Load() }

func status(id, sid string, released *atomic.Bool) *transport.Delivery {
	return &transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: id, Type: protocol.CommandSessionStatus,
			SID: sid, TS: 1787000000000, Payload: json.RawMessage(`{}`),
		},
		Ack:     func(context.Context) error { return nil },
		Release: func() { released.Store(true) },
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
