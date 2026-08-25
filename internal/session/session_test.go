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
