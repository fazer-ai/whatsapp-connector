package session_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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
	"github.com/fazer-ai/whatsapp-connector/internal/session"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// recorder stands in for the transport: it keeps what was published and answered so a
// test asserts on frames rather than on Redis.
type recorder struct {
	mu      sync.Mutex
	events  []protocol.Event
	replies map[string]protocol.Reply
	// refuse is what Publish answers with, for the tests that need a stream nothing
	// reaches. A Redis that is down is not a case a fake can be talked into otherwise.
	refuse error
	// gate holds a publish open, for the tests that need the pump busy.
	gate chan struct{}
}

func newRecorder() *recorder {
	return &recorder{replies: make(map[string]protocol.Reply)}
}

func (r *recorder) Publish(_ context.Context, event *protocol.Event) error {
	r.mu.Lock()
	gate := r.gate
	r.gate = nil
	r.mu.Unlock()
	if gate != nil {
		<-gate
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refuse != nil {
		return r.refuse
	}
	r.events = append(r.events, *event)
	return nil
}

// hold makes the next publish block until the returned func is called, which is the
// only way to catch the pump mid-publish with something else already queued behind it.
func (r *recorder) hold() func() {
	gate := make(chan struct{})
	r.mu.Lock()
	r.gate = gate
	r.mu.Unlock()
	return func() { close(gate) }
}

func (r *recorder) failWith(err error) {
	r.mu.Lock()
	r.refuse = err
	r.mu.Unlock()
}

func (r *recorder) Reply(ctx context.Context, replyTo string, reply protocol.Reply) error {
	// Refused on a dead context, the way a Redis client is. A recorder that took the
	// reply anyway would let every test pass that meant to prove an answer survives the
	// session ending, because the answer would be recorded from the dying context too.
	if err := ctx.Err(); err != nil {
		return err
	}
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
	ledger   *ledger
	manager  *session.Manager
}

// ledger is the harness's idempotency store, with a switch for the tests that need one
// nothing can read. A Redis that is up and a Redis that answers are different things,
// and only the second one is an answer about whether a command already ran.
type ledger struct {
	inner *redisx.Idempotency
	mu    sync.Mutex
	err   error

	// writes counts every attempt at a record, and refuseWrites is how many of them are
	// turned away before the store is let through. A Redis that refuses one call and
	// answers the next is the shape of failure the write retry is there for.
	writes       int
	refuseWrites int
}

// refuseWritesFor turns the next n attempts at a record away, whatever the read side is
// doing.
func (l *ledger) refuseWritesFor(n int) {
	l.mu.Lock()
	l.refuseWrites = n
	l.mu.Unlock()
}

func (l *ledger) attempts() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.writes
}

func (l *ledger) refuse(err error) {
	l.mu.Lock()
	l.err = err
	l.mu.Unlock()
}

func (l *ledger) refusal() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

func (l *ledger) Recall(ctx context.Context, sid, key string) (json.RawMessage, bool, error) {
	if err := l.refusal(); err != nil {
		return nil, false, err
	}
	return l.inner.Recall(ctx, sid, key)
}

func (l *ledger) Remember(ctx context.Context, sid, key string, result json.RawMessage) error {
	l.mu.Lock()
	l.writes++
	refusing := l.refuseWrites > 0
	if refusing {
		l.refuseWrites--
	}
	err := l.err
	l.mu.Unlock()

	switch {
	case err != nil:
		return err
	case refusing:
		return errors.New("redis refused the write")
	}
	return l.inner.Remember(ctx, sid, key, result)
}

func newHarness(t *testing.T) harness {
	t.Helper()
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	client := redisx.Wrap(rdb, "wa:", 8)
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	fakeEngine := fake.New()
	rec := newRecorder()

	// Minted from the pump goroutine while the test reads what it published, so the
	// counter is atomic like every other cross-goroutine value here.
	book := &ledger{inner: redisx.NewIdempotency(client, 0)}
	var ids atomic.Int64
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fakeEngine, Leases: leases, Publisher: rec, Replier: rec,
		Ledger: book,
		NewID:  func() string { return "evt-" + strconv.FormatInt(ids.Add(1), 10) },
		Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })
	return harness{leases: leases, engine: fakeEngine, recorder: rec, ledger: book, manager: manager}
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

// A batch is dispatched under a budget, so a command that finishes as that budget runs
// out would have its acknowledgement refused by the deadline rather than by Redis. That
// is not a retry: an entry whose ack did not land stays marked as being carried out here,
// on purpose, so a reclaim does not run it twice — and every later claim then skips it.
// In a fleet of one that is a command nothing retires until the process restarts.
func TestACommandThatWasCarriedOutIsRetiredEvenOutOfTime(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(), Leases: cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})

	// Spent by the time the command is done with, which is what a batch that ran to the
	// end of its budget hands the last command in it.
	spent, cancel := context.WithCancel(context.Background())
	cancel()

	var acked bool
	delivery := &transport.Delivery{
		Command: protocol.Command{V: protocol.Version, ID: "c1", Type: protocol.CommandAdminPing, SID: "s1"},
		Ack: func(ctx context.Context) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			acked = true
			return nil
		},
		Release: func() { t.Error("a command that was carried out was given back instead of retired") },
	}
	manager.Dispatch(spent, delivery)

	if !acked {
		t.Fatal("the acknowledgement was refused by the budget the command had already outlived")
	}
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

	acked, released, forfeited := false, false, false
	delivery := &transport.Delivery{
		Command: protocol.Command{V: protocol.Version, ID: "c1", Type: protocol.CommandSessionWake, SID: "s1"},
		Ack:     func(context.Context) error { acked = true; return nil },
		Release: func() { released = true },
		Forfeit: func() { forfeited = true },
	}
	manager.Dispatch(context.Background(), delivery)

	if acked {
		t.Fatal("a wake nobody could act on was acknowledged, so nothing will retry it")
	}
	if got := manager.Count(); got != 0 {
		t.Fatalf("the manager runs %d sessions after a refused adoption", got)
	}
	// Given back the way an instance that took its turn gives it back. Released instead,
	// it keeps the age that makes it the oldest entry pending, so this instance takes it
	// first on the next pass, and again — and every wake behind it, which is every other
	// session nobody is running, never gets a turn.
	if released || !forfeited {
		t.Fatalf("the wake was released=%v forfeited=%v, want forfeited", released, forfeited)
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

// And the one that was running is retired rather than left half-way. The session's
// context is exactly what has just been cancelled, so an acknowledgement made on it is
// refused for the cancellation — and an entry whose ack did not land stays marked as
// being carried out here, on purpose, so a reclaim does not run it twice. Every later
// claim then skips it, this same instance included if it adopts the session again, and
// the command is neither retired nor retried until the process restarts.
func TestACommandRunningWhenItsSessionStopsIsStillRetired(t *testing.T) {
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

	var acked atomic.Bool
	running := &transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "c1", Type: protocol.CommandSessionStatus,
			SID: "s1", TS: 1787000000000, Payload: json.RawMessage(`{}`),
		},
		Ack: func(ctx context.Context) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			acked.Store(true)
			return nil
		},
		Release: func() { t.Error("a command that ran was given back instead of retired") },
	}

	manager.Dispatch(ctx, running)
	waitUntil(t, "the command to reach the engine", func() bool {
		return manager.Count() == 1 && held.taken()
	})

	// Which is what releases the engine: heldEngine answers the cancellation, so the
	// command finishes on a session whose context has just gone.
	manager.StopAll(ctx)

	waitUntil(t, "the command that ran to be acknowledged", acked.Load)
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

// A drain claims with no minimum idle, on the grounds that this instance holds the
// lease. Once it does not, that reasoning is gone: claiming would take the current
// owner's commands out from under it, dispatch them as belonging to nobody, and release
// them again on every pass, so the account that actually owns them never drains.
func TestASessionThatMovedOnIsNoLongerWaitingToBeDrained(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{}),
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()
	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	// Handed to a peer, or shut down here; either way this instance stops running it.
	manager.StopAll(ctx)

	if waiting := manager.TakeNewlyAdopted(); len(waiting) != 0 {
		t.Fatalf("a session this instance no longer runs is still queued for a drain: %v", waiting)
	}
	manager.ReturnAdopted([]string{"s1"})
	if waiting := manager.TakeNewlyAdopted(); len(waiting) != 0 {
		t.Fatalf("a session this instance no longer runs was put back to be drained: %v", waiting)
	}
}

// errNoAnswer is a Redis call that failed. From the caller it reads the same whether the
// server never saw it or saw it and the reply was lost, which is the whole reason the
// renew loop cannot simply believe a failure.
var errNoAnswer = errors.New("the answer never came back")

// brokenReplies fails the lease scripts on their way through, in the two ways that
// differ where it matters. `lose` lets the script run and then reports it as failed,
// which is the ambiguous renewal: applied, the lease alive a full TTL longer, and the
// instance that asked unable to know. `drop` fails before the script runs, which is the
// call that never landed at all. Nothing outside a hook can build either.
type brokenReplies struct {
	lose func(redis.Cmder) bool
	drop func(redis.Cmder) bool
}

func (brokenReplies) DialHook(next redis.DialHook) redis.DialHook { return next }

func (brokenReplies) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h brokenReplies) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.drop != nil && h.drop(cmd) {
			cmd.SetErr(errNoAnswer)
			return errNoAnswer
		}
		err := next(ctx, cmd)
		if err != nil || h.lose == nil || !h.lose(cmd) {
			return err
		}
		cmd.SetErr(errNoAnswer)
		return errNoAnswer
	}
}

// isScript keeps a hook off the plain commands a test reads state with, and off the
// SETNX an instance acquires a lease with.
func isScript(cmd redis.Cmder) bool { return strings.HasPrefix(cmd.Name(), "eval") }

// mentions reports whether a command carries a key, which is how a test tells the two
// lease scripts apart: only the hand-back names the cooldown.
func mentions(cmd redis.Cmder, key string) bool {
	for _, arg := range cmd.Args() {
		if text, ok := arg.(string); ok && text == key {
			return true
		}
	}
	return false
}

// losesRenewals fails every renewal after it has been applied, and leaves the hand-back
// alone.
func losesRenewals(cooldownKey string) brokenReplies {
	return brokenReplies{lose: func(cmd redis.Cmder) bool {
		return isScript(cmd) && !mentions(cmd, cooldownKey)
	}}
}

// A renewal whose answer was lost has still been applied, so the lease outlives by a
// full TTL the session it named. Every wake for that session then finds it owned, is
// acknowledged, and retires, which leaves the account running nowhere.
func TestRenewAllHandsBackTheLeaseOfASessionItStopped(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	keys := client.Keys()

	clock := &steppingClock{now: time.Now()}
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{Clock: clock})
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(), Leases: leases,
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()

	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	rdb.AddHook(losesRenewals(keys.Cooldown("s1")))
	clock.step(cluster.DefaultTTL + time.Second)
	manager.RenewAll(ctx)

	if got := manager.Count(); got != 0 {
		t.Fatalf("the manager still runs %d sessions on a lease that ran out", got)
	}
	peer := cluster.NewLeases(client, "inst-b", cluster.Options{Clock: clock})
	if _, err := peer.Acquire(ctx, "s1"); err != nil {
		t.Fatalf("a peer could not take over a session this instance stopped: %v", err)
	}
}

// And when the hand-back itself does not land, it is the next tick's job: a lease naming
// an instance that is not running the session is the same outage whether one round trip
// failed or two.
func TestAHandBackThatDidNotLandIsTriedAgain(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	keys := client.Keys()

	clock := &steppingClock{now: time.Now()}
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{Clock: clock})
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(), Leases: leases,
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()

	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	var away atomic.Bool
	away.Store(true)
	hook := losesRenewals(keys.Cooldown("s1"))
	hook.drop = func(cmd redis.Cmder) bool {
		return away.Load() && isScript(cmd) && mentions(cmd, keys.Cooldown("s1"))
	}
	rdb.AddHook(hook)

	clock.step(cluster.DefaultTTL + time.Second)
	manager.RenewAll(ctx)

	if got, err := rdb.Get(ctx, keys.Lease("s1")).Result(); err != nil || got != "inst-a" {
		t.Fatalf("the lease should still be there for the retry to find (got %q, %v)", got, err)
	}

	// Redis answers again. Nothing renews this session any more, so the only thing left
	// that can hand its lease back is the tick itself.
	away.Store(false)
	manager.RenewAll(ctx)

	peer := cluster.NewLeases(client, "inst-b", cluster.Options{Clock: clock})
	if _, err := peer.Acquire(ctx, "s1"); err != nil {
		t.Fatalf("a peer could not take over after the hand-back was retried: %v", err)
	}
}

// The other side of that retry: a session taken again owns its lease for real, and a
// hand-back still queued for it compares the instance and nothing else, so it would
// delete a live one.
func TestAQueuedHandBackDoesNotTouchALeaseTakenAgain(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	keys := client.Keys()

	clock := &steppingClock{now: time.Now()}
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{Clock: clock})
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(), Leases: leases,
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()

	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	var away atomic.Bool
	away.Store(true)
	hook := losesRenewals(keys.Cooldown("s1"))
	hook.drop = func(cmd redis.Cmder) bool {
		return away.Load() && isScript(cmd) && mentions(cmd, keys.Cooldown("s1"))
	}
	rdb.AddHook(hook)

	clock.step(cluster.DefaultTTL + time.Second)
	manager.RenewAll(ctx)
	away.Store(false)

	// The orphaned key runs out on its own, and a wake arrives before the next tick:
	// this instance takes the session back, under a lease it now really holds.
	server.Del(keys.Lease("s1"))
	clock.step(cluster.DefaultTTL + time.Second)
	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("re-adopting: %v", err)
	}

	manager.RenewAll(ctx)

	if got := manager.Count(); got != 1 {
		t.Fatalf("the retry stopped a session this instance had taken again (running %d)", got)
	}
	if got, err := rdb.Get(ctx, keys.Lease("s1")).Result(); err != nil || got != "inst-a" {
		t.Fatalf("the retry deleted the lease of a running session (got %q, %v)", got, err)
	}
}

// Hand-backs are Redis round trips that can hang, and they run on the goroutine that
// renews every lease this instance holds. Doing them first means a Redis having a bad
// minute costs the renewals: peers acquire the sessions while this instance still holds
// their sockets open, which is the one thing the lease exists to prevent.
func TestRenewalsComeBeforeHandBacks(t *testing.T) {
	t.Parallel()

	// A short lease, so the window a tick gives its hand-backs is short too: what is
	// under test is the order, and the window is what keeps the tick from running past
	// the thing it is protecting.
	const ttl = 300 * time.Millisecond
	const hang = 4 * ttl

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	keys := client.Keys()

	clock := &steppingClock{now: time.Now()}
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{TTL: ttl, Margin: ttl / 10, Clock: clock})
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(), Leases: leases,
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()

	// One session goes stale and is stopped, which leaves a lease to hand back. Another
	// is running perfectly well and has to be renewed.
	for _, sid := range []string{"s1", "s2"} {
		if _, err := manager.Adopt(ctx, sid); err != nil {
			t.Fatalf("Adopt %s: %v", sid, err)
		}
	}
	var away atomic.Bool
	hook := losesRenewals(keys.Cooldown("s1"))
	hook.drop = func(cmd redis.Cmder) bool {
		return away.Load() && isScript(cmd) && mentions(cmd, keys.Cooldown("s1"))
	}
	rdb.AddHook(hook)
	away.Store(true)
	clock.step(ttl + time.Millisecond)
	manager.RenewAll(ctx)
	away.Store(false)

	// s1's hand-back is queued. From here every hand-back hangs for longer than a lease,
	// and the renewal of the live session must not be waiting behind it.
	rdb.AddHook(slowCommands{slow: func(cmd redis.Cmder) bool {
		return isScript(cmd) && mentions(cmd, keys.Cooldown("s1"))
	}, delay: hang})

	if _, err := manager.Adopt(ctx, "s2"); err != nil {
		t.Fatalf("re-adopting s2: %v", err)
	}
	before := time.Now()
	manager.RenewAll(ctx)

	if got := manager.Count(); got != 1 {
		t.Fatalf("the live session was dropped while a hand-back was hanging (running %d)", got)
	}
	if spent := time.Since(before); spent >= hang {
		t.Fatalf("the tick waited %s on a hand-back, longer than the %s lease it was renewing", spent, ttl)
	}
}

// slowCommands makes a command take longer than the lease it is holding up, which is what
// a Redis having a bad minute looks like from here.
type slowCommands struct {
	slow  func(redis.Cmder) bool
	delay time.Duration
}

func (slowCommands) DialHook(next redis.DialHook) redis.DialHook { return next }

func (slowCommands) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h slowCommands) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.slow(cmd) {
			select {
			case <-time.After(h.delay):
			case <-ctx.Done():
				cmd.SetErr(ctx.Err())
				return ctx.Err()
			}
		}
		return next(ctx, cmd)
	}
}

// A wake is acknowledged when adoption finds the session already owned, because in a
// fleet that means somebody else is running it. It does not mean that when the lease
// refusing the adoption is one this instance failed to give up: nobody is running the
// session, and acknowledging retires the only thing that would have started it. Once the
// stale key expires there is nothing left to start it at all.
func TestAWakeRefusedByThisInstancesOwnStaleLeaseStaysPending(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	keys := client.Keys()

	clock := &steppingClock{now: time.Now()}
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{Clock: clock})
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(), Leases: leases,
		Publisher: newRecorder(), Replier: newRecorder(),
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	ctx := context.Background()

	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	// The lease goes stale while Redis is unreachable, and the hand-back does not land:
	// the key still names this instance, which is running nothing.
	var away atomic.Bool
	away.Store(true)
	hook := losesRenewals(keys.Cooldown("s1"))
	hook.drop = func(cmd redis.Cmder) bool {
		return away.Load() && isScript(cmd) && mentions(cmd, keys.Cooldown("s1"))
	}
	rdb.AddHook(hook)
	clock.step(cluster.DefaultTTL + time.Second)
	manager.RenewAll(ctx)

	acked := &atomic.Bool{}
	released := &atomic.Bool{}
	wake := &transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "wake-1", Type: protocol.CommandSessionWake,
			SID: "s1", TS: time.Now().UnixMilli(), Payload: json.RawMessage(`{"desired":"connected"}`),
		},
		Ack:     func(context.Context) error { acked.Store(true); return nil },
		Release: func() { released.Store(true) },
	}
	manager.Dispatch(ctx, wake)

	if acked.Load() {
		t.Fatal("the wake was retired on the word of a lease this instance is still handing back")
	}
	if !released.Load() {
		t.Fatal("the wake was neither carried out nor let go of, so nothing will reclaim it")
	}
}

// The whole reason an engine can hold WhatsApp's acknowledgement back: the pump is the
// only thing that knows whether an event reached the stream, so it is the only thing
// that can say so. Nothing above it may assume a published event without hearing this.
func TestAnEventThatReachedTheStreamIsSettledAsPublished(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	if _, err := h.manager.Adopt(context.Background(), "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, ok := h.engine.Session("s1")
	if !ok {
		t.Fatal("the engine has no session after Adopt")
	}

	settled := make(chan error, 1)
	engineSession.EmitDurable(protocol.EventMessageReceived, map[string]any{"message": "one"},
		func(err error) { settled <- err })

	select {
	case err := <-settled:
		if err != nil {
			t.Fatalf("an event that was published settled as %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("an event that was published was never settled, so nothing above can ever acknowledge it")
	}
	if got := len(h.recorder.published()); got != 1 {
		t.Fatalf("the stream holds %d events, want the one that was settled as published", got)
	}
}

// The half that costs a message if it is wrong. A publish that failed and settled as a
// success is an inbound message acknowledged to WhatsApp and delivered to nobody:
// WhatsApp drops it, the client never had it, and there is no redelivery to recover it.
func TestAnEventThatNeverReachedTheStreamIsSettledAsAFailure(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	if _, err := h.manager.Adopt(context.Background(), "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, ok := h.engine.Session("s1")
	if !ok {
		t.Fatal("the engine has no session after Adopt")
	}
	refused := errors.New("redis is unreachable")
	h.recorder.failWith(refused)

	settled := make(chan error, 1)
	engineSession.EmitDurable(protocol.EventMessageReceived, map[string]any{"message": "one"},
		func(err error) { settled <- err })

	select {
	case err := <-settled:
		if !errors.Is(err, refused) {
			t.Fatalf("an event the stream refused settled as %v, want the publisher's own error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("an event the stream refused was never settled")
	}
}

// An emission dropped because the lease moved has to settle too. The engine waiting on
// it is holding a message off WhatsApp's acknowledgement queue, and a callback that
// never fires leaves it waiting out its own bound instead of letting the account be
// redelivered to whoever owns it now.
func TestAnEventDroppedForALeaseThatMovedIsSettledAsAFailure(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	clock := &steppingClock{now: time.Now()}
	leases := cluster.NewLeases(redisx.Wrap(rdb, "wa:", 8), "inst-a", cluster.Options{Clock: clock})
	fakeEngine := fake.New()
	rec := newRecorder()
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fakeEngine, Leases: leases, Publisher: rec, Replier: rec,
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	if _, err := manager.Adopt(context.Background(), "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, ok := fakeEngine.Session("s1")
	if !ok {
		t.Fatal("the engine has no session after Adopt")
	}
	// The lease goes stale under the session, which is what a handover looks like from
	// the pump: it is still running, and it is no longer allowed to write an epoch.
	clock.step(cluster.DefaultTTL + time.Second)

	settled := make(chan error, 1)
	engineSession.EmitDurable(protocol.EventMessageReceived, map[string]any{"message": "one"},
		func(err error) { settled <- err })

	select {
	case err := <-settled:
		if err == nil {
			t.Fatal("an event dropped for a lease this instance no longer holds settled as published")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("an event dropped for a lease that moved was never settled")
	}
	if got := len(rec.published()); got != 0 {
		t.Fatalf("the stream holds %d events written under a lease that moved", got)
	}
}

// The publisher owes a callback for every emission it takes, the ones it drops
// included. A pump that stops with a durable emission already handed to it and answers
// nothing leaves the engine holding WhatsApp's acknowledgement for a message this
// instance is done with, until its own bound runs out, instead of letting the account
// be redelivered to whoever takes the session next.
func TestAnEventAPumpStoppedBeforePublishingIsStillSettled(t *testing.T) {
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

	// Connected first, so the fake reports the close below by going back down. Adopt on
	// its own opens a session without dialling anything.
	if err := engineSession.Connect(ctx, engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	waitFor(t, "the session to report itself open", func() bool { return len(h.recorder.published()) == 1 })

	// The first emission holds the pump inside Publish; the second queues up behind it
	// and is what the shutdown finds still in hand.
	release := h.recorder.hold()
	first := make(chan error, 1)
	engineSession.EmitDurable(protocol.EventMessageReceived, map[string]any{"message": "one"},
		func(err error) { first <- err })
	waitFor(t, "the pump to be busy publishing", func() bool {
		h.recorder.mu.Lock()
		defer h.recorder.mu.Unlock()
		return h.recorder.gate == nil
	})

	second := make(chan error, 1)
	engineSession.EmitDurable(protocol.EventMessageReceived, map[string]any{"message": "two"},
		func(err error) { second <- err })

	stopped := make(chan struct{})
	go func() {
		h.manager.StopAll(context.Background())
		close(stopped)
	}()
	// Stop cancels the pump and closes the engine session before it waits for the
	// goroutines, and the fake reports the close by going disconnected. Waiting for it
	// is what puts the release below after the cancel rather than racing it.
	waitFor(t, "the session to be stopped", func() bool { return !engineSession.Connected() })
	release()

	select {
	case err := <-second:
		if err == nil {
			t.Fatal("an event a stopped pump never published settled as published")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an event a stopped pump never published was never settled")
	}
	<-stopped
	if err := <-first; err != nil {
		t.Fatalf("the event the pump was publishing when it was stopped settled as %v", err)
	}
}

// Two instances, which is where the dangerous bugs live. A lease can run out while a
// write is in flight, and the peer that takes the session publishes under a higher
// epoch immediately: the event lands behind one the client has already seen from a
// newer owner, and the contract lets a client drop what comes from a stale owner. A
// successful write is therefore not proof the client has it, so an engine holding
// WhatsApp's acknowledgement on that answer would spend the redelivery that was the
// only way to get the message back.
func TestAnEventPublishedUnderALeaseThatMovedMidWriteIsSettledAsAFailure(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	clock := &steppingClock{now: time.Now()}
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{Clock: clock})
	fakeEngine := fake.New()
	rec := newRecorder()
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fakeEngine, Leases: leases, Publisher: rec, Replier: rec,
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	ctx := context.Background()
	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, ok := fakeEngine.Session("s1")
	if !ok {
		t.Fatal("the engine has no session after Adopt")
	}

	release := rec.hold()
	settled := make(chan error, 1)
	engineSession.EmitDurable(protocol.EventMessageReceived, map[string]any{"message": "one"},
		func(err error) { settled <- err })
	waitFor(t, "the pump to be busy publishing", func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.gate == nil
	})

	// The write is in flight. The lease runs out and inst-b takes the session, which is
	// exactly the window this check exists for: the ownership was there when the pump
	// looked, and gone by the time the write landed.
	clock.step(cluster.DefaultTTL + time.Second)
	server.FastForward(cluster.DefaultTTL + time.Second)
	peer := cluster.NewLeases(client, "inst-b", cluster.Options{Clock: clock})
	if _, err := peer.Acquire(ctx, "s1"); err != nil {
		t.Fatalf("inst-b Acquire: %v", err)
	}
	release()

	select {
	case err := <-settled:
		if err == nil {
			t.Fatal("an event written under an epoch a peer had already replaced settled as published")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an event written under a lease that moved was never settled")
	}
}

// Invariant 5: a command redelivered after its acknowledgement was lost must not have
// its side effect a second time. For a send that is somebody's conversation showing two
// of the same message, which is the one failure a retry is supposed to prevent.
func TestARedeliveredSendIsAnsweredWithoutSendingAgain(t *testing.T) {
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
	if err := engineSession.Connect(ctx, engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	send := &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandMessageSend, SID: "s1", ReplyTo: "c1",
		Payload: json.RawMessage(`{"message_id":"3EB0ABCDEF","to":{"kind":"phone","id":"5511999990002"},
			"content":{"type":"text","body":"oi"}}`),
	}
	var acked atomic.Bool
	h.manager.Dispatch(ctx, delivery(send, &acked))
	waitFor(t, "the send to be answered", func() bool { _, ok := h.recorder.reply("c1"); return ok })
	first, _ := h.recorder.reply("c1")

	// The same command again, which is what a lost acknowledgement produces: the
	// transport reclaims it and hands it to whoever owns the session now.
	redelivered := *send
	redelivered.ID = "c2"
	redelivered.ReplyTo = "c2"
	var ackedAgain atomic.Bool
	h.manager.Dispatch(ctx, delivery(&redelivered, &ackedAgain))
	waitFor(t, "the redelivery to be answered", func() bool { _, ok := h.recorder.reply("c2"); return ok })
	second, _ := h.recorder.reply("c2")

	if got := len(engineSession.Commands()); got != 1 {
		t.Fatalf("the engine was asked to send %d times, want once", got)
	}
	if !second.OK {
		t.Fatalf("the redelivery was refused: %+v", second.Error)
	}
	if string(second.Result) != string(first.Result) {
		t.Fatalf("the redelivery answered %s, and the first run answered %s", second.Result, first.Result)
	}
}

// A refusal is the caller's to try again. Remembering one would answer every later
// attempt with it, so a number that was briefly unreachable would stay unreachable.
func TestACommandThatFailedIsNotRememberedAsDone(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, _ := h.engine.Session("s1")

	// Not connected, so the fake refuses the send.
	send := &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandMessageSend, SID: "s1", ReplyTo: "c1",
		Payload: json.RawMessage(`{"message_id":"3EB0ABCDEF","to":{"kind":"phone","id":"5511999990002"},
			"content":{"type":"text","body":"oi"}}`),
	}
	var acked atomic.Bool
	h.manager.Dispatch(ctx, delivery(send, &acked))
	waitFor(t, "the send to be refused", func() bool { _, ok := h.recorder.reply("c1"); return ok })
	if reply, _ := h.recorder.reply("c1"); reply.OK {
		t.Fatal("the fake accepted a send on a session that is not connected")
	}

	// Now it can be sent, and the earlier failure must not be standing in for it.
	if err := engineSession.Connect(ctx, engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	retry := *send
	retry.ID = "c2"
	retry.ReplyTo = "c2"
	var ackedAgain atomic.Bool
	h.manager.Dispatch(ctx, delivery(&retry, &ackedAgain))
	waitFor(t, "the retry to be answered", func() bool { _, ok := h.recorder.reply("c2"); return ok })

	if reply, _ := h.recorder.reply("c2"); !reply.OK {
		t.Fatalf("a retry of a command that had failed was refused: %+v", reply.Error)
	}
	if got := len(engineSession.Commands()); got != 2 {
		t.Fatalf("the engine saw %d attempts, want the failure and the retry", got)
	}
}

// The key a command is remembered under. A send names its own message and is keyed by
// it; everything else brings an idempotency_key, which the frame carries rather than
// the payload. Reading it from the payload leaves every non-send command undeduplicated
// while looking like it is covered.
func TestACommandIsKeyedByWhateverNamesItOnlyOnce(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, _ := h.engine.Session("s1")

	logout := &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandSessionLogout, SID: "s1", ReplyTo: "c1",
		IdempotencyKey: "logout-once", Payload: json.RawMessage(`{}`),
	}
	var acked atomic.Bool
	h.manager.Dispatch(ctx, delivery(logout, &acked))
	waitFor(t, "the logout to be answered", func() bool { _, ok := h.recorder.reply("c1"); return ok })

	redelivered := *logout
	redelivered.ID = "c2"
	redelivered.ReplyTo = "c2"
	var ackedAgain atomic.Bool
	h.manager.Dispatch(ctx, delivery(&redelivered, &ackedAgain))
	waitFor(t, "the redelivery to be answered", func() bool { _, ok := h.recorder.reply("c2"); return ok })

	if reply, _ := h.recorder.reply("c2"); !reply.OK {
		t.Fatalf("the redelivered logout was refused: %+v", reply.Error)
	}
	if got := engineSession.LoggedOut(); got != 1 {
		t.Fatalf("the account was logged out %d times, want once", got)
	}
}

// The contract's own session.logout fixture carries no idempotency_key, so a layer that
// keys only on one leaves the command it most needs to cover uncovered. The command's
// id is what is left, and it is enough for a transport redelivery: the same entry comes
// back carrying the same frame.
func TestASideEffectWithNoKeyOfItsOwnIsKeyedByTheCommandItArrivedAs(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, _ := h.engine.Session("s1")

	// The same frame twice, which is what the transport hands back: same id, same
	// reply address. Each delivery is waited on through its own acknowledgement,
	// because the reply is written to the same place both times and seeing one there
	// says nothing about which delivery put it there.
	logout := &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandSessionLogout, SID: "s1", ReplyTo: "c1",
		Payload: json.RawMessage(`{}`),
	}
	for i := range 2 {
		var acked atomic.Bool
		h.manager.Dispatch(ctx, delivery(logout, &acked))
		waitFor(t, fmt.Sprintf("delivery %d to be retired", i+1), acked.Load)
	}

	if got := engineSession.LoggedOut(); got != 1 {
		t.Fatalf("the account was logged out %d times, want once", got)
	}
}

// A question has to be asked again. Answering a redelivered session.status from a
// record reports the state the session was in when it was first asked, which is the one
// answer that is certainly stale.
func TestAQuestionIsAskedAgainRatherThanAnsweredFromARecord(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, _ := h.engine.Session("s1")

	// The same frame twice, so a layer that keyed questions as well as side effects
	// would have the second one answered from the first one's record.
	status := &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandSessionStatus, SID: "s1", ReplyTo: "c1",
		Payload: json.RawMessage(`{}`),
	}
	var acked atomic.Bool
	h.manager.Dispatch(ctx, delivery(status, &acked))
	waitFor(t, "the first question to be retired", acked.Load)
	if first, _ := h.recorder.reply("c1"); !bytes.Contains(first.Result, []byte(`"close"`)) {
		t.Fatalf("an unconnected session first reported %s", first.Result)
	}

	// The session comes up, and the same question arrives again.
	if err := engineSession.Connect(ctx, engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	var ackedAgain atomic.Bool
	h.manager.Dispatch(ctx, delivery(status, &ackedAgain))
	waitFor(t, "the second question to be retired", ackedAgain.Load)

	second, _ := h.recorder.reply("c1")
	if !bytes.Contains(second.Result, []byte(`"open"`)) {
		t.Fatalf("an open session reported %s, which is the answer from before it came up", second.Result)
	}
}

// Nobody knows whether it already ran, and both of the other answers are wrong: running
// it risks a second side effect, and refusing it reports a failure for a command that
// may have worked. It is handed back instead, for whoever claims it next to ask again.
func TestACommandWhoseRecordCannotBeReadIsHandedBackRatherThanRun(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, _ := h.engine.Session("s1")
	h.ledger.refuse(errors.New("redis is unreachable"))

	var acked, forfeited atomic.Bool
	handed := delivery(&protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandSessionLogout, SID: "s1", ReplyTo: "c1",
		Payload: json.RawMessage(`{}`),
	}, &acked)
	handed.Forfeit = func() { forfeited.Store(true) }
	h.manager.Dispatch(ctx, handed)

	waitFor(t, "the command to be handed back", forfeited.Load)
	if acked.Load() {
		t.Fatal("a command nobody could tell had run was retired anyway")
	}
	if _, answered := h.recorder.reply("c1"); answered {
		t.Fatal("a command nobody could tell had run was answered anyway")
	}
	if got := engineSession.LoggedOut(); got != 0 {
		t.Fatalf("the account was logged out %d times on a record nobody could read", got)
	}
}

// A command cut off by the session ending is not a command that failed. The reply would
// go out on the same dead context and be dropped without a word, and acknowledging it
// afterwards retires a send that may never have reached WhatsApp, with nobody told. It
// is handed back for the next owner, which under the caller's own message id costs a
// redelivery rather than a message.
func TestACommandCutOffByTheSessionEndingIsHandedBack(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, _ := h.engine.Session("s1")
	if err := engineSession.Connect(ctx, engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	release := engineSession.Hold()
	defer release()

	var acked, forfeited atomic.Bool
	handed := delivery(&protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandMessageSend, SID: "s1", ReplyTo: "c1",
		Payload: json.RawMessage(`{"message_id":"m1","to":{"kind":"phone","id":"5511999999999"},"content":{"type":"text","body":"oi"}}`),
	}, &acked)
	handed.Forfeit = func() { forfeited.Store(true) }
	h.manager.Dispatch(ctx, handed)

	// In flight before the session is taken away, or the command never reaches the
	// engine and the test proves only that a queued delivery is released.
	waitFor(t, "the send to reach the engine", func() bool { return len(engineSession.Commands()) == 1 })
	h.manager.Release(ctx, "s1")

	waitFor(t, "the command to be handed back", forfeited.Load)
	if acked.Load() {
		t.Fatal("a send that was cut off mid-flight was retired anyway")
	}
	if _, answered := h.recorder.reply("c1"); answered {
		t.Fatal("a send that was cut off mid-flight was answered on a dead context")
	}
}

// The other side of that race: WhatsApp accepts the send in the same instant the lease
// moves. The command worked and there is a record of it, so handing it back would be
// wrong — but answering on the dying context drops the reply while the acknowledgement
// retires the command, and the caller is left with nothing about a message that is
// already in somebody's chat. The answer goes out detached, like the acknowledgement.
func TestASendThatLandedAsTheSessionEndedIsStillAnswered(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, _ := h.engine.Session("s1")
	if err := engineSession.Connect(ctx, engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	release := engineSession.HoldUntilCanceled()
	defer release()

	var acked, forfeited atomic.Bool
	handed := delivery(&protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandMessageSend, SID: "s1", ReplyTo: "c1",
		Payload: json.RawMessage(`{"message_id":"m1","to":{"kind":"phone","id":"5511999999999"},"content":{"type":"text","body":"oi"}}`),
	}, &acked)
	handed.Forfeit = func() { forfeited.Store(true) }
	h.manager.Dispatch(ctx, handed)

	waitFor(t, "the send to reach the engine", func() bool { return len(engineSession.Commands()) == 1 })
	h.manager.Release(ctx, "s1")

	waitFor(t, "the send to be answered", func() bool { _, ok := h.recorder.reply("c1"); return ok })
	reply, _ := h.recorder.reply("c1")
	if !reply.OK {
		t.Fatalf("a send that landed was answered with a failure: %+v", reply.Error)
	}
	if forfeited.Load() {
		t.Fatal("a send that landed was handed back for another instance to send again")
	}
	if !acked.Load() {
		t.Fatal("a send that landed and was answered was left pending")
	}
}

// The side effect has already happened by the time the record is written, so the record
// is the only thing left that can stop it happening again. A Redis that refuses one call
// and answers the next is the common shape of a failure there, and giving up on the
// first refusal spends the whole window on it.
func TestARecordRefusedOnceIsStillWritten(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, _ := h.engine.Session("s1")
	h.ledger.refuseWritesFor(2)

	logout := &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandSessionLogout, SID: "s1", ReplyTo: "c1",
		Payload: json.RawMessage(`{}`),
	}
	var acked atomic.Bool
	h.manager.Dispatch(ctx, delivery(logout, &acked))
	waitFor(t, "the logout to be retired", acked.Load)
	if got := h.ledger.attempts(); got < 3 {
		t.Fatalf("the record was attempted %d times, want the refusals to have been tried past", got)
	}

	// The same frame again, which is what the transport hands back. It is answered from
	// the record the retry managed to write.
	var ackedAgain atomic.Bool
	h.manager.Dispatch(ctx, delivery(logout, &ackedAgain))
	waitFor(t, "the redelivery to be retired", ackedAgain.Load)
	if got := engineSession.LoggedOut(); got != 1 {
		t.Fatalf("the account was logged out %d times, want once", got)
	}
}
