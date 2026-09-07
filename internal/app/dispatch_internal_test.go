package app

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/observability"
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
// renews every lease this instance holds, so a batch that outlives its window delays
// those renewals and hands the accounts to peers while this instance still holds their
// sockets open. The window is the caller's -- the tick window, or a reclaim pass -- and
// a batch it cuts off has to let go of the rest, or the entries are held for good.
func TestABatchStopsWhenItsWindowIsSpent(t *testing.T) {
	t.Parallel()

	connector := &Connector{
		cfg:     Config{LeaseTTL: time.Minute},
		log:     zerolog.Nop(),
		manager: newTestManager(t),
	}

	spent, cancel := context.WithCancel(context.Background())
	cancel()
	acked, released := deliveryBatch(6)
	connector.dispatchWithin(spent, acked.deliveries)

	if acked.count.Load() != 0 {
		t.Fatalf("%d commands were carried out past the window", acked.count.Load())
	}
	if released.Load() != 6 {
		t.Fatalf("%d of 6 undispatched commands were released; the rest are held for good", released.Load())
	}
}

// And with room to work, everything in the batch is carried out.
func TestABatchWithRoomCarriesEverythingOut(t *testing.T) {
	t.Parallel()

	connector := &Connector{
		cfg:     Config{LeaseTTL: time.Minute},
		log:     zerolog.Nop(),
		manager: newTestManager(t),
	}

	window, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	acked, released := deliveryBatch(6)
	connector.dispatchWithin(window, acked.deliveries)

	waitFor(t, "the whole batch to be carried out", func() bool { return acked.count.Load() == 6 })
	if released.Load() != 0 {
		t.Fatalf("%d commands were released despite the window being ample", released.Load())
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
	answering(t, manager)
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
	answering(t, manager)
	return manager
}

// timedStreams records the deadline the loop granted each of its own calls.
//
// This is where the bound #7 is about is actually granted, so this is where it is read.
// It used to be inferred from the deadline an `admin.ping` was answered under, which
// held only while the manager answered a ping on the goroutine that dispatched it.
type timedStreams struct {
	inner commandStreams
	mu    sync.Mutex
	given []time.Duration
}

func (s *timedStreams) record(ctx context.Context) {
	deadline, bounded := ctx.Deadline()
	if !bounded {
		s.mu.Lock()
		s.given = append(s.given, time.Duration(math.MaxInt64))
		s.mu.Unlock()
		return
	}
	s.mu.Lock()
	s.given = append(s.given, time.Until(deadline))
	s.mu.Unlock()
}

// longest is the most any one call was given, which is what a ceiling is asserted
// against: one call under the bound proves nothing if another was over it.
func (s *timedStreams) longest() (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.given) == 0 {
		return 0, false
	}
	return slices.Max(s.given), true
}

func (s *timedStreams) Read(ctx context.Context, sids []string) ([]transport.Delivery, error) {
	s.record(ctx)
	return s.inner.Read(ctx, sids)
}

func (s *timedStreams) Claim(ctx context.Context, sids []string) ([]transport.Delivery, error) {
	s.record(ctx)
	return s.inner.Claim(ctx, sids)
}

func (s *timedStreams) ClaimControl(ctx context.Context) ([]transport.Delivery, error) {
	s.record(ctx)
	return s.inner.ClaimControl(ctx)
}

func (s *timedStreams) ClaimSessions(ctx context.Context, sids []string) ([]transport.Delivery, error) {
	s.record(ctx)
	return s.inner.ClaimSessions(ctx, sids)
}

// waitFor blocks until cond holds, which is how a test joins a goroutine that is not the
// one it is running on: the manager's, where what used to be finished by the time
// Dispatch returned is now queued, or a session's, where a command offered to it is
// carried out rather than by whoever dispatched it.
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

// answering starts the goroutine a manager carries its own commands out on, and stops it
// when the test ends. A wake, a ping and the refusal of a full session are queued there
// rather than run by whoever dispatched, so a test that asserts on one has to let that
// goroutine run.
func answering(t *testing.T, manager *session.Manager) {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	stopped := manager.Answer(ctx)
	t.Cleanup(func() {
		stop()
		<-stopped
	})
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

// The drain claims under the window the loop handed it, and never under one of its own.
// A budget opened here would stack a fresh deadline on top of whatever the loop had
// already spent, so a drain reached late in a period would go on holding the goroutine
// that renews every lease this instance holds for a budget more, and for another on the
// pass after that.
//
// Read off the claim itself. The dispatch that follows it no longer does any I/O -- a
// session's command is offered to that session's queue and the manager's own three are
// queued on its goroutine -- so the deadline the dispatch is handed decides nothing, and
// asserting on it would be asserting on a value nothing reads. What is left to protect
// is the claim, which is the whole of what the drain spends.
func TestADrainClaimsOnWhatIsLeftOfItsWindow(t *testing.T) {
	t.Parallel()

	// A short window keeps the test quick; what it is measuring is the deadline
	// dispatch is handed, not how long anything takes.
	const window = 600 * time.Millisecond

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	newStreams := func(instance string) *redisstream.Streams {
		streams, err := redisstream.New(client, redisstream.Options{
			Instance: instance, Block: 50 * time.Millisecond, ClaimMinIdle: time.Millisecond,
		})
		if err != nil {
			t.Fatalf("redisstream.New: %v", err)
		}
		return streams
	}
	streams := newStreams("inst-a")
	dead := newStreams("inst-dead")
	timed := &timedStreams{inner: streams}
	dispatched := &deadlineReplier{}
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: dispatched,
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	answering(t, manager)
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })

	connector := &Connector{
		cfg: Config{LeaseTTL: 30 * time.Second}, log: zerolog.Nop(),
		manager: manager, streams: timed,
	}

	// An admin.ping, because the manager answers that one on the dispatch goroutine: the
	// deadline it is answered under is the deadline the drain handed dispatch.
	if _, err := dead.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writePing(t, client, "s1")
	if taken, err := dead.Read(ctx, []string{"s1"}); err != nil || len(taken) != 1 {
		t.Fatalf("the previous owner read %d commands (err=%v), want 1", len(taken), err)
	}
	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	bounded, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	connector.drainAdopted(bounded)

	given, claimed := timed.longest()
	if !claimed {
		t.Fatal("the drain made no claim, so there is no grant to look at")
	}
	if given > window {
		t.Fatalf("the drain claimed on %s, which is a fresh deadline rather than the %s window it was handed",
			given, window)
	}
}

// A wake dispatched with a sliver of window has its adoption cut almost before it
// starts, and the manager rightly forfeits a wake whose turn was spent -- so the
// sliver costs the session a whole claim delay for an attempt that never was. The
// dispatch refuses to start a wake below a floor and releases it instead: age kept,
// retried on the next pass with a window worth having. Forfeit stays what it was,
// because a wake that consumed a real window and stalled must lose its place at the
// head of the queue, or a stuck store starves every wake behind it forever.
// The floor stops at the two commands this goroutine carries out. A command for a
// session is offered to that session's queue and returns at once, and holding one back
// would trade the order the single stream exists to give: released, it stays pending
// while the next `>` read hands over a newer command for the same session, and the
// reclaim walks the sessions a window at a time before it looks at that stream again.
func TestASessionCommandIsNotHeldBackByTheFloor(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t)
	const sid = "2f1c6f0e-0000-4000-8000-0000000000fd"
	if _, err := manager.Adopt(context.Background(), sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	connector := &Connector{
		cfg:     Config{LeaseTTL: 30 * time.Second, Heartbeat: 600 * time.Millisecond},
		log:     zerolog.Nop(),
		manager: manager,
	}

	var released atomic.Int64
	deliveries := []transport.Delivery{{
		Command: protocol.Command{
			V: protocol.Version, ID: "send-late", Type: protocol.CommandMessageSend,
			SID: sid, TS: 1787000000000,
			Payload: json.RawMessage(`{"to":{"kind":"user","id":"5511999999999"},"message_id":"m1",` +
				`"content":{"type":"text","text":"hi"}}`),
		},
		Ack:     func(context.Context) error { return nil },
		Release: func() { released.Add(1) },
	}}

	sliver, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	connector.dispatchWithin(sliver, deliveries)

	if released.Load() != 0 {
		t.Fatalf("a session command was released %d time(s) over a sliver, which leaves it pending while a newer one can be read and run first",
			released.Load())
	}
}

// What the floor used to buy, bought structurally instead. A wake and a ping used to be
// carried out by whoever dispatched them, so one dispatched with a sliver of window left
// had its round trip cut and was retired all the same: the session waited out a claim
// delay for an attempt that never was, and the ping's caller waited out its own timeout
// for an answer no redelivery would produce. They were held to a floor for that reason.
//
// Neither is carried out here any more. The dispatch queues them on the manager's own
// goroutine, where the adoption gets AdoptTimeout and the answer gets its own, so how
// much window was left when they arrived decides nothing -- which is what let the floor,
// and the guessing about what a turn is worth, be deleted rather than tuned.
func TestAnAnswerIsNotMeasuredAgainstTheWindowItWasDispatchedIn(t *testing.T) {
	t.Parallel()

	connector := &Connector{
		cfg:     Config{LeaseTTL: 30 * time.Second, Heartbeat: 600 * time.Millisecond},
		log:     zerolog.Nop(),
		manager: newTestManager(t),
	}

	var released, acked atomic.Int64
	deliveries := []transport.Delivery{{
		Command: protocol.Command{
			V: protocol.Version, ID: "ping-late", Type: protocol.CommandAdminPing,
			TS: 1787000000000, ReplyTo: "ping-late",
		},
		Ack:     func(context.Context) error { acked.Add(1); return nil },
		Release: func() { released.Add(1) },
	}}

	// Alive, and far shorter than the floor the old code would have refused it on.
	sliver, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	connector.dispatchWithin(sliver, deliveries)

	waitFor(t, "the ping to be answered", func() bool { return acked.Load() == 1 })
	if released.Load() != 0 {
		t.Fatalf("the ping was released %d time(s) over a window that no longer bounds it", released.Load())
	}
}

// The window the loop hands its optional work is when the next renewal is due, and
// nothing else. It used to be a budget summed from per-step constants, and the sum came
// out wrong three times running (#7): a drain given a third of the lease, on the
// goroutine that renews every lease this instance holds, is ten seconds of renewals not
// happening on the default timing. The deadline the dispatch is answered under says
// which of the two the loop granted.
func TestTheLoopBoundsItsOptionalWorkByTheTick(t *testing.T) {
	t.Parallel()

	const heartbeat = 200 * time.Millisecond

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	newStreams := func(instance string) *redisstream.Streams {
		streams, err := redisstream.New(client, redisstream.Options{
			Instance: instance, Block: 50 * time.Millisecond, ClaimMinIdle: time.Second,
		})
		if err != nil {
			t.Fatalf("redisstream.New: %v", err)
		}
		return streams
	}
	streams := newStreams("inst-a")
	dead := newStreams("inst-dead")
	dispatched := &deadlineReplier{}
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: dispatched,
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	answering(t, manager)
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	// A lease far longer than the heartbeat, which is what tells the two grants apart: a
	// third of it is seconds, a tick window is milliseconds.
	timed := &timedStreams{inner: streams}
	connector := &Connector{
		cfg:      Config{LeaseTTL: 30 * time.Second, Heartbeat: heartbeat},
		log:      zerolog.Nop(),
		metrics:  observability.New(),
		registry: cluster.NewRegistry(client, 3*heartbeat),
		manager:  manager,
		streams:  timed,
	}

	// A command the previous owner left pending, adopted before the loop starts: the
	// first thing the loop's first iteration does is drain it, on whatever deadline the
	// loop grants, and the tick cannot have fired by then.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := dead.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writePing(t, client, "s1")
	if taken, err := dead.Read(ctx, []string{"s1"}); err != nil || len(taken) != 1 {
		t.Fatalf("the previous owner read %d commands (err=%v), want 1", len(taken), err)
	}
	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	done := make(chan struct{})
	go func() { defer close(done); _ = connector.loop(ctx, make(chan error)) }()

	waitFor(t, "the pending command to be dispatched", func() bool {
		_, dispatchedIt := dispatched.left()
		return dispatchedIt
	})
	cancel()
	<-done

	// Read where it is granted rather than off what a command was answered under. Every
	// call the loop makes outside the tick's own work shares one deadline -- when the
	// next renewal is due -- so the longest any of them was given is the whole of what
	// the period lends, and it may not exceed it.
	longest, taken := timed.longest()
	if !taken {
		t.Fatal("the loop made no read or claim, so there is no grant to look at")
	}
	if longest > heartbeat {
		t.Fatalf("the loop granted %s against a %s heartbeat: more than the period it started in",
			longest, heartbeat)
	}
}

// A drain that spends most of the window still leaves a read behind it, and one that
// comes back inside the window.
//
// The read is the only way a fresh command is seen at all, so an iteration that skips
// it when the window is narrow reads nothing for as long as the drain stays slow, and
// nothing is what an instance whose tick consistently runs long would then read for
// good. Shortening the block covers the hazard skipping was meant to cover: an
// XREADGROUP severed by the deadline mid-flight leaves a command the server had just
// moved to this consumer's pending list waiting out the whole claim delay with no local
// record of it, while newer commands for the same session are read and run ahead.
func TestASlowDrainStillLeavesRoomToRead(t *testing.T) {
	t.Parallel()

	const heartbeat = 600 * time.Millisecond
	const window = 600 * time.Millisecond
	// Sized to leave the read less than its 300ms block, so a transport that only ever
	// blocks for the configured time outlives the window here.
	const slowClaim = 250 * time.Millisecond

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	newStreams := func(instance string) *redisstream.Streams {
		streams, err := redisstream.New(client, redisstream.Options{
			Instance: instance, Block: 50 * time.Millisecond, ClaimMinIdle: time.Millisecond,
		})
		if err != nil {
			t.Fatalf("redisstream.New: %v", err)
		}
		return streams
	}
	streams := newStreams("inst-a")
	dead := newStreams("inst-dead")
	dispatched := &deadlineReplier{}
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: dispatched,
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	answering(t, manager)
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })

	connector := &Connector{
		cfg: Config{LeaseTTL: 30 * time.Second, Heartbeat: heartbeat}, log: zerolog.Nop(),
		manager: manager, streams: streams,
	}

	if _, err := dead.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writePing(t, client, "s1")
	if taken, err := dead.Read(ctx, []string{"s1"}); err != nil || len(taken) != 1 {
		t.Fatalf("the previous owner read %d commands (err=%v), want 1", len(taken), err)
	}
	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	var reads atomic.Int64
	var once sync.Once
	rdb.AddHook(slowClaims{on: func(cmd redis.Cmder) bool {
		if cmd.Name() == "xreadgroup" {
			reads.Add(1)
		}
		slow := false
		if cmd.Name() == "xclaim" {
			once.Do(func() { slow = true })
		}
		return slow
	}, delay: slowClaim})

	deadline := time.Now().Add(window)
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	connector.readCommands(bounded)
	back := time.Now()

	// The positive control: the drain itself ran and dispatched what was pending, so
	// the read below happened after a drain that had spent most of the window.
	if _, ok := dispatched.left(); !ok {
		t.Fatal("the drain did not dispatch the pending command, so this exercises nothing")
	}
	if got := reads.Load(); got == 0 {
		t.Fatal("no read was started on what the drain left of the window")
	}
	if back.After(deadline) {
		t.Fatalf("the read came back %s past the deadline: its block outlived the window it was given",
			back.Sub(deadline))
	}
}

// slowClaims makes a claim take a fixed slice of the deadline its caller is working to.
type slowClaims struct {
	on    func(redis.Cmder) bool
	delay time.Duration
}

func (slowClaims) DialHook(next redis.DialHook) redis.DialHook { return next }

func (slowClaims) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h slowClaims) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if h.on(cmd) {
			time.Sleep(h.delay)
		}
		return err
	}
}

// deadlineReplier records how long the command it answered had left, which is the deadline
// whoever dispatched it was working to.
type deadlineReplier struct {
	mu        sync.Mutex
	remaining time.Duration
	seen      bool
}

func (r *deadlineReplier) Reply(ctx context.Context, _ string, _ protocol.Reply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if deadline, ok := ctx.Deadline(); ok && !r.seen {
		r.remaining = time.Until(deadline)
		r.seen = true
	}
	return nil
}

func (r *deadlineReplier) left() (time.Duration, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.remaining, r.seen
}

func writePing(t *testing.T, client *redisx.Client, sid string) {
	t.Helper()
	writePingTo(t, client, client.Keys().Commands(sid), sid)
}

// writePingTo is writePing onto whichever stream the caller is asking about. The control
// stream carries commands addressed to no session in particular, and a ping is one of
// them: it answers, which is how a test sees that it was dispatched at all.
func writePingTo(t *testing.T, client *redisx.Client, stream, sid string) {
	t.Helper()
	command := &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandAdminPing,
		SID: sid, TS: time.Now().UnixMilli(), ReplyTo: "reply-c1", Payload: json.RawMessage(`{}`),
	}
	fields, err := command.Fields()
	if err != nil {
		t.Fatalf("Fields: %v", err)
	}
	if err := client.XAdd(t.Context(), &redis.XAddArgs{
		Stream: stream, Values: fields,
	}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}
}

// failCommands makes the commands a test names come back as a Redis that is refusing.
type failCommands struct {
	on func(redis.Cmder) bool
}

func (failCommands) DialHook(next redis.DialHook) redis.DialHook { return next }

func (failCommands) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h failCommands) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.on(cmd) {
			err := errors.New("redis is having a bad minute")
			cmd.SetErr(err)
			return err
		}
		return next(ctx, cmd)
	}
}

// `session.wake` rides the control stream, and it is the command that puts a session from
// a dead instance back on an instance. Claimed alongside a window of session streams it
// is claimed last, and a claim that fails over any stream releases everything it took —
// so one bad session stream discards the control entries too, and nothing retries them
// beyond leaving them pending. On a Redis that is failing steadily that makes the wake
// the one command that never runs during exactly the minute it exists for.
func TestAFailingSessionClaimDoesNotTakeTheControlStreamWithIt(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	newStreams := func(instance string) *redisstream.Streams {
		streams, err := redisstream.New(client, redisstream.Options{
			Instance: instance, Block: 50 * time.Millisecond, ClaimMinIdle: time.Millisecond,
		})
		if err != nil {
			t.Fatalf("redisstream.New: %v", err)
		}
		return streams
	}
	streams := newStreams("inst-a")
	dead := newStreams("inst-dead")
	dispatched := &deadlineReplier{}
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: dispatched,
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	answering(t, manager)
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })

	connector := &Connector{
		cfg: Config{LeaseTTL: 30 * time.Second, Heartbeat: time.Second}, log: zerolog.Nop(),
		manager: manager, streams: streams,
	}

	// Left pending on the control stream by an instance that stopped.
	control := client.Keys().Control()
	if _, err := dead.Read(ctx, nil); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writePingTo(t, client, control, "s1")
	if taken, err := dead.Read(ctx, nil); err != nil || len(taken) != 1 {
		t.Fatalf("the previous owner read %d commands (err=%v), want 1", len(taken), err)
	}
	// And a session this instance owns, so the window has a stream to fail over.
	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	manager.TakeNewlyAdopted()
	time.Sleep(5 * time.Millisecond)

	owned := client.Keys().Commands("s1")
	rdb.AddHook(failCommands{on: func(cmd redis.Cmder) bool {
		return len(cmd.Args()) > 1 && cmd.Args()[1] == owned &&
			(cmd.Name() == "xpending" || cmd.Name() == "xclaim")
	}})

	connector.reclaimCommands(ctx)

	if _, ok := dispatched.left(); !ok {
		t.Fatal("the control stream's pending command went unreclaimed because a session stream failed")
	}
}

// The same rule on the heartbeat's own reclaim. A heartbeat configured close to the lease
// makes a claim that spends its pass plus a fresh dispatch budget longer than the lease
// this goroutine is renewing, and the sessions whose renewals it delayed are taken by
// peers while this instance still holds their sockets open.
func TestAReclaimDispatchesOnWhatIsLeftOfItsOwnDeadline(t *testing.T) {
	t.Parallel()

	// The heartbeat is split between the control pass and the session window, so what
	// bounds this dispatch is half of it. Sized so the slow claim spends a third of that
	// half, leaving a rest that is visibly short of a fresh budget and still above the
	// floor roomFor holds every delivery to.
	const heartbeat = 600 * time.Millisecond
	const slowClaim = 100 * time.Millisecond

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	newStreams := func(instance string) *redisstream.Streams {
		streams, err := redisstream.New(client, redisstream.Options{
			Instance: instance, Block: 50 * time.Millisecond, ClaimMinIdle: time.Millisecond,
		})
		if err != nil {
			t.Fatalf("redisstream.New: %v", err)
		}
		return streams
	}
	streams := newStreams("inst-a")
	dead := newStreams("inst-dead")
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	answering(t, manager)
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })

	// A lease far longer than the heartbeat, which is what makes a budget of the pass's
	// own tell itself apart from a share of the period.
	timed := &timedStreams{inner: streams}
	connector := &Connector{
		cfg: Config{LeaseTTL: 30 * time.Second, Heartbeat: heartbeat}, log: zerolog.Nop(),
		manager: manager, streams: timed,
	}

	if _, err := dead.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writePing(t, client, "s1")
	if taken, err := dead.Read(ctx, []string{"s1"}); err != nil || len(taken) != 1 {
		t.Fatalf("the previous owner read %d commands (err=%v), want 1", len(taken), err)
	}
	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	// Already drained, so the reclaim is what picks this up.
	manager.TakeNewlyAdopted()
	time.Sleep(5 * time.Millisecond)

	// On the session stream's own claim: the control stream is reclaimed on a pass of
	// its own, and slowing that one would be measuring the wrong deadline.
	stream := client.Keys().Commands("s1")
	var once sync.Once
	rdb.AddHook(slowClaims{on: func(cmd redis.Cmder) bool {
		slow := false
		if cmd.Name() == "xclaim" && len(cmd.Args()) > 1 && cmd.Args()[1] == stream {
			once.Do(func() { slow = true })
		}
		return slow
	}, delay: slowClaim})

	connector.reclaimCommands(ctx)

	// Each pass gets its share of the heartbeat and no more, so the two together stay
	// inside one period. Read off the claims themselves: what the dispatch after them is
	// handed decides nothing any more, because it does no I/O.
	given, claimed := timed.longest()
	if !claimed {
		t.Fatal("the reclaim made no claim, so there is no grant to look at")
	}
	if budget := heartbeat / reclaimPasses; given > budget {
		t.Fatalf("a reclaim pass claimed on %s, which is more than the %s share it is allowed",
			given, budget)
	}
}

// orderedReplier records which commands were answered, in the order they were.
type orderedReplier struct {
	mu       sync.Mutex
	answered []string
}

func (r *orderedReplier) Reply(_ context.Context, replyTo string, _ protocol.Reply) error {
	r.mu.Lock()
	r.answered = append(r.answered, replyTo)
	r.mu.Unlock()
	return nil
}

func (r *orderedReplier) order() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.answered)
}

// A command given back unrun stays pending under this instance's name, and `>` returns
// only what nobody has taken. Read for that session before the held entry is taken over
// and WhatsApp's order is gone: the newer command runs first, which for a
// `session.disconnect` followed by a `session.connect` leaves the session in the state
// the client asked it not to be in. Losing a command is a command missing; this is a
// command that ran and left the wrong state behind, which is worse.
func TestACommandGivenBackIsCarriedOutBeforeAnythingNewerForItsSession(t *testing.T) {
	t.Parallel()

	const sid = "2f1c6f0e-0000-4000-8000-00000000aa01"
	connector, replies, client, _ := heldSession(t, sid)

	// Held first, newer second, and only the newer one is still unread -- which is
	// exactly the pair `>` would hand over in the wrong order.
	writeStatus(t, client, sid, "newer")
	connector.readCommands(context.Background())

	waitFor(t, "both commands to be answered", func() bool { return len(replies.order()) == 2 })
	if got := replies.order(); got[0] != "held-1" {
		t.Fatalf("the commands were answered %v, want the held one first", got)
	}
}

// The site this is actually about. A batch cut off by its window gives the rest back,
// and that is where a session's command is most often left pending: reclaimed batches
// are dispatched under a share of the heartbeat, and what does not fit is released.
// Driven through the dispatch rather than by handing the delivery back directly, because
// a rule that lives only where a test calls it is a rule the production path can lose.
func TestABatchCutOffKeepsItsSessionsTurn(t *testing.T) {
	t.Parallel()

	const sid = "2f1c6f0e-0000-4000-8000-00000000aa05"
	connector, replies, client, streams := adoptedSession(t, sid)

	writeStatus(t, client, sid, "cut")
	taken, err := streams.Read(context.Background(), []string{sid})
	if err != nil || len(taken) != 1 {
		t.Fatalf("the command to cut was read %d time(s) (err=%v), want 1", len(taken), err)
	}
	spent, cancel := context.WithCancel(context.Background())
	cancel()
	connector.dispatchWithin(spent, taken)

	writeStatus(t, client, sid, "newer")
	connector.readCommands(context.Background())

	waitFor(t, "the cut command and the newer one to be answered", func() bool {
		order := replies.order()
		return slices.Contains(order, "cut") && slices.Contains(order, "newer")
	})
	order := replies.order()
	if slices.Index(order, "cut") > slices.Index(order, "newer") {
		t.Fatalf("the commands were answered %v, want the one the batch cut off first", order)
	}
}

// The case a mark cleared on the first acceptance gets wrong: with two entries held, the
// reclaim brings the first, it is accepted, the mark falls, and the second is overtaken
// by something newer. The drain lets a session back into the read when a pass finds
// nothing left for it, never when one command was taken.
func TestTwoCommandsGivenBackAreBothCarriedOutBeforeAnythingNewer(t *testing.T) {
	t.Parallel()

	const sid = "2f1c6f0e-0000-4000-8000-00000000aa02"
	connector, replies, client, streams := heldSession(t, sid)

	writeStatus(t, client, sid, "held-2")
	second, err := streams.Read(context.Background(), []string{sid})
	if err != nil || len(second) != 1 {
		t.Fatalf("the second command to hold was read %d time(s) (err=%v), want 1", len(second), err)
	}
	connector.manager.GiveBack(&second[0])

	writeStatus(t, client, sid, "newer")
	connector.readCommands(context.Background())

	waitFor(t, "all three commands to be answered", func() bool { return len(replies.order()) == 3 })
	got := replies.order()
	if slices.Index(got, "newer") != 2 {
		t.Fatalf("the commands were answered %v, want the newer one last", got)
	}
}

// The brake is per session, and this is the failure mode next door: one that always
// brakes is one that never reads, and nothing would report it -- no error, no crash, just
// commands that quietly stop arriving for every session on the instance.
//
// Held by a drain that cannot take the stream over, which is the state that outlives a
// single call: a session stays out of the read for as long as its backlog is not taken,
// and that is backpressure working rather than starvation. What must not happen is the
// session beside it paying for the wait.
func TestASessionThatStaysUndrainedDoesNotStopTheReadsForAnother(t *testing.T) {
	t.Parallel()

	const stuck = "2f1c6f0e-0000-4000-8000-00000000aa03"
	const other = "2f1c6f0e-0000-4000-8000-00000000aa04"
	connector, replies, client, _ := heldSession(t, stuck, other)
	connector.streams = &undrainableStreams{inner: connector.streams}

	writeStatus(t, client, other, "elsewhere")

	connector.readCommands(context.Background())

	waitFor(t, "the other session's command to be answered", func() bool {
		return slices.Contains(replies.order(), "elsewhere")
	})
	if slices.Contains(replies.order(), "held-1") {
		t.Fatal("the held command was carried out even though its stream was never taken over")
	}
}

// undrainableStreams is a transport whose drain never succeeds, which is what leaves a
// session out of the read across ticks instead of for the rest of one call.
type undrainableStreams struct {
	inner commandStreams
}

func (s *undrainableStreams) Read(ctx context.Context, sids []string) ([]transport.Delivery, error) {
	return s.inner.Read(ctx, sids)
}

func (s *undrainableStreams) Claim(ctx context.Context, sids []string) ([]transport.Delivery, error) {
	return s.inner.Claim(ctx, sids)
}

func (s *undrainableStreams) ClaimControl(ctx context.Context) ([]transport.Delivery, error) {
	return s.inner.ClaimControl(ctx)
}

func (s *undrainableStreams) ClaimSessions(context.Context, []string) ([]transport.Delivery, error) {
	return nil, errors.New("the drain cannot reach redis")
}

// writeStatus puts a session command on that session's own stream. Not a ping: a ping is
// answered by the manager itself and belongs to the control stream, so giving one back
// leaves nothing pending where the order this is about is decided.
func writeStatus(t *testing.T, client *redisx.Client, sid, id string) {
	t.Helper()
	command := &protocol.Command{
		V: protocol.Version, ID: id, Type: protocol.CommandSessionStatus,
		SID: sid, TS: time.Now().UnixMilli(), ReplyTo: id, Payload: json.RawMessage(`{}`),
	}
	fields, err := command.Fields()
	if err != nil {
		t.Fatalf("Fields: %v", err)
	}
	if err := client.XAdd(t.Context(), &redis.XAddArgs{
		Stream: client.Keys().Commands(sid), Values: fields,
	}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}
}

// heldSession builds a connector running `sid` with one command of its own given back
// unrun, which is the state this file is about.
// `also` are sessions adopted alongside it and drained before anything is held, so the
// only session waiting on a drain is the one the test is about.
func heldSession(
	t *testing.T, sid string, also ...string,
) (*Connector, *orderedReplier, *redisx.Client, *redisstream.Streams) {
	t.Helper()

	connector, replies, client, streams := adoptedSession(t, sid, also...)
	writeStatus(t, client, sid, "held-1")
	taken, err := streams.Read(context.Background(), []string{sid})
	if err != nil || len(taken) != 1 {
		t.Fatalf("the command to hold was read %d time(s) (err=%v), want 1", len(taken), err)
	}
	connector.manager.GiveBack(&taken[0])
	return connector, replies, client, streams
}

// adoptedSession is the same instance with the sessions running and nothing held, for the
// tests that want the give-back to be the only thing that ever marks a session.
func adoptedSession(
	t *testing.T, sid string, also ...string,
) (*Connector, *orderedReplier, *redisx.Client, *redisstream.Streams) {
	t.Helper()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)

	streams, err := redisstream.New(client, redisstream.Options{
		Instance: "inst-a", Block: 20 * time.Millisecond, ClaimMinIdle: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("redisstream.New: %v", err)
	}
	replies := &orderedReplier{}
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: replies,
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	connector := &Connector{
		cfg: Config{LeaseTTL: time.Minute, Heartbeat: time.Second}, log: zerolog.Nop(),
		manager: manager, streams: streams,
	}
	ctx := context.Background()
	for _, adopt := range append([]string{sid}, also...) {
		if _, err := manager.Adopt(ctx, adopt); err != nil {
			t.Fatalf("Adopt(%s): %v", adopt, err)
		}
	}
	// Adoption marks the session for draining on its own, and that is not what these
	// tests are about: taken here, so the only mark left is the give-back's.
	manager.TakeNewlyAdopted()

	return connector, replies, client, streams
}
