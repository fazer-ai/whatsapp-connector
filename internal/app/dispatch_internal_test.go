package app

import (
	"context"
	"encoding/json"
	"errors"
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
// sockets open. The window is the caller's — the tick window, or a reclaim pass — and
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

	if acked.count.Load() != 6 {
		t.Fatalf("%d of 6 commands were carried out", acked.count.Load())
	}
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

// The drain claims and then dispatches, and the two share the window the loop handed
// it. Opening a budget of its own for the dispatch would stack a fresh deadline on top
// of whatever the claim spent, so a drain that has already spent most of its window
// would go on holding the goroutine that renews every lease this instance holds for a
// budget more, and for another on the pass after that.
func TestADrainDispatchesOnWhatIsLeftOfItsWindow(t *testing.T) {
	t.Parallel()

	// A short window keeps the test quick; what it is measuring is the deadline
	// dispatch is handed, not how long anything takes.
	const window = 600 * time.Millisecond
	const slowClaim = 150 * time.Millisecond

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
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })

	connector := &Connector{
		cfg: Config{LeaseTTL: 30 * time.Second}, log: zerolog.Nop(),
		manager: manager, streams: streams,
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

	// The claim spends most of the drain's window, which is the case that tells the two
	// contexts apart: what is left of the window, or a fresh deadline over again.
	var once sync.Once
	rdb.AddHook(slowClaims{on: func(cmd redis.Cmder) bool {
		slow := false
		if cmd.Name() == "xclaim" {
			once.Do(func() { slow = true })
		}
		return slow
	}, delay: slowClaim})

	bounded, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	connector.drainAdopted(bounded)

	left, ok := dispatched.left()
	if !ok {
		t.Fatal("nothing was dispatched, so there is no deadline to look at")
	}
	if left > window-slowClaim/2 {
		t.Fatalf("dispatch was given %s, which is a fresh deadline rather than what was left of the drain's %s",
			left, window)
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
	t.Cleanup(func() { manager.StopAll(context.Background()) })

	connector := &Connector{
		// A lease far longer than the heartbeat, which is what tells the two grants
		// apart: a third of it is seconds, a tick window is milliseconds.
		cfg:      Config{LeaseTTL: 30 * time.Second, Heartbeat: heartbeat},
		log:      zerolog.Nop(),
		metrics:  observability.New(),
		registry: cluster.NewRegistry(client, 3*heartbeat),
		manager:  manager,
		streams:  streams,
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

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := dispatched.left(); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the pending command was never dispatched")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	if left, _ := dispatched.left(); left > heartbeat {
		t.Fatalf("the drain dispatched on %s against a %s heartbeat: the loop granted more than the period",
			left, heartbeat)
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
	// bounds this dispatch is half of it. Sized so the slow claim spends half of that
	// half and leaves the other half visibly short of a fresh budget.
	const heartbeat = 600 * time.Millisecond
	const slowClaim = 150 * time.Millisecond

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
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })

	connector := &Connector{
		// A lease far longer than the heartbeat, which is what makes a fresh dispatch
		// budget tell itself apart from what is left of the pass.
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

	left, ok := dispatched.left()
	if !ok {
		t.Fatal("nothing was dispatched, so there is no deadline to look at")
	}
	if budget := heartbeat / reclaimPasses; left > budget-slowClaim/2 {
		t.Fatalf("dispatch was given %s, which is a fresh budget rather than what was left of the pass's %s",
			left, budget)
	}
}
