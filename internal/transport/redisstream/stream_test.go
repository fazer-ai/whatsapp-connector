package redisstream_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
	"github.com/fazer-ai/whatsapp-connector/internal/transport/redisstream"
)

const shards = 8

type fleet struct {
	server *miniredis.Miniredis
	rdb    *redis.Client
	client *redisx.Client
}

func newFleet(t *testing.T) fleet {
	t.Helper()
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return fleet{server: server, rdb: rdb, client: redisx.Wrap(rdb, "wa:", shards)}
}

// count makes the fleet's client report how many commands it actually sends, which is
// the only way to tell a read that answered quietly from one that never went out.
func (f fleet) count(n *atomic.Int64) { f.rdb.AddHook(&sentCommands{n: n}) }

type sentCommands struct{ n *atomic.Int64 }

func (*sentCommands) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *sentCommands) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.n.Add(1)
		return next(ctx, cmd)
	}
}

func (h *sentCommands) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.n.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

// streamsLogging is `streams` with somewhere to read the log, for the one diagnosis this
// layer makes on its own instead of answering its caller with it.
func (f fleet) streamsLogging(t *testing.T, instance string, written io.Writer) *redisstream.Streams {
	t.Helper()
	streams, err := redisstream.New(f.client, redisstream.Options{
		Instance: instance, Block: 50 * time.Millisecond, ClaimMinIdle: time.Millisecond,
		Logger: zerolog.New(written),
	})
	if err != nil {
		t.Fatalf("redisstream.New: %v", err)
	}
	return streams
}

func (f fleet) streams(t *testing.T, instance string) *redisstream.Streams {
	t.Helper()
	// A short block keeps a read that finds nothing from holding the test for the
	// production interval, and a short min-idle keeps a reclaim from waiting out the
	// thirty seconds a fleet gives a peer to finish what it took.
	streams, err := redisstream.New(f.client, redisstream.Options{
		Instance: instance, Block: 50 * time.Millisecond, ClaimMinIdle: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("redisstream.New: %v", err)
	}
	return streams
}

func event(sid string, seq uint64) *protocol.Event {
	return &protocol.Event{
		V: protocol.Version, ID: "evt-" + sid, Type: protocol.EventSessionState,
		SID: sid, Epoch: 1, Seq: seq, TS: 1787000000000, Inst: "inst-a",
		Payload: json.RawMessage(`{"state":"open"}`),
	}
}

func command(id, sid, replyTo string) *protocol.Command {
	return &protocol.Command{
		V: protocol.Version, ID: id, Type: protocol.CommandSessionStatus,
		SID: sid, TS: 1787000000000, ReplyTo: replyTo, Payload: json.RawMessage(`{}`),
	}
}

// Every event of a session has to land on one stream, because that stream is read by
// one consumer and that is the whole of the ordering guarantee.
func TestPublishSendsASessionToOneShard(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	streams := f.streams(t, "inst-a")
	ctx := context.Background()

	for seq := uint64(1); seq <= 3; seq++ {
		if err := streams.Publish(ctx, event("s1", seq)); err != nil {
			t.Fatalf("Publish seq %d: %v", seq, err)
		}
	}

	key := f.client.Keys().EventsOf("s1")
	entries, err := f.client.XRange(ctx, key, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("%s holds %d entries, want 3", key, len(entries))
	}
	for i, entry := range entries {
		parsed, parseErr := protocol.ParseEvent(toFields(entry.Values))
		if parseErr != nil {
			t.Fatalf("entry %d does not parse back: %v", i, parseErr)
		}
		if parsed.Seq != uint64(i+1) {
			t.Errorf("entry %d has seq %d, want %d", i, parsed.Seq, i+1)
		}
	}
}

func TestPublishRoundTripsTheFrame(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	ctx := context.Background()
	want := event("s1", 7)

	if err := f.streams(t, "inst-a").Publish(ctx, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	entries, err := f.client.XRange(ctx, f.client.Keys().EventsOf("s1"), "-", "+").Result()
	if err != nil || len(entries) != 1 {
		t.Fatalf("XRange = %v, %v", entries, err)
	}
	got, err := protocol.ParseEvent(toFields(entries[0].Values))
	if err != nil {
		t.Fatalf("ParseEvent: %v", err)
	}
	if got.ID != want.ID || got.Type != want.Type || got.SID != want.SID ||
		got.Epoch != want.Epoch || got.Seq != want.Seq || got.TS != want.TS || got.Inst != want.Inst {
		t.Errorf("round trip changed the frame:\n got %+v\nwant %+v", got, want)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Errorf("payload = %s, want %s", got.Payload, want.Payload)
	}
}

func TestReadDeliversCommandsForOwnedSessions(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	streams := f.streams(t, "inst-a")
	ctx := context.Background()

	// The group has to exist before the command is written, exactly as it does in
	// production: the reader creates it on its first pass.
	if _, err := streams.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeCommand(t, f, f.client.Keys().Commands("s1"), command("c1", "s1", "c1"))

	delivered, err := streams.Read(ctx, []string{"s1"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(delivered) != 1 {
		t.Fatalf("Read returned %d commands, want 1", len(delivered))
	}
	if delivered[0].Command.ID != "c1" {
		t.Errorf("command id = %q, want c1", delivered[0].Command.ID)
	}
}

// A read takes the caller's deadline as the bound on its own block, so a window too
// narrow for the configured one costs a short read rather than a severed connection.
//
// Being cut off mid-flight is what that avoids: the server may move a command into this
// consumer's pending list just as the connection dies, and an entry pending here that no
// delivery was ever handed back for is claimable by nobody until the whole claim delay
// has passed, with newer commands for the same session read and run ahead of it. The
// caller cannot avoid it by refusing to read either -- a loop whose other work
// consistently leaves less than a block would then never read at all.
func TestAReadShortensItsBlockToTheCallersDeadline(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	// The group has to exist before the read under test, and creating it from another
	// instance keeps that first call off the long block this one is configured with.
	if _, err := f.streams(t, "inst-prep").Read(context.Background(), []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	// A block far longer than the window below, which is the shape a tick that ran long
	// leaves behind in production.
	streams, err := redisstream.New(f.client, redisstream.Options{
		Instance: "inst-a", Block: 2 * time.Second, ClaimMinIdle: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("redisstream.New: %v", err)
	}

	deadline := time.Now().Add(150 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	// Nothing is waiting, so the call is all block: what comes back is either the server
	// answering "nothing this round" inside the window, or the deadline cutting it off.
	delivered, err := streams.Read(ctx, []string{"s1"})
	back := time.Now()
	if err != nil {
		t.Fatalf("Read was cut off instead of shortening its block: %v", err)
	}
	if len(delivered) != 0 {
		t.Fatalf("Read returned %d commands from a stream nothing was written to", len(delivered))
	}
	if back.After(deadline) {
		t.Fatalf("Read came back %s past the deadline it was given", back.Sub(deadline))
	}
}

// A window with no room for a round trip buys a read nothing, and costs what the read
// would have cost: a command the server moves into this consumer's pending list on a
// call whose answer the deadline cuts off is claimable by nobody for the whole claim
// delay, with no delivery here to release it.
func TestAReadIsSkippedWhenNoRoundTripFitsTheWindow(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	streams := f.streams(t, "inst-a")
	if _, err := streams.Read(context.Background(), []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}

	var sent atomic.Int64
	f.count(&sent)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond))
	defer cancel()

	delivered, err := streams.Read(ctx, []string{"s1"})
	if err != nil {
		t.Fatalf("Read on a spent window = %v, want the quiet answer a read gives when it finds nothing", err)
	}
	if len(delivered) != 0 {
		t.Fatalf("Read returned %d commands from a window it should not have used", len(delivered))
	}
	if got := sent.Load(); got != 0 {
		t.Fatalf("%d command(s) went to Redis on a window with no room for the answer", got)
	}
}

// The group ensure spends the same window the block is measured against, so the block
// has to be measured again after it. A stream this instance has not read before costs an
// XGROUP CREATE, and a block decided before that trip outlives the deadline by whatever
// the trip took.
func TestAReadMeasuresItsBlockAgainAfterCreatingGroups(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	// A block long enough that the window, not the configured block, bounds the read.
	streams, err := redisstream.New(f.client, redisstream.Options{
		Instance: "inst-a", Block: 2 * time.Second, ClaimMinIdle: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("redisstream.New: %v", err)
	}
	// One group per stream, and a read always listens to control as well as the session,
	// so the ensure is two trips. Sized so together they eat most of the window: what is
	// left is well under the block a measurement taken before them would have chosen.
	const window = 300 * time.Millisecond
	f.rdb.AddHook(slowCommands{
		on:    func(cmd redis.Cmder) bool { return strings.HasPrefix(cmd.Name(), "xgroup") },
		delay: 90 * time.Millisecond,
	})

	deadline := time.Now().Add(window)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	if _, err := streams.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if back := time.Now(); back.After(deadline) {
		t.Fatalf("Read came back %s past the deadline: its block was measured before the group ensure spent the window",
			back.Sub(deadline))
	}
}

// slowCommands makes the commands a test names take a fixed slice of the window their
// caller is working to.
type slowCommands struct {
	on    func(redis.Cmder) bool
	delay time.Duration
}

func (slowCommands) DialHook(next redis.DialHook) redis.DialHook { return next }

func (slowCommands) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h slowCommands) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.on(cmd) {
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

// `session.wake` arrives for sessions nobody owns yet, so an instance that only read
// its own would never hear about a session it is supposed to pick up.
func TestReadAlwaysListensToControl(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	streams := f.streams(t, "inst-a")
	ctx := context.Background()

	if _, err := streams.Read(ctx, nil); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeCommand(t, f, f.client.Keys().Control(), command("c-wake", "s9", ""))

	delivered, err := streams.Read(ctx, nil)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(delivered) != 1 || delivered[0].Command.ID != "c-wake" {
		t.Fatalf("Read returned %+v, want the control command", delivered)
	}
}

// An acknowledged command is gone; an un-acknowledged one is what another instance
// claims after this one dies.
func TestAckRemovesTheCommandFromThePendingList(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	streams := f.streams(t, "inst-a")
	ctx := context.Background()

	if _, err := streams.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeCommand(t, f, f.client.Keys().Commands("s1"), command("c1", "s1", "c1"))

	delivered, err := streams.Read(ctx, []string{"s1"})
	if err != nil || len(delivered) != 1 {
		t.Fatalf("Read = %v, %v", delivered, err)
	}
	if pending := pendingCount(t, f, "s1"); pending != 1 {
		t.Fatalf("before ack, %d pending, want 1", pending)
	}

	if err := delivered[0].Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if pending := pendingCount(t, f, "s1"); pending != 0 {
		t.Fatalf("after ack, %d pending, want 0", pending)
	}
}

// A frame nothing can parse must not sit in the pending list forever: no retry fixes
// it, and the caller learns about it from its own reply timing out.
func TestUnreadableCommandIsDroppedRatherThanKeptPending(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	streams := f.streams(t, "inst-a")
	ctx := context.Background()

	if _, err := streams.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	if err := f.client.XAdd(ctx, &redis.XAddArgs{
		Stream: f.client.Keys().Commands("s1"),
		Values: map[string]any{"v": "1", "id": "c-bad", "type": "session.status", "sid": "s1"},
	}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}

	delivered, err := streams.Read(ctx, []string{"s1"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(delivered) != 0 {
		t.Fatalf("Read returned %d commands, want the malformed one dropped", len(delivered))
	}
	if pending := pendingCount(t, f, "s1"); pending != 0 {
		t.Fatalf("%d pending, want the malformed entry acknowledged", pending)
	}
}

func TestReplyPushesOneElementWithATTL(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	ctx := context.Background()
	reply := protocol.Reply{V: protocol.Version, ID: "c1", OK: true, Result: json.RawMessage(`{"state":"open"}`)}

	// The key as a client spells it in `reply_to`, which is where it is blocked: the
	// contract's own command frames carry it fully spelled, and this is what stops the
	// prefix from being applied a second time on the way out.
	key := f.client.Keys().Reply("c1")
	if err := f.streams(t, "inst-a").Reply(ctx, key, reply); err != nil {
		t.Fatalf("Reply: %v", err)
	}

	// Read the expiry before taking the element: popping the only element deletes the
	// key, and a deleted key reports no TTL.
	if ttl := f.server.TTL(key); ttl <= 0 {
		t.Errorf("reply key TTL = %v, want a positive expiry so a caller that gave up leaves nothing behind", ttl)
	}

	body, err := f.client.LPop(ctx, key).Result()
	if err != nil {
		t.Fatalf("LPop: %v", err)
	}
	var got protocol.Reply
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("unmarshal reply: %v", err)
	}
	if !got.OK || got.ID != "c1" {
		t.Errorf("reply = %+v, want ok for c1", got)
	}
}

// The destination comes off the wire, so it is checked rather than trusted: everything
// else under the prefix is fleet state, and one transaction would leave a TTL on it even
// where the push fails on the type.
func TestReplyRefusesADestinationOutsideTheReplyNamespace(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	ctx := context.Background()
	reply := protocol.Reply{V: protocol.Version, ID: "c1", OK: true}

	for _, key := range []string{
		f.client.Keys().Instances(),
		f.client.Keys().Commands("2f1c6f0e-0000-4000-8000-000000000001"),
		f.client.Keys().Prefix() + "reply:",
		"reply:c1",
		"wa:other:c1",
	} {
		if err := f.streams(t, "inst-a").Reply(ctx, key, reply); err == nil {
			t.Errorf("Reply to %q returned no error, want a refusal", key)
		}
		if ttl := f.server.TTL(key); ttl > 0 {
			t.Errorf("Reply to %q left a TTL of %v behind", key, ttl)
		}
	}
}

func TestReplyRefusesWithoutADestination(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	err := f.streams(t, "inst-a").Reply(context.Background(), "", protocol.Reply{V: protocol.Version, OK: true})
	if err == nil {
		t.Fatal("Reply with no destination returned no error")
	}
}

func TestNewRequiresAnInstanceID(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	if _, err := redisstream.New(f.client, redisstream.Options{}); err == nil {
		t.Fatal("New without an instance id returned no error")
	}
}

func writeCommand(t *testing.T, f fleet, stream string, cmd *protocol.Command) {
	t.Helper()
	fields, err := cmd.Fields()
	if err != nil {
		t.Fatalf("render command: %v", err)
	}
	values := make(map[string]any, len(fields))
	for key, value := range fields {
		values[key] = value
	}
	if err := f.client.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: values}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}
}

func pendingCount(t *testing.T, f fleet, sid string) int64 {
	t.Helper()
	pending, err := f.client.XPending(context.Background(), f.client.Keys().Commands(sid), redisstream.ConsumerGroup).Result()
	if err != nil {
		t.Fatalf("XPending: %v", err)
	}
	return pending.Count
}

func toFields(values map[string]any) map[string]string {
	fields := make(map[string]string, len(values))
	for key, value := range values {
		if text, ok := value.(string); ok {
			fields[key] = text
		}
	}
	return fields
}

var _ transport.Transport = (*redisstream.Streams)(nil)

// An entry a consumer read and never acknowledged is invisible to every later read:
// `>` only ever returns what nobody has taken. Claim is the only way back to it, which
// makes it the difference between an instance dying mid-command and that command being
// lost, and between a wake left pending on purpose and a session nobody ever adopts.
func TestClaimTakesOverWhatWasNeverAcknowledged(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	dead := f.streams(t, "inst-dead")
	alive := f.streams(t, "inst-alive")
	ctx := context.Background()

	if _, err := dead.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeCommand(t, f, f.client.Keys().Commands("s1"), command("c1", "s1", "c1"))

	taken, err := dead.Read(ctx, []string{"s1"})
	if err != nil || len(taken) != 1 {
		t.Fatalf("the first instance read %d commands (err=%v), want 1", len(taken), err)
	}
	// And then it dies, without acknowledging.

	fresh, err := alive.Read(ctx, []string{"s1"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(fresh) != 0 {
		t.Fatalf("a plain read returned %d commands; the entry is pending, not new", len(fresh))
	}

	claimed := claimEventually(t, alive, []string{"s1"})
	if len(claimed) != 1 {
		t.Fatalf("Claim returned %d commands, want 1", len(claimed))
	}
	if claimed[0].Command.ID != "c1" {
		t.Errorf("claimed command id = %q, want c1", claimed[0].Command.ID)
	}

	// Acknowledging it is what retires it, so a third instance finds nothing left.
	if err := claimed[0].Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	again, err := f.streams(t, "inst-third").Claim(ctx, []string{"s1"})
	if err != nil {
		t.Fatalf("Claim after an ack: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("an acknowledged command was claimed again (%d)", len(again))
	}
}

// A wake rides the control stream, and a wake that could not be acted on is exactly the
// entry this connector leaves pending on purpose. It is reclaimed apart from the session
// streams so that neither a window that spent the whole deadline nor one stream in it
// failing can take the wake down with it, but reclaimed it must be.
func TestTheControlStreamIsReclaimedOnItsOwn(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	dead := f.streams(t, "inst-dead")
	alive := f.streams(t, "inst-alive")
	ctx := context.Background()

	if _, err := dead.Read(ctx, nil); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeCommand(t, f, f.client.Keys().Control(), command("c-wake", "s9", ""))
	if taken, err := dead.Read(ctx, nil); err != nil || len(taken) != 1 {
		t.Fatalf("the first instance read %d commands (err=%v), want 1", len(taken), err)
	}

	// No owned sessions at all, which is the state an instance is in when the wake it
	// could not act on is the only thing outstanding.
	claimed := takeEventually(t, func() ([]transport.Delivery, error) {
		return alive.ClaimControl(context.Background())
	})
	if len(claimed) != 1 || claimed[0].Command.ID != "c-wake" {
		t.Fatalf("ClaimControl returned %v, want the pending wake", claimed)
	}

	// And the session claim leaves it alone, which is what keeps the two apart: a window
	// of sessions that fails or runs out of deadline never had the wake to lose.
	if session, err := f.streams(t, "inst-third").Claim(context.Background(), nil); err != nil || len(session) != 0 {
		t.Fatalf("the session claim took %d control entries (err=%v), want none", len(session), err)
	}
}

// claimEventually retries until the pending entry is older than the min-idle. Nothing
// about the wait is the behaviour under test: an entry is only claimable once it has
// been idle long enough, and asserting on the first attempt would be asserting on how
// fast the test itself ran.
func claimEventually(t *testing.T, streams *redisstream.Streams, sids []string) []transport.Delivery {
	t.Helper()
	return takeEventually(t, func() ([]transport.Delivery, error) {
		return streams.Claim(context.Background(), sids)
	})
}

// takeEventually is claimEventually over whichever claim the caller is asking about.
func takeEventually(t *testing.T, take func() ([]transport.Delivery, error)) []transport.Delivery {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		claimed, err := take()
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if len(claimed) > 0 || time.Now().After(deadline) {
			return claimed
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A command runs on the session's own executor, not on the loop that read it, so it is
// still pending while it runs. One that takes longer than the min-idle would otherwise
// be handed back to the very consumer already executing it and dispatched a second time
// alongside the first, which acknowledging the original does not undo.
func TestClaimLeavesThisConsumersOwnWorkAlone(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	streams := f.streams(t, "inst-a")
	ctx := context.Background()

	if _, err := streams.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeCommand(t, f, f.client.Keys().Commands("s1"), command("c-slow", "s1", "c-slow"))
	taken, err := streams.Read(ctx, []string{"s1"})
	if err != nil || len(taken) != 1 {
		t.Fatalf("Read returned %d commands (err=%v), want 1", len(taken), err)
	}

	// Still executing, so still pending, and by now idle for longer than the min-idle.
	time.Sleep(20 * time.Millisecond)

	claimed, err := streams.Claim(ctx, []string{"s1"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("an instance reclaimed %d of its own in-flight commands", len(claimed))
	}

	// A peer still takes it over, which is what makes the instance dying recoverable.
	peer := claimEventually(t, f.streams(t, "inst-b"), []string{"s1"})
	if len(peer) != 1 || peer[0].Command.ID != "c-slow" {
		t.Fatalf("a peer claimed %v, want the pending command", peer)
	}
}

// The other half of the rule above. A wake this connector deliberately left pending,
// and a command it walked away from because it owns no such session, are both entries
// pending under this instance's own name. In a fleet of one there is nobody else to
// take them, so releasing them has to make them claimable here.
func TestAReleasedCommandComesBackToTheSameInstance(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	streams := f.streams(t, "inst-a")
	ctx := context.Background()

	if _, err := streams.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeCommand(t, f, f.client.Keys().Commands("s1"), command("c-left", "s1", "c-left"))
	taken, err := streams.Read(ctx, []string{"s1"})
	if err != nil || len(taken) != 1 {
		t.Fatalf("Read returned %d commands (err=%v), want 1", len(taken), err)
	}

	// Walked away from without being carried out.
	taken[0].Release()

	claimed := claimEventually(t, streams, []string{"s1"})
	if len(claimed) != 1 || claimed[0].Command.ID != "c-left" {
		t.Fatalf("the instance reclaimed %v, want the command it left behind", claimed)
	}
}

// Entries this process is still running stay pending for as long as they run, so a page
// full of them would hide everything behind it on every heartbeat, forever.
func TestClaimLooksPastAPageItCannotUse(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	const page = 4
	streams, err := redisstream.New(f.client, redisstream.Options{
		Instance: "inst-a", Block: 50 * time.Millisecond,
		ClaimMinIdle: time.Millisecond, ReadCount: page,
	})
	if err != nil {
		t.Fatalf("redisstream.New: %v", err)
	}
	ctx := context.Background()
	stream := f.client.Keys().Commands("s1")

	if _, err := streams.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	// Two full pages of commands this process takes and never finishes, and one behind
	// them that a peer abandoned.
	for i := range page * 2 {
		writeCommand(t, f, stream, command("c-busy-"+strconv.Itoa(i), "s1", ""))
	}
	for held := 0; held < page*2; {
		taken, err := streams.Read(ctx, []string{"s1"})
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if len(taken) == 0 {
			t.Fatalf("only %d of %d commands were taken", held, page*2)
		}
		held += len(taken)
	}

	writeCommand(t, f, stream, command("c-abandoned", "s1", ""))
	abandoned := f.streams(t, "inst-dead")
	if taken, err := abandoned.Read(ctx, []string{"s1"}); err != nil || len(taken) != 1 {
		t.Fatalf("the peer read %d commands (err=%v), want 1", len(taken), err)
	}

	claimed := claimEventually(t, streams, []string{"s1"})
	if len(claimed) != 1 || claimed[0].Command.ID != "c-abandoned" {
		t.Fatalf("claimed %v, want only the command the peer abandoned", claimed)
	}
}

// XCLAIM resets an entry's idle to zero, so an entry claimed and then handed back is one
// no instance may reclaim for a whole ClaimMinIdle, however long it had already been
// waiting. A wake that arrived late then waits the delay twice over, and the session it
// names runs nowhere for both.
func TestAClaimedCommandHandedBackKeepsItsAge(t *testing.T) {
	t.Parallel()

	const minIdle = 10 * time.Second
	f := newFleet(t)
	patient, err := redisstream.New(f.client, redisstream.Options{
		Instance: "inst-alive", Block: 50 * time.Millisecond, ClaimMinIdle: minIdle,
	})
	if err != nil {
		t.Fatalf("redisstream.New: %v", err)
	}
	dead := f.streams(t, "inst-dead")
	ctx := context.Background()

	if _, err := dead.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeCommand(t, f, f.client.Keys().Commands("s1"), command("c1", "s1", "c1"))
	taken, err := dead.Read(ctx, []string{"s1"})
	if err != nil || len(taken) != 1 {
		t.Fatalf("the first instance read %d commands (err=%v), want 1", len(taken), err)
	}
	// And then it dies, without acknowledging. The entry waits out the delay.
	clock := time.Now().UTC()
	tick := func(d time.Duration) { clock = clock.Add(d); f.server.SetTime(clock) }
	tick(minIdle + time.Second)

	claimed, err := patient.Claim(ctx, []string{"s1"})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim returned %d commands (err=%v), want 1", len(claimed), err)
	}
	// The batch ran out of its budget, so this one is handed back unrun. No time passes:
	// the whole point is that it is already old enough and must not start over.
	claimed[0].Release()
	tick(time.Millisecond)

	again, err := patient.Claim(ctx, []string{"s1"})
	if err != nil {
		t.Fatalf("the second Claim: %v", err)
	}
	if len(again) != 1 {
		t.Fatalf("a command handed back unrun came back %d times, want 1: it is waiting out the delay a second time", len(again))
	}
	if again[0].Command.ID != "c1" {
		t.Fatalf("claimed command id = %q, want c1", again[0].Command.ID)
	}
}

// An instance that took its turn at a command and could not run it gives it back the
// other way. Keeping the age puts it at the head of the pending list, which is where the
// next claim takes it from — so an instance that keeps failing on it takes it first every
// pass, spends the pass on it, and the entries behind it never get a turn at all. For a
// wake that is every other session nobody is running.
func TestAForfeitedCommandGivesUpItsPlaceInTheQueue(t *testing.T) {
	t.Parallel()

	const minIdle = 10 * time.Second
	f := newFleet(t)
	patient, err := redisstream.New(f.client, redisstream.Options{
		Instance: "inst-alive", Block: 50 * time.Millisecond, ClaimMinIdle: minIdle,
	})
	if err != nil {
		t.Fatalf("redisstream.New: %v", err)
	}
	dead := f.streams(t, "inst-dead")
	ctx := context.Background()

	if _, err := dead.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeCommand(t, f, f.client.Keys().Commands("s1"), command("c1", "s1", "c1"))
	if taken, err := dead.Read(ctx, []string{"s1"}); err != nil || len(taken) != 1 {
		t.Fatalf("the first instance read %d commands (err=%v), want 1", len(taken), err)
	}
	clock := time.Now().UTC()
	tick := func(d time.Duration) { clock = clock.Add(d); f.server.SetTime(clock) }
	tick(minIdle + time.Second)

	claimed, err := patient.Claim(ctx, []string{"s1"})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim returned %d commands (err=%v), want 1", len(claimed), err)
	}
	if claimed[0].Forfeit == nil {
		t.Fatal("a claimed command came back with no way to give up its age")
	}
	claimed[0].Forfeit()
	tick(time.Millisecond)

	// The claim erased its age and the forfeit did not put it back, so it waits out the
	// delay from here, the way a command whose instance died does.
	again, err := patient.Claim(ctx, []string{"s1"})
	if err != nil {
		t.Fatalf("the second Claim: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("a forfeited command was taken again straight away (%d), so nothing behind it moves", len(again))
	}

	// Still pending, though: forfeiting is not acknowledging, and the command runs once
	// the delay is out.
	tick(minIdle + time.Second)
	later, err := patient.Claim(ctx, []string{"s1"})
	if err != nil || len(later) != 1 {
		t.Fatalf("the third Claim returned %d commands (err=%v), want the forfeited command back", len(later), err)
	}
	if later[0].Command.ID != "c1" {
		t.Fatalf("claimed command id = %q, want c1", later[0].Command.ID)
	}
}

// A read entry starts at zero idle and nothing moves it, so one this instance gives back
// unrun is invisible to both halves of this transport: `>` returns only what no consumer
// has taken, and a claim will not look at it until a whole delay has passed. A command
// carrying a deadline shorter than that expires without ever having been tried.
func TestAReadCommandGivenBackIsReclaimableAtOnce(t *testing.T) {
	t.Parallel()

	const minIdle = 10 * time.Second
	f := newFleet(t)
	streams, err := redisstream.New(f.client, redisstream.Options{
		Instance: "inst-alive", Block: 50 * time.Millisecond, ClaimMinIdle: minIdle,
	})
	if err != nil {
		t.Fatalf("redisstream.New: %v", err)
	}
	ctx := context.Background()

	if _, err := streams.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeCommand(t, f, f.client.Keys().Commands("s1"), command("c1", "s1", "c1"))
	read, err := streams.Read(ctx, []string{"s1"})
	if err != nil || len(read) != 1 {
		t.Fatalf("Read returned %d commands (err=%v), want 1", len(read), err)
	}

	clock := time.Now().UTC()
	tick := func(d time.Duration) { clock = clock.Add(d); f.server.SetTime(clock) }

	// The batch ran out of its budget before this one was dispatched.
	read[0].Release()
	tick(time.Millisecond)

	again, err := streams.Claim(ctx, []string{"s1"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(again) != 1 {
		t.Fatalf("a read command given back unrun came back %d times, want 1: it is waiting out the whole delay", len(again))
	}
	if again[0].Command.ID != "c1" {
		t.Fatalf("claimed command id = %q, want c1", again[0].Command.ID)
	}
}

// And what is being carried out keeps its zero: putting an age back on an entry this
// process is still running is how a peer comes to run it alongside.
func TestAgeIsNotPutBackOnWhatIsStillRunning(t *testing.T) {
	t.Parallel()

	const minIdle = 10 * time.Second
	f := newFleet(t)
	patient, err := redisstream.New(f.client, redisstream.Options{
		Instance: "inst-alive", Block: 50 * time.Millisecond, ClaimMinIdle: minIdle,
	})
	if err != nil {
		t.Fatalf("redisstream.New: %v", err)
	}
	dead := f.streams(t, "inst-dead")
	ctx := context.Background()

	if _, err := dead.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeCommand(t, f, f.client.Keys().Commands("s1"), command("c1", "s1", "c1"))
	if taken, err := dead.Read(ctx, []string{"s1"}); err != nil || len(taken) != 1 {
		t.Fatalf("the first instance read %d commands (err=%v), want 1", len(taken), err)
	}
	clock := time.Now().UTC()
	tick := func(d time.Duration) { clock = clock.Add(d); f.server.SetTime(clock) }
	tick(minIdle + time.Second)

	claimed, err := patient.Claim(ctx, []string{"s1"})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim returned %d commands (err=%v), want 1", len(claimed), err)
	}
	claimed[0].Release()
	tick(time.Millisecond)

	// Taken again, and this time it is being carried out.
	running, err := patient.Claim(ctx, []string{"s1"})
	if err != nil || len(running) != 1 {
		t.Fatalf("the second Claim returned %d commands (err=%v), want 1", len(running), err)
	}
	tick(minIdle + time.Second)

	// A third pass must find nothing: the entry is running here, and the age of the
	// hand-back before it is not a reason to offer it to anybody.
	third, err := patient.Claim(ctx, []string{"s1"})
	if err != nil {
		t.Fatalf("the third Claim: %v", err)
	}
	if len(third) != 0 {
		t.Fatalf("a command being carried out was offered again (%d)", len(third))
	}
}

// A hand-back left alone for longer than the delay is one any peer may take, and XCLAIM
// transfers an entry without ever asking who holds it. Putting an age back on one
// somebody else is carrying out hands it to this instance mid-flight: the command runs
// twice, and for a wake that means retiring the only wake there was while the peer's
// adoption is still going.
func TestAgeIsNotPutBackOnWhatAPeerHasTaken(t *testing.T) {
	t.Parallel()

	const minIdle = 10 * time.Second
	f := newFleet(t)
	newStreams := func(instance string) *redisstream.Streams {
		streams, err := redisstream.New(f.client, redisstream.Options{
			Instance: instance, Block: 50 * time.Millisecond, ClaimMinIdle: minIdle,
		})
		if err != nil {
			t.Fatalf("redisstream.New: %v", err)
		}
		return streams
	}
	patient := newStreams("inst-alive")
	peer := newStreams("inst-peer")
	dead := f.streams(t, "inst-dead")
	ctx := context.Background()

	if _, err := dead.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeCommand(t, f, f.client.Keys().Commands("s1"), command("c1", "s1", "c1"))
	if taken, err := dead.Read(ctx, []string{"s1"}); err != nil || len(taken) != 1 {
		t.Fatalf("the first instance read %d commands (err=%v), want 1", len(taken), err)
	}

	clock := time.Now().UTC()
	tick := func(d time.Duration) { clock = clock.Add(d); f.server.SetTime(clock) }
	tick(minIdle + time.Second)

	claimed, err := patient.Claim(ctx, []string{"s1"})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim returned %d commands (err=%v), want 1", len(claimed), err)
	}
	claimed[0].Release()

	// And then this instance is away for longer than the delay, so a peer takes the
	// entry over and starts carrying it out.
	tick(minIdle + time.Second)
	running, err := peer.Claim(ctx, []string{"s1"})
	if err != nil || len(running) != 1 {
		t.Fatalf("the peer claimed %d commands (err=%v), want 1", len(running), err)
	}

	tick(time.Millisecond)
	back, err := patient.Claim(ctx, []string{"s1"})
	if err != nil {
		t.Fatalf("Claim after the peer took it: %v", err)
	}
	if len(back) != 0 {
		t.Fatalf("took back %d commands a peer is carrying out", len(back))
	}
}

// The check and the age update have to be one operation. A peer that claims the entry in
// between is one already carrying the command out, and an age put on it then pulls it
// back mid-flight: the command runs twice, and for a wake that means retiring the only
// wake there was while the peer's adoption is still going.
func TestAgeIsNotPutBackOnAPeerThatCameInBetween(t *testing.T) {
	t.Parallel()

	const minIdle = 10 * time.Second
	f := newFleet(t)
	rdb := redis.NewClient(&redis.Options{Addr: f.server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	hooked := redisx.Wrap(rdb, "wa:", shards)

	newStreams := func(client *redisx.Client, instance string) *redisstream.Streams {
		streams, err := redisstream.New(client, redisstream.Options{
			Instance: instance, Block: 50 * time.Millisecond, ClaimMinIdle: minIdle,
		})
		if err != nil {
			t.Fatalf("redisstream.New: %v", err)
		}
		return streams
	}
	patient := newStreams(hooked, "inst-alive")
	peer := newStreams(f.client, "inst-peer")
	dead := f.streams(t, "inst-dead")
	ctx := context.Background()

	if _, err := dead.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeCommand(t, f, f.client.Keys().Commands("s1"), command("c1", "s1", "c1"))
	if taken, err := dead.Read(ctx, []string{"s1"}); err != nil || len(taken) != 1 {
		t.Fatalf("the first instance read %d commands (err=%v), want 1", len(taken), err)
	}

	clock := time.Now().UTC()
	tick := func(d time.Duration) { clock = clock.Add(d); f.server.SetTime(clock) }
	tick(minIdle + time.Second)

	claimed, err := patient.Claim(ctx, []string{"s1"})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim returned %d commands (err=%v), want 1", len(claimed), err)
	}
	claimed[0].Release()
	tick(minIdle + time.Second)

	// From here the peer takes the entry over on the way into the restore, which is the
	// window a check done separately from the update leaves open.
	var once sync.Once
	rdb.AddHook(beforeRestore{do: func() {
		once.Do(func() {
			running, err := peer.Claim(ctx, []string{"s1"})
			if err != nil || len(running) != 1 {
				t.Errorf("the peer claimed %d commands (err=%v), want 1", len(running), err)
			}
		})
	}})

	back, err := patient.Claim(ctx, []string{"s1"})
	if err != nil {
		t.Fatalf("Claim after the peer came in: %v", err)
	}
	if len(back) != 0 {
		t.Fatalf("took back %d commands a peer had started carrying out", len(back))
	}
}

// beforeRestore runs something of the test's just before the age restore reaches Redis,
// which is the only way to put a peer inside a window that is meant not to exist.
type beforeRestore struct{ do func() }

func (beforeRestore) DialHook(next redis.DialHook) redis.DialHook { return next }

func (beforeRestore) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h beforeRestore) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if isRestore(cmd) {
			h.do()
		}
		return next(ctx, cmd)
	}
}

// isRestore is the age restore on its way out, whether it goes as a script or as the
// XCLAIM a check-then-claim would send.
func isRestore(cmd redis.Cmder) bool {
	name := cmd.Name()
	if name == "xclaim" {
		return true
	}
	if !strings.HasPrefix(name, "eval") {
		return false
	}
	for _, arg := range cmd.Args() {
		if text, ok := arg.(string); ok && strings.Contains(text, "XCLAIM") {
			return true
		}
	}
	return false
}

// A command that ran and could not be acknowledged is pending only because the
// acknowledgement did not land. Letting go of the marker that says this process is
// carrying it out has the next reclaim hand it straight back here and run it a second
// time, side effects and all.
func TestACommandWhoseAcknowledgementFailedIsNotRunAgain(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	rdb := redis.NewClient(&redis.Options{Addr: f.server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", shards)
	streams, err := redisstream.New(client, redisstream.Options{
		Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("redisstream.New: %v", err)
	}
	ctx := context.Background()

	if _, err := streams.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeCommand(t, f, f.client.Keys().Commands("s1"), command("c1", "s1", "c1"))
	taken, err := streams.Read(ctx, []string{"s1"})
	if err != nil || len(taken) != 1 {
		t.Fatalf("Read returned %d commands (err=%v), want 1", len(taken), err)
	}

	// The command runs, and the acknowledgement is what fails.
	rdb.AddHook(brokenAcks{})
	if err := taken[0].Ack(ctx); err == nil {
		t.Fatal("the acknowledgement was reported as having landed")
	}

	// Past the minimum idle, so the only thing keeping the entry from coming back is the
	// marker saying this process carried it out.
	time.Sleep(10 * time.Millisecond)
	claimed, err := streams.Claim(ctx, []string{"s1"})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("a command that already ran was handed back %d times to run again", len(claimed))
	}
}

// brokenAcks fails every acknowledgement, which is a Redis having a bad second between a
// command finishing and its entry being retired.
type brokenAcks struct{}

func (brokenAcks) DialHook(next redis.DialHook) redis.DialHook { return next }

func (brokenAcks) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (brokenAcks) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "xack" {
			err := errors.New("the acknowledgement never landed")
			cmd.SetErr(err)
			return err
		}
		return next(ctx, cmd)
	}
}

// The split backpressure makes between a command whose sender is still waiting and one
// whose sender has gone is only as good as this flag: a claim is a command taken over
// from a holder that died or lost the session, a `>` read is one nobody has touched.
func TestADeliverySaysWhetherItWasTakenOverOrReadFresh(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	owner := f.streams(t, "inst-a")
	ctx := context.Background()

	if _, err := owner.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeCommand(t, f, f.client.Keys().Commands("s1"), command("c1", "s1", "c1"))

	fresh, err := owner.Read(ctx, []string{"s1"})
	if err != nil || len(fresh) != 1 {
		t.Fatalf("Read returned %d commands (err=%v), want 1", len(fresh), err)
	}
	if fresh[0].Redelivered {
		t.Error("a command read with `>` is marked as taken over, so backpressure would leave a caller waiting")
	}

	// Left pending by that reader, which is what a killed instance leaves behind.
	peer := f.streams(t, "inst-b")
	taken := claimEventually(t, peer, []string{"s1"})
	if len(taken) != 1 {
		t.Fatalf("the peer claimed %d commands, want 1", len(taken))
	}
	if !taken[0].Redelivered {
		t.Error("a claimed command is not marked as taken over, so backpressure could retire the only copy of it")
	}

	// The drain's claim takes without waiting out the delay, and says the same thing.
	writeCommand(t, f, f.client.Keys().Commands("s2"), command("c2", "s2", "c2"))
	if _, err := owner.Read(ctx, []string{"s2"}); err != nil {
		t.Fatalf("Read s2: %v", err)
	}
	adopted, err := peer.ClaimSessions(ctx, []string{"s2"})
	if err != nil {
		t.Fatalf("ClaimSessions: %v", err)
	}
	if len(adopted) != 1 || !adopted[0].Redelivered {
		t.Fatalf("the drain claimed %d commands, redelivered=%v; want 1 marked as taken over",
			len(adopted), len(adopted) == 1 && adopted[0].Redelivered)
	}
}

// RedisEnv names a real Redis for the one pass miniredis cannot stand in for. The double
// answers zero for `entries-added` and `entries-read`, which are the counters the trim
// report is built on, so against it the report is correctly silent and proves nothing
// about the case it exists for.
const RedisEnv = "WAC_TEST_REDIS_URL"

// realFleet is the fleet against the server RedisEnv names, under a prefix of its own so
// a run leaves nothing behind for the next one.
func realFleet(t *testing.T) fleet {
	t.Helper()
	url := os.Getenv(RedisEnv)
	if url == "" {
		t.Skipf("set %s to run this against a real Redis (see 'make test-redis')", RedisEnv)
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse %s: %v", RedisEnv, err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	prefix := "wactest:" + strconv.FormatInt(time.Now().UnixNano(), 36) + ":"
	t.Cleanup(func() {
		keys, err := rdb.Keys(context.Background(), prefix+"*").Result()
		if err == nil && len(keys) > 0 {
			_ = rdb.Del(context.Background(), keys...).Err()
		}
	})
	return fleet{rdb: rdb, client: redisx.Wrap(rdb, prefix, shards)}
}

// A client writes commands with `MAXLEN ~`, so Redis drops the oldest entries on its own
// and the consumer group is never consulted: the publisher got an id back, the group goes
// on from its last-delivered-id, and the next read simply returns what survived. Nothing
// on either side reports the commands cut in between, and `lag` even falls as if the work
// had been done.
//
// The numbers are the ones measured in #176: ten commands, three delivered, trimmed to
// two, five lost. Against a real server, because the counters that answer this are
// `entries-added` and `entries-read` and miniredis reports neither.
func TestATrimThatCutUndeliveredCommandsIsSaidOutLoud(t *testing.T) {
	t.Parallel()

	f := realFleet(t)
	ctx := context.Background()
	stream := f.client.Keys().Commands("s1")
	// An earlier owner read what there was and stopped there, which is where the group's
	// last-delivered-id stays.
	for i := range 3 {
		writeCommand(t, f, stream, command("c"+strconv.Itoa(i), "s1", ""))
	}
	if _, err := f.streams(t, "inst-a").Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("the first read: %v", err)
	}
	// The client went on writing while nobody was reading, and the cap took the ones
	// nobody had been handed.
	for i := 3; i < 10; i++ {
		writeCommand(t, f, stream, command("c"+strconv.Itoa(i), "s1", ""))
	}
	if err := f.rdb.XTrimMaxLen(ctx, stream, 2).Err(); err != nil {
		t.Fatalf("XTrimMaxLen: %v", err)
	}

	// A different instance adopts the session, which is the moment this is asked.
	written := &bytes.Buffer{}
	if _, err := f.streamsLogging(t, "inst-b", written).Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("the read after the trim: %v", err)
	}

	logged := written.String()
	if !strings.Contains(logged, "trimmed out of this stream before they were delivered") {
		t.Fatalf("the cut was not reported anywhere; the log was:\n%s", logged)
	}
	// The count and the stream, because an operator reading this has to know how much was
	// lost and which account lost it.
	if !strings.Contains(logged, `"commands_lost":5`) {
		t.Fatalf("the report does not say five commands were lost; the log was:\n%s", logged)
	}
	if !strings.Contains(logged, stream) {
		t.Fatalf("the report does not name the stream; the log was:\n%s", logged)
	}
}

// And the ordinary trim, the one that takes only what the group already read, says
// nothing. This is the case the obvious heuristic gets wrong: the oldest surviving entry
// is later than the last delivered one here too, exactly as in a real cut.
func TestATrimThatTookOnlyWhatWasDeliveredIsNotReported(t *testing.T) {
	t.Parallel()

	f := realFleet(t)
	ctx := context.Background()
	stream := f.client.Keys().Commands("s1")
	for i := range 3 {
		writeCommand(t, f, stream, command("c"+strconv.Itoa(i), "s1", ""))
	}
	if _, err := f.streams(t, "inst-a").Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("the first read: %v", err)
	}
	for i := 3; i < 10; i++ {
		writeCommand(t, f, stream, command("c"+strconv.Itoa(i), "s1", ""))
	}
	// Seven left, which is everything the group has not been handed.
	if err := f.rdb.XTrimMaxLen(ctx, stream, 7).Err(); err != nil {
		t.Fatalf("XTrimMaxLen: %v", err)
	}

	written := &bytes.Buffer{}
	if _, err := f.streamsLogging(t, "inst-b", written).Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if logged := written.String(); strings.Contains(logged, "trimmed out") {
		t.Fatalf("an ordinary trim was reported as a loss:\n%s", logged)
	}
}

// A server that cannot answer the counters must produce silence and not a clean bill.
// miniredis is exactly that server -- it reports zero for both -- which makes it the
// fixture for this and useless for the two above.
func TestAServerThatCannotAnswerTheCountersIsNotCalledClean(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	ctx := context.Background()
	stream := f.client.Keys().Commands("s1")
	for i := range 3 {
		writeCommand(t, f, stream, command("c"+strconv.Itoa(i), "s1", ""))
	}
	if _, err := f.streams(t, "inst-a").Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("the first read: %v", err)
	}
	for i := 3; i < 10; i++ {
		writeCommand(t, f, stream, command("c"+strconv.Itoa(i), "s1", ""))
	}
	if err := f.rdb.XTrimMaxLen(ctx, stream, 2).Err(); err != nil {
		t.Fatalf("XTrimMaxLen: %v", err)
	}

	written := &bytes.Buffer{}
	if _, err := f.streamsLogging(t, "inst-b", written).Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("Read: %v", err)
	}
	// Silence, and the read still works: the diagnosis is never allowed to fail the read
	// it runs in front of.
	if logged := written.String(); strings.Contains(logged, "trimmed out") {
		t.Fatalf("a server that cannot count reported a loss it cannot know about:\n%s", logged)
	}
}
