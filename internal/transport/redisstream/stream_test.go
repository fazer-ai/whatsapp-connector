package redisstream_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
	"github.com/fazer-ai/whatsapp-connector/internal/transport/redisstream"
)

const shards = 8

type fleet struct {
	server *miniredis.Miniredis
	client *redisx.Client
}

func newFleet(t *testing.T) fleet {
	t.Helper()
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return fleet{server: server, client: redisx.Wrap(rdb, "wa:", shards)}
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

	if err := f.streams(t, "inst-a").Reply(ctx, "c1", reply); err != nil {
		t.Fatalf("Reply: %v", err)
	}

	key := f.client.Keys().Reply("c1")
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
// entry this connector leaves pending on purpose. Claiming only the owned sessions
// would never look at the stream it is on.
func TestClaimLooksAtControlToo(t *testing.T) {
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
	claimed := claimEventually(t, alive, nil)
	if len(claimed) != 1 || claimed[0].Command.ID != "c-wake" {
		t.Fatalf("Claim returned %v, want the pending wake", claimed)
	}
}

// claimEventually retries until the pending entry is older than the min-idle. Nothing
// about the wait is the behaviour under test: an entry is only claimable once it has
// been idle long enough, and asserting on the first attempt would be asserting on how
// fast the test itself ran.
func claimEventually(t *testing.T, streams *redisstream.Streams, sids []string) []transport.Delivery {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		claimed, err := streams.Claim(context.Background(), sids)
		if err != nil {
			t.Fatalf("Claim: %v", err)
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
