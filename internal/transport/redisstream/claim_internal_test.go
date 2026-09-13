package redisstream

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx/redisxtest"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// A claim that walks several streams and fails partway has already handed some
// deliveries out, and handing one out is what marks it as work this process is doing.
// Returning nil over them leaves those entries unclaimable here for the life of the
// process while nothing ever dispatched them: in a fleet of one, the command is gone.
func TestAFailedClaimLetsGoOfWhatItAlreadyTook(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	ctx := context.Background()

	streams, err := New(client, Options{Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: time.Millisecond, ReadCount: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dead, err := New(client, Options{Instance: "inst-dead", Block: 50 * time.Millisecond, ClaimMinIdle: time.Millisecond, ReadCount: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	first, second := client.Keys().Commands("s1"), client.Keys().Commands("s2")

	// A command a peer abandoned on the first stream, which is what the claim picks up
	// before it reaches the second.
	if _, err := dead.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeOne(t, client, first, "s1")
	if taken, err := dead.Read(ctx, []string{"s1"}); err != nil || len(taken) != 1 {
		t.Fatalf("the peer read %d commands (err=%v), want 1", len(taken), err)
	}

	// The second stream is primed so its group is cached, and then replaced by a value
	// that is not a stream at all. That is what a Redis answering wrongly partway
	// through a pass looks like from in here, without a sleep or a race to arrange it.
	if _, err := streams.groups.ensure(ctx, client, []string{first, second}); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := client.Del(ctx, second).Err(); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if err := client.Set(ctx, second, "not a stream", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	claimed, err := streams.claim(ctx, []string{first, second}, time.Millisecond)
	if err == nil {
		t.Fatal("the claim reported success over a stream it could not read")
	}
	if !strings.Contains(err.Error(), "s2") {
		t.Fatalf("the error names %v, want the stream that failed", err)
	}
	if claimed != nil {
		t.Fatalf("a failed claim handed back %d deliveries", len(claimed))
	}

	streams.inFlightMu.Lock()
	held := len(streams.inFlight)
	streams.inFlightMu.Unlock()
	if held != 0 {
		t.Fatalf("a failed claim is still holding %d deliveries nobody will ever dispatch", held)
	}
}

func writeOne(t *testing.T, client *redisx.Client, stream, sid string) {
	t.Helper()

	command := &protocol.Command{
		V: protocol.Version, ID: "c-" + sid, Type: protocol.CommandSessionStatus,
		SID: sid, TS: 1787000000000, Payload: []byte(`{}`),
	}
	fields, err := command.Fields()
	if err != nil {
		t.Fatalf("render command: %v", err)
	}
	values := make(map[string]any, len(fields))
	for key, value := range fields {
		values[key] = value
	}
	if err := client.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: values}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}
}

// What a claim hands out past the mark is kept apart only while it is still pending here and
// no page has been read past it. Acknowledged, it is off the pending list and no page will
// ever carry it; passed by a page, the mark keeps it off every later page by itself. Either
// way it is forgotten, or an instance that claims for weeks holds every id it ever claimed.
func TestAClaimedEntryIsForgottenOnceAckedOrPassed(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	ctx := context.Background()
	stream := client.Keys().Commands("s1")

	a, err := New(client, Options{Instance: "inst-a", Block: 50 * time.Millisecond, ReadCount: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dead, err := New(client, Options{Instance: "inst-dead", Block: 50 * time.Millisecond, ReadCount: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, streams := range []*Streams{a, dead} {
		if _, err := streams.Read(ctx, []string{"s1"}); err != nil {
			t.Fatalf("priming Read: %v", err)
		}
	}
	abandon := func() {
		t.Helper()
		writeOne(t, client, stream, "s1")
		if taken, err := dead.Read(ctx, []string{"s1"}); err != nil || len(taken) != 1 {
			t.Fatalf("the peer read %d commands (err=%v), want 1", len(taken), err)
		}
	}
	claimOne := func() transport.Delivery {
		t.Helper()
		claimed, err := a.claim(ctx, []string{stream}, 0)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("claimed %d (err=%v), want the peer's command", len(claimed), err)
		}
		if kept := len(a.claimedPast[stream]); kept != 1 {
			t.Fatalf("%d claimed entries kept apart, want the one claimed past the mark", kept)
		}
		return claimed[0]
	}

	abandon()
	acked := claimOne()
	if err := acked.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if kept := len(a.claimedPast[stream]); kept != 0 {
		t.Fatalf("%d claimed entries still kept after the ack, want none", kept)
	}

	// Acknowledged while a page is on its way, it stays apart until that page is looked at:
	// the page may carry it.
	abandon()
	acked = claimOne()
	a.marksMu.Lock()
	a.pagesOut++
	a.marksMu.Unlock()
	if err := acked.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if kept := len(a.claimedPast[stream]); kept != 1 {
		t.Fatalf("%d claimed entries kept after an ack with a page out, want the one acknowledged", kept)
	}
	a.marksMu.Lock()
	a.pagesOut--
	a.marksMu.Unlock()
	if _, err := a.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if kept := len(a.claimedPast[stream]); kept != 0 {
		t.Fatalf("%d claimed entries still kept once no page was out, want none", kept)
	}

	abandon()
	claimOne().Release()
	writeOne(t, client, stream, "s1")
	if read, err := a.Read(ctx, []string{"s1"}); err != nil || len(read) != 1 {
		t.Fatalf("read %d commands (err=%v), want the newer one and not the one given back", len(read), err)
	}
	if kept := len(a.claimedPast[stream]); kept != 0 {
		t.Fatalf("%d claimed entries still kept after a page was read past them, want none", kept)
	}
}

// Which claimed entries a page skips and which it has passed both turn on this comparison,
// and an id is two numbers: compared as text, 1-10 comes before 1-9, and a claim of 1-10
// past a mark at 1-9 would not be kept apart from the page that reads it.
func TestEntryIDsCompareAsNumbers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		a, b  string
		after bool
	}{
		{"1-10", "1-9", true},
		{"1-9", "1-10", false},
		{"10-0", "9-5", true},
		{"9-5", "10-0", false},
		{"1-0", "1-0", false},
		{"1-0", "", true},
	} {
		if got := entryAfter(tc.a, tc.b); got != tc.after {
			t.Errorf("entryAfter(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.after)
		}
	}
}

// What `>` answered with is kept only as a fallback for an entry trimmed before a page reads
// it back. A page that passes it, or a read that no longer reads its stream, lets it go, or
// an instance holds every command it ever read.
func TestAReceivedPayloadIsForgottenOncePassedOrNoLongerRead(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	ctx := context.Background()

	a, err := New(client, Options{Instance: "inst-a", Block: 50 * time.Millisecond, ReadCount: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, sid := range []string{"s1", "s2"} {
		if _, err := a.Read(ctx, []string{sid}); err != nil {
			t.Fatalf("priming Read: %v", err)
		}
	}
	first, second := client.Keys().Commands("s1"), client.Keys().Commands("s2")

	writeOne(t, client, first, "s1")
	if read, err := a.Read(ctx, []string{"s1"}); err != nil || len(read) != 1 {
		t.Fatalf("read %d commands (err=%v), want 1", len(read), err)
	}
	if kept := len(a.received[first]); kept != 0 {
		t.Fatalf("%d payloads kept after the page that read them back, want none", kept)
	}

	// A read whose answer never arrived, as far as this process knows: the command is pending
	// here past the mark. The next read's page is that one, while its `>` carries a newer
	// command, whose payload stays kept.
	writeOne(t, client, second, "s2")
	if err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: ConsumerGroup, Consumer: "inst-a", Streams: []string{second, ">"}, Count: 1, Block: -1,
	}).Err(); err != nil {
		t.Fatalf("XREADGROUP: %v", err)
	}
	writeOne(t, client, second, "s2")
	if read, err := a.Read(ctx, []string{"s2"}); err != nil || len(read) != 1 {
		t.Fatalf("read %d commands (err=%v), want the older one", len(read), err)
	}
	if kept := len(a.received[second]); kept != 1 {
		t.Fatalf("%d payloads kept, want the one no page has passed yet", kept)
	}
	if _, err := a.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if _, kept := a.received[second]; kept {
		t.Fatal("payloads still kept for a stream the read no longer reads")
	}
}

// A claim keeps apart every entry it asks for, and XCLAIM leaves out one a peer acknowledged
// in between. Nothing acknowledges that one here and no page carries it, so kept apart it
// would stay for good; a fleet racing its claims over the control stream would pile them up.
// What XCLAIM left out and is still pending here is another matter: a claim moved it without
// hearing so, and it stays apart.
func TestAnEntryAPeerRetiredBeforeTheClaimTookItIsNotKeptApart(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	direct := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = direct.Close() })
	proxy := redisxtest.Listen(t, server.Addr())
	via := redis.NewClient(&redis.Options{Addr: proxy.Addr()})
	t.Cleanup(func() { _ = via.Close() })
	client, proxied := redisx.Wrap(direct, "wa:", 8), redisx.Wrap(via, "wa:", 8)
	ctx := context.Background()
	stream := client.Keys().Commands("s1")

	a, err := New(proxied, Options{Instance: "inst-a", Block: 50 * time.Millisecond, ReadCount: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dead, err := New(client, Options{Instance: "inst-dead", Block: 50 * time.Millisecond, ReadCount: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, streams := range []*Streams{a, dead} {
		if _, err := streams.Read(ctx, []string{"s1"}); err != nil {
			t.Fatalf("priming Read: %v", err)
		}
	}
	command := &protocol.Command{
		V: protocol.Version, ID: "retired", Type: protocol.CommandSessionStatus,
		SID: "s1", TS: 1787000000000, Payload: []byte(`{}`),
	}
	fields, err := command.Fields()
	if err != nil {
		t.Fatalf("render command: %v", err)
	}
	values := make(map[string]any, len(fields))
	for key, value := range fields {
		values[key] = value
	}
	const id = "7-1"
	if err := client.XAdd(ctx, &redis.XAddArgs{Stream: stream, ID: id, Values: values}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}
	if taken, err := dead.Read(ctx, []string{"s1"}); err != nil || len(taken) != 1 {
		t.Fatalf("the peer read %d commands (err=%v), want 1", len(taken), err)
	}

	// The list of what is pending is held, and the peer acknowledges the entry before the
	// claim sends XCLAIM for it.
	release := make(chan struct{})
	caught := proxy.Hold(id, release)
	type result struct {
		claimed []transport.Delivery
		err     error
	}
	done := make(chan result, 1)
	go func() {
		claimed, err := a.ClaimSessions(ctx, []string{"s1"})
		done <- result{claimed, err}
	}()
	<-caught
	if err := client.XAck(ctx, stream, ConsumerGroup, id).Err(); err != nil {
		t.Fatalf("XAck: %v", err)
	}
	close(release)
	if got := <-done; got.err != nil || len(got.claimed) != 0 {
		t.Fatalf("claimed %d (err=%v), want nothing: the peer retired it", len(got.claimed), got.err)
	}
	a.marksMu.Lock()
	kept := len(a.claimedPast[stream])
	a.marksMu.Unlock()
	if kept != 0 {
		t.Fatalf("%d entries kept apart after a claim that took nothing still pending here, want none", kept)
	}
}
