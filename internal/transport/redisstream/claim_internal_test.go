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
	if err := streams.groups.ensure(ctx, client, []string{first, second}); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := client.Del(ctx, second).Err(); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if err := client.Set(ctx, second, "not a stream", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	claimed, err := streams.claim(ctx, []string{first, second}, time.Millisecond, 0)
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
