package redisx_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

func newTestClient(t *testing.T, shards int) *redisx.Client {
	t.Helper()
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return redisx.Wrap(rdb, "wa:", shards)
}

func TestKeysNormalisePrefix(t *testing.T) {
	t.Parallel()

	for _, prefix := range []string{"wa", "wa:", ""} {
		keys := redisx.NewKeys(prefix, 8)
		if got := keys.Prefix(); got[len(got)-1] != ':' {
			t.Fatalf("prefix %q rendered as %q, want a trailing separator", prefix, got)
		}
	}
	if got := redisx.NewKeys("wa", 8).Meta(); got != "wa:meta" {
		t.Fatalf("Meta() = %q, want wa:meta", got)
	}
}

// A session always lands on the same stream, and both sides compute it the same
// way. Ordering rests on it, so the values are pinned rather than merely asserted
// to be stable within a run.
func TestShardOfIsStable(t *testing.T) {
	t.Parallel()

	keys := redisx.NewKeys("wa:", 8)
	want := map[string]int{
		"2f1c6f0e-0000-4000-8000-000000000001": 5,
		"2f1c6f0e-0000-4000-8000-000000000002": 4,
		"a-session":                            5,
	}
	for sid, shard := range want {
		if got := keys.ShardOf(sid); got != shard {
			t.Errorf("ShardOf(%q) = %d, want %d", sid, got, shard)
		}
	}
	for sid := range want {
		if got, second := keys.ShardOf(sid), keys.ShardOf(sid); got != second {
			t.Errorf("ShardOf(%q) is not deterministic: %d then %d", sid, got, second)
		}
	}
}

func TestShardOfStaysInRange(t *testing.T) {
	t.Parallel()

	keys := redisx.NewKeys("wa:", 4)
	for _, sid := range []string{"", "a", "session-with-a-much-longer-identifier", "🙂"} {
		if got := keys.ShardOf(sid); got < 0 || got > 3 {
			t.Errorf("ShardOf(%q) = %d, outside [0,3]", sid, got)
		}
	}
}

func TestNewRejectsZeroShards(t *testing.T) {
	t.Parallel()

	if _, err := redisx.New(redisx.Config{Shards: 0}); err == nil {
		t.Fatal("New with zero shards returned no error")
	}
}

func TestClaimMetaWritesTheFleetFacts(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, 8)
	ctx := context.Background()

	if err := client.ClaimMeta(ctx, redisx.Meta{ProtocolMin: 1, ProtocolMax: 1, Shards: 8}); err != nil {
		t.Fatalf("ClaimMeta: %v", err)
	}

	stored, err := client.HGetAll(ctx, client.Keys().Meta()).Result()
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	for field, want := range map[string]string{"protocol_min": "1", "protocol_max": "1", "event_shards": "8"} {
		if stored[field] != want {
			t.Errorf("meta[%q] = %q, want %q", field, stored[field], want)
		}
	}
}

// The dangerous case: the fleet is already publishing to a different number of
// streams, so this instance would hash sessions onto streams nobody reads them from.
func TestClaimMetaRefusesADifferentShardCount(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, 8)
	ctx := context.Background()

	if err := client.ClaimMeta(ctx, redisx.Meta{ProtocolMin: 1, ProtocolMax: 1, Shards: 8}); err != nil {
		t.Fatalf("first ClaimMeta: %v", err)
	}

	err := client.ClaimMeta(ctx, redisx.Meta{ProtocolMin: 1, ProtocolMax: 1, Shards: 16})
	if !errors.Is(err, redisx.ErrShardMismatch) {
		t.Fatalf("ClaimMeta with a different shard count = %v, want ErrShardMismatch", err)
	}
}

// A second instance of the same fleet is the ordinary case and must not be refused.
func TestClaimMetaAcceptsTheSameShardCount(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, 8)
	ctx := context.Background()

	if err := client.ClaimMeta(ctx, redisx.Meta{ProtocolMin: 1, ProtocolMax: 1, Shards: 8}); err != nil {
		t.Fatalf("first ClaimMeta: %v", err)
	}
	if err := client.ClaimMeta(ctx, redisx.Meta{ProtocolMin: 1, ProtocolMax: 2, Shards: 8}); err != nil {
		t.Fatalf("second ClaimMeta: %v", err)
	}

	advertised, err := client.HGet(ctx, client.Keys().Meta(), "protocol_max").Result()
	if err != nil {
		t.Fatalf("read protocol_max: %v", err)
	}
	if advertised != "2" {
		t.Errorf("protocol_max = %q, want the range of the instance that wrote last", advertised)
	}
}

func TestPingReachesTheServer(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, 8)
	if err := client.Ping(context.Background(), time.Second); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

// A command with no result still ran, and answering that is the point: `null` and
// "never happened" are the same bytes on the way out and not the same thing.
func TestARememberedCommandWithNoResultIsStillRemembered(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, 8)
	ledger := redisx.NewIdempotency(client, time.Minute)
	ctx := context.Background()

	if _, found, err := ledger.Recall(ctx, "s1", "msg:3EB0"); err != nil || found {
		t.Fatalf("a command nobody ran came back as found=%v, err=%v", found, err)
	}
	if err := ledger.Remember(ctx, "s1", "msg:3EB0", nil); err != nil {
		t.Fatalf("Remember: %v", err)
	}
	result, found, err := ledger.Recall(ctx, "s1", "msg:3EB0")
	if err != nil {
		t.Fatalf("Recall: %v", err)
	}
	if !found {
		t.Fatal("a command that was remembered came back as one that never ran")
	}
	if len(result) != 0 {
		t.Fatalf("a command with no result came back carrying %s", result)
	}
}

// The first run is the one every redelivery has to be answered with, or two answers to
// one command disagree about what it did.
func TestRememberingACommandTwiceKeepsTheFirstAnswer(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, 8)
	ledger := redisx.NewIdempotency(client, time.Minute)
	ctx := context.Background()

	if err := ledger.Remember(ctx, "s1", "msg:3EB0", json.RawMessage(`{"timestamp":1}`)); err != nil {
		t.Fatalf("Remember: %v", err)
	}
	if err := ledger.Remember(ctx, "s1", "msg:3EB0", json.RawMessage(`{"timestamp":2}`)); err != nil {
		t.Fatalf("Remember again: %v", err)
	}

	result, _, err := ledger.Recall(ctx, "s1", "msg:3EB0")
	if err != nil {
		t.Fatalf("Recall: %v", err)
	}
	if string(result) != `{"timestamp":1}` {
		t.Fatalf("the second answer overwrote the first: %s", result)
	}
}

// One session's record must not answer another's: the key is the caller's message id,
// and two sessions naming the same message are two different sends.
func TestARecordBelongsToOneSession(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, 8)
	ledger := redisx.NewIdempotency(client, time.Minute)
	ctx := context.Background()

	if err := ledger.Remember(ctx, "s1", "msg:3EB0", json.RawMessage(`{"timestamp":1}`)); err != nil {
		t.Fatalf("Remember: %v", err)
	}
	if _, found, err := ledger.Recall(ctx, "s2", "msg:3EB0"); err != nil || found {
		t.Fatalf("another session's send came back as already done (found=%v, err=%v)", found, err)
	}
}
