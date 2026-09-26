package redisx_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx/redisxtest"
)

func lagBench(t *testing.T, shards int) (raw *redis.Client, client *redisx.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb, redisx.Wrap(rdb, "wa:", shards)
}

// A client that has not read what the connector published is the thing none of the six
// existing metrics can see, and the whole reason this collector exists.
func TestAClientBehindOnAShardIsReportedAsLag(t *testing.T) {
	t.Parallel()

	rdb, client := lagBench(t, 1)
	ctx := context.Background()
	if err := rdb.XGroupCreateMkStream(ctx, "wa:events:0", "chatwoot", "0").Err(); err != nil {
		t.Fatalf("group: %v", err)
	}
	for range 3 {
		if err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "wa:events:0", Values: map[string]any{"type": "message.received"},
		}).Err(); err != nil {
			t.Fatalf("xadd: %v", err)
		}
	}

	want := `
# HELP wac_stream_lag Entries added to an event shard that this consumer group has not read yet.
# TYPE wac_stream_lag gauge
wac_stream_lag{group="chatwoot",stream="wa:events:0"} 3
`
	if err := testutil.CollectAndCompare(
		redisx.NewStreamLag(client), strings.NewReader(want), "wac_stream_lag",
	); err != nil {
		t.Fatalf("lag: %v", err)
	}
}

// Every group on the shard is reported, because the connector never creates the client's
// group and has no list of the ones that should be there: a second reader nobody
// remembered is exactly what this has to be able to show.
func TestEveryGroupOnAShardIsReported(t *testing.T) {
	t.Parallel()

	rdb, client := lagBench(t, 1)
	ctx := context.Background()
	for _, group := range []string{"chatwoot", "an-archiver-nobody-remembered"} {
		if err := rdb.XGroupCreateMkStream(ctx, "wa:events:0", group, "0").Err(); err != nil {
			t.Fatalf("group %s: %v", group, err)
		}
	}
	if err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "wa:events:0", Values: map[string]any{"type": "message.received"},
	}).Err(); err != nil {
		t.Fatalf("xadd: %v", err)
	}

	want := `
# HELP wac_stream_lag Entries added to an event shard that this consumer group has not read yet.
# TYPE wac_stream_lag gauge
wac_stream_lag{group="an-archiver-nobody-remembered",stream="wa:events:0"} 1
wac_stream_lag{group="chatwoot",stream="wa:events:0"} 1
`
	if err := testutil.CollectAndCompare(
		redisx.NewStreamLag(client), strings.NewReader(want), "wac_stream_lag",
	); err != nil {
		t.Fatalf("groups: %v", err)
	}
}

// A shard nothing has published to yet has no stream and no groups, and the collector
// says it could not read it rather than inventing a zero for a client that may be fine.
func TestAShardWithNoStreamYetIsReportedAsUnread(t *testing.T) {
	t.Parallel()

	rdb, client := lagBench(t, 2)
	ctx := context.Background()
	if err := rdb.XGroupCreateMkStream(ctx, "wa:events:0", "chatwoot", "0").Err(); err != nil {
		t.Fatalf("group: %v", err)
	}

	want := `
# HELP wac_stream_scrape_failed 1 when the group information for this shard could not be read.
# TYPE wac_stream_scrape_failed gauge
wac_stream_scrape_failed{stream="wa:events:0"} 0
wac_stream_scrape_failed{stream="wa:events:1"} 1
`
	if err := testutil.CollectAndCompare(
		redisx.NewStreamLag(client), strings.NewReader(want), "wac_stream_scrape_failed",
	); err != nil {
		t.Fatalf("scrape: %v", err)
	}
}

// Entries taken and not acknowledged are what separates "the fleet is busy" from "one
// reader took work and stopped answering".
func TestEntriesTakenAndNotAcknowledgedAreReportedAsPending(t *testing.T) {
	t.Parallel()

	rdb, client := lagBench(t, 1)
	ctx := context.Background()
	if err := rdb.XGroupCreateMkStream(ctx, "wa:events:0", "chatwoot", "0").Err(); err != nil {
		t.Fatalf("group: %v", err)
	}
	for range 2 {
		if err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "wa:events:0", Values: map[string]any{"type": "message.received"},
		}).Err(); err != nil {
			t.Fatalf("xadd: %v", err)
		}
	}
	if err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: "chatwoot", Consumer: "reader-1", Streams: []string{"wa:events:0", ">"}, Count: 2,
	}).Err(); err != nil {
		t.Fatalf("xreadgroup: %v", err)
	}

	want := `
# HELP wac_stream_pending Entries this consumer group has taken and not acknowledged.
# TYPE wac_stream_pending gauge
wac_stream_pending{group="chatwoot",stream="wa:events:0"} 2
`
	if err := testutil.CollectAndCompare(
		redisx.NewStreamLag(client), strings.NewReader(want), "wac_stream_pending",
	); err != nil {
		t.Fatalf("pending: %v", err)
	}
}

// Redis before 7.0 reports no lag for a group, and the group is behind or not for all
// anybody can tell (#320). What it cannot be is "up to date": no lag sample, and the
// unknown marker set, with what the server did report still published beside it.
func TestAServerThatReportsNoLagIsReportedAsUnknown(t *testing.T) {
	t.Parallel()

	stub := redisxtest.NewStub(t, helloMap("server", "redis", "version", "6.2.24", "proto", "3"))
	// XINFO GROUPS as 6.2 answers it: no entries-read, no lag.
	stub.Answer("XINFO", "*1\r\n%4\r\n"+bulks([]string{"name", "chatwoot"})+
		bulks([]string{"consumers"})+":1\r\n"+bulks([]string{"pending"})+":2\r\n"+
		bulks([]string{"last-delivered-id", "1-0"}))
	rdb := redis.NewClient(&redis.Options{Addr: stub.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })

	want := `
# HELP wac_stream_lag_unknown 1 when Redis could not compute the lag for this group, in which case wac_stream_lag is not reported at all.
# TYPE wac_stream_lag_unknown gauge
wac_stream_lag_unknown{group="chatwoot",stream="wa:events:0"} 1
# HELP wac_stream_pending Entries this consumer group has taken and not acknowledged.
# TYPE wac_stream_pending gauge
wac_stream_pending{group="chatwoot",stream="wa:events:0"} 2
`
	if err := testutil.CollectAndCompare(
		redisx.NewStreamLag(redisx.Wrap(rdb, "wa:", 1)), strings.NewReader(want),
		"wac_stream_lag", "wac_stream_lag_unknown", "wac_stream_pending",
	); err != nil {
		t.Fatalf("a group on a server with no lag field: %v", err)
	}
}

// The same reading against the server `make test-redis` names, which CI points at the
// newest Redis and at 6.2: a lag where the server gives one, and unknown where it does not.
func TestTheLagOfARealServerIsReportedOnlyWhereItHasOne(t *testing.T) {
	t.Parallel()

	url := os.Getenv("WAC_TEST_REDIS_URL")
	if url == "" {
		t.Skip("set WAC_TEST_REDIS_URL to run this against a real Redis (see 'make test-redis')")
	}
	client, err := redisx.New(redisx.Config{URL: url, Prefix: "wactest:" + t.Name() + ":" + strconv.FormatInt(time.Now().UnixNano(), 36), Shards: 1})
	if err != nil {
		t.Fatalf("redisx.New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	stream := client.Keys().Events(0)
	t.Cleanup(func() { _ = client.Del(context.Background(), stream).Err() })
	if err := client.XGroupCreateMkStream(ctx, stream, "chatwoot", "0").Err(); err != nil {
		t.Fatalf("group: %v", err)
	}
	for range 3 {
		if err := client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"type": "message.received"}}).Err(); err != nil {
			t.Fatalf("xadd: %v", err)
		}
	}
	version, found, err := client.ServerVersion(ctx)
	if err != nil || !found {
		t.Fatalf("ask the server its version: %q, found %v, err %v", version, found, err)
	}

	want := fmt.Sprintf(`
# HELP wac_stream_lag Entries added to an event shard that this consumer group has not read yet.
# TYPE wac_stream_lag gauge
wac_stream_lag{group="chatwoot",stream=%q} 3
# HELP wac_stream_lag_unknown 1 when Redis could not compute the lag for this group, in which case wac_stream_lag is not reported at all.
# TYPE wac_stream_lag_unknown gauge
wac_stream_lag_unknown{group="chatwoot",stream=%q} 0
`, stream, stream)
	if major, _, _ := strings.Cut(version, "."); major == "6" {
		want = fmt.Sprintf(`
# HELP wac_stream_lag_unknown 1 when Redis could not compute the lag for this group, in which case wac_stream_lag is not reported at all.
# TYPE wac_stream_lag_unknown gauge
wac_stream_lag_unknown{group="chatwoot",stream=%q} 1
`, stream)
	}
	if err := testutil.CollectAndCompare(
		redisx.NewStreamLag(client), strings.NewReader(want), "wac_stream_lag", "wac_stream_lag_unknown",
	); err != nil {
		t.Fatalf("Redis %s: %v", version, err)
	}
}
