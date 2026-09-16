package redisx_test

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
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
