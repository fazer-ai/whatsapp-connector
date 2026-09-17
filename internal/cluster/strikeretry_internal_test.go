package cluster

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// A strike that ran twice for one attempt counts once, and this is the seam that decides it.
//
// go-redis sends a command again when its answer never comes back, and it sends the same
// command: a script that reached the server, ran, and lost its reply runs a second time
// under the same arguments. The connector cannot see it happen -- the retry succeeds, so one
// failed adoption is logged once and recorded twice, and the wait is already doubled. It was
// measured on the wire by the holdout's proxy, one failed adoption producing `strikes=2` on
// this build and on the one before it, which is how it got found at all: it is not something
// the issue named and not something a hook can reproduce, because go-redis retries below the
// level hooks wrap.
//
// What a test can pin is the invariant that makes the retry harmless, and that is what this
// does. Two runs carrying one attempt's token are one strike; the wire is what decides how
// often that happens, and the token is what decides what it costs.
func TestOneAttemptRunTwiceIsOneStrike(t *testing.T) {
	t.Parallel()
	url := os.Getenv("WAC_TEST_REDIS_URL")
	if url == "" {
		t.Skipf("set WAC_TEST_REDIS_URL to run this against a real Redis (see 'make test-redis')")
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse WAC_TEST_REDIS_URL: %v", err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	prefix := "wactest:" + t.Name() + ":" + strconv.FormatInt(time.Now().UnixNano(), 36) + ":"
	t.Cleanup(func() {
		keys, err := rdb.Keys(context.Background(), prefix+"*").Result()
		if err == nil && len(keys) > 0 {
			_ = rdb.Del(context.Background(), keys...).Err()
		}
	})

	fixed := time.Date(2026, 9, 17, 21, 0, 0, 0, time.UTC)
	quarantine := NewQuarantine(redisx.Wrap(rdb, prefix, 8), func() time.Time { return fixed })
	// One attempt's token, handed out twice: what go-redis does to a command whose answer
	// was lost, without the wire it needs to do it.
	quarantine.random = func() uint64 { return 42 }

	const sid = "sess-retried"
	key := prefix + "quarantine:" + sid

	first, err := quarantine.Strike(context.Background(), sid)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	second, err := quarantine.Strike(context.Background(), sid)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}

	strikes, err := rdb.HGet(context.Background(), key, "strikes").Int64()
	if err != nil {
		t.Fatalf("HGet strikes: %v", err)
	}
	if strikes != 1 {
		t.Fatalf("one attempt run twice left %d strikes; the count is measuring round trips, not adoptions that failed", strikes)
	}
	if !second.Equal(first) {
		t.Fatalf("the second run answered %s and the first %s; a retry has to be told what the attempt already decided", second, first)
	}
	if got := first.Sub(fixed); got != QuarantineFloor {
		t.Fatalf("one failure waits %s, want the floor at %s", got, QuarantineFloor)
	}
}
