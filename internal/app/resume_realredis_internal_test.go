package app

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// The resume sweep against the server `make test-redis` names, which CI points at the
// newest Redis and at the oldest one README.md supports (#279).
//
// Everything else in this package runs on miniredis, and miniredis accepts command forms a
// supported server refuses: `SET NX GET` went into this loop in #272, passed every test
// here and CI's Redis 8, and on 6.2 is a syntax error that ends every pass before it brings
// a single account back. Against the floor, that is this test failing.
//
// Each path the pass can take through Redis, because each one is a command a server can
// refuse: the turn taken outright, the turn a live peer holds left alone, and the turn of
// an instance that is gone taken over by the script.
func TestTheResumeSweepRunsOnARealRedis(t *testing.T) {
	t.Parallel()

	url := os.Getenv("WAC_TEST_REDIS_URL")
	if url == "" {
		t.Skip("set WAC_TEST_REDIS_URL to run this against a real Redis (see 'make test-redis')")
	}

	cases := []struct {
		name   string
		holder string // who holds the account's turn before the pass; empty is nobody
		alive  bool   // whether the holder is in the fleet
		back   bool   // whether the pass brings the account back
	}{
		{name: "a free turn is taken", back: true},
		{name: "a live peer's turn is left alone", holder: "inst-b", alive: true, back: false},
		{name: "the turn of an instance that is gone is taken over", holder: "inst-gone", back: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rdb := realRedis(t, url)
			keys := redisx.NewKeys(rdbPrefix(t, rdb), DefaultEventShards)
			connector, container, engine := newResumeConnectorOver(t, redisx.Wrap(rdb, keys.Prefix(), DefaultEventShards))
			wantConnected(t, container, "sid-1", "5511999990001", false)
			if tc.holder != "" {
				if err := rdb.Set(t.Context(), keys.Resume("sid-1"), tc.holder, time.Minute).Err(); err != nil {
					t.Fatalf("mark the turn: %v", err)
				}
			}
			if tc.alive {
				if err := rdb.HSet(t.Context(), keys.Instance(tc.holder), "version", "test").Err(); err != nil {
					t.Fatalf("announce the peer: %v", err)
				}
			}

			connector.resumeOnce(t.Context())

			if !tc.back {
				holder, err := rdb.Get(t.Context(), keys.Resume("sid-1")).Result()
				if err != nil || holder != tc.holder {
					t.Fatalf("the turn is held by %q (err %v), want it left with %q", holder, err, tc.holder)
				}
				if _, ok := engine.Session("sid-1"); ok {
					t.Fatal("an account whose turn a live peer holds was opened here")
				}
				return
			}
			waitFor(t, "the account to be taken and connected", func() bool {
				account, ok := engine.Session("sid-1")
				return ok && account.Connected()
			})
			holder, err := rdb.Get(t.Context(), keys.Resume("sid-1")).Result()
			if err != nil || holder != "inst-a" {
				t.Fatalf("the turn is held by %q (err %v), want this instance's name on it", holder, err)
			}
		})
	}
}

// realRedis is a client for the server url names, closed when the test ends.
func realRedis(t *testing.T, url string) *redis.Client {
	t.Helper()
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse WAC_TEST_REDIS_URL: %v", err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// rdbPrefix is a key prefix of this test's own, deleted when it ends: the subtests run in
// parallel on one server, and sharing a prefix would have one's cleanup take the keys
// another is still reading.
func rdbPrefix(t *testing.T, rdb *redis.Client) string {
	t.Helper()
	prefix := "wactest:" + t.Name() + ":" + strconv.FormatInt(time.Now().UnixNano(), 36) + ":"
	t.Cleanup(func() {
		keys, err := rdb.Keys(context.Background(), prefix+"*").Result()
		if err == nil && len(keys) > 0 {
			_ = rdb.Del(context.Background(), keys...).Err()
		}
	})
	return prefix
}
