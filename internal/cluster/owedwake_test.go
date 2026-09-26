package cluster_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// owedServers is the Redis a test runs the owed-wake scripts against: miniredis always, and
// the server RedisEnv names when it is set, which CI points at the newest Redis and at 6.2.
// Both, because the release now writes to a stream from inside a script, and a double's
// scripting is not the server's.
func owedServers(t *testing.T) map[string]*redis.Client {
	t.Helper()
	servers := map[string]*redis.Client{}
	mini := miniredis.RunT(t)
	fake := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = fake.Close() })
	servers["miniredis"] = fake
	if url := os.Getenv(RedisEnv); url != "" {
		opts, err := redis.ParseURL(url)
		if err != nil {
			t.Fatalf("parse %s: %v", RedisEnv, err)
		}
		server := redis.NewClient(opts)
		t.Cleanup(func() { _ = server.Close() })
		servers["redis"] = server
	}
	return servers
}

// owedPrefix is a key prefix of this test's own, deleted when it ends.
func owedPrefix(t *testing.T, rdb *redis.Client) string {
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

var aWake = map[string]string{
	"v": "1", "id": "wake-again", "type": "session.wake", "sid": "s1", "ts": "1", "payload": `{"desired":"connected"}`,
}

// What a peer is told when it leaves a wake, in each of the three states the lease can be
// in, and what each leaves behind (#259).
func TestAWakeIsOwedOnlyToAnOwnerThatHasNotMarkedAHandBack(t *testing.T) {
	t.Parallel()

	for name, rdb := range owedServers(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			client := redisx.Wrap(rdb, owedPrefix(t, rdb), 8)
			keys := client.Keys()
			holder := cluster.NewLeases(client, "inst-a", cluster.Options{})
			peer := cluster.NewLeases(client, "inst-b", cluster.Options{})

			if owed, err := peer.OweWake(ctx, "s-free", aWake); err != nil || owed != cluster.OwedNobodyHolds {
				t.Fatalf("an account nobody holds answered %v (%v), want OwedNobodyHolds", owed, err)
			}
			if n, _ := rdb.Exists(ctx, keys.OwedWake("s-free")).Result(); n != 0 {
				t.Fatal("a wake was left owed to nobody")
			}

			if _, err := holder.Acquire(ctx, "s-back"); err != nil {
				t.Fatalf("given: %v", err)
			}
			if err := holder.MarkHandingBack(ctx, "s-back"); err != nil {
				t.Fatalf("given: %v", err)
			}
			if owed, err := peer.OweWake(ctx, "s-back", aWake); err != nil || owed != cluster.OwedHandingBack {
				t.Fatalf("an account being handed back answered %v (%v), want OwedHandingBack", owed, err)
			}

			if _, err := holder.Acquire(ctx, "s-kept"); err != nil {
				t.Fatalf("given: %v", err)
			}
			if owed, err := peer.OweWake(ctx, "s-kept", aWake); err != nil || owed != cluster.OwedToHolder {
				t.Fatalf("an account its owner runs answered %v (%v), want OwedToHolder", owed, err)
			}
			left, err := rdb.HGetAll(ctx, keys.OwedWake("s-kept")).Result()
			if err != nil {
				t.Fatalf("read the owed wake: %v", err)
			}
			if left["holder"] != "inst-a" || left["id"] != "wake-again" || left["payload"] != aWake["payload"] {
				t.Fatalf("the owed wake reads %v", left)
			}
			if ttl, _ := rdb.PTTL(ctx, keys.OwedWake("s-kept")).Result(); ttl <= 0 || ttl > cluster.DefaultTTL {
				t.Fatalf("the owed wake lives %v, want at most one lease TTL", ttl)
			}
		})
	}
}

// The release is where an owed wake is settled: the holder's release puts it on the control
// stream and deletes it in the same step, and a release by anybody else, or of an entry
// left for another holder, puts nothing back.
func TestTheHoldersReleasePutsTheOwedWakeBack(t *testing.T) {
	t.Parallel()

	for name, rdb := range owedServers(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			client := redisx.Wrap(rdb, owedPrefix(t, rdb), 8)
			keys := client.Keys()
			holder := cluster.NewLeases(client, "inst-a", cluster.Options{})
			peer := cluster.NewLeases(client, "inst-b", cluster.Options{})

			if _, err := holder.Acquire(ctx, "s1"); err != nil {
				t.Fatalf("given: %v", err)
			}
			if owed, err := peer.OweWake(ctx, "s1", aWake); err != nil || owed != cluster.OwedToHolder {
				t.Fatalf("given: %v %v", owed, err)
			}
			// A release by an instance that does not hold the lease changes nothing.
			if released, err := peer.Release(ctx, "s1"); err != nil || released {
				t.Fatalf("the peer released a lease it does not hold: %v %v", released, err)
			}
			if n, _ := rdb.XLen(ctx, keys.Control()).Result(); n != 0 {
				t.Fatalf("a release by a non-holder put %d entries on the control stream", n)
			}

			released, err := holder.Release(ctx, "s1")
			if err != nil || !released {
				t.Fatalf("the holder's release: %v %v", released, err)
			}
			entries, err := rdb.XRange(ctx, keys.Control(), "-", "+").Result()
			if err != nil {
				t.Fatalf("read the control stream: %v", err)
			}
			if len(entries) != 1 {
				t.Fatalf("the release put %d entries on the control stream, want the one owed", len(entries))
			}
			for field, want := range aWake {
				if got, _ := entries[0].Values[field].(string); got != want {
					t.Errorf("the wake put back has %s=%q, want %q", field, got, want)
				}
			}
			if _, has := entries[0].Values["holder"]; has {
				t.Error("the wake put back carries the holder field, which is the owed entry's bookkeeping")
			}
			if n, _ := rdb.Exists(ctx, keys.OwedWake("s1")).Result(); n != 0 {
				t.Fatal("the owed wake outlived the release that put it back, so a later release would put it back again")
			}

			// An entry left for another holder is about a hand-back that is not this one.
			if _, err := holder.Acquire(ctx, "s2"); err != nil {
				t.Fatalf("given: %v", err)
			}
			if err := rdb.HSet(ctx, keys.OwedWake("s2"), "holder", "inst-gone", "id", "stale").Err(); err != nil {
				t.Fatalf("given: %v", err)
			}
			if _, err := holder.Release(ctx, "s2"); err != nil {
				t.Fatalf("release s2: %v", err)
			}
			if n, _ := rdb.XLen(ctx, keys.Control()).Result(); n != 1 {
				t.Fatalf("an entry owed to another holder was put back: %d entries on the control stream", n)
			}
		})
	}
}

// An account deleted here puts no wake back when it is released (#259): one owed before the
// delete or after it asked for an account that no longer exists, and put back it would have
// a peer adopt it. Also when the delete could not reach Redis, since the teardown goes on
// regardless, and only for that account's next release: acquired again, it is an ordinary
// account.
func TestADeletedAccountPutsNoWakeBack(t *testing.T) {
	t.Parallel()

	for name, rdb := range owedServers(t) {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name        string
				forgetFails bool
			}{
				{"the delete reached Redis", false},
				{"the delete did not reach Redis", true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx := context.Background()
					own := redis.NewClient(rdb.Options())
					t.Cleanup(func() { _ = own.Close() })
					failing := &failEpochWrites{}
					own.AddHook(failing)
					client := redisx.Wrap(own, owedPrefix(t, rdb), 8)
					keys := client.Keys()
					holder := cluster.NewLeases(client, "inst-a", cluster.Options{})
					peer := cluster.NewLeases(client, "inst-b", cluster.Options{})

					if _, err := holder.Acquire(ctx, "s1"); err != nil {
						t.Fatalf("given: %v", err)
					}
					if owed, err := peer.OweWake(ctx, "s1", aWake); err != nil || owed != cluster.OwedToHolder {
						t.Fatalf("given: %v %v", owed, err)
					}
					failing.on.Store(tc.forgetFails)
					err := holder.ForgetEpoch(ctx, "s1")
					failing.on.Store(false)
					if tc.forgetFails != (err != nil) {
						t.Fatalf("given: ForgetEpoch answered %v", err)
					}
					if owed, err := peer.OweWake(ctx, "s1", aWake); err != nil || owed != cluster.OwedToHolder {
						t.Fatalf("given: a wake after the delete answered %v %v", owed, err)
					}
					if released, err := holder.Release(ctx, "s1"); err != nil || !released {
						t.Fatalf("the holder's release: %v %v", released, err)
					}
					if n, _ := rdb.XLen(ctx, keys.Control()).Result(); n != 0 {
						t.Fatalf("the release of a deleted account put %d wakes back", n)
					}
					if n, _ := rdb.Exists(ctx, keys.OwedWake("s1")).Result(); n != 0 {
						t.Fatal("the owed wake outlived the release of a deleted account")
					}

					if _, err := holder.Acquire(ctx, "s1"); err != nil {
						t.Fatalf("acquire again: %v", err)
					}
					if owed, err := peer.OweWake(ctx, "s1", aWake); err != nil || owed != cluster.OwedToHolder {
						t.Fatalf("given: %v %v", owed, err)
					}
					if _, err := holder.Release(ctx, "s1"); err != nil {
						t.Fatalf("release again: %v", err)
					}
					if n, _ := rdb.XLen(ctx, keys.Control()).Result(); n != 1 {
						t.Fatalf("the release of an account acquired again after a delete put %d wakes back, want the one owed", n)
					}
				})
			}
		})
	}
}

// failEpochWrites fails, while on, every command naming an epoch counter, which is what a
// Redis that went away in the middle of a teardown looks like to ForgetEpoch.
type failEpochWrites struct{ on atomic.Bool }

func (*failEpochWrites) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *failEpochWrites) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.on.Load() {
			for _, arg := range cmd.Args() {
				if key, ok := arg.(string); ok && strings.Contains(key, "lease-epoch:") {
					cmd.SetErr(errors.New("injected: redis went away"))
					return cmd.Err()
				}
			}
		}
		return next(ctx, cmd)
	}
}

func (*failEpochWrites) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// A wake owed to an account the fleet is leaving alone is dropped by the release rather than
// put back while the wait lasts, and put back once it is over. A wake put back is a new
// entry, read as a first delivery, so a put back inside the wait would bring the account up
// past the backoff; the resume sweep is what brings it back.
func TestAWakeOwedToAQuarantinedAccountWaitsForTheBackoff(t *testing.T) {
	t.Parallel()

	for name, rdb := range owedServers(t) {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				until   time.Duration
				putBack int64
			}{
				{"inside the wait", time.Hour, 0},
				{"after the wait", -time.Hour, 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx := context.Background()
					client := redisx.Wrap(rdb, owedPrefix(t, rdb), 8)
					keys := client.Keys()
					holder := cluster.NewLeases(client, "inst-a", cluster.Options{})
					peer := cluster.NewLeases(client, "inst-b", cluster.Options{})

					if _, err := holder.Acquire(ctx, "s1"); err != nil {
						t.Fatalf("given: %v", err)
					}
					if owed, err := peer.OweWake(ctx, "s1", aWake); err != nil || owed != cluster.OwedToHolder {
						t.Fatalf("given: %v %v", owed, err)
					}
					// Against the server's own clock, which is the one the release reads.
					now, err := rdb.Time(ctx).Result()
					if err != nil {
						t.Fatalf("read the server's clock: %v", err)
					}
					until := now.Add(tc.until).UnixMilli()
					if err := rdb.HSet(ctx, keys.Quarantine("s1"), "strikes", 1, "until", until).Err(); err != nil {
						t.Fatalf("given: %v", err)
					}
					if released, err := holder.Release(ctx, "s1"); err != nil || !released {
						t.Fatalf("the holder's release: %v %v", released, err)
					}
					if n, _ := rdb.XLen(ctx, keys.Control()).Result(); n != tc.putBack {
						t.Fatalf("the release put %d wakes back, want %d", n, tc.putBack)
					}
					if n, _ := rdb.Exists(ctx, keys.OwedWake("s1")).Result(); n != 0 {
						t.Fatal("the owed wake outlived the release that settled it")
					}
				})
			}
		})
	}
}
