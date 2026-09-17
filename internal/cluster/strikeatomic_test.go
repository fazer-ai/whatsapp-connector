package cluster_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// RedisEnv names a real Redis for the passes miniredis cannot stand in for. A mark that
// has to be all or nothing is exactly that case: the double runs a script without the
// server's atomicity behind it, so against it a two-step write and a one-step write are
// indistinguishable, and the pass would be green on the very defect it exists for.
const RedisEnv = "WAC_TEST_REDIS_URL"

// fixedClock is when these tests say it is.
//
// The deadline a strike returns is computed inside the script from the instant the caller
// handed it, so against a wall clock every assertion here would be "the wait, give or take
// however long Redis took", and a round trip over 500ms would put a correct deadline below
// the floor. Pinned, the arithmetic is exact and a slow server is slow rather than wrong.
var fixedClock = time.Date(2026, 9, 17, 21, 0, 0, 0, time.UTC)

// realQuarantine is the fleet's quarantine over the server RedisEnv names, under a prefix
// of its own so a run leaves nothing behind for the next one.
func realQuarantine(t *testing.T) (*cluster.Quarantine, *redis.Client, string) {
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
	return cluster.NewQuarantine(redisx.Wrap(rdb, prefix, 8), func() time.Time { return fixedClock }), rdb, prefix
}

// cutSecondHalf fails whichever command carries the second half of a mark, and it names
// both designs on purpose.
//
// The build before this one wrote the mark in two round trips and the second was an `HSET`
// inside a pipeline; this one writes it in a script, so the whole mark rides one `EVAL`.
// An instrument that only knew the old command would let the new design pass by missing it
// rather than by being atomic, which is a negative control collected in the wrong
// condition. Naming both means the failure is injected into whatever actually carries the
// write, and the assertion is the same either way: a strike that failed leaves nothing a
// later reader could mistake for a history.
type cutSecondHalf struct{ on atomic.Bool }

func (c *cutSecondHalf) DialHook(next redis.DialHook) redis.DialHook { return next }

func (c *cutSecondHalf) cut(name string) bool {
	if !c.on.Load() {
		return false
	}
	switch name {
	case "hset", "eval", "evalsha":
		return true
	}
	return false
}

func (c *cutSecondHalf) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if c.cut(cmd.Name()) {
				return errors.New("redis: the answer never came back")
			}
		}
		return next(ctx, cmds)
	}
}

func (c *cutSecondHalf) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if c.cut(cmd.Name()) {
			return errors.New("redis: the answer never came back")
		}
		return next(ctx, cmd)
	}
}

// TestAStrikeThatFailedLeavesNoHalfWrittenMark is the whole of #250 in one assertion.
//
// The mark used to be two round trips: the count, then the deadline and the lifetime
// together. A Redis that answered the first and not the second left a key holding a
// history with no wait, and `Waiting` reads that as an account to leave alone -- correctly,
// because a wait that never landed is not a wait. So nothing was paced, and the next strike
// computed its wait from a count that every unpaced beat had already inflated.
//
// Checked on the fields rather than on what the caller was told, because the caller is a
// sweep that has already given the account up and the fields are what the fleet reads
// afterwards. A key with `strikes` and no `until` is the state this exists to make
// unreachable, and a key with no lifetime contradicts `contract/PROTOCOL.md`, which tells
// clients this one is `HASH (EX wait + 1h)`.
func TestAStrikeThatFailedLeavesNoHalfWrittenMark(t *testing.T) {
	t.Parallel()
	quarantine, rdb, prefix := realQuarantine(t)
	const sid = "sess-half"
	key := prefix + "quarantine:" + sid

	cut := &cutSecondHalf{}
	cut.on.Store(true)
	rdb.AddHook(cut)

	if _, err := quarantine.Strike(t.Context(), sid); err == nil {
		t.Fatalf("Strike answered as if it had landed while the write was being cut")
	}

	fields, err := rdb.HGetAll(t.Context(), key).Result()
	if err != nil {
		t.Fatalf("HGetAll: %v", err)
	}
	if len(fields) != 0 {
		t.Fatalf("a strike that failed left %v behind; a count with no deadline reads as a history nobody served", fields)
	}
}

// TestAStrikeThatLandedCarriesBothHalves is the control beside it: the refusal above has to
// turn on the write failing, and not on strikes never writing anything.
func TestAStrikeThatLandedCarriesBothHalves(t *testing.T) {
	t.Parallel()
	quarantine, rdb, prefix := realQuarantine(t)
	const sid = "sess-whole"
	key := prefix + "quarantine:" + sid

	if _, err := quarantine.Strike(t.Context(), sid); err != nil {
		t.Fatalf("Strike: %v", err)
	}

	fields, err := rdb.HGetAll(t.Context(), key).Result()
	if err != nil {
		t.Fatalf("HGetAll: %v", err)
	}
	if fields["strikes"] == "" || fields["until"] == "" {
		t.Fatalf("a strike left %v; the mark has to carry both halves or neither", fields)
	}
	ttl, err := rdb.TTL(t.Context(), key).Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 {
		t.Fatalf("the mark has no lifetime (TTL %s); the contract tells clients this key is EX wait + 1h", ttl)
	}
}

// TestACountWithNoDeadlineIsNotAHistory fences the one legacy state the fix cannot prevent:
// a key written by the build before it, holding a count whose deadline never landed.
//
// Starting over from the floor rather than trusting the count, and the reason is that the
// house already reads that state this way. `Waiting` treats a count with no deadline as an
// account that is free, in as many words, because a strike whose second half did not land
// is not a wait the account has served. Computing the next wait from it would make the same
// pair of fields authoritative in one place and not in the other, and the account that
// crossed a slow Redis would come back to an hour it never earned -- which is the punishment
// the cap exists for a banned account, handed to one whose Redis was briefly away.
func TestACountWithNoDeadlineIsNotAHistory(t *testing.T) {
	t.Parallel()
	quarantine, rdb, prefix := realQuarantine(t)
	const sid = "sess-legacy"
	key := prefix + "quarantine:" + sid

	// What the build before this one left behind: six beats of a fleet that could not pace
	// itself, and no deadline to show for any of them.
	if err := rdb.HSet(t.Context(), key, "strikes", 6).Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}

	until, err := quarantine.Strike(t.Context(), sid)
	if err != nil {
		t.Fatalf("Strike: %v", err)
	}
	if got := until.Sub(fixedClock); got != cluster.QuarantineFloor {
		t.Fatalf("an orphan count of 6 produced a wait of %s, want the floor at %s: "+
			"a count with no deadline measures how often the loop span, not what the account has served",
			got, cluster.QuarantineFloor)
	}
}

// TestTheScriptAndTheGoSideAgreeOnTheWait is the fence over a rule that now lives twice.
//
// The doubling is spelled in the script, because the wait has to be decided where the
// count is read, and `QuarantineWait` keeps answering the same question on the Go side for
// everything that asks without striking. Two spellings of one rule is the drift this
// repository keeps paying for, and this one would drift silently: nothing else compares
// them, and a mark written with the wrong wait is still a well-formed mark.
//
// Walked one strike at a time against a real Redis rather than asserted at a couple of
// points, because the disagreement worth catching is at the shape of the curve -- an
// off-by-one in the loop, or a cap applied a step early -- and those hide between samples.
// Exact rather than rounded, which the pinned clock buys: a tolerance wide enough for a
// slow round trip is wide enough to swallow a wait that is off by a fraction of a step.
func TestTheScriptAndTheGoSideAgreeOnTheWait(t *testing.T) {
	t.Parallel()
	quarantine, _, _ := realQuarantine(t)
	const sid = "sess-curve"

	for strikes := int64(1); strikes <= 9; strikes++ {
		until, err := quarantine.Strike(t.Context(), sid)
		if err != nil {
			t.Fatalf("strike %d: %v", strikes, err)
		}
		want := cluster.QuarantineWait(strikes)
		if got := until.Sub(fixedClock); got != want {
			t.Fatalf("strike %d waits %s in the script and %s in QuarantineWait", strikes, got, want)
		}
	}
}

// TestAnExpiredWaitIsStillAHistory is the other side of the rule above, and the side that
// costs something if it is got wrong.
//
// Starting an orphan count over is right because a count with no deadline is a mark whose
// second half never landed. A count whose deadline has simply **passed** is the opposite: it
// is every wait the account has already served, still inside the hour the record outlives
// them for, and forgiving it would reset the backoff of every account whose current wait had
// run out -- which is every account the fleet is about to try again. The whole mechanism
// would collapse to the floor and never leave it.
//
// The two look alike from a distance and the discriminator is the field's presence, not its
// value. This is the test that says so.
func TestAnExpiredWaitIsStillAHistory(t *testing.T) {
	t.Parallel()
	quarantine, rdb, prefix := realQuarantine(t)
	const sid = "sess-served"
	key := prefix + "quarantine:" + sid

	// An account that has genuinely failed eight times, whose eighth wait ran out an hour
	// ago and whose record has not yet been forgotten.
	served := fixedClock.Add(-time.Hour).UnixMilli()
	if err := rdb.HSet(t.Context(), key, "strikes", 8, "until", served).Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}

	until, err := quarantine.Strike(t.Context(), sid)
	if err != nil {
		t.Fatalf("Strike: %v", err)
	}
	if got := until.Sub(fixedClock); got != cluster.QuarantineWait(9) {
		t.Fatalf("a ninth failure waits %s, want %s: a deadline that passed is every wait the "+
			"account already served, and forgiving it resets the backoff of every account due to be tried",
			got, cluster.QuarantineWait(9))
	}
}
