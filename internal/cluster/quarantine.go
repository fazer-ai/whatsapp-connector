package cluster

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// The backoff a session that will not connect is retried on. Doubling from a minute to
// an hour, which is four hours of trying spread over eight attempts rather than four
// hundred and eighty.
//
// The floor is a minute because the commonest reason to be here is not the account at
// all: WhatsApp refusing a build, a network that is away, a store that was unreachable
// for a moment. Those come back, and an account that is quarantined for an hour on the
// first failure is one that stays down long after the cause is gone.
//
// The ceiling is an hour because the other reason is permanent -- a number that was
// unlinked from the phone, an account WhatsApp banned -- and an attempt an hour is what
// that should cost. There is no giving up entirely: nothing in the connector can tell
// "permanently gone" from "away for a long time", and a session it stopped trying would
// need a human to notice, which is the state this whole mechanism exists to end.
const (
	QuarantineFloor = time.Minute
	QuarantineCap   = time.Hour
	// How long the count outlives the wait it produced. A session left alone for the
	// whole wait and this much again starts over from the floor, which is the right
	// answer for an account whose trouble is over: nothing has failed for hours.
	quarantineMemory = time.Hour
)

// Quarantine records the sessions that keep failing to come back, and how long the fleet
// should leave each of them alone.
//
// Fleet-wide rather than per instance, and that is the whole point: an account that
// cannot connect fails the same way everywhere, so a count kept in one process would let
// every other instance in the fleet make the same attempt at the same account, and the
// backoff would be divided by the number of instances rather than shared by them.
//
// It gates what the connector does on its own, and nothing else. A client that asks for a
// connection gets one: the operator is the one thing in the system that may know why the
// last attempt failed, and refusing them would make the backoff into a wall in front of
// the person fixing it. That is also why no command is ever answered `quarantined`.
type Quarantine struct {
	client *redisx.Client
	now    func() time.Time
	// random mints the token that tells one attempt from a retry of the same one. A seam
	// rather than a call to rand inline, because a test that wants two calls to collide
	// has no other way to arrange it, and the collision is the case that matters.
	random func() uint64
}

// NewQuarantine returns the fleet's quarantine over one Redis.
func NewQuarantine(client *redisx.Client, now func() time.Time) *Quarantine {
	if now == nil {
		now = time.Now
	}
	return &Quarantine{client: client, now: now, random: rand.Uint64}
}

// Strike records one more failure for a session and returns the moment the fleet may try
// it again.
//
// The count is incremented before the wait is computed from it, so two instances striking
// the same account at once produce two strikes rather than one: they are two attempts,
// and an account that just cost the fleet two failures has earned the longer wait.
func (q *Quarantine) Strike(ctx context.Context, sid string) (time.Time, error) {
	if sid == "" {
		return time.Time{}, errors.New("cluster: a strike needs a session")
	}
	key := q.client.Keys().Quarantine(sid)
	// Minted once per call, which is what makes a strike count an attempt rather than a
	// round trip. go-redis sends a command again when its answer never comes back, and it
	// sends the command it already built: a script that reached the server, ran, and lost
	// its reply runs a second time under the arguments this line produced. Measured on a
	// real Redis with the answer cut on the wire -- one failed adoption, two strikes, and
	// the connector logging success both times, because from here a retry that worked and
	// a first try that worked are the same thing.
	//
	// Written as its own statement for the reader rather than for the compiler: inlined
	// into the argument list it is still evaluated once, and a mutation that moves it
	// there changes nothing, which is why no test fences the position. What would break it
	// is minting per attempt instead of per call, and nothing here is shaped to do that.
	attempt := strconv.FormatUint(q.random(), 36)
	answer, err := strikeScript.Run(ctx, q.client, []string{key},
		q.now().UnixMilli(), QuarantineFloor.Milliseconds(), QuarantineCap.Milliseconds(),
		quarantineMemory.Milliseconds(), attempt,
	).Int64()
	if err != nil {
		return time.Time{}, fmt.Errorf("cluster: record a failure for %s: %w", sid, err)
	}
	return time.UnixMilli(answer), nil
}

// strikeScript writes the whole mark or none of it, and computes the wait where the count
// it comes from is read.
//
// One script and not two round trips, because the wait is a function of the count the
// increment returns: a transaction cannot read that result in the middle of itself, so
// MULTI does not reach this. Split across two calls, a Redis that answered the first and
// not the second left a key holding a count with no deadline -- an account with a history
// and no wait, which `Waiting` correctly reads as free, so nothing was paced and the next
// strike computed its wait from a count every unpaced beat had inflated. Seven of those
// reach the hour the cap keeps for a banned account.
//
// The doubling is spelled here rather than passed in, so the wait is decided in the same
// place the count is, and `QuarantineWait` on the Go side answers for the same rule. A
// count with no deadline is read as nothing rather than as history, which is the reading
// `Waiting` already gives that state: a strike whose second half never landed is not a
// wait the account has served, and trusting it would hand an account that crossed a slow
// Redis the hour it never earned.
var strikeScript = redis.NewScript(`
local now, floor, cap, memory = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local attempt = ARGV[5]
if redis.call("HGET", KEYS[1], "attempt") == attempt then
  return tonumber(redis.call("HGET", KEYS[1], "until"))
end
local strikes = 1
if redis.call("HGET", KEYS[1], "until") then
  strikes = tonumber(redis.call("HINCRBY", KEYS[1], "strikes", 1))
else
  redis.call("HSET", KEYS[1], "strikes", 1)
end
local wait = floor
for _ = 2, strikes do
  wait = wait * 2
  if wait >= cap then
    wait = cap
    break
  end
end
local until_ms = now + wait
redis.call("HSET", KEYS[1], "until", until_ms, "attempt", attempt)
redis.call("PEXPIRE", KEYS[1], wait + memory)
return until_ms
`)

// QuarantineWait is how long to leave an account alone after this many failures in a row.
func QuarantineWait(strikes int64) time.Duration {
	if strikes <= 1 {
		return QuarantineFloor
	}
	wait := QuarantineFloor
	for range strikes - 1 {
		wait *= 2
		if wait >= QuarantineCap {
			return QuarantineCap
		}
	}
	return wait
}

// Clear forgets a session's failures, which is what a connection that worked means.
//
// Cleared rather than decremented: what the count is for is how long to wait before the
// next attempt, and an account that is connected has no next attempt to wait for. If it
// falls over again the first failure starts at the floor, which is the right answer for a
// session that was working a moment ago.
func (q *Quarantine) Clear(ctx context.Context, sid string) error {
	if sid == "" {
		return nil
	}
	if err := q.client.Del(ctx, q.client.Keys().Quarantine(sid)).Err(); err != nil {
		return fmt.Errorf("cluster: forget the failures of %s: %w", sid, err)
	}
	return nil
}

// Waiting keeps the sessions the fleet should still leave alone, out of the ones asked
// about. One pipelined read for the whole list, because the caller is a sweep.
//
// A key with a count and no deadline is treated as free rather than as quarantined: that
// is a strike whose second half did not land, and holding an account forever on the
// strength of a write that failed is the one outcome worse than trying too often.
func (q *Quarantine) Waiting(ctx context.Context, sids []string) (map[string]time.Time, error) {
	if len(sids) == 0 {
		return nil, nil
	}
	keys := q.client.Keys()
	pipeline := q.client.Pipeline()
	until := make([]*redis.StringCmd, len(sids))
	for i, sid := range sids {
		until[i] = pipeline.HGet(ctx, keys.Quarantine(sid), "until")
	}
	if _, err := pipeline.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("cluster: read the quarantine of %d sessions: %w", len(sids), err)
	}

	now := q.now()
	waiting := make(map[string]time.Time, len(sids))
	for i, sid := range sids {
		raw, err := until[i].Result()
		if err != nil {
			continue
		}
		stamp, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			continue
		}
		if deadline := time.UnixMilli(stamp); deadline.After(now) {
			waiting[sid] = deadline
		}
	}
	return waiting, nil
}
