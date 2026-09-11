package cluster

import (
	"context"
	"errors"
	"fmt"
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
}

// NewQuarantine returns the fleet's quarantine over one Redis.
func NewQuarantine(client *redisx.Client, now func() time.Time) *Quarantine {
	if now == nil {
		now = time.Now
	}
	return &Quarantine{client: client, now: now}
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
	strikes, err := q.client.HIncrBy(ctx, key, "strikes", 1).Result()
	if err != nil {
		return time.Time{}, fmt.Errorf("cluster: record a failure for %s: %w", sid, err)
	}
	wait := QuarantineWait(strikes)
	until := q.now().Add(wait)

	pipeline := q.client.Pipeline()
	pipeline.HSet(ctx, key, "until", strconv.FormatInt(until.UnixMilli(), 10))
	pipeline.Expire(ctx, key, wait+quarantineMemory)
	if _, err := pipeline.Exec(ctx); err != nil {
		// The count landed and the deadline did not, which reads as an account with a
		// history and no wait: the next pass tries it at once and strikes again. Worth
		// saying out loud rather than swallowing, and not worth failing the caller for --
		// the caller is a sweep that has already given the account up.
		return time.Time{}, fmt.Errorf("cluster: set how long to leave %s alone: %w", sid, err)
	}
	return until, nil
}

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
