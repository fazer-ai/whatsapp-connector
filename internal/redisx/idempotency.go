package redisx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultIdempotencyTTL is how long a command's outcome is remembered.
//
// It has to outlive the longest a command can sit unacknowledged and still be
// reclaimed by another instance, because that is the redelivery this record answers.
// A day is far past that and costs one small string per message; forgetting too early
// is a duplicate side effect, and forgetting too late is nothing at all.
//
// Nothing bounds how long an entry stays pending on its own, so the transport is what
// makes "far past that" true: redisstream retires an entry idle longer than this one
// rather than take it over. The two constants move together, and changing this one
// without that one puts a command back in front of an instance that has no way left of
// telling whether it already ran.
const DefaultIdempotencyTTL = 24 * time.Hour

// Idempotency remembers what a command did, so a redelivery answers with the first
// run's result instead of carrying it out a second time.
//
// The record is written after the command succeeds rather than reserved before it. A
// reservation would only help if a crash between the side effect and the record could
// be resolved afterwards, and it cannot: an entry that says an attempt was made says
// nothing about whether it landed, so the reclaiming instance would have to choose
// between dropping a message that never went out and sending one that already did.
// What closes that window is the caller naming the message, which it does.
type Idempotency struct {
	client *Client
	ttl    time.Duration
}

// NewIdempotency returns the store, using DefaultIdempotencyTTL when ttl is zero.
func NewIdempotency(client *Client, ttl time.Duration) *Idempotency {
	if ttl <= 0 {
		ttl = DefaultIdempotencyTTL
	}
	return &Idempotency{client: client, ttl: ttl}
}

// Recall answers what a command with this key did the first time, and whether there was
// a first time at all.
func (i *Idempotency) Recall(ctx context.Context, sid, key string) (json.RawMessage, bool, error) {
	stored, err := i.client.Get(ctx, i.client.Keys().Idempotency(sid, key)).Bytes()
	switch {
	case errors.Is(err, redis.Nil):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("redisx: recall %s of %s: %w", key, sid, err)
	}
	// Asked about is kept: a record only has to outlive the entry it answers for, and
	// the entry announces itself by being asked about. Without this the two clocks run
	// independently, the record from a fixed point after the command ran and the entry
	// for as long as acknowledgements keep failing, and an entry still being handed
	// around outlives the only thing that can say it already ran.
	//
	// Best effort on purpose. The answer is already in hand, and refusing to give it
	// because the expiry could not be pushed out would turn a shortened record into no
	// record at all.
	// Best effort on purpose, and ignored rather than reported: the answer is already
	// in hand, and refusing to give it because the expiry could not be pushed out would
	// turn a shortened record into no record at all. What a failure here costs is the
	// original expiry, which is where this stood before, and the transport's own age
	// bound is what stands behind it.
	_ = i.client.Expire(ctx, i.client.Keys().Idempotency(sid, key), i.ttl).Err()
	if len(stored) == 0 {
		// A command whose result carried no data. It still ran, and answering that is
		// the point: `null` and "never happened" are the same bytes and not the same
		// thing.
		return nil, true, nil
	}
	return stored, true, nil
}

// Remember records what a command did. It never overwrites: the first run is the one
// every redelivery has to be answered with, or two answers to one command disagree.
func (i *Idempotency) Remember(ctx context.Context, sid, key string, result json.RawMessage) error {
	if err := i.client.SetNX(ctx, i.client.Keys().Idempotency(sid, key), []byte(result), i.ttl).Err(); err != nil {
		return fmt.Errorf("redisx: remember %s of %s: %w", key, sid, err)
	}
	return nil
}
