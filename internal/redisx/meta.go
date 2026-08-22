package redisx

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// ErrShardMismatch is returned when the fleet already publishes to a different
// number of streams than this instance was configured with.
var ErrShardMismatch = errors.New("redisx: event shard count disagrees with the fleet")

// Meta is what an instance publishes about the fleet as a whole.
type Meta struct {
	ProtocolMin int
	ProtocolMax int
	Shards      int
}

// ClaimMeta records this instance's protocol range and shard count, and refuses to
// go on when the fleet already publishes to a different number of streams.
//
// Refusing is the point. A session's shard is a hash modulo that count, so an
// instance running with a different one publishes a session's events to a stream no
// consumer associates with it: the events are not lost, they are read by the wrong
// consumer, out of order with the rest of the session. There is no recovery from
// that after the fact, so it is caught at startup instead.
func (c *Client) ClaimMeta(ctx context.Context, meta Meta) error {
	key := c.keys.Meta()
	stored, err := c.HGet(ctx, key, "event_shards").Result()
	switch {
	case errors.Is(err, redis.Nil):
	case err != nil:
		return fmt.Errorf("redisx: read meta: %w", err)
	default:
		shards, convErr := strconv.Atoi(stored)
		if convErr != nil {
			return fmt.Errorf("redisx: meta event_shards %q: %w", stored, convErr)
		}
		if shards != meta.Shards {
			return fmt.Errorf("%w: fleet publishes to %d, this instance to %d", ErrShardMismatch, shards, meta.Shards)
		}
	}

	// The protocol range is this instance's own claim and is refreshed on every
	// start, so a fleet mid-upgrade advertises what the instance that wrote last
	// serves. The shard count is not: it is a fleet-wide fact, written once.
	fields := map[string]any{
		"protocol_min": meta.ProtocolMin,
		"protocol_max": meta.ProtocolMax,
		"event_shards": meta.Shards,
	}
	if err := c.HSet(ctx, key, fields).Err(); err != nil {
		return fmt.Errorf("redisx: write meta: %w", err)
	}
	return nil
}
