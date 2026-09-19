package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// The client half, spoken the way the contract says a client speaks it.
//
// Not a handle into the connector: commands go on the control stream or on a session's
// own, replies come back on the reply list the command named, and events are read from
// the shard the fleet put them on. Everything this bench asserts, it asserts from out
// here, because that is where a client stands and it is the only place a second process
// can be observed from at all.
type client struct {
	rdb  *redis.Client
	keys redisx.Keys
}

func newClient(rdb *redis.Client, prefix string, shards int) *client {
	return &client{rdb: rdb, keys: redisx.NewKeys(prefix, shards)}
}

// fleetShards asks the fleet how many event streams it has, rather than assuming the
// number this bench passed in. They are the same number here, and the reading is still
// worth doing: it is what a client does, and a mismatch means a process came up with a
// configuration nobody asked for.
func (c *client) fleetShards(ctx context.Context) (int, error) {
	shards, err := c.rdb.HGet(ctx, c.keys.Meta(), "event_shards").Int()
	if err != nil {
		return 0, fmt.Errorf("read the fleet's event shard count from %s: %w", c.keys.Meta(), err)
	}
	if shards <= 0 {
		return 0, fmt.Errorf("the fleet says it has %d event shards, which names no stream", shards)
	}
	return shards, nil
}

func (c *client) send(ctx context.Context, stream string, command *protocol.Command) error {
	fields, err := command.Fields()
	if err != nil {
		return fmt.Errorf("render %s for %s: %w", command.Type, command.SID, err)
	}
	values := make(map[string]any, len(fields))
	for key, value := range fields {
		values[key] = value
	}
	return c.rdb.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: values}).Err()
}

// await blocks on the reply list the command named, the way a client does.
func (c *client) await(ctx context.Context, commandID string, within time.Duration) (protocol.Reply, error) {
	answer, err := c.rdb.BLPop(ctx, within, c.keys.Reply(commandID)).Result()
	if err != nil {
		return protocol.Reply{}, fmt.Errorf("wait for the reply to %s: %w", commandID, err)
	}
	var reply protocol.Reply
	if err := json.Unmarshal([]byte(answer[1]), &reply); err != nil {
		return protocol.Reply{}, fmt.Errorf("read the reply to %s: %w", commandID, err)
	}
	return reply, nil
}

// repliesTo drains every answer that is still on a command's reply list.
//
// More than one is the observable this bench uses for a duplicated side effect: a command
// carried out twice answers twice, and the two answers differ in what the second run
// produced. The recall path answers twice as well and the two are identical, which is the
// whole difference between remembering a command and doing it again.
func (c *client) repliesTo(ctx context.Context, commandID string) ([]string, error) {
	return c.rdb.LRange(ctx, c.keys.Reply(commandID), 0, -1).Result()
}

// eventsOn reads a whole shard, oldest first, and says how long the stream is and what
// its first id is.
//
// The two extra numbers are what keeps a gap in `seq` from being reported as a lost
// event when the stream was simply trimmed under the reader: a hole inside a stream that
// still holds its first entry is a hole; the same hole in a stream that starts later than
// it used to is a truncation, and they are different findings.
type shardRead struct {
	stream  string
	events  []protocol.Event
	length  int64
	firstID string
}

func (c *client) eventsOn(ctx context.Context, shard int) (shardRead, error) {
	stream := c.keys.Events(shard)
	read := shardRead{stream: stream}

	length, err := c.rdb.XLen(ctx, stream).Result()
	if err != nil {
		return read, fmt.Errorf("measure %s: %w", stream, err)
	}
	read.length = length

	entries, err := c.rdb.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		return read, fmt.Errorf("read %s: %w", stream, err)
	}
	if len(entries) > 0 {
		read.firstID = entries[0].ID
	}
	for _, entry := range entries {
		fields := make(map[string]string, len(entry.Values))
		for key, value := range entry.Values {
			if text, ok := value.(string); ok {
				fields[key] = text
			}
		}
		event, err := protocol.ParseEvent(fields)
		if err != nil {
			return read, fmt.Errorf("read an event of %s (%s): %w", stream, entry.ID, err)
		}
		read.events = append(read.events, event)
	}
	return read, nil
}

// leaseHolders answers who holds each session's lease right now, which is the reading
// operational invariant 1 is about.
func (c *client) leaseHolders(ctx context.Context, sids []string) (map[string]string, error) {
	held := make(map[string]string, len(sids))
	for _, sid := range sids {
		holder, err := c.rdb.Get(ctx, c.keys.Lease(sid)).Result()
		switch {
		case errors.Is(err, redis.Nil):
			continue
		case err != nil:
			return nil, fmt.Errorf("read the lease of %s: %w", sid, err)
		}
		held[sid] = holder
	}
	return held, nil
}

// instances is the fleet's own registry, which is how a client learns who is up.
func (c *client) instances(ctx context.Context) (map[string]map[string]string, error) {
	names, err := c.rdb.SMembers(ctx, c.keys.Instances()).Result()
	if err != nil {
		return nil, fmt.Errorf("read the instance registry: %w", err)
	}
	registry := make(map[string]map[string]string, len(names))
	for _, name := range names {
		fields, err := c.rdb.HGetAll(ctx, c.keys.Instance(name)).Result()
		if err != nil {
			return nil, fmt.Errorf("read the registry entry of %s: %w", name, err)
		}
		registry[name] = fields
	}
	return registry, nil
}
