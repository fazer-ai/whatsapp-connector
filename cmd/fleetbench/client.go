package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	answer, err := blockingPop(ctx, c.rdb, c.keys.Reply(commandID), within)
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
	lastID  string
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
		read.lastID = entries[len(entries)-1].ID
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

// pendingOn is how much work the consumer groups of this run still have: entries taken
// and not retired, plus entries nobody has read yet.
//
// Read from the consumer groups and not from what this bench sent: "I put six commands on
// a stream" says nothing about whether any of them is still in flight a moment later, and
// the fake engine answers a send before the next one is written.
//
// Both numbers, because the assertion at the end of the run reads both. A group can be at
// zero pending with a hundred entries still unread, and a drain that stopped there would
// hand `assertConsumerGroups` a fleet with a backlog and let it report the backlog as a
// hole. What is waited for has to be what is asserted.
// backlog is what a fleet still owes, kept in its two halves.
//
// They are different facts and a sum of them answers neither. `pending` counts entries
// handed to a consumer that never acknowledged them, which after a kill is the work the
// dead owner was in the middle of. `lag` counts entries no consumer has been handed at
// all, which under a steady load is simply the load that kept arriving. Added together, a
// fast owner that acknowledged its whole batch before dying reports a positive "work
// interrupted" made entirely of commands that arrived afterwards.
type backlog struct {
	pending int64
	lag     int64
}

func (b backlog) total() int64 { return b.pending + b.lag }

func (c *client) pendingOn(ctx context.Context, sids []string) (backlog, error) {
	streams := make([]string, 0, len(sids)+1)
	streams = append(streams, c.keys.Control())
	for _, sid := range sids {
		streams = append(streams, c.keys.Commands(sid))
	}
	found := backlog{}
	for _, stream := range streams {
		groups, err := c.rdb.XInfoGroups(ctx, stream).Result()
		if err != nil {
			// A stream nothing ever consumed has no group and no pending entries. Any
			// other failure is a reading that did not happen, and returning zero for it
			// would be a drain that was never observed.
			if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") {
				continue
			}
			return backlog{}, fmt.Errorf("read the consumer groups of %s: %w", stream, err)
		}
		for _, group := range groups {
			found.pending += group.Pending
			found.lag += group.Lag
		}
	}
	return found, nil
}

// repliesToAll takes the answers of every command in one go, so they are read while they
// are still there.
//
// A reply list carries a TTL: the connector puts one on it rather than leaving an answer
// nobody came back for in Redis forever. Reading them at the end of a run that spends a
// minute on a later phase reads a list that has already expired, and an empty list is not
// evidence that a command answered once.
func (c *client) repliesToAll(ctx context.Context, ids []string) (map[string][]string, error) {
	answers := make(map[string][]string, len(ids))
	for _, id := range ids {
		found, err := c.repliesTo(ctx, id)
		if err != nil {
			return nil, err
		}
		answers[id] = found
	}
	return answers, nil
}

// blockingPop waits for one entry, in slices, so that a cancelled context is noticed while
// the wait is still going.
//
// MEASURED against the pinned go-redis, on this machine, with a BLPOP of 60 s:
//
//	cancelled mid-read, ContextTimeoutEnabled off   60.05 s
//	cancelled mid-read, ContextTimeoutEnabled on    60.02 s
//	context with a deadline, the flag off           60.07 s
//	context with a deadline, the flag on             1.00 s
//	slices of one second, cancelled mid-read         1.04 s
//
// So the flag is about a deadline the call already knows when it starts, and it does
// nothing for a cancellation that arrives while a read is in flight -- which is the Ctrl-C
// case. Waiting in slices is what covers that one: between two of them there is a moment
// where the context is read, and the run gets to its cleanup instead of sitting on a dead
// socket for the rest of the minute.
//
// Each slice uses a context of its own on purpose. Handing the parent to BLPOP would make
// the slice inherit a cancellation that is precisely what the loop above is checking for,
// and the wait would end inside go-redis with an error instead of here with ctx.Err().
func blockingPop(ctx context.Context, rdb *redis.Client, key string, within time.Duration) ([]string, error) {
	const slice = time.Second
	deadline := time.Now().Add(within)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		left := time.Until(deadline)
		if left <= 0 {
			return nil, redis.Nil
		}
		answer, err := rdb.BLPop(context.WithoutCancel(ctx), min(slice, left), key).Result()
		if err == nil {
			return answer, nil
		}
		if !errors.Is(err, redis.Nil) {
			return nil, err
		}
	}
}
