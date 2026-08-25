// Package redisstream carries the protocol over Redis Streams.
//
// The layout is the contract's: events fan out to `wa:events:<shard>` with a session
// always landing on the same shard, commands arrive per session on `wa:cmd:<sid>` and
// fleet-wide on `wa:control`, and an RPC answer is a single-element list the caller
// blocks on.
package redisstream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// ConsumerGroup is the group the connector reads commands under. Clients read events
// under their own group, named in the contract.
const ConsumerGroup = "connector"

// Defaults for the stream bounds and the read loop. They are fields on Options so a
// deployment can trade memory for backlog, not because any of them is a tuning knob
// worth touching by default.
const (
	DefaultCommandMaxLen = 1000
	DefaultEventMaxLen   = 20000
	DefaultBlock         = 5 * time.Second
	DefaultReplyTTL      = 60 * time.Second
	DefaultClaimMinIdle  = 30 * time.Second
	DefaultReadCount     = 64
)

// Options configures the streams. The zero value asks for the defaults above.
type Options struct {
	// Instance is this connector's id. It is the consumer name inside the group, so
	// it has to be stable for the life of the process and unique in the fleet:
	// commands left pending are claimed back by name.
	Instance      string
	EventMaxLen   int64
	CommandMaxLen int64
	Block         time.Duration
	ReplyTTL      time.Duration
	ClaimMinIdle  time.Duration
	ReadCount     int64
}

// Streams is the Redis Streams implementation of transport.Transport.
type Streams struct {
	client *redisx.Client
	opts   Options
	groups *groupCache
}

// New returns the transport. It creates no keys: a stream and its group are created on
// first use, which is what lets a connector start before any client exists and the
// other way round.
func New(client *redisx.Client, opts Options) (*Streams, error) {
	if opts.Instance == "" {
		return nil, errors.New("redisstream: instance id is required")
	}
	if opts.EventMaxLen <= 0 {
		opts.EventMaxLen = DefaultEventMaxLen
	}
	if opts.CommandMaxLen <= 0 {
		opts.CommandMaxLen = DefaultCommandMaxLen
	}
	if opts.Block <= 0 {
		opts.Block = DefaultBlock
	}
	if opts.ReplyTTL <= 0 {
		opts.ReplyTTL = DefaultReplyTTL
	}
	if opts.ClaimMinIdle <= 0 {
		opts.ClaimMinIdle = DefaultClaimMinIdle
	}
	if opts.ReadCount <= 0 {
		opts.ReadCount = DefaultReadCount
	}
	return &Streams{client: client, opts: opts, groups: newGroupCache()}, nil
}

// Publish appends an event to its session's shard.
//
// `MAXLEN ~` rather than an exact trim: the exact form makes Redis walk the stream on
// every write, and the bound is a memory guard, not a correctness one. What keeps the
// client from missing an entry is its consumer group, not the length.
func (s *Streams) Publish(ctx context.Context, event *protocol.Event) error {
	fields, err := event.Fields()
	if err != nil {
		return fmt.Errorf("redisstream: render event %s: %w", event.ID, err)
	}
	args := &redis.XAddArgs{
		Stream: s.client.Keys().EventsOf(event.SID),
		MaxLen: s.opts.EventMaxLen,
		Approx: true,
		Values: toValues(fields),
	}
	if err := s.client.XAdd(ctx, args).Err(); err != nil {
		return fmt.Errorf("redisstream: publish %s: %w", event.ID, err)
	}
	return nil
}

// Reply pushes the single element the caller is blocked on, and puts a TTL on it so a
// caller that gave up does not leave the answer behind forever.
func (s *Streams) Reply(ctx context.Context, replyTo string, reply protocol.Reply) error {
	if replyTo == "" {
		return errors.New("redisstream: reply without a destination")
	}
	body, err := marshalReply(reply)
	if err != nil {
		return err
	}
	key := s.client.Keys().Reply(replyTo)
	_, err = s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.LPush(ctx, key, body)
		pipe.PExpire(ctx, key, s.opts.ReplyTTL)
		return nil
	})
	if err != nil {
		return fmt.Errorf("redisstream: reply to %s: %w", replyTo, err)
	}
	return nil
}

// Read returns the commands waiting for the sessions this instance owns, plus the
// fleet-wide ones. `>` asks for entries no consumer in the group has taken yet;
// anything already taken and not acknowledged is Claim's business.
func (s *Streams) Read(ctx context.Context, sids []string) ([]transport.Delivery, error) {
	streams := s.streamsFor(sids)
	if len(streams) == 0 {
		return nil, nil
	}
	if err := s.groups.ensure(ctx, s.client, streams); err != nil {
		return nil, err
	}

	args := &redis.XReadGroupArgs{
		Group:    ConsumerGroup,
		Consumer: s.opts.Instance,
		Streams:  append(streams, newEntries(len(streams))...),
		Count:    s.opts.ReadCount,
		Block:    s.opts.Block,
	}
	result, err := s.client.XReadGroup(ctx, args).Result()
	switch {
	case errors.Is(err, redis.Nil):
		return nil, nil
	case isNoGroup(err):
		// The group went away under us (a flush, an operator). Forgetting the streams
		// is what makes the next call recreate it instead of failing forever.
		s.groups.forgetAll(streams)
		return nil, nil
	case err != nil:
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("redisstream: read commands: %w", err)
	}
	return s.deliveries(result), nil
}

// Claim takes over commands another instance read and never acknowledged, which is
// what happens when it is killed between reading a command and carrying it out.
func (s *Streams) Claim(ctx context.Context, sids []string) ([]transport.Delivery, error) {
	streams := s.streamsFor(sids)
	if len(streams) == 0 {
		return nil, nil
	}
	if err := s.groups.ensure(ctx, s.client, streams); err != nil {
		return nil, err
	}

	var claimed []transport.Delivery
	for _, stream := range streams {
		ids, err := s.reclaimable(ctx, stream)
		switch {
		case isNoGroup(err):
			s.groups.forget(stream)
			continue
		case err != nil:
			return nil, err
		case len(ids) == 0:
			continue
		}

		messages, err := s.client.XClaim(ctx, &redis.XClaimArgs{
			Stream:   stream,
			Group:    ConsumerGroup,
			Consumer: s.opts.Instance,
			MinIdle:  s.opts.ClaimMinIdle,
			Messages: ids,
		}).Result()
		switch {
		case isNoGroup(err):
			s.groups.forget(stream)
			continue
		case err != nil && !errors.Is(err, redis.Nil):
			return nil, fmt.Errorf("redisstream: claim %s: %w", stream, err)
		}
		claimed = append(claimed, s.deliveries([]redis.XStream{{Stream: stream, Messages: messages}})...)
	}
	return claimed, nil
}

// reclaimable is the pending entries worth taking over: idle long enough, and held by
// somebody else.
//
// The second half is the reason this is not one XAUTOCLAIM call. A command runs on the
// session's own executor, not on the loop that read it, so it is still pending while it
// runs; one that takes longer than the min-idle would be handed back to the very
// consumer already executing it, and dispatched a second time alongside the first.
// Acknowledging the original does not retire the copy. XPENDING is the only form that
// says who holds an entry.
func (s *Streams) reclaimable(ctx context.Context, stream string) ([]string, error) {
	pending, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  ConsumerGroup,
		Idle:   s.opts.ClaimMinIdle,
		Start:  "-",
		End:    "+",
		Count:  s.opts.ReadCount,
	}).Result()
	switch {
	case errors.Is(err, redis.Nil):
		return nil, nil
	case isNoGroup(err):
		return nil, err
	case err != nil:
		return nil, fmt.Errorf("redisstream: list what is pending on %s: %w", stream, err)
	}

	ids := make([]string, 0, len(pending))
	for _, entry := range pending {
		if entry.Consumer == s.opts.Instance {
			continue
		}
		ids = append(ids, entry.ID)
	}
	return ids, nil
}

// streamsFor is the per-session command streams plus the control one. Control is
// always read: it carries `session.wake`, which is how a session with no owner gets
// one, so an instance that only listened to what it already owns would never hear it.
func (s *Streams) streamsFor(sids []string) []string {
	keys := s.client.Keys()
	streams := make([]string, 0, len(sids)+1)
	for _, sid := range sids {
		streams = append(streams, keys.Commands(sid))
	}
	return append(streams, keys.Control())
}

func (s *Streams) deliveries(result []redis.XStream) []transport.Delivery {
	var out []transport.Delivery
	for _, stream := range result {
		for _, message := range stream.Messages {
			command, err := protocol.ParseCommand(toFields(message.Values))
			if err != nil {
				// A frame this instance cannot read is not a frame a retry will fix,
				// and leaving it pending blocks nothing but fills the PEL forever. It
				// is acknowledged and dropped; the sender hears about it through the
				// reply it is waiting for timing out.
				s.ackUnreadable(stream.Stream, message.ID)
				continue
			}
			out = append(out, transport.Delivery{
				Command: command,
				Ack:     s.acker(stream.Stream, message.ID),
			})
		}
	}
	return out
}

func (s *Streams) acker(stream, id string) func(context.Context) error {
	return func(ctx context.Context) error {
		if err := s.client.XAck(ctx, stream, ConsumerGroup, id).Err(); err != nil {
			return fmt.Errorf("redisstream: ack %s on %s: %w", id, stream, err)
		}
		return nil
	}
}

func (s *Streams) ackUnreadable(stream, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.client.XAck(ctx, stream, ConsumerGroup, id).Err()
}

func newEntries(n int) []string {
	entries := make([]string, n)
	for i := range entries {
		entries[i] = ">"
	}
	return entries
}

func toValues(fields map[string]string) map[string]any {
	values := make(map[string]any, len(fields))
	for key, value := range fields {
		values[key] = value
	}
	return values
}

func toFields(values map[string]any) map[string]string {
	fields := make(map[string]string, len(values))
	for key, value := range values {
		if text, ok := value.(string); ok {
			fields[key] = text
		}
	}
	return fields
}
