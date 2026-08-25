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
	"sync"
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

	// inFlight is what this process has handed out and not finished with. A consumer
	// group cannot tell "still running here" from "left behind here" — both are
	// entries pending under this instance's name — and reclaiming the first duplicates
	// a running command while never reclaiming the second loses it in a fleet of one.
	inFlightMu sync.Mutex
	inFlight   map[string]struct{}

	// unrun is what was claimed and handed back without being carried out, with the age
	// each entry needs put back. XCLAIM resets an entry's idle to zero, so one released
	// afterwards is one no instance may reclaim for a whole ClaimMinIdle, however long
	// it had already been waiting: a wake that arrived late then waits the delay a
	// second time, and the session it names runs nowhere for both.
	unrunMu sync.Mutex
	unrun   map[string][]unrunEntry
}

// unrunEntry is one entry given back without being carried out, and the idle time it
// had before this process claimed it.
type unrunEntry struct {
	id   string
	idle time.Duration
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
	return &Streams{
		client: client, opts: opts, groups: newGroupCache(),
		inFlight: make(map[string]struct{}),
		unrun:    make(map[string][]unrunEntry),
	}, nil
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
	// First, because it is what makes this pass able to see what the last one gave back.
	s.restoreUnrun(ctx)
	return s.claim(ctx, s.streamsFor(sids), s.opts.ClaimMinIdle)
}

// restoreUnrun puts back the age XCLAIM erased on the entries this process took and gave
// back without carrying out.
//
// A claim resets an entry's idle to zero, so one released afterwards is unreclaimable by
// anybody for a whole ClaimMinIdle, however long it had been waiting before this process
// touched it. A wake that was already overdue then waits the delay twice over, and the
// session it names runs nowhere for both.
//
// Age is put back rather than the entry being kept for this instance alone: an entry
// only this process could take again is one it can starve every peer out of, by claiming
// it and giving it back on every heartbeat.
func (s *Streams) restoreUnrun(ctx context.Context) {
	s.unrunMu.Lock()
	pending := s.unrun
	s.unrun = make(map[string][]unrunEntry)
	s.unrunMu.Unlock()

	for stream, entries := range pending {
		here := s.pendingHere(ctx, stream)
		byIdle := make(map[time.Duration][]string)
		for _, entry := range entries {
			if s.running(stream, entry.id) {
				// Taken again since it was given back. Putting an age on an entry this
				// process is carrying out is how a peer comes to run it alongside.
				continue
			}
			if _, mine := here[entry.id]; !mine {
				// Acknowledged, or taken over by a peer while this instance was away for
				// longer than the delay. XCLAIM does not ask who holds an entry, so an
				// age put on this one would take it back out from under whoever is
				// running it, and for a wake that means retiring the only wake there was
				// while their adoption is still going.
				continue
			}
			byIdle[entry.idle] = append(byIdle[entry.idle], entry.id)
		}
		for idle, ids := range byIdle {
			// Raw, because go-redis models neither IDLE nor JUSTID on XCLAIM: IDLE is
			// the whole point, and JUSTID keeps the delivery counter from moving for a
			// hand-back that delivered nothing.
			args := make([]any, 0, len(ids)+8)
			args = append(args, "XCLAIM", stream, ConsumerGroup, s.opts.Instance, 0)
			for _, id := range ids {
				args = append(args, id)
			}
			args = append(args, "IDLE", idle.Milliseconds(), "JUSTID")
			if err := s.client.Do(ctx, args...).Err(); err != nil && !errors.Is(err, redis.Nil) {
				// Nothing to retry. The entries are still pending and still come back,
				// just no sooner than the delay: what is lost here is the age, not the
				// command.
				continue
			}
		}
	}
}

// ClaimSessions takes over what is pending on these sessions' own streams, and looks at
// nothing else.
//
// It is what a session just adopted needs before anything newer is read for it. The
// heartbeat's own reclaim is not enough on its own: it runs on a tick and over the
// sessions this instance already had, so a command abandoned by the previous owner would
// still be sitting pending while a fresh `>` read hands over commands that arrived after
// it. A `session.disconnect` running after the `session.connect` that replaced it leaves
// the account in the state nobody asked for, and per-session order is the one thing the
// single stream is for.
// It waits for nothing, unlike the heartbeat's reclaim. The min-idle there is what
// stops one instance taking work another is still doing, and that question is already
// settled here: this instance holds the lease, so whoever held these entries has lost
// it and is being torn down. Waiting the same delay would mean the abandoned command
// arrives after the newer one every time, which is the reordering this exists to
// prevent. What is left is the heartbeat or so between a lease moving and the old
// owner noticing, where a command could run twice; that is invariant 5's ground, and
// M2's.
func (s *Streams) ClaimSessions(ctx context.Context, sids []string) ([]transport.Delivery, error) {
	return s.claim(ctx, s.sessionStreams(sids), 0)
}

func (s *Streams) claim(ctx context.Context, streams []string, minIdle time.Duration) ([]transport.Delivery, error) {
	if len(streams) == 0 {
		return nil, nil
	}
	if err := s.groups.ensure(ctx, s.client, streams); err != nil {
		return nil, err
	}

	var claimed []transport.Delivery
	// Every early return past this point has to let go of what it already took: a
	// delivery handed out is one this process is counted as running, and one nobody
	// dispatched is a command that never runs again until the process restarts.
	fail := func(err error) ([]transport.Delivery, error) {
		for i := range claimed {
			if claimed[i].Release != nil {
				claimed[i].Release()
			}
		}
		return nil, err
	}
	for _, stream := range streams {
		ids, err := s.reclaimable(ctx, stream, minIdle)
		switch {
		case isNoGroup(err):
			s.groups.forget(stream)
			continue
		case err != nil:
			return fail(err)
		case len(ids) == 0:
			continue
		}

		messages, err := s.client.XClaim(ctx, &redis.XClaimArgs{
			Stream:   stream,
			Group:    ConsumerGroup,
			Consumer: s.opts.Instance,
			MinIdle:  minIdle,
			Messages: ids,
		}).Result()
		switch {
		case isNoGroup(err):
			s.groups.forget(stream)
			continue
		case err != nil && !errors.Is(err, redis.Nil):
			return fail(fmt.Errorf("redisstream: claim %s: %w", stream, err))
		}
		taken, ids := s.deliveriesWithIDs([]redis.XStream{{Stream: stream, Messages: messages}})
		if minIdle > 0 {
			s.rememberAge(stream, minIdle, taken, ids)
		}
		claimed = append(claimed, taken...)
	}
	return claimed, nil
}

// rememberAge has each delivery record, if it is given back unrun, the age this claim
// has just erased. A claim with no minimum idle takes nothing worth putting back: those
// entries are fresh, and the drain that took them asks for them again with no delay.
func (s *Streams) rememberAge(stream string, idle time.Duration, deliveries []transport.Delivery, ids []string) {
	for i := range deliveries {
		id := ids[i]
		release := deliveries[i].Release
		deliveries[i].Release = func() {
			if release != nil {
				release()
			}
			s.unrunMu.Lock()
			s.unrun[stream] = append(s.unrun[stream], unrunEntry{id: id, idle: idle})
			s.unrunMu.Unlock()
		}
	}
}

// pendingHere is what is still pending under this instance's own name on a stream.
//
// It is what tells a hand-back this process may put an age back on from one a peer has
// since taken: an entry released and left alone for longer than the delay is one anybody
// may have claimed, and XCLAIM transfers an entry without ever asking who holds it.
func (s *Streams) pendingHere(ctx context.Context, stream string) map[string]struct{} {
	here := make(map[string]struct{})
	start := "-"
	// Paged and capped the same way reclaimable is, and for the same reason: this runs
	// on the goroutine that renews every lease this instance holds.
	for range maxPendingPages {
		pending, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: stream, Group: ConsumerGroup, Consumer: s.opts.Instance,
			Start: start, End: "+", Count: s.opts.ReadCount,
		}).Result()
		if err != nil || len(pending) == 0 {
			// Nothing is put back rather than everything: an age not restored costs the
			// delay once more, an age restored on somebody else's entry costs a command
			// that runs twice.
			return here
		}
		for _, entry := range pending {
			here[entry.ID] = struct{}{}
		}
		if int64(len(pending)) < s.opts.ReadCount {
			return here
		}
		start = pending[len(pending)-1].ID
	}
	return here
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
func (s *Streams) reclaimable(ctx context.Context, stream string, minIdle time.Duration) ([]string, error) {
	ids := make([]string, 0, s.opts.ReadCount)
	start := "-"

	// Paged, because the filter is what makes a page yield nothing: entries this
	// process is still running stay pending for as long as they run, and a page full of
	// them would hide everything behind it on every heartbeat, forever. The page cap is
	// there so one heartbeat cannot walk an arbitrarily long list.
	for range maxPendingPages {
		pending, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: stream,
			Group:  ConsumerGroup,
			Idle:   minIdle,
			Start:  start,
			End:    "+",
			Count:  s.opts.ReadCount,
		}).Result()
		switch {
		case errors.Is(err, redis.Nil):
			return ids, nil
		case isNoGroup(err):
			return nil, err
		case err != nil:
			return nil, fmt.Errorf("redisstream: list what is pending on %s: %w", stream, err)
		case len(pending) == 0:
			return ids, nil
		}

		for _, entry := range pending {
			if entry.ID == start {
				// The page starts inclusively, so the entry the last page ended on
				// comes back once more.
				continue
			}
			// Only what this process is still carrying out is skipped. The consumer
			// name is the wrong question: a command this instance left behind before it
			// restarted, and a wake it deliberately left pending, are both pending under
			// this same name and both have to come back.
			if s.running(stream, entry.ID) {
				continue
			}
			ids = append(ids, entry.ID)
		}
		if len(ids) >= int(s.opts.ReadCount) || int64(len(pending)) < s.opts.ReadCount {
			return ids, nil
		}
		start = pending[len(pending)-1].ID
	}
	return ids, nil
}

// maxPendingPages bounds how much of the pending list one reclaim walks. The next
// heartbeat carries on from the front, so nothing is lost by stopping.
const maxPendingPages = 8

// streamsFor is the per-session command streams plus the control one. Control is
// always read: it carries `session.wake`, which is how a session with no owner gets
// one, so an instance that only listened to what it already owns would never hear it.
func (s *Streams) streamsFor(sids []string) []string {
	return append(s.sessionStreams(sids), s.client.Keys().Control())
}

// sessionStreams is the per-session command streams and nothing else.
func (s *Streams) sessionStreams(sids []string) []string {
	keys := s.client.Keys()
	streams := make([]string, 0, len(sids)+1)
	for _, sid := range sids {
		streams = append(streams, keys.Commands(sid))
	}
	return streams
}

func (s *Streams) deliveries(result []redis.XStream) []transport.Delivery {
	out, _ := s.deliveriesWithIDs(result)
	return out
}

// deliveriesWithIDs is deliveries plus the stream entry each one came from, which a
// claim needs to say what it took: the ids line up with the deliveries, and a frame that
// could not be read is in neither.
func (s *Streams) deliveriesWithIDs(result []redis.XStream) (out []transport.Delivery, ids []string) {
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
			held := s.hold(stream.Stream, message.ID)
			out = append(out, transport.Delivery{
				Command: command,
				Ack:     s.acker(stream.Stream, message.ID, held),
				Release: held,
			})
			ids = append(ids, message.ID)
		}
	}
	return out, ids
}

func (s *Streams) acker(stream, id string, release func()) func(context.Context) error {
	return func(ctx context.Context) error {
		defer release()
		if err := s.client.XAck(ctx, stream, ConsumerGroup, id).Err(); err != nil {
			return fmt.Errorf("redisstream: ack %s on %s: %w", id, stream, err)
		}
		return nil
	}
}

// hold records that this process has handed an entry out and is not finished with it.
// The returned function is what says it is: it runs once, on the ack or on the release,
// whichever comes.
func (s *Streams) hold(stream, id string) func() {
	key := stream + "\x00" + id
	s.inFlightMu.Lock()
	s.inFlight[key] = struct{}{}
	s.inFlightMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			s.inFlightMu.Lock()
			delete(s.inFlight, key)
			s.inFlightMu.Unlock()
		})
	}
}

// running reports whether this process is still carrying an entry out.
func (s *Streams) running(stream, id string) bool {
	s.inFlightMu.Lock()
	defer s.inFlightMu.Unlock()
	_, held := s.inFlight[stream+"\x00"+id]
	return held
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
