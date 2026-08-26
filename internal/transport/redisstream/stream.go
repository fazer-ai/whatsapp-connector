// Package redisstream carries the protocol over Redis Streams.
//
// The layout is the contract's: events fan out to `wa:events:<shard>` with a session
// always landing on the same shard, commands arrive per session on `wa:cmd:<sid>` and
// fleet-wide on `wa:control`, and an RPC answer is a single-element list the caller
// blocks on.
package redisstream

import (
	"context"
	"encoding/json"
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

// DefaultClaimMaxIdle is how long an entry may sit pending before this transport stops
// taking it over and retires it to the dead letter list instead.
//
// It is the idempotency TTL, and the two have to move together: what makes a reclaim
// safe is the record of whether the command already ran, and that record is what
// expires. An entry older than the record is one no instance can decide about, so
// carrying it out would be a guess with a side effect on the other end of it, and the
// dead letter list is where the contract puts what a human has to look at.
const DefaultClaimMaxIdle = redisx.DefaultIdempotencyTTL

// dlqMaxLen bounds the dead letter list. It is a record for an operator, not a queue,
// and one nobody reads must not be able to fill the instance.
const dlqMaxLen = 1000

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
	ClaimMaxIdle  time.Duration
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
	if opts.ClaimMaxIdle <= 0 {
		opts.ClaimMaxIdle = DefaultClaimMaxIdle
	}
	if opts.ClaimMaxIdle <= opts.ClaimMinIdle {
		return nil, fmt.Errorf(
			"redisstream: entries are retired after %s and not taken over before %s, so none is ever reclaimed",
			opts.ClaimMaxIdle, opts.ClaimMinIdle)
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
	var out []transport.Delivery
	for _, stream := range result {
		taken, ids := s.deliveriesWithIDs([]redis.XStream{stream})
		// A read entry starts at zero idle and stays there until somebody touches it, so
		// one this instance gives back unrun is claimable by nobody — not by `>`, which
		// returns only what no consumer has taken, and not by a claim, which will not
		// look at it for a whole delay. Recording the delay as its age has the next
		// reclaim see it instead, which for a command carrying a deadline is the
		// difference between running late and expiring unrun.
		s.rememberAge(stream.Stream, s.opts.ClaimMinIdle, taken, ids)
		out = append(out, taken...)
	}
	return out, nil
}

// Claim takes over commands another instance read and never acknowledged, which is
// what happens when it is killed between reading a command and carrying it out.
func (s *Streams) Claim(ctx context.Context, sids []string) ([]transport.Delivery, error) {
	// First, because it is what makes this pass able to see what the last one gave back.
	s.restoreUnrun(ctx)
	return s.claim(ctx, s.sessionStreams(sids), s.opts.ClaimMinIdle)
}

// ClaimControl takes over what is pending on the control stream.
//
// Apart from the session streams, and not merely ahead of them. A claim that fails over
// any one stream releases everything it took, so a control entry taken alongside a window
// of sessions is thrown away whenever a session in that window fails — and a window that
// spends the whole deadline never reaches the control stream at all. Either way the
// casualty is `session.wake`, which is the command that puts a session from a dead
// instance back on an instance: it would then be the one command that never runs on
// exactly the Redis that makes it necessary.
func (s *Streams) ClaimControl(ctx context.Context) ([]transport.Delivery, error) {
	s.restoreUnrun(ctx)
	return s.claim(ctx, []string{s.client.Keys().Control()}, s.opts.ClaimMinIdle)
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
		byIdle := make(map[time.Duration][]string)
		for _, entry := range entries {
			if s.running(stream, entry.id) {
				// Taken again since it was given back. Putting an age on an entry this
				// process is carrying out is how a peer comes to run it alongside.
				continue
			}
			byIdle[entry.idle] = append(byIdle[entry.idle], entry.id)
		}
		for idle, ids := range byIdle {
			args := make([]any, 0, len(ids)+3)
			args = append(args, ConsumerGroup, s.opts.Instance, idle.Milliseconds())
			for _, id := range ids {
				args = append(args, id)
			}
			if err := restoreAgeScript.Run(ctx, s.client, []string{stream}, args...).Err(); err != nil {
				// Nothing to retry. The entries are still pending and still come back,
				// just no sooner than the delay: what is lost here is the age, not the
				// command.
				continue
			}
		}
	}
}

// restoreAgeScript puts an entry's idle time back, and only while this instance is still
// the one holding it.
//
// One operation, because the two halves cannot be separated: XCLAIM transfers an entry
// without ever asking who holds it, so between a check that said "still mine" and a claim
// that acts on it a peer can take the entry and start carrying it out. The age would then
// pull it back mid-flight, the command would run twice, and for a wake that means
// retiring the only wake there was while the peer's adoption is still going.
//
// Raw XCLAIM inside, because go-redis models neither IDLE nor JUSTID: IDLE is the whole
// point, and JUSTID keeps the delivery counter from moving for a hand-back that delivered
// nothing.
var restoreAgeScript = redis.NewScript(`
local group, consumer, idle = ARGV[1], ARGV[2], ARGV[3]
for i = 4, #ARGV do
  local id = ARGV[i]
  local pending = redis.call("XPENDING", KEYS[1], group, "IDLE", 0, id, id, 1)
  if pending[1] and pending[1][2] == consumer then
    redis.call("XCLAIM", KEYS[1], group, consumer, 0, id, "IDLE", idle, "JUSTID")
  end
end
return 1
`)

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
		ids, expired, err := s.reclaimable(ctx, stream, minIdle)
		switch {
		case isNoGroup(err):
			s.groups.forget(stream)
			continue
		case err != nil:
			return fail(err)
		}
		s.retire(ctx, stream, expired)
		if len(ids) == 0 {
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

// rememberAge has each delivery record, if it is given back unrun, the age it should
// come back with. For a claim that is the age the claim erased; for a read it is the
// reclaim delay itself, because a read entry has no age to erase and one given back
// would otherwise be invisible to both halves of this transport: `>` returns only what
// nobody has taken, and a claim will not look at it until the delay has passed.
//
// Forfeit is the same release without the record, which is what an instance that took
// its turn and failed gives back.
func (s *Streams) rememberAge(stream string, idle time.Duration, deliveries []transport.Delivery, ids []string) {
	for i := range deliveries {
		id := ids[i]
		release := deliveries[i].Release
		deliveries[i].Forfeit = release
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

// reclaimable is the pending entries worth taking over: idle long enough, held by
// somebody else, and not so old that nothing can be known about them any more. The
// second list is that last group, which the caller retires rather than runs.
//
// The second half is the reason this is not one XAUTOCLAIM call. A command runs on the
// session's own executor, not on the loop that read it, so it is still pending while it
// runs; one that takes longer than the min-idle would be handed back to the very
// consumer already executing it, and dispatched a second time alongside the first.
// Acknowledging the original does not retire the copy. XPENDING is the only form that
// says who holds an entry.
func (s *Streams) reclaimable(ctx context.Context, stream string, minIdle time.Duration) (ids, expired []string, err error) {
	ids = make([]string, 0, s.opts.ReadCount)
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
			return ids, expired, nil
		case isNoGroup(err):
			return nil, expired, err
		case err != nil:
			return nil, expired, fmt.Errorf("redisstream: list what is pending on %s: %w", stream, err)
		case len(pending) == 0:
			return ids, expired, nil
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
			if entry.Idle > s.opts.ClaimMaxIdle {
				// Older than anything that could say whether it already ran. Taking it
				// over would be carrying out a command on a guess, so it is retired
				// instead, which is the one outcome that neither runs it twice nor
				// leaves it pending for the next heartbeat to reconsider.
				expired = append(expired, entry.ID)
				continue
			}
			ids = append(ids, entry.ID)
		}
		if len(ids) >= int(s.opts.ReadCount) || int64(len(pending)) < s.opts.ReadCount {
			return ids, expired, nil
		}
		start = pending[len(pending)-1].ID
	}
	return ids, expired, nil
}

// retire takes entries out of the pending list for good and leaves a copy of each on
// the dead letter list, which is where the contract keeps what the connector would not
// carry out.
//
// The copy is written before the acknowledgement, so a failure in between costs a
// duplicate record rather than a command nobody can see any more. An entry the stream
// has already trimmed away has no copy to make, and the acknowledgement is still worth
// sending: it is what clears the pending list of an id that no longer exists.
func (s *Streams) retire(ctx context.Context, stream string, ids []string) {
	keys := s.client.Keys()
	for _, id := range ids {
		entries, err := s.client.XRange(ctx, stream, id, id).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			// Nothing is retired without a record of it. The entry stays pending and
			// the next heartbeat comes back to it.
			continue
		}
		if len(entries) > 0 {
			record, err := json.Marshal(dlqEntry{
				Stream: stream, ID: id, Reason: "pending longer than the record of whether it ran",
				MaxIdle: s.opts.ClaimMaxIdle.String(), Fields: toFields(entries[0].Values),
			})
			if err != nil {
				continue
			}
			if err := s.client.LPush(ctx, keys.DLQCommands(), record).Err(); err != nil {
				continue
			}
			s.client.LTrim(ctx, keys.DLQCommands(), 0, dlqMaxLen-1)
		}
		_ = s.client.XAck(ctx, stream, ConsumerGroup, id).Err()
	}
}

// dlqEntry is one retired command as an operator reads it: where it was, how long it
// sat there, and the frame itself.
type dlqEntry struct {
	Stream  string            `json:"stream"`
	ID      string            `json:"id"`
	Reason  string            `json:"reason"`
	MaxIdle string            `json:"max_idle"`
	Fields  map[string]string `json:"fields"`
}

// maxPendingPages bounds how much of the pending list one reclaim walks. The next
// heartbeat carries on from the front, so nothing is lost by stopping.
const maxPendingPages = 8

// streamsFor is the per-session command streams plus the control one. Control is
// always read: it carries `session.wake`, which is how a session with no owner gets
// one, so an instance that only listened to what it already owns would never hear it.
// streamsFor is what one read covers: the sessions this instance owns and the stream
// addressed to no session in particular. A read blocks on all of them at once, so there
// is nothing for the control stream to be starved by; a claim walks them one at a time
// and gives up on all of them together, which is why it keeps the two apart.
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

// deliveriesWithIDs is the deliveries plus the stream entry each one came from, which
// both callers need to say what they took: the ids line up with the deliveries, and a
// frame that could not be read is in neither.
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
		if err := s.client.XAck(ctx, stream, ConsumerGroup, id).Err(); err != nil {
			// Still marked as being carried out here, on purpose. The command ran, and
			// the entry is pending only because the acknowledgement did not land: letting
			// go of the marker would have the next reclaim hand it back to this same
			// process and run it a second time, side effects and all.
			return fmt.Errorf("redisstream: ack %s on %s: %w", id, stream, err)
		}
		release()
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
