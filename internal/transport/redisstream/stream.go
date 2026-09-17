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
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

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
	Instance string
	// Logger is where a diagnosis this layer can only make once goes. The transport is
	// otherwise silent by design -- it answers its callers and they decide what is worth
	// saying -- but a trim that cut undelivered commands leaves a reading the next read
	// destroys, so there is nobody left to tell afterwards.
	Logger        zerolog.Logger
	EventMaxLen   int64
	CommandMaxLen int64
	Block         time.Duration
	ReplyTTL      time.Duration
	ClaimMinIdle  time.Duration
	ReadCount     int64
	// ReadBackMaxAge is the oldest an entry on the control stream may be and still be
	// handed out by a read that recovers it, rather than left to a claim. It has to be
	// shorter than ClaimMinIdle, and the zero value asks for half of it. See Read.
	ReadBackMaxAge time.Duration
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

	// marks is, per stream, how far Read has read this consumer's pending history,
	// claimedPast what a claim handed out beyond that point, and received the payloads `>`
	// answered with beyond it. See Read.
	marksMu     sync.Mutex
	marks       map[string]string
	claimedPast map[string]map[string]struct{}
	received    map[string]map[string]map[string]any
	// pagesOut counts history pages sent and not yet looked at, and lettingGo holds what
	// was acknowledged meanwhile, let go once none is left.
	pagesOut  int
	lettingGo map[string][]string

	// started is when this process began reading, and anything pending here that was
	// delivered before it is a predecessor's. See Read.
	started time.Time

	// afterPage runs once a page has been looked at and before what it hands out is, and
	// afterList once a claim has listed what it may take and before it keeps any of it
	// apart. Both are nil outside tests: they are where a read and a claim running
	// alongside each other would interleave.
	afterPage func()
	afterList func()
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
//
//nolint:gocritic // Options is heavy because it now carries a zerolog.Logger, which is designed to be copied
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
	if opts.ReadBackMaxAge <= 0 {
		opts.ReadBackMaxAge = opts.ClaimMinIdle / 2
	}
	if opts.ReadBackMaxAge >= opts.ClaimMinIdle {
		// A wake handed out that old would be claimable by a peer the moment it is handed
		// out, while this instance is still adopting its session. That is a deployment
		// bug, not a tuning choice.
		return nil, fmt.Errorf("redisstream: ReadBackMaxAge (%s) must be shorter than ClaimMinIdle (%s)",
			opts.ReadBackMaxAge, opts.ClaimMinIdle)
	}
	return &Streams{
		started: time.Now(),
		client:  client, opts: opts, groups: newGroupCache(),
		inFlight:    make(map[string]struct{}),
		unrun:       make(map[string][]unrunEntry),
		marks:       make(map[string]string),
		claimedPast: make(map[string]map[string]struct{}),
		received:    make(map[string]map[string]map[string]any),
		lettingGo:   make(map[string][]string),
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
//
// `reply_to` is the key itself, not a command id to build one from: the contract's own
// command frames carry it fully spelled (`wa:reply:cmd_000035`), because the client is
// the one blocked on it and it is the client that chose where to wait. Prefixing it
// again here answered at `wa:reply:wa:reply:<id>`, which nobody reads, so every RPC in
// the fleet timed out while the command it carried had already been carried out.
func (s *Streams) Reply(ctx context.Context, replyTo string, reply protocol.Reply) error {
	// The destination is the client's to choose and this connector's to check. Everything
	// else under the prefix is fleet state -- the session set, the leases, the streams --
	// and an answer written at one of those names would leave a TTL on it even where the
	// push itself fails on the type, since both run in one transaction.
	if !s.client.Keys().IsReply(replyTo) {
		return fmt.Errorf("redisstream: %q is not a reply destination", replyTo)
	}
	body, err := marshalReply(reply)
	if err != nil {
		return err
	}
	_, err = s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.LPush(ctx, replyTo, body)
		pipe.PExpire(ctx, replyTo, s.opts.ReplyTTL)
		return nil
	})
	if err != nil {
		return fmt.Errorf("redisstream: reply to %s: %w", replyTo, err)
	}
	return nil
}

// blockWithin is how long the server may hold this read: the configured block, unless
// the caller brought a deadline too near to fit one.
//
// Shortening it here rather than letting the caller decide whether a whole block fits.
// A caller can only refuse, and a loop whose other work consistently leaves less than a
// block would then stop reading `>` altogether while commands pile up unread -- the
// read starving on a rule meant to protect it. Cutting the block instead means a narrow
// window costs a short read, never no read.
//
// A third of what is left stays unspent, for the round trips the block does not cover:
// ensuring a group on a stream not seen before, the answer's own transit, and reading
// back what the answer moved in. A read cut off by the deadline mid-flight is the failure
// worth paying that for -- the server may have just moved a command into this consumer's
// pending list when the connection dies, and although the next read hands it out, that is
// a window later than it could have run.
//
// A window too thin to name a block in milliseconds is the one case with no read at
// all: what is left cannot express the reserve, and a non-blocking read would spend a
// whole round trip with nothing held back for the answer -- the very failure the
// reserve is for. Skipping starves nothing, unlike a gate measured in blocks: a window
// down to its last milliseconds is a window already over, and the loop is on its way
// back to the ticker to start a fresh one.
func (s *Streams) blockWithin(ctx context.Context) (time.Duration, bool) {
	deadline, bounded := ctx.Deadline()
	if !bounded {
		return s.opts.Block, true
	}
	block := min(s.opts.Block, time.Until(deadline)*2/3)
	if block < time.Millisecond {
		return 0, false
	}
	return block, true
}

// Read returns the commands waiting for the sessions this instance owns, plus the
// fleet-wide ones.
//
// It takes two trips, and neither hands out what the other returned. `>` asks for entries
// no consumer in the group has taken yet, which moves them into this consumer's pending
// list, and waits for some if there are none. What is handed out is then read back from
// that list: this consumer's own history, past the mark on each stream.
//
// The answer to `>` cannot be what is handed out, because it can go missing with its
// entries already moved (#202). Held past the window, the read returns the deadline's
// error. Dropped with the connection, go-redis sends the read again on a fresh one, and the
// read comes back with whatever arrived since -- or with nothing and no error at all. The
// entries of the lost answer are then pending under this instance's name with nobody here
// having seen them: `>` will not return them again, since somebody has taken them, and a
// claim will not look at them for a whole ClaimMinIdle, while newer commands for the same
// session are read and run ahead of them. The history has them either way, and in stream
// order, ahead of anything newer.
//
// The mark is how far that history has been read: the newest entry of the last page read
// back. Every entry pending under this consumer up to it was on a page, and so was handed
// out, or had been handed out already -- still running, given back, forfeited -- and none of
// that may come back through a read: a running command would run twice, and one given back
// is a claim's to hand out and would otherwise jump the queue on every read. Anything `>`
// moves in is past it, since `>` only returns what is newer than every entry any read has
// seen. Only a read moves it. A claim hands out entries past it too -- a peer's, or one a
// peer gave back -- but a claim says nothing about the entries before the one it took, and
// one of those may be a lost answer's; those claimed entries are kept apart instead, and a
// page skips them. Claims run on another goroutine, and so do the acknowledgements of what
// they hand out, so both have to be visible to a page already on its way: a claim keeps its
// entries apart before it sends XCLAIM, and an acknowledgement stops keeping one apart only
// once no page is on its way. What a claim moved here without handing it out -- its answer
// lost, or sent again and come back empty -- stays apart too, for a later claim to hand out
// as the redelivery it is. And the history is this consumer's alone, so what is pending
// under a peer, which may be running there right now, is never read. On a stream no read has been
// through yet, the mark is the start. What an earlier process under this name left pending
// is not this process's lost answer, though: it was delivered before this one started, it
// may be a wake for a session whose lease that process still holds, and the claim delay,
// which outlasts a lease, is what it waits for. The history skips it, for a claim. A group recreated under the read, which starts over at
// the beginning of the stream, starts the mark over too: `>` handing out an entry at or
// below it is how that shows.
//
// Reading back only looks. Nothing on this path resets an entry's idle time, which is what
// a claim goes by: an instance whose answers keep getting lost must not keep what it never
// sees looking freshly delivered, or a healthy peer would never take a wake from it. The
// cost falls on the control stream, the one stream a peer claims without holding a lease
// on it. A wake recovered late keeps the age it gathered while its answer was lost, and
// handed out like that it would be claimable by a peer early, while this instance is still
// adopting the session -- and the peer, finding a live lease, would retire the only wake
// there was. So on the control stream a read hands out a recovered entry only while it is
// younger than ReadBackMaxAge, counting the trip its page took, and leaves an older one to a claim, which resets the age as it
// hands it out. What the `>` of the same read carried is exempt: it was delivered a moment
// ago. The streams of sessions have no such limit, since a peer claims them only once it
// holds their lease, and order is what they are for.
//
// The history reads an entry's payload back from the stream, and a producer trimming it can
// remove the entry in between. What `>` answered with is kept until a page passes it and
// stands in for an entry no longer there, however old: a claim could only retire it.
//
// A history read that fails leaves what `>` moved in pending above the mark, for the
// history of the next read to hand out. Anything already taken and not acknowledged below
// the mark is Claim's business.
func (s *Streams) Read(ctx context.Context, sids []string) ([]transport.Delivery, error) {
	deliveries, err := s.read(ctx, sids)
	// Every trip of the read is measured against the same window, so the question of
	// which one ran out of it is one the caller cannot use and the operator cannot see.
	return deliveries, spentWindow(ctx, err)
}

func (s *Streams) read(ctx context.Context, sids []string) ([]transport.Delivery, error) {
	streams := s.streamsFor(sids)
	if len(streams) == 0 {
		return nil, nil
	}
	// Before the group ensure, so a window already over does not pay for it: creating a
	// group on a stream not seen before is a round trip of its own.
	if _, room := s.blockWithin(ctx); !room {
		return nil, nil
	}
	fresh, err := s.groups.ensure(ctx, s.client, streams)
	if err != nil {
		return nil, err
	}
	// Before the read, because the read is what destroys the reading this is taken from.
	s.reportTrimmed(ctx, fresh)
	// And again after it, because that trip spends the same window the block is measured
	// against: a block decided before it can outlive the deadline by whatever the ensure
	// took, which is the severed read this reserve exists to prevent.
	block, room := s.blockWithin(ctx)
	if !room {
		return nil, nil
	}

	args := &redis.XReadGroupArgs{
		Group:    ConsumerGroup,
		Consumer: s.opts.Instance,
		Streams:  slices.Concat(streams, newEntries(len(streams))),
		Count:    s.opts.ReadCount,
		Block:    block,
	}
	answer, err := s.client.XReadGroup(ctx, args).Result()
	switch {
	case errors.Is(err, redis.Nil):
		// Nothing new, and the history is still worth the trip: a read that lost its answer
		// and was sent again comes back exactly like this.
	case isNoGroup(err):
		// The group went away under us (a flush, an operator). Forgetting the streams
		// is what makes the next call recreate it instead of failing forever.
		s.groups.forgetAll(streams)
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("redisstream: read commands: %w", err)
	}
	// Per stream, because entry ids are unique within a stream, not across streams.
	answered := make(map[string]map[string]struct{})
	s.marksMu.Lock()
	// Only a stream still read will have a page carry what it received. A page empties a
	// stream's share as it passes it, so this is almost always nothing to look at.
	if len(s.received) > 0 {
		reading := make(map[string]struct{}, len(streams))
		for _, stream := range streams {
			reading[stream] = struct{}{}
		}
		for stream := range s.received {
			if _, still := reading[stream]; !still {
				delete(s.received, stream)
			}
		}
	}
	for _, stream := range answer {
		// `>` returns only what is past the group's cursor, which is never behind a mark this
		// group's pages set. An entry at or below the mark means the group was recreated,
		// starting over at the beginning of the stream, and the mark was the old group's.
		if len(stream.Messages) > 0 && !entryAfter(stream.Messages[0].ID, s.marks[stream.Stream]) {
			delete(s.marks, stream.Stream)
		}
		// Kept until a page passes it: a producer trimming the stream can remove an entry
		// before the history reads it back, and the payload is here already.
		if len(stream.Messages) > 0 && s.received[stream.Stream] == nil {
			s.received[stream.Stream] = make(map[string]map[string]any)
		}
		for _, message := range stream.Messages {
			s.received[stream.Stream][message.ID] = message.Values
		}
		answered[stream.Stream] = make(map[string]struct{}, len(stream.Messages))
		for _, message := range stream.Messages {
			answered[stream.Stream][message.ID] = struct{}{}
		}
	}
	s.marksMu.Unlock()
	return s.readHistory(ctx, streams, answered)
}

// readHistory hands out what is pending under this consumer past the mark on each stream.
// It never waits, and it only looks: an answer it loses leaves everything as it found it,
// for the next one. answered is what the `>` of the same read carried, per stream.
func (s *Streams) readHistory(ctx context.Context, streams []string, answered map[string]map[string]struct{}) ([]transport.Delivery, error) {
	args := make([]any, 0, len(streams)+3)
	args = append(args, ConsumerGroup, s.opts.Instance, s.opts.ReadCount)
	s.marksMu.Lock()
	for _, stream := range streams {
		args = append(args, s.marks[stream])
	}
	// From here until the page has been looked at, an entry acknowledged may still be on
	// it, so it stays kept apart until then.
	s.pagesOut++
	s.marksMu.Unlock()
	defer func() {
		s.marksMu.Lock()
		defer s.marksMu.Unlock()
		s.pagesOut--
		if s.pagesOut > 0 {
			return
		}
		for stream, ids := range s.lettingGo {
			s.forget(stream, ids)
			delete(s.lettingGo, stream)
		}
	}()

	sent := time.Now()
	// Anything idle for longer than this was delivered before this process started. Taken
	// as the page is sent, the script runs later still, so an entry this process was handed
	// right at its start may count as a predecessor's, and waits for a claim.
	uptime := sent.Sub(s.started)
	raw, err := pendingPastScript.Run(ctx, s.client, streams, args...).Result()
	if err != nil {
		return nil, fmt.Errorf("redisstream: read commands back: %w", err)
	}
	// The idle times are as of when the script ran, somewhere in this trip; counting the
	// whole trip errs toward leaving a wake to a claim.
	trip := time.Since(sent)
	pages, err := parsePendingPast(raw)
	if err != nil {
		return nil, err
	}

	control := s.client.Keys().Control()
	var out []transport.Delivery
	for _, page := range pages {
		handing := make([]pendingEntry, 0, len(page.entries))
		s.marksMu.Lock()
		for _, entry := range page.entries {
			if _, kept := s.claimedPast[page.stream][entry.ID]; kept {
				continue
			}
			// Gone from the stream, an entry is no claim's to recover: a claim finds no payload
			// and retires it as unreadable. Handed out late from what `>` answered with, it
			// runs, so the age limit does not apply to it.
			values, cached := s.received[page.stream][entry.ID]
			trimmed := cached && len(entry.Values) == 0
			if trimmed {
				entry.Values = values
			}
			_, entry.fresh = answered[page.stream][entry.ID]
			if !entry.fresh && !trimmed && entry.idle > uptime {
				continue
			}
			if page.stream == control && !entry.fresh && !trimmed && entry.idle+trip > s.opts.ReadBackMaxAge {
				continue
			}
			handing = append(handing, entry)
		}
		// Past everything on the page, what was handed out and what was skipped alike: a
		// page is every entry pending here up to its last, and what it skipped belongs to
		// a claim.
		if last := len(page.entries); last > 0 {
			s.marks[page.stream] = page.entries[last-1].ID
			for id := range s.claimedPast[page.stream] {
				if !entryAfter(id, s.marks[page.stream]) {
					delete(s.claimedPast[page.stream], id)
				}
			}
			for id := range s.received[page.stream] {
				if !entryAfter(id, s.marks[page.stream]) {
					delete(s.received[page.stream], id)
				}
			}
			if len(s.received[page.stream]) == 0 {
				delete(s.received, page.stream)
			}
		}
		// Marked as running before the lock is let go, which is what a claim checks under
		// it: past the mark the entry is no longer kept apart, and a claim in the gap would
		// take it too.
		held := make([]func(), len(handing))
		for i, entry := range handing {
			held[i] = s.hold(page.stream, entry.ID)
		}
		s.marksMu.Unlock()
		if s.afterPage != nil {
			s.afterPage()
		}

		// Handed out as read for the first time, whichever answer it first arrived in: nobody
		// has run it, and whoever sent it is still waiting. Unless an answer lost left it
		// unseen past the claim delay, when it is what a claim would have handed out, a
		// redelivery whose sender may have stopped listening.
		var taken []transport.Delivery
		var ids []string
		for i, entry := range handing {
			aged := !entry.fresh && entry.idle+trip >= s.opts.ClaimMinIdle
			// `!entry.fresh` alone is the fact the metric wants: it came out of the
			// pending list, whatever its age. `aged` is that plus a delay, and the two
			// come apart on the entry that comes back every block and never grows old.
			if delivery, ok := s.deliver(page.stream, entry.XMessage, held[i], aged, pending{before: !entry.fresh}); ok {
				taken, ids = append(taken, delivery), append(ids, entry.ID)
			}
		}
		// A read entry starts at zero idle and stays there until somebody touches it, so
		// one this instance gives back unrun is claimable by nobody — not by `>`, which
		// returns only what no consumer has taken, and not by a claim, which will not
		// look at it for a whole delay. Recording the delay as its age has the next
		// reclaim see it instead, which for a command carrying a deadline is the
		// difference between running late and expiring unrun.
		s.rememberAge(page.stream, s.opts.ClaimMinIdle, taken, ids)
		out = append(out, taken...)
	}
	return out, nil
}

// keepApart keeps apart what a claim is about to take past a stream's mark, and returns what
// is still to take. A read marks what it hands out as running under the same lock, so an
// entry a read has just handed out, which the list the claim works from may predate, is
// dropped here. See Read.
func (s *Streams) keepApart(stream string, ids []string) []string {
	s.marksMu.Lock()
	defer s.marksMu.Unlock()
	ids = slices.DeleteFunc(ids, func(id string) bool { return s.running(stream, id) })
	for _, id := range ids {
		if !entryAfter(id, s.marks[stream]) {
			continue
		}
		if s.claimedPast[stream] == nil {
			s.claimedPast[stream] = make(map[string]struct{})
		}
		s.claimedPast[stream][id] = struct{}{}
	}
	return ids
}

// letGo stops keeping anything for acknowledged entries: they are off the pending list, and
// no page sent from now on carries them to be passed. One already on its way may, so while any is,
// they are let go when the last one has been looked at.
func (s *Streams) letGo(stream string, ids ...string) {
	s.marksMu.Lock()
	defer s.marksMu.Unlock()
	if s.pagesOut > 0 {
		s.lettingGo[stream] = append(s.lettingGo[stream], ids...)
		return
	}
	s.forget(stream, ids)
}

// forget drops what is kept for entries no page will carry again: kept apart, and the
// payload `>` answered with. marksMu is held.
func (s *Streams) forget(stream string, ids []string) {
	for _, id := range ids {
		delete(s.claimedPast[stream], id)
		delete(s.received[stream], id)
	}
	if len(s.received[stream]) == 0 {
		delete(s.received, stream)
	}
}

// letGoRetired stops keeping apart what a claim asked for, did not get, and is not pending
// here: a peer took it, or ran it and acknowledged it, between the list and the claim.
// Nothing here would ever let go of it otherwise. What is pending here was moved by the
// claim all the same, its answer lost, and stays apart. A failure leaves everything kept
// apart, which costs memory until a page passes it, and nothing else.
func (s *Streams) letGoRetired(ctx context.Context, stream string, asked []string, took []redis.XMessage) {
	args := make([]any, 0, len(asked)+2)
	args = append(args, ConsumerGroup, s.opts.Instance)
	for _, id := range asked {
		if !slices.ContainsFunc(took, func(message redis.XMessage) bool { return message.ID == id }) {
			args = append(args, id)
		}
	}
	gone, err := notPendingHereScript.Run(ctx, s.client, []string{stream}, args...).StringSlice()
	if err != nil {
		return
	}
	s.letGo(stream, gone...)
}

// notPendingHereScript returns which of the given entries are not pending under a consumer.
var notPendingHereScript = redis.NewScript(`
local group, consumer = ARGV[1], ARGV[2]
local gone = {}
for i = 3, #ARGV do
  local pending = redis.call("XPENDING", KEYS[1], group, "IDLE", 0, ARGV[i], ARGV[i], 1)
  if not (pending[1] and pending[1][2] == consumer) then
    gone[#gone + 1] = ARGV[i]
  end
end
return gone
`)

// pendingPastScript returns, for each stream, up to a count of the entries pending under
// one consumer past a mark, oldest first, each with its idle time in milliseconds. An empty
// mark is the start of the stream.
//
// Not XREADGROUP with an id, which is the command made for reading a consumer's history.
// That one delivers what it returns again, and a delivery sets the entry's idle time back
// to zero. A history read whose answer keeps being lost would then keep an entry this
// process never hands out looking freshly delivered, for as long as its reads keep failing,
// and a claim goes by idle time: a healthy peer would never take a wake from an instance
// whose reads have stopped arriving, which is the instance it exists to route around.
// XPENDING and XRANGE only look.
//
// An entry pending but no longer in the stream -- trimmed, or deleted -- comes back with no
// fields, as XREADGROUP returns it, and is dropped as unreadable.
var pendingPastScript = redis.NewScript(`
local group, consumer, count = ARGV[1], ARGV[2], tonumber(ARGV[3])
local out = {}
for i, key in ipairs(KEYS) do
  local mark = ARGV[3 + i]
  local start = "-"
  if mark ~= "" then start = mark end
  local entries = {}
  for _, pending in ipairs(redis.call("XPENDING", key, group, start, "+", count + 1, consumer)) do
    if #entries == count then break end
    if pending[1] ~= mark then
      local found = redis.call("XRANGE", key, pending[1], pending[1])
      local fields = {}
      if found[1] then fields = found[1][2] end
      entries[#entries + 1] = {pending[1], fields, pending[3]}
    end
  end
  out[#out + 1] = {key, entries}
end
return out
`)

// pendingPage is one stream's part of pendingPastScript's answer.
type pendingPage struct {
	stream  string
	entries []pendingEntry
}

type pendingEntry struct {
	redis.XMessage
	idle time.Duration
	// fresh is whether the `>` of the same read carried it.
	fresh bool
}

// parsePendingPast reads pendingPastScript's answer.
func parsePendingPast(raw any) ([]pendingPage, error) {
	malformed := func() ([]pendingPage, error) {
		return nil, fmt.Errorf("redisstream: read commands back: unexpected answer %T", raw)
	}
	streams, ok := raw.([]any)
	if !ok {
		return malformed()
	}
	out := make([]pendingPage, 0, len(streams))
	for _, item := range streams {
		pair, ok := item.([]any)
		if !ok || len(pair) != 2 {
			return malformed()
		}
		name, nameOK := pair[0].(string)
		entries, entriesOK := pair[1].([]any)
		if !nameOK || !entriesOK {
			return malformed()
		}
		page := pendingPage{stream: name, entries: make([]pendingEntry, 0, len(entries))}
		for _, item := range entries {
			entry, ok := item.([]any)
			if !ok || len(entry) != 3 {
				return malformed()
			}
			id, idOK := entry[0].(string)
			flat, flatOK := entry[1].([]any)
			idle, idleOK := entry[2].(int64)
			if !idOK || !flatOK || !idleOK || len(flat)%2 != 0 {
				return malformed()
			}
			values := make(map[string]any, len(flat)/2)
			for i := 0; i < len(flat); i += 2 {
				field, ok := flat[i].(string)
				if !ok {
					return malformed()
				}
				values[field] = flat[i+1]
			}
			page.entries = append(page.entries, pendingEntry{
				XMessage: redis.XMessage{ID: id, Values: values},
				idle:     time.Duration(idle) * time.Millisecond,
			})
		}
		out = append(out, page)
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
	fresh, err := s.groups.ensure(ctx, s.client, streams)
	if err != nil {
		return nil, err
	}
	// The other door onto a stream this process has not touched yet, and it moves the
	// group just the same, so the reading has to be taken here too.
	s.reportTrimmed(ctx, fresh)

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
		ids, was, err := s.reclaimable(ctx, stream, minIdle)
		switch {
		case isNoGroup(err):
			s.groups.forget(stream)
			continue
		case err != nil:
			return fail(err)
		case len(ids) == 0:
			continue
		}
		if s.afterList != nil {
			s.afterList()
		}

		// Kept apart before the claim is sent, not once it answers: a read on another
		// goroutine can page an entry this claim has already moved here. And kept apart
		// whatever the answer, since one lost, or one sent again and come back empty, leaves
		// entries moved here all the same; they are a later claim's. See Read.
		if ids = s.keepApart(stream, ids); len(ids) == 0 {
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
		taken, handed := s.deliveriesWithIDs([]redis.XStream{{Stream: stream, Messages: messages}}, true, was)
		// What it took and could not read was acknowledged as unreadable.
		for _, message := range messages {
			if !slices.Contains(handed, message.ID) {
				s.letGo(stream, message.ID)
			}
		}
		if len(messages) < len(ids) {
			s.letGoRetired(ctx, stream, ids, messages)
		}
		if minIdle > 0 {
			s.rememberAge(stream, minIdle, taken, handed)
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

// reclaimable is the pending entries worth taking over: idle long enough, and held by
// somebody else.
//
// The second half is the reason this is not one XAUTOCLAIM call. A command runs on the
// session's own executor, not on the loop that read it, so it is still pending while it
// runs; one that takes longer than the min-idle would be handed back to the very
// consumer already executing it, and dispatched a second time alongside the first.
// Acknowledging the original does not retire the copy. XPENDING is the only form that
// says who holds an entry.
func (s *Streams) reclaimable(ctx context.Context, stream string, minIdle time.Duration) (ids []string, was map[string]pending, err error) {
	ids = make([]string, 0, s.opts.ReadCount)
	// What XPENDING said about each one. Kept rather than dropped because this is the
	// only command that reports either fact, and the claim that follows resets what it
	// would have been asked about: after it, the holder is this instance.
	was = make(map[string]pending, s.opts.ReadCount)
	start := "-"

	// Paged, because the filter is what makes a page yield nothing: entries this
	// process is still running stay pending for as long as they run, and a page full of
	// them would hide everything behind it on every heartbeat, forever. The page cap is
	// there so one heartbeat cannot walk an arbitrarily long list.
	for range maxPendingPages {
		entries, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: stream,
			Group:  ConsumerGroup,
			Idle:   minIdle,
			Start:  start,
			End:    "+",
			Count:  s.opts.ReadCount,
		}).Result()
		switch {
		case errors.Is(err, redis.Nil):
			return ids, was, nil
		case isNoGroup(err):
			return nil, nil, err
		case err != nil:
			return nil, nil, fmt.Errorf("redisstream: list what is pending on %s: %w", stream, err)
		case len(entries) == 0:
			return ids, was, nil
		}

		for _, entry := range entries {
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
			// As Redis reports it, with nothing added for the claim about to happen. That
			// claim does increment the delivery counter, but how many times is not
			// knowable from here: go-redis sends a command again when its answer never
			// arrives, and a retried XCLAIM increments it once more with the caller
			// hearing about none of it. A count taken here is a fact; one adjusted for
			// work still on the wire is a guess, and it is short precisely during the
			// connection trouble this is meant to show.
			was[entry.ID] = pending{before: true, consumer: entry.Consumer, deliveries: entry.RetryCount}
		}
		if len(ids) >= int(s.opts.ReadCount) || int64(len(entries)) < s.opts.ReadCount {
			return ids, was, nil
		}
		start = entries[len(entries)-1].ID
	}
	return ids, was, nil
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
func (s *Streams) deliveriesWithIDs(result []redis.XStream, redelivered bool, was map[string]pending) (out []transport.Delivery, ids []string) {
	for _, stream := range result {
		for _, message := range stream.Messages {
			if delivery, ok := s.deliver(stream.Stream, message, s.hold(stream.Stream, message.ID), redelivered, was[message.ID]); ok {
				out = append(out, delivery)
				ids = append(ids, message.ID)
			}
		}
	}
	return out, ids
}

// pending is what is known about an entry that was already in the group's pending list.
// The zero value is an entry arriving new, which is what a `>` read carries.
//
// consumer and deliveries are filled only by a claim, because XPENDING is the only command
// that reports either and a read never sends one. That asymmetry is the metric's blind spot
// and is written into its help text rather than left for a reader of a panel to find.
type pending struct {
	before     bool
	consumer   string
	deliveries int64
}

// deliver makes a delivery of an entry already marked as running, and reports false for one
// it cannot read.
func (s *Streams) deliver(stream string, message redis.XMessage, held func(), redelivered bool, was pending) (transport.Delivery, bool) {
	command, err := protocol.ParseCommand(toFields(message.Values))
	if err != nil {
		// A frame this instance cannot read is not a frame a retry will fix, and leaving
		// it pending blocks nothing but fills the PEL forever. It is acknowledged and
		// dropped; the sender hears about it through the reply it is waiting for timing out.
		held()
		s.ackUnreadable(stream, message.ID)
		return transport.Delivery{}, false
	}
	return transport.Delivery{
		Command:         command,
		Ack:             s.acker(stream, message.ID, held),
		Release:         held,
		Redelivered:     redelivered,
		DeliveredBefore: was.before,
		TakenFrom:       was.consumer,
		Deliveries:      was.deliveries,
	}, true
}

// entryAfter reports whether stream entry id a comes after b. Everything comes after an
// empty b. Ids are `<milliseconds>-<sequence>`, and neither half compares as text.
func entryAfter(a, b string) bool {
	if b == "" {
		return true
	}
	aMs, aSeq := splitEntryID(a)
	bMs, bSeq := splitEntryID(b)
	return aMs > bMs || (aMs == bMs && aSeq > bSeq)
}

func splitEntryID(id string) (ms, seq uint64) {
	msPart, seqPart, _ := strings.Cut(id, "-")
	ms, _ = strconv.ParseUint(msPart, 10, 64)
	seq, _ = strconv.ParseUint(seqPart, 10, 64)
	return ms, seq
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
		s.letGo(stream, id)
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

// spentWindow tells a window that ran out from a read that failed inside one, and does it
// by the clock rather than by asking the context.
//
// go-redis takes the earliest of the caller's deadline and its own read timeout, and for a
// blocking read its own is the block plus ten seconds, so the deadline on the socket is
// the window's own. The two then fire together, from two different timers, and whichever
// the scheduler runs first decides whether `ctx.Err()` is set by the time the error is
// looked at. A loaded box loses that race often: production logged sixteen of these in the
// first forty-two minutes of an instance whose reads never stopped landing (#209).
//
// The clock cannot be raced: past the deadline the window is over, whatever the context
// has got around to saying. A timeout on any other deadline is left alone -- it is a read
// that failed with time to spare, which is the failure this instance should report.
func spentWindow(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	deadline, bounded := ctx.Deadline()
	if !bounded || time.Now().Before(deadline) {
		return err
	}
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() {
		return err
	}
	return fmt.Errorf("%w: %w", transport.ErrWindowSpent, err)
}
