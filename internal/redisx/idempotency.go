package redisx

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// DefaultIdempotencyTTL is how long a command's outcome is remembered.
//
// It has to outlive the longest a command can sit unacknowledged and still be
// reclaimed by another instance, because that is the redelivery this record answers.
// A day is far past that and costs one small string per message; forgetting too early
// is a duplicate side effect, and forgetting too late is nothing at all.
//
// Nothing bounds how long an entry stays pending on its own, so what makes "far past
// that" true is Recall pushing the expiry out: an entry still being handed around is one
// somebody is still asking about, and asking is what keeps the answer alive. What is
// left over is a fleet down for longer than this with a command pending, which comes
// back to a record that has gone and carries the command out again. Bounding the entry
// instead was tried and taken back out: a pending entry carries no honest age — its idle
// resets on every claim, and its id is the moment it was enqueued rather than the moment
// it first ran — so every bound built on one retired commands that had never run.
const DefaultIdempotencyTTL = 24 * time.Hour

// Idempotency remembers what a command did, so a redelivery answers with the first
// run's result instead of carrying it out a second time.
//
// Two records and not one, because a command has three states and not two. The result is
// written after it succeeds; the attempt is written before it starts and removed when the
// outcome is known either way. What is left standing -- an attempt with no result -- is a
// command that ran and whose outcome nobody here can tell, which is what `not_settled`
// says to a client.
//
// This used to be one record, written only after a success, and the argument for that is
// worth keeping because half of it still holds: an entry saying an attempt was made says
// nothing on its own about whether it landed, so an instance reclaiming the command would
// have to choose between dropping a message that never went out and sending one that
// already did. What dissolves the dilemma is not knowing more, it is having a third thing
// to say. The reclaiming instance chooses neither: it reports that the outcome exists and
// is not known, and a client reads the state back rather than being told a lie in either
// direction.
//
// The half that still holds is why Release exists. A refusal the connector is certain
// about -- the pre-flight that never reached the socket -- takes the attempt back off, so
// the retry does the whole thing rather than being refused for ever. Without it this
// would answer `not_settled` for a crash during a media upload, where the message provably
// never went out and a resend gets it right, which is the objection that kept the
// reservation out until #282 measured the other side of it.
//
// For a send the old cover is still there and still worth having. WhatsApp delivers a
// resend under an id it has already seen in full, with no window at all -- measured from
// an immediate resend out to thirty minutes apart, direct and group alike (#215). Every
// client downstream deduplicates on the message id, which `contract/PROTOCOL.md` states as
// an obligation and which the connector's own inbound path already relies on in several
// places. What the attempt adds is the commands that have no such cover: a participant
// added or removed, a name set, a read marker, applied a second time on top of state that
// has since moved.
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

// Recall answers what a command with this key did the first time, and how far that
// first time got.
//
// `done` is the command having finished, and `result` is what it answered. `attempted`
// with no `done` is the command having run with nobody able to say how it ended. Neither
// is a command this session has never seen.
func (i *Idempotency) Recall(ctx context.Context, sid, key string) (json.RawMessage, bool, bool, error) {
	keys := i.client.Keys()
	done, attempt := keys.Idempotency(sid, key), keys.Attempt(sid, key)
	// One round trip for both, because the miss is the common case and it is the one
	// every command pays for.
	stored, err := i.client.MGet(ctx, done, attempt).Result()
	if err != nil {
		return nil, false, false, fmt.Errorf("redisx: recall %s of %s: %w", key, sid, err)
	}
	if len(stored) != 2 {
		return nil, false, false, fmt.Errorf("redisx: recall %s of %s: %d values for 2 keys", key, sid, len(stored))
	}
	if stored[0] == nil {
		// No result. The attempt, if there is one, is the answer, and its expiry is
		// deliberately left where Reserve put it: see Keys.Attempt.
		return nil, false, stored[1] != nil, nil
	}

	// Asked about is kept: a record only has to outlive the entry it answers for, and
	// the entry announces itself by being asked about. Without this the two clocks run
	// independently, the record from a fixed point after the command ran and the entry
	// for as long as acknowledgements keep failing, and an entry still being handed
	// around outlives the only thing that can say it already ran.
	//
	// Best effort on purpose, and ignored rather than reported: the answer is already
	// in hand, and refusing to give it because the expiry could not be pushed out would
	// turn a shortened record into no record at all. What a failure here costs is the
	// original expiry, which is where this stood before, and the transport's own age
	// bound is what stands behind it.
	_ = i.client.Expire(ctx, done, i.ttl).Err()

	text, ok := stored[0].(string)
	if !ok {
		return nil, false, false, fmt.Errorf("redisx: recall %s of %s: stored value is %T", key, sid, stored[0])
	}
	if text == "" {
		// A command whose result carried no data. It still ran, and answering that is
		// the point: `null` and "never happened" are the same bytes and not the same
		// thing.
		return nil, true, true, nil
	}
	return json.RawMessage(text), true, true, nil
}

// Reserve records that a command is about to be carried out, so that a redelivery of one
// whose outcome never came back is answered rather than run a second time.
//
// It never overwrites, for the same reason Remember does not: the first attempt is the
// one that matters, and a second Reserve under the same key is a redelivery of the first.
// Overwriting would restart the clock on every redelivery, which is the shape that never
// expires.
func (i *Idempotency) Reserve(ctx context.Context, sid, key string) error {
	if err := i.client.SetNX(ctx, i.client.Keys().Attempt(sid, key), "1", i.ttl).Err(); err != nil {
		return fmt.Errorf("redisx: reserve %s of %s: %w", key, sid, err)
	}
	return nil
}

// Release takes an attempt back off, for a command the connector is certain never reached
// WhatsApp. What is left is a key nobody has heard of, which is what it was before.
func (i *Idempotency) Release(ctx context.Context, sid, key string) error {
	if err := i.client.Del(ctx, i.client.Keys().Attempt(sid, key)).Err(); err != nil {
		return fmt.Errorf("redisx: release %s of %s: %w", key, sid, err)
	}
	return nil
}

// Remember records what a command did. It never overwrites: the first run is the one
// every redelivery has to be answered with, or two answers to one command disagree.
func (i *Idempotency) Remember(ctx context.Context, sid, key string, result json.RawMessage) error {
	if err := i.client.SetNX(ctx, i.client.Keys().Idempotency(sid, key), []byte(result), i.ttl).Err(); err != nil {
		return fmt.Errorf("redisx: remember %s of %s: %w", key, sid, err)
	}
	// The result settles the attempt, so the attempt goes. Second and not first, and its
	// failure is not reported: Recall looks at the result before the attempt, so a leftover
	// attempt key answers nothing and expires on its own. The other order would leave a
	// window in which the command reads as never having run.
	_ = i.client.Del(ctx, i.client.Keys().Attempt(sid, key)).Err()
	return nil
}
