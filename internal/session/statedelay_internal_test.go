package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// publisherThat answers every publish with the same error, nil for a landing.
type publisherThat struct {
	err  error
	sent int
}

func (p *publisherThat) Publish(context.Context, *protocol.Event) error {
	p.sent++
	return p.err
}

// sessionPublishing builds the smallest Session that can run `publish`, with a scripted
// clock. Built by hand rather than through the manager because what is under test is one
// function and the instants around it, and a manager would put a real clock between them.
func sessionPublishing(t *testing.T, watch Watch, publisher *publisherThat, at func() time.Time) *Session {
	t.Helper()

	server := miniredis.RunT(t)
	client := redisx.Wrap(redis.NewClient(&redis.Options{Addr: server.Addr()}), "wa:", 8)
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	const sid = "9c2b7d1e-0000-4000-8000-00000000018c"
	lease, err := leases.Acquire(context.Background(), sid)
	if err != nil {
		t.Fatalf("acquire the lease: %v", err)
	}
	return &Session{
		sid: sid, instance: "inst-a", lease: lease, leases: leases,
		publisher: publisher, watch: watch, now: at,
		newID: func() string { return "evt_000001" },
	}
}

// The distance the issue asks for is from the decision, and the only way to tell that
// apart from "how long Publish took" is to put time between the two and read which one
// came back.
//
// Scripted rather than slept: the first reading is the decision, the second is the publish
// landing, and the gap between them is a number chosen to be impossible to reach by
// accident. A test that let a real clock run would pass on microseconds whichever instant
// the delivery used, which is the reading that does not answer #182.
func TestAStatePublishIsReportedFromTheDecisionAndNotFromTheWrite(t *testing.T) {
	t.Parallel()

	decided := time.Unix(1_700_000_000, 0)
	// Two readings the arm cannot confuse: one 45s after the decision, which is the
	// landing, and none in between.
	ticks := []time.Time{decided.Add(45 * time.Second)}
	var read int
	watch := &spyWatch{}
	publisher := &publisherThat{}
	s := sessionPublishing(t, watch, publisher, func() time.Time {
		if read < len(ticks) {
			read++
		}
		return ticks[read-1]
	})

	if !s.publish(context.Background(), &engine.Emission{
		Type: protocol.EventSessionState, Payload: []byte(`{"state":"reconnecting"}`), Decided: decided,
	}) {
		t.Fatal("the publish did not land, so there is nothing to report on")
	}

	seen := watch.stateDelays()
	if len(seen) != 1 {
		t.Fatalf("got %d state delays, want exactly 1: one decision is one observation, and counting it on both sides of the seam doubles every episode", len(seen))
	}
	if seen[0] != 45*time.Second {
		t.Errorf("reported %s, want 45s: the distance was measured from something other than the decision, and the number that does not answer #182 is the one near zero, which is how long the write itself took", seen[0])
	}
}

// A frame that did not land told no client anything, and a distribution that mixed those
// in would report a drop that cost nothing as a publish that was fast.
func TestAStateThatNeverLandedIsNotReported(t *testing.T) {
	t.Parallel()

	decided := time.Unix(1_700_000_000, 0)
	watch := &spyWatch{}
	publisher := &publisherThat{err: errors.New("the stream is away")}
	s := sessionPublishing(t, watch, publisher, func() time.Time { return decided.Add(time.Second) })

	if s.publish(context.Background(), &engine.Emission{
		Type: protocol.EventSessionState, Payload: []byte(`{"state":"reconnecting"}`), Decided: decided,
	}) {
		t.Fatal("the publish reported a landing although the publisher refused")
	}
	if publisher.sent != 1 {
		t.Fatalf("the publisher was called %d times, want 1: the test proved nothing about the refusal path", publisher.sent)
	}
	if seen := watch.stateDelays(); len(seen) != 0 {
		t.Errorf("reported %v for a frame that never reached the stream: a drop costs nothing and would sit in the distribution looking like a fast publish", seen)
	}
}

// Everything else keeps the zero, and that is what makes the guard a guard rather than a
// filter on the event type: an emission nobody timed reports nothing, and no arm has to be
// changed here to start or stop being timed.
func TestAnEmissionNobodyTimedReportsNothing(t *testing.T) {
	t.Parallel()

	watch := &spyWatch{}
	publisher := &publisherThat{}
	s := sessionPublishing(t, watch, publisher, func() time.Time { return time.Unix(1_700_000_000, 0) })

	if !s.publish(context.Background(), &engine.Emission{
		Type: protocol.EventMessageReceived, Payload: []byte(`{}`),
	}) {
		t.Fatal("the publish did not land")
	}
	if seen := watch.stateDelays(); len(seen) != 0 {
		t.Errorf("reported %v for an emission with no decision instant: the zero would enter the histogram as 55 years", seen)
	}
}
