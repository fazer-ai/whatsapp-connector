package whatsmeow

import (
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

type emitRecord struct {
	waited time.Duration
	depth  int
}

type emitWatch struct {
	mu    sync.Mutex
	seen  []emitRecord
	drops []protocol.EventType
}

func (r *emitWatch) Dropped(eventType protocol.EventType) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drops = append(r.drops, eventType)
}

func (r *emitWatch) dropped() []protocol.EventType {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]protocol.EventType(nil), r.drops...)
}

func (r *emitWatch) Emitted(waited time.Duration, depth int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, emitRecord{waited: waited, depth: depth})
}

func (r *emitWatch) all() []emitRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]emitRecord(nil), r.seen...)
}

// An emission the inbox has room for must not be reported as a wait, or the metric that
// exists to find the tail is buried under every ordinary publish.
func TestAnEmissionWithRoomIsNotReportedAsAWait(t *testing.T) {
	t.Parallel()

	watch := &emitWatch{}
	s := &Session{
		inbox:    make(chan pending, 4),
		done:     make(chan struct{}),
		queueing: watch,
	}
	s.emitting(emissionOf(protocol.EventMessageReceived), map[string]any{"a": 1})

	seen := watch.all()
	if len(seen) != 1 {
		t.Fatalf("reported %d emissions, want 1", len(seen))
	}
	if seen[0].waited != 0 {
		t.Errorf("waited = %s, want 0 for an inbox with room", seen[0].waited)
	}
	if seen[0].depth != 0 {
		t.Errorf("depth = %d, want 0: an emission into an empty inbox arrived at an empty inbox, "+
			"and must not count itself", seen[0].depth)
	}
}

// The stall #221 is about: the inbox is full, and the emission holds the goroutine
// whatsmeow dispatched from until the pump moves. The time it held it is the number
// nobody had.
//
// Under synctest, so this is an assertion and not a race. `synctest.Wait` returns only
// once the emitting goroutine is durably blocked, which is what makes "it blocked"
// something the test knows rather than assumes, and the clock inside the bubble is fake,
// so the wait comes out as exactly the interval the test chose. Sleeping on the real
// clock and hoping the goroutine got there first is what AGENTS.md rules out, and it is
// also weaker: it can only assert a lower bound.
func TestAnEmissionThatWaitsForRoomIsReportedWithHowLongItWaited(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		watch := &emitWatch{}
		s := &Session{
			inbox:    make(chan pending, 1),
			done:     make(chan struct{}),
			queueing: watch,
		}
		s.emitting(emissionOf(protocol.EventMessageReceived), map[string]any{"first": 1})

		go s.emitting(emissionOf(protocol.EventMessageReceived), map[string]any{"second": 1})
		synctest.Wait() // returns once that goroutine is parked on the full inbox

		const held = 40 * time.Millisecond
		time.Sleep(held) // fake time: the emission is parked across the whole interval
		<-s.inbox
		synctest.Wait()

		seen := watch.all()
		if len(seen) != 2 {
			t.Fatalf("reported %d emissions, want 2", len(seen))
		}
		if seen[0].waited != 0 {
			t.Errorf("the first waited %s, want 0", seen[0].waited)
		}
		if seen[1].waited != held {
			t.Errorf("the second waited %s, want exactly %s", seen[1].waited, held)
		}
	})
}

// The depth reported is the depth the emission ARRIVED at, not what the pump had drained
// by the time it got in. Those differ exactly when the metric matters: under pressure the
// second reading is the low one, so a gauge built on it would say the inbox was nearly
// empty during the episode that filled it.
func TestTheDepthReportedIsTheOneTheEmissionArrivedAt(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const capacity = 4
		watch := &emitWatch{}
		s := &Session{
			inbox:    make(chan pending, capacity),
			done:     make(chan struct{}),
			queueing: watch,
		}
		for range capacity {
			s.emitting(emissionOf(protocol.EventMessageReceived), map[string]any{"fill": 1})
		}

		go s.emitting(emissionOf(protocol.EventMessageReceived), map[string]any{"late": 1})
		synctest.Wait()

		// The pump recovering: all but one entry gone before the blocked send gets in.
		for range capacity - 1 {
			<-s.inbox
		}
		synctest.Wait()

		seen := watch.all()
		if len(seen) != capacity+1 {
			t.Fatalf("reported %d emissions, want %d", len(seen), capacity+1)
		}
		if got := seen[len(seen)-1].depth; got != capacity {
			t.Errorf("the blocked emission reported depth %d, want %d (the buffer it arrived at); "+
				"reading the depth after the send reports what the pump drained while it waited",
				got, capacity)
		}
	})
}

// The same reading, on the path that never blocks: the depth is the one before this
// emission, not counting itself. It is the same `depth` the blocked path reports, so this
// is the cheap deterministic half of the invariant above.
func TestTheDepthDoesNotCountTheEmissionReportingIt(t *testing.T) {
	t.Parallel()

	watch := &emitWatch{}
	s := &Session{inbox: make(chan pending, 4), done: make(chan struct{}), queueing: watch}
	for range 3 {
		s.emitting(emissionOf(protocol.EventMessageReceived), map[string]any{"a": 1})
	}

	var got []int
	for _, e := range watch.all() {
		got = append(got, e.depth)
	}
	want := []int{0, 1, 2}
	if !slices.Equal(got, want) {
		t.Errorf("depths %v, want %v: each emission reports what was queued before it", got, want)
	}
}

// A session nobody is watching must still emit. Nil is what every test that is not about
// this passes, and what a fake engine leaves unset.
func TestASessionWithNobodyWatchingStillEmits(t *testing.T) {
	t.Parallel()

	s := &Session{inbox: make(chan pending, 1), done: make(chan struct{})}
	s.emitting(emissionOf(protocol.EventMessageReceived), map[string]any{"a": 1})

	if len(s.inbox) != 1 {
		t.Fatalf("inbox holds %d, want 1", len(s.inbox))
	}
}

func emissionOf(kind protocol.EventType) *engine.Emission {
	return &engine.Emission{Type: kind}
}

// The instrument has to cover every door into the inbox, not the one it was written
// against. Four write to it -- `emitting`, `deliverUnless`, `post` and the retry in
// `settled` -- and an instrument on one of them describes that door rather than the
// queue, which is how the backpressure on the path that carries the messages stayed
// invisible through a round of review.
//
// A fence rather than four assertions: a fifth door added later is the thing that breaks
// this silently, and nothing else would notice.
func TestEveryWriteToTheInboxIsMeasured(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"session.go", "message.go"} {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		lines := strings.Split(string(body), "\n")
		for i, line := range lines {
			if !strings.Contains(line, "s.inbox <- ") {
				continue
			}
			// The reporting sits in the arm this send opens, so look just past it.
			window := strings.Join(lines[i:min(i+6, len(lines))], "\n")
			if !strings.Contains(window, "s.queued(") {
				t.Errorf("%s:%d writes to the inbox and does not report it:\n\t%s\n"+
					"every door into the inbox reports, or the depth histogram describes "+
					"the doors that do rather than the queue", name, i+1, strings.TrimSpace(line))
			}
		}
	}
}

// The inbound path gives up after its bound and withholds the acknowledgement, so
// WhatsApp sends the message again -- invariant 4 paying a redelivery rather than a
// message. Nothing else records that it happened, which is what this counter is for.
func TestAnInboundEventTheInboxHadNoRoomForIsCounted(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		watch := &emitWatch{}
		s := &Session{
			inbox:       make(chan pending, 1),
			done:        make(chan struct{}),
			queueing:    watch,
			deliverWait: 250 * time.Millisecond,
			log:         zerolog.Nop(),
		}
		s.inbox <- pending{} // full

		if s.deliverUnless(protocol.EventMessageReceived, map[string]any{"a": 1}, 0, "") {
			t.Fatal("deliverUnless acknowledged an event it never queued")
		}

		if got := watch.dropped(); !slices.Equal(got, []protocol.EventType{protocol.EventMessageReceived}) {
			t.Errorf("dropped = %v, want one message.received", got)
		}
		if seen := watch.all(); len(seen) != 0 {
			t.Errorf("reported %d emissions as queued, want 0: nothing got in", len(seen))
		}
	})
}
