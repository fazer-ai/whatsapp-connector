package redisstream_test

import (
	"context"
	"testing"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/transport/redisstream"
)

// The only MAXLEN this connector emits, and until #246 nothing watched it.
//
// Measured while removing `CommandMaxLen`, the option beside this one that nothing read:
// take `MaxLen` and `Approx` off the `XAdd` in Publish and the whole suite still passes.
// The bound was being written and never observed, which is the same state the option next
// to it was in, one step further along.
//
// Asserted through Publish rather than by reading the args, because what has to be true is
// that Redis received the bound: go-redis omits MAXLEN entirely when the field is zero
// (`case a.MaxLen > 0`), so a default that goes missing does not produce a bound of zero,
// it produces an event stream with no bound at all -- no error, no symptom, and growth
// nobody is watching.
func TestPublishBoundsTheEventStream(t *testing.T) {
	t.Parallel()

	f := newFleet(t)
	streams, err := redisstream.New(f.client, redisstream.Options{
		Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: time.Millisecond,
		EventMaxLen: 2,
	})
	if err != nil {
		t.Fatalf("redisstream.New: %v", err)
	}

	const sid = "9c2b7d1e-0000-4000-8000-0000000000b1"
	ctx := context.Background()
	for seq := uint64(1); seq <= 5; seq++ {
		if err := streams.Publish(ctx, event(sid, seq)); err != nil {
			t.Fatalf("Publish %d: %v", seq, err)
		}
	}

	stream := f.client.Keys().EventsOf(sid)
	length, err := f.rdb.XLen(ctx, stream).Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if length != 2 {
		t.Fatalf("five events under EventMaxLen 2 left %d entries on %s, want 2: the publish is not carrying the bound to Redis", length, stream)
	}
}
