package redisstream

import (
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// The default behind that bound, asserted on both shapes of "unset" on purpose. The
// normalisation is `<= 0`: a rewrite to `== 0` passes the first row and fails the second,
// and the difference matters, because go-redis omits MAXLEN entirely when the field is not
// positive (`case a.MaxLen > 0`). A negative bound carried through does not produce a small
// stream, it produces an unbounded one -- no error, no symptom, growth nobody is watching.
//
// This one reads the field rather than Redis, and it is the deliberate half of a pair.
// Measuring construction does not measure use, so the use is measured next door by
// TestPublishBoundsTheEventStream, which publishes past an explicit bound and reads the
// length back: that the field reaches Redis is proved there, that it is filled in is proved
// here, and both are the same field. The end-to-end version costs 20001 publishes per row
// to say what the two together already say.
//
// The first draft of this test did go through Redis, publishing once under each unset shape
// and asserting one entry came back. It passed with the normalisation deleted, because one
// publish leaves one entry under any bound including none at all. That is what an assertion
// looks like when it is written for the mechanism instead of against it.
func TestNewFillsInTheEventBound(t *testing.T) {
	t.Parallel()

	for _, asked := range []int64{0, -1} {
		streams, err := New(&redisx.Client{}, Options{
			Instance: "inst-a", EventMaxLen: asked,
			Block: 50 * time.Millisecond, ClaimMinIdle: time.Millisecond,
			Logger: zerolog.Nop(),
		})
		if err != nil {
			t.Fatalf("New(EventMaxLen: %d): %v", asked, err)
		}
		if streams.opts.EventMaxLen != DefaultEventMaxLen {
			t.Errorf("New(EventMaxLen: %d) kept %d, want the default %d: an unset bound carried through reaches XADD as no MAXLEN at all",
				asked, streams.opts.EventMaxLen, DefaultEventMaxLen)
		}
	}
}
