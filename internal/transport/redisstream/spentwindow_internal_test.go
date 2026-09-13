package redisstream

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// The classification this file is about is the whole of the fix, and it has exactly two
// questions to answer: was the window over when the error came, and is the error the one a
// deadline produces. Each case below is one of the four answers, and the two that must
// stay failures are what keeps the fix from swallowing a Redis that is genuinely refusing.
func TestOnlyATimeoutPastTheDeadlineIsAWindowThatRanOut(t *testing.T) {
	t.Parallel()

	socket := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	refused := errors.New("READONLY You can't write against a read only replica")

	past := func() context.Context {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
		t.Cleanup(cancel)
		return ctx
	}
	ahead := func() context.Context {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
		t.Cleanup(cancel)
		return ctx
	}

	cases := []struct {
		name  string
		ctx   context.Context
		err   error
		spent bool
	}{
		{"a timeout past the deadline is the window running out", past(), socket, true},
		{"a timeout with the window still open is a read that failed", ahead(), socket, false},
		{"a refusal past the deadline is still a refusal", past(), refused, false},
		{"a timeout on a window with no deadline at all cannot be one running out", context.Background(), socket, false},
	}
	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			got := spentWindow(one.ctx, one.err)
			if spent := errors.Is(got, transport.ErrWindowSpent); spent != one.spent {
				t.Fatalf("spentWindow returned %v (window ran out: %t), want window ran out: %t", got, spent, one.spent)
			}
			// Named either way: an operator reading the line still needs what the socket
			// said, and the caller that retries needs to match on it.
			if !errors.Is(got, one.err) {
				t.Fatalf("spentWindow returned %v, which no longer carries the error it was given", got)
			}
		})
	}
}

// A read that came back with nothing to report is not an error to be classified: the nil
// has to survive, or every quiet read becomes a spent window the moment a window is over.
func TestAReadThatDidNotFailIsNotClassifiedAtAll(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute))
	defer cancel()

	if got := spentWindow(ctx, nil); got != nil {
		t.Fatalf("spentWindow turned a read that did not fail into %v", got)
	}
}
