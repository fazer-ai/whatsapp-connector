package session

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// #291 wraps a send's failure so the operator's line says what actually happened. The
// wrapping has to survive everything above it that reads the error, and what reads it is
// not one thing: the code the client gets, whether the command counts as having run, and
// the outcome that reaches the metric. All three go through `asProtocolError`, which is
// why they are asserted together rather than one per test.
//
// The four shapes are the ones an error of this kind can plausibly take, not a catalogue:
// bare, wrapped the way `because` wraps it, wrapped again by a caller adding context, and
// wrapped the other way round with the cause in front. Only the second is produced today;
// the rest are here so that a later change of wrapping order fails here rather than in a
// client's branch on `code`.
func wrappedFourWays(coded *protocol.Error, cause error) []struct {
	name string
	err  error
} {
	return []struct {
		name string
		err  error
	}{
		{"the coded error on its own", coded},
		{"the code first, then the cause, which is what `because` builds",
			fmt.Errorf("%w: %w", coded, cause)},
		{"the same with a caller's own sentence around it",
			fmt.Errorf("send m1: %w", fmt.Errorf("%w: %w", coded, cause))},
		{"the cause first and the code behind it",
			fmt.Errorf("%w: %w", cause, coded)},
	}
}

func TestAWrappedFailureStillDecidesTheCodeTheCountAndTheMetric(t *testing.T) {
	t.Parallel()

	coded := protocol.NewError(protocol.ErrorTimeout, "WhatsApp did not answer whether the message went out")
	command := &protocol.Command{ID: "cmd-1", Type: protocol.CommandMessageSend}
	for _, tc := range wrappedFourWays(coded, context.DeadlineExceeded) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := asProtocolError(tc.err)
			if got.Code != protocol.ErrorTimeout {
				t.Errorf("the client is told %q, want %q", got.Code, protocol.ErrorTimeout)
			}
			if got.Message != coded.Message {
				t.Errorf("the sentence is %q, want %q", got.Message, coded.Message)
			}
			frame, err := json.Marshal(got)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			const want = `{"code":"timeout","message":"WhatsApp did not answer whether the message went out"}`
			if string(frame) != want {
				t.Errorf("the frame reads %s,\nwant                %s", frame, want)
			}
			// A send that timed out is a command that happened on the account: the
			// message may well be in somebody's chat. Counting it as not having run
			// would have a teardown behind it read the session as untouched.
			if !ran(tc.err) {
				t.Error("a wrapped timeout stopped counting as a command that ran")
			}
			// And the metric still separates the kinds of failure, which is the reason
			// it carries the contract's code rather than a bare "error".
			watch := &spyWatch{}
			metered := &Session{watch: watch, now: func() time.Time { return time.Unix(1755440000, 0) }}
			metered.reportCommand(command, time.Unix(1755439999, 0), tc.err)
			if len(watch.done) != 1 {
				t.Fatalf("the metric saw %d commands, want 1", len(watch.done))
			}
			if watch.done[0].outcome != string(protocol.ErrorTimeout) {
				t.Errorf("the metric recorded %q, want %q", watch.done[0].outcome, protocol.ErrorTimeout)
			}
		})
	}
}

// The one branch of `asProtocolError` that a wrapped error could reach by the wrong door.
// `errors.As` runs first and returns there, so the `errors.Is` below it never sees a
// `*protocol.Error`; swap the two in the source and this goes red.
func TestTheCodedAnswerIsFoundBeforeAnythingIsAskedAboutContexts(t *testing.T) {
	t.Parallel()

	// A code that is not what the context branch would produce, wrapped around the
	// context error the branch below tests for. If the order ever inverts, this comes
	// back `timeout` with the generic sentence instead of the code it carries.
	coded := protocol.NewError(protocol.ErrorNotConnected, "the session is not connected to WhatsApp")
	wrapped := fmt.Errorf("%w: %w", coded, context.DeadlineExceeded)

	if !errors.Is(wrapped, context.DeadlineExceeded) {
		t.Fatal("the probe is wrong: the cause is not in the chain")
	}
	got := asProtocolError(wrapped)
	if got.Code != protocol.ErrorNotConnected {
		t.Errorf("the code is %q, want %q: the context branch answered before `errors.As` did",
			got.Code, protocol.ErrorNotConnected)
	}
	if got.Message != coded.Message {
		t.Errorf("the sentence is %q, want %q", got.Message, coded.Message)
	}
}

// The line an operator actually reads, written by the path that writes it, with `sid` on
// it as this repository requires of every session line.
func TestTheFailureLineCarriesTheCauseAndTheSession(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	s := &Session{sid: "sid-9", log: zerolog.New(&out).With().Str("sid", "sid-9").Logger()}
	command := &protocol.Command{ID: "cmd-1", Type: protocol.CommandMessageSend}
	cause := errors.New("this connector's ceiling on one send ran out: context deadline exceeded")
	failure := fmt.Errorf("%w: %w",
		protocol.NewError(protocol.ErrorTimeout, "WhatsApp did not answer whether the message went out"),
		cause)

	s.logFailure(command, asProtocolError(failure), failure)

	line := out.String()
	for _, want := range []string{
		`"sid":"sid-9"`,
		`"cmd_id":"cmd-1"`,
		`"code":"timeout"`,
		"this connector's ceiling on one send ran out",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("the line is missing %s:\n  %s", want, line)
		}
	}
	// And the same line without the cause is what the issue reported, so its absence has
	// to be what fails rather than something incidental.
	bare := protocol.NewError(protocol.ErrorTimeout, "WhatsApp did not answer whether the message went out")
	out.Reset()
	s.logFailure(command, bare, bare)
	if strings.Contains(out.String(), "ceiling on one send") {
		t.Errorf("the bare failure already names a ceiling, so this asserts nothing: %s", out.String())
	}
}
