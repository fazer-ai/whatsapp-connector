package whatsmeow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// #291: four causes reached `sendFailure` and left it as one sentence, so the operator's
// log said the same thing whichever had happened. The wire collapsing them is deliberate
// and stays; the log is what this is about.
//
// The distinction is asserted on the rendered line rather than on a field, because a
// field is not what an operator reads and because the obvious fix does not reach the
// line: `zerolog`'s `Err` prints `Error()` and consults neither `Unwrap` nor any other
// field, so a cause hung off `protocol.Error` would leave this identical while
// `errors.Is` passed. That was measured before this was written.
func renderedFailure(t *testing.T, err error) string {
	t.Helper()

	var out bytes.Buffer
	logger := zerolog.New(&out)
	logger.Warn().Err(err).Str("sid", "sid-1").Msg("a command failed")
	return out.String()
}

func theFourCauses() []struct {
	name string
	err  error
} {
	return []struct {
		name string
		err  error
	}{
		{"the command's own clock", context.DeadlineExceeded},
		{"the command being abandoned", context.Canceled},
		{"WhatsApp never answering the send", wm.ErrMessageTimedOut},
		{"WhatsApp never answering a query inside it", wm.ErrIQTimedOut},
	}
}

// The half that must not move: the client is told `timeout` and one sentence, whichever
// of the four happened, and the bytes are the bytes it already gets.
func TestTheFourCausesStillReachTheClientAsOneAnswer(t *testing.T) {
	t.Parallel()

	const want = `{"code":"timeout","message":"WhatsApp did not answer whether the message went out"}`
	for _, tc := range theFourCauses() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var coded *protocol.Error
			if !errors.As(sendFailure(tc.err), &coded) {
				t.Fatalf("no code a client can branch on came out of %v", tc.err)
			}
			if coded.Code != protocol.ErrorTimeout {
				t.Errorf("the client is told %q, want %q", coded.Code, protocol.ErrorTimeout)
			}
			frame, err := json.Marshal(coded)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(frame) != want {
				t.Errorf("the frame reads %s,\nwant                %s", frame, want)
			}
		})
	}
}

// The half this issue is: the line an operator reads names which of the four it was.
func TestTheLoggedFailureNamesWhichOfTheFourCausesItWas(t *testing.T) {
	t.Parallel()

	seen := make(map[string]string, 4)
	for _, tc := range theFourCauses() {
		line := renderedFailure(t, sendFailure(tc.err))
		if !strings.Contains(line, tc.err.Error()) {
			t.Errorf("the line for %s does not name it: %s", tc.name, line)
		}
		if before, clash := seen[line]; clash {
			t.Errorf("%s and %s write the same line, which is the whole of #291:\n  %s",
				before, tc.name, line)
		}
		seen[line] = tc.name
	}
	if len(seen) != 4 {
		t.Errorf("the four causes write %d distinct lines, want 4", len(seen))
	}
}

// A send has three clocks over it and two of them are this connector's, producing the
// same `context.DeadlineExceeded` value rather than two comparable ones. Only
// `putOnTheWire` holds both contexts, so only it can say which ran out.
func TestTheThreeClocksOverOneSendAreToldApartInTheLog(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		wireLimit  time.Duration
		caller     time.Duration
		fromWire   error
		cancels    bool
		waitsFirst bool
		names      string
		forbids    string
	}{
		{
			name:      "the caller's own clock, which is the shorter one",
			wireLimit: time.Minute,
			caller:    20 * time.Millisecond,
			names:     "the command's own context",
			forbids:   "this connector's ceiling",
		},
		{
			name:      "this connector's ceiling, with the caller still waiting",
			wireLimit: 20 * time.Millisecond,
			caller:    time.Minute,
			names:     "this connector's ceiling",
			forbids:   "the command's own context",
		},
		{
			// A cancel is not a deadline, and the caller's context is the one that
			// carries it. The sentence has to cover both without claiming a clock ran
			// out when nothing did.
			name:      "the caller giving up, which is that same context by another door",
			wireLimit: time.Minute,
			caller:    20 * time.Millisecond,
			cancels:   true,
			names:     "the command's own context",
			forbids:   "this connector's ceiling",
		},
		{
			name:      "WhatsApp's own answer running out, which is neither of ours",
			wireLimit: time.Minute,
			caller:    time.Minute,
			fromWire:  wm.ErrMessageTimedOut,
			names:     wm.ErrMessageTimedOut.Error(),
			forbids:   "ended",
		},
		{
			// The case the guard at the top of `whichClockRanOut` is for, and the only
			// one that reaches it: our ceiling has run out, so the cause on `wire` names
			// us, and the library still answers with a sentinel of its own. Without the
			// guard the library's answer is credited to this connector's ceiling, which
			// is the misattribution #291 exists to end. Found by the verifier: the
			// battery had mutated each half of the guard and never its removal.
			name:       "WhatsApp's own answer, with our ceiling already run out",
			wireLimit:  20 * time.Millisecond,
			caller:     time.Minute,
			waitsFirst: true,
			fromWire:   wm.ErrMessageTimedOut,
			names:      wm.ErrMessageTimedOut.Error(),
			forbids:    "ended",
		},
		{
			// The library reaching a deadline of one of its own contexts, with both of
			// ours still live. Named by neither of the two above, and left as it came.
			name:      "a deadline from inside the library, with both of ours still live",
			wireLimit: time.Minute,
			caller:    time.Minute,
			fromWire:  context.DeadlineExceeded,
			names:     context.DeadlineExceeded.Error(),
			forbids:   "ended",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _, _ := outboundSession(t)
			session.wireLimit = tc.wireLimit
			session.handOver = func(
				ctx context.Context, _ waTypes.JID, _ string, _ *waE2E.Message,
			) (wm.SendResponse, error) {
				if tc.waitsFirst {
					<-ctx.Done()
				}
				if tc.fromWire != nil {
					return wm.SendResponse{}, tc.fromWire
				}
				<-ctx.Done()
				return wm.SendResponse{}, ctx.Err()
			}

			ctx, cancel := context.WithTimeout(context.Background(), tc.caller)
			defer cancel()
			if tc.cancels {
				cancel()
			}

			_, err := session.putOnTheWire(ctx, peer, "m1", aBodyToSend())
			if err == nil {
				t.Fatal("the send came back without a failure")
			}
			var coded *protocol.Error
			if !errors.As(err, &coded) || coded.Code != protocol.ErrorTimeout {
				t.Fatalf("the client is told %v, want a timeout", err)
			}
			line := renderedFailure(t, err)
			if !strings.Contains(line, tc.names) {
				t.Errorf("the line does not name %q: %s", tc.names, line)
			}
			if strings.Contains(line, tc.forbids) {
				t.Errorf("the line names %q, which is the other clock: %s", tc.forbids, line)
			}
		})
	}
}

// The order matters and reading the two contexts afterwards cannot recover it. A send
// can sit inside the library long past its ceiling, on waits nothing interrupts (#290,
// #74), so a caller giving up during that window is ordinary rather than exotic: by the
// time the error comes back both contexts are done, and the one that ended the send is
// the one that was first.
//
// Found in review of this PR. The version before it asked each context whether it was
// done and called this one the caller's.
func TestACallerGivingUpAfterOurCeilingDoesNotTakeTheBlameForIt(t *testing.T) {
	t.Parallel()

	session, _, _ := outboundSession(t)
	session.wireLimit = 20 * time.Millisecond

	released := make(chan struct{})
	ctx, giveUp := context.WithCancel(context.Background())
	session.handOver = func(
		wire context.Context, _ waTypes.JID, _ string, _ *waE2E.Message,
	) (wm.SendResponse, error) {
		<-wire.Done()
		// Our ceiling has fired. Now the caller gives up, which is what the library
		// holding on past the ceiling would let happen, and only then does the send
		// come back.
		giveUp()
		<-ctx.Done()
		close(released)
		return wm.SendResponse{}, wire.Err()
	}

	_, err := session.putOnTheWire(ctx, peer, "m1", aBodyToSend())
	<-released
	if err == nil {
		t.Fatal("the send came back without a failure")
	}
	line := renderedFailure(t, err)
	if !strings.Contains(line, "this connector's ceiling") {
		t.Errorf("the line does not name the ceiling that actually fired: %s", line)
	}
	if strings.Contains(line, "the command's own context") {
		t.Errorf("the line blames the caller, which only gave up afterwards: %s", line)
	}
}

// The other six answers of `sendFailure` are not this issue's, and the table that guards
// them compares the sentence as well as the code from this round on: the change here
// rebuilds every one of those sentences, and a code alone would not have noticed.
func TestEveryOtherRefusalKeepsItsCodeAndItsSentence(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		err     error
		code    protocol.ErrorCode
		message string
	}{
		{"a store with no account in it", wm.ErrNotLoggedIn,
			protocol.ErrorNotPaired, "the session has no WhatsApp account to send from"},
		{"a socket that is down", wm.ErrNotConnected,
			protocol.ErrorNotConnected, "the session is not connected to WhatsApp"},
		{"a number nobody has registered",
			errors.New("no LID found for 5511999999999@s.whatsapp.net from server"),
			protocol.ErrorRecipientNotOnWhatsapp, "that number is not on WhatsApp"},
		{"a broadcast list, which the library does not send to", wm.ErrBroadcastListUnsupported,
			protocol.ErrorUnsupported, "this connector cannot send to a broadcast list yet"},
		{"an address a message cannot go to", wm.ErrUnknownServer,
			protocol.ErrorInvalidPayload, "that is not an address a message can be sent to"},
		{"a recipient that is an AD JID", wm.ErrRecipientADJID,
			protocol.ErrorInvalidPayload, "that is not an address a message can be sent to"},
		{"anything this does not name", errors.New("something new in the protocol"),
			protocol.ErrorWaError, "WhatsApp refused the message"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			failure := sendFailure(tc.err)
			var coded *protocol.Error
			if !errors.As(failure, &coded) {
				t.Fatalf("no code came out of %v", tc.err)
			}
			if coded.Code != tc.code {
				t.Errorf("the code is %q, want %q", coded.Code, tc.code)
			}
			if coded.Message != tc.message {
				t.Errorf("the sentence is %q,\nwant            %q", coded.Message, tc.message)
			}
			// And the cause is in the line for these too, which is what the `default`
			// branch is worth: it is the one that names nothing by itself.
			if line := renderedFailure(t, failure); !strings.Contains(line, tc.err.Error()) {
				t.Errorf("the line for %s does not carry what happened: %s", tc.name, line)
			}
		})
	}
}

// The one branch of `sendFailure` that decides on the *text* of an error, and the reason
// it survives wrapping: none of the sentences this function can produce contains the
// phrase that branch matches on, so an answer of its own can never be re-aimed into it.
//
// Written this way after the verifier measured that the first version asserted nothing.
// It had claimed that feeding the answer back in would not find the phrase again, and the
// opposite is what happens -- `because` keeps the cause's text inside `Error()`, so
// `sendFailure(sendFailure(x))` matches `no LID found` a second time and answers
// `recipient_not_on_whatsapp` again. That is harmless, and it is not what needed pinning.
// What needed pinning is that the phrase can only come from the input.
func TestNoSentenceThisBuildsCanReAimTheBranchThatReadsTheText(t *testing.T) {
	t.Parallel()

	// Every input the table above covers, plus the timeout four, so the set of sentences
	// is every one `sendFailure` can emit.
	inputs := []error{
		wm.ErrNotLoggedIn, wm.ErrNotConnected, wm.ErrBroadcastListUnsupported,
		wm.ErrUnknownServer, wm.ErrRecipientADJID, errors.New("something new in the protocol"),
	}
	for _, cause := range theFourCauses() {
		inputs = append(inputs, cause.err)
	}
	for _, in := range inputs {
		var coded *protocol.Error
		if !errors.As(sendFailure(in), &coded) {
			t.Fatalf("no code came out of %v", in)
		}
		if strings.Contains(coded.Message, noLIDForNumber) {
			t.Errorf("the sentence for %v contains %q, so this connector's own answer "+
				"would be read as a number nobody has registered: %q", in, noLIDForNumber, coded.Message)
		}
	}
	// And the branch does fire on the phrase, so the check above is about something that
	// exists rather than about a branch nothing reaches.
	var coded *protocol.Error
	if !errors.As(sendFailure(errors.New("no LID found for 5511999999999@s.whatsapp.net from server")), &coded) ||
		coded.Code != protocol.ErrorRecipientNotOnWhatsapp {
		t.Fatalf("the phrase no longer reaches its branch: %v", coded)
	}
}
