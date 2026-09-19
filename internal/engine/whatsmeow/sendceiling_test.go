package whatsmeow

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"
)

// #283: a send interrupted by the connection dropping is retried by the pinned library
// with a zero timeout, and zero there builds no timer at all, so the retry ends when the
// answer arrives, when the context does, or never. With no `deadline` and no
// `max_runtime_ms` the context reaching the engine is the session's own, which is to say
// the command had no ceiling of any kind.
//
// What is asserted here is the deadline the wire receives, and it is asserted at the seam
// rather than inside `overSocket` on purpose: `handOver` replaces `overSocket` whole, so a
// ceiling built under the seam is one no test using the seam can see, and a bound nothing
// can observe is the same defect wearing a fix.
func aBodyToSend() *waE2E.Message {
	body := "anything"
	return &waE2E.Message{Conversation: &body}
}

type timedWire struct {
	deadline time.Time
	had      bool
	calls    int
}

func (w *timedWire) hand(
	ctx context.Context, _ waTypes.JID, id string, _ *waE2E.Message,
) (wm.SendResponse, error) {
	w.calls++
	w.deadline, w.had = ctx.Deadline()
	return wm.SendResponse{ID: id, Timestamp: time.Unix(1755440000, 0)}, nil
}

func ceilingSession(t *testing.T) (*Session, *timedWire) {
	t.Helper()

	session, _, _ := outboundSession(t)
	timed := &timedWire{}
	session.handOver = timed.hand
	return session, timed
}

// The whole of the issue in one assertion: a send whose caller named no ceiling still
// reaches the wire under one.
func TestASendWithNoCallerCeilingStillCarriesOurs(t *testing.T) {
	t.Parallel()

	session, timed := ceilingSession(t)
	began := time.Now()
	if _, err := session.putOnTheWire(context.Background(), waTypes.JID{User: "5511999999999", Server: waTypes.DefaultUserServer}, "m1", aBodyToSend()); err != nil {
		t.Fatalf("putOnTheWire: %v", err)
	}
	if timed.calls != 1 {
		t.Fatalf("the wire was handed %d messages, want 1", timed.calls)
	}
	if !timed.had {
		t.Fatal("the context that reached the wire carries no deadline, which is #283: " +
			"the library's retry has no timer of its own, so nothing would end this send")
	}
	// Generous on both sides, because the point is which bound was applied and not how
	// fast the machine is.
	if left := timed.deadline.Sub(began); left < sendCeiling-time.Minute || left > sendCeiling+time.Minute {
		t.Fatalf("the wire got %s to work in, want about %s (sendCeiling)", left, sendCeiling)
	}
}

// Two commands carry a ceiling of their own and the contract says the shorter one wins.
// The connector's is a backstop under them, not a floor over them.
func TestTheShorterOfTheTwoCeilingsIsTheOneTheWireGets(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		callers time.Duration
		want    time.Duration
	}{
		{"caller is tighter, so the caller decides", 3 * time.Second, 3 * time.Second},
		{"caller is looser, so it does not stretch ours", 30 * time.Minute, sendCeiling},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, timed := ceilingSession(t)
			ctx, cancel := context.WithTimeout(context.Background(), tc.callers)
			defer cancel()

			began := time.Now()
			if _, err := session.putOnTheWire(ctx, waTypes.JID{User: "5511999999999", Server: waTypes.DefaultUserServer}, "m1", aBodyToSend()); err != nil {
				t.Fatalf("putOnTheWire: %v", err)
			}
			if !timed.had {
				t.Fatal("no deadline reached the wire")
			}
			if left := timed.deadline.Sub(began); left < tc.want-time.Minute || left > tc.want+time.Minute {
				t.Fatalf("the wire got %s, want about %s", left, tc.want)
			}
		})
	}
}

// The number is a sum, and this holds both halves of that: the value, and the fact that
// the source says it as a sum.
//
// The value check alone would be a tautology, and it was one when this was first written:
// `sendCeiling` replaced by a literal `620 * time.Second` compares equal to the sum and
// passes, so the comment claiming it caught that was wrong. The declaration is read below
// for that reason. A derived number stops being derived exactly when somebody writes the
// answer down instead of the arithmetic, and that edit leaves the value untouched.
//
// The shape of the sum is itself the thing review corrected, so it is spelled out rather
// than folded into one multiplication: four stages, each attempted twice with a reconnect
// window between the attempts. The first version counted one attempt per stage and came to
// 380s, which is under what the pinned library will spend on the prerequisites alone.
func TestTheCeilingIsTheSumOfTheStagesInsideIt(t *testing.T) {
	t.Parallel()

	const (
		stagesInOneSend = 4
		perStage        = 2*upstreamRequestWait + upstreamReconnectWait
		wanted          = stagesInOneSend * perStage
	)
	if sendCeiling != wanted {
		t.Fatalf("sendCeiling is %s but the stages inside one send add up to %s "+
			"(%d stages at %s each: two waits of %s with %s of reconnect between them); "+
			"if a stage was added or removed, say which in the comment on sendCeiling "+
			"and change both",
			sendCeiling, wanted, stagesInOneSend, perStage, upstreamRequestWait, upstreamReconnectWait)
	}
	if declared := howSendCeilingIsWritten(t); !strings.Contains(declared, "upstreamRequestWait") ||
		!strings.Contains(declared, "upstreamReconnectWait") {
		t.Errorf("sendCeiling is declared as %q, which no longer names what it is made of; "+
			"a literal here is a number nobody can check against the stages it came from",
			declared)
	}
	if upstreamRequestWait != 75*time.Second {
		t.Fatalf("upstreamRequestWait is %s; the pinned library's defaultRequestTimeout is 75s, "+
			"and upstream_test.go is what fails when that stops being true", upstreamRequestWait)
	}
}

// howSendCeilingIsWritten returns the source text of sendCeiling's declaration, so the
// test above can tell a sum from a number that happens to equal one.
func howSendCeilingIsWritten(t *testing.T) string {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "send.go", nil, 0)
	if err != nil {
		t.Fatalf("parse send.go: %v", err)
	}
	source, err := os.ReadFile("send.go")
	if err != nil {
		t.Fatalf("read send.go: %v", err)
	}
	for _, decl := range file.Decls {
		general, ok := decl.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			valued, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range valued.Names {
				if name.Name != "sendCeiling" || i >= len(valued.Values) {
					continue
				}
				value := valued.Values[i]
				return string(source[fset.Position(value.Pos()).Offset:fset.Position(value.End()).Offset])
			}
		}
	}
	t.Fatal("send.go declares no sendCeiling constant, so the ceiling this file is about is gone")
	return ""
}
