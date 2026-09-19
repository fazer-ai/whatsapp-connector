package whatsmeow

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Defects that live in whatsmeow and that this connector cannot work around. CLAUDE.md
// says why none of them is reported upstream from here, and the issue named by each one
// carries the diagnosis with file and line.
//
// The fence is not a test of the connector. It is a tripwire on the pin: it asserts that
// the defect is STILL THERE, so that the day a bump changes it the suite fails and somebody
// re-reads the issue. Without it, "is it fixed yet?" is a question nobody remembers to ask,
// and a bump that silently fixed one would leave the limitation documented as current for
// as long as the note survives.
//
// A failure here is therefore good news, and never a reason to change connector code.
type upstreamDefect struct {
	issue string // where the diagnosis lives
	file  string // relative to the whatsmeow module root
	// stillThere are the fragments the defect is made of. All of them have to be present.
	// Text, not line numbers: the write of #207 moved from line 83 to line 82 between two
	// pins 27 days apart without changing at all.
	stillThere []string
	// inOrder are fragments that have to appear, in this order, inside enclosing. Some
	// defects are not a line but a sequence: #74 is a lock taken BEFORE the context is
	// read, and both lines survive a fix that swaps them, so presence alone would call the
	// repaired function defective and keep a limitation documented forever.
	inOrder []string
	// absent is what the fix would add. Checked inside enclosing, so that the same call
	// appearing elsewhere in the file does not read as a repair.
	absent    []string
	enclosing string // the func whose body `absent` is checked against
	what      string // what the reader should understand from a failure
	// reliedOn flips what a failure means. Every other entry here describes something
	// broken upstream that this repository works around, so its failure reads "upstream
	// may have fixed it, delete this entry". An entry marked this way is the opposite: a
	// property upstream has that a fix on this side is built on, whose disappearance
	// breaks us rather than freeing us. Telling a reader to delete the entry would be
	// exactly the wrong instruction.
	reliedOn bool
	// restingOn names what stops working when a relied-on property goes, so the failure
	// points at the code and the prose that have to be re-read rather than at nothing.
	restingOn string
}

var upstreamDefects = []upstreamDefect{
	{
		issue: "fazer-ai/whatsapp-connector#283",
		file:  "request.go",
		// Not a defect. `retryFrame` watching the caller's context is the single reason a
		// ceiling set on this side reaches the retry at all: the retry's own timer does not
		// exist (the entries below), so `ctx.Done()` is the whole of the remedy. The
		// contract already promises a client that `max_runtime_ms` ends this wait, and
		// without this case that promise would be false with the suite green.
		inOrder:   []string{"case <-ctx.Done():", "return nil, ctx.Err()"},
		enclosing: "func (cli *Client) retryFrame(",
		what: "the retry watching the caller's context, which is what lets a ceiling set " +
			"here end a retry that has no timer of its own",
		reliedOn: true,
		restingOn: "the `sendCeiling` in internal/engine/whatsmeow/send.go and the paragraph " +
			"in contract/PROTOCOL.md that tells a client `max_runtime_ms` reaches this retry",
	},
	{
		issue: "fazer-ai/whatsapp-connector#283",
		file:  "send.go",
		// A send interrupted by the connection dropping is retried, and the retry is handed
		// a zero where every other caller of retryFrame passes a duration. Zero there is not
		// the default: `retryFrame` only builds a timer `if timeout > 0`, so the retry waits
		// on the answer and the context and nothing else. The literal is what is asserted,
		// because a fix is exactly this argument becoming something else.
		stillThere: []string{`cli.retryFrame(ctx, "message send", req.ID, data, respNode, 0)`},
		enclosing:  "func (cli *Client) SendMessage(",
		what: "the retry of an interrupted send being given no timeout, which leaves it " +
			"bounded by the caller's context alone",
	},
	{
		issue:      "fazer-ai/whatsapp-connector#283",
		file:       "sendfb.go",
		stillThere: []string{`cli.retryFrame(ctx, "message send", req.ID, data, respNode, 0)`},
		enclosing:  "func (cli *Client) SendFBMessage(",
		what:       "the same zero timeout on the FB send path",
	},
	{
		issue: "fazer-ai/whatsapp-connector#283",
		file:  "request.go",
		// The other half of the same defect, and the half a fix would most likely touch:
		// the timer is built only for a positive value, so zero switches it off rather
		// than selecting the default the way `sendIQAsync` does a few lines above.
		inOrder:   []string{"if timeout > 0 {", "timeoutChan = time.After(timeout)"},
		enclosing: "func (cli *Client) retryFrame(",
		what:      "zero meaning no timer at all rather than the seventy five second default",
	},
	{
		issue: "fazer-ai/whatsapp-connector#283",
		file:  "request.go",
		// Not a defect, and one of the two things `sendCeiling` counts with. An info query
		// interrupted by a disconnect is sent again, and unlike the message send it is
		// handed `query.Timeout` rather than zero, so a stage is two bounded waits with a
		// reconnect window between them. That is where the 155 seconds per stage in
		// send.go comes from; the first version of that derivation counted one wait per
		// stage and was short by the whole of the second attempt.
		inOrder: []string{
			"if isDisconnectNode(res) {",
			`cli.retryFrame(ctx, "info query", query.ID, data, res, query.Timeout)`,
		},
		enclosing: "func (cli *Client) sendIQ(",
		what: "an interrupted info query being retried once, under the same timeout the " +
			"first attempt had",
		reliedOn:  true,
		restingOn: "the per-stage arithmetic in `sendCeiling`, internal/engine/whatsmeow/send.go",
	},
	{
		issue: "fazer-ai/whatsapp-connector#283",
		file:  "request.go",
		// The other thing it counts with: the retry is one retry. A second disconnect on
		// the same frame gives up instead of going round again, so a stage cannot cost
		// three waits, or ten. If this ever became a loop, `sendCeiling` would start
		// cutting sends the library would have completed, which is the failure this
		// connector least wants to introduce with a ceiling.
		inOrder: []string{
			"if isDisconnectNode(resp) {",
			"not retrying anymore",
			"return nil, &DisconnectedError{",
		},
		enclosing: "func (cli *Client) retryFrame(",
		what:      "the retry being attempted once and not in a loop",
		reliedOn:  true,
		restingOn: "the `4 stages` in `sendCeiling`, internal/engine/whatsmeow/send.go",
	},
	{
		issue: "fazer-ai/whatsapp-connector#207",
		file:  "socket/noisesocket.go",
		// NoiseSocket.Stop clears the callback with no lock at all, while FrameSocket.Close
		// reads the same field holding fs.lock. Locking one side of a pair synchronises
		// nothing, which is why the race detector names these two lines and not others.
		stillThere: []string{"ns.fs.OnDisconnect = nil"},
		absent:     []string{"Lock()"},
		enclosing:  "func (ns *NoiseSocket) Stop(",
		what:       "the unsynchronised write to fs.OnDisconnect",
	},
	{
		issue:      "fazer-ai/whatsapp-connector#207",
		file:       "socket/framesocket.go",
		stillThere: []string{"fs.lock.Lock()", "if fs.OnDisconnect != nil {"},
		enclosing:  "func (fs *FrameSocket) Close(",
		what:       "the read of fs.OnDisconnect under fs.lock",
	},
	{
		issue: "fazer-ai/whatsapp-connector#74",
		file:  "socket/noisesocket.go",
		// The lock comes first and the context is read after it, so the wait for the lock
		// is outside every deadline a caller sets. A fix is the same two lines the other
		// way round, or a lock that takes a context -- and both leave every fragment of
		// this function present, which is why the order is what is asserted.
		inOrder:   []string{"ns.writeLock.Lock()", "if ctx.Err() != nil {"},
		enclosing: "func (ns *NoiseSocket) SendFrame(",
		what: "the write lock being taken before the caller's context is looked at, which " +
			"is what puts the wait for it outside every deadline this connector sets",
	},
	{
		issue: "fazer-ai/whatsapp-connector#90",
		file:  "download-to-file.go",
		// The retry rewinds and writes over the same file without shortening it, so an
		// attempt that returns fewer bytes than the one before leaves that one's tail in
		// place. `Truncate` is what the fix adds, and it already appears twice elsewhere in
		// this file -- the File interface declares it and the HMAC trim calls it -- so
		// checking the file instead of the body would be red on an unfixed pin.
		stillThere: []string{"file.Seek(0, io.SeekStart)"},
		absent:     []string{"Truncate"},
		enclosing:  "func (cli *Client) downloadPossiblyEncryptedMediaWithRetriesToFile(",
		what: "the retry rewinding without shortening the file, which leaves a shorter " +
			"attempt carrying the tail of the one before it",
	},
}

func TestTheUpstreamDefectsWeLiveWithAreStillThere(t *testing.T) {
	t.Parallel()
	root := whatsmeowRoot(t)
	for _, defect := range upstreamDefects {
		t.Run(defect.file+" "+defect.what, func(t *testing.T) {
			t.Parallel()
			source, err := os.ReadFile(filepath.Join(root, defect.file))
			if err != nil {
				t.Fatalf("read %s of the pinned whatsmeow: %v", defect.file, err)
			}
			body := funcBody(t, string(source), defect.enclosing)
			at := 0
			for _, fragment := range defect.inOrder {
				found := strings.Index(body[at:], fragment)
				if found < 0 {
					t.Fatal(whatAFailureMeans(&defect, fragment, "no longer holds, in order,"))
				}
				at += found + len(fragment)
			}
			for _, fragment := range defect.stillThere {
				if !strings.Contains(body, fragment) {
					t.Fatal(whatAFailureMeans(&defect, fragment, "no longer holds"))
				}
			}
			for _, fragment := range defect.absent {
				if strings.Contains(body, fragment) {
					// "may be fixed" rather than anything about locks: this branch is not
					// only about #207 any more, and a message that names the repair it
					// expects reads as nonsense on an entry whose fix is a truncate.
					t.Fatalf("%s now contains %q, which is what a fix would add, so %s "+
						"may be over.\nRe-read %s before trusting it, then delete this entry.",
						defect.enclosing, fragment, defect.what, defect.issue)
				}
			}
		})
	}
}

// whatAFailureMeans writes the two opposite instructions this table can carry. A defect
// gone is good news and the entry should go with it; a relied-on property gone is the
// opposite, and the reader has to be sent to the code that was built on it instead of
// being told to delete the line that noticed.
func whatAFailureMeans(defect *upstreamDefect, fragment, how string) string {
	if defect.reliedOn {
		return fmt.Sprintf("%s %s %q, and that is not a fix upstream: it is a property this "+
			"repository depends on, namely %s.\nWhat rests on it: %s.\nRe-read %s. Do not "+
			"delete this entry to make the suite green.",
			defect.enclosing, how, fragment, defect.what, defect.restingOn, defect.issue)
	}
	return fmt.Sprintf("%s %s %q, so %s may be gone.\nThis is not a defect in this "+
		"repository: re-read %s, and if upstream fixed it, delete this entry and the "+
		"limitation it documents.", defect.enclosing, how, fragment, defect.what, defect.issue)
}

// funcBody returns the text between the opening line and the closing brace in column zero,
// which is what gofmt guarantees for a top-level func. Scoping to the body is what stops a
// lock taken by some other function in the same file from reading as a repair.
func funcBody(t *testing.T, source, signature string) string {
	t.Helper()
	start := strings.Index(source, signature)
	if start < 0 {
		t.Fatalf("the pinned whatsmeow has no %q: the shape this fence describes is gone, "+
			"so re-read the issue rather than adjusting the string", signature)
	}
	rest := source[start:]
	if end := strings.Index(rest, "\n}\n"); end >= 0 {
		return rest[:end]
	}
	return rest
}

func whatsmeowRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "go", "list", "-m", "-f", "{{.Dir}}", "go.mau.fi/whatsmeow").Output()
	if err != nil {
		t.Fatalf("locate the pinned whatsmeow: %v", err)
	}
	dir := strings.TrimSpace(string(out))
	if dir == "" {
		t.Fatal("the pinned whatsmeow has no directory on this machine")
	}
	return dir
}
