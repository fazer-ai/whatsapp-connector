package whatsmeow

import (
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
}

var upstreamDefects = []upstreamDefect{
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
					t.Fatalf("%s no longer holds %q after the fragments before it, so %s "+
						"may be gone.\nThis is not a defect in this repository: re-read %s, "+
						"and if upstream fixed it, delete this entry and the limitation it "+
						"documents.",
						defect.enclosing, fragment, defect.what, defect.issue)
				}
				at += found + len(fragment)
			}
			for _, fragment := range defect.stillThere {
				if !strings.Contains(body, fragment) {
					t.Fatalf("%s no longer holds %q, so %s may be gone.\n"+
						"This is not a defect in this repository: re-read %s, and if upstream "+
						"fixed it, delete this entry and the limitation it documents.",
						defect.enclosing, fragment, defect.what, defect.issue)
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
