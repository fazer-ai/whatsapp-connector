package whatsmeow

import (
	"bytes"
	"errors"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/rs/zerolog"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

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
// The fence over the package's own source, and the promise it keeps is what the depth
// histogram means: every door into the session inbox reports the wait it paid and the
// depth it arrived at, or `wac_session_inbox_depth` describes the doors that report
// rather than the queue.
func TestEveryWriteToTheInboxIsMeasured(t *testing.T) {
	t.Parallel()

	// The package's own directory: the test runs there.
	const pkg = "."

	doors, scanned := inboxDoors(t, pkg)
	// A fence that finds nothing passes everything. Renaming the field, moving the
	// package, or pointing this at the wrong directory would each leave a green test
	// asserting nothing whatsoever -- which is this fence's own bug one level up: the
	// thing that went missing with no way left to announce itself. The count of files is
	// in the message because it separates the two ways of finding nothing, and they have
	// different causes: nothing to read, or nothing to measure in what was read.
	if len(doors) == 0 {
		t.Fatalf("scanned %d production file(s) in %q and found no write to an inbox in any "+
			"of them: this fence is measuring nothing at all, whatever its result says",
			scanned, pkg)
	}
	for _, found := range doors {
		if found.reports {
			continue
		}
		t.Errorf("%s:%d writes to the inbox and does not report it:\n\t%s\n"+
			"every door into the inbox reports, or the depth histogram describes "+
			"the doors that do rather than the queue", found.file, found.line, found.text)
	}
}

// inboxDoor is one send to a session inbox, and whether the reporting sits where the
// send does.
type inboxDoor struct {
	file    string
	line    int
	text    string
	reports bool
}

// inboxDoors parses every production file in dir and finds the sends to `.inbox`.
//
// It walks the directory rather than a list of file names. The list this started as was
// right on the day it was written -- it named the two files that had inbox writes -- and
// that is exactly the property a fence must not have, because the twenty-two files it did
// not name were free to open a door nobody measured.
//
// Structural rather than textual, too. The version this replaces looked for `s.queued(`
// within six lines of the send, which is a guess about layout: it passed a send whose
// only nearby `s.queued(` belonged to something else, and it failed a send whose
// reporting sat a comment block further down, with a message saying the send did not
// report when it did. Both were out of reach while two files were fenced and both come
// into reach at twenty-four, so the check is now the thing it was approximating all
// along -- the report is a statement of the same list the send belongs to.
func inboxDoors(t *testing.T, dir string) (doors []inboxDoor, scanned int) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read the package directory %q: %v", dir, err)
	}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		// A file that was listed and then cannot be read fails here instead of being
		// skipped. Skipping would shrink the fence by exactly the amount nobody notices.
		parsed, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		scanned++
		ast.Inspect(parsed, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CommClause:
				// `case s.inbox <- x:` -- the send is the guard, and the reporting
				// belongs to the arm that guard opens.
				if send, ok := n.Comm.(*ast.SendStmt); ok && sendsToAnInbox(send) {
					doors = append(doors, oneDoor(fset, name, send, reportedIn(n.Body)))
				}
				doors = append(doors, doorsIn(fset, name, n.Body)...)
			case *ast.CaseClause:
				doors = append(doors, doorsIn(fset, name, n.Body)...)
			case *ast.BlockStmt:
				doors = append(doors, doorsIn(fset, name, n.List)...)
			}
			return true
		})
	}
	return doors, scanned
}

// doorsIn finds the sends written straight into a statement list, which is the shape a
// door takes when it is not the guard of a select.
func doorsIn(fset *token.FileSet, file string, list []ast.Stmt) []inboxDoor {
	var doors []inboxDoor
	for _, stmt := range list {
		if send, ok := stmt.(*ast.SendStmt); ok && sendsToAnInbox(send) {
			doors = append(doors, oneDoor(fset, file, send, reportedIn(list)))
		}
	}
	return doors
}

// reportedIn is the whole judgement: a call to queued standing as a statement of this
// same list. Nested deeper it is some other path's reporting, and a send that is measured
// only when an `if` happens to go one way is a send that is not measured.
func reportedIn(list []ast.Stmt) bool {
	for _, stmt := range list {
		expr, ok := stmt.(*ast.ExprStmt)
		if !ok {
			continue
		}
		call, ok := expr.X.(*ast.CallExpr)
		if !ok {
			continue
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "queued" {
			return true
		}
	}
	return false
}

// sendsToAnInbox is the fence's boundary, and it is worth stating rather than leaving to
// be discovered: the channel has to be named at the send. Bound to a local first --
// `ch := s.inbox` and then `ch <- pending{...}` -- the send walks past unmeasured. That
// is a known limit and not an oversight. All five doors in production are written
// `s.inbox <-` directly, the textual fence this replaces had exactly the same hole, and
// resolving aliases means carrying types into a test whose job is catching the omission
// somebody makes by accident, not the evasion nobody has written. Somebody who does write
// it will find this paragraph before they find the missing metric.
func sendsToAnInbox(send *ast.SendStmt) bool {
	sel, ok := send.Chan.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "inbox"
}

func oneDoor(fset *token.FileSet, file string, send *ast.SendStmt, reports bool) inboxDoor {
	var rendered bytes.Buffer
	if err := printer.Fprint(&rendered, fset, send); err != nil {
		rendered.WriteString("<the send would not render>")
	}
	return inboxDoor{
		file:    file,
		line:    fset.Position(send.Pos()).Line,
		text:    rendered.String(),
		reports: reports,
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

// The second door, driven the way WhatsApp drives it. `TestPresenceLeavesNothingBehind`
// already proves the board is left clean; what it cannot say is whether the loss shows up
// anywhere, and a presence dropped here is never told to the client, never retried and
// never logged as anything but a debug line. This counter is the only place it exists.
func TestAPresenceTheInboxHadNoRoomForIsCounted(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	watch := &emitWatch{}
	session.queueing = watch
	filler := pending{event: engine.Emission{Type: protocol.EventSessionState, Payload: []byte(`{}`)}}
	// Park the forwarder before filling, for the reason the neighbouring test gives: it
	// races the fill for the slot it frees on its way to parking.
	session.inbox <- filler
	waitUntil(t, "the forwarder to be holding an emission", func() bool { return len(session.inbox) == 0 })
	for range cap(session.inbox) {
		session.inbox <- filler
	}

	jid := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: jid, Sender: jid}, State: waTypes.ChatPresencePaused,
	})

	if got := watch.dropped(); !slices.Equal(got, []protocol.EventType{protocol.EventChatPresence}) {
		t.Errorf("dropped = %v, want one chat.presence", got)
	}
}

// The third door, and the one that is easiest to leave uncounted: the presence is not
// dropped where it was posted but a publish later, when the retry finds the queue it fit
// into the first time now full. The client was never told the first attempt failed, so
// without this the state simply stops being true and nothing anywhere says so.
func TestAPresenceRetriedIntoAFullInboxIsCounted(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	watch := &emitWatch{}
	session.queueing = watch
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)

	jid := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	// `paused` is the state that ends a burst: nothing comes after it to correct it, which
	// is why it carries a Settle at all and why the retry exists.
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: jid, Sender: jid}, State: waTypes.ChatPresencePaused,
	})

	var settle func(error)
	session.boardMu.Lock()
	for _, entry := range session.board {
		settle = entry.emission.Settle
	}
	held := len(session.board)
	session.boardMu.Unlock()
	if held != 1 || settle == nil {
		t.Fatalf("the board holds %d entries and a settle func that is %v, want 1 and non-nil",
			held, settle != nil)
	}
	// Room for the first marker and none for its retry, which is the whole state this
	// test is about.
	for range cap(session.inbox) - len(session.inbox) {
		session.inbox <- pending{event: engine.Emission{Type: protocol.EventSessionState, Payload: []byte(`{}`)}}
	}

	settle(errors.New("the publisher would not take it"))

	if got := watch.dropped(); !slices.Equal(got, []protocol.EventType{protocol.EventChatPresence}) {
		t.Errorf("dropped = %v, want one chat.presence", got)
	}
	session.boardMu.Lock()
	left := len(session.board)
	session.boardMu.Unlock()
	if left != 0 {
		t.Errorf("%d presences are on a board with nothing coming to publish them", left)
	}
}
