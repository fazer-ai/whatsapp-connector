package whatsmeow

import (
	"bytes"
	"errors"
	"fmt"
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
// be discovered: the channel has to be named at the send. Anything that puts the channel
// somewhere else first walks past unmeasured -- a local (`ch := s.inbox`, then `ch <- p`)
// and, the one to actually watch for, a helper that takes it as a parameter:
//
//	func enqueue(inbox chan<- pending, p pending) { inbox <- p }
//
// Extracting a send helper is an ordinary refactor, which makes that the plausible way to
// lose a door by accident rather than on purpose. Closing it properly needs go/types,
// because the channel's identity is a type fact and not a syntactic one; the cheap AST
// trick would only catch the local and leave the shape that is actually likely.
//
// It is a known limit and not an oversight. All five doors in production name the channel
// at the send, and the textual fence this replaces had the same hole. What softens it is
// the vacuity guard above: move every door behind a helper and the fence fails loudly,
// saying it read N files and found no write at all. Losing one door quietly means
// extracting exactly one of the five and leaving the rest, which somebody who reads this
// paragraph first will not do.
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

// inboxElement is the type the rule is about, named once.
//
// It used to be spelled out in three independent places -- the match, the control, and the
// guard that says the type is still declared here -- and two of them agreeing was enough
// to look healthy. Change the match and the control together and the rule goes on
// reporting its control, finds nothing in the package, and passes with a real door open.
// One name means no two of the three can drift into agreeing with each other.
const inboxElement = "pending"

// carriersAllowed names the functions that may take the session inbox as a parameter, and
// what each one is for.
//
// It is empty, and that is the healthy state rather than a starting point: no production
// function in this package takes a channel of `pending` today, and the fence below exists
// so that stays a decision instead of an accident. Adding an entry is the whole point --
// the rule cannot tell a deliberate helper from a door lost by mistake, so it asks the
// person who knows.
//
// The key names one function, and the failure prints the exact string to paste: a plain
// name for a function, `(*Session).name` for a method, and for a literal what calls it
// plus where it lives, as in `offer in beta@queueing_internal_test.go:9`. A method and a
// function of the same name coexist in Go, and two functions can each hold a local
// `offer`, so anything shorter would let one entry excuse two. The value is why, and it
// may not be blank: an entry that has to say what it is for is harder to add without
// thinking than a name on a list.
var carriersAllowed = map[string]string{}

// The second half of the inbox fence, and it exists because the first half can only see a
// send whose channel is named at the send.
//
//	func offer(inbox chan<- pending, p pending) bool { ... }
//
// Extract the fast path of `emitting` into that and one of the five doors leaves the
// measurement: the suite stays green, `TestEveryWriteToTheInboxIsMeasured` still finds
// four doors and so never reaches its own vacuity guard, and `wac_session_inbox_depth`
// quietly begins describing four fifths of the traffic. Extracting a send helper is an
// ordinary Tuesday refactor, which is what makes it the plausible way to lose a door.
//
// Knowing that such a parameter and `s.inbox` are the same channel is a type fact, so
// closing this properly means go/types. This is the cheap rule instead: nobody takes the
// channel as a parameter unless they have written their name and their reason above. It
// does not ask what the function does with it, so it is broader than the real risk and
// narrower than the whole hole -- the boundary is written out on `carriesTheInbox`, which is where the match is.
func TestNoFunctionTakesTheInboxWithoutSayingWhy(t *testing.T) {
	t.Parallel()

	const pkg = "."

	// The rule fires at all. Its expected finding in this package is zero, for ever, so
	// "found nothing" is the healthy answer and cannot double as evidence that the
	// matching still works. A control that must be reported is what separates the two.
	control := carriersIn(t, token.NewFileSet(), "control.go", fmt.Sprintf(
		"package whatsmeow\nfunc aControlThatMustBeSeen(inbox chan<- %[1]s, p %[1]s) { inbox <- p }\n",
		inboxElement))
	if len(control) != 1 || control[0].name != "aControlThatMustBeSeen" {
		t.Fatalf("the rule did not report its own control (%d finding(s)): it is matching "+
			"nothing, so a green result below would mean nothing either", len(control))
	}

	found, scanned, declared := carriers(t, pkg)
	if scanned == 0 {
		t.Fatalf("read %d production file(s) in %q: a rule with nothing to read is not a rule",
			scanned, pkg)
	}
	// And it is still about this package's channel. Rename the type and every match
	// silently stops happening, which reads exactly like the healthy zero above.
	if !declared {
		t.Fatalf("no type named %q is declared in the %d file(s) read: this rule has lost its "+
			"subject and now matches nothing by construction", inboxElement, scanned)
	}

	for _, taker := range found {
		why, allowed := carriersAllowed[taker.key]
		switch {
		case !allowed:
			t.Errorf("%s:%d: %s takes the session inbox as a parameter:\n\t%s\n"+
				"a door reached through a parameter reports no wait and no depth, and nothing "+
				"fails. Either do not write it, or add %q to carriersAllowed with what it is for",
				taker.file, taker.line, taker.name, taker.text, taker.key)
		case strings.TrimSpace(why) == "":
			t.Errorf("%s:%d: %s is in carriersAllowed with no reason given:\n"+
				"the reason is the whole exemption, since the rule cannot tell a deliberate "+
				"helper from a door lost by accident", taker.file, taker.line, taker.name)
		}
	}
	// An entry that names nothing is an exemption nobody can check, and it outlives the
	// function it was written for.
	for key := range carriersAllowed {
		if !slices.ContainsFunc(found, func(c inboxCarrier) bool { return c.key == key }) {
			t.Errorf("carriersAllowed names %q, and no function in the package matches it: "+
				"either it was renamed or it is gone, and the exemption should go with it", key)
		}
	}
}

// inboxCarrier is one function that takes a channel of pending.
type inboxCarrier struct {
	key  string
	name string
	file string
	line int
	text string
}

// carriers reads the package and reports every function that takes the inbox, whether the
// type it matches is still declared here, and how many files it got to read.
func carriers(t *testing.T, dir string) (found []inboxCarrier, scanned int, declared bool) {
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
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		found = append(found, carriersIn(t, fset, name, string(body))...)
		scanned++
		if declaresTheElement(t, fset, name, string(body)) {
			declared = true
		}
	}
	return found, scanned, declared
}

// carriersIn is the rule itself, over one file's source. It takes the source rather than
// a path so the control above can hold the rule to a case the test wrote itself.
//
// A function literal answers to whatever names it -- the variable it is bound to, or
// failing that the function it sits inside -- and then to where it is written. Both halves
// are load-bearing. A bare `file:line` is accurate and useless, since the key is what
// somebody has to type into carriersAllowed; a bare name is worse than useless, because
// two functions can each hold a local `offer := func(inbox chan<- pending)` and one
// exemption would then quietly excuse both, which is the thing keys exist to prevent.
func carriersIn(t *testing.T, fset *token.FileSet, name, src string) []inboxCarrier {
	t.Helper()

	parsed, err := parser.ParseFile(fset, name, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	var found []inboxCarrier
	seen := map[token.Pos]bool{}
	record := func(key string, sig *ast.FuncType, pos token.Pos) {
		if seen[pos] || !carriesTheInbox(sig) {
			return
		}
		seen[pos] = true
		found = append(found, oneCarrier(fset, name, key, sig, pos))
	}
	// A literal's key carries where it is as well as what it is called. Declarations do
	// not need it: Go already makes a function name unique in a package, and a method
	// unique on its type.
	literal := func(called string, sig *ast.FuncType, pos token.Pos) {
		record(keyFor(fset, parsed, name, called, pos), sig, pos)
	}

	// Bound to a name first, so the name wins over the position.
	ast.Inspect(parsed, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.ValueSpec:
			for i, value := range n.Values {
				if lit, ok := value.(*ast.FuncLit); ok && i < len(n.Names) {
					literal(n.Names[i].Name, lit.Type, lit.Pos())
				}
			}
		case *ast.AssignStmt:
			for i, value := range n.Rhs {
				lit, ok := value.(*ast.FuncLit)
				if !ok || i >= len(n.Lhs) {
					continue
				}
				if to, ok := n.Lhs[i].(*ast.Ident); ok {
					literal(to.Name, lit.Type, lit.Pos())
				}
			}
		}
		return true
	})
	for _, decl := range parsed.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok {
			record(declName(fn), fn.Type, fn.Pos())
		}
	}
	// Whatever is left is genuinely anonymous, and answers to where it was written.
	ast.Inspect(parsed, func(node ast.Node) bool {
		if lit, ok := node.(*ast.FuncLit); ok {
			record(keyFor(fset, parsed, name, "a literal", lit.Pos()), lit.Type, lit.Pos())
		}
		return true
	})
	return found
}

// keyFor names a literal after what calls it, the function it sits in, and where it is.
//
// All three matter, and the last two for the same reason. Without the enclosing function,
// `offer@file.go:9` says nothing about which `offer` it is: insert five lines of comment
// above the function before it and another function's literal lands on line 9, inheriting
// an exemption that was written for its neighbour, with one entry and one failure before
// and after. With the function in the key that shift makes the exemption match nothing,
// and an entry that names nothing already fails loudly.
func keyFor(fset *token.FileSet, file *ast.File, name, called string, pos token.Pos) string {
	at := fset.Position(pos).Line
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || pos < fn.Pos() || pos > fn.End() {
			continue
		}
		return fmt.Sprintf("%s in %s@%s:%d", called, declName(fn), name, at)
	}
	// Package level, so there is no function to sit in.
	return fmt.Sprintf("%s@%s:%d", called, name, at)
}

// carriesTheInbox is the match, and it reads a spelling: a channel of `pending` written
// out in a parameter list.
//
// The direction is the whole judgement on what counts. A parameter that can only be
// received from is not a door, because it cannot be sent to at all. Letting it through is
// a decision and not an oversight: it is a different danger -- a second consumer racing
// the forwarder, which is invariant 3's business -- and folding it in here would file it
// under the wrong name and call it handled.
//
// Because spelling is all it reads, this is where the rule stops, and the list is written
// out rather than left to be discovered. Anything that puts the same channel behind
// another name gets past: a type alias or a named channel type (`type inboxCh = chan
// pending`, one line and as innocent as the refactor this catches), a type parameter, and
// the channel wrapped in a struct field, a slice or a map. So does a function that returns
// the channel instead of taking it, which is past both halves of the fence, since the
// channel of that send is a call and not a selector. Each one needs go/types to see,
// because each is the same channel wearing a different spelling.
//
// Two shapes that look like they belong on that list are caught, by the other half rather
// than this one: a struct whose field is named `inbox`, and a closure that captures the
// session. Both end up writing `.inbox <-` somewhere, which is what
// TestEveryWriteToTheInboxIsMeasured reads.
func carriesTheInbox(sig *ast.FuncType) bool {
	if sig.Params == nil {
		return false
	}
	for _, param := range sig.Params.List {
		kind := param.Type
		if spread, ok := kind.(*ast.Ellipsis); ok {
			kind = spread.Elt
		}
		channel, ok := kind.(*ast.ChanType)
		if !ok || channel.Dir == ast.RECV {
			continue
		}
		if named, ok := channel.Value.(*ast.Ident); ok && named.Name == inboxElement {
			return true
		}
	}
	return false
}

// declName tells a method from a function of the same name, which Go lets coexist. One
// entry in carriersAllowed must not excuse both.
func declName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	var receiver bytes.Buffer
	if err := printer.Fprint(&receiver, token.NewFileSet(), fn.Recv.List[0].Type); err != nil {
		receiver.WriteString("?")
	}
	return fmt.Sprintf("(%s).%s", receiver.String(), fn.Name.Name)
}

func declaresTheElement(t *testing.T, fset *token.FileSet, name, src string) bool {
	t.Helper()

	parsed, err := parser.ParseFile(fset, name, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	declared := false
	ast.Inspect(parsed, func(node ast.Node) bool {
		if spec, ok := node.(*ast.TypeSpec); ok && spec.Name.Name == inboxElement {
			declared = true
		}
		return true
	})
	return declared
}

func oneCarrier(fset *token.FileSet, file, key string, sig *ast.FuncType, pos token.Pos) inboxCarrier {
	var rendered bytes.Buffer
	if err := printer.Fprint(&rendered, fset, sig); err != nil {
		rendered.WriteString("<the signature would not render>")
	}
	return inboxCarrier{
		key:  key,
		name: key,
		file: file,
		line: fset.Position(pos).Line,
		text: rendered.String(),
	}
}
