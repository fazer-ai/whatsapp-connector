package whatsmeow

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// The count is what the rule is about, so it is asked directly rather than through a
// session: whatsmeow resets it on the first answered ping, so "two" means two in a row
// and not two since the process started.
func TestOnlyAKeepAliveMissedTwiceInARowIsALostSocket(t *testing.T) {
	t.Parallel()

	for _, missed := range []struct {
		count int
		lost  bool
	}{
		{count: 0, lost: false},
		{count: 1, lost: false},
		{count: 2, lost: true},
		{count: 7, lost: true},
	} {
		if got := keepAliveIsLost(&waEvents.KeepAliveTimeout{ErrorCount: missed.count}); got != missed.lost {
			t.Errorf("%d missed keepalive(s) read as lost=%v, want %v", missed.count, got, missed.lost)
		}
	}
}

// A single stalled ping is a blip, and a session that took its socket down for one would
// reconnect through every bad minute a mobile network has.
func TestOneMissedKeepAliveLeavesTheSocketAlone(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	var written bytes.Buffer
	session.log = zerolog.New(&written)
	dialedAndConnected(session)

	// The last answered ping is dated inside this connection, which is what a timeout
	// about it looks like. The stamp it is compared against is the one `setConnected`
	// writes, so nothing here fills in for the production path.
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 1, LastSuccess: time.Now()})

	if got := session.state(); got != "open" {
		t.Fatalf("the session left itself %q over one missed keepalive", got)
	}
	if written.Len() != 0 {
		t.Fatalf("a single missed keepalive was acted on: %s", written.String())
	}
}

// A timeout dispatched about a socket that is already gone. whatsmeow sends each one from
// a goroutine of its own, so one can arrive after the connection it is about has been
// replaced, and taking that at face value takes down the healthy socket that replaced it.
func TestAKeepAliveTimeoutFromAReplacedSocketIsIgnored(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	var written bytes.Buffer
	session.log = zerolog.New(&written)
	dialedAndConnected(session)

	// The run of failed pings belongs to a connection that ended before this one began.
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now().Add(-5 * time.Minute)})

	if got := session.state(); got != "open" {
		t.Fatalf("a timeout about an older socket left the session %q", got)
	}
	if written.Len() != 0 {
		t.Fatalf("a timeout about an older socket was acted on: %s", written.String())
	}
}

// And a session with no socket has nothing to take down.
func TestAKeepAliveTimeoutWithNoConnectionIsIgnored(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	var written bytes.Buffer
	session.log = zerolog.New(&written)

	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now().Add(-70 * time.Second)})

	if written.Len() != 0 {
		t.Fatalf("a timeout with no connection under it was acted on: %s", written.String())
	}
}

// whatsmeow redials on its own after a drop, and that retry never passes through `dial`:
// it goes straight into its own `connect`. A session that dated the connection only from
// the dials it asks for would carry the dropped socket's stamp into the socket that
// replaced it, and the pings that went unanswered before the drop -- dispatched from
// goroutines that outlive the loop they came from -- would read as current and take the
// replacement down.
func TestAKeepAliveTimeoutFromBeforeAReconnectIsIgnored(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	var written bytes.Buffer
	session.log = zerolog.New(&written)

	dialled := time.Now()
	clock := dialled
	session.wallClock = func() time.Time { return clock }
	// So the drop reads as one whatsmeow will redial, which is what it is: the session is
	// paired, and the arm answers an unpaired one with `close` instead.
	session.relearn(session.current())
	dialedAndConnected(session)

	// A minute in the socket drops and whatsmeow starts its own retry. Nothing in this
	// path dials, so nothing else can re-date the connection.
	clock = dialled.Add(time.Minute)
	session.handle(&waEvents.Disconnected{})
	if state := decode(t, next(t, session).Payload)["state"]; state != "reconnecting" {
		t.Fatalf("the drop published state=%v, so this is not the path whatsmeow redials", state)
	}
	clock = dialled.Add(70 * time.Second)
	session.setConnected(true)

	// The run of pings the old socket stopped answering, arriving after the new one is up.
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: dialled.Add(5 * time.Second)})

	if got := session.state(); got != "open" {
		t.Fatalf("a timeout from before the reconnect left the session %q", got)
	}
	if written.Len() != 0 {
		t.Fatalf("a timeout from before the reconnect was acted on: %s", written.String())
	}
}

// And the socket whatsmeow swaps under a session that never hears about it. Its 515 path
// ("restart required") disconnects and reconnects inside itself, and the disconnect it
// marks as expected publishes nothing, so the first thing this session learns is a
// `Connected` while it still believes it is connected. Nothing before that instant is
// observable from here, and the stamp left behind describes the socket that is gone.
func TestAKeepAliveTimeoutFromASocketSwappedUnderTheSessionIsIgnored(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	var written bytes.Buffer
	session.log = zerolog.New(&written)

	dialled := time.Now()
	clock := dialled
	session.wallClock = func() time.Time { return clock }
	dialedAndConnected(session)

	// A minute in, whatsmeow answers a 515 by replacing the socket on its own. No drop,
	// no `Disconnected`, no dial: this event is the whole of what the session sees.
	clock = dialled.Add(time.Minute)
	session.handle(&waEvents.Connected{})
	if emission := next(t, session); emission.Type != protocol.EventSessionState {
		t.Fatalf("the replacement published %s", emission.Type)
	}

	// The run of pings the socket it replaced stopped answering, arriving late.
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: dialled.Add(5 * time.Second)})

	if got := session.state(); got != "open" {
		t.Fatalf("a timeout about the socket that was swapped out left the session %q", got)
	}
	if written.Len() != 0 {
		t.Fatalf("a timeout about the socket that was swapped out was acted on: %s", written.String())
	}
}

// And the second one is acted on rather than waited out. What the log line stands for is
// the reset beside it; the reset itself is not observable from here, because a client with
// no socket has nothing to take down, and the phase that measures it is the live one.
func TestTheSecondMissedKeepAliveIsActedOn(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	var written bytes.Buffer
	session.log = zerolog.New(&written)
	dialedAndConnected(session)

	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})

	// Before the reset and not after it: ResetConnection blocks on the close handshake
	// holding whatsmeow's socket lock, and a session still reporting `open` in that window
	// accepts commands into the lock the close is holding.
	if got := session.state(); got != "reconnecting" {
		t.Fatalf("the session reports %q while taking its own socket down, so readyToSend still accepts", got)
	}
	// The client reads the event stream and nothing else: an in-memory flag it cannot see
	// is a session that refuses its commands while its last published state says `open`.
	emission := next(t, session)
	if emission.Type != protocol.EventSessionState {
		t.Fatalf("the session published %s while taking its socket down", emission.Type)
	}
	var published map[string]any
	if err := json.Unmarshal(emission.Payload, &published); err != nil {
		t.Fatalf("the session published something unreadable: %v", err)
	}
	if state := published["state"]; state != "reconnecting" {
		t.Fatalf("the session published state=%v while taking its socket down", state)
	}

	out := written.String()
	if !strings.Contains(out, "taking it down") {
		t.Fatalf("the second missed keepalive was waited out instead: %q", out)
	}
	if !strings.Contains(out, `"missed":2`) {
		t.Fatalf("the log does not say how many went unanswered: %q", out)
	}
}

// The drop the session causes is published by the handler that causes it, and whatsmeow
// starts the reconnect from the same instant it dispatches the event. Handled after the
// replacement announced itself, the drop would describe a socket that is up, and nothing
// would come after it to put that right: the replacement is healthy, so it produces no
// further event, and every command is refused from then on.
func TestTheDropTheSessionCausedIsNotAppliedOverTheSocketThatReplacedIt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)

	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})
	if state := decode(t, next(t, session).Payload)["state"]; state != "reconnecting" {
		t.Fatalf("the session published state=%v while taking its socket down", state)
	}

	// whatsmeow authenticates the replacement before the drop it dispatched is handled.
	session.handle(&waEvents.Connected{})
	if state := decode(t, next(t, session).Payload)["state"]; state != "open" {
		t.Fatalf("the replacement published state=%v", state)
	}
	session.handle(&waEvents.Disconnected{})

	if got := session.state(); got != "open" {
		t.Fatalf("the drop the session caused was applied over the socket that replaced it, "+
			"leaving the session %q with nothing to correct it", got)
	}
}

// And the mark that does it cannot outlive the socket it was made for: `ResetConnection`
// on a client whose socket is already gone produces no `Disconnected` at all, and a mark
// left standing would swallow the next real drop instead.
func TestAFreshDialDropsAMarkNoDisconnectEverClaimed(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)

	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})
	next(t, session)

	// Nothing claimed that mark, and the session dials again on its own.
	dialedAndConnected(session)
	session.handle(&waEvents.Disconnected{})

	if got := session.state(); got != "reconnecting" {
		t.Fatalf("a drop on a new socket was swallowed by a mark left over from an older one: %q", got)
	}
}

// A timeout whose run of failures is already over. whatsmeow dispatches the recovery from
// a goroutine of its own too, so it can be handled before a timeout that preceded it, and
// acting on that one resets a socket that is answering again.
func TestAKeepAliveTimeoutTheSocketRecoveredFromIsIgnored(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	var written bytes.Buffer
	session.log = zerolog.New(&written)

	started := time.Now()
	clock := started
	session.wallClock = func() time.Time { return clock }
	dialedAndConnected(session)

	// The socket went quiet and then answered again, on the same connection.
	clock = started.Add(time.Minute)
	session.handle(&waEvents.KeepAliveRestored{})

	// The second timeout of the run that just ended, arriving after it.
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: started.Add(5 * time.Second)})

	if got := session.state(); got != "open" {
		t.Fatalf("a timeout the socket already recovered from left the session %q", got)
	}
	if written.Len() != 0 {
		t.Fatalf("a timeout the socket already recovered from was acted on: %s", written.String())
	}
}

// And the run that begins from the very ping that ended the last one. A recovery is
// recorded when this session handles it, which is later than whatsmeow saw it, so a rule
// with no slack in it would read the next run of failures as the previous one arriving late
// and leave a genuinely dead socket up.
func TestARunOfFailuresThatBeganRightAfterARecoveryIsStillActedOn(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	started := time.Now()
	clock := started
	session.wallClock = func() time.Time { return clock }
	dialedAndConnected(session)

	clock = started.Add(time.Minute)
	session.handle(&waEvents.KeepAliveRestored{})

	// The pings stopped again, counted from one answered just before this session got
	// round to the recovery.
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: started.Add(55 * time.Second)})

	if got := session.state(); got != "reconnecting" {
		t.Fatalf("a run of failures dated from the ping that ended the last one left the session %q", got)
	}
	if state := decode(t, next(t, session).Payload)["state"]; state != "reconnecting" {
		t.Fatalf("the session published state=%v", state)
	}
}

// The two ways to take a socket down differ in one thing and it is the thing this change
// is about: `Disconnect` marks the disconnect as expected, and `onDisconnect` publishes
// `events.Disconnected` only when it was not -- so a session that used it would take its
// socket down and go on reporting `open`, which is the state this exists to leave. Read
// off the source because nothing else here can see the difference: a client with no socket
// does the same nothing under either call, and the phase that can tell them apart is the
// live one.
func TestTheKeepAliveHandlerResetsTheConnectionRatherThanDisconnecting(t *testing.T) {
	t.Parallel()

	taking := theBodyOf(t, "func (s *Session) resetUnlessReplaced(")
	if !strings.Contains(taking, "ResetConnection()") {
		t.Fatalf("the socket is not taken down with a reset:\n%s", taking)
	}
	if strings.Contains(taking, "Disconnect()") {
		t.Fatalf("the socket is taken down with a disconnect, which publishes nothing and leaves "+
			"the session reporting open:\n%s", taking)
	}
	handler := theCaseFor(t, "*waEvents.KeepAliveTimeout")
	if strings.Contains(handler, "Disconnect()") {
		t.Fatalf("the keepalive handler disconnects, which publishes nothing and leaves the session reporting open:\n%s", handler)
	}
}

// theBodyOf returns a function of the session, from its signature to its closing brace.
func theBodyOf(t *testing.T, signature string) string {
	t.Helper()

	raw, err := os.ReadFile("session.go")
	if err != nil {
		t.Fatalf("read the session: %v", err)
	}
	lines := strings.Split(string(raw), "\n")
	for i, line := range lines {
		if !strings.HasPrefix(line, signature) {
			continue
		}
		for end := i + 1; end < len(lines); end++ {
			if lines[end] == "}" {
				return strings.Join(lines[i:end+1], "\n")
			}
		}
		t.Fatalf("%s does not end", signature)
	}
	t.Fatalf("the session has no %s", signature)
	return ""
}

// theCaseFor returns one arm of the event switch, from its case line to the next one.
func theCaseFor(t *testing.T, event string) string {
	t.Helper()

	raw, err := os.ReadFile("session.go")
	if err != nil {
		t.Fatalf("read the session: %v", err)
	}
	lines := strings.Split(string(raw), "\n")
	opens := "\tcase " + event + ":"
	for i, line := range lines {
		if line != opens {
			continue
		}
		for end := i + 1; end < len(lines); end++ {
			if strings.HasPrefix(lines[end], "\tcase ") || strings.HasPrefix(lines[end], "\tdefault:") {
				return strings.Join(lines[i:end], "\n")
			}
		}
		t.Fatalf("the arm for %s does not end", event)
	}
	t.Fatalf("the event switch has no arm for %s", event)
	return ""
}

// And the goroutine it is started on stands down if a connection landed while it waited to
// run. `ResetConnection` reads the client's socket when it runs, so one scheduled late over
// a socket whatsmeow replaced on its own would close the replacement.
func TestTheResetStandsDownWhenAConnectionLandedFirst(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	var written bytes.Buffer
	session.log = zerolog.New(&written)
	dialedAndConnected(session)

	judged := session.transitions.Load()
	// A replacement announced itself while the reset was still waiting for a turn.
	session.setConnected(true)

	session.resetUnlessReplaced(session.current(), judged)

	if !strings.Contains(written.String(), "leaving it alone") {
		t.Fatalf("the reset went ahead over a connection that landed after the judgement: %q", written.String())
	}
}

// And goes ahead when nothing did.
func TestTheResetGoesAheadWhenNothingLandedFirst(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	var written bytes.Buffer
	session.log = zerolog.New(&written)
	dialedAndConnected(session)

	session.resetUnlessReplaced(session.current(), session.transitions.Load())

	if written.Len() != 0 {
		t.Fatalf("the reset stood down with nothing between it and the judgement: %s", written.String())
	}
}

// The order inside the arm is load-bearing and invisible to every test that can run here:
// with no socket under it, a reset returns at once and an inbox nobody filled never makes
// `emit` wait, so every arrangement of these three lines looks the same from outside. What
// they cost live is the window in which the session refuses commands while its last
// published state still says `open`, and the socket the reset lands on when the publish in
// front of it waited on a full inbox.
func TestTheKeepAliveHandlerTakesTheSocketDownBeforeItWaitsOnThePublish(t *testing.T) {
	t.Parallel()

	handler := theCaseFor(t, "*waEvents.KeepAliveTimeout")
	refused := strings.Index(handler, "setReconnecting(true)")
	closed := strings.Index(handler, "resetUnlessReplaced(")
	published := strings.Index(handler, "s.emit(")
	if refused < 0 || closed < 0 || published < 0 {
		t.Fatalf("the keepalive handler does not refuse, close and publish:\n%s", handler)
	}
	if refused > closed {
		t.Fatalf("the keepalive handler closes the socket before it stops accepting for it, so "+
			"`readyToSend` hands commands into the lock the close is holding:\n%s", handler)
	}
	if closed > published {
		t.Fatalf("the keepalive handler publishes before it closes, and `emit` waits on a full "+
			"inbox: the reset would then land on whatever socket the client had by the time it "+
			"cleared, which is a healthy one:\n%s", handler)
	}
}

// And the close does not hold the publish. `ResetConnection` blocks on the close handshake
// holding whatsmeow's socket lock, seconds of it on a quiet path, and a session that waited
// for that before saying anything would spend them refusing commands with `open` as its last
// published state. Read off the source because a client with no socket returns from the
// reset at once, so here the two are indistinguishable.
func TestTheKeepAliveHandlerDoesNotWaitForTheCloseHandshake(t *testing.T) {
	t.Parallel()

	handler := theCaseFor(t, "*waEvents.KeepAliveTimeout")
	if !strings.Contains(handler, "go s.resetUnlessReplaced(client, judged)") {
		t.Fatalf("the keepalive handler waits for the close handshake before publishing:\n%s", handler)
	}
	// And on the client it judged, not on whatever the session is holding once that
	// goroutine is scheduled.
	if !strings.Contains(handler, "client := s.current()") {
		t.Fatalf("the keepalive handler resets whatever socket the session has when the "+
			"goroutine runs, rather than the one it judged:\n%s", handler)
	}
	// The count guards the socket being replaced under the same client, which is what
	// whatsmeow does on its own. It does not guard the session swapping the client itself:
	// `adopt` rebuilds one after a logout and writes no connection, so the count does not
	// move and only the pinned pointer separates the two. Read off the source because a
	// client with no socket is reset invisibly, whichever one it is.
	taking := theBodyOf(t, "func (s *Session) resetUnlessReplaced(")
	if strings.Contains(taking, "s.current()") {
		t.Fatalf("the reset reads the session's client when it runs instead of the one it was "+
			"handed, so a session that adopted a new client in between takes that one down:\n%s", taking)
	}
}

func TestTheConnectionIsStampedWhenItIsDialledAndNotWhenItIsAnnounced(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	dialled := time.Now()
	clock := dialled
	session.wallClock = func() time.Time { return clock }

	session.setDialing(true)
	// The authentication a dial waits for is not instant: prekeys and the passive switch
	// come first, and whatsmeow's keepalive clock has been running since the socket came
	// up. Driven rather than measured, because the gap this is about is the one the real
	// thing can take seconds over and two `time.Now()` calls in a row cannot produce.
	clock = dialled.Add(30 * time.Second)
	session.setConnected(true)

	if stamped := session.lastKnownAlive(); !stamped.Equal(dialled) {
		t.Fatalf("the connection is dated %s, the announcement, rather than %s, the dial, "+
			"so every keepalive timeout on a socket slow to answer reads as stale and the socket stays up",
			stamped.Format(time.TimeOnly), dialled.Format(time.TimeOnly))
	}
}

// Every other arm of the event switch that moves the connection takes this, and this one
// reads the connection and then acts on it: a hand-back or a reconnect settling in between
// leaves it publishing `reconnecting` over a session that is closing, or resetting a socket
// that replaced the one these pings were about. Read off the source because a lock held
// correctly and a lock not taken at all look the same from a single goroutine.
func TestTheKeepAliveHandlerSerialisesWithTheOtherTransitions(t *testing.T) {
	t.Parallel()

	handler := theCaseFor(t, "*waEvents.KeepAliveTimeout")
	if !strings.Contains(handler, "s.transition.Lock()") {
		t.Fatalf("the keepalive handler does not serialise with the other connection transitions:\n%s", handler)
	}
}

// dialedAndConnected puts a session on a socket the way a real one gets there: the attempt
// first, which is what whatsmeow's keepalive clock starts with, and the authentication
// after it.
func dialedAndConnected(session *Session) {
	session.setDialing(true)
	session.setConnected(true)
}
