package whatsmeow

import (
	"bytes"
	"encoding/json"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	wm "go.mau.fi/whatsmeow"
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

	session, written := newLoggedTestSession(t, "5511999990001")
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

	session, written := newLoggedTestSession(t, "5511999990001")
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

	session, written := newLoggedTestSession(t, "5511999990001")

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

	session, written := newLoggedTestSession(t, "5511999990001")

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

	session, written := newLoggedTestSession(t, "5511999990001")

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

	session, written := newLoggedTestSession(t, "5511999990001")
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

// A timeout whose run of failures is already over. whatsmeow dispatches the recovery from
// a goroutine of its own too, so it can be handled before a timeout that preceded it, and
// acting on that one resets a socket that is answering again.
func TestAKeepAliveTimeoutTheSocketRecoveredFromIsIgnored(t *testing.T) {
	t.Parallel()

	session, written := newLoggedTestSession(t, "5511999990001")

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

// The recovery is serialised with the timeout decision, which is the other half of it: the
// timeout arm reads the stamp and then acts on what it read, so a recovery recorded in
// between would come too late to stop a reset of the socket it says is answering. Read off
// the source because both arms are driven from one goroutine here.
func TestTheKeepAliveRecoverySerialisesWithTheTimeoutDecision(t *testing.T) {
	t.Parallel()

	recovered := theCaseFor(t, "*waEvents.KeepAliveRestored")
	if !strings.Contains(recovered, "s.transition.Lock()") {
		t.Fatalf("the recovery does not serialise with the decision that reads it:\n%s", recovered)
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

// The two ways to take a socket down differ in one thing and it is the thing this change is
// about: `Disconnect` marks the disconnect as expected, and `onDisconnect` publishes
// `events.Disconnected` only when it was not -- so a session that used it would take its
// socket down and go on reporting `open`, which is the state this exists to leave. Read off
// the source because nothing else here can see the difference: a client with no socket does
// the same nothing under either call.
func TestTheKeepAliveHandlerResetsTheConnectionRatherThanDisconnecting(t *testing.T) {
	t.Parallel()

	taking := theBodyOf(t, "func (s *Session) resetUnlessReplaced(")
	if !strings.Contains(taking, "ResetConnection()") {
		t.Fatalf("the socket is not taken down with a reset:\n%s", taking)
	}
	if strings.Contains(taking, "client.Disconnect()") {
		t.Fatalf("the socket is taken down with a disconnect, which publishes nothing and leaves "+
			"the session reporting open:\n%s", taking)
	}
	handler := theCaseFor(t, "*waEvents.KeepAliveTimeout")
	if strings.Contains(handler, "Disconnect()") {
		t.Fatalf("the keepalive handler disconnects, which publishes nothing and leaves the "+
			"session reporting open:\n%s", handler)
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

	// The mark the handler puts up before it takes the socket down. Set here rather than by
	// driving the handler, because a session with no socket under it has no reset to take
	// down and the mark would be retired again before this test could use it; that the
	// handler is what puts it up is fenced below.
	session.announceDrop()

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

// And the handler is what puts that mark up, before it starts the reset that produces the
// drop. Read off the source because a client with no socket produces no drop to suppress.
func TestTheKeepAliveHandlerAnnouncesTheDropItIsAboutToCause(t *testing.T) {
	t.Parallel()

	handler := theCaseFor(t, "*waEvents.KeepAliveTimeout")
	announced := strings.Index(handler, "s.announceDrop()")
	closed := strings.Index(handler, "takeDownSoon(")
	if announced < 0 || closed < 0 {
		t.Fatalf("the keepalive handler does not announce the drop it causes:\n%s", handler)
	}
	if announced > closed {
		t.Fatalf("the keepalive handler starts the reset before marking the drop it causes, so "+
			"the drop can be handled before the mark is up:\n%s", handler)
	}
}

// And the goroutine the reset is started on stands down if a connection landed while it
// waited to run. `ResetConnection` reads the client's socket when it runs, so one scheduled
// late over a socket whatsmeow replaced on its own would close the replacement.
func TestTheResetStandsDownWhenAConnectionLandedFirst(t *testing.T) {
	t.Parallel()

	session, written := newLoggedTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)
	session.announceDrop()

	judged := session.transitions.Load()
	// A replacement announced itself while the reset was still waiting for a turn.
	session.setConnected(true)

	session.resetUnlessReplaced(session.current(), judged)

	if !strings.Contains(written.String(), "leaving it alone") {
		t.Fatalf("the reset went ahead over a connection that landed after the judgement: %q", written.String())
	}
	assertTheMarkStillStands(t, session)
}

// And it goes back to waiting when a command started between the decision and this
// goroutine getting a turn. Only the lifecycle three can start there, everything else being
// refused at the gate by then, and `logout` sends its removal IQ over the socket that is
// still up: cutting that off is the resend the whole guard exists to avoid.
func TestTheResetGoesBackToWaitingWhenACommandStartedFirst(t *testing.T) {
	t.Parallel()

	session, written := newLoggedTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)
	session.announceDrop()

	judged := session.transitions.Load()
	// A logout started while the takedown was still waiting for a turn.
	session.startCommand()

	session.resetUnlessReplaced(session.current(), judged)

	if !strings.Contains(written.String(), "waiting for its answer") {
		t.Fatalf("the socket was taken down under a command that started first: %q", written.String())
	}
	session.mu.Lock()
	owed := session.owed
	session.mu.Unlock()
	if owed == nil {
		t.Fatal("the takedown was dropped instead of going back to waiting, so the mute socket stays up")
	}
}

// A connection this session declared over takes its mark with it, for the same reason. An
// explicit disconnect is marked expected inside whatsmeow and publishes no `Disconnected`,
// so nothing is ever going to claim that mark, and after the operator reconnects it would
// swallow the next genuine drop instead.
func TestAConnectionDeclaredOverRetiresItsMark(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)
	session.announceDrop()

	session.offline()

	dialedAndConnected(session)
	session.handle(&waEvents.Disconnected{})

	if got := session.state(); got != "reconnecting" {
		t.Fatalf("a drop after the operator reconnected was swallowed by the mark of a "+
			"connection declared over, leaving the session %q with no socket under it", got)
	}
}

// But a client this session no longer holds takes its mark with it. The argument for
// keeping one rests on whatsmeow announcing its own reconnect, and a client adopted after a
// logout has no device to reconnect with: a drop swallowed during the pairing that follows
// is swallowed for good, and the session sits with no socket and nothing to say so.
func TestAdoptingAClientRetiresTheMarkOfTheOneItReplaces(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)
	session.announceDrop()

	if !session.adopt(wm.NewClient(session.current().Store, nil)) {
		t.Fatal("the session refused to adopt a client")
	}
	dialedAndConnected(session)
	session.handle(&waEvents.Disconnected{})

	if got := session.state(); got != "reconnecting" && got != "close" {
		t.Fatalf("a drop on the adopted client was swallowed by the mark of the one it "+
			"replaced, leaving the session %q with no socket under it", got)
	}
}

// And a dial of this session's own does not retire the mark either. A drop from the socket
// the dial replaces can still be on its way, and applying it after the new connection
// announces itself is the same wedge by another door.
func TestAFreshDialKeepsTheMark(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)
	session.announceDrop()

	dialedAndConnected(session)
	session.handle(&waEvents.Disconnected{})

	if got := session.state(); got != "open" {
		t.Fatalf("a dial retired the mark, so a drop still on its way from the socket it "+
			"replaced left the session %q with nothing to correct it", got)
	}
}

// And a reset that finds no socket does nothing, quietly.
func TestTheResetDoesNothingWhenThereIsNoSocketToTakeDown(t *testing.T) {
	t.Parallel()

	session, written := newLoggedTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)
	session.announceDrop()

	// The generation is untouched, so nothing this session can see replaced the connection.
	session.resetUnlessReplaced(session.current(), session.transitions.Load())

	if !strings.Contains(written.String(), "already gone") {
		t.Fatalf("the reset claimed to take down a socket that was not there: %q", written.String())
	}
	assertTheMarkStillStands(t, session)
}

// Neither of those retires the mark, and that is the decision the field comment records.
// Whether a drop is still on its way is not knowable from here: the socket may have died
// with its `Disconnected` dispatched and not yet handled, and a mark retired on the guess
// that none is coming lets that one write `reconnecting` over the replacement, with nothing
// after it to say otherwise. Keeping it costs one future drop swallowed, and that one the
// reconnect announces its way out of.
func assertTheMarkStillStands(t *testing.T, session *Session) {
	t.Helper()

	session.setConnected(true)
	session.handle(&waEvents.Disconnected{})
	if got := session.state(); got != "open" {
		t.Fatalf("the mark was retired on a guess that no drop was coming, so a late one left "+
			"the session %q over a socket that is up, with nothing to correct it", got)
	}
}

// The drop that was still on its way when the reset stood down. The socket dies on its own
// with the takedown still owed, whatsmeow reconnects, the command is answered, and only then
// the drop is handled: it describes the socket that is gone, and applying it leaves the
// session refusing commands over the replacement with nothing after it to say otherwise.
func TestADropStillOnItsWayIsSuppressedAfterTheResetStandsDown(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)

	session.startCommand()
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})
	next(t, session)

	// The socket dies, its drop is dispatched and not handled yet, and whatsmeow's own
	// reconnect authenticates the replacement.
	session.handle(&waEvents.Connected{})
	next(t, session)

	// The command is answered, so the takedown finally gets its turn and stands down.
	session.endCommand()
	waitFor(t, func() bool { return session.state() == "open" }, "the session never settled")

	// And only now the drop from the socket that is gone.
	session.handle(&waEvents.Disconnected{})

	if got := session.state(); got != "open" {
		t.Fatalf("a drop from the socket that was replaced left the session %q over a healthy "+
			"one, with nothing after it to correct the state", got)
	}
}

// A socket taken down under a command that is still waiting on WhatsApp is what turns one
// write into two: `ResetConnection` clears whatsmeow's response waiters, `sendIQ` answers
// the disconnect node by resending the identical frame under the same stanza id, and
// WhatsApp does not deduplicate an IQ across connections. Measured on the real service, a
// `group.create` caught by that resend left the account with two groups and the caller was
// told about the second one only.
func TestAMuteSocketIsNotTakenDownUnderACommandStillWaiting(t *testing.T) {
	t.Parallel()

	session, written := newLoggedTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)

	// A command is out at WhatsApp, the way `Execute` counts one.
	session.startCommand()
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})

	if !strings.Contains(written.String(), "comes down with its answer") {
		t.Fatalf("the socket was taken down under a command still waiting: %q", written.String())
	}
	// The half that does not wait: the client is told and the gate closes at once, because
	// neither needs the socket to be down.
	if got := session.state(); got != "reconnecting" {
		t.Fatalf("the session reports %q, so readyToSend still accepts while the socket is mute", got)
	}
	if state := decode(t, next(t, session).Payload)["state"]; state != "reconnecting" {
		t.Fatalf("the session published state=%v", state)
	}
}

// And it comes down as soon as that command is answered.
func TestTheOwedTakeDownRunsWhenTheCommandIsAnswered(t *testing.T) {
	t.Parallel()

	session, written := newLoggedTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)

	session.startCommand()
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})
	next(t, session)

	session.endCommand()

	// The reset owed for that socket runs off a goroutine, and with no socket under it the
	// only thing it leaves behind is the line saying so.
	waitFor(t, func() bool { return strings.Contains(written.String(), "already gone") },
		"the takedown owed to the answered command never ran")
}

// waitFor polls a condition the production code reaches from a goroutine of its own.
func waitFor(t *testing.T, done func() bool, complaint string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if done() {
			return
		}
		runtime.Gosched()
	}
	t.Fatal(complaint)
}

// A socket that answers again while the takedown is still waiting for a command gets to
// keep its connection, and the session says so. Without this the session spends the whole
// wait reporting `reconnecting` and refusing commands over a connection that works, and
// then takes it down anyway when the command is answered.
func TestASocketThatAnswersAgainBeforeTheTakeDownKeepsItsConnection(t *testing.T) {
	t.Parallel()

	session, written := newLoggedTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)

	session.startCommand()
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})
	if state := decode(t, next(t, session).Payload)["state"]; state != "reconnecting" {
		t.Fatalf("the session published state=%v while giving up on the socket", state)
	}

	// The socket answers again while that command is still out at WhatsApp.
	session.handle(&waEvents.KeepAliveRestored{})

	if got := session.state(); got != "open" {
		t.Fatalf("a socket that answered again left the session %q, refusing commands over a "+
			"connection that works", got)
	}
	if state := decode(t, next(t, session).Payload)["state"]; state != "open" {
		t.Fatalf("the session published state=%v for a socket that answered again", state)
	}
	if !strings.Contains(written.String(), "answered again") {
		t.Fatalf("the recovery went unsaid: %q", written.String())
	}

	// And the takedown it was owed is gone, so the command being answered does not take a
	// healthy socket down after all.
	session.endCommand()
	session.mu.Lock()
	owed := session.owed
	session.mu.Unlock()
	if owed != nil {
		t.Fatal("the takedown survived the recovery")
	}
	if got := session.state(); got != "open" {
		t.Fatalf("answering the command took down a socket that had recovered: %q", got)
	}

	// And the mark that went up with the takedown came down with it: no drop is coming from
	// a reset that never ran, so a mark left standing would swallow the next genuine one.
	session.handle(&waEvents.Disconnected{})
	if got := session.state(); got != "reconnecting" {
		t.Fatalf("a genuine drop was swallowed by the mark of a takedown that was cancelled, "+
			"leaving the session %q over a socket on the floor", got)
	}
}

// A recovery dispatched just before the socket dropped, and handled just after. whatsmeow
// sends both from goroutines of their own, so the drop can take the transition lock first;
// the recovery then arrives to a session that is already off that socket, and reviving it
// there leaves the session accepting commands for a connection that does not exist.
func TestARecoveryHandledAfterTheDropDoesNotReviveTheSocket(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)

	session.startCommand()
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})
	next(t, session)

	// The socket drops for real, and this is the event the takedown had marked.
	session.handle(&waEvents.Disconnected{})
	// And only now the recovery that was dispatched before it.
	session.handle(&waEvents.KeepAliveRestored{})

	if got := session.state(); got != "reconnecting" {
		t.Fatalf("a recovery handled after the drop put the session back to %q over a socket "+
			"that is gone, so every command is accepted for a connection that does not exist", got)
	}
}

// And it waits for the last of them, not the first. The session runs its commands one at a
// time, but the count is what says the socket is clear, and releasing it early is the same
// resend under a different command.
func TestTheOwedTakeDownWaitsForTheLastCommandOut(t *testing.T) {
	t.Parallel()

	session, written := newLoggedTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)

	session.startCommand()
	session.startCommand()
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})
	next(t, session)

	session.endCommand()
	session.mu.Lock()
	owed := session.owed
	session.mu.Unlock()
	if owed == nil {
		t.Fatalf("the takedown was released with a command still waiting on WhatsApp: %q", written.String())
	}

	session.endCommand()
	waitFor(t, func() bool { return strings.Contains(written.String(), "already gone") },
		"the takedown owed to the answered commands never ran")
}

// And what counts a command as being in flight is `Execute` itself, for the whole of it.
// Read off the source because every command a test can run here answers from memory, so the
// count is back to zero before anything could look at it.
func TestEveryCommandBoundaryCountsWhatItIsCarryingOut(t *testing.T) {
	t.Parallel()

	// Execute is not the only one: the session layer routes `session.connect`,
	// `session.disconnect` and `session.logout` to these three directly
	// (`internal/session/session.go`, lifecycle), so a pairing code or a logout would have
	// a mutating IQ out at WhatsApp with nothing counting it.
	for _, boundary := range []string{
		"func (s *Session) Execute(",
		"func (s *Session) Connect(",
		"func (s *Session) Disconnect(",
		"func (s *Session) Logout(",
	} {
		carrying := theBodyOf(t, boundary)
		started := strings.Index(carrying, "s.startCommand()")
		ended := strings.Index(carrying, "defer s.endCommand()")
		if started < 0 || ended < 0 {
			t.Fatalf("%s does not count the command in flight, so the keepalive handler takes "+
				"the socket down under it and WhatsApp applies the resent frame twice:\n%s",
				boundary, carrying)
		}
		if started > ended {
			t.Fatalf("%s releases the count before it takes it:\n%s", boundary, carrying)
		}
	}
}

// And a connection is dated from when whatsmeow dispatched the event that announced it,
// not from when this session got round to handling it. Every arm that moves the connection
// takes the transition lock first, and one already held across a publish waiting on a full
// inbox delays the next by as long as that takes: a connection dated from then reads as
// later than the socket it describes, and genuine timeouts on that socket read as stale.
// Read off the source because the delay it is about is another goroutine's.
func TestTheConnectionIsDatedFromTheDispatchAndNotTheHandling(t *testing.T) {
	t.Parallel()

	handling := theBodyOf(t, "func (s *Session) handle(")
	dated := strings.Index(handling, "dispatched := s.now()")
	waits := strings.Index(handling, "s.transition.Lock()")
	if dated < 0 || waits < 0 {
		t.Fatalf("the event handler neither dates the event nor serialises:\n%s", handling[:400])
	}
	if dated > waits {
		t.Fatalf("the event is dated after the first thing in the handler that can wait")
	}
	drop := theCaseFor(t, "*waEvents.Disconnected")
	if !strings.Contains(drop, "dispatched)") {
		t.Fatalf("the drop dates the reconnect from its own handling:\n%s", drop)
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
	refused := strings.Index(handler, "setReconnecting(true")
	closed := strings.Index(handler, "takeDownSoon(")
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
	if !strings.Contains(theBodyOf(t, "func (s *Session) takeDownSoon("), "go s.resetUnlessReplaced(") {
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

// theBodyOf returns a function of the session, from its signature to its closing brace.
func theBodyOf(t *testing.T, signature string) string {
	t.Helper()

	lines := theSessionSource(t)
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

	lines := theSessionSource(t)
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

func theSessionSource(t *testing.T) []string {
	t.Helper()

	raw, err := os.ReadFile("session.go")
	if err != nil {
		t.Fatalf("read the session: %v", err)
	}
	return strings.Split(string(raw), "\n")
}

// newLoggedTestSession is a session whose log can be read while it is being written. The
// keepalive handler takes the socket down from a goroutine of its own, and that goroutine
// logs, so a plain bytes.Buffer here is a data race and not a test.
func newLoggedTestSession(t *testing.T, phone string) (*Session, *syncBuffer) {
	t.Helper()

	session, _ := newTestSession(t, phone)
	written := &syncBuffer{}
	session.log = zerolog.New(written)
	return session, written
}

type syncBuffer struct {
	mu      sync.Mutex
	written bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.written.Write(p) //nolint:wrapcheck // a test writer, and the caller is zerolog
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.written.String()
}

func (b *syncBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.written.Len()
}

// dialedAndConnected puts a session on a socket the way a real one gets there: the attempt
// first, which is what whatsmeow's keepalive clock starts with, and the authentication
// after it.
func dialedAndConnected(session *Session) {
	session.setDialing(true)
	session.setConnected(true)
}
