package whatsmeow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
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

// And the socket that replaced it is dated from when the session heard about it, not from
// when it got the lock to write it down. Whichever arm holds the transition lock may be
// waiting on a publish into an inbox nobody is draining, and a replacement stamped from
// the far side of that wait is one whose own timeouts all read as stale: the session would
// leave a mute socket up for whatsmeow's three minutes, which is where main already is.
func TestASwappedSocketIsDatedFromWhenTheSessionHeardAboutIt(t *testing.T) {
	t.Parallel()

	session, written := newLoggedTestSession(t, "5511999990001")
	dialedAndConnected(session)

	// The clock the handler reads: the event's arrival for the instant it takes on entry,
	// and a lock that came free a good deal later for every read after that. Scripted
	// rather than slept, because what stands between the two in production is a publish
	// that blocks for as long as the inbox stays full.
	heard := time.Now()
	late := heard.Add(keepAliveStaleAfter + time.Second)
	reads := 0
	session.wallClock = func() time.Time {
		reads++
		if reads == 1 {
			return heard
		}
		return late
	}

	// whatsmeow's 515 path again: the socket is replaced and this event is the whole of
	// what the session sees.
	session.handle(&waEvents.Connected{})
	if emission := next(t, session); emission.Type != protocol.EventSessionState {
		t.Fatalf("the replacement published %s", emission.Type)
	}

	// A run of pings the replacement itself stopped answering, dated from the socket it
	// came up on.
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: heard})

	if got := session.state(); got != "reconnecting" {
		t.Fatalf("the replacement was dated %s, when the lock came free, rather than %s, when the "+
			"session heard about it, so its own timeouts read as stale and the session stayed %q over "+
			"a mute socket", late.Format(time.TimeOnly), heard.Format(time.TimeOnly), got)
	}
	if written.Len() == 0 {
		t.Fatalf("the mute replacement was not taken down")
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
	mustStartCommand(t, session)

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

	mustStartCommand(t, session)
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
	mustStartCommand(t, session)
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

	mustStartCommand(t, session)
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})
	next(t, session)

	session.endCommand()

	// The reset owed for that socket runs off a goroutine, and with no socket under it the
	// only thing it leaves behind is the line saying so.
	waitFor(t, func() bool { return strings.Contains(written.String(), "already gone") },
		"the takedown owed to the answered command never ran")
}

// mustStartCommand counts a command in flight for a test that is not about the wait, and
// fails loudly if the wait is what it got: a test that silently ran without its command
// counted would be asserting about a different session than it thinks.
func mustStartCommand(t *testing.T, session *Session) {
	t.Helper()

	if err := session.startCommand(context.Background()); err != nil {
		t.Fatalf("the command could not be counted in flight: %v", err)
	}
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

	mustStartCommand(t, session)
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

	mustStartCommand(t, session)
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

// And a recovery about a socket that has already been replaced does not take the drop mark
// down with it. Three events from three goroutines, in an order whatsmeow allows and this
// session cannot prevent: the replacement announces itself first, the recovery of the socket
// it replaced lands after that, and the drop of that same socket lands last. The mark exists
// for exactly that drop. A recovery that clears it leaves nothing to swallow it, and
// `reconnecting` is written over a healthy socket with nothing after it to put that right --
// the replacement is fine, so it produces no further event.
func TestARecoveryAboutAReplacedSocketLeavesTheDropMarkStanding(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)

	// A mute socket judged lost with a command still out at WhatsApp, so the takedown is
	// owed rather than run, and the mark stands for a drop that has not happened yet.
	mustStartCommand(t, session)
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})
	if state := decode(t, next(t, session).Payload)["state"]; state != "reconnecting" {
		t.Fatalf("the session published state=%v while giving up on the socket", state)
	}

	// whatsmeow puts the session on another socket, and this session hears the replacement
	// before it hears anything about the one that went.
	session.handle(&waEvents.Connected{})
	if state := decode(t, next(t, session).Payload)["state"]; state != "open" {
		t.Fatalf("the replacement published state=%v", state)
	}

	// The recovery of the socket that is gone, dispatched before the replacement was and
	// handled after it, then the drop of that same socket, last of the three.
	session.handle(&waEvents.KeepAliveRestored{})
	session.handle(&waEvents.Disconnected{})

	if got := session.state(); got != "open" {
		t.Fatalf("the drop of the replaced socket was applied over the healthy replacement, "+
			"leaving the session %q and refusing every command for a connection that works", got)
	}

	// And nothing is owed to a socket that is gone, so answering the command does not take
	// the replacement down either.
	session.endCommand()
	if got := session.state(); got != "open" {
		t.Fatalf("answering the command took down the replacement: %q", got)
	}
}

// And the debt of a mute socket is dropped without waiting for the transition lock. The
// command a takedown waits on can be answered at any moment, and the reset that fires then
// judges by a connection count the drop handler has not been able to move yet: behind a
// publish waiting on a full inbox that is minutes, and minutes is enough for whatsmeow to
// have redialled, so the reset finds a socket under the client and the socket it finds is
// the replacement.
func TestTheDebtOfAMuteSocketIsDroppedWithoutWaitingForTheLock(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)

	mustStartCommand(t, session)
	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})
	next(t, session)
	session.mu.Lock()
	owed := session.owed
	session.mu.Unlock()
	if owed == nil {
		t.Fatal("no takedown was owed, so this test is about nothing")
	}

	// Another arm holding the lock across a publish nobody is draining.
	session.transition.Lock()
	defer session.transition.Unlock()

	go session.handle(&waEvents.Disconnected{})

	waitFor(t, func() bool {
		session.mu.Lock()
		defer session.mu.Unlock()
		return session.owed == nil
	}, "the drop handler waited for the transition lock before dropping the debt, so a command "+
		"answered while it waits takes down whatever socket the client has by then")
}

// And the connection is checked once more after a socket is found, because finding one is
// what takes the time: that read waits on whatsmeow's socket lock, a redial holds it for the
// length of an attempt, and a `true` returned at the end of one describes the socket that
// replaced the mute one. Read off the source, because reaching it needs a real socket and a
// real redial.
func TestTheConnectionIsCheckedAgainAfterASocketIsFound(t *testing.T) {
	t.Parallel()

	taking := theBodyOf(t, "func (s *Session) resetUnlessReplaced(")
	found := strings.Index(taking, "client.IsConnected()")
	recheck := strings.LastIndex(taking, "s.transitions.Load() != judged")
	reset := strings.Index(taking, "client.ResetConnection()")
	if found < 0 || recheck < 0 || reset < 0 {
		t.Fatalf("the takedown no longer reads as a check, a re-check and a reset:\n%s", taking)
	}
	if found > recheck || recheck > reset {
		t.Fatalf("the takedown does not re-read the connection count between finding a socket and "+
			"resetting it, so a socket found at the end of a redial is taken down even though it is "+
			"the one that replaced the mute socket:\n%s", taking)
	}
}

// And a drop swallowed by the mark still tells the takedown that its socket is gone. The
// takedown runs on its own goroutine and judges by the connection count; the question it
// asks whatsmeow -- is there still a socket -- costs a wait on a lock a redial holds for a
// whole attempt, so the answer that comes back at the end of one describes the socket that
// replaced the mute one. The replacement itself is invisible until whatsmeow finishes its
// prekey and passive IQs (#181), but the drop of the judged socket is not, and it is enough.
func TestADropSwallowedByTheMarkStillEndsTheConnectionItWasJudgedOn(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)

	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})
	if state := decode(t, next(t, session).Payload)["state"]; state != "reconnecting" {
		t.Fatalf("the session published state=%v while giving up on the socket", state)
	}
	judged := session.transitions.Load()

	// The drop the takedown caused, or the one that beat it there: either way the mark
	// swallows it, and the arm returns without touching the state the handler already wrote.
	session.handle(&waEvents.Disconnected{})

	if session.transitions.Load() == judged {
		t.Fatal("the drop was swallowed without recording that the judged connection is over, so " +
			"a takedown waking from its question at the end of a redial still reads its socket as " +
			"current and takes down the one that replaced it")
	}
}

// And it records that without waiting for the transition lock either, which is the whole
// point of recording it: the takedown is already awake on its own goroutine, and the arm
// holding the lock may be the one blocked publishing into an inbox nobody drains.
func TestAJudgedConnectionEndsWithoutWaitingForTheLock(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)

	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})
	next(t, session)
	judged := session.transitions.Load()

	session.transition.Lock()
	defer session.transition.Unlock()

	go session.handle(&waEvents.Disconnected{})

	waitFor(t, func() bool { return session.transitions.Load() != judged },
		"the drop handler waited for the transition lock before recording that the judged "+
			"connection is over, so a takedown waking from its question at the end of a redial "+
			"reads its socket as current and takes down the replacement")
}

// And the generation a takedown is judged by is the one its own transition wrote, not one
// read back afterwards. `dropped` is deliberately outside the transition lock, so a drop can
// land between the write and a read: counted into the snapshot instead of invalidating it,
// it leaves the takedown agreeing with a socket that is already gone.
func TestTheGenerationASocketIsGivenUpOnIsInvalidatedByALaterDrop(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	dialedAndConnected(session)

	judged := session.setConnected(false)
	if judged != session.transitions.Load() {
		t.Fatalf("giving up on the socket returned generation %d for a connection the session "+
			"counts as %d, so every takedown reads as superseded and none of them runs",
			judged, session.transitions.Load())
	}
	session.dropped()
	if session.transitions.Load() == judged {
		t.Fatal("a drop after the socket was given up on left the generation it was judged by " +
			"unchanged, so the takedown compares equal and resets whatever socket is under the " +
			"client by then")
	}
}

// Read off the source, because the window the snapshot has to survive is another goroutine's.
func TestTheTakedownIsJudgedByTheGenerationItsOwnTransitionWrote(t *testing.T) {
	t.Parallel()

	handler := theCaseFor(t, "*waEvents.KeepAliveTimeout")
	if !strings.Contains(handler, "judged := s.setConnected(false)") {
		t.Fatalf("the takedown reads the generation back after writing it instead of taking the "+
			"one its own transition returned, so a drop landing in between is counted into the "+
			"snapshot rather than invalidating it:\n%s", handler)
	}
}

// And no command begins while a takedown is closing the socket. The takedown only claims
// when nothing is running, and what would arrive in that window is a lifecycle command --
// the one kind that does not pass `readyToSend` -- putting a removal IQ on a socket that is
// going down underneath it, which whatsmeow resends once the connection is back and
// WhatsApp applies twice.
func TestNoCommandBeginsWhileATakedownIsClosingTheSocket(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	closing := make(chan struct{})
	session.mu.Lock()
	session.resetting = closing
	session.mu.Unlock()

	reached, started := make(chan struct{}), make(chan error, 1)
	go func() {
		close(reached)
		started <- session.startCommand(context.Background())
	}()

	// The goroutine is running before anything is concluded from its silence, so what the
	// window below measures is a command held back and not a goroutine never scheduled.
	<-reached
	for range 1000 {
		runtime.Gosched()
	}
	select {
	case <-started:
		t.Fatal("a command began while a takedown was closing the socket, so its removal IQ can " +
			"be cut off mid-flight and resent once the connection is back")
	default:
	}

	session.mu.Lock()
	session.resetting = nil
	session.mu.Unlock()
	close(closing)

	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("the command was refused after the takedown let the socket go: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the command never began after the takedown let the socket go")
	}
	session.endCommand()
}

// And a command whose caller has stopped waiting does not begin at all. The wait here is
// for a close handshake, which is seconds; a command let through after its deadline is one
// the session layer has already answered, and for a lifecycle command that is a socket
// effect launched for nobody.
func TestACommandWhoseDeadlineExpiredDoesNotBeginAfterTheWait(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	closing := make(chan struct{})
	defer close(closing)
	session.mu.Lock()
	session.resetting = closing
	session.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	refused := make(chan error, 1)
	reached := make(chan struct{})
	go func() {
		close(reached)
		refused <- session.startCommand(ctx)
	}()
	<-reached
	cancel()

	select {
	case err := <-refused:
		if err == nil {
			t.Fatal("a command whose caller stopped waiting began anyway, after the only expiry " +
				"check the session layer makes")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the refusal does not carry the caller's own reason, so it cannot be told "+
				"from a connector failure: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the wait did not honour the caller's context at all")
	}

	session.mu.Lock()
	running := session.running
	session.mu.Unlock()
	if running != 0 {
		t.Fatalf("a command refused at the wait was still counted in flight (%d), so the takedown "+
			"waits for an answer nobody is coming back with", running)
	}
}

// And the takedown claims that only after asking whatsmeow whether the socket is still
// there, because asking is what takes the time: both readings taken before it are stale by
// the time it answers. Read off the source, because reaching the claim needs a real socket.
func TestTheTakedownJudgesAgainAfterAskingWhetherTheSocketIsThere(t *testing.T) {
	t.Parallel()

	taking := theBodyOf(t, "func (s *Session) resetUnlessReplaced(")
	asked := strings.Index(taking, "client.IsConnected()")
	running := strings.LastIndex(taking, "s.running > 0")
	claim := strings.Index(taking, "s.resetting = closing")
	reset := strings.Index(taking, "client.ResetConnection()")
	if asked < 0 || running < 0 || claim < 0 || reset < 0 {
		t.Fatalf("the takedown no longer reads as ask, judge again, claim, reset:\n%s", taking)
	}
	if asked > running || running > claim || claim > reset {
		t.Fatalf("the takedown does not judge the command count again and claim the socket "+
			"between asking whether it is there and closing it, so a lifecycle command starting "+
			"in that window has its removal IQ cut off and resent:\n%s", taking)
	}
}

// And a command with no deadline of its own is let go when the session is. Nothing above
// bounds that wait: the command carries no expiry, and what it is waiting for is a close
// handshake that whatever went wrong may never finish. Without this the goroutine holds a
// session that is already gone.
func TestACommandWithNoDeadlineIsLetGoWhenTheSessionCloses(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	closing := make(chan struct{})
	defer close(closing)
	session.mu.Lock()
	session.resetting = closing
	session.mu.Unlock()

	refused := make(chan error, 1)
	reached := make(chan struct{})
	go func() {
		close(reached)
		refused <- session.startCommand(context.Background())
	}()
	<-reached
	session.cancel()

	select {
	case err := <-refused:
		if err == nil {
			t.Fatal("a command began on a session that is closing, under a takedown nobody is " +
				"left to finish")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a command with no deadline of its own waited on a session that had already closed")
	}
}

// And the two boundaries that must not wait do not. A disconnect refused at the wait never
// records that the account was asked to stay down, and the takedown it queued behind puts
// the socket back up -- so the operator's disconnect is undone by the guard meant to protect
// it. Neither of them can have a mutating IQ cut off mid-flight, which is all the wait is
// for: one sends nothing WhatsApp applies, the other dials a socket of its own.
func TestTheBoundariesThatMustNotWaitOnATakedownDoNot(t *testing.T) {
	t.Parallel()

	for _, boundary := range []string{"func (s *Session) Connect(", "func (s *Session) Disconnect("} {
		carrying := theBodyOf(t, boundary)
		if strings.Contains(carrying, "s.startCommand(") {
			t.Fatalf("%s waits on a takedown, so a caller that gives up leaves the account "+
				"recorded as one that should be connected and the reset brings it back:\n%s",
				boundary, carrying)
		}
		if !strings.Contains(carrying, "s.countCommand()") {
			t.Fatalf("%s does not count the command in flight at all:\n%s", boundary, carrying)
		}
	}
}

// And the status read is the only command answered before the counting. It is the one that
// puts nothing on the socket, so there is nothing for a takedown to cut off -- and it has to
// stay out of the wait for a second reason: the session layer answers every `session.connect`
// with one, and that connect deliberately does not wait, so a status that waited would put
// the wait straight back where it was taken out.
//
// Read off the source, and by counting rather than by naming: an exemption added later is
// caught here whatever it is called.
func TestOnlyTheStatusReadIsAnsweredBeforeTheCounting(t *testing.T) {
	t.Parallel()

	carrying := theBodyOf(t, "func (s *Session) Execute(")
	counted := strings.Index(carrying, "s.startCommand(ctx)")
	if counted < 0 {
		t.Fatalf("Execute no longer counts the command in flight:\n%s", carrying)
	}
	exempt := map[string]bool{}
	for _, name := range regexp.MustCompile(`protocol\.Command[A-Za-z]+`).FindAllString(carrying[:counted], -1) {
		exempt[name] = true
	}
	if len(exempt) != 1 || !exempt["protocol.CommandSessionStatus"] {
		t.Fatalf("the commands answered before the counting are %v, and the only one that may be "+
			"is the status read: anything else can put a frame on a socket a takedown is about "+
			"to close, and whatsmeow resends what it was cut off from:\n%s", exempt, carrying[:counted])
	}
}

// The unlink a `session.delete` sends is the sharpest case of all: it is a lifecycle
// command, so it never passes through `Execute`, and cutting it off would have whatsmeow
// resend the removal to WhatsApp. Driven rather than fenced, because this one cannot be
// exercised against the real service without unpairing the account it would run on.
func TestAMuteSocketIsNotTakenDownUnderATeardownStillWaiting(t *testing.T) {
	t.Parallel()

	session, written := newLoggedTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)

	// The unlink is out at WhatsApp and has not been answered.
	unlinking := make(chan struct{})
	release := make(chan struct{})
	session.logout = func(context.Context, *wm.Client) error {
		close(unlinking)
		<-release
		return nil
	}
	deleted := make(chan error, 1)
	go func() { deleted <- session.Delete(t.Context()) }()
	<-unlinking

	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})

	if !strings.Contains(written.String(), "comes down with its answer") {
		t.Fatalf("the socket was taken down under the unlink, so whatsmeow resends the removal: %q",
			written.String())
	}
	close(release)
	if err := <-deleted; err != nil {
		t.Fatalf("Delete: %v", err)
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

	mustStartCommand(t, session)
	mustStartCommand(t, session)
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
//
// Execute is not the only one: the session layer routes the lifecycle commands straight to
// the engine's own methods, so a pairing code, a logout or a teardown would otherwise have
// a mutating IQ out at WhatsApp with nothing counting it. Which methods those are is read
// out of that file rather than written down here, and the difference is not tidiness: a
// hand-kept list is exactly what let `Delete` arrive uncounted when #157 landed under this
// branch. A sixth routed there is caught by this without anybody remembering to come back.
func TestEveryCommandBoundaryCountsWhatItIsCarryingOut(t *testing.T) {
	t.Parallel()

	for _, method := range theEngineMethodsTheSessionLayerRoutesTo(t) {
		boundary := "func (s *Session) " + method + "("
		carrying := theBodyOf(t, boundary)
		started := strings.Index(carrying, "s.startCommand(ctx)")
		if started < 0 {
			// The two that must not wait on a takedown count through their own door.
			started = strings.Index(carrying, "s.countCommand()")
		}
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

// theEngineMethodsTheSessionLayerRoutesTo reads `lifecycle` in `internal/session` and
// returns every engine method a command can reach through it.
//
// Every arm of that switch is required to reach one, and helpers are followed as far as
// they go rather than a fixed number of hops: `session.delete` reaches the engine through
// `tearDown` instead of calling it in the arm, so a fence that read only the arms would
// have missed the exact boundary that went missing. An arm that reaches nothing is the
// failure this is for -- whatever it routes to is then not being checked at all.
func theEngineMethodsTheSessionLayerRoutesTo(t *testing.T) []string {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("..", "..", "session", "session.go"))
	if err != nil {
		t.Fatalf("read the session layer: %v", err)
	}
	source := strings.Split(string(raw), "\n")

	onEngine := regexp.MustCompile(`s\.engine\.([A-Z]\w*)\(`)
	ownHelper := regexp.MustCompile(`\bs\.([a-z]\w*)\(ctx`)

	var reach func(body string, followed map[string]bool) []string
	reach = func(body string, followed map[string]bool) []string {
		found := []string{}
		for _, call := range onEngine.FindAllStringSubmatch(body, -1) {
			found = append(found, call[1])
		}
		for _, call := range ownHelper.FindAllStringSubmatch(body, -1) {
			if followed[call[1]] {
				continue
			}
			followed[call[1]] = true
			helper := theBodyOfIn(t, source, "func (s *Session) "+call[1]+"(", "the session layer")
			found = append(found, reach(helper, followed)...)
		}
		return found
	}

	counted := map[string]bool{}
	for _, arm := range theArmsOfTheLifecycleSwitch(t, source) {
		reached := reach(arm.body, map[string]bool{"lifecycle": true})
		if len(reached) == 0 {
			t.Fatalf("the %s arm of the session layer's lifecycle switch reaches no engine method "+
				"by any route this fence can follow, so whatever carries that command out is not "+
				"being held to counting it:\n%s", arm.name, arm.body)
		}
		for _, method := range reached {
			counted[method] = true
		}
	}

	methods := []string{}
	for method := range counted {
		methods = append(methods, method)
	}
	sort.Strings(methods)
	return methods
}

// lifecycleArm is one arm of that switch: the command it answers, and what it does.
type lifecycleArm struct{ name, body string }

// theArmsOfTheLifecycleSwitch splits the routing into its arms, so each can be held to
// reaching the engine on its own. A lifecycle this cannot read as a switch fails here
// rather than quietly reading as no arms at all.
func theArmsOfTheLifecycleSwitch(t *testing.T, source []string) []lifecycleArm {
	t.Helper()

	routing := strings.Split(
		theBodyOfIn(t, source, "func (s *Session) lifecycle(", "the session layer"), "\n",
	)
	opens := func(line string) bool {
		return strings.HasPrefix(line, "\tcase ") || line == "\tdefault:"
	}

	arms := []lifecycleArm{}
	for i, line := range routing {
		if !opens(line) {
			continue
		}
		end := len(routing)
		for j := i + 1; j < len(routing); j++ {
			if opens(routing[j]) {
				end = j
				break
			}
		}
		arms = append(arms, lifecycleArm{
			name: strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(line), "case "), ":"),
			body: strings.Join(routing[i:end], "\n"),
		})
	}
	if len(arms) == 0 {
		t.Fatalf("the session layer's lifecycle is not a switch this fence can read:\n%s",
			strings.Join(routing, "\n"))
	}
	return arms
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

	return theBodyOfIn(t, theSessionSource(t), signature, "the session")
}

// theBodyOfIn is theBodyOf over source read from somewhere else, which the boundary fence
// needs: what it has to read is the session layer, a package away.
func theBodyOfIn(t *testing.T, lines []string, signature, what string) string {
	t.Helper()

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
	t.Fatalf("%s has no %s", what, signature)
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

// The instant a drop is dated from has to be the one the event was dispatched at, and the
// arm that publishes `reconnecting` is the one where that is hardest to see: it takes the
// transition lock, and whatever held that lock before it may have been waiting on a publish
// into a full inbox. A stamp read after that wait is later than the socket it describes,
// and once it is more than `keepAliveStaleAfter` later every real timeout on the socket
// that follows reads as stale.
func TestTheKeepAliveDropIsDatedFromTheDispatchAndNotTheHandling(t *testing.T) {
	t.Parallel()

	session, _ := newLoggedTestSession(t, "5511999990001")
	dialedAndConnected(session)

	// Driven rather than measured: the gap this is about is one the real thing can take
	// minutes over while two `time.Now()` calls in a row cannot produce it at all. The
	// first reading is the dispatch, and everything after it is the handling.
	dispatched := time.Now()
	var readings int
	session.wallClock = func() time.Time {
		readings++
		if readings == 1 {
			return dispatched
		}
		return dispatched.Add(time.Minute)
	}

	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: dispatched})

	session.mu.Lock()
	stamped := session.connectedAt
	session.mu.Unlock()
	if !stamped.Equal(dispatched) {
		t.Fatalf("the reconnect is dated %s, when the handler got round to it, rather than %s, "+
			"when the event was dispatched, so a timeout on the socket that follows reads as stale",
			stamped.Format(time.TimeOnly), dispatched.Format(time.TimeOnly))
	}
}

// Same question on the other side of the same decision. A recovery is compared against the
// connection stamp to decide which of the two is later, so one dated from its handling can
// win that comparison over a socket that replaced it -- and then the timeouts of a dead
// keepalive loop read as current and the healthy replacement is taken down for them.
func TestARecoveryIsDatedFromTheDispatchAndNotTheHandling(t *testing.T) {
	t.Parallel()

	session, _ := newLoggedTestSession(t, "5511999990001")
	dialedAndConnected(session)

	dispatched := time.Now()
	var readings int
	session.wallClock = func() time.Time {
		readings++
		if readings == 1 {
			return dispatched
		}
		return dispatched.Add(time.Minute)
	}

	session.handle(&waEvents.KeepAliveRestored{})

	session.mu.Lock()
	answered := session.keepAliveAnsweredAt
	session.mu.Unlock()
	if !answered.Equal(dispatched) {
		t.Fatalf("the ping is recorded as answered at %s, when the handler got round to it, "+
			"rather than %s, when whatsmeow dispatched it, so a recovery about a socket that "+
			"has since been replaced outlives the replacement's own stamp",
			answered.Format(time.TimeOnly), dispatched.Format(time.TimeOnly))
	}
}

// A session that is not on a socket has no evidence of life, and the stamp of the socket it
// used to be on is not evidence about the one it is not on. Left standing, it answers the
// staleness question for a connection that does not exist.
func TestASessionWithNoSocketHasNoEvidenceOfLife(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	dialedAndConnected(session)
	session.setConnected(false)

	if alive := session.lastKnownAlive(); !alive.IsZero() {
		t.Fatalf("a session with no socket says it was alive at %s, the socket it no longer "+
			"has, so a timeout about that dead socket reads as one about the present",
			alive.Format(time.TimeOnly))
	}
}

// The debt carries the connection it was judged on, not the count at the moment it is
// written down. Those differ by exactly the thing the judgement is for: a drop landing in
// between moves the count, and a debt that took the new one would be paid over whatever
// socket the client has by the time the command answers.
func TestTheDebtKeepsTheGenerationItWasJudgedOn(t *testing.T) {
	t.Parallel()

	session, _ := newLoggedTestSession(t, "5511999990001")
	dialedAndConnected(session)
	judged := session.setConnected(false)
	// The drop of the socket this takedown is about, landing before the debt is recorded.
	session.dropped()
	session.countCommand()
	defer session.endCommand()

	session.takeDownSoon(session.current(), judged)

	session.mu.Lock()
	owed := session.owed
	session.mu.Unlock()
	if owed == nil {
		t.Fatal("the takedown did not wait for the command in flight")
	}
	if owed.judged != judged {
		t.Fatalf("the debt was written down against connection %d, the one current when it was "+
			"recorded, rather than %d, the one it was judged on, so it is paid over a socket "+
			"nobody judged", owed.judged, judged)
	}
}

// `Connect` and `Disconnect` do not wait on a takedown, and the reason they still count is
// this one: a takedown must not fire in the middle of one. Counting is the whole of what
// they do here, so a count that does not happen is invisible everywhere else.
func TestAConnectInFlightHoldsOffTheTakedown(t *testing.T) {
	t.Parallel()

	session, written := newLoggedTestSession(t, "5511999990001")
	dialedAndConnected(session)
	session.countCommand()
	defer session.endCommand()

	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})

	if !strings.Contains(written.String(), "comes down with its answer") {
		t.Fatalf("the socket was taken down under a lifecycle command that counts but does not "+
			"wait, so whatsmeow resends whatever it was carrying: %q", written.String())
	}
}

// The mark answers for one drop, and the drop it answers for is the next one handled. Left
// standing it swallows the one after that as well, and then the session reports `open` over
// a socket on the floor until whatsmeow announces a reconnect of its own.
func TestTheMarkIsConsumedByTheDropItSuppresses(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.relearn(session.current())
	dialedAndConnected(session)
	session.announceDrop()

	// The drop the mark was raised for.
	session.handle(&waEvents.Disconnected{})
	// whatsmeow's own reconnect, and then a second drop that nobody announced.
	dialedAndConnected(session)
	session.handle(&waEvents.Disconnected{})

	if got := session.state(); got == "open" {
		t.Fatal("the second drop was swallowed by a mark raised for the first, so the session " +
			"reports open with no socket under it")
	}
}

// Going offline is the session saying the connection is over and nothing is coming back on
// its own. A debt left behind it is a takedown owed to a socket that no longer exists, and
// the command it waits for can answer at any time.
func TestGoingOfflineDropsTheDebtWithTheConnection(t *testing.T) {
	t.Parallel()

	session, _ := newLoggedTestSession(t, "5511999990001")
	dialedAndConnected(session)
	judged := session.setConnected(false)
	session.countCommand()
	session.takeDownSoon(session.current(), judged)

	session.offline()

	session.mu.Lock()
	owed := session.owed
	session.mu.Unlock()
	session.endCommand()
	if owed != nil {
		t.Fatal("a takedown is still owed to a socket the session has declared gone, and the " +
			"command in flight will pay it against whatever the client holds by then")
	}
}

// The socket coming back is a transition like any other, and it has to count as one: what
// reads the count is the takedown that was judged before it, and the whole of standing that
// takedown down is the count having moved.
func TestTheSocketComingBackCountsAsATransition(t *testing.T) {
	t.Parallel()

	session, written := newLoggedTestSession(t, "5511999990001")
	dialedAndConnected(session)
	judged := session.setConnected(false)

	session.recovered()
	session.resetUnlessReplaced(session.current(), judged)

	if !strings.Contains(written.String(), "leaving it alone") {
		t.Fatalf("the socket answered again and the takedown judged before it went ahead "+
			"anyway, closing a connection that works: %q", written.String())
	}
}

// The teardowns are the two commands `readyToSend` does not refuse, so nothing else stops
// one from starting on a socket that is being closed underneath it -- and a removal IQ cut
// off mid-flight is resent by whatsmeow and applied twice by WhatsApp. They wait, and a
// caller whose deadline runs out inside that wait is answered rather than let through.
func TestATeardownDoesNotBeginWhileATakedownIsClosingTheSocket(t *testing.T) {
	t.Parallel()

	for _, teardown := range []struct {
		name string
		call func(*Session, context.Context) error
	}{
		{name: "Logout", call: func(s *Session, ctx context.Context) error { return s.Logout(ctx) }},
		{name: "Delete", call: func(s *Session, ctx context.Context) error { return s.Delete(ctx) }},
	} {
		t.Run(teardown.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newLoggedTestSession(t, "5511999990001")
			dialedAndConnected(session)
			session.logout = func(context.Context, *wm.Client) error {
				t.Error("the teardown reached WhatsApp over a socket that is being closed")
				return nil
			}

			// The takedown has claimed the socket and is in the close handshake.
			closing := make(chan struct{})
			session.mu.Lock()
			session.resetting = closing
			session.mu.Unlock()
			defer func() {
				session.mu.Lock()
				session.resetting = nil
				session.mu.Unlock()
				close(closing)
			}()

			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			err := teardown.call(session, ctx)

			if err == nil || !strings.Contains(err.Error(), "waiting for the socket to be taken down") {
				t.Fatalf("%s began while the socket was being closed under it, so its removal IQ "+
					"is resent on the connection that follows and applied twice: %v", teardown.name, err)
			}
		})
	}
}

// Three windows nothing deterministic can stand inside, so the fence reads the source. Each
// is a place where the correct code and the wrong code differ only in what another goroutine
// can do between two statements, which is exactly what a test cannot hold still.
func TestTheWindowsNoTestCanStandInsideAreFencedOff(t *testing.T) {
	t.Parallel()

	// The judgement is the value the transition wrote, not one read back after it: `dropped`
	// is deliberately outside the transition lock, so a drop landing between the write and a
	// read-back is counted into the snapshot instead of invalidating it.
	dating := theBodyOf(t, "func (s *Session) setConnectedAt(")
	if !strings.Contains(dating, "generation := s.transitions.Add(1)") {
		t.Fatalf("the transition does not take its own generation from the write:\n%s", dating)
	}
	if strings.Contains(dating, "s.transitions.Load()") {
		t.Fatalf("the transition reads the count back after writing it:\n%s", dating)
	}

	// A command woken by one takedown must go back and look again, because a second takedown
	// can have claimed the socket while it was waking.
	waiting := theBodyOf(t, "func (s *Session) startCommand(")
	if !strings.Contains(waiting, "case <-closing:\n\t\tcase <-ctx.Done():") {
		t.Fatalf("the wait for a takedown does something other than look again when it wakes, "+
			"so a command can start under the takedown that claimed the socket next:\n%s", waiting)
	}

	// And the claim is released before the waiters are woken, so one that wakes finds the
	// claim already gone rather than queueing behind a takedown that is finished.
	taking := theBodyOf(t, "func (s *Session) resetUnlessReplaced(")
	released := strings.Index(taking, "s.resetting = nil")
	woken := strings.Index(taking, "close(closing)")
	if released < 0 || woken < 0 {
		t.Fatalf("the takedown neither claims the socket nor releases the claim:\n%s", taking)
	}
	if released > woken {
		t.Fatalf("the takedown wakes the commands waiting on it before it releases the claim, "+
			"so one that wakes sees a claim that is already over:\n%s", taking)
	}

	// The debt is paid against the socket it was judged on. `s.current()` at that point is
	// whatever the client holds after however long the command took.
	paying := theBodyOf(t, "func (s *Session) endCommand(")
	if !strings.Contains(paying, "owed.client") {
		t.Fatalf("the debt is paid against a socket other than the one it names:\n%s", paying)
	}
}
