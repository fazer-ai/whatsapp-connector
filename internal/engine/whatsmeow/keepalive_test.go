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

// The two ways to take a socket down differ in one thing and it is the thing this change
// is about: `Disconnect` marks the disconnect as expected, and `onDisconnect` publishes
// `events.Disconnected` only when it was not -- so a session that used it would take its
// socket down and go on reporting `open`, which is the state this exists to leave. Read
// off the source because nothing else here can see the difference: a client with no socket
// does the same nothing under either call, and the phase that can tell them apart is the
// live one.
func TestTheKeepAliveHandlerResetsTheConnectionRatherThanDisconnecting(t *testing.T) {
	t.Parallel()

	handler := theCaseFor(t, "*waEvents.KeepAliveTimeout")
	if !strings.Contains(handler, "ResetConnection()") {
		t.Fatalf("the keepalive handler does not reset the connection:\n%s", handler)
	}
	if strings.Contains(handler, "Disconnect()") {
		t.Fatalf("the keepalive handler disconnects, which publishes nothing and leaves the session reporting open:\n%s", handler)
	}
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

// The order inside the arm is load-bearing and invisible to every test that can run here:
// with no socket under it, ResetConnection returns at once, so a session that published
// after it looks the same from outside as one that published before. What it costs live is
// the seconds the close handshake holds the socket lock while the session still says
// `open`, which is the window this change exists to close.
func TestTheKeepAliveHandlerPublishesBeforeItCloses(t *testing.T) {
	t.Parallel()

	handler := theCaseFor(t, "*waEvents.KeepAliveTimeout")
	published := strings.Index(handler, "setReconnecting(true)")
	closed := strings.Index(handler, "ResetConnection()")
	if published < 0 || closed < 0 {
		t.Fatalf("the keepalive handler neither publishes nor closes:\n%s", handler)
	}
	if published > closed {
		t.Fatalf("the keepalive handler closes the socket before saying so, so `readyToSend` accepts "+
			"commands into the lock the close is holding:\n%s", handler)
	}
}

// The stamp the staleness check reads has to start with the socket, not with the
// authentication that follows it: whatsmeow's keepalive clock starts its first "last
// answered" when the connection is up, while `Connected` waits for prekeys and the passive
// switch. A stamp taken there is later than the clock it is compared against, and on a
// socket that took its time answering every timeout would read as one about an older
// connection -- which is the quiet socket this change exists to take down, left up.
func TestTheConnectionIsStampedWhenItIsDialledAndNotWhenItIsAnnounced(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")

	session.setDialing(true)
	afterTheDialStarted := time.Now()
	session.setConnected(true)

	if !session.connectedSince().Before(afterTheDialStarted) {
		t.Fatal("the connection is stamped when it is announced rather than when it is dialled, " +
			"so every keepalive timeout on a socket slow to answer reads as stale and the socket stays up")
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
