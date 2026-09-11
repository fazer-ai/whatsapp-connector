package whatsmeow

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	waEvents "go.mau.fi/whatsmeow/types/events"
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
	session.setConnected(true)

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
	session.setConnected(true)

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

// And the second one is acted on rather than waited out. What the log line stands for is
// the reset beside it; the reset itself is not observable from here, because a client with
// no socket has nothing to take down, and the phase that measures it is the live one.
func TestTheSecondMissedKeepAliveIsActedOn(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	var written bytes.Buffer
	session.log = zerolog.New(&written)
	session.setConnected(true)

	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})

	// Before the reset and not after it: ResetConnection blocks on the close handshake
	// holding whatsmeow's socket lock, and a session still reporting `open` in that window
	// accepts commands into the lock the close is holding.
	if got := session.state(); got != "reconnecting" {
		t.Fatalf("the session reports %q while taking its own socket down, so readyToSend still accepts", got)
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
