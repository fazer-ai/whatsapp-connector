package whatsmeow

import (
	"time"

	waEvents "go.mau.fi/whatsmeow/types/events"
)

// keepAlivesBeforeReset is how many keepalive pings may go unanswered before this session
// stops waiting and takes its own socket down.
//
// Two, because one is not evidence: a ping is answered inside KeepAliveResponseDeadline or
// it is not, and a single ten-second stall on an otherwise healthy path is a blip. Two in a
// row is a minute of a server that stopped answering on a connection that is still open, and
// on that the account has nothing left to wait for.
const keepAlivesBeforeReset = 2

// keepAliveIsLost reports whether a keepalive timeout is the one to act on.
//
// The count is whatsmeow's, and it counts consecutively: a ping that comes back sets it to
// zero and emits KeepAliveRestored. So this asks "has the path been quiet since the last
// answer", not "have this many pings failed today".
func keepAliveIsLost(event *waEvents.KeepAliveTimeout) bool {
	return event.ErrorCount >= keepAlivesBeforeReset
}

// keepAliveStaleAfter is how much later than the last answered ping a moment of known
// liveness may be before the timeout that counts from that ping is read as describing
// something already over.
//
// On the connection a timeout is really about, the last answered ping is *later* than the
// connection came up, so the difference is negative -- except in the one case where no ping
// was ever answered, and there whatsmeow dates it from the moment the loop started, which
// is the same connection a hair earlier. The two shapes this has to separate from that are
// both a minute or more out: a socket that was replaced spent two failed pings first, and a
// run of failures the socket recovered from took at least that long to recover. Anything
// between the two separates them, and this is the low end of it.
//
// The slack is not padding. The moments this session can compare against are dated by when
// it handled an event, and the ping they are compared to is dated by whatsmeow's own clock
// inside the keepalive loop, so a session that was slow to handle something would otherwise
// read a fresh timeout as an old one.
const keepAliveStaleAfter = 20 * time.Second

// keepAliveIsStale reports whether a keepalive timeout describes something already over:
// a socket this session is no longer on, or a run of failures the socket recovered from.
//
// Both are the same question, which is why they are one comparison. Every timeout in a run
// is dated from the same last answered ping; a connection that started well after that date
// is a different connection, and a recovery seen well after it ended the run that timeout
// belongs to. A session with no connection at all has nothing to take down, which reads the
// same way here.
func keepAliveIsStale(aliveAt time.Time, event *waEvents.KeepAliveTimeout) bool {
	if aliveAt.IsZero() {
		return true
	}
	return aliveAt.After(event.LastSuccess.Add(keepAliveStaleAfter))
}

// socketUp is the instant to date a socket that replaced the previous one inside whatsmeow,
// where nothing observable stands between the two and the announcement can trail the socket
// by minutes.
//
// `authenticated` is when the library said it authenticated a socket, taken from its own log
// on its own goroutine (`authenticatedLine`). It is one auth round trip after the keepalive
// loop this stamp is compared against started, which the slack above covers, so believing it
// cuts the window from the whole announcing sequence to nothing that matters.
//
// Believing it is still not the same as trusting it, because nothing ties that line to the
// connection being announced here: a second connection authenticating while this arm runs
// logs its own. So the value is bounded, and the two bounds are the two things a wrong one
// could be. Later than `heard` is a value this connection cannot have produced, because the
// library logs the line before it dispatches the announcement: that is a later connection's,
// and a stamp in the future would poison every comparison until the clock passes it. Not
// later than `replaced` is a value from a connection that is already over, or the zero a
// session carries before the library has authenticated anything: dating a new socket from
// before the old one was dated would make the old socket's own timeouts read as current and
// take the healthy replacement down for them.
//
// Anything outside those falls back to `heard`, which is where main already is: a window
// that is too wide, never a stamp that is wrong.
func socketUp(authenticated, replaced, heard time.Time) time.Time {
	if authenticated.After(replaced) && !authenticated.After(heard) {
		return authenticated
	}
	return heard
}
