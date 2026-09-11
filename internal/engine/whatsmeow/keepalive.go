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
