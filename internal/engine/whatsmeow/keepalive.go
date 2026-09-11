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

// keepAliveStaleAfter is how far a connection may have started after the last answered
// ping before a timeout about it is read as belonging to an earlier socket.
//
// On the connection a timeout is really about, the last answered ping is *later* than the
// connection came up, so the difference is negative -- except in the one case where no ping
// was ever answered, and there whatsmeow dates it from the moment the loop started, which
// is the same connection a hair earlier. A timeout left over from a socket that has already
// been replaced is the other shape: the replacement came up after that socket spent its two
// failed pings, so it is a minute or more later. Anything between the two separates them,
// and this is the low end of it.
const keepAliveStaleAfter = 20 * time.Second

// keepAliveIsStale reports whether a keepalive timeout is about a connection this session
// is no longer on. A session with no connection at all has nothing to take down, which
// reads the same way here.
func keepAliveIsStale(connectedSince time.Time, event *waEvents.KeepAliveTimeout) bool {
	if connectedSince.IsZero() {
		return true
	}
	return connectedSince.After(event.LastSuccess.Add(keepAliveStaleAfter))
}
