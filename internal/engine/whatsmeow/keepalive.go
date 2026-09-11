package whatsmeow

import (
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
