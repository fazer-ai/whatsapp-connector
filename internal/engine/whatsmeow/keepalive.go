package whatsmeow

import (
	"time"

	wm "go.mau.fi/whatsmeow"
)

// keepAliveGiveUp is how long whatsmeow may go without an answered keepalive before it
// takes the socket down and dials again.
//
// It matters because of what the wait costs everywhere else. A session runs its commands
// one at a time, so an account whose transport died holds every command behind the one in
// flight until somebody notices. Noticing is what this value sets.
//
// The operating system notices for us only when it has something to notice: an interface
// going down errors the socket, and the account is answered in tens of seconds. A path
// that goes quiet instead -- associated with no connectivity, a captive portal, a NAT
// entry that expired -- errors nothing, and then the only thing watching is whatsmeow's
// keepalive, which pings every KeepAliveIntervalMin..Max, waits KeepAliveResponseDeadline
// for the answer, and forces the reconnect once the time since the last answered ping
// passes this. At its own default of three minutes, that is how long an account stays
// behind a socket nobody is going to answer on.
//
// The value is not really a number of seconds, it is a number of consecutive failed
// pings. The first failure lands at worst at KeepAliveIntervalMax+KeepAliveResponseDeadline
// and the second at best at twice KeepAliveIntervalMin+KeepAliveResponseDeadline, so
// anything strictly between those two asks for exactly two failures in a row and nothing
// else does: below it, one lost ping -- or one ten-second stall -- takes a healthy session
// down; above it, three failures and the account waits half again as long.
//
// Inside the window nothing observable separates one value from another: the trigger is
// the second failure concluding, wherever it lands. What the value buys is margin, and the
// two errors it guards against are not worth the same. Falling below the window takes down
// sessions that were fine, in every session in the process, because the knob is one symbol
// for all of them. Rising above it costs twenty or thirty more seconds on a socket that is
// already dead. So the margin goes where the mistake hurts: 50s is ten seconds clear of
// the lower edge and pays for it on the side where being wrong is only slower.
//
// Giving up sooner is safe here in the way a ceiling on the command is not. A forced
// reconnect does not abandon the write: whatsmeow answers the pending query with a
// disconnect node, and `sendIQ` waits up to five seconds for the socket to come back and
// resends the same frame under the same id. What invariant 5 forbids is deciding a write
// failed while WhatsApp may have applied it, and nothing here decides that.
//
// Written in an init because whatsmeow keeps it package-wide and every connected client's
// keepalive loop reads it: the only moment a write cannot race a read is before any client
// exists, which is what an init is.
const keepAliveGiveUp = 50 * time.Second

func init() {
	wm.KeepAliveMaxFailTime = keepAliveGiveUp
}
