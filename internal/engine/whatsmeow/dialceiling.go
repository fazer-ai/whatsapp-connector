package whatsmeow

import (
	"net/http"
	"time"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// dialCeiling bounds the websocket dial, which is the one wait inside whatsmeow's
// connection that has no ceiling of its own.
//
// `connect` takes `cli.socketLock` for writing and runs the whole of `unlockedConnect`
// under it: the dial and then the noise handshake. The handshake already ends on its own,
// at the library's `NoiseHandshakeResponseTimeout` of twenty seconds. The dial did not,
// and it is bounded only by the context it is handed, which on the reconnection path is
// the library's own and not ours. Meanwhile every node this connector writes reads the
// socket pointer through `socketLock.RLock()`, and `sync.RWMutex` takes no context, so a
// dial that hangs holds every one of those waiting with no deadline of their own -- which
// is precisely the reconnection a send's ceiling exists to survive (#290).
//
// Twenty seconds because that is what the library already chose for the other half of the
// same connection, so the two halves now end rather than one. Measured against the real
// endpoint, five dials took 209, 399, 465, 517 and 833ms, so this is a ceiling on a wait
// that has gone wrong rather than a bound on a healthy one.
const dialCeiling = 20 * time.Second

// newClient is the only place a whatsmeow client is made, so the ceiling above cannot be
// missing from one of them.
//
// `SetWebsocketHTTPClient` is what carries it: `coder/websocket` turns `HTTPClient.Timeout`
// into a `context.WithTimeout` around the dial and zeroes it on the client it then uses, so
// the ceiling ends the handshake for the connection rather than the connection itself.
// Read out of the pinned `dial.go` rather than assumed, and measured by
// `TestTheCeilingDoesNotOutliveTheDialItBounds`, because a timeout that killed the socket
// after it opened would be a much worse bug than the one this fixes.
//
// Both of them, because whatsmeow dials through two different clients and picks between
// them on whether the device has an ID: a session that has never paired goes out through
// the pre-login one. That is the QR dial, which is a connect like any other and holds the
// same write lock, so a ceiling on one of the two would leave pairing with none.
//
// Neither is the client media downloads use, so a ceiling here does not put one on a
// download of any size. Each gets its own clone of the default transport, which is what
// the library builds its own from; a pin that starts configuring that transport would not
// reach these, and `TestTheClientWeBuildIsTheOneTheLibraryWouldHave` is what notices.
func newClient(device *store.Device, log waLog.Logger) *wm.Client {
	client := wm.NewClient(device, log)
	client.SetWebsocketHTTPClient(&http.Client{
		Timeout:   dialCeiling,
		Transport: http.DefaultTransport.(*http.Transport).Clone(),
	})
	client.SetPreLoginHTTPClient(&http.Client{
		Timeout:   dialCeiling,
		Transport: http.DefaultTransport.(*http.Transport).Clone(),
	})
	return client
}
