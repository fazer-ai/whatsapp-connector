package whatsmeow

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	wm "go.mau.fi/whatsmeow"
	"golang.org/x/net/proxy"
)

// egressDial is how long a dial to WhatsApp, or to the proxy in front of it, may take to
// open a connection, and how often an open one is kept alive. The values
// `http.DefaultTransport` dials with, which are also the ones whatsmeow's own
// `SetProxyAddress` gives a SOCKS5 dialer: replacing the dialer to add a check is not a
// reason to change how long it waits.
const egressDial = 30 * time.Second

// egressRoute decides where a session's traffic with WhatsApp leaves from: through the
// proxy its client asked for, or directly from this instance when it asked for none.
//
// All three of the library's HTTP clients, because all three talk to WhatsApp: the
// pre-login websocket (pairing), the logged-in one, and the media uploads and downloads,
// which go to WhatsApp's own hosts. A proxy on the socket alone would have every file the
// account sends or receives leave from the instance's address. `fetchTransport`, which
// fetches the files a client names, is not among them and is not touched: that is a
// request to wherever the client said, not traffic with WhatsApp, and routing it through
// the proxy would dial the proxy while the proxy fetched anything, switching off the
// address check that transport exists for.
//
// One per session, installed once on every client the session adopts, and changed from
// the inside. whatsmeow keeps its three clients in plain fields and reads the media one
// from whatever goroutine is downloading, including a download still running after the
// socket that announced it went down, so replacing a client on a live session is a write
// that races those reads. What `set` replaces is the transport behind a round tripper the
// client already holds, atomically: a request already under way finishes on the route it
// started on, and the next one takes the new one.
//
// Built here rather than through `SetProxyAddress`, for three reasons. The dial ceiling
// lives on the websocket clients, and a client built here keeps it whichever way the
// traffic goes. The dial to the proxy goes through `refuseMetadataAddress`, which the
// library's own dialer has no place for: a client naming the cloud's metadata service as
// its proxy would otherwise have this instance dial it. And a session with no proxy stops
// reading `https_proxy` from the environment, which the library's default does -- an
// account whose client never asked for a proxy is not routed through one because of a
// variable nobody set for it.
type egressRoute struct {
	websocket, preLogin, media swappedTransport
}

// newEgressRoute is a route that goes out directly, which is where every session starts:
// the proxy arrives with the connect, and nothing is dialled before one.
func newEgressRoute() *egressRoute {
	route := &egressRoute{}
	if err := route.set(""); err != nil {
		// Nothing to parse on the direct route, so nothing can fail. Said rather than
		// swallowed, because a route with no transport behind it panics on first use.
		panic(fmt.Sprintf("whatsmeow: the direct route could not be built: %v", err))
	}
	return route
}

// set moves the route to a proxy, or to "" for direct. Only the dials that start after it
// take the new path: a socket already open keeps the one it was opened on, which is why a
// session moving an open socket hangs it up first.
func (r *egressRoute) set(proxyURL string) error {
	built := make([]*http.Transport, 3)
	for i := range built {
		transport, err := egressTransport(proxyURL)
		if err != nil {
			return err
		}
		built[i] = transport
	}
	r.websocket.swap(built[0])
	r.preLogin.swap(built[1])
	r.media.swap(built[2])
	return nil
}

// install hands a client the route's three clients. Called once per client, before
// anything else can be using it.
//
// The ceiling from #290 is on the two dial clients and on neither of the others: the
// media client carries no timeout, as whatsmeow builds it, because a download of a large
// file is not a dial and the ceiling would cut it off.
func (r *egressRoute) install(client egressClient) {
	client.SetWebsocketHTTPClient(&http.Client{Timeout: dialCeiling, Transport: &r.websocket})
	client.SetPreLoginHTTPClient(&http.Client{Timeout: dialCeiling, Transport: &r.preLogin})
	client.SetMediaHTTPClient(&http.Client{Transport: &r.media})
}

// egressClient is the part of `*wm.Client` install sets. An interface only because the
// library keeps the three clients in fields nothing outside it can read, so a test that
// wants to say which of them took the route has to be handed them.
type egressClient interface {
	SetPreLoginHTTPClient(*http.Client)
	SetWebsocketHTTPClient(*http.Client)
	SetMediaHTTPClient(*http.Client)
}

var _ egressClient = (*wm.Client)(nil)

// swappedTransport is a round tripper whose transport can be replaced while requests are
// going through it.
type swappedTransport struct {
	current atomic.Pointer[http.Transport]
}

// RoundTrip sends a request through whichever transport is current when it starts.
func (t *swappedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return t.current.Load().RoundTrip(request) //nolint:wrapcheck // a round tripper passes its transport's errors on untouched
}

// swap installs the next transport and lets go of the previous one's idle connections,
// which lead to the path being left. Its connections in use finish where they are.
func (t *swappedTransport) swap(next *http.Transport) {
	if previous := t.current.Swap(next); previous != nil {
		previous.CloseIdleConnections()
	}
}

// egressTransport is the transport one of those clients dials through.
//
// A clone of the transport whatsmeow clones, so the proxy and the dialer are the only
// differences from the library's own. The URL has already been through `Validate`; the
// errors below are for a caller that skipped it, and none of them repeats the URL, which
// carries credentials.
func egressTransport(proxyURL string) (*http.Transport, error) {
	transport := dialTransport().Clone()
	// Not `ProxyFromEnvironment`, which is what the clone carries: see routeThrough.
	transport.Proxy = nil
	dialer := &net.Dialer{Timeout: egressDial, KeepAlive: egressDial, Control: refuseMetadataAddress}
	transport.DialContext = dialer.DialContext
	if proxyURL == "" {
		return transport, nil
	}

	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return nil, errors.New("whatsmeow: the session's proxy is not a URL")
	}
	switch parsed.Scheme {
	case "http", "https":
		// The transport dials the proxy through DialContext above, so the address check
		// is on the proxy's resolved address -- a name that resolves into the metadata
		// range is refused the same as the address written out.
		transport.Proxy = http.ProxyURL(parsed)
	case "socks5":
		// The forward dialer is the one above, for the same reason.
		socks, err := proxy.FromURL(parsed, dialer)
		if err != nil {
			// x/net's errors here name the scheme, never the URL.
			return nil, fmt.Errorf("whatsmeow: the session's SOCKS5 proxy: %w", err)
		}
		contextual, ok := socks.(proxy.ContextDialer)
		if !ok {
			// x/net's SOCKS5 dialer implements it; a pin where it does not would dial
			// without the command's deadline, which is worth failing over.
			return nil, fmt.Errorf("whatsmeow: the SOCKS5 dialer is %T and takes no context", socks)
		}
		transport.DialContext = contextual.DialContext
	default:
		return nil, fmt.Errorf("whatsmeow: a proxy with the scheme %q", parsed.Scheme)
	}
	return transport, nil
}
