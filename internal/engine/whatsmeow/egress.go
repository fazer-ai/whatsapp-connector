package whatsmeow

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
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

// routeThrough decides where a session's traffic with WhatsApp leaves from: through the
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
// Replaced as clients rather than through `SetProxyAddress`, for three reasons. The dial
// ceiling lives on the websocket clients, and a client built here keeps it whichever way
// the traffic goes. The dial to the proxy goes through `refuseMetadataAddress`, which the
// library's own dialer has no place for: a client naming the cloud's metadata service as
// its proxy would otherwise have this instance dial it. And a session with no proxy stops
// reading `https_proxy` from the environment, which the library's default does -- an
// account whose client never asked for a proxy is not routed through one because of a
// variable nobody set for it.
//
// Called before a dial, never during one: the library reads these clients when it dials,
// and a socket already open keeps the path it was opened on.
func routeThrough(client egressClient, proxyURL string) error {
	transports := make([]*http.Transport, 3)
	for i := range transports {
		transport, err := egressTransport(proxyURL)
		if err != nil {
			return err
		}
		transports[i] = transport
	}
	client.SetWebsocketHTTPClient(&http.Client{Timeout: dialCeiling, Transport: transports[0]})
	client.SetPreLoginHTTPClient(&http.Client{Timeout: dialCeiling, Transport: transports[1]})
	// No timeout, as whatsmeow builds it: a download of a large file is not a dial, and
	// the ceiling above would cut it off.
	client.SetMediaHTTPClient(&http.Client{Transport: transports[2]})
	return nil
}

// egressClient is the part of `*wm.Client` routeThrough sets. An interface only because the
// library keeps the three clients in fields nothing outside it can read, so a test that
// wants to say which of them took the route has to be handed them.
type egressClient interface {
	SetPreLoginHTTPClient(*http.Client)
	SetWebsocketHTTPClient(*http.Client)
	SetMediaHTTPClient(*http.Client)
}

var _ egressClient = (*wm.Client)(nil)

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
