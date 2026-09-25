package whatsmeow

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	wm "go.mau.fi/whatsmeow"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
	"github.com/fazer-ai/whatsapp-connector/internal/testwait"
)

// proxyListener stands where a proxy would: it takes every connection, reports the first
// line or bytes it was sent, and hangs up. That is enough to say which protocol reached
// it -- `CONNECT web.whatsapp.com:443` from an HTTP client, the version byte 5 from a
// SOCKS5 one -- which is the statement these tests make: not that a dial happened, but
// that it went here, and spoke the proxy's protocol when it did.
type proxyListener struct {
	addr string
	seen chan string
}

func listenAsProxy(t *testing.T) *proxyListener {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	p := &proxyListener{addr: listener.Addr().String(), seen: make(chan string, 64)}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				_ = conn.SetReadDeadline(time.Now().Add(testwait.Budget))
				reader := bufio.NewReader(conn)
				if first, _ := reader.Peek(1); len(first) == 1 && first[0] == 5 {
					p.seen <- "socks5"
					return
				}
				line, _ := reader.ReadString('\n')
				p.seen <- strings.TrimSpace(line)
			}()
		}
	}()
	return p
}

// next is the first thing a connection to the listener said, or a failure if nothing
// connected within the budget.
func (p *proxyListener) next(t *testing.T) string {
	t.Helper()

	select {
	case said := <-p.seen:
		return said
	case <-time.After(testwait.Budget):
		t.Fatal("nothing dialled the proxy")
		return ""
	}
}

// quiet fails if anything connects to the listener within a short window.
func (p *proxyListener) quiet(t *testing.T, window time.Duration) {
	t.Helper()

	select {
	case said := <-p.seen:
		t.Fatalf("the proxy was dialled (%q) when nothing should have gone through it", said)
	case <-time.After(window):
	}
}

// reach asks for WhatsApp's front page through a client, which is enough to make it dial.
// What came back is not the point: every proxy in these tests hangs up or refuses.
func reach(t *testing.T, client *http.Client) error {
	t.Helper()

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://web.whatsapp.com/", http.NoBody)
	if err != nil {
		t.Fatalf("build the request: %v", err)
	}
	response, err := client.Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	return err
}

// standOn puts a session on a proxy the way a connect that already happened would have.
func standOn(t *testing.T, session *Session, proxyURL string) {
	t.Helper()

	if err := session.route.set(proxyURL); err != nil {
		t.Fatalf("route.set: %v", err)
	}
	session.setProxy(proxyURL)
}

// A session that asked for no proxy goes out directly, whatever the environment says.
//
// The transport whatsmeow clones reads `https_proxy` through `ProxyFromEnvironment`, and
// so did the one `newClient` cloned before #217: measured against the base, a QR connect
// with no proxy went out through the proxy the environment named. An account whose client
// never asked for a proxy is not routed through one because of a variable nobody set for
// it -- and the check is the field, not the environment, because Go reads that variable
// once per process and a test that set it would be measuring whichever test ran first.
func TestASessionWithoutAProxyIgnoresTheEnvironment(t *testing.T) {
	t.Parallel()

	transport, err := egressTransport("")
	if err != nil {
		t.Fatalf("egressTransport: %v", err)
	}
	if transport.Proxy != nil {
		t.Fatal("a session with no proxy still picks one up from somewhere -- the environment, " +
			"if the transport kept ProxyFromEnvironment -- so an account whose client asked for " +
			"nothing is routed through whatever the deployment's environment names")
	}
}

// The three HTTP clients whatsmeow talks to WhatsApp through all take the session's route.
//
// The pre-login one dials a pairing, the logged-in one dials the socket of a paired
// account, and the media one uploads and downloads files from WhatsApp's own hosts. A
// proxy on any two of them leaves the third going out from this instance, and which one
// is missing decides whether it is pairing, the running session, or every file.
func TestEveryClientWhatsmeowDialsThroughTakesTheRoute(t *testing.T) {
	t.Parallel()

	for _, scheme := range []string{"http", "socks5"} {
		t.Run(scheme, func(t *testing.T) {
			t.Parallel()

			proxy := listenAsProxy(t)
			recorded := &clientRecorder{}
			route := newEgressRoute()
			if err := route.set(scheme + "://user:secret@" + proxy.addr); err != nil {
				t.Fatalf("route.set: %v", err)
			}
			route.install(recorded)
			for name, client := range map[string]*http.Client{
				"pre-login": recorded.preLogin, "websocket": recorded.websocket, "media": recorded.media,
			} {
				if client == nil {
					t.Fatalf("the %s client was never set, so it keeps the library's own route", name)
				}
				_ = reach(t, client)
				said := proxy.next(t)
				if scheme == "http" && said != "CONNECT web.whatsapp.com:443 HTTP/1.1" {
					t.Fatalf("the %s client reached the proxy saying %q, not asking it to tunnel", name, said)
				}
				if scheme == "socks5" && said != "socks5" {
					t.Fatalf("the %s client reached the proxy saying %q, not speaking SOCKS5", name, said)
				}
			}
			// The ceiling stays on the two dials it bounds, and stays off the download.
			if recorded.preLogin.Timeout != dialCeiling || recorded.websocket.Timeout != dialCeiling {
				t.Fatal("routing a session through a proxy took the dial ceiling off its sockets")
			}
			if recorded.media.Timeout != 0 {
				t.Fatal("the media client has a timeout, which would cut off a large download")
			}
		})
	}
}

// clientRecorder stands in for a whatsmeow client, which keeps the three it is given in
// fields nothing outside the library can read.
type clientRecorder struct {
	preLogin, websocket, media *http.Client
}

func (c *clientRecorder) SetPreLoginHTTPClient(h *http.Client)  { c.preLogin = h }
func (c *clientRecorder) SetWebsocketHTTPClient(h *http.Client) { c.websocket = h }
func (c *clientRecorder) SetMediaHTTPClient(h *http.Client)     { c.media = h }

// The proxy's own address goes through the same refusal as a file a client names.
//
// A proxy is an address this instance dials because a client said so, which is exactly
// what `fetchTransport` guards for media: a client naming the cloud's metadata service as
// its proxy would otherwise have the instance connect to it, from inside. The check is on
// the address actually dialled, so a name that resolves into the range is refused like
// the address itself, and it happens before the connection is attempted rather than after
// a timeout.
//
// Loopback and the private ranges stay reachable: a proxy running beside the connector,
// or on the same network, is how this is usually deployed.
func TestAProxyOnAMetadataAddressIsRefusedBeforeItIsDialled(t *testing.T) {
	t.Parallel()

	for _, proxyURL := range []string{
		"socks5://169.254.169.254:1080",
		"http://169.254.170.2:80",
		"http://[fe80::1]:8080",
		"socks5://user:secret@100.100.100.200:1080",
	} {
		t.Run(proxyURL, func(t *testing.T) {
			t.Parallel()

			transport, err := egressTransport(proxyURL)
			if err != nil {
				t.Fatalf("egressTransport: %v", err)
			}
			started := time.Now()
			err = reach(t, &http.Client{Transport: transport})
			if !errors.Is(err, errMetadataAddress) {
				t.Fatalf("a proxy at a metadata address answered %v, want the metadata refusal", err)
			}
			// Refused at the socket, before a SYN: a dial that went out would sit in
			// SYN_SENT against an unroutable address for well over a minute.
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("the refusal took %s, which is a dial that went out and timed out", elapsed)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("the refusal quotes the proxy's credentials: %v", err)
			}
		})
	}

	proxy := listenAsProxy(t)
	transport, err := egressTransport("socks5://" + proxy.addr)
	if err != nil {
		t.Fatalf("egressTransport: %v", err)
	}
	_ = reach(t, &http.Client{Transport: transport})
	if said := proxy.next(t); said != "socks5" {
		t.Fatalf("a proxy on loopback was reached saying %q, want it dialled like any other", said)
	}
}

// A connect naming the proxy the session is already on changes nothing, and one naming a
// different proxy takes the socket down and dials again through the new one.
//
// The first half is the client's periodic reconnect, which carries the same proxy every
// time and must not recycle a healthy socket on each of them. The second is an operator
// moving an account's egress: whatsmeow reads its HTTP clients when it dials, so a socket
// left up stays on the old proxy until something closes it, and a session that answered
// `ok` to the move without closing it would go on leaving from the address the operator
// just asked to leave.
func TestAConnectMovesTheSessionOnlyWhenTheProxyChanges(t *testing.T) {
	t.Parallel()

	before, after := listenAsProxy(t), listenAsProxy(t)
	session, _ := newTestSession(t, "5511999990001")
	hungUp := make(chan struct{}, 4)
	session.disconnect = func(*wm.Client) { hungUp <- struct{}{} }
	onBefore := "socks5://" + before.addr
	standOn(t, session, onBefore)
	session.handle(&waEvents.Connected{})
	next(t, session)

	if err := session.Connect(t.Context(), engine.ConnectRequest{
		Pairing: "resume", Proxy: &engine.ProxyRequest{URL: onBefore},
	}); err != nil {
		t.Fatalf("a connect on the proxy the session is already on answered %v", err)
	}
	select {
	case <-hungUp:
		t.Fatal("a connect on the same proxy took the socket down, which every periodic reconnect would do")
	default:
	}
	before.quiet(t, 200*time.Millisecond)

	onAfter := "http://" + after.addr
	// The dial fails -- the listener hangs up -- and that is the connect's own outcome to
	// report. What this is about is where it went.
	_ = session.Connect(t.Context(), engine.ConnectRequest{
		Pairing: "resume", Proxy: &engine.ProxyRequest{URL: onAfter},
	})
	select {
	case <-hungUp:
	default:
		t.Fatal("a connect on a different proxy left the socket up, still leaving from the old one")
	}
	if said := after.next(t); said != "CONNECT web.whatsapp.com:443 HTTP/1.1" {
		t.Fatalf("the redial reached the new proxy saying %q", said)
	}
	before.quiet(t, 200*time.Millisecond)
	if got := session.proxyURL(); got != onAfter {
		t.Fatalf("the session believes it is on %q after moving", got)
	}
}

// A hang-up that ran out of time leaves the session on the proxy it was on.
//
// The socket is still up, on the old route, and it goes down a moment later regardless.
// A session that had already recorded the new proxy would read the next identical connect
// as no change at all, and never move.
func TestAMoveThatCouldNotHangUpIsTriedAgain(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	session.disconnect = func(*wm.Client) { <-release }
	standOn(t, session, "socks5://127.0.0.1:1")
	session.handle(&waEvents.Connected{})
	next(t, session)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := session.Connect(ctx, engine.ConnectRequest{
		Pairing: "resume", Proxy: &engine.ProxyRequest{URL: "socks5://127.0.0.1:2"},
	}); err == nil {
		t.Fatal("a move whose hang-up never finished answered success")
	}
	if got := session.proxyURL(); got != "socks5://127.0.0.1:1" {
		t.Fatalf("the session records %q, so the retry of this move will look like no change", got)
	}
}

// A pairing code asked for on a session with a proxy is asked for through that proxy.
//
// `pairing.request_code` carries no proxy of its own: the command has no field for one, and
// the connect this engine builds from it is the only request the session sees. A connect
// without a proxy is a request to go out directly, so a request built without it would
// move the account to this instance's address at the moment its operator asked for a code.
func TestAPairingCodeIsAskedForThroughTheSessionsProxy(t *testing.T) {
	t.Parallel()

	proxy := listenAsProxy(t)
	session, _ := newTestSession(t, "")
	onProxy := "socks5://" + proxy.addr
	standOn(t, session, onProxy)

	_ = session.requestCode(t.Context(), &protocol.Command{
		Type: protocol.CommandPairingRequestCode, Payload: json.RawMessage(`{"phone":"5511999990002"}`),
	})
	if said := proxy.next(t); said != "socks5" {
		t.Fatalf("the pairing dial reached the proxy saying %q", said)
	}
	if got := session.proxyURL(); got != onProxy {
		t.Fatalf("asking for a code left the session on %q", got)
	}
}

// A client built to replace a stale one dials through the session's proxy.
//
// The second of the two places a client is made: a connect on a session whose device is
// gone -- a pairing replaced by its operator, a logout that stopped halfway -- repairs it
// by building a fresh client, and that client is what dials the pairing that follows. One
// built without the proxy is a session that leaves through it until its first relogin and
// from this instance's own address after, with nothing on the wire to say it moved.
func TestARebuiltClientKeepsTheSessionsProxy(t *testing.T) {
	t.Parallel()

	proxy := listenAsProxy(t)
	session, _ := newTestSession(t, "")
	onProxy := "socks5://" + proxy.addr
	standOn(t, session, onProxy)
	previous := session.current()
	session.markStale()

	_ = session.Connect(t.Context(), engine.ConnectRequest{
		Pairing: "qr", Proxy: &engine.ProxyRequest{URL: onProxy},
	})
	if session.current() == previous {
		t.Fatal("the stale client was not replaced, so this says nothing about the one that replaces it")
	}
	if said := proxy.next(t); said != "socks5" {
		t.Fatalf("the rebuilt client reached the proxy saying %q", said)
	}
}

// Moving a session's route while a request is going through it is not a race.
//
// whatsmeow reads its media client from whichever goroutine is downloading, and a download
// can still be running after the socket that announced it went down -- which is exactly
// when a connect moving the session to another proxy runs. Replacing the client there
// would be a write racing that read; what moves is the transport behind a round tripper
// the client already holds, and this is the test that runs the two at once under -race.
func TestMovingTheRouteDoesNotRaceARequestGoingThroughIt(t *testing.T) {
	t.Parallel()

	first, second := listenAsProxy(t), listenAsProxy(t)
	route := newEgressRoute()
	recorded := &clientRecorder{}
	route.install(recorded)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 20 {
			_ = reach(t, recorded.media)
			if i%2 == 0 {
				_ = reach(t, recorded.websocket)
			}
		}
	}()
	for i := range 20 {
		at := first.addr
		if i%2 == 1 {
			at = second.addr
		}
		if err := route.set("socks5://" + at); err != nil {
			t.Fatalf("route.set: %v", err)
		}
	}
	<-done
}

// A session this instance opens stands on what its client last asked for, before anything
// is dialled.
//
// A `session.wake` brings an account up on an instance that never saw the connect behind
// it, and a pairing code is a command that opens a socket without carrying a request of
// its own. Standing on nothing, that socket is a request for no proxy, which is a request
// to go out directly: the account would dial WhatsApp from this instance's own address,
// the one its client asked this connector not to use.
func TestASessionOpenedHereStandsOnWhatItsClientAskedFor(t *testing.T) {
	t.Parallel()

	proxy := listenAsProxy(t)
	container := openStore(t)
	waEngine, err := New(container, Options{}, zerolog.Nop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = waEngine.Close() })
	onProxy := "socks5://" + proxy.addr
	if err := container.For("sid-1").PutDesiredConnected(t.Context(), store.Wants{
		Groups: true, CallAutoReject: true, Proxy: onProxy,
	}); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
	}

	opened, err := waEngine.Open(t.Context(), "sid-1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	session, _ := opened.(*Session)
	if session.proxyURL() != onProxy || !session.wantsGroups() || !session.rejectsCalls() {
		t.Fatalf("the session opened standing on proxy=%q groups=%v auto_reject=%v, not on what "+
			"its client asked for", session.proxyURL(), session.wantsGroups(), session.rejectsCalls())
	}
	_, _ = session.Execute(t.Context(), &protocol.Command{
		Type: protocol.CommandPairingRequestCode, Payload: json.RawMessage(`{"phone":"5511999990002"}`),
	})
	if said := proxy.next(t); said != "socks5" {
		t.Fatalf("the pairing dial reached the proxy saying %q", said)
	}
}
