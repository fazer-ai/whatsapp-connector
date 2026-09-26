package whatsmeow

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/fazer-ai/whatsapp-connector/internal/testwait"
)

// A dial that never completes ends on the ceiling instead of holding the lock for ever.
//
// Against a listener that accepts the connection and then says nothing, which is the shape
// that matters: a refused dial comes back at once and was never the problem. What is being
// asserted is the mechanism the fix rests on -- that `HTTPClient.Timeout` bounds the
// handshake -- because a timeout that did not reach it would leave `newClient` looking like
// a fix and doing nothing.
func TestADialThatNeverAnswersEndsOnTheCeiling(t *testing.T) {
	t.Parallel()

	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- conn // held open and never answered
	}()

	const ceiling = 300 * time.Millisecond
	client := &http.Client{Timeout: ceiling, Transport: http.DefaultTransport.(*http.Transport).Clone()}
	began := time.Now()
	_, refused, err := websocket.Dial(context.Background(), "ws://"+listener.Addr().String(),
		&websocket.DialOptions{HTTPClient: client})
	took := time.Since(began)
	closeBody(refused)

	if err == nil {
		t.Fatal("the dial completed against a listener that never answered")
	}
	if took > ceiling+2*time.Second {
		t.Errorf("the dial took %s against a %s ceiling: HTTPClient.Timeout does not bound "+
			"the handshake on this pin, so newClient's ceiling reaches nothing", took, ceiling)
	}
	select {
	case conn := <-accepted:
		_ = conn.Close()
	default:
		t.Error("the listener never accepted, so what was measured is not a hanging handshake")
	}
}

// And the client this connector builds carries it.
func TestTheClientWeBuildIsTheOneTheLibraryWouldHave(t *testing.T) {
	t.Parallel()

	// Read off the constant rather than restated, so the two cannot drift.
	if dialCeiling <= 0 {
		t.Fatal("dialCeiling is not a ceiling, so the dial is bounded by nothing again")
	}
	// The library's own ceiling on the other half of the same connection. If a pin moves
	// it, the reasoning written on `dialCeiling` -- that the two halves now match -- is
	// what has to be re-read, and this is what says so.
	if wmHandshakeCeiling := 20 * time.Second; dialCeiling != wmHandshakeCeiling {
		t.Errorf("dialCeiling is %s and whatsmeow's NoiseHandshakeResponseTimeout is %s: the "+
			"two halves of a connection under the same lock no longer end together, which "+
			"is the reason written on the constant", dialCeiling, wmHandshakeCeiling)
	}
	// The same transport and not merely one of the same type: a fresh `&http.Transport{}`
	// would dial without the proxy settings and the HTTP/2 configuration the process was
	// started with, which is a second difference from whatsmeow's own client hiding behind
	// a field that looks set.
	if dialTransport() != http.DefaultTransport {
		t.Error("the dial clients are built from a transport that is not http.DefaultTransport, " +
			"so they are not the clients whatsmeow would have built")
	}
}

// Nothing builds a whatsmeow client except `newClient`.
//
// A second construction site is how the ceiling goes missing from one path and not the
// other, and the path it would go missing from is whichever one a later change adds. There
// were two before this, which is why it is a fence and not a comment.
func TestEveryWhatsmeowClientIsBuiltHere(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	built := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, isCall := n.(*ast.CallExpr)
				if !isCall {
					return true
				}
				selector, isSel := call.Fun.(*ast.SelectorExpr)
				if !isSel || selector.Sel.Name != "NewClient" {
					return true
				}
				pkg, isIdent := selector.X.(*ast.Ident)
				if !isIdent || pkg.Name != "wm" {
					return true
				}
				built++
				if fn.Name.Name != "newClient" {
					t.Errorf("%s:%s calls wm.NewClient directly, so the client it builds has "+
						"no ceiling on its dial and holds whatsmeow's socketLock for as long "+
						"as the dial hangs (#290)", name, fn.Name.Name)
				}
				return true
			})
		}
	}
	if built == 0 {
		t.Fatal("nothing in this package builds a whatsmeow client, so this fence is measuring nothing")
	}
}

// And it stops at the dial: a connection opened under the ceiling still works after it.
//
// This is the failure that would be worse than the one being fixed. `http.Client.Timeout`
// is documented as covering the whole of a request including reading the body, and a
// websocket connection is a hijacked response, so the obvious reading of the field is that
// it would cut every session at twenty seconds. `coder/websocket` zeroes the field on the
// client it actually dials with and turns it into a context around the handshake alone --
// which is a property of the pin, not of `net/http`, and therefore something to measure
// rather than to quote.
func TestTheCeilingDoesNotOutliveTheDialItBounds(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		for {
			kind, message, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if err := conn.Write(r.Context(), kind, message); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	const ceiling = 300 * time.Millisecond
	client := &http.Client{Timeout: ceiling, Transport: http.DefaultTransport.(*http.Transport).Clone()}
	conn, upgraded, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http"),
		&websocket.DialOptions{HTTPClient: client})
	closeBody(upgraded)
	if err != nil {
		t.Fatalf("dial the echo server under a %s ceiling: %v", ceiling, err)
	}
	defer func() { _ = conn.CloseNow() }()

	// Past the ceiling by a wide enough margin that a timeout covering the connection
	// would have fired, and not a synchronisation: the elapsed time is the measurement.
	time.Sleep(ceiling + time.Second)

	talk, hangUp := context.WithTimeout(t.Context(), testwait.Budget)
	defer hangUp()
	if err := conn.Write(talk, websocket.MessageText, []byte("still here")); err != nil {
		t.Fatalf("write %s after the dial, under a %s ceiling: the ceiling outlived the "+
			"dial, so every session would end on it (#290): %v", ceiling+time.Second, ceiling, err)
	}
	kind, echoed, err := conn.Read(talk)
	if err != nil {
		t.Fatalf("read back %s after the dial, under a %s ceiling: %v", ceiling+time.Second, ceiling, err)
	}
	if kind != websocket.MessageText || string(echoed) != "still here" {
		t.Fatalf("the echo came back as %v %q", kind, echoed)
	}
}

// The two clients whatsmeow dials through, read off the pin rather than remembered.
//
// `unlockedConnect` picks between `websocketHTTP` and `preLoginHTTP` on whether the device
// has an ID, so a ceiling installed on one of the two leaves the other dial unbounded, and
// the unbounded one would be pairing. A pin that adds a third, or renames one, needs
// `egressRoute.install` to move with it, and nothing else in this repository would notice: the
// fields are unexported, so the ceiling cannot be read back off the client at run time.
func TestTheDialGoesOutThroughTheClientsNewClientSets(t *testing.T) {
	t.Parallel()

	source, err := os.ReadFile(filepath.Join(whatsmeowRoot(t), "client.go"))
	if err != nil {
		t.Fatalf("read client.go of the pinned whatsmeow: %v", err)
	}
	body := funcBody(t, string(source), "func (cli *Client) unlockedConnect(")

	found := map[string]bool{}
	for _, match := range regexp.MustCompile(`cli\.(\w*HTTP\w*)`).FindAllStringSubmatch(body, -1) {
		found[match[1]] = true
	}
	for _, wanted := range []string{"websocketHTTP", "preLoginHTTP"} {
		if !found[wanted] {
			t.Errorf("the pinned whatsmeow no longer dials through cli.%s: re-read which "+
				"clients unlockedConnect picks between and move the route's ceiling to "+
				"them, because a dial through a client it does not set has none (#290)", wanted)
		}
		delete(found, wanted)
	}
	for extra := range found {
		t.Errorf("the pinned whatsmeow also dials through cli.%s, which the route does not "+
			"set: that dial holds the socket write lock with no ceiling on it (#290)", extra)
	}
}

// And every client a session adopts gets the ceiling on both of them, whichever way the
// session goes out.
//
// Read off the clients themselves since #217. The route sets them through an interface, so
// a recorder can be handed in where the library's client keeps them in fields nothing can
// read back; before that, the only thing that could be checked was the shape of the calls
// in newClient's source. What still has to be read off the source is that `adopt` installs
// the route at all, which is the half below.
func TestNewClientPutsTheCeilingOnBothDialClients(t *testing.T) {
	t.Parallel()

	for _, proxyURL := range []string{"", "socks5://127.0.0.1:1080", "http://127.0.0.1:3128"} {
		route := newEgressRoute()
		if err := route.set(proxyURL); err != nil {
			t.Fatalf("route.set(%q): %v", proxyURL, err)
		}
		recorded := &clientRecorder{}
		route.install(recorded)
		for name, client := range map[string]*http.Client{
			"SetWebsocketHTTPClient": recorded.websocket, "SetPreLoginHTTPClient": recorded.preLogin,
		} {
			if client == nil {
				t.Fatalf("with proxy %q nothing calls %s, so that dial keeps whatsmeow's own client "+
					"and is bounded by the context it is handed alone (#290)", proxyURL, name)
			}
			if client.Timeout != dialCeiling {
				t.Errorf("with proxy %q, %s is given a client whose Timeout is %s, not dialCeiling, "+
					"so the ceiling this package documents is not on that dial (#290)",
					proxyURL, name, client.Timeout)
			}
		}
		if recorded.media == nil || recorded.media.Timeout != 0 {
			t.Errorf("with proxy %q the media client is %v: it has to be set, or files leave from "+
				"this instance, and have no timeout, or every download longer than it is cut off",
				proxyURL, recorded.media)
		}
		// Each its own transport, and none the process-wide one: a zero field would hand the
		// client http.DefaultTransport, shared with everything else in the process.
		seen := map[*http.Transport]bool{}
		for _, client := range []*http.Client{recorded.websocket, recorded.preLogin, recorded.media} {
			if client == nil {
				continue
			}
			swapped, isRoute := client.Transport.(*swappedTransport)
			if !isRoute {
				t.Errorf("with proxy %q a client dials through %T, not the session's route", proxyURL, client.Transport)
				continue
			}
			current := swapped.current.Load()
			if current == nil || current == http.DefaultTransport {
				t.Errorf("with proxy %q a client's route goes through %v, not a transport of its own", proxyURL, current)
			}
			if seen[current] {
				t.Errorf("with proxy %q two clients share one transport", proxyURL)
			}
			seen[current] = true
		}
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "session.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse session.go: %v", err)
	}
	installed := false
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "adopt" {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, isCall := n.(*ast.CallExpr)
			if !isCall {
				return true
			}
			if selector, isSel := call.Fun.(*ast.SelectorExpr); isSel && selector.Sel.Name == "install" {
				installed = true
			}
			return true
		})
	}
	if !installed {
		t.Fatal("adopt does not install the session's route, so a client it takes on keeps " +
			"whatsmeow's own dial clients: no ceiling on the dial (#290), and the environment's " +
			"proxy instead of the session's (#217)")
	}
}

// closeBody drains what websocket.Dial hands back alongside the connection. A successful
// upgrade leaves nothing to read, and a failed one leaves a body the linter is right to
// want closed.
func closeBody(response *http.Response) {
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
}

// A transport nobody chose is said out loud rather than dialled through.
//
// Through the seam and not by replacing http.DefaultTransport: every other test in this
// package runs in parallel and would see that replacement.
func TestATransportTheProcessDidNotChooseIsRefused(t *testing.T) {
	t.Parallel()

	defer func() {
		said, panicked := recover().(string)
		if !panicked {
			t.Error("a round tripper that is not an *http.Transport was accepted, so the " +
				"dial clients would be built from something nobody chose")
			return
		}
		if !strings.Contains(said, "not *http.Transport") {
			t.Errorf("the panic says %q, which does not name what went wrong", said)
		}
	}()
	_ = transportOf(roundTripperThatIsNotATransport{})
}

type roundTripperThatIsNotATransport struct{}

func (roundTripperThatIsNotATransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, nil
}
