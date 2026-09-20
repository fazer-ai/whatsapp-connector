package whatsmeow

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
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

	listener, err := net.Listen("tcp", "127.0.0.1:0")
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
	_, _, err = websocket.Dial(context.Background(), "ws://"+listener.Addr().String(),
		&websocket.DialOptions{HTTPClient: client})
	took := time.Since(began)

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
