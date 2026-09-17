package session

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The contract now says in as many words that neither ceiling reaches the control stream:
// a client should not expect `expired` for a `session.wake`, a `session.delete` or an
// `admin.ping`, and the reason is that nothing on that path reads the field. That sentence
// travels to every vendoring client and is the reason #241 closes without a ceiling on the
// command, so it needs something that fails when it stops being true.
//
// It would stop being true quietly. A later round adding an `expired(command, ...)` guard
// to `wake` would be a small, locally sensible change -- it is exactly what the session
// path does at the other end -- and it would turn a promise the client is held to into a
// lie, with every test still green: the three handlers have no test that asserts a command
// is *not* refused for arriving late, because nothing here refuses anything.
//
// What the fence reads is the three handlers themselves, by name, rather than a list of
// forbidden lines: `expired` and `Deadline` are the two ways to ask the question in this
// package, and a handler that asks it is a handler that may answer `expired`.
func TestNoControlHandlerReadsTheDeadline(t *testing.T) {
	t.Parallel()

	// The three commands Dispatch carries out before any session is involved. Kept here by
	// name because that is the set the contract's sentence names.
	handlers := map[string]bool{"wake": false, "takeForDelete": false, "pong": false}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "manager.go", nil, 0)
	if err != nil {
		t.Fatalf("parse manager.go: %v", err)
	}

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil {
			continue
		}
		if _, wanted := handlers[fn.Name.Name]; !wanted {
			continue
		}
		handlers[fn.Name.Name] = true
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			switch named := node.(type) {
			case *ast.Ident:
				if named.Name == "expired" {
					t.Errorf("%s reads the deadline; contract/PROTOCOL.md tells clients no control command has a ceiling", fn.Name.Name)
				}
			case *ast.SelectorExpr:
				if named.Sel.Name == "Deadline" {
					t.Errorf("%s reads the deadline; contract/PROTOCOL.md tells clients no control command has a ceiling", fn.Name.Name)
				}
			}
			return true
		})
	}

	for name, found := range handlers {
		if !found {
			t.Errorf("%s is gone from manager.go; the contract still names it as a control command with no ceiling", name)
		}
	}
}

// The sentence the test above guards has to be in the half that a client receives. The
// fence in internal/protocol catches an obligation written into the unvendored README;
// this one catches the other direction, a round that deletes the paragraph outright.
//
// Matched against the prose with its line breaks flattened, because the file is wrapped by
// hand and a fence that a re-wrap turns red is a fence somebody deletes.
func TestTheContractSaysControlHasNoCeiling(t *testing.T) {
	t.Parallel()
	source, err := os.ReadFile(filepath.Join("..", "..", "contract", "PROTOCOL.md"))
	if err != nil {
		t.Fatalf("read the contract: %v", err)
	}
	prose := strings.Join(strings.Fields(string(source)), " ")
	for _, phrase := range []string{
		"a client should not expect `expired` for a command sent there",
		"A control command therefore has no ceiling, whatever it carries",
		"should publish another `session.wake` rather than wait on the one it already sent",
	} {
		if !strings.Contains(prose, phrase) {
			t.Errorf("contract/PROTOCOL.md no longer says %q; #241 closed on that sentence being there", phrase)
		}
	}
}
