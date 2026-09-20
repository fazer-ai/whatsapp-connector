package protocol_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// enginePackage is where the commands are carried out, read as source rather than imported.
//
// Source and not a call, because what is being asked is which commands reach a particular
// gate, and nothing at runtime answers that: the gate is reached through four handlers and
// a client cannot see which. Reading it here keeps `internal/protocol` free of a dependency
// on the engine, which would be backwards -- the contract does not know how it is served.
const enginePackage = "../engine/whatsmeow"

// commandsThatReach returns the commands whose handler reaches a method, directly or
// through other methods of the same type.
//
// The path is what it follows, not the name: `react` renamed to `reactToMessage` is the
// same command reaching the same gate, and a fence that reprimands a rename is a fence that
// reprimands the first refactor that changes no contract (#292).
func commandsThatReach(t *testing.T, gate string) map[protocol.CommandType]bool {
	t.Helper()

	files := parseEngine(t)
	calls := methodCalls(files)
	handlers := commandHandlers(t, files)

	reaching := map[protocol.CommandType]bool{}
	for command, handlers := range handlers {
		for _, handler := range handlers {
			if reaches(calls, handler, gate, map[string]bool{}) {
				reaching[command] = true
			}
		}
	}
	if len(reaching) == 0 {
		t.Fatalf("no command's handler reaches %s, so either the gate was renamed and this "+
			"fence is now reading nothing, or the switch it walks has changed shape", gate)
	}
	return reaching
}

func parseEngine(t *testing.T) []*ast.File {
	t.Helper()

	entries, err := os.ReadDir(enginePackage)
	if err != nil {
		t.Fatalf("read %s: %v", enginePackage, err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, filepath.Join(enginePackage, name), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files = append(files, parsed)
	}
	if len(files) == 0 {
		t.Fatalf("no production source under %s", enginePackage)
	}
	return files
}

// methodCalls maps each method of the session to the methods it calls on the receiver.
func methodCalls(files []*ast.File) map[string][]string {
	calls := map[string][]string{}
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil {
				continue
			}
			receiver := receiverName(fn)
			if receiver == "" {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, isCall := n.(*ast.CallExpr)
				if !isCall {
					return true
				}
				selector, isSel := call.Fun.(*ast.SelectorExpr)
				if !isSel {
					return true
				}
				if ident, isIdent := selector.X.(*ast.Ident); isIdent && ident.Name == receiver {
					calls[fn.Name.Name] = append(calls[fn.Name.Name], selector.Sel.Name)
				}
				return true
			})
		}
	}
	return calls
}

func receiverName(fn *ast.FuncDecl) string {
	field := fn.Recv.List[0]
	if len(field.Names) == 0 {
		return ""
	}
	return field.Names[0].Name
}

// commandHandlers reads the switch that routes a command to the methods that carry it out.
func commandHandlers(t *testing.T, files []*ast.File) map[protocol.CommandType][]string {
	t.Helper()

	byIdentifier := commandsByIdentifier(t)
	handlers := map[protocol.CommandType][]string{}
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			clause, ok := n.(*ast.CaseClause)
			if !ok {
				return true
			}
			for _, match := range clause.List {
				selector, isSel := match.(*ast.SelectorExpr)
				if !isSel {
					continue
				}
				pkg, isIdent := selector.X.(*ast.Ident)
				if !isIdent || pkg.Name != "protocol" {
					continue
				}
				command, known := byIdentifier[selector.Sel.Name]
				if !known {
					continue
				}
				// Every method the arm calls, not the last one: an arm that calls two
				// and reaches the gate through the first would otherwise be read as not
				// reaching it at all. The fence's own holdout caught this version of it.
				handlers[command] = append(handlers[command], calledOnReceiver(clause.Body)...)
			}
			return true
		})
	}
	if len(handlers) == 0 {
		t.Fatal("no `case protocol.CommandX:` routes to a method, so the switch this fence " +
			"walks is not where commands are dispatched any more")
	}
	return handlers
}

// calledOnReceiver returns the methods a case body calls on whatever it is a method of.
func calledOnReceiver(body []ast.Stmt) []string {
	var called []string
	for _, stmt := range body {
		ast.Inspect(stmt, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if selector, isSel := call.Fun.(*ast.SelectorExpr); isSel {
				if ident, isIdent := selector.X.(*ast.Ident); isIdent && len(ident.Name) <= 2 {
					called = append(called, selector.Sel.Name)
				}
			}
			return true
		})
	}
	return called
}

// commandsByIdentifier maps `CommandMessageSend` to the string it is declared as, read out
// of this package's own source so the fence does not carry a second copy of the catalogue.
func commandsByIdentifier(t *testing.T) map[string]protocol.CommandType {
	t.Helper()

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the protocol package: %v", err)
	}
	byIdentifier := map[string]protocol.CommandType{}
	declared := map[string]bool{}
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
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				value, isValue := spec.(*ast.ValueSpec)
				if !isValue || len(value.Values) != 1 {
					continue
				}
				literal, isLiteral := value.Values[0].(*ast.BasicLit)
				if !isLiteral || literal.Kind != token.STRING {
					continue
				}
				for _, ident := range value.Names {
					declared[ident.Name] = true
					byIdentifier[ident.Name] = protocol.CommandType(strings.Trim(literal.Value, `"`))
				}
			}
		}
	}
	// Only the ones the catalogue lists, so a constant that is not a command type cannot
	// wander in through a name that happens to start with Command.
	known := map[string]protocol.CommandType{}
	for _, command := range protocol.AllCommandTypes {
		for identifier, value := range byIdentifier {
			if value == command && strings.HasPrefix(identifier, "Command") {
				known[identifier] = command
			}
		}
	}
	if len(known) != len(protocol.AllCommandTypes) {
		t.Fatalf("read %d command constants out of the source against %d in AllCommandTypes, "+
			"so the catalogue and the declarations have come apart", len(known), len(protocol.AllCommandTypes))
	}
	return known
}

func reaches(calls map[string][]string, from, gate string, seen map[string]bool) bool {
	if from == gate {
		return true
	}
	if seen[from] {
		return false
	}
	seen[from] = true
	for _, called := range calls[from] {
		if reaches(calls, called, gate, seen) {
			return true
		}
	}
	return false
}
