package protocol_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// Eleven event types in the contract have nothing in this build that produces them,
// and types.go marks each one. A comment is all that can be written there, and a comment
// is what goes stale: the day somebody wires up `group.updated`, nothing makes them come
// back here and say so, and the catalog then tells a client the opposite of the truth
// about what it can expect to receive. So the marking is checked rather than trusted --
// the test reads the packages that would do the producing and asks which types they
// name. Which is what happened: `group.joined`, `group.updated` and `group.activity`
// left this list because the session's event handler started publishing them, and the
// test is what said so.
//
// The value of getting this right is the same one the reserved error codes in errors.go
// have: a client that matches on an event it will never receive writes a branch that
// never runs, and unlike a command answered with `unsupported`, nothing tells it so.
var eventTypesWithNoProducer = []protocol.EventType{
	protocol.EventSessionOfflineSyncPreview,
	protocol.EventSessionOfflineSyncCompleted,
	protocol.EventContactPictureChanged,
	protocol.EventContactIdentityChanged,
	protocol.EventGroupPictureChanged,
	protocol.EventAccountReachoutTimelock,
	protocol.EventAccountNewChatCap,
	protocol.EventCallOffer,
	protocol.EventCallTerminate,
	protocol.EventHistorySync,
	protocol.EventRaw,
}

func TestEveryEventTypeIsProducedOrMarkedAsNotProduced(t *testing.T) {
	t.Parallel()

	catalog := make([]string, len(protocol.AllEventTypes))
	for i, event := range protocol.AllEventTypes {
		catalog[i] = string(event)
	}
	marked := make([]string, len(eventTypesWithNoProducer))
	for i, event := range eventTypesWithNoProducer {
		marked[i] = string(event)
	}

	assertProducers(t, "EventType", "types.go", "producer", catalog, marked)
}

// Four command types are in the contract with nothing in this build that carries them
// out, and types.go marks each one. A client that sends one is answered `unsupported`,
// so unlike an unproduced event this is told at the time -- but only to a client that
// already sent it, and only for a session some instance owns. A command for a session
// nobody is running is answered by nobody at all (#151), so the caller waits out its own
// deadline and learns nothing about why.
var commandTypesWithNoHandler = []protocol.CommandType{
	protocol.CommandSessionUpdate,
	protocol.CommandHistoryRequest,
	protocol.CommandContactInfo,
	protocol.CommandCallReject,
}

func TestEveryCommandTypeIsHandledOrMarkedAsNotHandled(t *testing.T) {
	t.Parallel()

	catalog := make([]string, len(protocol.AllCommandTypes))
	for i, command := range protocol.AllCommandTypes {
		catalog[i] = string(command)
	}
	marked := make([]string, len(commandTypesWithNoHandler))
	for i, command := range commandTypesWithNoHandler {
		marked[i] = string(command)
	}

	assertProducers(t, "CommandType", "types.go", "handler", catalog, marked)
}

// Three error codes are declared and never sent, and errors.go marks each one with what
// reaches a client in its place. The same drift is possible there as with the events,
// and worse to read from the outside: a client branching on a code it cannot receive
// gets no signal at all, which is the reasoning errors.go and contract/README.md both
// already spell out. This is what keeps that marking honest.
var errorCodesWithNoProducer = []protocol.ErrorCode{
	protocol.ErrorSessionNotFound,
	protocol.ErrorQuarantined,
	protocol.ErrorClientOutdated,
}

func TestEveryErrorCodeIsProducedOrMarkedAsNotProduced(t *testing.T) {
	t.Parallel()

	catalog := make([]string, len(protocol.AllErrorCodes))
	for i, code := range protocol.AllErrorCodes {
		catalog[i] = string(code)
	}
	marked := make([]string, len(errorCodesWithNoProducer))
	for i, code := range errorCodesWithNoProducer {
		marked[i] = string(code)
	}

	assertProducers(t, "ErrorCode", "errors.go", "producer", catalog, marked)
}

// assertProducers is the check all three catalogs get: every value is either named
// somewhere outside this package or marked as having nothing behind it, and never both.
// `role` is what the naming would have been -- a producer for the events and the error
// codes, a handler for the commands -- and only shapes the failures.
func assertProducers(t *testing.T, declaredType, catalogFile, role string, catalog, marked []string) {
	t.Helper()

	named := namedOutsideThisPackage(t, declaredType, catalog)

	inCatalog := make(map[string]bool, len(catalog))
	for _, value := range catalog {
		inCatalog[value] = true
	}
	unproduced := make(map[string]bool, len(marked))
	for _, value := range marked {
		if !inCatalog[value] {
			t.Errorf("%s is marked as having no %s and is not in the %s catalog", value, role, declaredType)
		}
		unproduced[value] = true
	}

	for _, value := range catalog {
		where, produced := named[value]
		switch {
		case unproduced[value] && produced:
			t.Errorf("%s is marked in %s as having no %s, and %s names it: move it out of the marked group", value, catalogFile, role, where)
		case !unproduced[value] && !produced:
			t.Errorf("%s is not marked and nothing outside internal/protocol names it: either it lost its %s or the marking in %s is behind", value, role, catalogFile)
		}
	}
}

// namedOutsideThisPackage reports which of one catalog's values the production code of
// the other packages mentions, and in which file.
//
// Reading the sources rather than importing them, because internal/protocol is what they
// all import: a test that pulled them in would be a cycle, and one that asked each
// package to declare what it produces would be the same comment written twice.
//
// A value counts as named when a package selects its constant, and -- only where the
// value cannot be an ordinary English word -- when a package writes the value out. The
// literal is a net for a producer that skips the constant, and it is cast no wider than
// that on purpose: `raw` and `internal` are both catalog values, and a test that took
// any string spelling one as a producer would report a producer for whatever a comment,
// a log key or a struct tag happens to say.
func namedOutsideThisPackage(t *testing.T, declaredType string, values []string) map[string]string {
	t.Helper()

	byLiteral := map[string]string{}
	for _, value := range values {
		// A dot is what makes a catalog value unmistakable: every event and command type
		// is qualified (`group.updated`), and no error code is.
		if strings.Contains(value, ".") {
			byLiteral[value] = value
		}
	}
	byConstant := constantNames(t, declaredType, values)

	named := map[string]string{}
	root := filepath.Join("..", "..")
	fileSet := token.NewFileSet()
	walk := func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		switch {
		case entry.IsDir():
			// The contract's own package is where the catalog lives, so every type is
			// named in it. Only the packages that could publish one count here.
			if entry.Name() == "protocol" {
				return fs.SkipDir
			}
			return nil
		case !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go"):
			return nil
		}

		// Comments left out: a type named in prose is not a type anybody produces, and
		// the marking in types.go is prose about exactly these types.
		file, parseErr := parser.ParseFile(fileSet, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		where, _ := filepath.Rel(root, path)
		ast.Inspect(file, func(node ast.Node) bool {
			switch found := node.(type) {
			case *ast.SelectorExpr:
				if pkg, ok := found.X.(*ast.Ident); ok && pkg.Name == "protocol" {
					if value, ok := byConstant[found.Sel.Name]; ok {
						named[value] = where
					}
				}
			case *ast.BasicLit:
				if found.Kind == token.STRING {
					if value, unquoteErr := strconv.Unquote(found.Value); unquoteErr == nil {
						if spelled, ok := byLiteral[value]; ok {
							named[spelled] = where
						}
					}
				}
			}
			return true
		})
		return nil
	}
	// The two directories that hold this build's code. Naming them rather than walking
	// the repository keeps the contract's own fixtures, the tooling and .git out of it.
	for _, dir := range []string{"internal", "cmd"} {
		if err := filepath.WalkDir(filepath.Join(root, dir), walk); err != nil {
			t.Fatalf("read the packages that produce %s values: %v", declaredType, err)
		}
	}
	if len(named) == 0 {
		t.Fatalf("no package names any %s, which means this test read nothing", declaredType)
	}
	return named
}

// constantNames maps the identifier of each of a catalog's constants to its value, read
// from the declarations themselves so that the two never have to be listed side by side.
func constantNames(t *testing.T, declaredType string, values []string) map[string]string {
	t.Helper()

	// This package's own sources, read one by one: the catalogs are spread over more than
	// one file (types.go and errors.go), and naming them here would be one more pair that
	// has to be kept in step.
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("list this package's sources: %v", err)
	}
	fileSet := token.NewFileSet()
	var sources []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fileSet, name, nil, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", name, parseErr)
		}
		sources = append(sources, file)
	}

	inContract := make(map[string]bool, len(values))
	for _, value := range values {
		inContract[value] = true
	}

	names := map[string]string{}
	for _, source := range sources {
		ast.Inspect(source, func(node ast.Node) bool {
			spec, ok := node.(*ast.ValueSpec)
			if !ok || len(spec.Names) != 1 || len(spec.Values) != 1 {
				return true
			}
			// By the declared type, not by the value: `chat.presence` is both an event
			// and a command, and reading it off the value alone picks up the other one.
			if declared, ok := spec.Type.(*ast.Ident); !ok || declared.Name != declaredType {
				return true
			}
			literal, ok := spec.Values[0].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil || !inContract[value] {
				return true
			}
			names[spec.Names[0].Name] = value
			return true
		})
	}
	if len(names) != len(values) {
		t.Fatalf("read %d %s constants out of the catalog, want %d", len(names), declaredType, len(values))
	}
	return names
}
