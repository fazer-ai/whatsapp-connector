package protocol_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// Fourteen event types in the contract have nothing in this build that produces them,
// and types.go marks each one. A comment is all that can be written there, and a comment
// is what goes stale: the day somebody wires up `group.updated`, nothing makes them come
// back here and say so, and the catalog then tells a client the opposite of the truth
// about what it can expect to receive. So the marking is checked rather than trusted --
// the test reads the packages that would do the producing and asks which types they
// name.
//
// The value of getting this right is the same one the reserved error codes in errors.go
// have: a client that matches on an event it will never receive writes a branch that
// never runs, and unlike a command answered with `unsupported`, nothing tells it so.
var eventTypesWithNoProducer = []protocol.EventType{
	protocol.EventSessionOfflineSyncPreview,
	protocol.EventSessionOfflineSyncCompleted,
	protocol.EventContactPictureChanged,
	protocol.EventContactIdentityChanged,
	protocol.EventGroupJoined,
	protocol.EventGroupUpdated,
	protocol.EventGroupPictureChanged,
	protocol.EventGroupActivity,
	protocol.EventAccountReachoutTimelock,
	protocol.EventAccountNewChatCap,
	protocol.EventCallOffer,
	protocol.EventCallTerminate,
	protocol.EventHistorySync,
	protocol.EventRaw,
}

func TestEveryEventTypeIsProducedOrMarkedAsNotProduced(t *testing.T) {
	t.Parallel()

	named := eventTypesNamedOutsideThisPackage(t)

	unproduced := make(map[protocol.EventType]bool, len(eventTypesWithNoProducer))
	for _, event := range eventTypesWithNoProducer {
		if !event.Valid() {
			t.Errorf("%s is marked as having no producer and is not an event type in the contract", event)
		}
		unproduced[event] = true
	}

	for _, event := range protocol.AllEventTypes {
		where, produced := named[event]
		switch {
		case unproduced[event] && produced:
			t.Errorf("%s is marked in types.go as having no producer, and %s names it: move it out of the marked group", event, where)
		case !unproduced[event] && !produced:
			t.Errorf("%s is not marked as unproduced and nothing outside internal/protocol names it: either it lost its producer or the marking in types.go is behind", event)
		}
	}
}

// eventTypesNamedOutsideThisPackage reports which event types the production code of the
// other packages mentions, by the constant or by its literal value.
//
// Reading the sources rather than importing them, because internal/protocol is what they
// all import: a test that pulled them in would be a cycle, and one that asked each
// package to declare what it produces would be the same comment written twice.
func eventTypesNamedOutsideThisPackage(t *testing.T) map[protocol.EventType]string {
	t.Helper()

	byLiteral := make(map[string]protocol.EventType, len(protocol.AllEventTypes))
	for _, event := range protocol.AllEventTypes {
		byLiteral[string(event)] = event
	}
	byConstant := eventConstantNames(t)

	named := map[protocol.EventType]string{}
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
					if event, ok := byConstant[found.Sel.Name]; ok {
						named[event] = where
					}
				}
			case *ast.BasicLit:
				if found.Kind == token.STRING {
					if value, unquoteErr := strconv.Unquote(found.Value); unquoteErr == nil {
						if event, ok := byLiteral[value]; ok {
							named[event] = where
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
			t.Fatalf("read the packages that produce events: %v", err)
		}
	}
	if len(named) == 0 {
		t.Fatal("no package names any event type, which means this test read nothing")
	}
	return named
}

// eventConstantNames maps the identifier of each event constant to its value, read from
// the catalog itself so that the two never have to be listed side by side.
func eventConstantNames(t *testing.T) map[string]protocol.EventType {
	t.Helper()

	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "types.go", nil, 0)
	if err != nil {
		t.Fatalf("parse the type catalog: %v", err)
	}
	inContract := make(map[string]bool, len(protocol.AllEventTypes))
	for _, event := range protocol.AllEventTypes {
		inContract[string(event)] = true
	}

	names := map[string]protocol.EventType{}
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.ValueSpec)
		if !ok || len(spec.Names) != 1 || len(spec.Values) != 1 {
			return true
		}
		// By the declared type, not by the value: `chat.presence` is both an event and a
		// command, and reading it off the value alone picks up the command constant too.
		if declared, ok := spec.Type.(*ast.Ident); !ok || declared.Name != "EventType" {
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
		names[spec.Names[0].Name] = protocol.EventType(value)
		return true
	})
	if len(names) != len(protocol.AllEventTypes) {
		t.Fatalf("read %d event constants out of the catalog, want %d", len(names), len(protocol.AllEventTypes))
	}
	return names
}
