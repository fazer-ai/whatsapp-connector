package redisx_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Four of the key constructors render a name this connector never has to render, and
// keys.go marks them. Everything else has to be reachable from this build's production
// code.
//
// The reason this is a test rather than a comment is what #160 found: a constructor is
// read as a promise that the key exists -- contract/README.md had a row for `wa:sessions`
// and `wa:session:<sid>` describing a session registry no connector ever wrote, and it
// cost a holdout agent a set of acceptance criteria built on it. Four names were in that
// state at once, each findable by grep and each found only when somebody happened to
// build next to it. A comment saying "nothing uses this" goes stale in the direction that
// hurts, because wiring one up is exactly when nobody comes back here to say so.
var keysTheClientRenders = []string{
	// Three keys the client owns outright: this connector neither reads nor writes them.
	"EventsLease",
	"Consumer",
	"Cursor",
	// The reply list is the one the connector writes and does not name: the destination
	// rides on the command as `reply_to`, so what this side needs is IsReply, which
	// checks the name a client chose rather than building one.
	"Reply",
}

// Prefix renders the namespace every key sits under, not a key. It is the one string
// method on Keys that names nothing a client could read.
var notAKey = map[string]bool{"Prefix": true}

func TestEveryKeyIsUsedHereOrMarkedAsTheClientsToWrite(t *testing.T) {
	t.Parallel()

	catalog, calls := readKeyConstructors(t)
	named := keysNamedByProductionCode(t, catalog)

	// A constructor another constructor builds on counts as used: Events is reached only
	// through EventsOf, and being one call deeper than the call site does not make it a
	// name nobody writes.
	reachable := map[string]bool{}
	queue := make([]string, 0, len(named))
	for name := range named {
		queue = append(queue, name)
	}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		if reachable[name] {
			continue
		}
		reachable[name] = true
		queue = append(queue, calls[name]...)
	}

	marked := map[string]bool{}
	for _, name := range keysTheClientRenders {
		if !catalog[name] {
			t.Errorf("%s is marked as the client's to render and is not a key constructor in keys.go", name)
		}
		marked[name] = true
	}

	names := make([]string, 0, len(catalog))
	for name := range catalog {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		switch {
		case marked[name] && reachable[name]:
			t.Errorf("Keys.%s is marked as the client's to render, and %s names it: move it out of the marked group", name, named[name])
		case !marked[name] && !reachable[name]:
			t.Errorf("nothing outside keys.go names Keys.%s and it is not marked as the client's to render: either delete the constructor, or mark it and say in contract/README.md who writes the key", name)
		}
	}
}

// readKeyConstructors returns the exported methods on Keys that render a key, and which
// of them each one calls. Read off the declarations so that adding a constructor puts it
// in the catalog by itself -- a list written by hand here would be the same comment this
// test exists to stop trusting.
func readKeyConstructors(t *testing.T) (catalog map[string]bool, calls map[string][]string) {
	t.Helper()

	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "keys.go", nil, 0)
	if err != nil {
		t.Fatalf("parse keys.go: %v", err)
	}

	catalog = map[string]bool{}
	bodies := map[string]*ast.FuncDecl{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Name == nil || !fn.Name.IsExported() || notAKey[fn.Name.Name] {
			continue
		}
		if receiverTypeName(fn.Recv) != "Keys" {
			continue
		}
		results := fn.Type.Results
		if results == nil || len(results.List) != 1 {
			continue
		}
		// A key is a string. Shards and ShardOf answer questions about the layout, and
		// IsReply answers one about a key somebody else chose.
		if named, ok := results.List[0].Type.(*ast.Ident); !ok || named.Name != "string" {
			continue
		}
		catalog[fn.Name.Name] = true
		bodies[fn.Name.Name] = fn
	}
	if len(catalog) == 0 {
		t.Fatal("read no key constructors out of keys.go, which means this test read nothing")
	}

	calls = map[string][]string{}
	for name, fn := range bodies {
		receiver := receiverName(fn.Recv)
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if ident, ok := selector.X.(*ast.Ident); ok && ident.Name == receiver && catalog[selector.Sel.Name] {
				calls[name] = append(calls[name], selector.Sel.Name)
			}
			return true
		})
	}
	return catalog, calls
}

// keysNamedByProductionCode reports which constructors this build calls, and where.
//
// A call counts when the receiver is a key set: the value is spelled `keys` where it is
// held in a field or a variable, and `<something>.Keys()` where it is asked of the client.
// Matching on the method name alone would count `manager.Resume(sid)` as a use of
// `Keys.Resume` and report a key as written when nothing writes it, which is the exact
// lie this test exists to catch. A third spelling fails the test rather than passing it
// silently: the constructor reads as unnamed, and the message says to teach this.
func keysNamedByProductionCode(t *testing.T, catalog map[string]bool) map[string]string {
	t.Helper()

	named := map[string]string{}
	root := filepath.Join("..", "..")
	fileSet := token.NewFileSet()
	walk := func(path string, entry fs.DirEntry, err error) error {
		switch {
		case err != nil:
			return err
		case entry.IsDir():
			return nil
		case !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go"):
			return nil
		case filepath.Base(path) == "keys.go":
			// The declarations themselves, where every constructor is named.
			return nil
		}

		file, parseErr := parser.ParseFile(fileSet, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		where, _ := filepath.Rel(root, path)
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !catalog[selector.Sel.Name] || !isKeySet(selector.X) {
				return true
			}
			named[selector.Sel.Name] = where
			return true
		})
		return nil
	}
	for _, dir := range []string{"internal", "cmd"} {
		if err := filepath.WalkDir(filepath.Join(root, dir), walk); err != nil {
			t.Fatalf("read the packages that name keys: %v", err)
		}
	}
	if len(named) == 0 {
		t.Fatal("no package names any key constructor, which means this test read nothing")
	}
	return named
}

// isKeySet reports whether an expression is a key set: `keys`, anything ending in
// `.keys`, or a call to a `Keys()` accessor.
func isKeySet(expr ast.Expr) bool {
	switch found := expr.(type) {
	case *ast.Ident:
		return found.Name == "keys"
	case *ast.SelectorExpr:
		return found.Sel.Name == "keys"
	case *ast.CallExpr:
		switch fn := found.Fun.(type) {
		case *ast.Ident:
			return fn.Name == "Keys"
		case *ast.SelectorExpr:
			return fn.Sel.Name == "Keys"
		}
	}
	return false
}

func receiverTypeName(recv *ast.FieldList) string {
	if recv == nil || len(recv.List) != 1 {
		return ""
	}
	switch found := recv.List[0].Type.(type) {
	case *ast.Ident:
		return found.Name
	case *ast.StarExpr:
		if ident, ok := found.X.(*ast.Ident); ok {
			return ident.Name
		}
	}
	return ""
}

func receiverName(recv *ast.FieldList) string {
	if recv == nil || len(recv.List) != 1 || len(recv.List[0].Names) != 1 {
		return ""
	}
	return recv.List[0].Names[0].Name
}
