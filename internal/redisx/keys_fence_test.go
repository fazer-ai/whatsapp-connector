package redisx_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Four of the key constructors render a name this connector never has to render, and
// keys.go marks them. Everything else has to be reachable from this build's production
// code.
//
// The reason this is a test rather than a comment is what #160 found: a constructor is
// read as a promise that the key exists -- contract/PROTOCOL.md had a row for `wa:sessions`
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
			t.Errorf("nothing outside keys.go names Keys.%s and it is not marked as the client's to render: either delete the constructor, or mark it and say in contract/PROTOCOL.md who writes the key", name)
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

// Keys documented nowhere on purpose. Each one carries the reason here, because a bare
// list is the comment this file exists to stop trusting, and the next person to add a
// constructor needs to know which of the two exits applies to it.
var keysTheContractDoesNotName = map[string]string{}

// The other direction, and the one #248 found open: a key the connector renders and the
// contract's tables never name.
//
// #160 fenced constructors against production code, which catches a row describing a key
// nothing writes -- `wa:sessions` described a registry no connector ever built. Nothing
// caught the reverse, and `wa:handback:<sid>` sat in five production call sites in
// `internal/cluster/lease.go` with no row anywhere, while the paragraph nearest to it said
// a `wa:handoff:<sid>` key "was declared for it once and removed here, having never had
// anything behind it" -- true of handoff, and read as settling handback too.
//
// Matched by shape rather than by name. Every segment a caller supplies becomes `*` on
// both sides, so the fence compares `wa:idem:*:*` with the row's `wa:idem:<sid>:<key>` and
// does not care what the contract calls the argument or what Go calls the parameter.
// Matched against table rows only, never the prose: a fence satisfied by a mention would
// let the row itself -- owner, type, lifetime and meaning -- be deleted with the suite
// green, which is exactly the deletion that goes unnoticed today.
//
// Both tables count. The contract has one for the streams and one for the keys around
// them, and a fence that demanded the key table would turn red on `wa:cmd:<sid>`,
// `wa:control`, `wa:events:<shard>` and `wa:reply:<command_id>`, which are documented in
// the right place already. A fence that pushes five correct rows into the wrong table is
// worse than no fence: it cements the mistake and stands in the way of whoever fixes it.
func TestEveryKeyTheConnectorRendersHasARowInTheContract(t *testing.T) {
	t.Parallel()

	catalog, _ := readKeyConstructors(t)
	shapes := keyShapes(t, catalog)
	documented := shapesNamedByContractTables(t)

	names := make([]string, 0, len(catalog))
	for name := range catalog {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		reason, excused := keysTheContractDoesNotName[name]
		shape, known := shapes[name]
		switch {
		case excused && reason == "":
			t.Errorf("Keys.%s is excused from the contract's tables with no reason written beside it", name)
		case excused && documented[shape]:
			t.Errorf("Keys.%s is excused from the contract's tables and %s is in one of them: drop the exception", name, shape)
		case excused:
		case !known:
			t.Errorf("could not work out the key Keys.%s renders, so this fence cannot say whether the contract names it; teach keyShapes the shape or excuse the constructor with a reason", name)
		case !documented[shape]:
			t.Errorf("Keys.%s renders %s and no row of contract/PROTOCOL.md names it: either add the row, with owner, type, lifetime and meaning, or add it to keysTheContractDoesNotName with the reason it is not the client's business", name, shape)
		}
	}

	for _, name := range keysTheClientRenders {
		if _, ok := keysTheContractDoesNotName[name]; ok {
			t.Errorf("Keys.%s is marked as the client's to render and excused from the contract at the same time: a key a client writes is one both sides have to agree on", name)
		}
	}
}

// keyShapes renders each constructor's key with every caller-supplied segment collapsed to
// `*`, read off the declarations for the same reason the catalog is: a shape written by
// hand here would go stale exactly when somebody changes a constructor.
//
// A constructor this cannot work out is reported rather than skipped. Silence would turn
// the fence off for the one constructor whose body somebody just made interesting, which
// is the shape every fence in this repository is written to avoid.
func keyShapes(t *testing.T, catalog map[string]bool) map[string]string {
	t.Helper()

	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "keys.go", nil, 0)
	if err != nil {
		t.Fatalf("parse keys.go: %v", err)
	}

	bodies := map[string]*ast.FuncDecl{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Recv != nil && fn.Name != nil && catalog[fn.Name.Name] {
			bodies[fn.Name.Name] = fn
		}
	}

	shapes := map[string]string{}
	var shapeOf func(name string, depth int) (string, bool)
	shapeOf = func(name string, depth int) (string, bool) {
		if shape, done := shapes[name]; done {
			return shape, shape != ""
		}
		// One constructor builds on another (EventsOf on Events, EventsLease on Events),
		// and the chain is short. The bound is here so a cycle is a failed shape rather
		// than a hung test.
		if depth > 4 {
			return "", false
		}
		fn := bodies[name]
		if fn == nil || fn.Body == nil || len(fn.Body.List) != 1 {
			return "", false
		}
		ret, ok := fn.Body.List[0].(*ast.ReturnStmt)
		if !ok || len(ret.Results) != 1 {
			return "", false
		}
		var render func(ast.Expr) (string, bool)
		render = func(expr ast.Expr) (string, bool) {
			switch node := expr.(type) {
			case *ast.BinaryExpr:
				left, okL := render(node.X)
				right, okR := render(node.Y)
				return left + right, okL && okR
			case *ast.BasicLit:
				text, err := strconv.Unquote(node.Value)
				return text, err == nil
			case *ast.SelectorExpr:
				// `k.prefix` is the namespace the contract spells out in every row.
				if ident, ok := node.X.(*ast.Ident); ok && ident.Name == "k" && node.Sel.Name == "prefix" {
					return "wa:", true
				}
				return "", false
			case *ast.Ident:
				// A parameter, which is whatever the caller passes.
				return "*", true
			case *ast.CallExpr:
				// Either another constructor on Keys, or a conversion of a parameter
				// (`strconv.Itoa(shard)`); both stand for one caller-supplied segment,
				// except the constructor, which contributes its whole shape.
				if sel, ok := node.Fun.(*ast.SelectorExpr); ok {
					if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "k" && catalog[sel.Sel.Name] {
						return shapeOf(sel.Sel.Name, depth+1)
					}
				}
				return "*", true
			}
			return "", false
		}
		shape, ok := render(ret.Results[0])
		if !ok {
			shapes[name] = ""
			return "", false
		}
		shapes[name] = shape
		return shape, true
	}
	for name := range catalog {
		shapeOf(name, 0)
	}
	for name, shape := range shapes {
		if shape == "" {
			delete(shapes, name)
		}
	}
	return shapes
}

// shapesNamedByContractTables reads the first cell of every markdown table row in the
// contract and returns the shapes of the keys named there.
//
// Table rows only. A key named in a sentence is a key somebody mentioned; a key in a row
// has an owner, a type, a lifetime and a meaning beside it, and it is the row this fence
// is about. Both tables are read, because the streams are documented in one and the keys
// around them in the other, and both are the right place for what they hold.
func shapesNamedByContractTables(t *testing.T) map[string]bool {
	t.Helper()

	source, err := os.ReadFile(filepath.Join("..", "..", "contract", "PROTOCOL.md"))
	if err != nil {
		t.Fatalf("read the contract: %v", err)
	}
	named := map[string]bool{}
	for _, line := range strings.Split(string(source), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") {
			continue
		}
		cell, _, _ := strings.Cut(strings.TrimPrefix(line, "|"), "|")
		for _, quoted := range codeSpans(cell) {
			if strings.HasPrefix(quoted, "wa:") {
				named[placeholdersCollapsed(quoted)] = true
			}
		}
	}
	if len(named) == 0 {
		t.Fatal("no key is named in any table of contract/PROTOCOL.md, so this fence would pass for every constructor at once")
	}
	return named
}

// codeSpans returns the contents of each backticked span in a line.
func codeSpans(line string) []string {
	var spans []string
	for {
		start := strings.Index(line, "`")
		if start < 0 {
			return spans
		}
		rest := line[start+1:]
		end := strings.Index(rest, "`")
		if end < 0 {
			return spans
		}
		spans = append(spans, rest[:end])
		line = rest[end+1:]
	}
}

// placeholdersCollapsed rewrites `<sid>` and friends to `*`, so a row matches whatever the
// constructor calls its parameters.
func placeholdersCollapsed(key string) string {
	var out strings.Builder
	for {
		start := strings.Index(key, "<")
		if start < 0 {
			out.WriteString(key)
			return out.String()
		}
		end := strings.Index(key[start:], ">")
		if end < 0 {
			out.WriteString(key)
			return out.String()
		}
		out.WriteString(key[:start])
		out.WriteString("*")
		key = key[start+end+1:]
	}
}

// The row is fenced above; the sentence beside it is not, and it is half of what #248 was
// about. A row says what a key is. This paragraph says what it is *not* -- that the key
// nothing was ever written behind is `wa:handoff:<sid>`, the giving-up on demand, and not
// `wa:handback:<sid>`, the giving-up an owner starts itself. Reading the first as settling
// the second is the mistake the issue found, and it is the mistake this sentence removes.
//
// Deleting it leaves every test in this repository green, which is how it drifted in the
// first place. Fenced in the shape internal/session already uses for the contract's
// sentences, matched against the prose with its line breaks flattened, because the file is
// wrapped by hand and a fence a re-wrap turns red is a fence somebody deletes.
func TestTheContractKeepsHandoffAndHandbackApart(t *testing.T) {
	t.Parallel()

	source, err := os.ReadFile(filepath.Join("..", "..", "contract", "PROTOCOL.md"))
	if err != nil {
		t.Fatalf("read the contract: %v", err)
	}
	prose := strings.Join(strings.Fields(string(source)), " ")
	for _, phrase := range []string{
		"An owner giving a session up **of its own accord** is a different thing and does exist",
		"it is `wa:handback:<sid>` in the table below",
	} {
		if !strings.Contains(prose, phrase) {
			t.Errorf("contract/PROTOCOL.md no longer says %q; without it the paragraph reads as though the key that was removed settled the one that exists, which is what #248 found", phrase)
		}
	}
}
