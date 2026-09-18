package app

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// How many event streams the fleet publishes to is a fleet-wide fact with exactly one
// home, `WAC_EVENT_SHARDS` and the `DefaultEventShards` behind it, and this fence is
// what stops a second copy of it appearing in this package.
//
// A second copy does not fail the way a wrong constant usually does. `ShardOf` is a hash
// modulo the count, so two counts agree for some sids and disagree for others: the side
// holding the stale copy reads a stream nobody wrote to and gets an empty list, with no
// error and no warning. That is what #269 was: the test client counted eight streams
// while the connector published to sixteen, four of the package's eight sids fell on the
// disagreeing side, and all three tests that read events happened to hold a sid from the
// agreeing half. Green by luck, and the next test written would have drawn from the same
// hat.
//
// So the rule is not "use the right number", it is "do not carry a number at all". A key
// layout in a test names command streams, the control stream, reply lists and `wa:meta`,
// none of which depend on the count, and it is built with zero for exactly that reason:
// taking a shard from it divides by zero and stops, rather than answering with a number
// this side invented. The one place that really needs the count asks the fleet for it.
//
// Derived values are allowed and are the point of the exception: a test that deliberately
// runs an instance at a different count, to prove the fleet refuses it, writes that as
// `app.DefaultEventShards*2` and stays tied to the one home.
//
// Two spellings build a key layout here and the fence covers both. `redisx.NewKeys` is
// the one the external tests use; `redisx.Wrap` is the one the internal tests use, and
// twelve of them carried a literal 8 while the connector they exercise runs on sixteen.
// None of the twelve could fail the way #269 failed, because the same wrapped client both
// publishes and reads in those tests, so there is no second party to disagree with. They
// are still a literal count sitting in a test file, which is the one shape that made the
// defect invisible, so they name `DefaultEventShards` and the fence holds them to it.
func TestNoTestInThisPackageCarriesItsOwnShardCount(t *testing.T) {
	t.Parallel()

	// Every test file in the directory, not one file: a fence that parses a single file
	// is green for every copy somebody adds next to it, which is the shape the defect
	// took. Walked by hand rather than through parser.ParseDir, which is deprecated.
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read this package's directory: %v", err)
	}

	fset := token.NewFileSet()
	checked := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, entry.Name(), nil, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", entry.Name(), parseErr)
		}
		checked++
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			at := shardCountArg(call)
			if at < 0 {
				return true
			}
			literal, ok := call.Args[at].(*ast.BasicLit)
			if !ok || literal.Kind != token.INT || literal.Value == "0" {
				return true
			}
			t.Errorf("%s:%d builds a key layout carrying its own shard count of %s.\n"+
				"That is a second copy of a fleet-wide number, and the way it fails is "+
				"silent: it agrees with the fleet for some sids and sends the others to "+
				"a stream nobody wrote to, which reads as an empty list rather than an "+
				"error. Build the layout with 0 for names that do not depend on the "+
				"count, and ask the fleet (`wa:meta`, field `event_shards`) where a "+
				"shard is really needed. See #269.",
				entry.Name(), fset.Position(literal.Pos()).Line, literal.Value)
			return true
		})
	}

	// A fence that walked nothing passes for the wrong reason, and this one walks a
	// directory listing, which is the kind of thing that quietly comes back empty.
	if checked == 0 {
		t.Fatal("no test file was parsed, so this fence proved nothing about any of them")
	}
}

// shardCountArg is which argument of a call carries the fleet's stream count, or -1 when
// the call is not one that carries it at all.
func shardCountArg(call *ast.CallExpr) int {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return -1
	}
	pkg, ok := selector.X.(*ast.Ident)
	if !ok || pkg.Name != "redisx" {
		return -1
	}
	// The arity is checked along with the name so that a future overload, or a different
	// `redisx` function of the same name, does not have its second argument read as a
	// shard count on the strength of the name alone.
	switch {
	case selector.Sel.Name == "NewKeys" && len(call.Args) == 2:
		return 1
	case selector.Sel.Name == "Wrap" && len(call.Args) == 3:
		return 2
	}
	return -1
}
