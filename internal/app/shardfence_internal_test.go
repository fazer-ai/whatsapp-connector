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
// while the connector published to sixteen, four of the eight sids in `app_test.go` fell
// on the disagreeing side, and all three tests that read events happened to hold a sid
// from the agreeing half. (Four of eight is the count for that file, which is where those
// tests live; the package as a whole names twenty-five sids and twelve of them disagree.) Green by luck, and the next test written would have drawn from the same
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
			// Any literal, not only an INT one. `redisx.NewKeys("wa:", 'a')` compiles,
			// because a rune is assignable to `int`, and it builds a layout counting 97
			// streams: measured, it sends this package's `...0002` to `wa:events:83`,
			// and an INT-only check let that through.
			//
			// The compiler takes a literal here on its value, not on its kind: what
			// compiles is any literal whose constant value is representable as an `int`,
			// whichever family it is written in. Measured: `4`, `0x10`, `'a'`, `4.0`,
			// `1e1` and `0i` compile; `4.5`, `4i`, `1e100` and an integer past the word
			// size are all rejected, and rejected for their value. The one family that
			// never compiles here is STRING, whatever it holds. So the INT restriction
			// was not keeping out anything that had a right to be there, which is what
			// makes deleting it the whole fix rather than a widening with a cost.
			//
			// The exception below is a spelling and not a value, so zero written another
			// way is reported: measured, `00`, `0x0` and `0.0` each are, and `0` is not.
			// That is the message this fence wants to send anyway, since it asks for `0`,
			// and every layout here that is built with no count is written that way.
			//
			// What still gets through is not a literal at all. `(4)`, `+4`, `4+0` and
			// `int(4)` carry the count past this check, each measured, because none of
			// them is a `BasicLit`. Closing that means folding constants through
			// `go/types` instead of reading the syntax, and folding does not stop where
			// this fence stops. Measured: it reports each of those four as the constant
			// 4, and it reports `DefaultEventShards` as the constant 16, which is the
			// name this fence exists to make people write and the one most counts here
			// are written as. Erasing the difference between naming the one home and
			// carrying the number is what folding is for, and that difference is the
			// whole rule, so a fence built on it would report the idiom it asks for. The
			// one thing folding still separates is a count read from the fleet, which
			// folds to nothing. So the instrument that closes the hole closes the rule
			// with it, and the hole is a shape no key layout here is built with. The
			// rune was worth closing because the fence was already looking at exactly
			// that node.
			literal, ok := call.Args[at].(*ast.BasicLit)
			if !ok || literal.Value == "0" {
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
