package app_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// The transport makes one diagnosis nobody else can: a `MAXLEN` trim that took commands
// before the consumer group was handed them, which is visible for one moment and only to
// the process that reads the stream next. It says so through the logger on its options,
// and a zero `zerolog.Logger` discards silently -- so leaving that field out does not
// fail to build, does not fail a test, and does not fail in production either. It just
// never says anything, which is the exact state the report exists to end.
//
// This is the only place that can catch it. Every test that boots the connector runs
// against miniredis, which answers zero for the counters the report is built on, so the
// report is correctly silent there whether the logger was wired or not.
func TestTheTransportIsBuiltWithSomewhereToReport(t *testing.T) {
	t.Parallel()

	file, err := parser.ParseFile(token.NewFileSet(), "run.go", nil, 0)
	if err != nil {
		t.Fatalf("parse run.go: %v", err)
	}

	var built, withLogger int
	ast.Inspect(file, func(node ast.Node) bool {
		lit, ok := node.(*ast.CompositeLit)
		if !ok {
			return true
		}
		selector, ok := lit.Type.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "Options" {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok || pkg.Name != "redisstream" {
			return true
		}
		built++
		for _, field := range lit.Elts {
			if pair, ok := field.(*ast.KeyValueExpr); ok {
				if key, ok := pair.Key.(*ast.Ident); ok && key.Name == "Logger" {
					withLogger++
				}
			}
		}
		return true
	})

	if built == 0 {
		t.Fatal("no redisstream.Options is built in run.go; this sweep is passing over nothing")
	}
	if withLogger != built {
		t.Fatalf("%d of %d redisstream.Options in run.go name a Logger; the ones that do not "+
			"discard the trim report instead of writing it", withLogger, built)
	}
}
