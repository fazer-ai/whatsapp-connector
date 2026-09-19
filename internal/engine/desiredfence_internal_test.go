package engine

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// What a client asked for is recorded above the engines, and this is what keeps it there.
//
// It used to be recorded inside one of them. `engine.Engine` has two implementations and
// only whatsmeow wrote the row, so an account running on anything else paired, published,
// answered commands, and left nothing anywhere saying it should be in the air. The sweep
// that brings accounts back read an empty list and every test stayed green, because every
// test that asserted the row happened to run the engine that wrote it. That was #266.
//
// The obligation is not expressible in `engine.Session`: it is six methods and none of
// them mentions a store, so nothing in the type system carries "and also record the
// request". A fence is what carries it, and a fence has to say what it quantifies over.
//
// The dimension, in writing: every Go file under `internal/engine/`, at any depth, that
// does not end in `_test.go`. Production code of any engine, in other words, and not the
// doubles a test builds -- a double that records nothing harms nobody, and one that
// recorded would be writing rows for a session nothing owns.
//
// Deleting is not writing, and that is deliberate rather than an oversight. An engine
// still forgets the request when it forgets the credentials: a logout, a delete, and the
// revocation WhatsApp sends arrive at the engine, and the last one is an event from the
// library that no layer above hears without reacting to `session.logged_out`. Which layer
// ends a session's record is a question this fence does not answer; what it answers is
// that no engine invents one.
func TestNoEngineRecordsWhatTheClientAsked(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	checked := 0
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		checked++
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !strings.HasPrefix(selector.Sel.Name, "PutDesired") {
				return true
			}
			t.Errorf("%s:%d calls %s, which records what a client asked for.\n"+
				"That belongs above the engines, in `internal/session`, and an engine that "+
				"writes it is #266: the row is the client's request rather than a fact about "+
				"WhatsApp, one engine writing it leaves every other account with nothing for "+
				"the sweep to read, and the way it fails is an empty list rather than an "+
				"error. The session layer writes it before it calls Connect.",
				path, fset.Position(call.Pos()).Line, selector.Sel.Name)
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk the engine packages: %v", err)
	}

	// A walk that found nothing passes for the wrong reason, and this one walks a
	// directory tree, which is the kind of thing that quietly comes back empty.
	if checked == 0 {
		t.Fatal("no engine file was parsed, so this fence proved nothing about any of them")
	}
}
