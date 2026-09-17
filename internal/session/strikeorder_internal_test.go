package session

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// The strike is recorded before the account goes back, at both places that give one up,
// and swapping the two is invisible to every other test in this package.
//
// That is what this fence is for. Both orders end with the same two facts -- a quarantine
// key written, a lease released -- so nothing about the outcome tells them apart. What
// they do not share is the window in between: release the lease first and a peer's resume
// sweep can find the account free with nothing yet saying to leave it alone, and start the
// attempt this instance just failed. A mutation that swapped the two lines survived the
// whole suite, which is how this test came to exist.
//
// Source order rather than behaviour, said out loud because a fence that measures the
// wrong thing is worse than none: this cannot see an ordering broken by anything other
// than moving the calls -- a release moved into a goroutine, or a strike made
// asynchronous, would read as correct here. It holds the shape the two sites are written
// in today, which is what a later round tidying them would change.
func TestTheStrikeIsRecordedBeforeTheAccountGoesBack(t *testing.T) {
	t.Parallel()

	// The two places an account this instance could not keep is given up, and the call
	// each one hands it back with.
	//
	// `adopt` and not `Adopt`: #249 split the exported name into a wrapper that only
	// drops the "did this call open it" flag, and the body that does the work kept the
	// lowercase name. The fence went red on that split, which is the behaviour asked of
	// it -- a body it cannot find is a fence holding nothing -- and the last loop below is
	// what turned a silent pass into a failure.
	sites := map[string]string{"adopt": "abandon", "SweepRetired": "releaseThis"}

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
		handBack, watched := sites[fn.Name.Name]
		if !watched {
			continue
		}
		delete(sites, fn.Name.Name)

		var struck, released token.Pos
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch selector.Sel.Name {
			case "failing":
				if !struck.IsValid() {
					struck = call.Pos()
				}
			case handBack:
				if !released.IsValid() {
					released = call.Pos()
				}
			}
			return true
		})

		switch {
		case !struck.IsValid():
			t.Errorf("%s gives an account up without recording the failure; the fleet's backoff never starts for it", fn.Name.Name)
		case !released.IsValid():
			t.Errorf("%s no longer hands the account back through %s; this fence is reading the wrong call", fn.Name.Name, handBack)
		case struck > released:
			t.Errorf("%s hands the account back before recording the failure; a peer's resume sweep can take it with nothing yet saying to leave it alone", fn.Name.Name)
		}
	}

	for name := range sites {
		t.Errorf("%s is gone from manager.go; the ordering this fence holds may have gone with it", name)
	}
}
