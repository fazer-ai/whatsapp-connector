package testwait_test

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Every polling helper in this repository's tests takes its budget from this package.
//
// The fence and not a list, because the failure this closes is not "these two disagree",
// which a list would have caught once: it is that nothing stopped the next one from being
// written with a number of its own. Eight of them existed when #298 was filed, across two
// packages and four values, and the issue named two.
//
// What counts as a polling helper is the shape all eight had, and it is a narrow one: a
// function that takes a `*testing.T`, builds a deadline from `time.Now().Add(...)`, loops
// on `time.Now().Before(...)`, and fails the test when the loop runs out. A test that
// waits on a channel with `time.After` is not one of these -- there are 33 of those in
// `internal/session` alone, with ceilings from 300ms to 5s, and they are waiting for one
// specific thing to arrive rather than sweeping a condition nobody signals.
//
// Some of those deadlines are the assertion rather than patience, and those keep their own
// number with the reason written beside it, as `// testwait: ...` on the line above. Two
// exist: a live media download, which waits on WhatsApp and not on this machine, and a
// test that holds a state past the reclaim delay, where a shorter wait would stop testing
// what it is named after. The marker is what makes that a decision somebody wrote down
// instead of a number nobody questioned, which is the whole failure #298 came from.
func TestEveryPollingHelperTakesItsBudgetFromHere(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	var offenders, pollers []string
	for _, file := range everyTestFile(t, root) {
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, file, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		excused := linesWithAReason(fset, parsed)
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !takesATestingT(fn) {
				continue
			}
			deadline, at, found := deadlineExpr(fn)
			if !found || !loopsOnTheClock(fn) || !failsTheTest(fn) {
				continue
			}
			if excused[fset.Position(at).Line] {
				// A deadline with its own reason keeps its own interval too: what it is
				// pacing is whatever the reason describes, and in the one case here that
				// is how often a status command is sent, not how eagerly a condition is
				// read.
				continue
			}
			if budgeted(deadline) {
				// A helper that takes the budget from here and then polls on an interval
				// of its own has only moved the disagreement one line down. Two of them
				// did exactly that before #298: 1ms in one package's helper and 5ms in
				// the other's, in functions that were otherwise the same code.
				if interval, spelled := pollInterval(fn); spelled && interval != "testwait.Poll" {
					where, _ := filepath.Rel(root, file)
					pollers = append(pollers, where+":"+fn.Name.Name+" sleeps "+interval)
				}
				continue
			}
			where, _ := filepath.Rel(root, file)
			offenders = append(offenders, where+":"+fn.Name.Name+" waits "+deadline)
		}
	}
	if len(pollers) > 0 {
		t.Errorf("these polling helpers sleep on an interval of their own, which is the "+
			"same disagreement as the budget one file down (#298):\n  %s", strings.Join(pollers, "\n  "))
	}
	if len(offenders) > 0 {
		t.Errorf("these polling helpers carry a budget of their own, so the next runner "+
			"that suspends one of them for longer than its author guessed reports a defect "+
			"that is not there (#298):\n  %s", strings.Join(offenders, "\n  "))
	}
}

// pollInterval returns what a helper sleeps between reads, as written.
func pollInterval(fn *ast.FuncDecl) (string, bool) {
	found, expr := false, ""
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || found || render(call.Fun) != "time.Sleep" || len(call.Args) != 1 {
			return true
		}
		found, expr = true, render(call.Args[0])
		return false
	})
	return expr, found
}

// budgeted accepts the budget itself, and a delay added to it.
//
// The sum is for a wait that has to outlast something the scenario configures -- a reclaim
// delay of seven and a half seconds, in the one case that needs it -- where the patience
// half still has to come from the one place. Written as a shape and not as a substring
// check: `0 * testwait.Budget` mentions the budget and is not one.
func budgeted(deadline string) bool {
	if deadline == "testwait.Budget" {
		return true
	}
	before, after, isSum := strings.Cut(deadline, " + ")
	return isSum && (strings.TrimSpace(before) == "testwait.Budget" || strings.TrimSpace(after) == "testwait.Budget")
}

// takesATestingT is the first half of what makes a function a test helper rather than
// production code that happens to poll.
func takesATestingT(fn *ast.FuncDecl) bool {
	if fn.Type.Params == nil {
		return false
	}
	for _, param := range fn.Type.Params.List {
		if render(param.Type) == "*testing.T" {
			return true
		}
	}
	return false
}

// deadlineExpr returns the duration a function builds its deadline from, as written, and
// where it is.
func deadlineExpr(fn *ast.FuncDecl) (string, token.Pos, bool) {
	found, expr, at := false, "", token.NoPos
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || found || render(call.Fun) != "time.Now().Add" || len(call.Args) != 1 {
			return true
		}
		// A negative offset is test data -- a deadline already in the past on a command --
		// and not a budget. `time.Now().Add(-time.Minute)` appears five times in
		// `internal/session` alone.
		if unary, isUnary := call.Args[0].(*ast.UnaryExpr); isUnary && unary.Op == token.SUB {
			return true
		}
		found, expr, at = true, render(call.Args[0]), call.Pos()
		return false
	})
	return expr, at, found
}

// linesWithAReason returns the lines a `// testwait: ...` marker excuses, which is the
// line the marker sits on and the one after it.
//
// The reason has to be there: a bare marker would be the same silence with a comment on
// it. Eight words is not a high bar, and it is higher than nothing.
func linesWithAReason(fset *token.FileSet, file *ast.File) map[int]bool {
	excused := map[int]bool{}
	for _, group := range file.Comments {
		// The whole group and not the marker line: a reason worth writing runs onto the
		// next line, and what the deadline follows is the end of the comment.
		marked, words := false, 0
		for _, comment := range group.List {
			text := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(comment.Text, "//"), "/*"))
			if reason, found := strings.CutPrefix(text, "testwait:"); found {
				marked = true
				words += len(strings.Fields(reason))
				continue
			}
			if marked {
				words += len(strings.Fields(text))
			}
		}
		if !marked || words < 8 {
			continue
		}
		line := fset.Position(group.End()).Line
		excused[line], excused[line+1] = true, true
	}
	return excused
}

func loopsOnTheClock(fn *ast.FuncDecl) bool {
	looping := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		loop, ok := n.(*ast.ForStmt)
		if !ok || loop.Cond == nil {
			return true
		}
		if strings.HasPrefix(render(loop.Cond), "time.Now().Before(") {
			looping = true
		}
		return !looping
	})
	return looping
}

func failsTheTest(fn *ast.FuncDecl) bool {
	failing := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if name := render(call.Fun); strings.HasSuffix(name, ".Fatal") ||
			strings.HasSuffix(name, ".Fatalf") || strings.HasSuffix(name, ".Errorf") {
			failing = true
		}
		return !failing
	})
	return failing
}

// everyTestFile walks the repository rather than one package: the disagreement #298 is
// about spanned two, and a fence that reads one of them would have passed on the day the
// issue was filed.
func everyTestFile(t *testing.T, root string) []string {
	t.Helper()

	var files []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			// Vendored or generated trees are nobody's to fix from here.
			if name := entry.Name(); name == ".git" || name == "vendor" || name == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if len(files) == 0 {
		t.Fatalf("no _test.go under %s, so this fence is measuring nothing", root)
	}
	return files
}

func repoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the working directory, so the fence cannot find the repository")
		}
		dir = parent
	}
}

func render(node ast.Expr) string {
	var out strings.Builder
	if err := printer.Fprint(&out, token.NewFileSet(), node); err != nil {
		return ""
	}
	return out.String()
}
