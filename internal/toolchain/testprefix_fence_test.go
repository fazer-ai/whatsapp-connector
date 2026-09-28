package toolchain_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	repoRoot = "../.."
	// The one place a test's keys on a real Redis get their prefix, and the word that marks
	// a key as a test's. Spelled in two pieces so that this file holds no literal of it and
	// does not find itself.
	prefixHelper = "internal/redisx/redisxtest/prefix.go"
	testKeyMark  = "wactest" + ":"
)

// A test's keys on a real Redis are set apart by a prefix, and the prefix comes from one
// helper. Six tests used to spell it themselves, from the test's name and the clock, and
// the clock does not set two processes apart: on this machine time.Now has microsecond
// resolution, two processes running the same test in the same microsecond against the same
// database shared their keys, and a test with nothing wrong in it came out red (#345).
//
// Read as syntax, every string literal in every Go file of the repository, so a seventh copy
// is found whatever it builds the rest from: the clock, a counter, a Sprintf.
func TestEveryTestKeyPrefixComesFromTheHelper(t *testing.T) {
	t.Parallel()

	var inHelper bool
	var copies []string
	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); name == ".git" || name == "node_modules" || name == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Errorf("parse %s: %v", rel, err)
			return nil
		}
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(lit.Value)
			if err != nil || !strings.Contains(value, testKeyMark) {
				return true
			}
			if filepath.ToSlash(rel) == prefixHelper {
				inHelper = true
			} else {
				copies = append(copies, filepath.ToSlash(rel))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", repoRoot, err)
	}
	for _, path := range copies {
		t.Errorf("%s builds a test key prefix of its own (a %q literal):\n"+
			"\ttake it from redisxtest.Prefix, which two processes running the same test at the same moment cannot share",
			path, testKeyMark)
	}
	if !inHelper {
		t.Errorf("%s does not spell %q: the helper moved or lost it, and this reads nothing", prefixHelper, testKeyMark)
	}
}
