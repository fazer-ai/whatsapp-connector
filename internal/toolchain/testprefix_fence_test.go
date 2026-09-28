package toolchain_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
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

	inHelper, copies, err := keyPrefixCopies(repoRoot)
	if err != nil {
		t.Fatalf("read %s: %v", repoRoot, err)
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

// The fence above only ever sees a tree with no copy in it, which leaves the half that tells
// a copy from the helper unexercised: a reader that took every file for the helper would pass
// it. So it is run once here over a tree built for it, with the helper and one copy.
func TestTheKeyPrefixFenceTellsACopyFromTheHelper(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	write := func(rel, body string) {
		t.Helper()
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
	mark := strconv.Quote(testKeyMark)
	write(prefixHelper, "package redisxtest\n\nconst p = "+mark+"\n")
	write("internal/store/copy_test.go", "package store\n\nvar p = "+mark+" + \"x\"\n")
	write("internal/store/clean_test.go", "package store\n\nvar q = \"nothing to see\"\n")

	inHelper, copies, err := keyPrefixCopies(root)
	if err != nil {
		t.Fatalf("read the built tree: %v", err)
	}
	if !inHelper {
		t.Error("the helper's own literal was not recognised as the helper's")
	}
	if want := []string{"internal/store/copy_test.go"}; !equal(copies, want) {
		t.Errorf("the copies found were %v, want %v", copies, want)
	}
}

// keyPrefixCopies reads every string literal of every Go file under root, as syntax, and
// reports whether the helper spells the mark and which other files do.
func keyPrefixCopies(root string) (inHelper bool, copies []string, err error) {
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
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
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			return fmt.Errorf("parse %s: %w", rel, err)
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
			} else if !slices.Contains(copies, filepath.ToSlash(rel)) {
				copies = append(copies, filepath.ToSlash(rel))
			}
			return true
		})
		return nil
	})
	return inHelper, copies, err
}
