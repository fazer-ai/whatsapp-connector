package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// packageFiles parses every .go file of this directory, keyed by file name.
//
// In a file of its own, and not beside the fence that first needed it, so that each fence
// can be removed on its own. MEASURED: with this living in `load_fence_test.go`, deleting
// that file to check what the fence was worth took the helper with it, the flush fence
// stopped compiling, and the run came back red for a reason that had nothing to do with
// either fence. A fence whose absence cannot be measured is a fence nobody can price.
//
// Walked by hand rather than through `parser.ParseDir`, which is deprecated since Go 1.25
// -- the same way `internal/app` walks its own package. It reads the DIRECTORY and not a
// list of names, because a phase added in a new file is exactly the case a fence that
// knows its filenames would miss.
func packageFiles(t *testing.T) map[string]*ast.File {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("a cerca nao conseguiu ler o diretorio do pacote: %v", err)
	}
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			t.Fatalf("a cerca nao conseguiu ler %s: %v", name, parseErr)
		}
		files[name] = file
	}
	if len(files) == 0 {
		t.Fatal("a cerca nao achou arquivo .go nenhum, entao ela nao esta lendo o que pensa que le")
	}
	return files
}
