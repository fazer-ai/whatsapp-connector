package redisstream_test

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// `CommandMaxLen` was a field of Options with a default, a normalisation in New, and no
// reader anywhere else: the four lines only ever spoke to each other (#246). It sat beside
// `EventMaxLen`, which has the same shape and is wired, so neither grep nor review
// separated them -- the two were written together and only one was connected.
//
// The fence has to be "read outside New", not "read anywhere", and that distinction is the
// whole test. `CommandMaxLen` *was* read, by its own `if opts.CommandMaxLen <= 0`, so a
// fence that accepts any read at all goes green on precisely the field it exists to catch.
// Normalising a field is not using it; it is preparing it for a use that has to exist
// somewhere else.
//
// A field that genuinely has nothing to do past construction goes here with the reason,
// the same way internal/redisx marks the keys the connector does not render. There is none
// today, and an empty map is the honest starting state rather than a placeholder.
var optionsFieldsNothingReadsOutsideNew = map[string]string{}

func TestEveryOptionsFieldIsReadOutsideNew(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	files := parseProductionFiles(t, fset, ".")

	fields := optionsFields(t, files)
	if len(fields) == 0 {
		t.Fatal("found no fields on Options: the fence is reading the wrong type and would pass on anything")
	}

	newBody := funcBody(t, files, "New")
	read := map[string]bool{}
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			// `opts.Block` inside New, or `s.opts.Block` anywhere. Anything else named
			// Block belongs to another type and says nothing about this one.
			if base := render(fset, sel.X); base != "opts" && !strings.HasSuffix(base, ".opts") {
				return true
			}
			if newBody.Pos() <= sel.Pos() && sel.Pos() <= newBody.End() {
				return true
			}
			read[sel.Sel.Name] = true
			return true
		})
	}

	for _, field := range fields {
		if read[field] {
			if why, excused := optionsFieldsNothingReadsOutsideNew[field]; excused {
				t.Errorf("Options.%s is excused as unread (%q) but production code does read it outside New: drop the entry", field, why)
			}
			continue
		}
		if _, excused := optionsFieldsNothingReadsOutsideNew[field]; excused {
			continue
		}
		t.Errorf("nothing outside New reads Options.%s: normalising a field is not using it, so either wire it up or delete it (#246)", field)
	}
}

// parseProductionFiles reads the package's own non-test sources. Test files are left out on
// purpose: a test that pokes at s.opts keeps a dead option alive on the evidence of the
// test written for it, which is the loop this fence exists to break.
func parseProductionFiles(t *testing.T, fset *token.FileSet, dir string) []*ast.File {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files = append(files, file)
	}
	if len(files) == 0 {
		t.Fatalf("no production sources under %s: the fence would pass by having nothing to read", dir)
	}
	return files
}

func optionsFields(t *testing.T, files []*ast.File) []string {
	t.Helper()

	var fields []string
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			spec, ok := n.(*ast.TypeSpec)
			if !ok || spec.Name.Name != "Options" {
				return true
			}
			structType, ok := spec.Type.(*ast.StructType)
			if !ok {
				return true
			}
			for _, field := range structType.Fields.List {
				for _, name := range field.Names {
					fields = append(fields, name.Name)
				}
			}
			return false
		})
	}
	sort.Strings(fields)
	return fields
}

func funcBody(t *testing.T, files []*ast.File, name string) *ast.BlockStmt {
	t.Helper()

	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Name.Name != name || fn.Body == nil {
				continue
			}
			return fn.Body
		}
	}
	t.Fatalf("no func %s in the package: the fence cannot tell a normalisation from a use", name)
	return nil
}

func render(fset *token.FileSet, expr ast.Expr) string {
	var out strings.Builder
	if err := printer.Fprint(&out, fset, expr); err != nil {
		return ""
	}
	return out.String()
}
