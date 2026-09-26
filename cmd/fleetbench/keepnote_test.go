package main

import (
	"bytes"
	"go/ast"
	"go/token"
	"strings"
	"testing"
)

// A run started with -keep says in its report what it left behind, and the report is
// where it says it (#310). The note used to be added by the deferred cleanup, which runs
// after every `rep.write` in `runBench`: it went into a report nobody printed again, and
// whoever asked for -keep had to find the database and the prefix by hand.
//
// Read from the source, because the path it guards needs a PostgreSQL, a Redis and a built
// connector to run, minutes rather than seconds: the note has to be taken in `runBench`
// itself, outside any function literal (a deferred one runs after the report is out),
// and before the first call that prints the report.
func TestTheKeepNoteIsTakenBeforeTheReportIsPrinted(t *testing.T) {
	t.Parallel()

	var body *ast.BlockStmt
	for _, file := range packageFiles(t) {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "runBench" && fn.Recv == nil {
				body = fn.Body
			}
		}
	}
	if body == nil {
		t.Fatal("runBench nao foi encontrada; a cerca nao le o que pensa que le")
	}

	firstWrite, taken := token.NoPos, token.NoPos
	var walk func(n ast.Node, deferred bool)
	walk = func(n ast.Node, deferred bool) {
		ast.Inspect(n, func(node ast.Node) bool {
			if lit, ok := node.(*ast.FuncLit); ok && node != n {
				walk(lit.Body, true)
				return false
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if sel.Sel.Name == "write" && !deferred && (firstWrite == token.NoPos || call.Pos() < firstWrite) {
				firstWrite = call.Pos()
			}
			if sel.Sel.Name == "note" && !deferred && len(call.Args) == 1 {
				if inner, ok := call.Args[0].(*ast.CallExpr); ok {
					if id, ok := inner.Fun.(*ast.Ident); ok && id.Name == "keptNote" && (taken == token.NoPos || call.Pos() < taken) {
						taken = call.Pos()
					}
				}
			}
			return true
		})
	}
	walk(body, false)

	if firstWrite == token.NoPos {
		t.Fatal("runBench nao chama rep.write; a cerca nao le o que pensa que le")
	}
	if taken == token.NoPos {
		t.Fatal("runBench nao poe keptNote no relatorio fora de uma funcao adiada: quem roda com -keep " +
			"nao fica sabendo o banco, o prefixo e a pasta que tem de apagar depois")
	}
	if taken > firstWrite {
		t.Fatal("runBench poe keptNote no relatorio depois do primeiro rep.write: num dos caminhos o " +
			"relatorio sai sem a nota")
	}
}

// What the note says is what a cleanup by hand needs: the database, the prefix and the
// directory, and it goes out with the report.
func TestTheKeepNoteGoesOutWithTheReport(t *testing.T) {
	t.Parallel()

	active := &run{database: "wacbench42", prefix: "wacbench42:"}
	rep := &report{engine: "fake"}
	rep.note(keptNote(active, "/var/tmp/wac-fleetbench-42-x"))
	var out bytes.Buffer
	rep.write(&out, outcomeSetup, nil)

	want := "guardado a pedido (-keep): banco wacbench42, prefixo wacbench42:, /var/tmp/wac-fleetbench-42-x"
	if got := strings.Count(out.String(), want); got != 1 {
		t.Fatalf("o relatorio traz a nota %d vezes, quer 1:\n%s", got, out.String())
	}
	if !rep.written {
		t.Fatal("o relatorio saiu e nao se marcou como escrito, entao a limpeza repetiria a nota no stderr")
	}
	if got := keptNote(active, ""); !strings.Contains(got, "nenhuma pasta criada") {
		t.Fatalf("sem pasta, a nota diz %q", got)
	}
}
