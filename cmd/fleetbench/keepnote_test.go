package main

import (
	"bytes"
	"context"
	"database/sql"
	"go/ast"
	"go/token"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
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

// The run itself, with -keep, reading what it prints (#310): the line naming the database,
// prefix and directory it kept is in the report, once, and the three exist afterwards.
//
// Against the servers `make check` names, and stopped once the connector is built, which is
// the interrupt path: the report goes out with what the run had got to, and the question is
// only what that report says. A whole run costs minutes and adds nothing to the answer.
func TestARunWithKeepSaysWhatItKeptInItsReport(t *testing.T) {
	if os.Getenv(databaseVar) == "" || os.Getenv(redisVar) == "" {
		t.Skipf("set %s and %s to run the bench against real servers (see 'make check')", databaseVar, redisVar)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var out, errOut bytes.Buffer
	code, err := runBench(ctx, benchIO{out: &out, errOut: &errOut, afterBuild: cancel}, 2, 2, 2, 1, 0, true)
	if err != nil {
		// A run that stopped before its report (a connector that did not build) still kept
		// what it had made, and says so on stderr: given back by those names before failing.
		if left := keptLine.FindStringSubmatch(errOut.String()); left != nil {
			giveBack(t, left[1], left[2], left[3])
		}
		t.Fatalf("the run stopped before it had a report: %v\nstderr:\n%s", err, errOut.String())
	}

	// Given back by the names the run's own first note prints, which is there whether or not
	// the -keep line is: -keep is exactly what the run did not do, and a failure here must
	// not leave a database behind on the server make check shares.
	own := regexp.MustCompile(`banco desta corrida: (\S+) · prefixo: (\S+) · arquivos: (\S+)`).FindStringSubmatch(out.String())
	if own == nil {
		t.Fatalf("the report does not say which database and prefix the run made:\n%s", out.String())
	}
	t.Cleanup(func() { giveBack(t, own[1], own[2], own[3]) })

	found := keptLine.FindAllStringSubmatch(out.String(), -1)
	if len(found) != 1 {
		t.Fatalf("the report (outcome %v) carries the -keep line %d times, want once:\n%s", code, len(found), out.String())
	}
	database, prefix, dir := found[0][1], found[0][2], found[0][3]
	if database != own[1] || prefix != own[2] || dir != own[3] {
		t.Errorf("the -keep line names %s, %s, %s, and the run made %s, %s, %s", database, prefix, dir, own[1], own[2], own[3])
	}
	if strings.Contains(errOut.String(), "guardado a pedido") {
		t.Errorf("the -keep line went to stderr as well, so a reader of the whole output sees it twice:\n%s", errOut.String())
	}
	if _, statErr := os.Stat(dir); statErr != nil {
		t.Errorf("the directory the note names is not there: %v", statErr)
	}
	admin, err := sql.Open("postgres", os.Getenv(databaseVar))
	if err != nil {
		t.Fatalf("open the server: %v", err)
	}
	defer func() { _ = admin.Close() }()
	var exists bool
	if err := admin.QueryRowContext(t.Context(),
		`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`, database).Scan(&exists); err != nil {
		t.Fatalf("look the database up: %v", err)
	}
	if !exists {
		t.Errorf("the database the note names, %s, is not on the server", database)
	}
}

// keptLine is the note a run with -keep prints, with the three names it gives.
var keptLine = regexp.MustCompile(`guardado a pedido \(-keep\): banco (\S+), prefixo (\S+), (\S+)`)

// giveBack drops what a run with -keep left, by the names it printed.
func giveBack(t *testing.T, database, prefix, dir string) {
	t.Helper()
	options, err := redis.ParseURL(os.Getenv(redisVar))
	if err != nil {
		t.Errorf("give back: parse %s: %v", redisVar, err)
		return
	}
	left := &run{database: database, prefix: prefix, adminURL: os.Getenv(databaseVar), rdb: redis.NewClient(options)}
	for _, trouble := range left.cleanup(context.Background()) {
		t.Errorf("give back: %s", trouble)
	}
	_ = os.RemoveAll(dir)
}
