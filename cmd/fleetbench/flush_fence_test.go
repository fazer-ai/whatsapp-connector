package main

import (
	"go/ast"
	"strings"
	"testing"
)

// No flush of any kind reaches this Redis, and this is the check that says so.
//
// The machine this bench runs on shares one Redis and one PostgreSQL with everything else
// on it. `FLUSHDB` would take another fleet's keys; `SCRIPT FLUSH` would take them from
// every database on the server at once, because the script cache is per server and not per
// database. The isolation here is by identifier only: a database named after the run, a
// key prefix named after the run, processes killed by the pids the run captured.
//
// Read as CALLS, through the AST, and not as text. The comment in `run.go` used to say
// "`grep -rn 'FLUSH' cmd/fleetbench` comes back empty", and that recipe matched the
// comment itself: a check that reports its own prose as the thing it was looking for is a
// check that cannot come back empty, so it was never a check at all. A word in a comment
// is not a call, and this tells them apart.
func TestNoFlushReachesTheSharedRedis(t *testing.T) {
	t.Parallel()

	calls := 0
	for path, file := range packageFiles(t) {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			calls++
			name := ""
			switch fn := call.Fun.(type) {
			case *ast.SelectorExpr:
				name = fn.Sel.Name
			case *ast.Ident:
				name = fn.Name
			}
			// FlushDB, FlushAll, FlushDBAsync, ScriptFlush: every one of them reaches past
			// this run's prefix, and go-redis spells them all with `Flush`.
			if strings.Contains(name, "Flush") {
				t.Errorf("%s: chama %s. Esta bancada divide o Redis com outras frotas da maquina, "+
					"e o isolamento dela e so por identificador: um flush leva chave que nao e desta "+
					"corrida", path, name)
			}
			return true
		})
	}
	if calls == 0 {
		t.Fatal("a cerca nao achou chamada nenhuma, entao ela nao esta lendo o que pensa que le")
	}
}
