package main

import (
	"go/ast"
	"testing"
)

// The load has to be stopped on every exit path of the function that starts it.
//
// MEASURED: the handover phase can return an error from any of six places between starting
// the load and the line that stops it. On any of those, the goroutines keep sending into
// this run's command streams while the deferred cleanup scans and deletes the run's keys,
// and the run then reports keys of its own as strays it could not remove. The explicit
// stop stays where it is -- the order matters, because nothing may still be writing when
// the assertions read the streams -- and the deferred one is what covers the error
// returns.
//
// Read from the package directory rather than from one named file: a phase added in a new
// file is exactly the case a fence that knows one filename would miss.
func TestEveryLoadIsStoppedOnEveryPath(t *testing.T) {
	t.Parallel()

	found := 0
	for path, file := range packageFiles(t) {
		ast.Inspect(file, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				return true
			}
			for _, name := range startedLoads(fn.Body) {
				found++
				if !deferredStopOf(fn.Body, name) {
					t.Errorf("%s: %s recebe o retorno de startLoad e nao tem `defer %s.end()`. "+
						"Um erro em qualquer fase seguinte volta com as goroutines da carga ainda "+
						"escrevendo nos streams desta corrida, enquanto a limpeza apaga as chaves dela",
						path, name, name)
				}
			}
			return true
		})
	}
	if found == 0 {
		t.Fatal("a cerca nao achou nenhuma chamada a startLoad, entao ela nao esta lendo o que pensa que le")
	}
}

// startedLoads returns the names assigned from a startLoad call in this body.
func startedLoads(body *ast.BlockStmt) []string {
	var names []string
	ast.Inspect(body, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
			return true
		}
		call, ok := assign.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		if fn, ok := call.Fun.(*ast.Ident); !ok || fn.Name != "startLoad" {
			return true
		}
		if ident, ok := assign.Lhs[0].(*ast.Ident); ok {
			names = append(names, ident.Name)
		}
		return true
	})
	return names
}

// deferredStopOf reports whether this body defers name.end().
func deferredStopOf(body *ast.BlockStmt, name string) bool {
	stopped := false
	ast.Inspect(body, func(n ast.Node) bool {
		d, ok := n.(*ast.DeferStmt)
		if !ok {
			return true
		}
		sel, ok := d.Call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "end" {
			return true
		}
		if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == name {
			stopped = true
		}
		return true
	})
	return stopped
}
