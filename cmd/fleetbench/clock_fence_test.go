package main

import (
	"go/ast"
	"go/token"
	"testing"
)

// The recovery clock starts at the death, and this is what keeps it there.
//
// MEASURED: with the mark taken after `group.start` returned, the number came out SHORTER
// the slower the replacement started. The dead process's lease expires on a clock that
// began at the kill, so what the loop below it waits for is the remainder of a
// WAC_LEASE_TTL, and starting the stopwatch late subtracts the startup from a wait it does
// not shorten. A slow start therefore made recovery look fast and could carry a run past
// its own `-max-adoption`, and a replacement that adopted everything before answering
// /healthz would have reported a recovery of zero.
//
// Read as POSITIONS in the source, because that is what the defect was: the same three
// statements in the wrong order, each of them correct on its own.
//
// Every function that marks the clock, and not the one the walk happened to end on: the
// package is read from a directory into a map, whose order is not the source's, so a fence
// that keeps the last match it saw gives a different verdict on different runs.
func TestTheAdoptionClockStartsAtTheDeath(t *testing.T) {
	t.Parallel()

	marked := 0
	for _, file := range packageFiles(t) {
		for _, decl := range file.Decls {
			fn, isFunc := decl.(*ast.FuncDecl)
			if !isFunc || fn.Body == nil {
				continue
			}
			mark, marks := clockMark(fn.Body)
			if !marks {
				continue
			}
			marked++
			kills := calledAt(fn.Body, "kill")
			if len(kills) == 0 {
				t.Errorf("%s marca adoptionStart e nao mata ninguem, entao nao ha morte para o "+
					"relogio comecar em", fn.Name.Name)
				continue
			}
			// One clause and not two: "before the first kill" already covers the position
			// this was measured in, which was after the replacement had been started. A
			// second clause naming the start could be deleted without turning anything red,
			// and a clause that cannot go red is not part of the fence.
			if mark > kills[0] {
				t.Errorf("adoptionStart e marcado depois da morte em %s: o arranque do substituto "+
					"e o enterro do dono morto saem da medida de quanto as sessoes ficaram fora, "+
					"e quanto mais lento esse arranque for, mais rapida a recuperacao parece",
					fn.Name.Name)
			}
		}
	}
	if marked == 0 {
		t.Fatal("nenhuma funcao do pacote marca adoptionStart, entao esta cerca nao esta lendo o " +
			"relogio que pensa que le")
	}
}

// clockMark returns where the body starts the adoption stopwatch, and whether it does.
func clockMark(body *ast.BlockStmt) (token.Pos, bool) {
	var at token.Pos
	var found bool
	ast.Inspect(body, func(n ast.Node) bool {
		assign, isAssign := n.(*ast.AssignStmt)
		if !isAssign {
			return true
		}
		for _, lhs := range assign.Lhs {
			if name, isName := lhs.(*ast.Ident); isName && name.Name == "adoptionStart" {
				if !found || assign.Pos() < at {
					at, found = assign.Pos(), true
				}
			}
		}
		return true
	})
	return at, found
}

// calledAt returns, in source order, the position of every call to a method of this name
// inside the body.
func calledAt(body *ast.BlockStmt, method string) []token.Pos {
	var at []token.Pos
	ast.Inspect(body, func(n ast.Node) bool {
		call, isCall := n.(*ast.CallExpr)
		if !isCall {
			return true
		}
		if sel, isSel := call.Fun.(*ast.SelectorExpr); isSel && sel.Sel.Name == method {
			at = append(at, call.Pos())
		}
		return true
	})
	return at
}
