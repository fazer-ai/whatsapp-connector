package main

import (
	"go/ast"
	"testing"
)

// The frozen-owner phase claims the fence exactly once, on its way out.
//
// There are several ways out of that phase and each says something different about the
// fence: nobody held a session, nobody adopted, the phase ran, an error cut it short. A
// branch that returns without claiming leaves the run green over a fence it never reached
// -- which happened twice, in two different branches, and was found by review rather than
// by the suite both times. Asserting on the way out removes the class instead of the two
// instances, and this is what keeps a later branch from reintroducing it.
func TestTheFrozenPhaseClaimsTheFenceOnTheWayOut(t *testing.T) {
	t.Parallel()

	file, ok := packageFiles(t)["frozen.go"]
	if !ok {
		t.Fatal("frozen.go nao esta no pacote, entao esta cerca nao esta lendo o que pensa que le")
	}
	var phase *ast.FuncDecl
	ast.Inspect(file, func(n ast.Node) bool {
		if fn, isFunc := n.(*ast.FuncDecl); isFunc && fn.Name.Name == "frozenOwner" {
			phase = fn
		}
		return true
	})
	if phase == nil || phase.Body == nil {
		t.Fatal("frozenOwner nao foi encontrada em frozen.go")
	}

	claims, deferred := 0, 0
	ast.Inspect(phase.Body, func(n ast.Node) bool {
		if d, isDefer := n.(*ast.DeferStmt); isDefer {
			ast.Inspect(d, func(inner ast.Node) bool {
				if call, isCall := inner.(*ast.CallExpr); isCall && calleeName(call) == "fenceExercised" {
					deferred++
				}
				return true
			})
		}
		if call, isCall := n.(*ast.CallExpr); isCall && calleeName(call) == "fenceExercised" {
			claims++
		}
		return true
	})
	if deferred != 1 {
		t.Errorf("frozenOwner tem %d chamadas adiadas a fenceExercised, e a cerca se afirma uma vez, "+
			"na saida: cada ramo que volta sem afirmar deixa a corrida verde sobre uma cerca que ela "+
			"nao alcancou", deferred)
	}
	if claims != deferred {
		t.Errorf("frozenOwner afirma a cerca %d vezes, e %d delas adiadas: uma afirmacao fora do defer "+
			"e um ramo decidindo por conta propria", claims, deferred)
	}
}

// calleeName is the name of the function a call expression calls, for a plain identifier
// or a selector.
func calleeName(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		return fn.Sel.Name
	}
	return ""
}
