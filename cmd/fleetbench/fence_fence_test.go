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

// Every Redis client this bench builds has ContextTimeoutEnabled on.
//
// MEASURED as a real wait, not a style rule: with it off, which is go-redis's default, a
// blocking read such as the BLPOP this bench waits a reply on ignores the cancellation of
// its context and runs to its own timeout. A Ctrl-C during that wait kills the connectors
// at once and then leaves the run sitting there for up to a minute before the cleanup that
// drops its database and its keys even starts -- and a run somebody interrupted is exactly
// the run whose leftovers nobody goes looking for.
//
// Read as an assignment on the options this package hands to redis.NewClient, so a second
// client added later is covered by the same check.
func TestEveryRedisClientHonoursCancellation(t *testing.T) {
	t.Parallel()

	built := 0
	for path, file := range packageFiles(t) {
		ast.Inspect(file, func(n ast.Node) bool {
			call, isCall := n.(*ast.CallExpr)
			if !isCall || calleeName(call) != "NewClient" {
				return true
			}
			built++
			// The options variable this call is given, so the assignment that turns the
			// flag on can be looked for on that same name in the same function.
			name := ""
			if len(call.Args) == 1 {
				if arg, isIdent := call.Args[0].(*ast.Ident); isIdent {
					name = arg.Name
				}
			}
			if name == "" {
				t.Errorf("%s: redis.NewClient recebe algo que esta cerca nao sabe seguir, entao ela nao "+
					"pode dizer se o cancelamento chega ao socket", path)
				return true
			}
			if !setsContextTimeout(file, name) {
				t.Errorf("%s: o cliente montado de %s nao liga ContextTimeoutEnabled. Sem isso, uma "+
					"leitura bloqueante ignora o cancelamento e a corrida fica parada ate o timeout "+
					"dela, com os conectores ja mortos e a limpeza sem comecar", path, name)
			}
			return true
		})
	}
	if built == 0 {
		t.Fatal("a cerca nao achou nenhum redis.NewClient, entao ela nao esta lendo o que pensa que le")
	}
}

// setsContextTimeout reports whether the file turns ContextTimeoutEnabled on for this
// options variable.
func setsContextTimeout(file *ast.File, options string) bool {
	on := false
	ast.Inspect(file, func(n ast.Node) bool {
		assign, isAssign := n.(*ast.AssignStmt)
		if !isAssign || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
			return true
		}
		target, isSelector := assign.Lhs[0].(*ast.SelectorExpr)
		if !isSelector || target.Sel.Name != "ContextTimeoutEnabled" {
			return true
		}
		if base, isIdent := target.X.(*ast.Ident); !isIdent || base.Name != options {
			return true
		}
		if value, isIdent := assign.Rhs[0].(*ast.Ident); isIdent && value.Name == "true" {
			on = true
		}
		return true
	})
	return on
}
