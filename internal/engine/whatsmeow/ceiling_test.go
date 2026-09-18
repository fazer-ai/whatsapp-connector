package whatsmeow

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// #165 asked which commands should be given a ceiling and found, when it was measured, that
// none of them needs one: whatsmeow bounds every request it sends at `defaultRequestTimeout`,
// seventy five seconds, and that number covers an info query, a send and a sendfb alike; every
// wait this package builds for itself comes off a timer. So the catalogue has no unbounded
// command, and the thing worth having is not a ceiling but the two fences below, because both
// ways of losing that property are one word long and neither has a symptom.
//
// The reach is `internal/engine/whatsmeow` and that is deliberate rather than an oversight.
// This is the only package that talks to WhatsApp, so it is the only one where a wait can
// outlive a command without somebody upstream ending it; `internal/session` waits on the
// engine, which is bounded here, and on the store, which is bounded by `storeLimit`. A wait
// planted in another package is not caught, and if one is ever built there this comment is
// the thing that should stop being true first.
//
// What neither fence reaches, said out loud so a green run is not read as more than it is:
// the socket write lock (#74). `NoiseSocket.SendFrame` takes `ns.writeLock` before it looks at
// the context, so a node write can outlive every deadline set anywhere in this repository,
// and no ceiling expressible here changes that. These fences keep the bounds that do work
// from being switched off; they do not create one where the library has none.
func theSourceOfThisPackage(t *testing.T) (*token.FileSet, []*ast.File, []string) {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		// Parsed one file at a time rather than through `parser.ParseDir`, which is
		// deprecated, and over the directory rather than over a list, so a wait or a
		// request added in a file nobody thought to name here is read like every other.
		file, err := parser.ParseFile(fset, name, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files = append(files, file)
		names = append(names, name)
	}
	if len(files) == 0 {
		t.Fatal("no production files parsed, so both fences below would pass on nothing")
	}
	return fset, files, names
}

// A request to WhatsApp carries its own ceiling and whatsmeow fills it in: a `Timeout` left
// at zero becomes `defaultRequestTimeout`. Writing the field at all is therefore either a
// tighter bound, which is the point, or one of two ways of giving the bound up -- a negative
// value, which whatsmeow documents as switching the timeout off outright, and a zero, which
// says in this repository's own source that the wait is upstream's to choose rather than
// this connector's. Both are a word, both leave the suite green, and the second is the one
// that would survive a pin bump changing what the default is.
//
// So the rule is the positive value, and the field is read wherever it appears rather than
// on a list of request types. A list would have to be kept, and the shape it would miss is
// the one #160 is about: the type added later that nobody adds to the list. Nothing in this
// package sets a `Timeout` today -- `wm.SendRequestExtra{ID: messageID}` and
// `wm.DangerousInfoQuery{...}` both leave it out, which is what puts them under the seventy
// five seconds -- so there is no exception to carry, and the `http.Transport` in
// `outbound.go` is untouched because its fields are `IdleConnTimeout` and
// `TLSHandshakeTimeout`, and the `net.Dialer` one it does spell `Timeout` is thirty seconds.
func TestNoRequestToWhatsAppGivesUpItsOwnCeiling(t *testing.T) {
	t.Parallel()

	fset, files, _ := theSourceOfThisPackage(t)
	found := 0
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			for _, element := range literal.Elts {
				pair, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := pair.Key.(*ast.Ident)
				if !ok || key.Name != "Timeout" {
					continue
				}
				found++
				if positiveDuration(pair.Value) {
					continue
				}
				t.Errorf("%s: %s is written with a value that is not a positive duration, "+
					"which hands the ceiling of this request to whatsmeow's default or "+
					"switches it off; give it a duration this repository chooses",
					fset.Position(pair.Pos()), rendered(pair))
			}
			return true
		})
	}
	// The fence is about a field that nothing sets today, so a change that stopped it
	// reading composite literals at all would pass silently. The `net.Dialer` in
	// `outbound.go` is the one occurrence in the package and it is what keeps this honest.
	if found == 0 {
		t.Fatal("no `Timeout` field was read anywhere in the package, so this fence " +
			"proved nothing; it should at least have reached the net.Dialer in outbound.go")
	}
}

// positiveDuration answers for the forms a ceiling is written in here, and the honest summary
// of its reach is: it reads the value, not the program. A literal `0` and a negation are the
// two ways of giving the bound up that are spelled in the expression itself, and both are
// caught, through `30 * time.Second` and through parentheses.
//
// What it does not do is resolve a name. `Timeout: turnedOff`, with the constant declared a
// few lines up, and `Timeout: whatever`, with a variable, both come back positive and pass.
// That is a gap and it is the deliberate half of the trade: closing it means a constant
// folder or a full type-checked load of the package, and the failure mode of getting either
// slightly wrong is a fence that reports a bound where there is none, which is worse than one
// whose reach is written down. The reach is written down here, and #165's PR reports which
// forms were planted and which were caught.
func positiveDuration(value ast.Expr) bool {
	switch typed := value.(type) {
	case *ast.BasicLit:
		return typed.Kind == token.INT && typed.Value != "0"
	case *ast.UnaryExpr:
		// `-x` is the documented way to switch whatsmeow's timeout off.
		return typed.Op != token.SUB && positiveDuration(typed.X)
	case *ast.BinaryExpr:
		// `30 * time.Second`, and the only way to make that not positive is a negative
		// operand, which the recursion catches.
		return positiveDuration(typed.X) && positiveDuration(typed.Y)
	case *ast.SelectorExpr, *ast.Ident, *ast.CallExpr, *ast.ParenExpr:
		return true
	default:
		return false
	}
}

func rendered(pair *ast.KeyValueExpr) string {
	if ident, ok := pair.Key.(*ast.Ident); ok {
		return ident.Name
	}
	return "Timeout"
}

// The other way a command stops being bounded is a wait nothing ends. Every wait this
// package builds comes off something that fires on its own -- a timer, a context, or the
// channel that closes when the session goes away -- and a `select` missing all three, or a
// bare receive with no `select` at all, is a goroutine that ends when its peer decides to
// let it and not before. There are six of the first kind and five of the second in the tree
// this was written against, and none of the third.
//
// The rule is written as what ends a wait rather than as what a wait may contain, because
// the list of channels somebody might block on has no end while the list of things that can
// end a wait without the peer's cooperation is three items long.
func TestNoWaitInThisPackageDependsOnItsPeerToEnd(t *testing.T) {
	t.Parallel()

	fset, files, _ := theSourceOfThisPackage(t)
	selects := 0
	for _, file := range files {
		// A receive spelled in a `case` is the select's, and the select is judged as a
		// whole below. Collected first because `ast.Inspect` reaches the receive inside
		// the clause on its own, and reporting it there called every guarded wait in the
		// package unguarded -- twenty seven of them on the first run of this fence.
		guarded := map[token.Pos]bool{}
		// A receive on a channel this same file sends to is the release half of a
		// semaphore, not a wait: the value it takes back is the one the send put there, so
		// it cannot block. `takePresenceWrite` in `presence.go` is the one in the tree this
		// was written against, and it is recognised by the shape rather than by name,
		// because a list of blessed lines is the thing #160 is about.
		released := map[string]bool{}
		ast.Inspect(file, func(node ast.Node) bool {
			if send, ok := node.(*ast.SendStmt); ok {
				if name := channelName(send.Chan); name != "" {
					released[name] = true
				}
			}
			statement, ok := node.(*ast.SelectStmt)
			if !ok {
				return true
			}
			for _, clause := range statement.Body.List {
				comm, ok := clause.(*ast.CommClause)
				if !ok || comm.Comm == nil {
					continue
				}
				ast.Inspect(comm.Comm, func(inner ast.Node) bool {
					if unary, ok := inner.(*ast.UnaryExpr); ok && unary.Op == token.ARROW {
						guarded[unary.Pos()] = true
					}
					return true
				})
			}
			return true
		})
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.SelectStmt:
				selects++
				if selectCanEndOnItsOwn(typed) {
					return true
				}
				t.Errorf("%s: this select waits only on channels its peer controls, so "+
					"nothing ends it if the peer never writes; give it a timer, a "+
					"ctx.Done(), the session's done channel, or a default",
					fset.Position(typed.Pos()))
			case *ast.UnaryExpr:
				if typed.Op == token.ARROW && !guarded[typed.Pos()] && !released[channelName(typed.X)] {
					t.Errorf("%s: this receive has no select around it, so nothing but "+
						"the sender ends it; wait on it alongside a timer, a ctx.Done() "+
						"or the session's done channel", fset.Position(typed.Pos()))
				}
			}
			return true
		})
	}
	if selects == 0 {
		t.Fatal("no select was read anywhere in the package, so this fence proved nothing")
	}
}

// channelName renders the channel of a send or a receive as the source spells it, which is
// how the two halves of a semaphore are matched. Anything more complicated than a name or a
// field selector comes back empty and is therefore never treated as a release.
func channelName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		if inner := channelName(typed.X); inner != "" {
			return inner + "." + typed.Sel.Name
		}
	case *ast.ParenExpr:
		return channelName(typed.X)
	}
	return ""
}

// selectCanEndOnItsOwn is the whole judgement of the fence above, kept in one place so the
// three accepted endings can be read as a list. A `default` counts because a select with one
// does not wait at all.
func selectCanEndOnItsOwn(statement *ast.SelectStmt) bool {
	for _, clause := range statement.Body.List {
		comm, ok := clause.(*ast.CommClause)
		if !ok {
			continue
		}
		if comm.Comm == nil {
			return true // default:
		}
		if endsAWaitOnItsOwn(comm.Comm) {
			return true
		}
	}
	return false
}

func endsAWaitOnItsOwn(statement ast.Stmt) bool {
	text := ""
	ast.Inspect(statement, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.SelectorExpr:
			// `t.C` for a timer, `ctx.Done()` for a context, `s.done` for the session's
			// lifetime. Spelled as names because the alternative is type information the
			// fence would have to build a package loader to get, and these three names are
			// what this package actually uses.
			switch typed.Sel.Name {
			case "C", "Done", "done":
				text = typed.Sel.Name
			}
		case *ast.Ident:
			if typed.Name == "After" || typed.Name == "Tick" {
				text = typed.Name
			}
		}
		return text == ""
	})
	return text != ""
}

// The two fences above read this package and nothing else, and `TestTheFencesReadEveryFile`
// is what keeps that reach honest: a file added to the package is read because the directory
// is, and a file added to another package is not read at all, which is the limitation the
// comment at the top of this file states rather than hides.
func TestTheFencesReadEveryFile(t *testing.T) {
	t.Parallel()

	_, files, names := theSourceOfThisPackage(t)
	if len(files) != len(names) {
		t.Fatalf("parsed %d files for %d names", len(files), len(names))
	}
	wanted := map[string]bool{"session.go": false, "group.go": false, "outbound.go": false, "send.go": false}
	for _, name := range names {
		if _, ok := wanted[name]; ok {
			wanted[name] = true
		}
	}
	for name, read := range wanted {
		if !read {
			t.Errorf("%s was not among the files the fences read", name)
		}
	}
}
