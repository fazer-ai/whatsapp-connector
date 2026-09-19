package engine_test

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
	"github.com/fazer-ai/whatsapp-connector/internal/store/storetest"
)

// conformingEngines names the engine packages held to recording a pairing.
//
// A table and not a discovery, so that adding an engine is a decision somebody makes in
// writing rather than something that happens by being compiled. The fence below fails
// when a package implements `engine.Session` and is not named here, which is the half
// that makes the table more than documentation.
var conformingEngines = []string{"fake", "whatsmeow"}

// sessionMethods is what makes a type an `engine.Session`.
//
// The whole set rather than one distinctive name: a fence that recognised an engine by a
// single method would stop recognising it the day that method is renamed, and would then
// pass by finding nothing, which is the failure mode this kind of test is worst at.
var sessionMethods = []string{"Connect", "Disconnect", "Logout", "Delete", "Execute", "Events"}

// Every engine records the account it paired, or it is not an engine this connector can
// run.
//
// The obligation the desired-state row could be lifted out of the engines; this one
// cannot. `store.Wanted` joins the request with the pairing, so a session with no pairing
// row is one the sweep can never bring back however well the request was recorded -- and
// the pairing is knowledge only an engine has. In whatsmeow the JID arrives inside the
// library's own handshake, as the argument to `PrePairCallback`, and the callback's return
// value is what accepts or refuses the pairing. There is no seam above that.
//
// So a third implementation would be born with #266 in it, and this is what asks.
//
// The "when" is an order between an event and a table, and never a position inside a
// function: **there is no `pairing.success` for a session without a row in
// `wac_session_device` for it**. That is a promise a client can observe and a crash can
// freeze, and it is what makes this obligation expressible where the desired-state one was
// not: "record it" without a "when" is honoured in opposite places by two engines, and a
// fence over it passes both.
//
// The dimension, in writing: types declared in files under `internal/engine/` that do not
// end in `_test.go`, at any depth. Not the doubles a test builds, and not by naming them:
// an exception by name hands the same free pass to the next double and to the next real
// engine that happens to be called something similar.
//
// A limit, measured rather than papered over: whatsmeow does not reach `pairing.success`
// on a bench, because a real pairing needs a physical phone -- `Connect(qr)` on a test
// session publishes `pairing.qr` and stops there. So the behavioural half of this fence
// runs on the fake, and whatsmeow is held by the source half below, over the wiring that
// makes the order true for it. A test that claimed to cover both by looping over two
// engines and skipping one in silence would be this scenario passing while measuring
// nothing.
func TestEveryEngineRecordsThePairingBeforeItAnnouncesIt(t *testing.T) {
	t.Parallel()

	t.Run("every engine in the tree is one this fence knows about", func(t *testing.T) {
		t.Parallel()

		found := map[string][]string{}
		fset := token.NewFileSet()
		parsed := 0
		err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, parseErr := parser.ParseFile(fset, path, nil, 0)
			if parseErr != nil {
				return parseErr
			}
			parsed++
			for typeName, methods := range methodsByReceiver(file) {
				if implementsSession(methods) {
					pkg := filepath.Base(filepath.Dir(path))
					found[pkg] = append(found[pkg], typeName)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk the engine packages: %v", err)
		}
		if parsed == 0 {
			t.Fatal("no engine file was parsed, so this fence proved nothing about any of them")
		}
		if len(found) == 0 {
			t.Fatalf("no type under internal/engine/ implements %v, which cannot be right: "+
				"this fence recognises an engine by that method set, and finding none means "+
				"it is recognising nothing rather than that nothing is there", sessionMethods)
		}
		for pkg, types := range found {
			if !slices.Contains(conformingEngines, pkg) {
				t.Errorf("package %s implements engine.Session (%v) and is not held to "+
					"recording a pairing.\n"+
					"The sweep that brings accounts back joins the desired row with the "+
					"pairing, so an engine that pairs and records nothing leaves every "+
					"account it runs unresumable: no error, no warning, an empty list. Add "+
					"it to conformingEngines and give it a case below, or say in writing why "+
					"it is exempt.", pkg, types)
			}
		}
	})

	t.Run("whatsmeow records inside the handshake, where the JID arrives", func(t *testing.T) {
		t.Parallel()

		source := readEngineSource(t, "whatsmeow")
		// Two clauses and not one: the wiring says the callback is reached, the call says
		// it records. Either alone is satisfied by a callback that does nothing.
		if !strings.Contains(source, "PrePairCallback = s.bind") {
			t.Error("whatsmeow no longer wires `bind` to `PrePairCallback`.\n" +
				"That wiring is what makes the order true for this engine: the callback " +
				"runs before WhatsApp is told the pairing succeeded, and returning false " +
				"refuses it. Without it the pairing row can land after the announcement, or " +
				"not at all.")
		}
		if !strings.Contains(source, "s.store.Bind(ctx, jid)") {
			t.Error("whatsmeow's pairing callback no longer records the account.\n" +
				"The JID arrives here and nowhere else: no layer above this one sees it, " +
				"so a callback that does not write it leaves the sweep with nothing to join.")
		}
	})

	t.Run("the fake records before it announces", func(t *testing.T) {
		t.Parallel()

		container := openStore(t, store.AlwaysOwned)
		waEngine := fake.New(fake.WithStore(container))
		session, err := waEngine.Open(t.Context(), "sid-pairs")
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		announced := watchFor(t, session.Events(), protocol.EventPairingSuccess)
		if err := session.Connect(t.Context(), engineConnectQR()); err != nil {
			t.Fatalf("Connect: %v", err)
		}
		if !announced(2 * time.Second) {
			t.Fatal("the fake never announced a pairing, so this case measured nothing")
		}
		if _, bound, err := container.For("sid-pairs").JID(t.Context()); err != nil || !bound {
			t.Fatalf("the fake announced a pairing and recorded none (bound=%v, err=%v).\n"+
				"The sweep joins the request with this row, so the account it just paired is "+
				"one nothing can bring back.", bound, err)
		}
	})

	t.Run("a pairing that could not be recorded is not announced", func(t *testing.T) {
		t.Parallel()

		// The ordering, asserted without depending on timing. A store that refuses every
		// write is the one condition under which "record, then announce" and "announce,
		// then record" give different observable answers, and it is deterministic: a
		// connect that announces here has announced a pairing that was never written, and
		// a crash a moment later leaves the client acting on an account nothing holds.
		container := openStore(t, func(string) bool { return false })
		waEngine := fake.New(fake.WithStore(container))
		session, err := waEngine.Open(t.Context(), "sid-refused")
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		announced := watchFor(t, session.Events(), protocol.EventPairingSuccess)
		if err := session.Connect(t.Context(), engineConnectQR()); err == nil {
			t.Error("a connect whose pairing could not be recorded answered success")
		}
		// A bounded wait rather than an instant read, and the same bound the case above
		// uses: there the wait is what proves the watcher works, so a negative here is a
		// negative under a watcher that has been seen to answer.
		if announced(200 * time.Millisecond) {
			t.Fatal("the fake announced a pairing it could not record.\n" +
				"A client that acts on `pairing.success` is acting on an account this " +
				"connector has no row for, and the sweep will never bring it back.")
		}
	})
}

// methodsByReceiver is which methods each named type in a file declares.
func methodsByReceiver(file *ast.File) map[string][]string {
	byType := map[string][]string{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
			continue
		}
		expr := fn.Recv.List[0].Type
		if star, ok := expr.(*ast.StarExpr); ok {
			expr = star.X
		}
		ident, ok := expr.(*ast.Ident)
		if !ok {
			continue
		}
		byType[ident.Name] = append(byType[ident.Name], fn.Name.Name)
	}
	return byType
}

func implementsSession(methods []string) bool {
	for _, want := range sessionMethods {
		if !slices.Contains(methods, want) {
			return false
		}
	}
	return true
}

// readEngineSource is every production line of one engine package, joined.
//
// Joined rather than parsed because what the two clauses above look for is a wiring and a
// call, and an AST walk that found them would be a second implementation of `strings.
// Contains` with more places to be wrong. It fails on an empty read for the same reason
// the walk above fails on an empty parse.
func readEngineSource(t *testing.T, pkg string) string {
	t.Helper()

	var joined strings.Builder
	err := filepath.WalkDir(pkg, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		joined.Write(body)
		return nil
	})
	if err != nil {
		t.Fatalf("read the %s engine: %v", pkg, err)
	}
	if joined.Len() == 0 {
		t.Fatalf("read nothing from the %s engine, so the clauses below prove nothing", pkg)
	}
	return joined.String()
}

func openStore(t *testing.T, owned store.Ownership) *store.Container {
	t.Helper()

	container, err := store.Open(t.Context(), storetest.New(t).URL, owned, zerolog.Nop())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return container
}

func engineConnectQR() engine.ConnectRequest {
	return engine.ConnectRequest{Pairing: "qr"}
}

// watchFor answers whether a type was emitted, reading the channel off the test's
// goroutine because the emitter blocks on it.
//
// It returns a wait rather than a flag: the emitter hands the event over and carries on,
// so a flag read the instant `Connect` returns is a race, and a race here reads as "never
// announced", which is the answer this fence gives to its worst failure.
func watchFor(t *testing.T, events <-chan engine.Emission, want protocol.EventType) func(time.Duration) bool {
	t.Helper()

	seen := make(chan struct{})
	var once sync.Once
	done := make(chan struct{})
	go func() {
		defer close(done)
		for emission := range events {
			if emission.Type == want {
				once.Do(func() { close(seen) })
			}
		}
	}()
	t.Cleanup(func() {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	})
	return func(within time.Duration) bool {
		select {
		case <-seen:
			return true
		case <-time.After(within):
			return false
		}
	}
}

// The pairing write is the caller's command, not a job of its own.
//
// It used to run on a context built from `context.Background()`, so a client's
// `max_runtime_ms`, a session going away and a lease moving all stopped meaning anything
// the moment the write started: a store that blocked held the session's executor for the
// whole bind timeout after its caller had given up, and then committed a pairing nobody
// was waiting for any more.
//
// A context already cancelled is the same condition with the timing taken out of it, which
// is what makes this deterministic rather than a race with a slow store.
//
// The order this leaves is the one `TestEveryEngineRecordsThePairingBeforeItAnnouncesIt`
// holds: the write is refused, so the connect fails and `pairing.success` is never
// announced. A client is never told about an account that was not recorded.
func TestAPairingWriteHonoursTheConnectsContext(t *testing.T) {
	t.Parallel()

	container := openStore(t, store.AlwaysOwned)
	waEngine := fake.New(fake.WithStore(container))
	announcedFor := func(session engine.Session) func(time.Duration) bool {
		return watchFor(t, session.Events(), protocol.EventPairingSuccess)
	}
	session, err := waEngine.Open(t.Context(), "sid-cancelled")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	announced := announcedFor(session)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := session.Connect(ctx, engine.ConnectRequest{Pairing: "qr"}); err == nil {
		t.Fatal("a connect whose context was already cancelled paired the account anyway. " +
			"The command it belongs to is over, and what it wrote outlives the ceiling its " +
			"client put on it and the ownership the write was fenced against.")
	}

	var rows int
	if err := container.DB().QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM wac_session_device WHERE sid = 'sid-cancelled'`).Scan(&rows); err != nil {
		t.Fatalf("count the device rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("the cancelled connect left %d pairing rows behind", rows)
	}
	// The same bound the case above uses, for the same reason: a negative under a watcher
	// that the case above has been seen to answer.
	if announced(200 * time.Millisecond) {
		t.Fatal("the cancelled connect announced `pairing.success` for an account it did not record")
	}
}
