package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"modernc.org/sqlite"

	"github.com/fazer-ai/whatsapp-connector/internal/store/storetest"
)

// #293: `busy_timeout` and a caller's deadline were two ceilings that did not know about
// each other, and the smaller did not win. These are about the one that now does.
//
// The contention is made with a second *process* rather than a second pool, because that
// is the shape the defect actually takes: one pool with `SetMaxOpenConns(1)` serialises in
// `database/sql`'s queue and honours the context exactly, so a second pool in this process
// would be measuring something a deployment does not have. What a deployment has is the
// overlap of a deploy, an operator's `sqlite3`, or a backup.
func aFileWithASecondWriter(t *testing.T) (path string, hold, release func()) {
	t.Helper()

	path = t.TempDir() + "/held.db"
	held, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)&_txlock=immediate")
	if err != nil {
		t.Fatalf("open the holder: %v", err)
	}
	held.SetMaxOpenConns(1)
	if _, err := held.ExecContext(t.Context(), `CREATE TABLE IF NOT EXISTS wac293 (k TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var tx *sql.Tx
	hold = func() {
		tx, err = held.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatalf("begin the holding write: %v", err)
		}
		if _, err := tx.ExecContext(t.Context(), `INSERT INTO wac293 VALUES ('held')`); err != nil {
			t.Fatalf("the holding write: %v", err)
		}
	}
	return path, hold, func() {
		if tx != nil {
			_ = tx.Rollback()
		}
		_ = held.Close()
	}
}

// Through the package's own front door, so this compiles and runs on the base: what the
// red proof has to fail on is behaviour, not a symbol the base does not have.
//
// The holding write is taken after the container is up on purpose. `OpenWith` brings
// whatsmeow's schema up, and a lock held across that would fail the open rather than the
// statement under test.
func budgeted(t *testing.T, path string, hold func()) *sql.DB {
	t.Helper()

	container, err := Open(context.Background(), "sqlite:"+path, AlwaysOwned, zerolog.Nop())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })
	hold()
	return container.DB()
}

// The whole of the issue in one assertion: a write blocked by another writer comes back
// on the caller's clock instead of the DSN's.
func TestAContendedWriteComesBackOnTheCallersClock(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		budget time.Duration
	}{
		{"a tight caller", 300 * time.Millisecond},
		{"a roomier one", 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path, hold, release := aFileWithASecondWriter(t)
			defer release()
			db := budgeted(t, path, hold)

			ctx, cancel := context.WithTimeout(context.Background(), tc.budget)
			defer cancel()

			began := time.Now()
			_, err := db.ExecContext(ctx, `INSERT INTO wac293 VALUES ('mine')`)
			took := time.Since(began)

			if err == nil {
				t.Fatal("the write went through, so nothing was holding the file")
			}
			// Generous, because what is under test is which ceiling decided and not how
			// fast the machine is. The DSN's is ten seconds, so anything under a second
			// of slack separates them by a wide margin.
			if ceiling := tc.budget + time.Second; took > ceiling {
				t.Errorf("the write took %s, want at most %s: the DSN's busy_timeout decided, not the caller's deadline", took, ceiling)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("the failure is %v, want the caller's own deadline: every classifier above branches on that", err)
			}
			// And it does not come back early either, which is what makes the overrun
			// the comments above and in `group.go` name a measured number rather than a
			// guess. What ends the wait is the busy handler, at the caller's deadline
			// plus `budgetSlack`; the context only names the error. A write that came
			// back before that did not wait out the budget it installed, and the
			// arithmetic said one thing while the connection did another.
			if floor := tc.budget + budgetSlack; took < floor {
				t.Errorf("the write took %s, want at least %s: the busy handler gave up "+
					"before the budget this call installed ran out", took, floor)
			}
		})
	}
}

// The slack is what makes the error the context's rather than a race with `SQLITE_BUSY`,
// and a race would show up as the same call answering two different ways.
func TestTheSameContendedWriteAlwaysFailsTheSameWay(t *testing.T) {
	t.Parallel()

	path, hold, release := aFileWithASecondWriter(t)
	defer release()
	db := budgeted(t, path, hold)

	seen := make(map[bool]int, 2)
	for range 20 {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, err := db.ExecContext(ctx, `INSERT INTO wac293 VALUES ('mine')`)
		cancel()
		seen[errors.Is(err, context.DeadlineExceeded)]++
	}
	if seen[true] != 20 {
		t.Errorf("twenty identical writes answered %d times with the caller's deadline and %d times with something else; "+
			"which error a caller gets is a race", seen[true], seen[false])
	}
}

// A caller that named no deadline is not given one. The DSN's value is what it had before
// this existed, and shortening it here would turn waiting into failing for every path that
// never asked for a ceiling.
func TestAWriteWithNoDeadlineOfItsOwnStillWaitsTheDSNsValue(t *testing.T) {
	t.Parallel()

	path, hold, release := aFileWithASecondWriter(t)
	defer release()
	db := budgeted(t, path, hold)

	// Read it back rather than timing a ten-second wait: the value on the connection is
	// the thing being asserted, and a test that waits it out costs the suite ten seconds
	// to learn the same fact.
	var onTheConnection int
	if err := db.QueryRowContext(context.Background(), `PRAGMA busy_timeout`).Scan(&onTheConnection); err != nil {
		t.Fatalf("read the pragma back: %v", err)
	}
	if want := whatTheDSNOpensWith(t, path); onTheConnection != want {
		t.Errorf("a call with no deadline left %dms on the connection, want the DSN's %dms", onTheConnection, want)
	}
}

// One connection is one setting, shared with whatsmeow's device store. A budget left
// behind by a tight caller would become the ceiling of whatever ran next.
func TestOneCallersBudgetIsNotTheNextCallersCeiling(t *testing.T) {
	t.Parallel()

	path, hold, release := aFileWithASecondWriter(t)
	defer release()
	db := budgeted(t, path, hold)

	tight, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, _ = db.ExecContext(tight, `INSERT INTO wac293 VALUES ('tight')`)
	cancel()

	var inherited int
	if err := db.QueryRowContext(context.Background(), `PRAGMA busy_timeout`).Scan(&inherited); err != nil {
		t.Fatalf("read the pragma back: %v", err)
	}
	if want := whatTheDSNOpensWith(t, path); inherited != want {
		t.Errorf("the next call inherited %dms from the one before it, want the DSN's %dms", inherited, want)
	}
}

// whatTheDSNOpensWith is the `busy_timeout` a connection of this store's own making
// starts on, read from a connection outside the wrapper so the expectation is not taken
// from the thing under test.
func whatTheDSNOpensWith(t *testing.T, path string) int {
	t.Helper()

	_, dsn, err := parseURL("sqlite:" + path)
	if err != nil {
		t.Fatalf("parseURL: %v", err)
	}
	connector, err := sqlite.NewConnector(dsn)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	raw, err := connector.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = raw.Close() }()

	rows, err := raw.(driver.QueryerContext).QueryContext(t.Context(), "PRAGMA busy_timeout", nil)
	if err != nil {
		t.Fatalf("read busy_timeout: %v", err)
	}
	defer func() { _ = rows.Close() }()
	values := make([]driver.Value, len(rows.Columns()))
	if err := rows.Next(values); err != nil {
		t.Fatalf("read busy_timeout row: %v", err)
	}
	ms, ok := values[0].(int64)
	if !ok {
		t.Fatalf("busy_timeout came back as %T", values[0])
	}
	return int(ms)
}

// A wrapper decides what `database/sql` may use by what it implements, and it falls back
// in silence: a transaction started without its context or isolation level, a prepare
// without its context, a connection never reset and never checked for validity. Nothing
// reports that, and the first version of this change had exactly that defect with its
// tests green.
//
// So this is a table of what the pinned driver offers, and the assertion is that the
// wrapper offers the same. A pin bump that adds one fails here, which is the signal to
// forward it rather than to discover it missing in a deployment.
func TestTheWrappedConnectionOffersWhatTheDriverOffers(t *testing.T) {
	t.Parallel()

	raw := rawConn(t)
	defer func() { _ = raw.Close() }()
	wrapped, err := newBudgetedConn(raw)
	if err != nil {
		t.Fatalf("newBudgetedConn: %v", err)
	}

	for _, tc := range []struct {
		name string
		has  func(any) bool
	}{
		{"driver.ExecerContext", func(c any) bool { _, ok := c.(driver.ExecerContext); return ok }},
		{"driver.QueryerContext", func(c any) bool { _, ok := c.(driver.QueryerContext); return ok }},
		{"driver.ConnBeginTx", func(c any) bool { _, ok := c.(driver.ConnBeginTx); return ok }},
		{"driver.ConnPrepareContext", func(c any) bool { _, ok := c.(driver.ConnPrepareContext); return ok }},
		{"driver.SessionResetter", func(c any) bool { _, ok := c.(driver.SessionResetter); return ok }},
		{"driver.Validator", func(c any) bool { _, ok := c.(driver.Validator); return ok }},
		{"driver.Pinger", func(c any) bool { _, ok := c.(driver.Pinger); return ok }},
		{"driver.NamedValueChecker", func(c any) bool { _, ok := c.(driver.NamedValueChecker); return ok }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.has(raw) && !tc.has(wrapped) {
				t.Errorf("the driver implements %s and the wrapper does not, so database/sql "+
					"silently stops using it", tc.name)
			}
			if !tc.has(raw) && tc.has(wrapped) {
				t.Errorf("the wrapper claims %s the driver does not implement, so a call through "+
					"it panics rather than falling back", tc.name)
			}
		})
	}
}

func rawConn(t *testing.T) driver.Conn {
	t.Helper()

	connector, err := sqlite.NewConnector(t.TempDir() + "/raw.db?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	conn, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return conn
}

// A transaction still gets its context and its isolation level, which is what
// `ConnBeginTx` being forwarded buys. Asserted through `database/sql` rather than on the
// wrapper directly, because the fallback this catches is `database/sql`'s own.
func TestATransactionComesBackOnTheCallersClockToo(t *testing.T) {
	t.Parallel()

	path, hold, release := aFileWithASecondWriter(t)
	defer release()
	db := budgeted(t, path, hold)

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	started := time.Now()
	tx, err := db.BeginTx(ctx, nil)
	took := time.Since(started)
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("the transaction began although another process holds the write lock")
	}
	if took > time.Second {
		t.Errorf("BeginTx under a 300ms deadline took %v: `_txlock=immediate` takes the "+
			"write lock at BEGIN, so this is a contended write like any other and has to "+
			"run under the caller's ceiling", took)
	}
}

// `PrepareContext` is forwarded and deliberately not budgeted, because a prepared
// statement executes through its own `ExecContext` and never through the connection's: a
// budget set at prepare time is a ceiling that looks applied and is not. Nothing reaches
// it today, and this is what says so out loud rather than in a comment alone.
func TestNothingHereReachesThePreparePath(t *testing.T) {
	t.Parallel()

	for _, dir := range []string{".", "../engine/whatsmeow", "../session", "../app"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !isGoSource(name) {
				continue
			}
			body, err := os.ReadFile(dir + "/" + name)
			if err != nil {
				t.Fatalf("read %s: %v", name, err)
			}
			if contains(string(body), "PrepareContext") && name != "sqlitebudget.go" && name != "sqlitebudget_test.go" {
				t.Errorf("%s/%s calls PrepareContext: statements prepared on this handle run "+
					"outside the budget, so either budget the statement or say why it is safe",
					dir, name)
			}
		}
	}
}

func isGoSource(name string) bool {
	return len(name) > 3 && name[len(name)-3:] == ".go"
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// PostgreSQL has no `busy_timeout`, is not given a wrapped connector, and must not get
// worse: the dialect a real deployment runs is not the one this change is about. What is
// asserted is the property, not the absence of the wrapper -- a contended write there is
// stopped by the caller's deadline, and two of them cost two deadlines rather than
// stacking into something longer.
func TestPostgresStillStopsOnTheCallersDeadline(t *testing.T) {
	t.Parallel()

	address := os.Getenv("WAC_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("no PostgreSQL named; the SQLite pass does not exercise this")
	}
	target := storetest.New(t)
	if !target.Postgres() {
		t.Skip("the named server is not PostgreSQL")
	}
	container, err := Open(context.Background(), target.URL, AlwaysOwned, zerolog.Nop())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })

	if _, err := container.DB().ExecContext(context.Background(),
		`CREATE TABLE IF NOT EXISTS wac293 (k TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := container.DB().ExecContext(context.Background(),
		`INSERT INTO wac293 VALUES ('held') ON CONFLICT DO NOTHING`); err != nil {
		t.Fatalf("seed the row: %v", err)
	}

	// A second pool holding the row, which is PostgreSQL's shape of the same contention.
	holder, err := sql.Open("postgres", target.URL)
	if err != nil {
		t.Fatalf("open the holder: %v", err)
	}
	defer func() { _ = holder.Close() }()
	tx, err := holder.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin the holding transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(context.Background(),
		`SELECT k FROM wac293 WHERE k = 'held' FOR UPDATE`); err != nil {
		t.Fatalf("take the row lock: %v", err)
	}

	began := time.Now()
	for range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		_, err := container.DB().ExecContext(ctx,
			`UPDATE wac293 SET k = 'held' WHERE k = 'held'`)
		cancel()
		if err == nil {
			t.Fatal("the update went through, so the row was not locked")
		}
		if ctx.Err() == nil {
			t.Fatalf("the update failed for something other than the deadline: %v", err)
		}
	}
	// Two calls, two deadlines. The SQLite defect was that each call paid a whole
	// `busy_timeout` on top of the last; this is the assertion that PostgreSQL never did.
	if took := time.Since(began); took > 1200*time.Millisecond {
		t.Errorf("two contended writes under 300ms each took %s: they are stacking rather than "+
			"each ending on its own deadline", took)
	}
}

// The wrapper declares each of those methods, so the table above can no longer catch one
// going missing from the driver: what catches it is the open refusing. This is that.
//
// A `driver.Conn` that implements nothing beyond the interface itself stands in for a pin
// that dropped one, and the refusal has to name which, because "wrapping its connection
// would drop it" is only actionable with the name in it.
func TestADriverThatStoppedOfferingOneOfThemFailsTheOpen(t *testing.T) {
	t.Parallel()

	_, err := newBudgetedConn(bareConn{})
	if err == nil {
		t.Fatal("a connection offering none of the optional interfaces was wrapped anyway, " +
			"so database/sql would fall back to the paths this file exists to keep")
	}
	// Every one of them, not the first: a pin bump that drops two should cost one round
	// of reading rather than two.
	for _, name := range []string{
		"ExecerContext", "QueryerContext", "ConnBeginTx", "ConnPrepareContext",
		"SessionResetter", "Validator", "Pinger",
	} {
		if !strings.Contains(err.Error(), "driver."+name) {
			t.Errorf("the refusal does not name driver.%s, which this connection is missing: %v",
				name, err)
		}
	}
}

// bareConn implements `driver.Conn` and nothing else.
type bareConn struct{}

func (bareConn) Prepare(string) (driver.Stmt, error) { return nil, errors.ErrUnsupported }
func (bareConn) Close() error                        { return nil }
func (bareConn) Begin() (driver.Tx, error)           { return nil, errors.ErrUnsupported }

// The number that ends up on the connection is the caller's remaining time plus the slack,
// read back on the very connection the statement runs on.
//
// Going through `db.QueryContext` is the point: the read is itself a statement, so what it
// reports is what `budget` installed for that statement's own context. Without it, the
// slack and the arithmetic around it would only be observable as wall-clock timing, and a
// test that measures the slack by waiting for it cannot tell 50ms of ceiling from 50ms of
// a loaded machine.
func TestTheBudgetOnTheConnectionIsTheCallersTimePlusTheSlack(t *testing.T) {
	t.Parallel()

	path, _, release := aFileWithASecondWriter(t)
	defer release()
	db := budgeted(t, path, func() {})

	for _, tc := range []struct {
		name     string
		deadline time.Duration
	}{
		{"a tight caller", 300 * time.Millisecond},
		{"a roomier one", 3 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), tc.deadline)
			defer cancel()

			// The bounds are read off the same clock the budget is, on either side of
			// the call, rather than assumed from the timeout: the pragma is installed
			// somewhere inside this window, so the value has to be what was left at
			// some instant in it. A fixed allowance would instead be a bet on how long
			// a loaded runner takes to get here, which under `-race` and t.Parallel is
			// not a bet worth making.
			before := time.Until(deadlineOf(ctx, t))
			var got int64
			if err := db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&got); err != nil {
				t.Fatalf("read back busy_timeout: %v", err)
			}
			after := time.Until(deadlineOf(ctx, t))

			low, up := after.Milliseconds()+slackMs, before.Milliseconds()+slackMs
			if got < low || got > up {
				t.Errorf("busy_timeout on the connection is %dms, wanted %d..%dms: the "+
					"statement is not running under the ceiling its caller asked for",
					got, low, up)
			}
		})
	}

	// No deadline: the DSN's own value, so a background writer does not lose the backstop
	// that exists to keep `database is locked` off the pairing path.
	t.Run("one with no deadline at all", func(t *testing.T) {
		t.Parallel()

		var got int
		if err := db.QueryRowContext(context.Background(), "PRAGMA busy_timeout").Scan(&got); err != nil {
			t.Fatalf("read back busy_timeout: %v", err)
		}
		if want := whatTheDSNOpensWith(t, path); got != want {
			t.Errorf("busy_timeout on the connection is %dms, wanted the DSN's %dms", got, want)
		}
	})
}

// slackMs is `budgetSlack` in the unit the pragma reports, so the bounds above move with
// the constant instead of restating it.
var slackMs = budgetSlack.Milliseconds()

// Which is exactly why the value of the constant needs its own assertion: bounds derived
// from it follow it anywhere, zero included, and zero is the one value that breaks what it
// is for. At zero the busy wait and the context expire together and the error a caller
// gets is the scheduler's to pick -- `context deadline exceeded`, which every caller above
// reads with `errors.Is`, or SQLite's `database is locked`, which none of them do.
func TestTheSlackIsLongerThanTheDeadlineItIsDerivedFrom(t *testing.T) {
	t.Parallel()

	if budgetSlack <= 0 {
		t.Errorf("budgetSlack is %v: the two clocks expire at the same instant, so which "+
			"error a contended write returns stops being decided and starts being raced",
			budgetSlack)
	}
	// And not so long that the ceiling stops being the caller's. A `storeLimit` is
	// measured in seconds; a slack of that order would be a second ceiling in disguise.
	if budgetSlack > 500*time.Millisecond {
		t.Errorf("budgetSlack is %v, which is a meaningful share of the ceilings callers "+
			"actually set: the wait is no longer the caller's", budgetSlack)
	}
}

func deadlineOf(ctx context.Context, t *testing.T) time.Time {
	t.Helper()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("the context carries no deadline, so there is nothing to derive a budget from")
	}
	return deadline
}

// Three things this file decides are true of its source and not of anything the pinned
// driver lets a test observe, so this is the only place that can hold them.
//
// The pragma runs on `context.Background()` rather than the caller's context. Measured on
// `modernc.org/sqlite v1.58.0`: an `ExecContext` under an already-cancelled context
// returns `context canceled` and applies the statement anyway, so whether the pragma lands
// is a race between the execution and the interrupt handler, won today because a pragma is
// fast. A test of the effect passes either way; what it would be testing is the race.
//
// `BeginTx` forwards its context and its options. This driver accepts `ReadOnly` and every
// isolation level and honours none of them -- measured: an INSERT inside a `ReadOnly`
// transaction succeeds -- so dropping `opts` costs nothing observable here and costs a
// silent wrong answer on any dialect or pin that does honour them.
//
// `IsValid` answers the driver rather than `true`. A connection the driver has given up on
// stays in the pool otherwise, and there is no way to make this driver give up on one from
// a test without reaching into it.
//
// Each of the three survived the mutation battery as a live mutant before this existed,
// which is what says the suite could not reach them.
func TestEachForwardGoesToTheDriverItNamed(t *testing.T) {
	t.Parallel()

	methods := budgetedConnMethods(t)
	for _, tc := range []struct {
		method string
		call   string
		args   []string
	}{
		// The pragma text and the nil argument list are not what this is about, so
		// they are wildcards; the context is the whole point.
		{"budget", "c.exec.ExecContext", []string{"context.Background()", "", ""}},
		{"ExecContext", "c.exec.ExecContext", []string{"ctx", "query", "args"}},
		{"QueryContext", "c.query.QueryContext", []string{"ctx", "query", "args"}},
		{"BeginTx", "c.begin.BeginTx", []string{"ctx", "opts"}},
		{"PrepareContext", "c.prepare.PrepareContext", []string{"ctx", "query"}},
		{"ResetSession", "c.reset.ResetSession", []string{"ctx"}},
		{"IsValid", "c.valid.IsValid", nil},
		{"Ping", "c.ping.Ping", []string{"ctx"}},
	} {
		t.Run(tc.method, func(t *testing.T) {
			t.Parallel()

			body, ok := methods[tc.method]
			if !ok {
				t.Fatalf("(*budgetedConn).%s is gone, so nothing forwards it any more", tc.method)
			}
			args, found := callArgs(body, tc.call)
			if !found {
				t.Fatalf("%s does not call %s: whatever it forwards to, it is not the "+
					"interface this connection resolved at open", tc.method, tc.call)
			}
			if len(args) != len(tc.args) {
				t.Fatalf("%s calls %s with %d arguments, wanted %d: %v",
					tc.method, tc.call, len(args), len(tc.args), args)
			}
			for i, want := range tc.args {
				if want == "" {
					continue // a wildcard: this position is not what the row is about
				}
				if args[i] != want {
					t.Errorf("%s passes %q to %s where argument %d should be %q",
						tc.method, args[i], tc.call, i, want)
				}
			}
		})
	}
	// The budget has to be taken before the statement runs, not after, and on the same
	// call. `budget` itself is excluded: it is what installs the pragma.
	for _, method := range []string{"ExecContext", "QueryContext", "BeginTx"} {
		if _, found := callArgs(methods[method], "c.budget"); !found {
			t.Errorf("%s does not call c.budget, so it runs under whatever ceiling the "+
				"previous caller left on the connection", method)
		}
	}
	// And `PrepareContext` has to stay out of it, for the reason written on it.
	if _, found := callArgs(methods["PrepareContext"], "c.budget"); found {
		t.Error("PrepareContext budgets: the prepared statement runs through its own " +
			"ExecContext, so this is a ceiling that looks applied and is not")
	}
}

// budgetedConnMethods returns each `*budgetedConn` method in this package by name.
//
// Over the directory rather than over this one file: a method moved to a new file would
// otherwise read as deleted, and the fence would pass on a package it no longer describes.
func budgetedConnMethods(t *testing.T) map[string]*ast.FuncDecl {
	t.Helper()

	found := map[string]*ast.FuncDecl{}
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}
			star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			if ident, ok := star.X.(*ast.Ident); ok && ident.Name == "budgetedConn" {
				found[fn.Name.Name] = fn
			}
		}
	}
	if len(found) == 0 {
		t.Fatal("no *budgetedConn methods in this package, so this fence is measuring nothing")
	}
	return found
}

// callArgs finds a call to `want` inside body and returns its arguments as written.
func callArgs(body *ast.FuncDecl, want string) ([]string, bool) {
	if body == nil {
		return nil, false
	}
	var args []string
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || found || render(call.Fun) != want {
			return true
		}
		found = true
		for _, arg := range call.Args {
			args = append(args, render(arg))
		}
		return false
	})
	return args, found
}

// render prints an expression back the way it was written, which is what the table above
// compares against.
func render(node ast.Expr) string {
	var out strings.Builder
	if err := printer.Fprint(&out, token.NewFileSet(), node); err != nil {
		return ""
	}
	return out.String()
}

// The DSN's value is read off the connection, not parsed out of the DSN, and this is why.
//
// `modernc.org/sqlite` accepts several spellings of the same pragma and `sqliteDefaults`
// leaves an operator's alone once it recognises the name, so a DSN can legitimately arrive
// in any of these. A parser that knows one spelling answers zero for the others, and zero
// is not a small error here: it is the ceiling every call with no deadline of its own
// inherits, and at zero a contended write fails on the spot with `database is locked`,
// which is the pairing failure `busy_timeout` was added to stop.
func TestAnOperatorsOwnSpellingOfBusyTimeoutIsNotReadAsZero(t *testing.T) {
	t.Parallel()

	for _, spelling := range []string{
		"busy_timeout(7777)",
		"busy_timeout=7777",
		" busy_timeout(7777) ",
		"busy_timeout (7777)",
	} {
		t.Run(spelling, func(t *testing.T) {
			t.Parallel()

			dsn := "sqlite:" + t.TempDir() + "/spelled.db?_pragma=" + url.QueryEscape(spelling)
			container, err := Open(t.Context(), dsn, AlwaysOwned, zerolog.Nop())
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = container.Close() }()

			var onTheConnection int
			if err := container.DB().QueryRowContext(context.Background(),
				"PRAGMA busy_timeout").Scan(&onTheConnection); err != nil {
				t.Fatalf("read the pragma back: %v", err)
			}
			if onTheConnection != 7777 {
				t.Errorf("a call with no deadline left %dms on a connection opened with %q, "+
					"want the 7777ms the operator asked for", onTheConnection, spelling)
			}
		})
	}
}
