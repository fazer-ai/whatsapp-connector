// Package storetest decides where a test's database lives.
//
// The store speaks two dialects and a deployment runs the one the tests did not. A
// statement that is valid in SQLite and not in Postgres compiles, lints and passes the
// whole suite, because `rebind` turns `?` into `$1` on both and neither the type system
// nor the linter ever executes it. The first execution is in a running deployment, on
// whichever path happens to reach the statement.
//
// So the suite runs twice against the same code: SQLite by default, and Postgres when
// WAC_TEST_DATABASE_URL names a server. There is no separate Postgres suite to keep in
// step, which is the point -- a test written for one dialect is a test of both, and a
// dialect difference fails the run that has one.
package storetest

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	// The same two drivers the store opens. Registered here as well because a test
	// that looks at the database after the container closed it opens its own pool.
	_ "github.com/lib/pq"
	_ "modernc.org/sqlite"
)

// AddressEnv names the server the suite runs its second pass against. Unset is not a
// skip and not a failure: it is the SQLite pass, which is what a developer gets by
// running `go test` and what a single-instance deployment actually uses.
const AddressEnv = "WAC_TEST_DATABASE_URL"

// Target is one test's own database: a file under SQLite, a database on the server
// under Postgres. Both are torn down with the test.
type Target struct {
	// URL is what store.Open takes.
	URL string

	driver string
	dsn    string
}

// New gives the test a database nothing else is using.
func New(t *testing.T) Target {
	t.Helper()

	server := os.Getenv(AddressEnv)
	if server == "" {
		path := t.TempDir() + "/wac.db"
		return Target{URL: "sqlite:" + path, driver: "sqlite", dsn: path}
	}
	return newDatabase(t, server)
}

// Postgres reports which pass this is, for the handful of tests that are about a
// dialect rather than about the store. There should be very few: a test that branches
// on this is a test that only half runs, so the bar for one is that the other dialect
// has nothing to answer, not that making it run on both is awkward.
func (tg Target) Postgres() bool { return tg.driver == "postgres" }

// Rebind turns `?` placeholders into `$1`-style ones, for the tests that write their
// own SQL against the schema instead of going through the store. It is the same
// translation Container.rebind does, and it is duplicated rather than exported because
// a test reaching around the store is the test's business: exporting it would put a
// method on the production type that nothing in production calls.
func (tg Target) Rebind(query string) string {
	if !tg.Postgres() {
		return query
	}
	var out strings.Builder
	out.Grow(len(query) + 8)
	n := 0
	for i := range len(query) {
		if query[i] != '?' {
			out.WriteByte(query[i])
			continue
		}
		n++
		out.WriteByte('$')
		out.WriteString(strconv.Itoa(n))
	}
	return out.String()
}

// Pool opens a second connection to the same database, for the tests that read the
// schema back after the container has closed it. It runs no migration: the tests that
// want one call store.Open, and the ones that call this are checking what a failed
// Open left behind.
func (tg Target) Pool(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open(tg.driver, tg.dsn)
	if err != nil {
		t.Fatalf("open a second pool on %s: %v", tg.driver, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// admin is the pool the per-test databases are created through. One for the whole run:
// the tests hold their own connections, and a second pool per test to issue two DDL
// statements would double the count against a server that caps it.
var (
	adminOnce sync.Once
	adminDB   *sql.DB
	adminErr  error
	nameSeq   atomic.Uint64
)

// newDatabase hands the test a database of its own on the configured server.
//
// A database rather than a schema, which is the cheaper isolation and the one that does
// not work here. whatsmeow brings its schema up through go.mau.fi/util/dbutil, whose
// Postgres existence checks read `information_schema.tables` and `.columns` filtered by
// table name alone, with no schema condition. Those views span every schema the role can
// see, so with a schema per test the migration of the second test finds the first test's
// `whatsmeow_version`, concludes the table is already there, and then fails on the
// ALTER against its own schema, which has nothing. A database is opaque to
// information_schema, so each test's migration sees only its own work.
func newDatabase(t *testing.T, server string) Target {
	t.Helper()

	adminOnce.Do(func() {
		adminDB, adminErr = sql.Open("postgres", server)
		if adminErr != nil {
			return
		}
		// CREATE DATABASE serialises on the template anyway, and the admin pool issues
		// two short statements per test and then sits idle. Left uncapped it would open
		// a connection per parallel test on top of the ones those tests already hold,
		// which is how a package that passes one test at a time fails as
		// `sorry, too many clients already` when it is run whole.
		adminDB.SetMaxOpenConns(4)
	})
	if adminErr != nil {
		t.Fatalf("open %s: %v", AddressEnv, adminErr)
	}

	name := databaseName(t)
	// Concatenated rather than bound: a placeholder cannot carry an identifier, and
	// CREATE DATABASE takes no parameters at all.
	if _, err := adminDB.ExecContext(t.Context(), `CREATE DATABASE `+quote(name)); err != nil {
		t.Fatalf("create the database for this test: %v", err)
	}
	t.Cleanup(func() {
		// A context of its own: t.Context() is already cancelled by the time cleanup
		// runs, and a database left behind is one the next run collides with under the
		// same name. Bounded rather than Background, so a server that stops answering
		// fails the test that noticed instead of hanging the suite.
		//
		// FORCE because a pool closes its idle connections and does not wait for the
		// server to notice; without it the drop loses a race it has no reason to be in.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := adminDB.ExecContext(ctx, `DROP DATABASE `+quote(name)+` WITH (FORCE)`); err != nil {
			t.Errorf("drop the database for this test: %v", err)
		}
	})

	dsn, err := url.Parse(server)
	if err != nil {
		t.Fatalf("parse %s: %v", AddressEnv, err)
	}
	dsn.Path = "/" + name
	return Target{URL: dsn.String(), driver: "postgres", dsn: dsn.String()}
}

// run tells this process's databases apart from every other process's on the same
// server. The counter below cannot: it is process-local, so it restarts at 1 in each
// `go test` binary -- and `go test ./...` is several of those at once -- and it hands
// the same name to the same test on every run, so a run interrupted before its cleanup
// leaves behind exactly the database the next run tries to create.
//
// Debris is the trade. Under the counter alone a crashed run blocked the next one,
// loudly; under this it accumulates quietly instead. A CI runner is thrown away either
// way, and a developer's server takes
// `DROP DATABASE` over what `SELECT datname FROM pg_database WHERE datname LIKE 'wac\_%'`
// lists once the run that owned them is gone.
var run = func() string {
	var token [4]byte
	if _, err := rand.Read(token[:]); err != nil {
		// crypto/rand does not fail on any platform this builds for, and a helper that
		// carried on with a constant here would reintroduce the collision it exists to
		// avoid -- on the machine where it failed, silently.
		panic("storetest: read random bytes for the run id: " + err.Error())
	}
	return hex.EncodeToString(token[:])
}()

// databaseName builds an identifier that says which run and which test own it, because
// one left behind by a crash is only useful if it names them. The counter separates the
// tests within a run: two subtests can sanitise to the same string, and truncating at 32
// makes that likelier rather than less.
func databaseName(t *testing.T) string {
	t.Helper()

	var out strings.Builder
	for _, r := range strings.ToLower(t.Name()) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out.WriteRune(r)
		default:
			out.WriteByte('_')
		}
		if out.Len() >= 32 {
			break
		}
	}
	// Postgres truncates an identifier at 63 bytes, and truncation would collapse two
	// long names into one database two parallel tests then share. This is 4 + 8 + 1 +
	// 32 + 1 + the counter, so it has room the test name cannot eat.
	return fmt.Sprintf("wac_%s_%s_%d", run, out.String(), nameSeq.Add(1))
}

// quote wraps an identifier for DDL. The name is built from a test name and a counter
// so it holds nothing to escape, but the statements it goes into are concatenation --
// a placeholder cannot carry an identifier -- and an unquoted concatenated identifier is
// the shape that stops being safe the moment somebody feeds it something else.
func quote(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
