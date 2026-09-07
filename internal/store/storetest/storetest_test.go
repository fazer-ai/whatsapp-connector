package storetest

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// A drop that never got a connection is congestion this package inflicts on itself, and
// the test it belonged to has already passed or failed on its own merits. Failing on it
// reports several unrelated tests as breaking at once a little past the bound, because
// they queue on the same four connections and give up together -- which is what #71
// recorded and what nobody could diagnose afterwards, since the failures name the
// harness rather than anything the tests had in common.
func TestADropThatNeverGotAConnectionIsCongestion(t *testing.T) {
	if os.Getenv(AddressEnv) == "" {
		t.Skipf("%s is unset, so there is no server to be busy", AddressEnv)
	}
	// Through New, which is what opens the admin pool the drop below goes through.
	New(t)

	spent, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-spent.Done()

	err := dropDatabase(spent, adminDB, databaseName(t))
	if err == nil {
		t.Fatal("a drop on a context with no time left reported success, so this proves nothing")
	}
	if !errors.Is(err, errQueued) {
		t.Fatalf("a drop that never got a connection is not reported as congestion: %v", err)
	}
}

// And the other half: a drop that reached the server and failed there has to stay loud,
// which is the same branch a server that stops answering mid-statement lands in. A name
// no database answers to is the reachable version of that.
func TestADropTheServerRefusedIsNotCongestion(t *testing.T) {
	if os.Getenv(AddressEnv) == "" {
		t.Skipf("%s is unset, so there is no server to refuse", AddressEnv)
	}
	New(t)

	ctx, cancel := context.WithTimeout(context.Background(), dropTimeout)
	defer cancel()

	err := dropDatabase(ctx, adminDB, "wac_no_such_database_for_"+databaseName(t))
	if err == nil {
		t.Fatal("dropping a database that does not exist reported success")
	}
	if errors.Is(err, errQueued) {
		t.Fatalf("a drop the server refused is filed as congestion, so a broken server would pass quietly: %v", err)
	}
}

// The whole point, on the cleanup rather than on the drop: a test that did its job is
// not failed by the tidying up after it. The bound is shortened here to reach in a
// moment the branch that ordinarily needs a loaded machine to produce.
func TestACleanupThatRanOutOfTimeDoesNotFailItsTest(t *testing.T) {
	if os.Getenv(AddressEnv) == "" {
		t.Skipf("%s is unset, so there is no database to leave behind", AddressEnv)
	}
	restore := dropTimeout
	dropTimeout = time.Nanosecond
	defer func() { dropTimeout = restore }()

	var left Target
	passed := t.Run("a test whose cleanup runs out of time", func(inner *testing.T) {
		left = New(inner)
	})
	// Dropped properly here: the subtest was made to give up on purpose, and debris a
	// test creates deliberately is debris that test owns.
	t.Cleanup(func() {
		dropTimeout = restore
		ctx, cancel := context.WithTimeout(context.Background(), dropTimeout)
		defer cancel()
		if name, err := databaseIn(left.dsn); err == nil {
			_ = dropDatabase(ctx, adminDB, name)
		}
	})

	if !passed {
		t.Fatal("a cleanup that ran out of time failed the test it belonged to, " +
			"which is how a busy server reads as several unrelated tests breaking at once")
	}
}

// databaseIn is the database a dsn names, for the one test that has to tidy up after a
// cleanup it stopped on purpose.
func databaseIn(dsn string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(parsed.Path, "/"), nil
}

// The case the split exists for: a drop that got its connection and then ran out of time
// is a server that stopped answering, not a queue. Both halves end on the same context,
// so a single check on that context would file the two under the same heading -- and the
// queue is the harmless one.
func TestADropThatStalledAfterItStartedIsNotCongestion(t *testing.T) {
	if os.Getenv(AddressEnv) == "" {
		t.Skipf("%s is unset, so there is no server to stall", AddressEnv)
	}
	New(t)

	conn, err := adminDB.Conn(context.Background())
	if err != nil {
		t.Fatalf("take an admin connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// The connection is already in hand, so what runs out below is the statement's time
	// and not the pool's.
	spent, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-spent.Done()

	err = dropOn(spent, conn, databaseName(t))
	if err == nil {
		t.Fatal("a statement on a context with no time left reported success, so this proves nothing")
	}
	if errors.Is(err, errQueued) {
		t.Fatalf("a server that stopped answering mid-drop is filed as congestion, so it would pass quietly: %v", err)
	}
}

// A pool with room that still cannot hand over a connection is a server that went away,
// not a queue. Filing it under congestion would let the suite pass quietly on a
// PostgreSQL that died after the tests themselves had finished.
func TestAConnectionThatCouldNotBeMadeIsNotCongestion(t *testing.T) {
	if os.Getenv(AddressEnv) == "" {
		t.Skipf("%s is unset, so there is no server to compare against", AddressEnv)
	}
	// A pool of its own, pointed at nothing: no waiting is involved, the dial simply
	// fails.
	gone, err := sql.Open("postgres", "postgres://wac:wac@127.0.0.1:1/wac?sslmode=disable")
	if err != nil {
		t.Fatalf("open a pool pointing nowhere: %v", err)
	}
	defer func() { _ = gone.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = dropDatabase(ctx, gone, "wac_whatever")
	if err == nil {
		t.Fatal("a drop against a server that is not there reported success")
	}
	if errors.Is(err, errQueued) {
		t.Fatalf("a server that could not be reached is filed as congestion, so it would pass quietly: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("the context ran out, so this measured a deadline rather than a refused connection")
	}
}

// The statement is bounded from when it starts, not from what the queue left over. A
// connection that comes free a moment before the caller's deadline would otherwise leave
// the drop to fail as a stalled server -- the loud reading, produced by the very
// congestion this is meant to forgive.
func TestAQueuedDropStillGetsAFullTimeoutToRunIn(t *testing.T) {
	if os.Getenv(AddressEnv) == "" {
		t.Skipf("%s is unset, so there is nothing to drop", AddressEnv)
	}
	// One through New to open the admin pool, and a second one by hand for this test to
	// drop: a database New handed out is dropped again by its own cleanup, which would
	// then fail on the one this test already removed.
	New(t)
	name := databaseName(t)
	if _, err := adminDB.ExecContext(context.Background(), `CREATE DATABASE `+quote(name)); err != nil {
		t.Fatalf("create a database to drop: %v", err)
	}

	// Long enough to take a connection from an idle pool, far short of a drop: the
	// fastest drop measured on this suite was 42ms.
	nearlySpent, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := dropDatabase(nearlySpent, adminDB, name); err != nil {
		t.Fatalf("a drop that got its connection in time still failed on the caller's leftovers: %v", err)
	}
}
