package storetest

import (
	"context"
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

	err := dropDatabase(spent, databaseName(t))
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

	err := dropDatabase(ctx, "wac_no_such_database_for_"+databaseName(t))
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
			_ = dropDatabase(ctx, name)
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
