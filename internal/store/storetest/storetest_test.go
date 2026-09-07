package storetest

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// A test that did its job is not failed by the tidying up after it. The bound is
// shortened here to reach in a moment the branch that ordinarily needs a machine under
// load to produce: several unrelated tests giving up together a little past thirty
// seconds, queueing on the same four admin connections, which is what #71 recorded and
// what nobody could diagnose from the failures afterwards.
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
