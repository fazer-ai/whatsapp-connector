package cluster_test

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// The backoff is the whole mechanism: an account that cannot connect has to cost less
// and less, and an account that is merely away for a moment has to be tried again soon.
// A flat wait gets one of those wrong whichever value it takes.
func TestTheWaitDoublesWithEveryFailureAndStopsAtTheCeiling(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		strikes int64
		want    time.Duration
	}{
		{0, cluster.QuarantineFloor},
		{1, cluster.QuarantineFloor},
		{2, 2 * time.Minute},
		{3, 4 * time.Minute},
		{6, 32 * time.Minute},
		{7, cluster.QuarantineCap},
		{50, cluster.QuarantineCap},
	} {
		if got := cluster.QuarantineWait(test.strikes); got != test.want {
			t.Errorf("after %d failures the fleet waits %s, want %s", test.strikes, got, test.want)
		}
	}
}

// Fleet-wide by construction: the count is in Redis, so the instance that reads it is
// not the one that wrote it. A count kept in a process would give every instance its own
// backoff, and the fleet would attempt the same broken account N times per window.
func TestAFailureOneInstanceRecordsIsAWaitTheOthersObserve(t *testing.T) {
	t.Parallel()

	clock := newClock()
	server, _, _ := newFleet(t, clock)
	writing := quarantineOn(t, server.Addr(), clock)
	reading := quarantineOn(t, server.Addr(), clock)
	ctx := t.Context()

	until, err := writing.Strike(ctx, "s1")
	if err != nil {
		t.Fatalf("Strike: %v", err)
	}
	if want := clock.Now().Add(cluster.QuarantineFloor); !until.Equal(want) {
		t.Fatalf("the first failure waits until %s, want %s", until, want)
	}

	waiting, err := reading.Waiting(ctx, []string{"s1", "s2"})
	if err != nil {
		t.Fatalf("Waiting: %v", err)
	}
	if _, held := waiting["s1"]; !held {
		t.Fatal("a peer does not see the account this instance just failed on; the backoff would be divided by the fleet")
	}
	if _, held := waiting["s2"]; held {
		t.Fatal("an account nothing failed on is being left alone")
	}

	// And the wait ends on its own: the sweep is what tries again, and it has to be
	// allowed to.
	clock.advance(cluster.QuarantineFloor + time.Second)
	waiting, err = reading.Waiting(ctx, []string{"s1"})
	if err != nil {
		t.Fatalf("Waiting: %v", err)
	}
	if len(waiting) != 0 {
		t.Fatal("the wait never ended, so nothing would ever try the account again")
	}
}

// A second failure is longer than the first, and the count that makes it longer survives
// the wait it produced: a record that expired with its own deadline would restart the
// backoff at the floor every time, and an account that is permanently gone would cost a
// minute forever.
func TestAFailureAfterTheWaitIsLongerThanTheOneBeforeIt(t *testing.T) {
	t.Parallel()

	clock := newClock()
	server, _, _ := newFleet(t, clock)
	quarantine := quarantineOn(t, server.Addr(), clock)
	ctx := t.Context()

	if _, err := quarantine.Strike(ctx, "s1"); err != nil {
		t.Fatalf("Strike: %v", err)
	}
	clock.advance(cluster.QuarantineFloor + time.Second)
	server.FastForward(cluster.QuarantineFloor + time.Second)

	until, err := quarantine.Strike(ctx, "s1")
	if err != nil {
		t.Fatalf("Strike: %v", err)
	}
	if want := clock.Now().Add(2 * cluster.QuarantineFloor); !until.Equal(want) {
		t.Fatalf("the second failure waits until %s, want %s: the count did not outlive the wait", until, want)
	}
}

// What ends a quarantine is the account working, and it has to end completely: a session
// that connects and falls over again months later is not one to leave alone for an hour
// on its first failure.
func TestAConnectionThatWorkedForgetsEveryFailureBeforeIt(t *testing.T) {
	t.Parallel()

	clock := newClock()
	server, _, _ := newFleet(t, clock)
	quarantine := quarantineOn(t, server.Addr(), clock)
	ctx := t.Context()

	for range 3 {
		if _, err := quarantine.Strike(ctx, "s1"); err != nil {
			t.Fatalf("Strike: %v", err)
		}
	}
	if err := quarantine.Clear(ctx, "s1"); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	waiting, err := quarantine.Waiting(ctx, []string{"s1"})
	if err != nil {
		t.Fatalf("Waiting: %v", err)
	}
	if len(waiting) != 0 {
		t.Fatal("an account that is connected is still being left alone")
	}

	until, err := quarantine.Strike(ctx, "s1")
	if err != nil {
		t.Fatalf("Strike: %v", err)
	}
	if want := clock.Now().Add(cluster.QuarantineFloor); !until.Equal(want) {
		t.Fatalf("the failure after a working connection waits until %s, want the floor at %s", until, want)
	}
}

func quarantineOn(t *testing.T, addr string, clock cluster.Clock) *cluster.Quarantine {
	t.Helper()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	return cluster.NewQuarantine(redisx.Wrap(rdb, "wa:", 8), clock.Now)
}
