package session

import (
	"testing"
	"time"
)

// The window the strike gets is a number somebody measured, so it needs to fail when the
// arithmetic behind it moves.
//
// `failing` takes `sharing(ctx, ackTimeout)` of the caller's budget, and at the call site
// that matters -- an adoption whose engine would not open the account -- that caller is
// bounded by `releaseTimeout`. So the strike gets `min(ackTimeout, releaseTimeout/2)`, and
// the hand-back behind it gets what is left.
//
// That product was measured against a real Redis by holding the one command `Strike` sends
// before it can compute anything, and the knee is between 900ms and 1100ms of added
// latency: below it the mark lands and the account is paced, above it the mark is lost and
// the account is retried at the claim beat, which is the behaviour before this. Both
// numbers are written into the pull request, and neither is derived from anything a
// deployment can set -- they come from these two constants alone.
//
// Which is why this is a test and not a comment. Either constant moving would move the
// knee, leave the measured number describing a build that no longer exists, and break
// nothing: the suite would stay green, because every other test here uses a Redis that
// answers at once. Whoever changes one of them should measure again and rewrite the number,
// and this is what says so.
//
// What it holds is the budget, which is narrower than the knee, and the difference matters
// to whoever trusts it. The knee is that budget divided by how many round trips the mark
// spends, and today `Strike` spends its first on the `HINCRBY` this was measured against.
// Give it a command before that one and the arithmetic below is still a second, this stays
// green, and the knee drops without a word. So the number is owed a fresh measurement by
// anything that changes what a strike costs, not only by these two constants.
func TestTheStrikeGetsTheWindowTheMeasurementAssumed(t *testing.T) {
	t.Parallel()

	const measuredAt = time.Second
	if got := min(ackTimeout, releaseTimeout/2); got != measuredAt {
		t.Fatalf("the strike now gets %s of the hand-back's budget, and the knee in the pull request was measured at %s.\n"+
			"releaseTimeout=%s ackTimeout=%s. Measure the knee again against a real Redis and rewrite it, "+
			"or this branch is promising a backoff on arithmetic nobody has checked.",
			got, measuredAt, releaseTimeout, ackTimeout)
	}
}
