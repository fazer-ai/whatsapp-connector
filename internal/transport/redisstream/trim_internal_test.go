package redisstream

import "testing"

// The counters are the only thing that can answer this, and the numbers here are the ones
// measured on Redis 8 in issue #176: a stream of 10 entries, a group handed 3 of them,
// trimmed to 2. Five commands were lost and nothing else in Redis says so.
//
// Asserted on the pure reading rather than through the transport, because miniredis
// answers zero for both counters and would make every case below indistinguishable from
// the one where the server cannot say.
func TestWorkingOutHowManyCommandsATrimTook(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		added, read, length, lag int64
		wantLost                 int64
		wantKnown                bool
	}{
		// The measured cut: 7 owed, 2 left, so 5 no longer exist.
		"a trim reached entries the group had not been handed": {
			added: 10, read: 3, length: 2, lag: 2, wantLost: 5, wantKnown: true,
		},
		// The same trim, stopping where the group had already read. This is the case an
		// id comparison gets wrong: the oldest surviving entry is later than the last
		// delivered one here too, and nothing was lost.
		"a trim took only what had already been delivered": {
			added: 10, read: 3, length: 7, lag: 7, wantKnown: false,
		},
		"nothing was trimmed at all": {
			added: 10, read: 3, length: 10, lag: 7, wantKnown: false,
		},
		"the group is caught up on a stream at its cap": {
			added: 1000, read: 1000, length: 1000, lag: 0, wantKnown: false,
		},
		// A backlog is not a loss: everything owed is still there to be read.
		"a full backlog nobody has read yet": {
			added: 1000, read: 0, length: 1000, lag: 1000, wantKnown: false,
		},
		// Every entry cut before the group reached any of them.
		"the whole backlog was cut": {
			added: 1000, read: 0, length: 1, lag: 1, wantLost: 999, wantKnown: true,
		},
		// A server too old to report the counters at all. Nothing can be owed, so the
		// arithmetic is silent on its own.
		"the server does not report the counters": {
			added: 0, read: 0, length: 2, lag: 0, wantKnown: false,
		},
		// The one that needs a guard. A group carried across an upgrade from Redis 6 has
		// an unknown `entries-read`, which arrives here as zero and is indistinguishable
		// from a group that has read nothing -- so the arithmetic would call a thousand
		// ordinary trimmed entries a loss. Redis says it cannot work the position out by
		// answering null for `lag`, which arrives as -1.
		"the group's position cannot be worked out": {
			added: 2000, read: 0, length: 1000, lag: -1, wantKnown: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			lost, known := wasCut(test.added, test.read, test.length, test.lag)
			if known != test.wantKnown {
				t.Fatalf("wasCut(%d, %d, %d, %d) knew %v, want %v",
					test.added, test.read, test.length, test.lag, known, test.wantKnown)
			}
			if lost != test.wantLost {
				t.Fatalf("wasCut(%d, %d, %d, %d) counted %d lost, want %d",
					test.added, test.read, test.length, test.lag, lost, test.wantLost)
			}
		})
	}
}
