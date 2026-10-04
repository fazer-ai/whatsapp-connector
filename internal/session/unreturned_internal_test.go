package session

import "testing"

// A lease whose hand-back did not reach Redis stays queued in the orphans, and that is
// what the stop counts as left to expire. Sessions this stop did not give up are not its
// to count, even when they are queued.
func TestUnreturnedCountsOnlyTheGivenSessionsStillQueued(t *testing.T) {
	t.Parallel()

	m := &Manager{orphans: map[string]bool{"a": true, "b": false, "other": false}}

	if got := m.Unreturned([]string{"a", "b", "c"}); got != 2 {
		t.Errorf("Unreturned = %d, want 2 (a and b are queued, c went back)", got)
	}
	if got := m.Unreturned(nil); got != 0 {
		t.Errorf("Unreturned of no sessions = %d, want 0", got)
	}
}
