package app

import (
	"slices"
	"strconv"
	"testing"
)

// Reclaiming runs on the goroutine that renews every lease this instance holds, and
// costs at least one round trip per stream, so a pass over hundreds of sessions can take
// longer than a lease survives. A window keeps the pass short; rotating it is what keeps
// the window from being a permanent blind spot over everything outside it.
func TestTheReclaimWindowReachesEverySessionInTurn(t *testing.T) {
	t.Parallel()

	sids := make([]string, 0, maxReclaimStreams*3+5)
	for i := range cap(sids) {
		sids = append(sids, "s"+strconv.Itoa(i))
	}

	connector := &Connector{}
	seen := make(map[string]int, len(sids))
	// Enough passes to cover the list, and no more: if it takes longer than this, the
	// rotation is resampling rather than advancing.
	passes := (len(sids) + maxReclaimStreams - 1) / maxReclaimStreams
	for range passes {
		window := connector.windowOver(slices.Clone(sids))
		if len(window) != maxReclaimStreams {
			t.Fatalf("a pass covered %d streams, want %d", len(window), maxReclaimStreams)
		}
		for _, sid := range window {
			seen[sid]++
		}
	}

	for _, sid := range sids {
		if seen[sid] == 0 {
			t.Fatalf("%s was never reclaimed in %d passes over %d sessions", sid, passes, len(sids))
		}
	}
}

// A fleet member holding few sessions pays for no rotation at all.
func TestASmallInstanceReclaimsEverythingEveryPass(t *testing.T) {
	t.Parallel()

	connector := &Connector{}
	sids := []string{"s1", "s2", "s3"}
	for range 3 {
		window := connector.windowOver(slices.Clone(sids))
		if !slices.Equal(window, sids) {
			t.Fatalf("a pass covered %v, want all of %v", window, sids)
		}
	}
}
