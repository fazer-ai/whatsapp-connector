package observability_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/fazer-ai/whatsapp-connector/internal/observability"
)

// A histogram whose top finite bucket is shorter than the episode it exists to show
// answers the same thing for a bad minute and a catastrophic hour: everything past the top
// falls into `+Inf`, and `+Inf` has no upper edge to read a latency off.
//
// The edges are held to by comparing the whole family rather than by looking for one
// substring, because the two failures this has to separate look alike from a distance. A
// top that is too short and a bucket set that simply moved both leave `le="60"` missing;
// only the full text says which, and only the full text catches an edge that exists but
// holds the wrong side of an episode.
//
// The numbers are not arbitrary. What makes this metric interesting is a consumer slow
// enough to fill a session's inbox, and whatsmeow's patience on a socket the server has
// stopped answering is KeepAliveMaxFailTime, three minutes -- so tens of seconds is the
// ordinary bad case rather than the extreme one, and 0.2s is the healthy floor the issue
// asks it to be comparable against.
func TestTheStateNoticeDelaySeparatesTheHealthyFloorFromTheTail(t *testing.T) {
	t.Parallel()

	metrics := observability.New()
	metrics.StateNoticeDelay.Observe(45)
	metrics.StateNoticeDelay.Observe(0.2)

	const want = `# HELP wac_state_notice_delay_seconds Time from this connector judging a session's connection lost to the state frame reaching the shard, including the wait for the transition lock and for room in the session's inbox.
# TYPE wac_state_notice_delay_seconds histogram
wac_state_notice_delay_seconds_bucket{le="0.01"} 0
wac_state_notice_delay_seconds_bucket{le="0.1"} 0
wac_state_notice_delay_seconds_bucket{le="0.5"} 1
wac_state_notice_delay_seconds_bucket{le="1"} 1
wac_state_notice_delay_seconds_bucket{le="2"} 1
wac_state_notice_delay_seconds_bucket{le="5"} 1
wac_state_notice_delay_seconds_bucket{le="15"} 1
wac_state_notice_delay_seconds_bucket{le="60"} 2
wac_state_notice_delay_seconds_bucket{le="+Inf"} 2
wac_state_notice_delay_seconds_sum 45.2
wac_state_notice_delay_seconds_count 2
`
	if err := testutil.CollectAndCompare(metrics.StateNoticeDelay, strings.NewReader(want), "wac_state_notice_delay_seconds"); err != nil {
		t.Errorf("%v\n\nThe 45s episode has to land under a finite edge and the 0.2s floor has to land well below it. "+
			"Sharing `+Inf` is the failure this exists for, and it is invisible in a `_count` that looks perfectly healthy.", err)
	}
}

// The metric is exported under the name an operator will type, and a rename is a broken
// dashboard rather than a compile error.
func TestTheStateNoticeDelayIsExportedUnderItsName(t *testing.T) {
	t.Parallel()

	metrics := observability.New()
	found, err := testutil.GatherAndCount(metrics.Registry, "wac_state_notice_delay_seconds")
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	if found != 1 {
		t.Errorf("gathered %d families named wac_state_notice_delay_seconds, want 1: "+
			"a histogram that is registered on nothing an operator scrapes measures nothing", found)
	}
}
