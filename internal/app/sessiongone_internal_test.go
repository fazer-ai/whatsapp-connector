package app

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/fazer-ai/whatsapp-connector/internal/observability"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// A session this instance stops running takes its series with it, at the moment it stops
// and not thirty minutes later.
//
// The sealed scenario s10 asks for exactly this: the series labelled with a session has
// to be gone from the exposition within a few heartbeats of the instance losing it,
// because otherwise the set of series is "every session this instance ever had" rather
// than "the ones it has". Eviction by silence alone left it there for `labelQuiet`, which
// is three hundred times the scenario's window.
//
// What this is NOT is eviction by ownership, which an earlier round tried and reverted:
// that swept every session this instance does not own, and a `session.wake` the fleet
// cannot adopt names a session nobody owns, so it deleted the one series the metric was
// built for on every heartbeat. This fires only for a session this instance was running
// and stopped running, which is the event that increments `wac_leases_lost_total`, and
// it never touches a session this instance never had.
func TestASessionThisInstanceStopsRunningTakesItsSeriesWithIt(t *testing.T) {
	t.Parallel()

	metrics := observability.New()
	c := &Connector{metrics: metrics, cfg: Config{Instance: "inst-owner"}}

	// One this instance owns and is counting against.
	c.measure([]transport.Delivery{{
		Command:         protocol.Command{SID: "wac224-mine", Type: protocol.CommandSessionConnect},
		DeliveredBefore: true,
		TakenFrom:       "inst-gone",
		Deliveries:      2,
	}})
	// And the wake nobody can adopt, which names a session no instance holds.
	c.measure([]transport.Delivery{{
		Command:         protocol.Command{SID: "wac224-orphan", Type: protocol.CommandSessionWake},
		DeliveredBefore: true,
		TakenFrom:       "inst-owner",
		Deliveries:      9,
	}})
	if got := testutil.CollectAndCount(metrics.CommandsDeliveredAgain, "wac_commands_delivered_again_total"); got != 2 {
		t.Fatalf("the exposition has %d series before anything is lost, want 2", got)
	}

	c.forgetSession("wac224-mine")

	const left = `# HELP wac_commands_delivered_again_total Commands handed out that had been handed out before, by where this delivery came from. source=restored is one this instance gave back unrun and took again through its own claim, which is the loop a wake nobody can act on makes; source=claim is one a claim took off another consumer; source=read is one that came back out of the pending history in a read.
# TYPE wac_commands_delivered_again_total counter
wac_commands_delivered_again_total{sid="wac224-orphan",source="restored"} 1
`
	if err := testutil.CollectAndCompare(metrics.CommandsDeliveredAgain, strings.NewReader(left), "wac_commands_delivered_again_total"); err != nil {
		t.Errorf("what a scrape shows after the instance stops running one session:\n%v", err)
	}

	// And the eviction by silence no longer has anything to say about it: a session
	// already forgotten must not be swept a second time, and the orphan has to survive.
	c.forgetLabelsGoneQuiet(time.Now().Add(labelQuiet + time.Second))
	if got := testutil.CollectAndCount(metrics.CommandsDeliveredAgain, "wac_commands_delivered_again_total"); got != 0 {
		t.Errorf("after the quiet window the exposition still has %d series", got)
	}
}

// And the wiring: the session layer's report of a lease it stopped running a session
// over is what calls it. Written because the eviction being correct and the eviction
// being reached are different facts, and the one that had no test was the second: an
// earlier round shipped `forgetLabelsGoneQuiet` with nothing calling it from the tick,
// and every test stayed green because they all called it themselves.
func TestTheSessionLayerLosingALeaseIsWhatForgetsTheSeries(t *testing.T) {
	t.Parallel()

	metrics := observability.New()
	c := &Connector{metrics: metrics, cfg: Config{Instance: "inst-owner"}}
	watch := &watching{metrics: metrics, owner: c}

	c.measure([]transport.Delivery{{
		Command:         protocol.Command{SID: "wac224-handed-over", Type: protocol.CommandSessionConnect},
		DeliveredBefore: true,
		TakenFrom:       "inst-gone",
		Deliveries:      2,
	}})
	if got := testutil.CollectAndCount(metrics.CommandsDeliveredAgain, "wac_commands_delivered_again_total"); got != 1 {
		t.Fatalf("the exposition has %d series before the lease is lost, want 1", got)
	}

	watch.LeaseLost("wac224-handed-over")

	if got := testutil.CollectAndCount(metrics.CommandsDeliveredAgain, "wac_commands_delivered_again_total"); got != 0 {
		t.Errorf("the session layer reported the lease lost and the series is still there (%d left): "+
			"the eviction is written but the report does not reach it", got)
	}
	if got := testutil.ToFloat64(metrics.LeasesLost); got != 1 {
		t.Errorf("wac_leases_lost_total reads %v, want 1: the report has to keep counting too", got)
	}
}
