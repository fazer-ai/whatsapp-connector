package app

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// The eviction only bounds anything if something runs it.
//
// Nothing tears a label value down on its own: what is counted is a session this instance
// may never own and a peer that may no longer exist, so neither has a lifecycle this can
// hang off. The heartbeat is the only place left, and a pass that renews every lease,
// reclaims, announces and then skips this one leaves two unbounded labels growing for as
// long as the process lives -- with every test above still green, because they all call
// the eviction themselves.
//
// So this one does not call it. It runs one real heartbeat, on a connector built the way
// `serve` builds it, and asks the exposition what is left.
func TestTheHeartbeatIsWhatDropsALabelGoneQuiet(t *testing.T) {
	server := miniredis.RunT(t)
	t.Setenv("REDIS_URL", "redis://"+server.Addr())
	t.Setenv("WAC_INSTANCE", "inst-heartbeat")
	t.Setenv("WAC_ENGINE", "fake")
	t.Setenv("WAC_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("WAC_HEARTBEAT", "200ms")

	cfg, err := LoadConfig("test-host")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	c, err := New(&cfg, zerolog.Nop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Counted long enough ago to be quiet, and counted for real rather than seeded, so
	// the series that has to disappear is the one the counting path creates.
	c.measure([]transport.Delivery{{
		Command:         protocol.Command{SID: "wac224-heartbeat", Type: protocol.CommandSessionConnect},
		DeliveredBefore: true,
		TakenFrom:       "connector-gone",
		Deliveries:      2,
	}})
	c.seenMu.Lock()
	for label := range c.seen {
		c.seen[label] = time.Now().Add(-labelQuiet - time.Minute)
	}
	c.seenMu.Unlock()

	if got := testutil.CollectAndCount(c.metrics.CommandsDeliveredAgain, "wac_commands_delivered_again_total"); got != 1 {
		t.Fatalf("before the heartbeat the exposition has %d series, want 1", got)
	}

	c.tick(context.Background())

	if got := testutil.CollectAndCount(c.metrics.CommandsDeliveredAgain, "wac_commands_delivered_again_total"); got != 0 {
		t.Errorf("a heartbeat went by and the quiet session still has %d series: the eviction "+
			"is written but nothing calls it, so the label grows for the life of the process", got)
	}
	if got := testutil.CollectAndCount(c.metrics.CommandsReclaimed, "wac_commands_reclaimed_total"); got != 0 {
		t.Errorf("a heartbeat went by and the gone consumer still has %d series", got)
	}
}

// A session this instance has never owned keeps its series across a heartbeat.
//
// This is the fence over the design that was written, shipped and reverted in this same
// round: an eviction keyed on "the sessions I do not own", swept once a heartbeat. It
// satisfies the sealed scenario about a session going away and destroys the metric,
// because a `session.wake` the fleet cannot adopt names a session nobody owns, so the one
// series this was built for was deleted on every beat while the redeliveries went on.
//
// It is here because the holdout's execution found that reintroducing it passes the whole
// suite: fourteen packages green, `go vet` clean, and the only thing that noticed was a
// live bench watching the series vanish while `wac_command_redeliveries` kept climbing.
// A defect that a bench catches and no test does is a defect that comes back.
func TestASessionThisInstanceNeverOwnedKeepsItsSeriesAcrossAHeartbeat(t *testing.T) {
	server := miniredis.RunT(t)
	t.Setenv("REDIS_URL", "redis://"+server.Addr())
	t.Setenv("WAC_INSTANCE", "inst-never-owned")
	t.Setenv("WAC_ENGINE", "fake")
	t.Setenv("WAC_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("WAC_HEARTBEAT", "200ms")

	cfg, err := LoadConfig("test-host")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	c, err := New(&cfg, zerolog.Nop())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// The wake nobody can act on: counted against a session this instance does not run
	// and has never run, which is why `wac_sessions_running` stays at zero throughout.
	c.measure([]transport.Delivery{{
		Command:         protocol.Command{SID: "wac224-never-mine", Type: protocol.CommandSessionWake},
		DeliveredBefore: true,
		TakenFrom:       cfg.Instance,
		Deliveries:      6,
	}})
	if got := testutil.CollectAndCount(c.metrics.CommandsDeliveredAgain, "wac_commands_delivered_again_total"); got != 1 {
		t.Fatalf("before the heartbeat the exposition has %d series, want 1", got)
	}

	for range 3 {
		c.tick(context.Background())
	}

	if got := testutil.CollectAndCount(c.metrics.CommandsDeliveredAgain, "wac_commands_delivered_again_total"); got != 1 {
		t.Errorf("three heartbeats went by and the series for a session this instance never owned "+
			"reads %d, want 1. Eviction is keyed on a session this instance stopped running, not on "+
			"the ones it does not have: a wake the fleet cannot adopt names a session nobody holds, "+
			"and sweeping by ownership deletes the one series this metric exists for while the "+
			"redeliveries go on.", got)
	}
	if got := testutil.ToFloat64(c.metrics.LeasesLost); got != 0 {
		t.Errorf("wac_leases_lost_total reads %v: nothing lost a lease here, so nothing should have "+
			"reached the eviction at all", got)
	}
}
