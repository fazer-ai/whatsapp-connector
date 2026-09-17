package app

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/fazer-ai/whatsapp-connector/internal/observability"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// An instance taking back a command it gave back itself is not an instance reclaiming
// from a peer, and must not be counted as one.
//
// Measured, not supposed. A `session.wake` for a session the fleet cannot adopt is given
// back unrun, and `rememberAge` stamps it with `ClaimMinIdle` so the next claim takes it
// instead of the read leaving it forever: that is the design, and it means the loop comes
// back through the claim with XPENDING naming this instance as the holder. On a bench
// reproducing the sealed scenario s8 at this HEAD, fifty stuck sessions produced 6450
// counts in four minutes and twenty-five seconds, every one of them under
// `wac_commands_reclaimed_total{from="<this instance>"}`.
//
// That label exists to separate "the fleet is busy" from "one instance took commands and
// stopped answering". An instance naming itself there is a false positive on the alarm
// the label was created for, and it fires hardest during the incident the metric exists
// to show.
func TestAnInstanceDoesNotReclaimFromItself(t *testing.T) {
	t.Parallel()

	metrics := observability.New()
	c := &Connector{metrics: metrics, cfg: Config{Instance: "inst-alone"}}

	// The wake loop: given back unrun, aged, and taken again by this instance's own claim.
	for range 3 {
		c.measure([]transport.Delivery{{
			Command:         protocol.Command{SID: "wac224-stuck", Type: protocol.CommandSessionWake},
			DeliveredBefore: true,
			TakenFrom:       "inst-alone",
			Deliveries:      7,
		}})
	}
	// And one genuinely taken off a peer that stopped answering.
	c.measure([]transport.Delivery{{
		Command:         protocol.Command{SID: "wac224-peer", Type: protocol.CommandSessionConnect},
		DeliveredBefore: true,
		TakenFrom:       "inst-gone",
		Deliveries:      2,
	}})

	const reclaimed = `# HELP wac_commands_reclaimed_total Commands a claim took back from another consumer, by the consumer that was holding them. A command this instance gave back unrun and took again through its own claim is not in it: it was taken from nobody.
# TYPE wac_commands_reclaimed_total counter
wac_commands_reclaimed_total{from="inst-gone"} 1
`
	if err := testutil.CollectAndCompare(metrics.CommandsReclaimed, strings.NewReader(reclaimed), "wac_commands_reclaimed_total"); err != nil {
		t.Errorf("what a scrape shows for commands taken back:\n%v", err)
	}

	// The loop is still counted, under a source of its own: it is the fleet handing the
	// same command out again, and it is the case the issue's third question describes.
	const again = `# HELP wac_commands_delivered_again_total Commands handed out that had been handed out before, by where this delivery came from. source=restored is one this instance gave back unrun and took again through its own claim, which is the loop a wake nobody can act on makes; source=claim is one a claim took off another consumer; source=read is one that came back out of the pending history in a read.
# TYPE wac_commands_delivered_again_total counter
wac_commands_delivered_again_total{sid="wac224-peer",source="claim"} 1
wac_commands_delivered_again_total{sid="wac224-stuck",source="restored"} 3
`
	if err := testutil.CollectAndCompare(metrics.CommandsDeliveredAgain, strings.NewReader(again), "wac_commands_delivered_again_total"); err != nil {
		t.Errorf("what a scrape shows for commands handed out again:\n%v", err)
	}
}
