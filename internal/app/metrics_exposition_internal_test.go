package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/fazer-ai/whatsapp-connector/internal/observability"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// What a scrape actually shows, driven through the real adapters rather than a spy.
//
// The unit tests on either side prove the engine reports and the session reports; nothing
// proved what comes out of `/metrics` at the end of it, which is the only thing an
// operator ever sees. That gap is how #226 survived in the first place: every piece
// looked right and the exposition was empty.
//
// The backpressure half is the one the sealed scenarios could not reach, because none of
// them fills an inbox, so it is exercised here: an emission that waited, a queue pressed
// against its ceiling, and an event dropped for want of room.
func TestWhatAScrapeShowsUnderBackpressure(t *testing.T) {
	t.Parallel()

	metrics := observability.New()
	q := queueing{metrics: metrics}

	// Four emissions through the door that waits, at rising depth, then two the inbox had
	// no room for at all -- the shape a stalled publisher makes.
	q.Emitted(0, 0)
	q.Emitted(0, 8)
	q.Emitted(1500*time.Millisecond, 256)
	q.Emitted(30*time.Second, 256)
	q.Dropped(protocol.EventChatPresence)
	q.Dropped(protocol.EventChatPresence)
	q.Dropped(protocol.EventMessageReceived)

	wait := `
# HELP wac_emission_wait_seconds Time an event waited for room in its session's inbox before the pump took it.
# TYPE wac_emission_wait_seconds histogram
wac_emission_wait_seconds_bucket{le="0.001"} 2
wac_emission_wait_seconds_bucket{le="0.01"} 2
wac_emission_wait_seconds_bucket{le="0.1"} 2
wac_emission_wait_seconds_bucket{le="0.5"} 2
wac_emission_wait_seconds_bucket{le="1"} 2
wac_emission_wait_seconds_bucket{le="5"} 3
wac_emission_wait_seconds_bucket{le="15"} 3
wac_emission_wait_seconds_bucket{le="60"} 4
wac_emission_wait_seconds_bucket{le="+Inf"} 4
wac_emission_wait_seconds_sum 31.5
wac_emission_wait_seconds_count 4
`
	if err := testutil.CollectAndCompare(metrics.EmissionWait, strings.NewReader(wait),
		"wac_emission_wait_seconds"); err != nil {
		t.Errorf("the wait a stalled publisher caused is not what a scrape shows: %v", err)
	}

	dropped := `
# HELP wac_emissions_dropped_total Events the session inbox had no room for, by event type.
# TYPE wac_emissions_dropped_total counter
wac_emissions_dropped_total{type="chat.presence"} 2
wac_emissions_dropped_total{type="message.received"} 1
`
	if err := testutil.CollectAndCompare(metrics.EmissionsDropped, strings.NewReader(dropped),
		"wac_emissions_dropped_total"); err != nil {
		t.Errorf("the events lost for want of room are not what a scrape shows: %v", err)
	}

	// The ceiling is 256, so a distribution pressed against the top bucket is the fleet
	// about to start waiting. That is the reading the depth histogram exists to give.
	if got := testutil.ToFloat64(metrics.SessionsRunning); got != 0 {
		t.Errorf("unrelated gauge moved: %v", got)
	}
	depth := `
# HELP wac_session_inbox_depth How full a session's inbox was when an emission arrived, out of 256.
# TYPE wac_session_inbox_depth histogram
wac_session_inbox_depth_bucket{le="1"} 1
wac_session_inbox_depth_bucket{le="8"} 2
wac_session_inbox_depth_bucket{le="32"} 2
wac_session_inbox_depth_bucket{le="64"} 2
wac_session_inbox_depth_bucket{le="128"} 2
wac_session_inbox_depth_bucket{le="192"} 2
wac_session_inbox_depth_bucket{le="240"} 2
wac_session_inbox_depth_bucket{le="256"} 4
wac_session_inbox_depth_bucket{le="+Inf"} 4
wac_session_inbox_depth_sum 520
wac_session_inbox_depth_count 4
`
	if err := testutil.CollectAndCompare(metrics.InboxDepth, strings.NewReader(depth),
		"wac_session_inbox_depth"); err != nil {
		t.Errorf("the queue depth is not what a scrape shows: %v", err)
	}
}

// The three that #226 is about, end to end: what a scrape shows after the events, the
// commands and the lease losses this build now counts. Each one read zero -- or was
// absent entirely -- before this.
func TestWhatAScrapeShowsForTheThreeThatCountedNothing(t *testing.T) {
	t.Parallel()

	metrics := observability.New()
	publisher := countingPublisher{to: &stubPublisher{}, metrics: metrics}
	w := watching{metrics: metrics}
	ctx := context.Background()

	for range 3 {
		if err := publisher.Publish(ctx, &protocol.Event{Type: protocol.EventMessageReceived}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	refused := countingPublisher{to: &stubPublisher{err: errors.New("refused")}, metrics: metrics}
	if err := refused.Publish(ctx, &protocol.Event{Type: protocol.EventMessageReceived}); err == nil {
		t.Fatal("a refused publish returned nil")
	}

	w.CommandDone(protocol.CommandMessageSend, "ok", 250*time.Millisecond)
	w.CommandDone(protocol.CommandMessageSend, string(protocol.ErrorRateLimited), 2*time.Second)
	w.CommandDone("whatever-the-client-wrote", string(protocol.ErrorInvalidPayload), time.Millisecond)
	w.LeaseLost()
	w.LeaseLost()

	published := `
# HELP wac_events_published_total Events published to the client, by event type.
# TYPE wac_events_published_total counter
wac_events_published_total{type="message.received"} 3
`
	if err := testutil.CollectAndCompare(metrics.EventsPublished, strings.NewReader(published),
		"wac_events_published_total"); err != nil {
		t.Errorf("wac_events_published_total: %v", err)
	}

	if got := testutil.ToFloat64(metrics.LeasesLost); got != 2 {
		t.Errorf("wac_leases_lost_total = %v, want 2", got)
	}

	// The whole family, because the two halves of this metric fail in different ways and
	// only the full text catches both. A wrong label is a series nobody queries; a
	// duration that is always the same number is a series that answers every query with
	// a lie, and a histogram with the right children reads perfectly healthy while doing
	// it. Comparing the family also proves what is absent: `whatever-the-client-wrote`
	// has no child of its own, so a malformed frame cannot grow this for ever.
	command := `
# HELP wac_command_duration_seconds Time from picking a command up to answering it, by command type and outcome.
# TYPE wac_command_duration_seconds histogram
wac_command_duration_seconds_bucket{outcome="invalid_payload",type="unknown",le="0.005"} 1
wac_command_duration_seconds_bucket{outcome="invalid_payload",type="unknown",le="0.01"} 1
wac_command_duration_seconds_bucket{outcome="invalid_payload",type="unknown",le="0.025"} 1
wac_command_duration_seconds_bucket{outcome="invalid_payload",type="unknown",le="0.05"} 1
wac_command_duration_seconds_bucket{outcome="invalid_payload",type="unknown",le="0.1"} 1
wac_command_duration_seconds_bucket{outcome="invalid_payload",type="unknown",le="0.25"} 1
wac_command_duration_seconds_bucket{outcome="invalid_payload",type="unknown",le="0.5"} 1
wac_command_duration_seconds_bucket{outcome="invalid_payload",type="unknown",le="1"} 1
wac_command_duration_seconds_bucket{outcome="invalid_payload",type="unknown",le="2.5"} 1
wac_command_duration_seconds_bucket{outcome="invalid_payload",type="unknown",le="5"} 1
wac_command_duration_seconds_bucket{outcome="invalid_payload",type="unknown",le="10"} 1
wac_command_duration_seconds_bucket{outcome="invalid_payload",type="unknown",le="+Inf"} 1
wac_command_duration_seconds_sum{outcome="invalid_payload",type="unknown"} 0.001
wac_command_duration_seconds_count{outcome="invalid_payload",type="unknown"} 1
wac_command_duration_seconds_bucket{outcome="ok",type="message.send",le="0.005"} 0
wac_command_duration_seconds_bucket{outcome="ok",type="message.send",le="0.01"} 0
wac_command_duration_seconds_bucket{outcome="ok",type="message.send",le="0.025"} 0
wac_command_duration_seconds_bucket{outcome="ok",type="message.send",le="0.05"} 0
wac_command_duration_seconds_bucket{outcome="ok",type="message.send",le="0.1"} 0
wac_command_duration_seconds_bucket{outcome="ok",type="message.send",le="0.25"} 1
wac_command_duration_seconds_bucket{outcome="ok",type="message.send",le="0.5"} 1
wac_command_duration_seconds_bucket{outcome="ok",type="message.send",le="1"} 1
wac_command_duration_seconds_bucket{outcome="ok",type="message.send",le="2.5"} 1
wac_command_duration_seconds_bucket{outcome="ok",type="message.send",le="5"} 1
wac_command_duration_seconds_bucket{outcome="ok",type="message.send",le="10"} 1
wac_command_duration_seconds_bucket{outcome="ok",type="message.send",le="+Inf"} 1
wac_command_duration_seconds_sum{outcome="ok",type="message.send"} 0.25
wac_command_duration_seconds_count{outcome="ok",type="message.send"} 1
wac_command_duration_seconds_bucket{outcome="rate_limited",type="message.send",le="0.005"} 0
wac_command_duration_seconds_bucket{outcome="rate_limited",type="message.send",le="0.01"} 0
wac_command_duration_seconds_bucket{outcome="rate_limited",type="message.send",le="0.025"} 0
wac_command_duration_seconds_bucket{outcome="rate_limited",type="message.send",le="0.05"} 0
wac_command_duration_seconds_bucket{outcome="rate_limited",type="message.send",le="0.1"} 0
wac_command_duration_seconds_bucket{outcome="rate_limited",type="message.send",le="0.25"} 0
wac_command_duration_seconds_bucket{outcome="rate_limited",type="message.send",le="0.5"} 0
wac_command_duration_seconds_bucket{outcome="rate_limited",type="message.send",le="1"} 0
wac_command_duration_seconds_bucket{outcome="rate_limited",type="message.send",le="2.5"} 1
wac_command_duration_seconds_bucket{outcome="rate_limited",type="message.send",le="5"} 1
wac_command_duration_seconds_bucket{outcome="rate_limited",type="message.send",le="10"} 1
wac_command_duration_seconds_bucket{outcome="rate_limited",type="message.send",le="+Inf"} 1
wac_command_duration_seconds_sum{outcome="rate_limited",type="message.send"} 2
wac_command_duration_seconds_count{outcome="rate_limited",type="message.send"} 1
`
	if err := testutil.CollectAndCompare(metrics.CommandDuration, strings.NewReader(command),
		"wac_command_duration_seconds"); err != nil {
		t.Errorf("the commands this instance answered are not what a scrape shows: %v", err)
	}
}

// What a scrape shows about commands the fleet is handing out again.
//
// The whole family text, with values that tell four from one from zero, because that is
// the only comparison that separates a metric being written from a metric being
// registered: a CounterVec nobody writes has no children and is absent from the
// exposition entirely, so "nobody writes it" and "it should not exist" read identically
// from outside. That is exactly how #226 survived, and the test that catches it has to
// compare text rather than assert a family is present or that a count is above zero.
func TestWhatAScrapeShowsAboutCommandsHandedOutAgain(t *testing.T) {
	t.Parallel()

	metrics := observability.New()
	c := &Connector{metrics: metrics}

	// A wake the fleet cannot act on, coming back out of the pending history four times,
	// which no claim ever sees because its idle never grows. Then one command claimed
	// back off a dead peer, which is the other half and the only half XPENDING reports.
	for range 4 {
		c.measure([]transport.Delivery{{
			Command:         protocol.Command{SID: "wac224-loop", Type: protocol.CommandSessionConnect},
			DeliveredBefore: true,
		}})
	}
	c.measure([]transport.Delivery{{
		Command:         protocol.Command{SID: "wac224-taken", Type: protocol.CommandSessionConnect},
		DeliveredBefore: true,
		TakenFrom:       "connector-b",
		Deliveries:      3,
	}})
	// And one arriving new, which is not a redelivery and must not move anything.
	c.measure([]transport.Delivery{{
		Command: protocol.Command{SID: "wac224-fresh", Type: protocol.CommandSessionConnect},
	}})

	const again = `# HELP wac_commands_delivered_again_total Commands handed out that had been handed out before, by where this delivery came from. source=read is a command coming back out of the pending history, which no claim ever sees.
# TYPE wac_commands_delivered_again_total counter
wac_commands_delivered_again_total{sid="wac224-loop",source="read"} 4
wac_commands_delivered_again_total{sid="wac224-taken",source="claim"} 1
`
	if err := testutil.CollectAndCompare(metrics.CommandsDeliveredAgain, strings.NewReader(again), "wac_commands_delivered_again_total"); err != nil {
		t.Errorf("what a scrape shows for commands handed out again:\n%v", err)
	}

	const reclaimed = `# HELP wac_commands_reclaimed_total Commands a claim took back, by the consumer that was holding them.
# TYPE wac_commands_reclaimed_total counter
wac_commands_reclaimed_total{from="connector-b"} 1
`
	if err := testutil.CollectAndCompare(metrics.CommandsReclaimed, strings.NewReader(reclaimed), "wac_commands_reclaimed_total"); err != nil {
		t.Errorf("what a scrape shows for commands taken back:\n%v", err)
	}

	// The whole histogram family, buckets and sum together. The sum is what separates
	// "observed the three deliveries Redis reported" from "observed zero", which every
	// bucket above the first reads the same for.
	const redeliveries = `# HELP wac_command_redeliveries How many times Redis says a reclaimed command had been delivered, counting that one. Only a claim can read this, so commands coming back through the read loop are not in it.
# TYPE wac_command_redeliveries histogram
wac_command_redeliveries_bucket{le="1"} 0
wac_command_redeliveries_bucket{le="2"} 0
wac_command_redeliveries_bucket{le="3"} 1
wac_command_redeliveries_bucket{le="5"} 1
wac_command_redeliveries_bucket{le="10"} 1
wac_command_redeliveries_bucket{le="25"} 1
wac_command_redeliveries_bucket{le="100"} 1
wac_command_redeliveries_bucket{le="1000"} 1
wac_command_redeliveries_bucket{le="+Inf"} 1
wac_command_redeliveries_sum 3
wac_command_redeliveries_count 1
`
	if err := testutil.CollectAndCompare(metrics.CommandRedeliveries, strings.NewReader(redeliveries), "wac_command_redeliveries"); err != nil {
		t.Errorf("what a scrape shows for how many times a reclaimed command had been delivered:\n%v", err)
	}
}

// A session this instance no longer owns takes its series with it.
//
// Without this the label set is every session the process has ever held rather than the
// ones it holds, and a session id has no ceiling: nothing limits how many an instance
// adopts over its life, however few run at once. It is the first label in this build
// without one, and it is only defensible because it is dropped.
func TestASessionThatIsGoneTakesItsSeriesWithIt(t *testing.T) {
	t.Parallel()

	metrics := observability.New()
	c := &Connector{metrics: metrics}

	c.measure([]transport.Delivery{{
		Command:         protocol.Command{SID: "wac224-gone", Type: protocol.CommandSessionConnect},
		DeliveredBefore: true,
	}})
	if got := testutil.CollectAndCount(metrics.CommandsDeliveredAgain, "wac_commands_delivered_again_total"); got != 1 {
		t.Fatalf("after one redelivery the exposition has %d series, want 1", got)
	}

	// Nothing owned any more, which is what a lease lost or a hand-back leaves.
	c.forgetSessionsGone(nil)

	if got := testutil.CollectAndCount(metrics.CommandsDeliveredAgain, "wac_commands_delivered_again_total"); got != 0 {
		t.Errorf("the session is gone and the exposition still has %d series for it", got)
	}
}
