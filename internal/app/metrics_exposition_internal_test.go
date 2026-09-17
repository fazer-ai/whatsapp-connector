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
	w.LeaseLost("wac224-lease")
	w.LeaseLost("wac224-lease")

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
	c := &Connector{metrics: metrics, cfg: Config{Instance: "inst-under-test"}}

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
		// What the transport hands over: the count XPENDING listed, with nothing added
		// for the claim that took it.
		Deliveries: 3,
	}})
	// And one arriving new, which is not a redelivery and must not move anything.
	c.measure([]transport.Delivery{{
		Command: protocol.Command{SID: "wac224-fresh", Type: protocol.CommandSessionConnect},
	}})

	const again = `# HELP wac_commands_delivered_again_total Commands handed out that had been handed out before, by where this delivery came from. source=restored is one this instance gave back unrun and took again through its own claim, which is the loop a wake nobody can act on makes; source=claim is one a claim took off another consumer; source=read is one that came back out of the pending history in a read.
# TYPE wac_commands_delivered_again_total counter
wac_commands_delivered_again_total{sid="wac224-loop",source="read"} 4
wac_commands_delivered_again_total{sid="wac224-taken",source="claim"} 1
`
	if err := testutil.CollectAndCompare(metrics.CommandsDeliveredAgain, strings.NewReader(again), "wac_commands_delivered_again_total"); err != nil {
		t.Errorf("what a scrape shows for commands handed out again:\n%v", err)
	}

	const reclaimed = `# HELP wac_commands_reclaimed_total Commands a claim took back from another consumer, by the consumer that was holding them. A command this instance gave back unrun and took again through its own claim is not in it: it was taken from nobody.
# TYPE wac_commands_reclaimed_total counter
wac_commands_reclaimed_total{from="connector-b"} 1
`
	if err := testutil.CollectAndCompare(metrics.CommandsReclaimed, strings.NewReader(reclaimed), "wac_commands_reclaimed_total"); err != nil {
		t.Errorf("what a scrape shows for commands taken back:\n%v", err)
	}

	// The whole histogram family, buckets and sum together. The sum is what separates
	// "observed the three deliveries Redis reported" from "observed zero", which every
	// bucket above the first reads the same for.
	const redeliveries = `# HELP wac_command_redeliveries How many times Redis said a reclaimed command had been delivered when the claim listed it, not counting the claim itself. Only a claim can read this, so commands coming back through the read loop are not in it.
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

// A label value nothing has been counted against for a while takes its series with it.
//
// On going quiet, not on the session going away, and the difference is the whole reason
// the metric exists. A wake for a session this instance cannot adopt is the case it was
// built for, and that session is in nobody's owned list: evicting by ownership deleted
// its series on every heartbeat, so the one series that mattered read as a run of resets.
// The consumer label goes the same way, because an instance name defaults to the hostname
// and a fleet of constant size still coins a new one every time a replica is replaced.
func TestALabelGoneQuietTakesItsSeriesWithIt(t *testing.T) {
	t.Parallel()

	metrics := observability.New()
	c := &Connector{metrics: metrics, cfg: Config{Instance: "inst-under-test"}}

	c.measure([]transport.Delivery{{
		Command:         protocol.Command{SID: "wac224-quiet", Type: protocol.CommandSessionConnect},
		DeliveredBefore: true,
		TakenFrom:       "connector-gone",
		Deliveries:      2,
	}})
	if got := testutil.CollectAndCount(metrics.CommandsDeliveredAgain, "wac_commands_delivered_again_total"); got != 1 {
		t.Fatalf("after one redelivery the exposition has %d series, want 1", got)
	}

	// A heartbeat while it is still being counted against leaves it alone, which is what
	// keeps a session that keeps retrying from vanishing under the scrape reading it.
	c.forgetLabelsGoneQuiet(time.Now())
	if got := testutil.CollectAndCount(metrics.CommandsDeliveredAgain, "wac_commands_delivered_again_total"); got != 1 {
		t.Errorf("a label counted a moment ago lost its series: %d left, want 1", got)
	}
	if got := testutil.CollectAndCount(metrics.CommandsReclaimed, "wac_commands_reclaimed_total"); got != 1 {
		t.Errorf("a consumer seen a moment ago lost its series: %d left, want 1", got)
	}

	c.forgetLabelsGoneQuiet(time.Now().Add(labelQuiet + time.Second))

	if got := testutil.CollectAndCount(metrics.CommandsDeliveredAgain, "wac_commands_delivered_again_total"); got != 0 {
		t.Errorf("the session went quiet and the exposition still has %d series for it", got)
	}
	if got := testutil.CollectAndCount(metrics.CommandsReclaimed, "wac_commands_reclaimed_total"); got != 0 {
		t.Errorf("the consumer went quiet and the exposition still has %d series for it", got)
	}
}
