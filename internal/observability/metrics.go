// Package observability holds the logger and the metrics the connector publishes.
package observability

import "github.com/prometheus/client_golang/prometheus"

// Metrics is everything this build measures. It carries its own registry so a test can
// build a set without touching the process-wide default, where a second one would
// panic on a duplicate registration.
type Metrics struct {
	Registry *prometheus.Registry

	// SessionsRunning is how many sessions this instance owns. A fleet whose total
	// does not match the number of configured inboxes has sessions nobody is running.
	SessionsRunning prometheus.Gauge
	// EventsPublished counts by type, which is what tells an incident whether the
	// silence is "no messages arriving" or "messages arriving and not published".
	EventsPublished *prometheus.CounterVec
	// CommandDuration is the time from picking a command up to answering it.
	CommandDuration *prometheus.HistogramVec
	// LeasesLost counts ownership taken away, which is the shape a flapping instance
	// or a partitioned Redis makes.
	LeasesLost prometheus.Counter
	// CommandReadsFailed counts command reads that came back with an error.
	CommandReadsFailed prometheus.Counter
	// CommandReadLastSuccess is when this instance last read commands without an error.
	//
	// The counter alone cannot say what an operator needs to know, because a read that
	// fails is ordinary and a run of them is not: while the run lasts this instance
	// carries out nothing for any session it owns, answers nobody, and goes on reporting
	// itself ready and holding every lease. What separates the two is elapsed time with
	// no success, which is `time() - this`, and it is the only signal an instance that is
	// silently not serving gives off at all.
	CommandReadLastSuccess prometheus.Gauge
	// EmissionWait is how long an event waited for room in its session's inbox.
	//
	// Zero almost always, and the tail is the whole point: the wait happens on
	// whatsmeow's dispatch goroutine, so an emission that waits is an account whose
	// other traffic is not being handled meanwhile (#221). Buckets reach far past a
	// second because the question is how bad the tail gets, not whether the median is
	// fast.
	EmissionWait prometheus.Histogram
	// InboxDepth is how full a session's inbox was when an emission arrived. The
	// ceiling is 256, and a distribution pressed against it is a fleet about to start
	// waiting.
	InboxDepth prometheus.Histogram
	// CommandsDeliveredAgain counts commands handed out that had been handed out before,
	// by where this delivery came from.
	//
	// It is the fleet doing the same work twice, and it is a counter rather than a gauge
	// on purpose: the reclaim pass looks at a rotating window of sessions, so a gauge
	// would be making a claim about the sessions this pass did not look at. A counter
	// claims nothing beyond what was seen.
	//
	// `source="read"` is the one that matters and the one an instrument built on the
	// reclaim alone cannot see. A wake the fleet cannot act on is handed back unrun and
	// read out of the pending history on the very next block, so its idle never grows
	// past a couple of seconds and no claim ever looks at it; measured on 2771941, twenty
	// redeliveries in forty-five seconds, none of them visible to a claim.
	CommandsDeliveredAgain *prometheus.CounterVec
	// CommandsReclaimed counts commands a claim took back, by the consumer that was
	// holding them.
	//
	// The label is what separates "the fleet is busy" from "one instance took commands
	// and stopped answering", and it is the instance that went quiet whose own metrics
	// nobody is reading. Its cardinality has the fleet's own ceiling, unlike a session id.
	CommandsReclaimed *prometheus.CounterVec
	// CommandRedeliveries is how many times Redis said a command a claim took back had
	// been delivered when the claim listed it, not counting the claim itself.
	//
	// Not counting it because how many times a claim reaches Redis is not knowable from
	// the side that sent it: a lost answer has go-redis send XCLAIM again, and the
	// second one increments the delivery counter with the caller told nothing. Adding
	// one for the claim would be short by every retry, during exactly the connection
	// trouble this is here to show.
	//
	// A counter says the fleet is retrying; this says whether that is many commands once
	// or one command forever, which is the difference #224's third question turns on and
	// the number nobody can pick a retry ceiling without. Only a claim can observe it,
	// so it does not see the read loop above: the two together are the whole picture and
	// neither is on its own.
	CommandRedeliveries prometheus.Histogram
	// CommandReclaimPasses counts reclaim passes that completed, whatever they found.
	//
	// Without it, a fleet that is retrying nothing and a build where nobody writes the
	// three metrics above look identical from outside: a counter with no children is
	// absent from the exposition entirely, so "zero" and "not measured" are the same
	// text. This is the series that says the loop ran.
	CommandReclaimPasses prometheus.Counter
	// EmissionsDropped counts events the inbox had no room for and whose caller chose
	// not to wait. Presence does that by design and an inbound delivery does it once
	// its bound runs out; the client is never told either way, so this is the only
	// place the loss shows up at all.
	EmissionsDropped *prometheus.CounterVec
}

// New builds the metric set and registers it.
func New() *Metrics {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		Registry: registry,
		SessionsRunning: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wac_sessions_running",
			Help: "Sessions this instance currently owns.",
		}),
		EventsPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wac_events_published_total",
			Help: "Events published to the client, by event type.",
		}, []string{"type"}),
		CommandDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wac_command_duration_seconds",
			Help:    "Time from picking a command up to answering it, by command type and outcome.",
			Buckets: prometheus.DefBuckets,
		}, []string{"type", "outcome"}),
		LeasesLost: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wac_leases_lost_total",
			Help: "Sessions whose lease was taken away from this instance.",
		}),
		CommandReadsFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wac_command_reads_failed_total",
			Help: "Command reads that came back with an error.",
		}),
		CommandReadLastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wac_command_read_last_success_timestamp_seconds",
			Help: "When this instance last read commands without an error.",
		}),
		EmissionWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "wac_emission_wait_seconds",
			Help:    "Time an event waited for room in its session's inbox before the pump took it.",
			Buckets: []float64{.001, .01, .1, .5, 1, 5, 15, 60},
		}),
		CommandsDeliveredAgain: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wac_commands_delivered_again_total",
			Help: "Commands handed out that had been handed out before, by where this delivery came from. " +
				"source=read is a command coming back out of the pending history, which no claim ever sees.",
		}, []string{"source", "sid"}),
		CommandsReclaimed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wac_commands_reclaimed_total",
			Help: "Commands a claim took back, by the consumer that was holding them.",
		}, []string{"from"}),
		CommandRedeliveries: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "wac_command_redeliveries",
			Help: "How many times Redis said a reclaimed command had been delivered when the claim listed it, not counting the claim itself. " +
				"Only a claim can read this, so commands coming back through the read loop are not in it.",
			Buckets: []float64{1, 2, 3, 5, 10, 25, 100, 1000},
		}),
		CommandReclaimPasses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wac_command_reclaim_passes_total",
			Help: "Reclaim passes that completed, whatever they found. Tells a fleet retrying nothing from a build where nobody writes these.",
		}),
		EmissionsDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wac_emissions_dropped_total",
			Help: "Events the session inbox had no room for, by event type.",
		}, []string{"type"}),
		InboxDepth: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "wac_session_inbox_depth",
			Help:    "How full a session's inbox was when an emission arrived, out of 256.",
			Buckets: []float64{1, 8, 32, 64, 128, 192, 240, 256},
		}),
	}
	registry.MustRegister(
		m.SessionsRunning, m.EventsPublished, m.CommandDuration, m.LeasesLost,
		m.CommandReadsFailed, m.CommandReadLastSuccess, m.EmissionWait, m.InboxDepth, m.EmissionsDropped,
		m.CommandsDeliveredAgain, m.CommandsReclaimed, m.CommandRedeliveries, m.CommandReclaimPasses,
	)
	return m
}
