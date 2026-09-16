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
		InboxDepth: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "wac_session_inbox_depth",
			Help:    "How full a session's inbox was when an emission arrived, out of 256.",
			Buckets: []float64{1, 8, 32, 64, 128, 192, 240, 256},
		}),
	}
	registry.MustRegister(
		m.SessionsRunning, m.EventsPublished, m.CommandDuration, m.LeasesLost,
		m.CommandReadsFailed, m.CommandReadLastSuccess, m.EmissionWait, m.InboxDepth,
	)
	return m
}
