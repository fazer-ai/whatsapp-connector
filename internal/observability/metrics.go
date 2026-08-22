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
	}
	registry.MustRegister(m.SessionsRunning, m.EventsPublished, m.CommandDuration, m.LeasesLost)
	return m
}
