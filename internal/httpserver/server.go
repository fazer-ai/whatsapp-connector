// Package httpserver exposes what an operator and an orchestrator need to see.
//
// It is deliberately not the command plane: commands travel over Redis. What is here
// is liveness, readiness and metrics, plus the media endpoints a later milestone adds.
package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// DefaultAddr is where the server listens unless the deployment says otherwise.
const DefaultAddr = ":8080"

// Health is what the readiness probe asks the running connector.
type Health interface {
	// Ready reports whether this instance can do its job right now. A connector that
	// cannot reach Redis is not ready: it can neither publish nor be told anything.
	Ready(ctx context.Context) error
	// Sessions is how many sessions this instance runs, for the status endpoint.
	Sessions() int
}

// Server is the HTTP surface.
type Server struct {
	server *http.Server
	health Health
}

// Options configures the server.
type Options struct {
	Addr     string
	Health   Health
	Registry *prometheus.Registry
	Version  string
	Instance string
	// Media serves the bytes of inbound media messages. Left out, the routes are not
	// registered at all rather than registered and refusing: an instance with no blob
	// store answers 404 for them, which is what a client that reaches the wrong
	// instance should hear.
	Media http.Handler
}

// New builds the server without listening.
//
//nolint:gocritic // one call at startup; a pointer here would only invite a caller to keep it
func New(opts Options) *Server {
	if opts.Addr == "" {
		opts.Addr = DefaultAddr
	}
	s := &Server{health: opts.Health}

	mux := http.NewServeMux()
	// Liveness answers "is this process running", which is a different question from
	// readiness: an orchestrator that restarts on a failed liveness probe would
	// restart every instance in the fleet the moment Redis blinked.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "version": opts.Version, "inst": opts.Instance})
	})
	mux.HandleFunc("GET /readyz", s.ready)
	if opts.Registry != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(opts.Registry, promhttp.HandlerOpts{}))
	}
	if opts.Media != nil {
		// HEAD as well as GET, because a client checking whether a blob is still there
		// before queueing a download should not have to pull the file to find out. The
		// standard library answers a HEAD from a GET handler by dropping the body.
		mux.Handle("GET /media/{id}", opts.Media)
		mux.Handle("HEAD /media/{id}", opts.Media)
	}

	s.server = &http.Server{
		Addr:              opts.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

// Addr is where the server listens.
func (s *Server) Addr() string { return s.server.Addr }

// Handler is the mux, so a test can exercise the routes without a socket.
func (s *Server) Handler() http.Handler { return s.server.Handler }

// Start listens and serves until Shutdown. It returns nil on a clean shutdown, so a
// caller can treat any error as a real one.
func (s *Server) Start() error {
	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown stops serving, letting in-flight requests finish.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.health == nil {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
		return
	}
	if err := s.health.Ready(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not ready", "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "sessions": s.health.Sessions()})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
