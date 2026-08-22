package httpserver_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/fazer-ai/whatsapp-connector/internal/httpserver"
)

type health struct {
	err      error
	sessions int
}

func (h health) Ready(context.Context) error { return h.err }
func (h health) Sessions() int               { return h.sessions }

// The server is built without listening, so the handler is exercised through its own
// mux rather than through a socket.
func get(t *testing.T, server *httpserver.Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody)
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

func TestHealthzAnswersWhileTheProcessRuns(t *testing.T) {
	t.Parallel()

	server := httpserver.New(httpserver.Options{
		Health: health{err: errors.New("redis is unreachable")}, Version: "1.0.0", Instance: "inst-a",
	})

	got := get(t, server, "/healthz")
	if got.Code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200 even while readiness fails", got.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(got.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body["version"] != "1.0.0" || body["inst"] != "inst-a" {
		t.Errorf("body = %v, want the version and instance", body)
	}
}

func TestReadyzReportsTheInstanceIsUsable(t *testing.T) {
	t.Parallel()

	server := httpserver.New(httpserver.Options{Health: health{sessions: 3}})

	got := get(t, server, "/readyz")
	if got.Code != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200", got.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(got.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body["sessions"] != float64(3) {
		t.Errorf("sessions = %v, want 3", body["sessions"])
	}
}

// An instance that cannot reach Redis can neither publish nor be told anything, so
// reporting ready would have an orchestrator send it traffic it cannot act on.
func TestReadyzRefusesWhenTheInstanceCannotServe(t *testing.T) {
	t.Parallel()

	server := httpserver.New(httpserver.Options{Health: health{err: errors.New("redis is unreachable")}})

	got := get(t, server, "/readyz")
	if got.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503", got.Code)
	}
}

func TestMetricsAreServedFromTheGivenRegistry(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "wac_test_gauge", Help: "for the test"})
	registry.MustRegister(gauge)
	gauge.Set(7)

	server := httpserver.New(httpserver.Options{Health: health{}, Registry: registry})

	got := get(t, server, "/metrics")
	if got.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", got.Code)
	}
	if body := got.Body.String(); !strings.Contains(body, "wac_test_gauge 7") {
		t.Errorf("/metrics did not serve the registry's own metric:\n%s", body)
	}
}
