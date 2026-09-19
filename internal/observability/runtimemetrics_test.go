package observability_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/httpserver"
	"github.com/fazer-ai/whatsapp-connector/internal/observability"
)

// alive answers the health interface without opinion: this test is about the metrics
// route, and a probe that could fail would only add a second reason for a red.
type alive struct{}

func (alive) Ready(context.Context) error { return nil }
func (alive) Sessions() int               { return 0 }

// What it costs to run is a metric too, and until these were registered nothing on
// `/metrics` answered it.
//
// Every `wac_*` series says what the connector did. None says what it cost, so "is this
// instance leaking goroutines", "how much memory does a fleet of N sessions need" and
// "how close is it to its descriptor ceiling" had no series behind them: the answer came
// from `ps` on the host, which no dashboard and no alert can reach.
//
// The status check is not ceremony. `httpserver` serves the registry through
// `promhttp.HandlerOpts{}`, whose zero value is `HTTPErrorOnError`, so a collector that
// cannot read the platform it is on does not go missing quietly -- it turns the whole
// endpoint into a 500 and takes every other metric with it. The process collector is the
// one that reads the operating system, which is why this asserts the scrape succeeds on
// whatever platform the suite runs on, not just that the names are somewhere in a body.
func TestTheRuntimeAndTheProcessAreExposed(t *testing.T) {
	t.Parallel()

	server := httpserver.New(httpserver.Options{Health: alive{}, Registry: observability.New().Registry})
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", http.NoBody))

	if recorder.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200; a collector that fails on this platform takes the "+
			"whole endpoint down under HTTPErrorOnError:\n%s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, want := range []string{
		"go_goroutines ",
		"go_memstats_heap_inuse_bytes ",
		"process_resident_memory_bytes ",
		"process_open_fds ",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics carries no %q, so there is no series for a capacity panel to read", want)
		}
	}
}
