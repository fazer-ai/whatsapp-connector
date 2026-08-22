package observability_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/observability"
)

// The connector is not supposed to log a phone number, and this is the net under the
// call sites that would.
func TestLoggerRedactsAnythingPhoneShaped(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	logger := observability.NewLogger(&out, "info", "inst-a", "1.0.0")

	logger.Info().Str("who", "5541988887777").Msg("paired")

	line := out.String()
	if strings.Contains(line, "5541988887777") {
		t.Fatalf("the number reached the log: %s", line)
	}
	if !strings.Contains(line, "[redacted]") {
		t.Fatalf("nothing was redacted: %s", line)
	}
}

// A session id is a UUID and has to survive: redacting it would make every log line
// useless for the one thing they are read for.
func TestLoggerKeepsTheSessionID(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	logger := observability.NewLogger(&out, "info", "inst-a", "1.0.0")

	logger.Info().Str("sid", "2f1c6f0e-0000-4000-8000-000000000001").Msg("adopted")

	if !strings.Contains(out.String(), "2f1c6f0e-0000-4000-8000-000000000001") {
		t.Fatalf("the session id was mangled: %s", out.String())
	}
}

func TestLoggerCarriesTheInstanceAndVersion(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	logger := observability.NewLogger(&out, "info", "inst-a", "1.2.3")
	logger.Info().Msg("up")

	line := out.String()
	for _, want := range []string{`"inst":"inst-a"`, `"version":"1.2.3"`} {
		if !strings.Contains(line, want) {
			t.Errorf("log line %s is missing %s", line, want)
		}
	}
}

func TestLoggerFallsBackToInfoOnAnUnknownLevel(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	logger := observability.NewLogger(&out, "shouty", "inst-a", "1.0.0")
	logger.Info().Msg("up")

	if out.Len() == 0 {
		t.Fatal("an unparsable level silenced the logger")
	}
}

func TestMetricsRegisterOnTheirOwnRegistry(t *testing.T) {
	t.Parallel()

	metrics := observability.New()
	metrics.SessionsRunning.Set(3)
	metrics.EventsPublished.WithLabelValues("message.received").Inc()

	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	seen := make(map[string]bool, len(families))
	for _, family := range families {
		seen[family.GetName()] = true
	}
	for _, want := range []string{"wac_sessions_running", "wac_events_published_total"} {
		if !seen[want] {
			t.Errorf("%s was not gathered", want)
		}
	}
}
