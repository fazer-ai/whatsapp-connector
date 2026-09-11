package app

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/observability"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// gathered is what the registry would hand Prometheus for one metric, by the name it is
// published under. Read from the registry rather than off the field, because the way this
// goes wrong is a metric that is measured and never registered, and reading the object
// the test itself was given cannot tell.
func gathered(t *testing.T, c *Connector, name string) float64 {
	t.Helper()

	families, err := c.metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather the metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		if len(family.GetMetric()) != 1 {
			t.Fatalf("%s has %d series, want 1", name, len(family.GetMetric()))
		}
		metric := family.GetMetric()[0]
		if counter := metric.GetCounter(); counter != nil {
			return counter.GetValue()
		}
		if gauge := metric.GetGauge(); gauge != nil {
			return gauge.GetValue()
		}
		t.Fatalf("%s is neither a counter nor a gauge", name)
	}
	t.Fatalf("%s is measured and not registered, so nothing outside this process can read it", name)
	return 0
}

// muteStreams is a transport whose reads fail, and fail at once: the case that turns the
// loop's retry into a spin is a read that comes back immediately, not one that times out.
type muteStreams struct {
	inner commandStreams

	mu       sync.Mutex
	reads    int
	failing  bool
	waitsOut bool
}

func (s *muteStreams) Read(ctx context.Context, sids []string) ([]transport.Delivery, error) {
	s.mu.Lock()
	s.reads++
	failing, waitsOut := s.failing, s.waitsOut
	s.mu.Unlock()
	if waitsOut {
		// What the real transport does with a window it was given: block until the
		// deadline and come back with the context's error, having read nothing.
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if failing {
		return nil, errors.New("redisstream: read commands: read tcp 127.0.0.1:1->127.0.0.1:2: i/o timeout")
	}
	return s.inner.Read(ctx, sids)
}

func (s *muteStreams) Claim(ctx context.Context, sids []string) ([]transport.Delivery, error) {
	return s.inner.Claim(ctx, sids)
}

func (s *muteStreams) ClaimControl(ctx context.Context) ([]transport.Delivery, error) {
	return s.inner.ClaimControl(ctx)
}

func (s *muteStreams) ClaimSessions(ctx context.Context, sids []string) ([]transport.Delivery, error) {
	return s.inner.ClaimSessions(ctx, sids)
}

func (s *muteStreams) recovers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failing = false
}

func (s *muteStreams) attempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

// muted is one adopted session on an instance whose command reads fail, with what the
// instance says kept where the test can read it.
func muted(t *testing.T) (*Connector, *muteStreams, *bytes.Buffer) {
	t.Helper()

	connector, _, _, _ := adoptedSession(t, "2f1c6f0e-0000-4000-8000-0000000bb001")
	said := &bytes.Buffer{}
	connector.log = zerolog.New(said).Level(zerolog.DebugLevel)
	connector.metrics = observability.New()
	streams := &muteStreams{inner: connector.streams, failing: true}
	connector.streams = streams
	// Short enough that a run of failed reads is not a wall-clock cost of the test, and
	// long enough that the backoff is measurable against it.
	connector.cfg.Heartbeat = 40 * time.Millisecond
	return connector, streams, said
}

// A command read that fails is ordinary and a run of them is not: while the run lasts the
// instance carries out nothing for any session it owns, and nothing above it can tell.
// What it says has to change when that line is crossed, or an operator reads four
// identical error lines and learns nothing from the fourth that the first did not say.
func TestAnInstanceThatStopsReadingCommandsSaysSoOnce(t *testing.T) {
	t.Parallel()

	connector, _, said := muted(t)
	ctx := context.Background()

	for range silentReadsBeforeAlarm + 3 {
		connector.readCommands(ctx)
	}

	const alarm = "this instance has stopped reading commands"
	if got := strings.Count(said.String(), alarm); got != 1 {
		t.Fatalf("the instance said it had stopped reading %d time(s), want exactly 1:\n%s", got, said)
	}
	// The failures before the alarm are still reported one by one: the first one is where
	// a diagnosis starts, and swallowing it to wait for a run would lose the only line
	// that names the error when reads come back on their own.
	if got := strings.Count(said.String(), "failed to read commands"); got != silentReadsBeforeAlarm-1 {
		t.Fatalf("%d ordinary failures reported, want %d:\n%s", got, silentReadsBeforeAlarm-1, said)
	}
	if got := gathered(t, connector, "wac_command_reads_failed_total"); got != float64(silentReadsBeforeAlarm+3) {
		t.Fatalf("the failed-read counter is at %v, want %d", got, silentReadsBeforeAlarm+3)
	}
}

// And the recovery is what an operator is actually waiting for. Said only where the
// alarm was said, so a single failed read does not come with a recovery notice for
// something nobody was told about.
func TestReadingCommandsAgainIsSaidWhenTheInstanceHadGoneQuiet(t *testing.T) {
	t.Parallel()

	connector, streams, said := muted(t)
	ctx := context.Background()

	for range silentReadsBeforeAlarm {
		connector.readCommands(ctx)
	}
	streams.recovers()
	connector.readCommands(ctx)

	if !strings.Contains(said.String(), "reading commands again") {
		t.Fatalf("nothing said the reads came back:\n%s", said)
	}
	if last := gathered(t, connector, "wac_command_read_last_success_timestamp_seconds"); last <= 0 {
		t.Fatalf("the last successful read is recorded as %v, want a timestamp", last)
	}

	// And the run is closed, not merely reported: the next failure starts a new one and
	// has to be reported from the beginning again.
	said.Reset()
	streams.mu.Lock()
	streams.failing = true
	streams.mu.Unlock()
	connector.readCommands(ctx)
	if !strings.Contains(said.String(), "failed to read commands") {
		t.Fatalf("the failure after the recovery was not reported as the first of a run:\n%s", said)
	}

	// And a second run reaches the alarm the same way the first one did. An instance that
	// announced itself mute once and stayed marked would go quiet a second time in
	// silence, which is the failure this whole path exists to stop.
	for range silentReadsBeforeAlarm {
		connector.readCommands(ctx)
	}
	if got := strings.Count(said.String(), "this instance has stopped reading commands"); got != 1 {
		t.Fatalf("the second run of failures raised the alarm %d time(s), want 1:\n%s", got, said)
	}
}

// A read that fails at once returns while the window it ran under is still open, and the
// loop goes straight back in. Without a pause that is a spin against a dependency already
// in trouble, for as long as the window lasts.
func TestAFailedCommandReadIsNotTriedAgainAtOnce(t *testing.T) {
	t.Parallel()

	connector, streams, _ := muted(t)
	ctx := context.Background()

	const rounds = 4
	started := time.Now()
	for range rounds {
		connector.readCommands(ctx)
	}
	elapsed := time.Since(started)

	floor := rounds * (connector.cfg.Heartbeat / 4)
	if elapsed < floor {
		t.Fatalf("%d failed reads took %s, want at least %s: the loop is retrying without a pause",
			rounds, elapsed.Round(time.Millisecond), floor)
	}
	if got := streams.attempts(); got != rounds {
		t.Fatalf("the transport was read %d time(s), want %d", got, rounds)
	}
}

// The pause after a failed read is the loop's, not the caller's: it must not outlive the
// window it was given, because what the instance owes next is a lease renewal.
func TestTheWaitAfterAFailedReadEndsWithTheWindow(t *testing.T) {
	t.Parallel()

	connector, _, _ := muted(t)
	// Long enough that waiting the backoff out would be unmistakable against the window.
	connector.cfg.Heartbeat = 10 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	started := time.Now()
	connector.readCommands(ctx)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("a failed read held on for %s after its window ended, want the wait to end with it",
			elapsed.Round(time.Millisecond))
	}
}

// A read that comes back because its window ran out is the deadline working, not the
// instance failing: the tick hands out what is left of its period on purpose. Counting it
// would make an ordinary busy instance look like one that has stopped serving, which is
// the distinction the alarm is worth anything for.
func TestAWindowThatRanOutIsNotCountedAgainstTheInstance(t *testing.T) {
	t.Parallel()

	connector, streams, said := muted(t)
	streams.mu.Lock()
	streams.waitsOut = true
	streams.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	connector.readCommands(ctx)

	if strings.Contains(said.String(), "failed to read commands") {
		t.Fatalf("a spent window was reported as a failed read:\n%s", said)
	}
	if got := gathered(t, connector, "wac_command_reads_failed_total"); got != 0 {
		t.Fatalf("the failed-read counter is at %v after a spent window, want 0", got)
	}
}
