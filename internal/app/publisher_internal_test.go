package app

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/fazer-ai/whatsapp-connector/internal/observability"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

type stubPublisher struct {
	err  error
	sent int
}

func (s *stubPublisher) Publish(context.Context, *protocol.Event) error {
	s.sent++
	return s.err
}

// `wac_events_published_total` is the one thing that could tell an incident "events are
// arriving and not being published" from "no events are arriving", and for that it has
// to count what reached the client rather than what was attempted.
func TestOnlyAnEventThatReachedTheClientIsCounted(t *testing.T) {
	t.Parallel()

	metrics := observability.New()
	stub := &stubPublisher{}
	publisher := countingPublisher{to: stub, metrics: metrics}
	ctx := context.Background()

	for range 2 {
		if err := publisher.Publish(ctx, &protocol.Event{Type: protocol.EventMessageReceived}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if err := publisher.Publish(ctx, &protocol.Event{Type: protocol.EventSessionState}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// The stream refuses this one: it never reached the client and must not be counted.
	stub.err = errors.New("the stream refused it")
	if err := publisher.Publish(ctx, &protocol.Event{Type: protocol.EventMessageReceived}); err == nil {
		t.Fatal("publish returned nil for a refused event")
	}

	want := `
# HELP wac_events_published_total Events published to the client, by event type.
# TYPE wac_events_published_total counter
wac_events_published_total{type="message.received"} 2
wac_events_published_total{type="session.state"} 1
`
	if err := testutil.CollectAndCompare(
		metrics.EventsPublished, strings.NewReader(want), "wac_events_published_total",
	); err != nil {
		t.Fatalf("counts: %v", err)
	}
	if stub.sent != 4 {
		t.Errorf("the wrapper passed %d events through, want 4", stub.sent)
	}
}

// A refusal has to reach the caller unchanged: invariant 4 says the WhatsApp ack waits on
// the publish, so a wrapper that swallowed the error would acknowledge a message nobody
// was ever told about.
func TestTheWrapperDoesNotSwallowARefusal(t *testing.T) {
	t.Parallel()

	refused := errors.New("the stream refused it")
	publisher := countingPublisher{to: &stubPublisher{err: refused}, metrics: observability.New()}
	if err := publisher.Publish(context.Background(), &protocol.Event{Type: protocol.EventMessageReceived}); !errors.Is(err, refused) {
		t.Fatalf("err = %v, want %v", err, refused)
	}
}
