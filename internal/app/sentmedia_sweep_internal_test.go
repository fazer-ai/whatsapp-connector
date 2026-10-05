package app

import (
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/observability"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// The connector's own sweep pass reaches the media messages kept for their captions, with
// the retention the store names: one older than it is gone after a pass, one inside it
// stays. Without the pass the table grows by a row per file ever sent (#32).
func TestTheSweepPassDropsSentMediaPastItsRetention(t *testing.T) {
	t.Parallel()

	container := openTestStore(t)
	seedExpiredPart(t, container, "sid-1", "3EB0PART")
	scoped := container.For("sid-1")
	for _, id := range []string{"3EB0OLD", "3EB0NEW"} {
		if err := scoped.PutSentMedia(t.Context(), id, store.SentMedia{Chat: "c@s.whatsapp.net", Body: []byte{1}}); err != nil {
			t.Fatalf("PutSentMedia %s: %v", id, err)
		}
	}
	// Written a moment ago, and moved back past the retention by an hour: the store takes
	// the clock at the write, and this is the one row the test needs older than that.
	past := (store.SentMediaRetention + time.Hour).Milliseconds()
	if _, err := container.DB().ExecContext(t.Context(),
		"UPDATE wac_sent_media SET sent_at = sent_at - "+strconv.FormatInt(past, 10)+" WHERE message_id = '3EB0OLD'"); err != nil {
		t.Fatalf("age the row: %v", err)
	}

	connector := &Connector{
		metrics: observability.New(),
		cfg:     Config{MediaRefetch: DefaultMediaRefetch},
		log:     zerolog.Nop(),
		store:   container,
	}
	if stop := connector.sweepPartsOnce(t.Context()); stop {
		t.Fatal("the sweep pass stopped as if its context were done")
	}

	for id, want := range map[string]bool{"3EB0OLD": false, "3EB0NEW": true} {
		if _, found, err := scoped.SentMedia(t.Context(), id); err != nil {
			t.Fatalf("read %s: %v", id, err)
		} else if found != want {
			t.Errorf("%s present=%v after a sweep pass, want %v", id, found, want)
		}
	}
}
