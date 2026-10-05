package store_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/store"
	"github.com/fazer-ai/whatsapp-connector/internal/store/storetest"
)

// A media message is kept as it went out and read back the same, and a restart between
// the send and the edit is a reopen of the same store: the caption is still somebody's to
// correct after a deploy (#32).
func TestASentMediaMessageIsReadBackAfterAReopen(t *testing.T) {
	t.Parallel()
	target := storetest.New(t)
	container := openAt(t, target)
	pair(t, container, "sid-1", "5511999990001")

	body := []byte{0x0a, 0x03, 'a', 'b', 0x00, 0xff}
	if err := container.For("sid-1").PutSentMedia(t.Context(), "3EB0FILE",
		store.SentMedia{Chat: "5511999990002@s.whatsapp.net", Body: body}); err != nil {
		t.Fatalf("PutSentMedia: %v", err)
	}
	if err := container.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openAt(t, target)
	kept, found, err := reopened.For("sid-1").SentMedia(t.Context(), "3EB0FILE")
	if err != nil {
		t.Fatalf("SentMedia: %v", err)
	}
	if !found {
		t.Fatal("a media message kept before a restart was gone after it")
	}
	if kept.Chat != "5511999990002@s.whatsapp.net" || !bytes.Equal(kept.Body, body) {
		t.Errorf("read back %q and % x, want what was kept", kept.Chat, kept.Body)
	}

	if _, found, err := reopened.For("sid-2").SentMedia(t.Context(), "3EB0FILE"); err != nil || found {
		t.Errorf("another session read this one's message (found=%v, err=%v)", found, err)
	}
}

// The sweep takes what is older than the window and leaves the rest, and what it takes is
// gone for an edit too.
func TestTheSweepDropsSentMediaPastTheWindowOnly(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")

	for _, id := range []string{"3EB0OLD", "3EB0NEW"} {
		if err := scoped.PutSentMedia(t.Context(), id, store.SentMedia{Chat: "c@s.whatsapp.net", Body: []byte{1}}); err != nil {
			t.Fatalf("PutSentMedia %s: %v", id, err)
		}
	}
	// Everything kept so far is older than a moment from now, and nothing is older than a
	// moment ago.
	if dropped, err := container.SweepSentMedia(t.Context(), time.Now().Add(-time.Minute)); err != nil || dropped != 0 {
		t.Fatalf("a sweep of the past dropped %d rows (err=%v), want none", dropped, err)
	}
	dropped, err := container.SweepSentMedia(t.Context(), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("SweepSentMedia: %v", err)
	}
	if dropped != 2 {
		t.Errorf("dropped %d rows, want both", dropped)
	}
	if _, found, _ := scoped.SentMedia(t.Context(), "3EB0OLD"); found {
		t.Error("a swept message is still read back")
	}
}

// The window is a contract with the client: an edit of a caption is honoured for at least
// as long as WhatsApp itself takes one, which is about fifteen minutes.
func TestTheWindowOutlastsWhatsAppsOwn(t *testing.T) {
	t.Parallel()
	if store.SentMediaRetention < 15*time.Minute {
		t.Errorf("sent media is kept for %s, shorter than WhatsApp's own edit window", store.SentMediaRetention)
	}
}

// The row holds a file's key on behalf of a pairing, and unpairing takes it with it.
func TestSentMediaGoesWithThePairing(t *testing.T) {
	t.Parallel()
	container := open(t)
	pair(t, container, "sid-1", "5511999990001")
	scoped := container.For("sid-1")
	if err := scoped.PutSentMedia(t.Context(), "3EB0FILE", store.SentMedia{Chat: "c@s.whatsapp.net", Body: []byte{1}}); err != nil {
		t.Fatalf("PutSentMedia: %v", err)
	}
	if err := scoped.ForgetCredentialsAndDesired(t.Context()); err != nil {
		t.Fatalf("ForgetCredentialsAndDesired: %v", err)
	}
	if _, found, err := scoped.SentMedia(t.Context(), "3EB0FILE"); err != nil || found {
		t.Errorf("an unpaired session still holds a sent file's key (found=%v, err=%v)", found, err)
	}
}
