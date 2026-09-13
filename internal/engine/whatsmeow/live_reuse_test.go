//go:build live

package whatsmeow

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/media"
)

// TestLiveReuse is issue #24 against WhatsApp: a file that arrived over a real socket,
// asked for twice, served from this instance's disk the second time.
//
// It is the one question no unit test in this package answers. Everywhere else the
// download is a fake that writes whatever the test handed it, so "the reused blob serves
// the right bytes" is a statement about a fixture. Here the bytes went up to WhatsApp,
// came back down encrypted, were decrypted into the store, and what is compared is what
// the sending side actually put on the wire.
//
// No person is needed and that is deliberate. Every account this drives is already
// paired in the live store, and the file is sent by the second account through this
// connector's own send path, so the phase is one command away from being repeatable in
// review. The media root is this phase's own: the default one is shared between runs and
// holds blobs from earlier ones, and counting files in it is half of what this measures.
func TestLiveReuse(t *testing.T) {
	const token = "live-check"

	root := t.TempDir()
	blobs, err := media.New(media.Options{Root: root})
	if err != nil {
		t.Fatalf("open the blob store at %s: %v", root, err)
	}
	t.Cleanup(func() { _ = blobs.Close() })

	mux := http.NewServeMux()
	mux.Handle("GET /media/{id}", media.Handler(media.HandlerOptions{Blobs: blobs, Token: token}))
	endpoint := httptest.NewServer(mux)
	t.Cleanup(endpoint.Close)

	subject, counterpart, container := liveBoth(t, MediaOptions{Blobs: blobs, BaseURL: endpoint.URL})

	// Where the file is going: the account under test, as the store knows it.
	to := liveMustBePaired(t, container, liveSID)
	liveMustBePaired(t, container, liveCounterpartSID)

	// Watched before either connects, because the event that matters arrives on the
	// subject as soon as the counterpart's send lands.
	events := watch(t, subject)
	// Each side waited on separately, and the sender as much as the receiver. Connect
	// answers once the socket is open and authentication finishes after it, so a send
	// issued on the strength of the other account being up is refused `not_connected` by
	// a session that was simply slower.
	resume := engine.ConnectRequest{Pairing: "resume"}
	liveResumeAsking(t, subject, resume)
	liveResumeAsking(t, counterpart, resume)

	// The second account sends a real file, fetched from here over HTTP the way a client's
	// outbound attachment is.
	file, name, kind := liveFileToSend(t)
	outbound := http.NewServeMux()
	outbound.HandleFunc("GET /outbound/{name}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", liveMime(name))
		w.Header().Set("Content-Length", strconv.Itoa(len(file)))
		_, _ = w.Write(file)
	})
	sending := httptest.NewServer(outbound)
	t.Cleanup(sending.Close)

	sent := liveSendOne(t, counterpart, to.User, map[string]any{
		"type": "media", "kind": kind, "filename": name, "size": len(file),
		"ref": map[string]any{"kind": "url", "url": sending.URL + "/outbound/" + name},
	})
	say("%s sent %s (%d bytes) to %s as %s", liveCounterpartSID, name, len(file), to.User, sent)

	messageID, content := oneLiveFile(t, events, endpoint.URL, token, liveDeadline(t, 3*time.Minute))
	published := content.Ref
	if digest := fmt.Sprintf("%x", sha256.Sum256(file)); published.SHA256 != digest {
		t.Fatalf("WhatsApp delivered a file hashing to %s and %s put %s on the wire",
			published.SHA256, liveCounterpartSID, digest)
	}
	say("the round trip preserved the bytes: %s", published.SHA256)

	blobsAfterEvent, bytesAfterEvent := countBlobs(t, root)
	if blobsAfterEvent != 1 {
		t.Fatalf("the event left %d blobs under %s, want the one it downloaded", blobsAfterEvent, root)
	}

	// And now the command the issue is about, twice, with nothing taken off the disk in
	// between. Before this, each one paid for the file again.
	first := refetch(t, subject, messageID, nil)
	second := refetch(t, subject, messageID, nil)

	if first.ID != published.ID || second.ID != published.ID {
		t.Errorf("the event published %s and the commands answered %s and %s, want one blob for the three",
			published.ID, first.ID, second.ID)
	}
	if first.URL != published.URL || second.URL != published.URL {
		t.Errorf("the commands published %q and %q, want the event's own %q", first.URL, second.URL, published.URL)
	}
	if blobs, held := countBlobs(t, root); blobs != 1 || held != bytesAfterEvent {
		t.Errorf("two commands took the store from 1 blob and %d bytes to %d and %d, so they downloaded again",
			bytesAfterEvent, blobs, held)
	}
	if now := time.Now().UnixMilli(); first.ExpiresAt <= now || second.ExpiresAt <= now {
		t.Errorf("a command answered a reference that lapsed at %d and %d, and it is %d",
			first.ExpiresAt, second.ExpiresAt, now)
	}

	// What a client does with what it was handed, both times: the bytes, and the headers
	// it renders the attachment from.
	one, two := fetchBlob(t, first.URL, token), fetchBlob(t, second.URL, token)
	for i, got := range []fetched{one, two} {
		if got.status != http.StatusOK {
			t.Fatalf("fetching the reference of command %d answered %d: %s", i+1, got.status, got.body)
		}
		if digest := fmt.Sprintf("%x", sha256.Sum256(got.body)); digest != published.SHA256 {
			t.Errorf("command %d served bytes hashing to %s, want the file that was sent, %s",
				i+1, digest, published.SHA256)
		}
	}
	if one.mime != two.mime || one.disposition != two.disposition {
		t.Errorf("the two fetches were served as (%q, %q) and (%q, %q)",
			one.mime, one.disposition, two.mime, two.disposition)
	}
	if content.Mime != "" && one.mime != content.Mime {
		t.Errorf("the reused blob is served as %q and the message said %q", one.mime, content.Mime)
	}
	if content.Filename != "" && one.disposition == "" {
		t.Errorf("the message named the file %q and the reused blob is served with no disposition", content.Filename)
	}
	say("both commands answered %s, served %d bytes as %q (%s), and nothing was downloaded twice",
		first.ID, len(one.body), one.mime, one.disposition)

	if state := subject.state(); state != "open" {
		t.Fatalf("the session did not stay up: state=%s", state)
	}
}
