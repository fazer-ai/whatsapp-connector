//go:build live

// The live checks. They talk to WhatsApp with a real account and need a human holding
// the phone, so they are behind a build tag and never run in CI or in `make check`.
//
// What they exist for: the failures left in this engine are the ones reading cannot
// reach. A lock taken in the wrong order, an event arriving before the state it
// describes, the 515 stream error WhatsApp sends in the middle of pairing, a device
// that resumes on one boot and asks for a new code on the next. Every one of those
// shows up within minutes of a real pairing and within none of a unit test.
//
// Run one phase at a time, in this order:
//
//	go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLivePairWithQR
//	go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveResume
//	go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveHangUpAndBack
//	go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveListen
//	go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveMedia
//	go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveRefetch
//	go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveViewOnce
//	WAC_LIVE_TO=<number> go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveSend$
//	WAC_LIVE_TO=<number> go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveSendMedia
//	  (WAC_LIVE_FILE names the file; WAC_LIVE_VOICE=1 sends it as a voice note)
//	WAC_LIVE_TO=<number> go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveSendLocation
//	WAC_LIVE_TO=<number> go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveSendContacts
//	go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveLogout
//
// The store is deliberately not a t.TempDir: resuming is the thing being checked, and
// a pairing that does not outlive the process proves nothing. Point WAC_LIVE_DB
// somewhere durable, or accept the default under the user's state directory.
package whatsmeow

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// liveSID is the account under test. One is enough: what is being exercised is a
// device's own lifecycle, not the fleet's.
const liveSID = "live-1"

// TestLivePairWithQR pairs by scanning. It is the first phase and the only one that
// needs the phone in hand.
func TestLivePairWithQR(t *testing.T) {
	session, container := liveSession(t)
	events := watch(t, session)

	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "qr"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	events.await(t, protocol.EventPairingSuccess, liveDeadline(t, 5*time.Minute))
	events.awaitState(t, "open", 2*time.Minute)

	jid, bound, err := container.JID(t.Context(), liveSID)
	if err != nil || !bound {
		t.Fatalf("the pairing was announced but nothing was written down (bound=%v, err=%v)", bound, err)
	}
	if jid.Device == 0 {
		t.Fatalf("WhatsApp issued %s with no device part, which resume cannot key on", jid)
	}
	t.Logf("paired as %s", jid)
}

// TestLivePairWithCode pairs by typing an eight-character code into the phone. The
// alternative to TestLivePairWithQR, not a step after it.
func TestLivePairWithCode(t *testing.T) {
	phone := os.Getenv("WAC_LIVE_PHONE")
	if phone == "" {
		t.Skip("set WAC_LIVE_PHONE to the number to pair, country code and no plus")
	}

	session, container := liveSession(t)
	events := watch(t, session)

	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "code", Phone: phone}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	events.await(t, protocol.EventPairingCode, 2*time.Minute)
	events.await(t, protocol.EventPairingSuccess, liveDeadline(t, 5*time.Minute))
	events.awaitState(t, "open", 2*time.Minute)

	if _, bound, err := container.JID(t.Context(), liveSID); err != nil || !bound {
		t.Fatalf("the pairing was announced but nothing was written down (bound=%v, err=%v)", bound, err)
	}
}

// TestLiveResume is the one that catches a broken device key. It runs in a process
// that never saw the pairing, which is exactly what a connector restart is.
func TestLiveResume(t *testing.T) {
	session, container := liveSession(t)

	jid, bound, err := container.JID(t.Context(), liveSID)
	if err != nil || !bound {
		t.Fatalf("nothing is paired yet; run TestLivePairWithQR first (err=%v)", err)
	}

	events := watch(t, session)
	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Read the moment Connect returns, which is the moment the reply to
	// `session.connect` is built. ConnectContext comes back once the socket is up and
	// the handshake runs on from there, so this is the window where a session that is
	// connecting perfectly well can be reported as closed.
	if state := session.state(); state == "close" {
		t.Fatal("a resume in progress reported itself closed, which is the reply the client acts on")
	}

	events.awaitState(t, "open", 2*time.Minute)

	if events.count(protocol.EventPairingQR) > 0 {
		t.Fatal("a resume asked for a new code, which means the stored device was not found")
	}

	status, err := session.Execute(t.Context(), &protocol.Command{Type: protocol.CommandSessionStatus})
	if err != nil {
		t.Fatalf("session.status: %v", err)
	}
	var reported map[string]any
	if err := json.Unmarshal(status, &reported); err != nil {
		t.Fatalf("unmarshal the status: %v", err)
	}
	if reported["connection"] != "open" {
		t.Fatalf("connection=%v, want open", reported["connection"])
	}
	if reported["phone_number"] != jid.User {
		t.Fatalf("phone_number=%v, want %s", reported["phone_number"], jid.User)
	}
}

// TestLiveHangUpAndBack takes the socket down and brings it back on one session. A
// hang-up that leaves the client believing it is still logged in, or a reconnect that
// races the teardown, shows up here and nowhere in a unit test.
func TestLiveHangUpAndBack(t *testing.T) {
	session, _ := liveSession(t)
	events := watch(t, session)

	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	events.awaitState(t, "open", 2*time.Minute)

	if err := session.Disconnect(t.Context()); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	events.awaitState(t, "close", 30*time.Second)

	// A session that comes back has to come back on its own credentials, without a
	// code and without a second device.
	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect after a hang-up: %v", err)
	}
	events.awaitState(t, "open", 2*time.Minute)
	if events.count(protocol.EventPairingQR) > 0 {
		t.Fatal("coming back from a hang-up asked for a new code")
	}
}

// TestLiveListen holds a paired session open and waits for a real message. A human
// sends a text from another phone, and the phase passes only if it comes back out as
// the event the contract names: it is the one check that the whole inbound path works
// against WhatsApp rather than against a fixture.
func TestLiveListen(t *testing.T) {
	window := liveWindow(t, 2*time.Minute)

	session, _ := liveSession(t)
	events := watch(t, session)

	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	events.awaitState(t, "open", 2*time.Minute)

	fmt.Fprintf(os.Stderr, "send a text to the paired number now; waiting up to %s\n", window)
	received := events.await(t, protocol.EventMessageReceived, window)

	var body struct {
		Message struct {
			ID      string                      `json:"id"`
			Chat    struct{ Kind, ID string }   `json:"chat"`
			Content struct{ Type, Body string } `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(received.Payload, &body); err != nil {
		t.Fatalf("unmarshal the message: %v", err)
	}
	if body.Message.ID == "" || body.Message.Chat.ID == "" {
		t.Fatalf("the message arrived without an id or a chat: %s", received.Payload)
	}
	if body.Message.Content.Type != "text" || body.Message.Content.Body == "" {
		t.Fatalf("the message arrived without a text body: %s", received.Payload)
	}
	if state := session.state(); state != "open" {
		t.Fatalf("the session did not stay up: state=%s", state)
	}
}

// TestLiveMedia is the media half of TestLiveListen, and the only check that walks the
// whole path end to end: WhatsApp encrypts a file, whatsmeow fetches and decrypts it,
// the store keeps it, the event says where to fetch it, and the endpoint hands it back
// over HTTP against a bearer token. Every step of that has a unit test and none of them
// had ever run against a file somebody really sent.
//
// The endpoint is served here on a throwaway port and the reference is published under
// it, which is what makes the URL on the event the URL that is actually fetched: a
// reference nobody dials proves nothing about whether it could be dialled.
func TestLiveMedia(t *testing.T) {
	const token = "live-check"

	root := filepath.Join(liveDir(t), "blobs")
	blobs, err := media.New(media.Options{Root: root})
	if err != nil {
		t.Fatalf("open the blob store at %s: %v", root, err)
	}
	t.Cleanup(func() {
		if err := blobs.Close(); err != nil {
			t.Errorf("close the blob store: %v", err)
		}
	})
	t.Logf("blobs: %s", root)

	// Started before the session, because the engine refuses a store it has no address
	// to publish under and the address is this server's.
	mux := http.NewServeMux()
	mux.Handle("GET /media/{id}", media.Handler(media.HandlerOptions{Blobs: blobs, Token: token}))
	endpoint := httptest.NewServer(mux)
	t.Cleanup(endpoint.Close)

	session, _ := liveSessionWith(t, MediaOptions{Blobs: blobs, BaseURL: endpoint.URL})
	events := watch(t, session)

	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	events.awaitState(t, "open", 2*time.Minute)

	window := liveWindow(t, 3*time.Minute)
	// One file proves the path; several prove the translation, and each kind is the only
	// one that carries something: a document has the filename, a voice note the flag and
	// the duration, a sticker the PNG preview where every other kind has a JPEG one.
	wanted := liveCount(t)
	say("send %d file(s) to the paired number now — photo, video, VOICE NOTE, DOCUMENT, STICKER; waiting up to %s",
		wanted, window)

	seen := map[protocol.MediaKind]int{}
	deadline := time.Now().Add(window)
	for i := 1; i <= wanted; i++ {
		left := time.Until(deadline)
		if left <= 0 {
			t.Fatalf("only %d of %d files arrived before the window closed", i-1, wanted)
		}
		_, content := oneLiveFile(t, events, endpoint.URL, token, left)
		seen[content.Kind]++
		say("%d/%d done", i, wanted)
	}
	say("kinds that arrived: %v", seen)

	if state := session.state(); state != "open" {
		t.Fatalf("the session did not stay up: state=%s", state)
	}
}

// oneLiveFile waits for one media message, checks what the contract says about it, and
// fetches the bytes from the URL the event carried. The id comes back with the content
// because a message is asked about again by name.
func oneLiveFile(t *testing.T, events *recorder, base, token string, within time.Duration) (string, protocol.MediaContent) {
	t.Helper()

	received := events.await(t, protocol.EventMessageReceived, within)
	var body struct {
		Message struct {
			ID      string                `json:"id"`
			Content protocol.MediaContent `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(received.Payload, &body); err != nil {
		t.Fatalf("unmarshal the message: %v", err)
	}
	content := body.Message.Content
	if content.Type != "media" {
		t.Fatalf("that was not a media message (%s). Send a file rather than a text.", content.Type)
	}
	if content.Ref == nil {
		t.Fatalf("the message arrived with nothing to fetch: %s", received.Payload)
	}
	say("got a %s: mime=%q filename=%q size=%d duration=%d voice_note=%v thumbnail=%dB",
		content.Kind, content.Mime, content.Filename, content.Size, content.Duration,
		content.VoiceNote, len(content.Thumbnail))

	ref := content.Ref
	if ref.Kind != protocol.MediaRefConnectorBlob || ref.ID == "" || ref.URL == "" || ref.SHA256 == "" {
		t.Fatalf("the reference does not describe a blob on this instance: %+v", ref)
	}
	if want := base + "/media/" + ref.ID; ref.URL != want {
		t.Fatalf("the reference points at %q, want %q", ref.URL, want)
	}

	// The whole point of the phase: fetch what the client would fetch, from the URL the
	// event carried, with the token the registry publishes.
	fetched := fetchBlob(t, ref.URL, token)
	if fetched.status != http.StatusOK {
		t.Fatalf("fetching the blob answered %d: %s", fetched.status, fetched.body)
	}
	if int64(len(fetched.body)) != ref.Size {
		t.Fatalf("the blob is %d bytes and the reference says %d", len(fetched.body), ref.Size)
	}
	if digest := fmt.Sprintf("%x", sha256.Sum256(fetched.body)); digest != ref.SHA256 {
		t.Fatalf("the bytes hash to %s and the reference says %s", digest, ref.SHA256)
	}
	if content.Mime != "" && fetched.mime != content.Mime {
		t.Fatalf("the endpoint served %q and the message said %q", fetched.mime, content.Mime)
	}
	say("fetched %d bytes from %s, digest matches", len(fetched.body), ref.URL)

	// And the other half of the endpoint: what guards a URL that hands out somebody's
	// file is the token, and nothing else.
	if refused := fetchBlob(t, ref.URL, ""); refused.status != http.StatusUnauthorized {
		t.Fatalf("fetching without the token answered %d, want 401", refused.status)
	}
	return body.Message.ID, content
}

// liveCount is how many files this phase waits for.
func liveCount(t *testing.T) int {
	t.Helper()

	raw := os.Getenv("WAC_LIVE_FILES")
	if raw == "" {
		return 1
	}
	count, err := strconv.Atoi(raw)
	if err != nil || count < 1 {
		t.Fatalf("WAC_LIVE_FILES=%q: want a positive count", raw)
	}
	return count
}

// TestLiveRefetch is M2.2c against the real CDN. A blob is instance-local and
// time-bounded, so the address published with a message stops working, and what has to
// hold is that the coordinates filed with it are still enough to fetch the same file
// again minutes or days later. A fake downloader cannot answer that: whether a
// `directPath` outlives its blob, and whether the key kept with it still decrypts what
// comes back, is WhatsApp's answer to give.
//
// Run it with no file in hand and it asks for one. Run it with WAC_LIVE_REFETCH_ID set
// to a message a previous run filed, and it fetches that one instead, which is the same
// check across a real restart rather than across a sweep.
func TestLiveRefetch(t *testing.T) {
	const token = "live-check"

	// This phase's own root, thrown away with it: the file not being cached is the
	// premise, and a root shared with the media phase would start out holding it.
	root := t.TempDir()

	// The sweep reads the clock through the store, so the phase moves the clock instead
	// of waiting a TTL out. A TTL short enough to expire on its own would be racing the
	// download of the very message being set up.
	var ahead atomic.Int64
	blobs, err := media.New(media.Options{
		Root: root,
		TTL:  5 * time.Minute,
		Now:  func() time.Time { return time.Now().Add(time.Duration(ahead.Load())) },
	})
	if err != nil {
		t.Fatalf("open the blob store at %s: %v", root, err)
	}
	t.Cleanup(func() {
		if err := blobs.Close(); err != nil {
			t.Errorf("close the blob store: %v", err)
		}
	})

	mux := http.NewServeMux()
	mux.Handle("GET /media/{id}", media.Handler(media.HandlerOptions{Blobs: blobs, Token: token}))
	endpoint := httptest.NewServer(mux)
	t.Cleanup(endpoint.Close)

	session, container := liveSessionWith(t, MediaOptions{Blobs: blobs, BaseURL: endpoint.URL})
	events := watch(t, session)

	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	events.awaitState(t, "open", 2*time.Minute)

	// gone is the reference the message was published with, and it is only known when
	// this run is the one that received it.
	messageID, gone := os.Getenv("WAC_LIVE_REFETCH_ID"), protocol.MediaRef{}
	if messageID == "" {
		window := liveWindow(t, 3*time.Minute)
		say("send ONE file to the paired number now; waiting up to %s", window)

		var content protocol.MediaContent
		messageID, content = oneLiveFile(t, events, endpoint.URL, token, window)
		gone = *content.Ref
		say("message %s — pass it as WAC_LIVE_REFETCH_ID on a later run to check the same thing across a restart", messageID)

		// The premise, made true the way production makes it true: the blob ages out
		// and the sweep collects it.
		ahead.Store(int64(10 * time.Minute))
		dropped, freed, err := blobs.Sweep(t.Context())
		ahead.Store(0)
		if err != nil {
			t.Fatalf("sweep the blob store: %v", err)
		}
		if dropped == 0 {
			t.Fatal("the sweep collected nothing, so the file this phase is about is still cached and nothing below would be proven")
		}
		say("swept %d blob(s), %d bytes freed", dropped, freed)

		if still := fetchBlob(t, gone.URL, token); still.status != http.StatusNotFound {
			t.Fatalf("the address published with the message still answers %d, want 404", still.status)
		}
	}

	// What makes the refetch possible at all is a row, not a file. Reading it here says
	// which of the two is missing when the command below fails.
	kept, found, err := container.MediaPart(t.Context(), liveSID, messageID)
	if err != nil {
		t.Fatalf("read what was filed for %s: %v", messageID, err)
	}
	if !found {
		t.Fatalf("nothing was filed for %s, so there is nothing to fetch again", messageID)
	}
	say("filed: kind=%s mime=%q filename=%q size=%d", kept.Kind, kept.Mime, kept.Filename, kept.FileLength)

	again := refetch(t, session, messageID, nil)
	if again.Kind != protocol.MediaRefConnectorBlob {
		t.Fatalf("the refetch answered a %q reference, want the blob it just wrote", again.Kind)
	}
	if want := endpoint.URL + "/media/" + again.ID; again.URL != want {
		t.Fatalf("the refetch published under %q, want %q", again.URL, want)
	}
	if gone.ID != "" {
		if again.ID == gone.ID {
			t.Fatal("the refetch handed back the blob that was swept")
		}
		if again.Size != gone.Size || again.SHA256 != gone.SHA256 {
			t.Fatalf("the refetch served %d bytes (%s), want the same file as %d (%s)",
				again.Size, again.SHA256, gone.Size, gone.SHA256)
		}
	}

	// The whole point: these bytes came from WhatsApp just now, on coordinates that
	// outlived the file they were filed with.
	fetched := fetchBlob(t, again.URL, token)
	if fetched.status != http.StatusOK {
		t.Fatalf("fetching the refetched blob answered %d: %s", fetched.status, fetched.body)
	}
	if int64(len(fetched.body)) != again.Size {
		t.Fatalf("the blob is %d bytes and the reference says %d", len(fetched.body), again.Size)
	}
	if digest := fmt.Sprintf("%x", sha256.Sum256(fetched.body)); digest != again.SHA256 {
		t.Fatalf("the bytes hash to %s and the reference says %s", digest, again.SHA256)
	}
	say("fetched %d bytes from %s, digest matches", len(fetched.body), again.URL)

	// Asking twice is what a redelivered command does, and it is answered by paying for
	// the file a second time rather than by handing back the first answer. That cost is
	// issue #24; what is checked here is that the answer is at least a working one.
	twice := refetch(t, session, messageID, nil)
	if twice.ID == again.ID {
		t.Fatalf("two refetches named the same blob %s, which the ledger is not supposed to be keeping", twice.ID)
	}
	if twice.SHA256 != again.SHA256 {
		t.Fatalf("the second refetch served %s and the first served %s", twice.SHA256, again.SHA256)
	}
	if repeat := fetchBlob(t, twice.URL, token); repeat.status != http.StatusOK {
		t.Fatalf("fetching the second refetch answered %d: %s", repeat.status, repeat.body)
	}

	if state := session.state(); state != "open" {
		t.Fatalf("the session did not stay up: state=%s", state)
	}
}

// TestLiveViewOnce is the decision this build makes about a file sent to be seen once:
// the message goes out, the file is not kept, and the preview does not travel either.
//
// It usually does not get that far, and finding out is what it was written for. WhatsApp
// does not hand a view-once message to a companion device: what arrives is a stub with no
// ciphertext (`Unavailable message ... type: "view_once"`), which whatsmeow acknowledges
// while asking the primary phone to send the real one over. If the phone answers, this
// phase sees the message and the assertions below hold. If it does not, nothing arrives
// at all, which is issue #20 rather than a failure of this code.
func TestLiveViewOnce(t *testing.T) {
	const token = "live-check"

	root := filepath.Join(liveDir(t), "blobs")
	blobs, err := media.New(media.Options{Root: root})
	if err != nil {
		t.Fatalf("open the blob store at %s: %v", root, err)
	}
	t.Cleanup(func() { _ = blobs.Close() })

	mux := http.NewServeMux()
	mux.Handle("GET /media/{id}", media.Handler(media.HandlerOptions{Blobs: blobs, Token: token}))
	endpoint := httptest.NewServer(mux)
	t.Cleanup(endpoint.Close)

	session, _ := liveSessionWith(t, MediaOptions{Blobs: blobs, BaseURL: endpoint.URL})
	events := watch(t, session)

	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	events.awaitState(t, "open", 2*time.Minute)

	window := liveWindow(t, 3*time.Minute)
	say("send a VIEW ONCE photo or video to the paired number now; waiting up to %s", window)
	received := events.await(t, protocol.EventMessageReceived, window)

	var body struct {
		Message struct {
			ID      string                `json:"id"`
			Content protocol.MediaContent `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(received.Payload, &body); err != nil {
		t.Fatalf("unmarshal the message: %v", err)
	}
	content := body.Message.Content
	if content.Type != "media" {
		t.Fatalf("that was not a media message (%s). Send a view-once photo.", content.Type)
	}
	if content.Ref != nil {
		t.Fatalf("a view-once file was kept and handed out as %+v", content.Ref)
	}
	if content.Thumbnail != "" {
		t.Fatalf("a view-once message carried a %d-byte preview of what was not kept", len(content.Thumbnail))
	}

	// Published after the message and never instead of it, which is what lets the client
	// find the message it belongs to.
	failure := events.await(t, protocol.EventMediaDownloadFailed, 30*time.Second)
	var why protocol.MediaDownloadFailure
	if err := json.Unmarshal(failure.Payload, &why); err != nil {
		t.Fatalf("unmarshal the failure: %v", err)
	}
	if why.MessageID != body.Message.ID || why.Reason != reasonViewOnce {
		t.Fatalf("the failure is %+v, want %s for %s", why, reasonViewOnce, body.Message.ID)
	}
	say("a view-once %s arrived, nothing was kept, and the client was told why", content.Kind)
}

// fetched is one answer from the media endpoint.
type fetched struct {
	status int
	mime   string
	body   []byte
}

// fetchBlob asks the endpoint for a blob the way the client does. An empty token sends
// no header at all, which is the unauthenticated case rather than a wrong one.
func fetchBlob(t *testing.T, url, token string) fetched {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatalf("build the request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	answer, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("fetch %s: %v", url, err)
	}
	defer func() { _ = answer.Body.Close() }()

	read, err := io.ReadAll(answer.Body)
	if err != nil {
		t.Fatalf("read the blob: %v", err)
	}
	return fetched{status: answer.StatusCode, mime: answer.Header.Get("Content-Type"), body: read}
}

// liveWindow is how long a phase waits for a human to send something.
func liveWindow(t *testing.T, fallback time.Duration) time.Duration {
	t.Helper()

	raw := os.Getenv("WAC_LIVE_SECONDS")
	if raw == "" {
		return fallback
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("WAC_LIVE_SECONDS=%q: %v", raw, err)
	}
	return time.Duration(seconds) * time.Second
}

// TestLiveSend sends a text from the paired account, which is the half no fake can
// check: what a unit test can prove is that the request was built and refused
// correctly, and what only WhatsApp can answer is whether it took the message.
//
// Send twice with the same WAC_LIVE_MESSAGE_ID to see the idempotency for yourself. The
// caller names the message, so both sends go out under one id and the recipient's own
// client shows one message. That last part is on the phone, not in here.
func TestLiveSend(t *testing.T) {
	to := os.Getenv("WAC_LIVE_TO")
	if to == "" {
		t.Skip("set WAC_LIVE_TO to the number this account should send to")
	}

	session, _ := liveSession(t)
	events := watch(t, session)

	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	events.awaitState(t, "open", 2*time.Minute)

	messageID := os.Getenv("WAC_LIVE_MESSAGE_ID")
	if messageID == "" {
		messageID = session.current().GenerateMessageID()
	}
	body := map[string]any{
		"message_id": messageID,
		"to":         map[string]any{"kind": "phone", "id": to},
		"content":    map[string]any{"type": "text", "body": "conector nativo, fatia de envio"},
	}
	if quoted := os.Getenv("WAC_LIVE_QUOTE"); quoted != "" {
		// The half no unit test can answer: the quote goes out as a stanza id and a
		// participant, with no copy of the message it answers, because the caller does
		// not send one and this connector keeps no messages. Whether the recipient's
		// client renders it from the id alone is a question for a phone.
		body["quoted"] = map[string]any{"id": quoted, "from_me": false}
		body["content"] = map[string]any{"type": "text", "body": "conector nativo, em resposta a mensagem acima"}
	}
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("build the send: %v", err)
	}

	result, err := session.Execute(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend, Payload: payload,
	})
	if err != nil {
		t.Fatalf("message.send: %v", err)
	}

	var sent struct {
		MessageID string `json:"message_id"`
		Timestamp int64  `json:"timestamp"`
	}
	if err := json.Unmarshal(result, &sent); err != nil {
		t.Fatalf("unmarshal the result: %v", err)
	}
	if sent.MessageID != messageID {
		t.Fatalf("the send came back as %q, and the caller named it %q", sent.MessageID, messageID)
	}
	if sent.Timestamp == 0 {
		t.Fatal("the send came back without the timestamp WhatsApp stamped it with")
	}
	fmt.Fprintf(os.Stderr, "sent %s at %s\n", sent.MessageID, time.UnixMilli(sent.Timestamp))
}

// TestLiveActOnAMessage is M2.5's outbound half against a real recipient: a message
// corrected, one reacted to, and one deleted.
//
// This is the phase the unit tests cannot stand in for, and the reason is the same for
// all three. Each names a message that already exists with a key, and WhatsApp accepts a
// key that resolves to nothing exactly as readily as one that resolves: the send answers
// with a timestamp either way, and nothing afterwards says which happened. A test on this
// side can check that the key was built from the right parts. Only a recipient's client
// can say whether the parts were the right ones.
//
// Two messages are sent rather than one, so everything is on screen at the end. The first
// is corrected and reacted to and stays; the second is deleted. A single message would
// leave the earlier steps invisible behind the deletion.
func TestLiveActOnAMessage(t *testing.T) {
	to := os.Getenv("WAC_LIVE_TO")
	if to == "" {
		t.Skip("set WAC_LIVE_TO to the number this account should send to")
	}

	session, _ := liveSession(t)
	events := watch(t, session)
	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	events.awaitState(t, "open", 2*time.Minute)

	chat := map[string]any{"kind": "phone", "id": to}
	standing := liveSendOne(t, session, to, map[string]any{
		"type": "text", "body": "conector nativo: esta mensagem vai ser corrigida",
	})
	doomed := liveSendOne(t, session, to, map[string]any{
		"type": "text", "body": "conector nativo: esta mensagem vai ser apagada",
	})

	liveActOne(t, session, protocol.CommandMessageEdit, map[string]any{
		"to": chat, "target_id": standing,
		"content": map[string]any{"type": "text", "body": "conector nativo: corrigida"},
	})
	// On the account's own message, which is the only kind of target this phase has: the
	// other branch, a reaction on the contact's message, needs an id from their side and
	// is what WAC_LIVE_REACT_TO is for.
	liveActOne(t, session, protocol.CommandMessageReact, map[string]any{
		"to": chat, "target_id": standing, "target_from_me": true, "emoji": "❤️",
	})
	liveActOne(t, session, protocol.CommandMessageRevoke, map[string]any{
		"to": chat, "target_id": doomed,
	})

	// A reaction on somebody else's message builds a different key -- `from_me` false,
	// and in a direct chat no participant -- and a key that is wrong there fails the same
	// silent way. Set WAC_LIVE_REACT_TO to an id the recipient sent, or to `wait` to have
	// the phase take the next message they send: an id copied by hand is an id nobody
	// copies, and the branch then never runs.
	if theirs := os.Getenv("WAC_LIVE_REACT_TO"); theirs != "" {
		// Reacted in the chat the message arrived in, which is not always the one this
		// phase sends to: the key carries the chat as its `remoteJID`, and a direct chat
		// this account addresses by phone can deliver under a LID chat. Reacting to the
		// phone chat then names a message that chat does not hold. An id given by hand
		// has no event to take a chat from and falls back to the one being sent to,
		// which is the reason `wait` is the better way to run this.
		where := chat
		if theirs == "wait" {
			theirs, where = liveAwaitTheirMessage(t, events, liveWindow(t, 2*time.Minute))
		}
		liveActOne(t, session, protocol.CommandMessageReact, map[string]any{
			"to": where, "target_id": theirs, "emoji": "👍",
		})
	}
	// And taking one off is a reaction with an empty emoji, not a command of its own.
	reaction := "carries the reaction"
	if os.Getenv("WAC_LIVE_UNREACT") != "" {
		liveActOne(t, session, protocol.CommandMessageReact, map[string]any{
			"to": chat, "target_id": standing, "target_from_me": true, "emoji": "",
		})
		reaction = "carries no reaction, the one put on it having been taken off"
	}

	fmt.Fprintf(os.Stderr, "check the recipient: %s reads \"corrigida\", %s, "+
		"and says it was edited; %s is gone\n", standing, reaction, doomed)
}

// liveAwaitTheirMessage is the next message the recipient sends: its id, and the chat it
// arrived in. Both, because the chat is half of the key and is not always the one this
// phase addresses -- see the caller.
//
// Waiting rather than taking an id by hand is what makes this branch get run at all: an
// id somebody has to copy off a phone is an id nobody copies.
func liveAwaitTheirMessage(
	t *testing.T, events *recorder, window time.Duration,
) (id string, chat map[string]any) {
	t.Helper()

	fmt.Fprintf(os.Stderr, "send a text from the recipient's phone now; waiting up to %s\n", window)
	received := events.await(t, protocol.EventMessageReceived, window)
	var body struct {
		Message struct {
			ID     string `json:"id"`
			FromMe bool   `json:"from_me"`
			Chat   struct {
				Kind string `json:"kind"`
				ID   string `json:"id"`
			} `json:"chat"`
		} `json:"message"`
	}
	if err := json.Unmarshal(received.Payload, &body); err != nil {
		t.Fatalf("unmarshal the message: %v", err)
	}
	if body.Message.ID == "" || body.Message.Chat.ID == "" || body.Message.FromMe {
		// An echo of this account's own send would build the other key entirely, which
		// is the one the rest of the phase already covers.
		t.Fatalf("that is not a message from the other side: %s", received.Payload)
	}
	return body.Message.ID, map[string]any{"kind": body.Message.Chat.Kind, "id": body.Message.Chat.ID}
}

// liveActOne runs one of the three commands that act on an existing message and fails on
// anything but a clean answer. What it cannot check is the part that matters, which is
// why it prints and the phase ends with a look at the phone.
func liveActOne(t *testing.T, session *Session, kind protocol.CommandType, payload map[string]any) {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("build the %s: %v", kind, err)
	}
	// The command id is what the stanza id is derived from when the payload names none,
	// so two actions sharing one go out under the same id and the recipient discards the
	// second as a duplicate of the first. That is the mechanism working, and in here it
	// would read as the action silently failing.
	//
	// Taken from the payload rather than from a field picked out of it, because picking
	// has to be redone every time the phase grows an action: the type and the target were
	// enough until a reaction and its removal, which differ only in the emoji. Two
	// identical payloads are the same command and should collide; anything else is a
	// different one.
	result, err := session.Execute(t.Context(), &protocol.Command{
		Type: kind, ID: fmt.Sprintf("live-%s-%x", kind, sha256.Sum256(body)), Payload: body,
	})
	if err != nil {
		t.Fatalf("%s: %v", kind, err)
	}
	fmt.Fprintf(os.Stderr, "%s on %v answered %s\n", kind, payload["target_id"], result)
}

// TestLiveLogout unlinks the device. It is the last phase, and running it means the
// next run starts at TestLivePairWithQR again.
// TestLiveSendMedia is M2.4 against a real recipient: a file fetched over HTTP, uploaded
// to WhatsApp, and rendered in somebody's chat.
//
// A fake upload proves the plumbing and nothing else. What only WhatsApp and a recipient
// client can answer is whether the key this build derives decrypts on the other side,
// whether the mimetype it sends is one the client renders rather than files as an
// attachment, and whether a sticker uploaded under the image key arrives as a sticker.
// Every one of those fails as something the recipient sees and the sender does not.
//
// The file is served from this process, so the fetch is a real HTTP round trip against a
// real address, headers included. Point WAC_LIVE_FILE at a file to send your own.
func TestLiveSendMedia(t *testing.T) {
	to := os.Getenv("WAC_LIVE_TO")
	if to == "" {
		t.Skip("set WAC_LIVE_TO to the number this account should send to")
	}
	const token = "live-check"

	file, name, kind := liveFileToSend(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /outbound/{name}", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			// Asserted by serving a 401: the send then fails with the reason, which is
			// how this phase would report a header that did not travel.
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", liveMime(name))
		w.Header().Set("Content-Length", strconv.Itoa(len(file)))
		_, _ = w.Write(file)
	})
	endpoint := httptest.NewServer(mux)
	t.Cleanup(endpoint.Close)

	session, _ := liveSession(t)
	events := watch(t, session)
	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	events.awaitState(t, "open", 2*time.Minute)

	// A voice note is not something a filename can say: an .ogg is as likely to be music,
	// and what makes one is the caller setting the flag. It is worth its own run because
	// it is the one kind whose type this build fills in on its own.
	if os.Getenv("WAC_LIVE_VOICE") != "" {
		kind = "audio"
	}
	content := map[string]any{
		"type": "media", "kind": kind, "filename": name, "size": len(file),
		"voice_note": os.Getenv("WAC_LIVE_VOICE") != "",
		"ref": map[string]any{
			"kind": "url", "url": endpoint.URL + "/outbound/" + name,
			"headers": map[string]any{"Authorization": "Bearer " + token},
		},
	}
	// Only where WhatsApp has somewhere to put one. A caption on an audio or a sticker is
	// refused on the payload, so a phase that added one to everything would fail before
	// fetching anything and never exercise the file at all.
	if captions[protocol.MediaKind(kind)] {
		content["caption"] = "conector nativo, fatia de saída"
	}
	// WAC_LIVE_DURATION is how long the caller says an audio or a video runs. Worth being
	// able to leave off as well as set: what a recipient does with a length it was not
	// given is a question for a phone.
	// WAC_LIVE_MIME pins what the message says the file is, for telling a type WhatsApp
	// refuses apart from one it merely renders differently.
	if forced := os.Getenv("WAC_LIVE_MIME"); forced != "" {
		content["mime"] = forced
	}
	if raw := os.Getenv("WAC_LIVE_DURATION"); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil || seconds < 0 {
			t.Fatalf("WAC_LIVE_DURATION=%q: want a duration in seconds", raw)
		}
		content["duration"] = seconds
	}
	sent := liveSendOne(t, session, to, content)
	say("sent a %s (%d bytes) as %s -- check the recipient renders it, not just receives it", kind, len(file), sent)
}

// TestLiveSendLocation and TestLiveSendContacts are the two bodies that carry no file.
// They are here for the same reason as the media phase: what they render as on the other
// side is not something a protobuf comparison can answer.
func TestLiveSendLocation(t *testing.T) {
	to := os.Getenv("WAC_LIVE_TO")
	if to == "" {
		t.Skip("set WAC_LIVE_TO to the number this account should send to")
	}

	session, _ := liveSession(t)
	events := watch(t, session)
	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	events.awaitState(t, "open", 2*time.Minute)

	sent := liveSendOne(t, session, to, map[string]any{
		"type": "location", "latitude": -25.4284, "longitude": -49.2733,
		"name": "Praça Tiradentes", "address": "Curitiba, PR",
	})
	say("sent a pin as %s -- check it shows the name and the address, not two numbers", sent)
}

func TestLiveSendContacts(t *testing.T) {
	to := os.Getenv("WAC_LIVE_TO")
	if to == "" {
		t.Skip("set WAC_LIVE_TO to the number this account should send to")
	}

	session, _ := liveSession(t)
	events := watch(t, session)
	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	events.awaitState(t, "open", 2*time.Minute)

	// A name with a semicolon in it on purpose: a vCard reads one as structure, and the
	// escaping this build does is only ever confirmed by a recipient rendering the name
	// in one piece.
	one := liveSendOne(t, session, to, map[string]any{
		"type": "contacts",
		"contacts": []map[string]any{
			{"display_name": "Souza; Ana", "phone": to},
		},
	})
	say("sent one card as %s -- check the name reads as one line and tapping it opens a chat", one)

	several := liveSendOne(t, session, to, map[string]any{
		"type": "contacts",
		"contacts": []map[string]any{
			{"display_name": "Souza; Ana", "phone": to},
			{"display_name": "Bruno", "phone": to},
		},
	})
	say("sent two cards as %s -- check they arrive as a stack rather than as one card", several)
}

// liveSendOne sends one body and answers the id WhatsApp stamped it with.
func liveSendOne(t *testing.T, session *Session, to string, content map[string]any) string {
	t.Helper()

	messageID := session.current().GenerateMessageID()
	payload, err := json.Marshal(map[string]any{
		"message_id": messageID,
		"to":         map[string]any{"kind": "phone", "id": to},
		"content":    content,
	})
	if err != nil {
		t.Fatalf("build the send: %v", err)
	}

	result, err := session.Execute(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend, Payload: payload,
	})
	if err != nil {
		t.Fatalf("message.send: %v", err)
	}
	var sent struct {
		MessageID string `json:"message_id"`
		Timestamp int64  `json:"timestamp"`
	}
	if err := json.Unmarshal(result, &sent); err != nil {
		t.Fatalf("unmarshal the result: %v", err)
	}
	if sent.MessageID != messageID {
		t.Fatalf("the send came back as %q, and the caller named it %q", sent.MessageID, messageID)
	}
	if sent.Timestamp == 0 {
		t.Fatal("the send came back without the timestamp WhatsApp stamped it with")
	}
	return sent.MessageID
}

// liveFileToSend is what the media phase sends: the operator's own file when WAC_LIVE_FILE
// names one, and a small generated JPEG otherwise, so the phase runs with nothing set up.
func liveFileToSend(t *testing.T) (content []byte, name, kind string) {
	t.Helper()

	if path := os.Getenv("WAC_LIVE_FILE"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		name = filepath.Base(path)
		return raw, name, liveKind(name)
	}
	// A one-pixel JPEG, so the phase has something real to upload with nothing prepared.
	// Small on purpose: what is being checked is the round trip, not the transfer.
	raw, err := base64.StdEncoding.DecodeString(
		"/9j/4AAQSkZJRgABAQEAYABgAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkSEw8UHRofHh0a" +
			"HBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/wAALCAABAAEBAREA/8QAFAABAAAAAAAA" +
			"AAAAAAAAAAAACf/EABQQAQAAAAAAAAAAAAAAAAAAAAD/2gAIAQEAAD8AKp//2Q==")
	if err != nil {
		t.Fatalf("decode the built-in file: %v", err)
	}
	return raw, "pixel.jpg", "image"
}

// liveKind guesses the contract's kind from a filename, so an operator pointing
// WAC_LIVE_FILE at a PDF does not also have to say what a PDF is.
func liveKind(name string) string {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".jpg", ".jpeg", ".png":
		return "image"
	case ".mp4", ".mov":
		return "video"
	case ".ogg", ".opus", ".mp3", ".m4a":
		return "audio"
	case ".webp":
		return "sticker"
	default:
		return "document"
	}
}

// liveMime is what the little server labels the file with, so the phase also exercises
// the fallback that reads the mimetype off the response.
func liveMime(name string) string {
	if guessed := mime.TypeByExtension(strings.ToLower(filepath.Ext(name))); guessed != "" {
		return guessed
	}
	return "application/octet-stream"
}

func TestLiveLogout(t *testing.T) {
	if os.Getenv("WAC_LIVE_LOGOUT") != "yes" {
		t.Skip("set WAC_LIVE_LOGOUT=yes to unlink the device")
	}

	session, container := liveSession(t)
	events := watch(t, session)

	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	events.awaitState(t, "open", 2*time.Minute)

	if err := session.Logout(t.Context()); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	events.await(t, protocol.EventSessionLoggedOut, 60*time.Second)

	if _, bound, err := container.JID(t.Context(), liveSID); err != nil || bound {
		t.Fatalf("the mapping survived a logout (bound=%v, err=%v)", bound, err)
	}
}

// liveSession opens the durable store and one session on it, with nowhere to keep a
// file. Every phase but the media ones wants that.
func liveSession(t *testing.T) (*Session, *store.Container) {
	t.Helper()

	return liveSessionWith(t, MediaOptions{})
}

// liveSessionWith is the same, with somewhere to put the file of an inbound message.
func liveSessionWith(t *testing.T, blobs MediaOptions) (*Session, *store.Container) {
	t.Helper()

	path := os.Getenv("WAC_LIVE_DB")
	if path == "" {
		path = filepath.Join(liveDir(t), "live.db")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("prepare %s: %v", filepath.Dir(path), err)
	}

	log := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.TimeOnly}).
		Level(zerolog.DebugLevel).With().Timestamp().Logger()

	container, err := store.Open(t.Context(), "sqlite:"+path, log)
	if err != nil {
		t.Fatalf("open the store at %s: %v", path, err)
	}
	t.Cleanup(func() {
		if err := container.Close(); err != nil {
			t.Errorf("close the store: %v", err)
		}
	})
	t.Logf("store: %s", path)

	waEngine := mustEngine(t, container, Options{DeviceName: "fazer.ai live check", Media: blobs}, log)
	t.Cleanup(func() {
		if err := waEngine.Close(); err != nil {
			t.Errorf("close the engine: %v", err)
		}
	})

	opened, err := waEngine.Open(t.Context(), liveSID)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	session, ok := opened.(*Session)
	if !ok {
		t.Fatalf("the engine handed back a %T", opened)
	}
	return session, container
}

// liveDir is where the store and the rendered codes go. Outside the repo, because a
// pairing is a credential and the working tree is not where credentials live.
func liveDir(t *testing.T) string {
	t.Helper()

	if dir := os.Getenv("WAC_LIVE_OUT"); dir != "" {
		return dir
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("resolve the home directory: %v", err)
	}
	return filepath.Join(home, ".local", "state", "whatsapp-connector-live")
}

// liveDeadline is how long a phase waits on a human. Scanning a code is the only step
// that does, so it is the only one that reads the override.
func liveDeadline(t *testing.T, fallback time.Duration) time.Duration {
	t.Helper()

	raw := os.Getenv("WAC_LIVE_SCAN_SECONDS")
	if raw == "" {
		return fallback
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("WAC_LIVE_SCAN_SECONDS=%q: %v", raw, err)
	}
	return time.Duration(seconds) * time.Second
}

// recorder is the reader of the session's event channel. Every phase needs one, and
// there can only be one: Events is a channel, so a second reader would steal frames
// from the first.
type recorder struct {
	seen  chan engine.Emission
	tally map[protocol.EventType]*atomic.Int64
	dir   string
	codes atomic.Int64
}

func watch(t *testing.T, session *Session) *recorder {
	t.Helper()

	r := &recorder{
		seen:  make(chan engine.Emission, 256),
		tally: make(map[protocol.EventType]*atomic.Int64, 24),
		dir:   liveDir(t),
	}
	go func() {
		for emission := range session.Events() {
			r.record(emission)
			// This harness stands in for the publisher, so it owes the same answer:
			// an inbound message waits here for word that its event landed, and a
			// reader that only drains the channel leaves every one of them stalled
			// until the session gives up and refuses the acknowledgement.
			if emission.Settle != nil {
				emission.Settle(nil)
			}
			select {
			case r.seen <- emission:
			default:
				// A phase that is not reading fast enough must not stall the
				// session's own forwarder, which is what publishes the state.
			}
		}
		close(r.seen)
	}()
	return r
}

// record prints one event and, for a pairing image, writes it somewhere a camera can
// reach. The payload of everything else goes out whole: this is a debugging harness on
// a throwaway account, and eliding the field that explains the failure defeats it.
//
// It writes to stderr rather than through t.Logf because the testing package buffers a
// test's log until the test ends, and a pairing code nobody sees until the deadline has
// passed is a pairing code nobody can scan.
func (r *recorder) record(emission engine.Emission) {
	counter, ok := r.tally[emission.Type]
	if !ok {
		counter = &atomic.Int64{}
		r.tally[emission.Type] = counter
	}
	counter.Add(1)

	if emission.Type == protocol.EventPairingQR {
		path, expires, err := r.writeCode(emission.Payload)
		if err != nil {
			say("could not save the pairing image: %v", err)
			return
		}
		say("SCAN THIS: %s (valid for %s)", path, expires)
		return
	}
	say("%-28s %s", emission.Type, emission.Payload)
}

// say reports one line as it happens, timestamped like the logger beside it.
func say(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "%s EVENT %s\n", time.Now().Format(time.TimeOnly), fmt.Sprintf(format, args...))
}

// writeCode turns the data URL the contract carries back into a file.
func (r *recorder) writeCode(payload json.RawMessage) (string, time.Duration, error) {
	var body struct {
		Image     string `json:"png_data_url"`
		ExpiresIn int64  `json:"expires_in_ms"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return "", 0, err
	}
	const prefix = "data:image/png;base64,"
	if !strings.HasPrefix(body.Image, prefix) {
		return "", 0, fmt.Errorf("the payload carries no png: %.40s", body.Image)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(body.Image, prefix))
	if err != nil {
		return "", 0, err
	}
	if err := os.MkdirAll(r.dir, 0o700); err != nil {
		return "", 0, err
	}
	// The newest code overwrites the last, so whoever is holding the phone is always
	// looking at the one that is still valid; the numbered copy is for the log.
	path := filepath.Join(r.dir, fmt.Sprintf("qr-%02d.png", r.codes.Add(1)))
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return "", 0, err
	}
	current := filepath.Join(r.dir, "qr.png")
	if err := os.WriteFile(current, raw, 0o600); err != nil {
		return "", 0, err
	}
	return current, time.Duration(body.ExpiresIn) * time.Millisecond, nil
}

func (r *recorder) count(eventType protocol.EventType) int64 {
	if counter, ok := r.tally[eventType]; ok {
		return counter.Load()
	}
	return 0
}

// await blocks until one event type arrives, and fails on a pairing that ended badly
// rather than waiting out the deadline on a session that is never going to connect.
func (r *recorder) await(t *testing.T, want protocol.EventType, within time.Duration) engine.Emission {
	t.Helper()

	deadline := time.After(within)
	for {
		select {
		case emission, ok := <-r.seen:
			if !ok {
				t.Fatalf("the session ended before %s arrived", want)
			}
			if emission.Type == want {
				return emission
			}
			if emission.Type == protocol.EventPairingError && want != protocol.EventPairingError {
				t.Fatalf("the pairing failed while waiting for %s: %s", want, emission.Payload)
			}
			if emission.Type == protocol.EventSessionLoggedOut && want != protocol.EventSessionLoggedOut {
				t.Fatalf("the account was logged out while waiting for %s: %s", want, emission.Payload)
			}
		case <-deadline:
			t.Fatalf("%s did not arrive within %s", want, within)
		}
	}
}

// awaitState waits for one value of session.state, ignoring the ones on the way to it.
func (r *recorder) awaitState(t *testing.T, want string, within time.Duration) {
	t.Helper()

	deadline := time.After(within)
	for {
		select {
		case emission, ok := <-r.seen:
			if !ok {
				t.Fatalf("the session ended before it reported %q", want)
			}
			if emission.Type != protocol.EventSessionState {
				continue
			}
			var body struct {
				State string `json:"state"`
			}
			if err := json.Unmarshal(emission.Payload, &body); err != nil {
				t.Fatalf("unmarshal a session state: %v", err)
			}
			if body.State == want {
				return
			}
		case <-deadline:
			t.Fatalf("the session did not report %q within %s", want, within)
		}
	}
}
