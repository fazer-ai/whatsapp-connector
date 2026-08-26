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
//	WAC_LIVE_TO=<number> go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveSend
//	go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveLogout
//
// The store is deliberately not a t.TempDir: resuming is the thing being checked, and
// a pairing that does not outlive the process proves nothing. Point WAC_LIVE_DB
// somewhere durable, or accept the default under the user's state directory.
package whatsmeow

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
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
	window := 2 * time.Minute
	if raw := os.Getenv("WAC_LIVE_SECONDS"); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("WAC_LIVE_SECONDS=%q: %v", raw, err)
		}
		window = time.Duration(seconds) * time.Second
	}

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
		body["content"] = map[string]any{"type": "text", "body": "conector nativo, respondendo a mensagem acima"}
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

// TestLiveLogout unlinks the device. It is the last phase, and running it means the
// next run starts at TestLivePairWithQR again.
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

// liveSession opens the durable store and one session on it.
func liveSession(t *testing.T) (*Session, *store.Container) {
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

	waEngine := New(container, "fazer.ai live check", log)
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
