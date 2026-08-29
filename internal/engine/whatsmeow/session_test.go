package whatsmeow

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/santhosh-tekuri/jsonschema/v6"
	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waAdv"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

func TestQRDataURLIsAnImageTheContractAccepts(t *testing.T) {
	t.Parallel()

	url, err := qrDataURL("2@abc,def,ghi")
	if err != nil {
		t.Fatalf("qrDataURL: %v", err)
	}

	const prefix = "data:image/png;base64,"
	if !strings.HasPrefix(url, prefix) {
		t.Fatalf("the pairing image does not carry the prefix the schema requires: %.40s", url)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(url, prefix))
	if err != nil {
		t.Fatalf("the pairing image is not base64: %v", err)
	}
	if _, err := png.Decode(bytes.NewReader(raw)); err != nil {
		t.Fatalf("the pairing image is not a png: %v", err)
	}
}

func TestConnectRefusesWhatItCannotDo(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		request engine.ConnectRequest
		code    protocol.ErrorCode
	}{
		"an unknown pairing mode": {
			request: engine.ConnectRequest{Pairing: "carrier-pigeon"},
			code:    protocol.ErrorInvalidPayload,
		},
		"code pairing with no number": {
			request: engine.ConnectRequest{Pairing: "code"},
			code:    protocol.ErrorInvalidPayload,
		},
		"resuming a session that never paired": {
			request: engine.ConnectRequest{Pairing: "resume"},
			code:    protocol.ErrorNotPaired,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "")

			err := session.Connect(t.Context(), test.request)
			var coded *protocol.Error
			if !errors.As(err, &coded) {
				t.Fatalf("Connect returned %v, want a protocol error", err)
			}
			if coded.Code != test.code {
				t.Fatalf("Connect answered %q, want %q", coded.Code, test.code)
			}
		})
	}
}

func TestExecuteAnswersTheSessionAndRefusesTheRest(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, "")

	result, err := session.Execute(t.Context(), &protocol.Command{Type: protocol.CommandSessionStatus})
	if err != nil {
		t.Fatalf("session.status: %v", err)
	}
	// The RPC result is a `connection_state`, whose key is `connection`. The event that
	// reports the same change spells it `state`, and answering the RPC with the event's
	// shape leaves the caller without the one field the result requires.
	var status struct {
		Connection string `json:"connection"`
	}
	if err := json.Unmarshal(result, &status); err != nil {
		t.Fatalf("unmarshal the status: %v", err)
	}
	if status.Connection != "close" {
		t.Fatalf("an unconnected session reports %q, want close", status.Connection)
	}

	// A later milestone brings these. Until then a refusal is the honest answer:
	// acknowledging a command this build cannot carry out would lose whatever it was
	// asked to do and report success.
	for _, unsupported := range []protocol.CommandType{
		protocol.CommandMessageMarkUnread, protocol.CommandHistoryRequest,
		protocol.CommandContactCheck, protocol.CommandGroupInfo, protocol.CommandCallReject,
	} {
		if _, err := session.Execute(t.Context(), &protocol.Command{Type: unsupported}); !errors.Is(err, engine.ErrNotSupported) {
			t.Fatalf("%s answered %v, want ErrNotSupported", unsupported, err)
		}
	}

	// And the three that act on an existing message are reached now, which is a
	// different thing from being carried out: an empty payload names no message, so what
	// comes back says the payload is wrong rather than that the command is unknown. A
	// client told `unsupported` stops asking, so the two answers cannot be swapped.
	for _, reached := range []protocol.CommandType{
		protocol.CommandMessageEdit, protocol.CommandMessageRevoke, protocol.CommandMessageReact,
		protocol.CommandMessageMarkRead, protocol.CommandPresenceSet,
		protocol.CommandPresenceSubscribe, protocol.CommandChatPresence,
	} {
		_, err := session.Execute(t.Context(), &protocol.Command{Type: reached, Payload: json.RawMessage(`{}`)})
		if errors.Is(err, engine.ErrNotSupported) {
			t.Fatalf("%s is wired up and still answers ErrNotSupported", reached)
		}
		assertCode(t, err, protocol.ErrorInvalidPayload)
	}
}

func TestStatusCarriesTheAddressOncePaired(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, "5511999990001")

	result, err := session.Execute(t.Context(), &protocol.Command{Type: protocol.CommandSessionStatus})
	if err != nil {
		t.Fatalf("session.status: %v", err)
	}
	var status map[string]any
	if err := json.Unmarshal(result, &status); err != nil {
		t.Fatalf("unmarshal the status: %v", err)
	}
	if status["phone_number"] != "5511999990001" {
		t.Fatalf("the status carries phone_number=%v", status["phone_number"])
	}
	if status["phone"] != nil {
		t.Fatalf("the status carries the event's key too: phone=%v", status["phone"])
	}
}

func TestHandleTranslatesWhatWhatsappReports(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		event any
		phone string
		want  protocol.EventType
		check func(t *testing.T, payload map[string]any)
	}{
		"a connection": {
			event: &waEvents.Connected{},
			want:  protocol.EventSessionState,
			check: func(t *testing.T, payload map[string]any) {
				// whatsmeow emits this once the socket is up and authenticated, which is
				// the whole of what open means.
				if payload["state"] != "open" {
					t.Fatalf("state=%v after a connection", payload["state"])
				}
			},
		},
		"a disconnection while paired": {
			event: &waEvents.Disconnected{},
			phone: "5511999990001",
			want:  protocol.EventSessionState,
			check: func(t *testing.T, payload map[string]any) {
				if payload["state"] != "reconnecting" {
					t.Fatalf("state=%v, want reconnecting", payload["state"])
				}
			},
		},
		// whatsmeow only reconnects a device it has an id for, so a drop before pairing
		// finishes ends that attempt. Calling it reconnecting leaves the dashboard
		// waiting on something nothing is going to do.
		"a disconnection before pairing finished": {
			event: &waEvents.Disconnected{},
			want:  protocol.EventSessionState,
			check: func(t *testing.T, payload map[string]any) {
				if payload["state"] != "close" {
					t.Fatalf("state=%v, want close", payload["state"])
				}
			},
		},
		"another device taking the stream": {
			event: &waEvents.StreamReplaced{},
			want:  protocol.EventSessionStreamReplaced,
		},
		"a temporary ban": {
			event: &waEvents.TemporaryBan{Code: waEvents.TempBanReason(101), Expire: time.Hour},
			want:  protocol.EventSessionTemporaryBan,
			check: func(t *testing.T, payload map[string]any) {
				ban, ok := payload["ban"].(map[string]any)
				if !ok {
					t.Fatalf("the ban is %T, want an object", payload["ban"])
				}
				if ban["kind"] != "temporary" {
					t.Fatalf("kind=%v, want temporary", ban["kind"])
				}
				if _, ok := ban["expires_at"].(float64); !ok {
					t.Fatalf("expires_at is %T, want a timestamp", ban["expires_at"])
				}
			},
		},
		"a build WhatsApp refuses": {
			event: &waEvents.ClientOutdated{},
			want:  protocol.EventSessionClientOutdated,
		},
		"a connect failure": {
			event: &waEvents.ConnectFailure{Reason: waEvents.ConnectFailureServiceUnavailable, Message: "later"},
			want:  protocol.EventSessionConnectFailure,
			check: func(t *testing.T, payload map[string]any) {
				if payload["reason"] == nil || payload["code"] == nil {
					t.Fatalf("the failure carries reason=%v code=%v", payload["reason"], payload["code"])
				}
			},
		},
		"a successful pairing": {
			event: &waEvents.PairSuccess{
				ID:       waTypes.JID{User: "5511999990001", Server: waTypes.DefaultUserServer},
				LID:      waTypes.JID{User: "192676662091991", Server: waTypes.HiddenUserServer},
				Platform: "android",
			},
			want: protocol.EventPairingSuccess,
			check: func(t *testing.T, payload map[string]any) {
				if payload["phone"] != "5511999990001" {
					t.Fatalf("phone=%v", payload["phone"])
				}
				if payload["lid"] != "192676662091991" {
					t.Fatalf("lid=%v", payload["lid"])
				}
			},
		},
		"a failed pairing": {
			event: &waEvents.PairError{Error: errors.New("pair-database: insert into whatsmeow_device failed")},
			want:  protocol.EventPairingError,
			check: func(t *testing.T, payload map[string]any) {
				if payload["reason"] != "pair_error" {
					t.Fatalf("reason=%v", payload["reason"])
				}
				// The library's own words carry SQL and internals that mean nothing to an
				// operator and have no business in a client's UI.
				message, _ := payload["message"].(string)
				if strings.Contains(message, "whatsmeow_device") || strings.Contains(message, "insert into") {
					t.Fatalf("the library's error text reached the wire: %q", message)
				}
				if message == "" {
					t.Fatal("the failure travelled without a message")
				}
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, test.phone)

			session.handle(test.event)
			emission := next(t, session)
			if emission.Type != test.want {
				t.Fatalf("published %q, want %q", emission.Type, test.want)
			}
			if test.check != nil {
				test.check(t, decode(t, emission.Payload))
			}
		})
	}
}

// whatsmeow delivers a terminal pairing outcome to every handler, the QR channel's
// included, so publishing from both paths turns one outcome into two canonical events
// with two sequence numbers.
func TestTerminalPairingOutcomesArePublishedOnce(t *testing.T) {
	t.Parallel()

	for name, event := range map[string]any{
		"an outdated build": any(&waEvents.ClientOutdated{}),
		"a failed pairing":  any(&waEvents.PairError{Error: errors.New("no")}),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "")
			session.startPairing(t.Context(), func() {})

			session.handle(event)

			select {
			case emission := <-session.Events():
				t.Fatalf("the general handler published %q while a pairing was open", emission.Type)
			case <-time.After(200 * time.Millisecond):
			}
		})
	}
}

func TestBeingLoggedOutForgetsTheDevice(t *testing.T) {
	t.Parallel()
	session, container := newTestSession(t, "5511999990001")

	if _, bound, err := container.JID(t.Context(), session.sid); err != nil || !bound {
		t.Fatalf("the session is not bound to begin with (bound=%v, err=%v)", bound, err)
	}

	session.handle(&waEvents.LoggedOut{OnConnect: true})

	emission := next(t, session)
	if emission.Type != protocol.EventSessionLoggedOut {
		t.Fatalf("published %q, want %q", emission.Type, protocol.EventSessionLoggedOut)
	}
	if payload := decode(t, emission.Payload); payload["on_connect"] != true {
		t.Fatalf("on_connect=%v, want true", payload["on_connect"])
	}

	// Credentials WhatsApp has revoked are worse than none: a session that keeps them
	// looks resumable and fails every reconnect.
	if _, bound, err := container.JID(t.Context(), session.sid); err != nil || bound {
		t.Fatalf("the device survived a logout (bound=%v, err=%v)", bound, err)
	}

	// And the client behind it is a new one. whatsmeow marks the device deleted rather
	// than emptying it, so a session left on the old client answers every later call
	// with ErrDeviceDeleted, including the pairing a client does next.
	if phone, _ := session.identity(); phone != "" {
		t.Fatalf("the session still reports %q after being logged out", phone)
	}
	err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"})
	var coded *protocol.Error
	if !errors.As(err, &coded) || coded.Code != protocol.ErrorNotPaired {
		t.Fatalf("resuming a logged-out session answered %v, want not_paired", err)
	}
}

func TestPairingChannelIsTranslated(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		item wm.QRChannelItem
		want protocol.EventType
	}{
		"a code":            {item: wm.QRChannelItem{Event: "code", Code: "2@a,b,c", Timeout: 20 * time.Second}, want: protocol.EventPairingQR},
		"nobody scanned it": {item: wm.QRChannelTimeout, want: protocol.EventPairingError},
		"an outdated build": {item: wm.QRChannelClientOutdated, want: protocol.EventSessionClientOutdated},
		"an error": {
			item: wm.QRChannelItem{Event: "error", Error: errors.New("no")},
			want: protocol.EventPairingError,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "")
			run := session.startPairing(t.Context(), func() {})

			session.publishPairing(run, test.item, true)
			if emission := next(t, session); emission.Type != test.want {
				t.Fatalf("published %q, want %q", emission.Type, test.want)
			}
		})
	}
}

func TestCodePairingDoesNotPublishImages(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, "")
	run := session.startPairing(t.Context(), func() {})

	// An operator who asked for a code is not looking at an image, and publishing both
	// would offer the dashboard two ways to pair one session.
	session.publishPairing(run, wm.QRChannelItem{Event: "code", Code: "2@a,b,c"}, false)
	session.publishPairing(run, wm.QRChannelTimeout, false)

	if emission := next(t, session); emission.Type != protocol.EventPairingError {
		t.Fatalf("published %q first, want the timeout alone", emission.Type)
	}
}

func TestCloseEndsTheEventChannelAndIsSafeTwice(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, "")

	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, open := <-session.Events(); open {
		t.Fatal("Events is still open after Close")
	}
	if err := session.Close(); err != nil {
		t.Fatalf("the second Close: %v", err)
	}
	if !session.Closed() {
		t.Fatal("Closed reports false after Close")
	}
}

func TestEngineKeepsOneSessionPerAccount(t *testing.T) {
	t.Parallel()
	container := openStore(t)
	waEngine := mustEngine(t, container, Options{DeviceName: "fazer.ai test"}, zerolog.Nop())
	t.Cleanup(func() {
		if err := waEngine.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	first, err := waEngine.Open(t.Context(), "sid-1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	again, err := waEngine.Open(t.Context(), "sid-1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if first != again {
		t.Fatal("a second Open built a second client on one account's credentials")
	}

	// A closed session holds a socket that cannot be reopened, so the next Open has to
	// build a new one rather than hand back one that will never connect.
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	replacement, err := waEngine.Open(t.Context(), "sid-1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if replacement == first {
		t.Fatal("Open handed back a closed session")
	}
}

// newTestSession builds a session on a real device store and no socket. A phone number
// makes it a paired one.
// mustEngine builds an engine and fails the test rather than the run when it cannot.
// The only refusal it has is a misconfiguration, which no test here is arranging.
//
//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func mustEngine(t *testing.T, container *store.Container, opts Options, log zerolog.Logger) *Engine {
	t.Helper()

	waEngine, err := New(container, opts, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return waEngine
}

func newTestSession(t *testing.T, phone string) (*Session, *store.Container) {
	t.Helper()

	container := openStore(t)
	sid := "sid-" + t.Name()
	// Fenced the way Open fences it, so a test session refuses a write after it closes for
	// the same reason a real one does.
	fence := &store.Fence{}
	device, err := container.Device(t.Context(), sid, fence)
	if err != nil {
		t.Fatalf("Device: %v", err)
	}
	if phone != "" {
		// A companion device JID, the way WhatsApp issues one.
		jid, err := waTypes.ParseJID(phone + ":12@" + waTypes.DefaultUserServer)
		if err != nil {
			t.Fatalf("ParseJID: %v", err)
		}
		device.ID = &jid
		device.Account = &waAdv.ADVSignedDeviceIdentity{
			Details:             make([]byte, 32),
			AccountSignature:    make([]byte, 64),
			AccountSignatureKey: make([]byte, 32),
			DeviceSignature:     make([]byte, 64),
		}
		// Saved through the device rather than through the container behind it: the save
		// is what installs whatsmeow's own stores, and only the fenced container puts the
		// fence back over them.
		if err := device.Save(t.Context()); err != nil {
			t.Fatalf("Save: %v", err)
		}
		if err := container.Bind(t.Context(), sid, jid); err != nil {
			t.Fatalf("Bind: %v", err)
		}
	}

	session := newSession(sid, wm.NewClient(device, nil), container, fence, MediaOptions{}, zerolog.Nop(), newLibraryLogger(zerolog.Nop(), sid))
	t.Cleanup(func() { _ = session.Close() })
	return session, container
}

func openStore(t *testing.T) *store.Container {
	t.Helper()

	address := "sqlite:" + filepath.Join(t.TempDir(), "wac.db")
	container, err := store.Open(t.Context(), address, zerolog.Nop())
	if err != nil {
		t.Fatalf("Open the store: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Close(); err != nil {
			t.Errorf("Close the store: %v", err)
		}
	})
	return container
}

// next reads the emission the session just published, or fails rather than hanging.
func next(t *testing.T, session *Session) engine.Emission {
	t.Helper()

	select {
	case emission, ok := <-session.Events():
		if !ok {
			t.Fatal("the session published nothing and closed")
		}
		return emission
	case <-time.After(2 * time.Second):
		t.Fatal("the session published nothing")
		return engine.Emission{}
	}
}

func decode(t *testing.T, payload json.RawMessage) map[string]any {
	t.Helper()

	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal the payload: %v", err)
	}
	return decoded
}

// whatsmeow debug-logs the material this connector must never write down: its pairing
// channel logs every raw QR code, and its client logs the nodes it sends and receives.
// The process redactor masks phone-shaped tokens and nothing else, so a deployment set
// to debug for an unrelated reason would ship pairing credentials to wherever its logs
// go.
func TestTheLibraryLoggerDropsItsDebugOutput(t *testing.T) {
	t.Parallel()

	var written bytes.Buffer
	logger := newLibraryLogger(zerolog.New(&written).Level(zerolog.DebugLevel), "sid-1")

	logger.Debugf("Emitting QR code %s", "2@secret,pairing,code")
	logger.Sub("qrchannel").Debugf("Sending node %s", "<iq to=s.whatsapp.net>")
	// Info is where the library announces an authentication and a pairing, by JID.
	logger.Infof("Successfully paired %s", "5511999990001:12@s.whatsapp.net")
	if written.Len() != 0 {
		t.Fatalf("the library logger wrote what it was not meant to: %s", written.String())
	}

	// What an operator needs when a session will not connect still gets through, minus
	// the payload the library carries along with it.
	logger.Warnf("Failed to send node %s", "0aQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	out := written.String()
	if !strings.Contains(out, "Failed to send node") {
		t.Fatalf("the library logger dropped a warning: %s", out)
	}
	if strings.Contains(out, "0aQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=") {
		t.Fatalf("the library logger wrote the payload out: %s", out)
	}
	if !strings.Contains(out, "[redacted]") {
		t.Fatalf("the payload was dropped rather than masked: %s", out)
	}
}

// whatsmeow sets its logged-in flag on authentication and clears it only on a stream
// error, so it stays true through a Disconnect. A session that trusted it would report
// a disconnected account as open and short-circuit every reconnect.
func TestADisconnectedSessionDoesNotReportItselfOpen(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, "5511999990001")

	session.handle(&waEvents.Connected{})
	if emission := next(t, session); emission.Type != protocol.EventSessionState {
		t.Fatalf("published %q on connecting", emission.Type)
	}
	if state := session.state(); state != "open" {
		t.Fatalf("a connected session reports %q", state)
	}

	if err := session.Disconnect(t.Context()); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if state := session.state(); state != "close" {
		t.Fatalf("a disconnected session reports %q, want close", state)
	}
}

// An item rendered after the operator gave up on that attempt lands after the state
// that replaced it, which is how a dashboard ends up showing an expired code over a
// live pairing.
func TestACancelledPairingRunPublishesNothingMore(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, "")

	run := session.startPairing(t.Context(), func() {})
	session.cancelPairing()

	session.publishPairing(run, wm.QRChannelItem{Event: "code", Code: "2@a,b,c"}, true)

	select {
	case emission := <-session.Events():
		t.Fatalf("a cancelled run published %q", emission.Type)
	case <-time.After(200 * time.Millisecond):
	}
}

// whatsmeow suppresses the ordinary Disconnected when another client takes the stream,
// so nothing else would bring the connection down and the session would go on
// reporting itself open over a stream somebody else now holds.
func TestAReplacedStreamStopsReportingOpen(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, "5511999990001")

	session.handle(&waEvents.Connected{})
	next(t, session)
	if state := session.state(); state != "open" {
		t.Fatalf("a connected session reports %q", state)
	}

	session.handle(&waEvents.StreamReplaced{})
	if emission := next(t, session); emission.Type != protocol.EventSessionStreamReplaced {
		t.Fatalf("published %q, want %q", emission.Type, protocol.EventSessionStreamReplaced)
	}
	if state := session.state(); state != "close" {
		t.Fatalf("a replaced stream still reports %q", state)
	}
}

// The dial returns as soon as the socket is up and the pairing conversation runs on
// from there, so a session in the middle of one is connecting. Calling it closed has
// the reply to session.connect overwrite the `connecting` it just published, while the
// operator is looking at a code.
func TestAPairingSessionReportsConnecting(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, "")

	if state := session.state(); state != "close" {
		t.Fatalf("an idle session reports %q", state)
	}
	session.startPairing(t.Context(), func() {})
	if state := session.state(); state != "connecting" {
		t.Fatalf("a session mid-pairing reports %q, want connecting", state)
	}
}

// An acknowledged message nobody published is a message that is gone. Until M2 can put
// one on the stream, refusing the ack is what leaves it on the phone, which is the
// invariant: losing an event costs a redelivery, never a message.
func TestAnEventThisSessionPublishedNothingForIsNotAcknowledged(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, "5511999990001")

	// A message with no chat, no id and no content: the shape whatsmeow delivers when a
	// node arrives without anything this session can address or render.
	if session.handle(&waEvents.Message{}) {
		t.Fatal("the session let WhatsApp mark a message it published nothing for delivered")
	}

	// Everything it does handle is acknowledged as usual.
	if !session.handle(&waEvents.Connected{}) {
		t.Fatal("the session refused an event it publishes")
	}
}

// whatsmeow retries a paired socket on its own, outside this session's dial. A status
// answered from the dial alone would say `close` while the event stream says
// reconnecting, and a resume would start a second dial alongside the retry.
func TestAReconnectingSessionSaysSo(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, "5511999990001")

	session.handle(&waEvents.Connected{})
	next(t, session)

	session.handle(&waEvents.Disconnected{})
	if emission := next(t, session); emission.Type != protocol.EventSessionState {
		t.Fatalf("published %q on a drop", emission.Type)
	}
	if state := session.state(); state != "reconnecting" {
		t.Fatalf("a dropped paired session reports %q, want reconnecting", state)
	}

	session.handle(&waEvents.Connected{})
	next(t, session)
	if state := session.state(); state != "open" {
		t.Fatalf("a recovered session reports %q", state)
	}
}

// Decoding a proxy is not honouring one. Connecting directly for a deployment that
// asked for egress routing puts its own address on the wire, and does it silently.
func TestAProxyIsRefusedRatherThanIgnored(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, "5511999990001")

	err := session.Connect(t.Context(), engine.ConnectRequest{
		Pairing: "resume",
		Proxy:   &engine.ProxyRequest{URL: "socks5://10.0.0.9:1080"},
	})
	var coded *protocol.Error
	if !errors.As(err, &coded) || coded.Code != protocol.ErrorUnsupported {
		t.Fatalf("a connect carrying a proxy answered %v, want unsupported", err)
	}
}

// whatsmeow stops retrying once the connection ends for a reason retrying cannot fix.
// A session left holding the retry flag reports itself reconnecting forever.
func TestATerminalOutcomeEndsTheReconnect(t *testing.T) {
	t.Parallel()

	for name, event := range map[string]any{
		"a temporary ban":            any(&waEvents.TemporaryBan{Expire: time.Hour}),
		"a connect failure":          any(&waEvents.ConnectFailure{Reason: waEvents.ConnectFailureServiceUnavailable}),
		"another device taking over": any(&waEvents.StreamReplaced{}),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")

			session.handle(&waEvents.Connected{})
			next(t, session)
			session.handle(&waEvents.Disconnected{})
			next(t, session)
			if state := session.state(); state != "reconnecting" {
				t.Fatalf("a dropped session reports %q", state)
			}

			session.handle(event)
			next(t, session)
			if state := session.state(); state != "close" {
				t.Fatalf("%s left the session reporting %q", name, state)
			}
		})
	}
}

// whatsmeow's auto-reconnect can already be past its wait when a disconnect lands, and
// it then opens a socket nobody asked for, after the command has answered `close`.
func TestASocketThatComesBackAfterADisconnectIsClosedAgain(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, "5511999990001")

	session.handle(&waEvents.Connected{})
	next(t, session)
	if err := session.Disconnect(t.Context()); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if emission := next(t, session); emission.Type != protocol.EventSessionState {
		t.Fatalf("published %q on a disconnect", emission.Type)
	}

	// The reconnect that was already in flight.
	session.handle(&waEvents.Connected{})

	select {
	case emission := <-session.Events():
		t.Fatalf("an uninvited socket published %q as if the operator had asked", emission.Type)
	case <-time.After(200 * time.Millisecond):
	}
	if state := session.state(); state != "close" {
		t.Fatalf("the session reports %q after a disconnect it performed", state)
	}

	// A connect the operator did ask for clears the mark, so the next connection counts.
	_ = session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"})
	session.handle(&waEvents.Connected{})
	if emission := next(t, session); emission.Type != protocol.EventSessionState {
		t.Fatalf("published %q on a connection the operator asked for", emission.Type)
	}
}

// A reconnect already under way is not a reason to start a second one: dialling
// alongside whatsmeow's retry loses the race about half the time and answers the caller
// with ErrAlreadyConnected for a socket that was recovering perfectly well.
func TestResumeLeavesAReconnectAlone(t *testing.T) {
	t.Parallel()
	session, _ := newTestSession(t, "5511999990001")

	session.handle(&waEvents.Connected{})
	next(t, session)
	session.handle(&waEvents.Disconnected{})
	next(t, session)

	if err := session.Connect(t.Context(), engine.ConnectRequest{Pairing: "resume"}); err != nil {
		t.Fatalf("resuming a reconnecting session: %v", err)
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("a resume during a reconnect published %q", emission.Type)
	case <-time.After(200 * time.Millisecond):
	}
}

// A logout that never left the machine has not revoked anything. whatsmeow answers
// ErrNotConnected before it sends the unlink request, and the device is then exactly as
// good as it was: still paired, still resumable, and still listed on the operator's
// phone. Treating that as "WhatsApp may have accepted it" costs them a fresh pairing
// for a logout that visibly failed, and leaves the device they asked to remove linked.
func TestALogoutThatNeverReachedWhatsappKeepsTheCredentials(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")

	// Not connected, which is where a logout arrives when the socket is already down.
	err := session.Logout(t.Context())
	if err == nil {
		t.Fatal("a logout on a disconnected client reported success")
	}
	if !errors.Is(err, wm.ErrNotConnected) {
		t.Fatalf("Logout failed with %v, want ErrNotConnected", err)
	}

	if session.isStale() {
		t.Fatal("the session was marked stale, so the next connect would delete credentials that still resume")
	}
	if _, bound, err := container.JID(t.Context(), session.sid); err != nil || !bound {
		t.Fatalf("the pairing was forgotten anyway (bound=%v, err=%v)", bound, err)
	}
}

// The other half of the same rule: a failure the server answered with means the unlink
// may well have been carried out, and a session that kept its credentials would report
// itself open over a device WhatsApp has already thrown away.
func TestOnlyAPreSendFailureCountsAsHavingSentNothing(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		err  error
		want bool
	}{
		"the socket was down":        {err: wm.ErrNotConnected, want: true},
		"there was no device at all": {err: fmt.Errorf("wrapped: %w", wm.ErrNotLoggedIn), want: true},
		"the client was nil":         {err: wm.ErrClientIsNil, want: true},
		"the server refused it":      {err: errors.New("error sending logout request: server said no"), want: false},
		"the store could not be emptied": {
			err: fmt.Errorf("error deleting data from store: %w", errors.New("disk full")), want: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := sentNothing(test.err); got != test.want {
				t.Fatalf("sentNothing(%v) = %v, want %v", test.err, got, test.want)
			}
		})
	}
}

// whatsmeow calls its handlers under a lock that RemoveEventHandler also waits for, so
// a handler blocked inside emit can only be released by the channel Close closes.
// Removing the handler first would have the two wait on each other, with the socket
// still open and the lease already gone, which is the one thing the teardown exists to
// prevent.
//
// The library half of that cannot be driven from a test: nothing outside whatsmeow can
// make it dispatch an event. So the removal is a field, and this stands in for the
// library by refusing to return until the publisher it is standing in front of has been
// released. A Close that removes before it closes never gets past it.
func TestCloseReleasesABlockedHandlerBeforeWaitingOnIt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")

	// Nothing reads Events, so the forwarder parks on its first send and the inbox
	// fills behind it, leaving a publisher blocked exactly where a whatsmeow callback
	// would be.
	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		for range inboxDepth + 8 {
			session.emit(protocol.EventSessionState, map[string]any{"state": "open"})
		}
	}()
	waitForBlockedInbox(t, session)

	session.detach = func(*wm.Client, uint32) {
		// What RemoveEventHandler does: wait for the in-flight handler to return.
		select {
		case <-blocked:
		case <-time.After(5 * time.Second):
			t.Error("the handler was still blocked when Close came to remove it")
		}
	}

	closed := make(chan error, 1)
	go func() { closed <- session.Close() }()

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return with a publisher blocked on a full inbox")
	}
}

// waitForBlockedInbox waits until the session cannot take another emission without
// blocking, which is the state the test above needs before it starts.
func waitForBlockedInbox(t *testing.T, session *Session) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(session.inbox) == inboxDepth {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the inbox never filled")
}

// hungUp is what rejects a Connected event whatsmeow had already queued when a manual
// disconnect completed, and a manual disconnect is followed by no Disconnected event to
// undo it. A connect request that is refused before it does anything must therefore
// leave the guard standing: dropping it would let that stale event report a session
// that is down as open, permanently, with nothing arriving later to correct it.
func TestARefusedConnectLeavesTheDisconnectGuardStanding(t *testing.T) {
	t.Parallel()

	for name, request := range map[string]engine.ConnectRequest{
		"a pairing mode nobody knows": {Pairing: "telepathy"},
		"a proxy this build cannot honour": {
			Pairing: "resume", Proxy: &engine.ProxyRequest{URL: "socks5://127.0.0.1:1080"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			if err := session.Disconnect(t.Context()); err != nil {
				t.Fatalf("Disconnect: %v", err)
			}
			if !session.disconnected() {
				t.Fatal("the guard was not raised by the disconnect")
			}

			if err := session.Connect(t.Context(), request); err == nil {
				t.Fatal("the request was accepted")
			}
			if !session.disconnected() {
				t.Fatal("a refused connect dropped the guard, so a queued Connected event can still report this session open")
			}
		})
	}
}

// disconnected reports whether the manual-disconnect guard is still standing.
func (s *Session) disconnected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hungUp
}

// whatsmeow holds its socket lock for the length of a dial and Disconnect waits for the
// same lock, so a disconnect arriving mid-handshake outlives its deadline. The command
// is answered as failed and the socket goes down a moment later regardless: a session
// that recorded nothing would go on reporting itself open over a connection that no
// longer exists, with no close event to correct it.
func TestADisconnectThatOutlivesItsDeadlineIsStillRecorded(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	holding := make(chan struct{})
	session.disconnect = func(*wm.Client) { <-holding }

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := session.Disconnect(ctx); err == nil {
		t.Fatal("a disconnect that could not close the socket reported success")
	}
	if state := session.state(); state != "open" {
		t.Fatalf("state = %q before the socket actually closed, want open", state)
	}

	// And now the socket does close, long after the caller gave up.
	close(holding)

	emission := next(t, session)
	if emission.Type != protocol.EventSessionState {
		t.Fatalf("published %q, want a session state", emission.Type)
	}
	var body struct {
		State  string `json:"state"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(emission.Payload, &body); err != nil {
		t.Fatalf("unmarshal the state: %v", err)
	}
	if body.State != "close" || body.Reason != "disconnect_requested" {
		t.Fatalf("published %+v, want close/disconnect_requested", body)
	}
	if !session.disconnected() {
		t.Fatal("the guard against a queued Connected event was never raised")
	}
}

// A dial the caller stopped waiting for still has to say how it ended, or the client
// sits on the `connecting` published just before it for as long as the session lasts.
func TestADetachedDialReportsOnlyWhatIsWorthReporting(t *testing.T) {
	t.Parallel()

	t.Run("a failure is reported", func(t *testing.T) {
		t.Parallel()
		session, _ := newTestSession(t, "5511999990001")

		reported := make(chan error, 1)
		dialed := make(chan error, 1)
		dialed <- errors.New("the server hung up")
		session.awaitDetachedDial(dialed, func(err error) { reported <- err })

		select {
		case err := <-reported:
			if err == nil {
				t.Fatal("reported a nil failure")
			}
		default:
			t.Fatal("a dial that failed after its command gave up reported nothing")
		}
	})

	t.Run("a success needs nobody", func(t *testing.T) {
		t.Parallel()
		session, _ := newTestSession(t, "5511999990001")

		dialed := make(chan error, 1)
		dialed <- nil
		session.awaitDetachedDial(dialed, func(error) {
			t.Error("a connect that succeeded was reported as a failure; whatsmeow announces it itself")
		})
	})

	t.Run("the session ending is not a failure", func(t *testing.T) {
		t.Parallel()
		session, _ := newTestSession(t, "5511999990001")
		session.cancel()

		dialed := make(chan error, 1)
		dialed <- context.Canceled
		session.awaitDetachedDial(dialed, func(error) {
			t.Error("a dial interrupted by the session ending was published as a connect failure")
		})
	})
}

// whatsmeow dispatches from whichever goroutine produced the event, so a Disconnected
// and the Connected that follows it can be handled at the same time. Which of the two
// lands last is the library's to decide and not knowable here; what must not happen is
// the two crossing, so that `session.status` says one thing while the last event on the
// stream says the other and nothing ever reconciles them.
func TestConcurrentSocketEventsLeaveStateAndStreamAgreeing(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")

	// Read as they arrive, or the inbox fills and the emits stop being the thing under
	// test.
	last := make(chan string, 512)
	go func() {
		for emission := range session.Events() {
			if emission.Type != protocol.EventSessionState {
				continue
			}
			var body struct {
				State string `json:"state"`
			}
			if err := json.Unmarshal(emission.Payload, &body); err != nil {
				return
			}
			last <- body.State
		}
	}()

	const pairs = 64
	var wg sync.WaitGroup
	for range pairs {
		wg.Add(2)
		go func() { defer wg.Done(); session.handle(&waEvents.Connected{}) }()
		go func() { defer wg.Done(); session.handle(&waEvents.Disconnected{}) }()
	}
	wg.Wait()

	// Every handler published exactly one state, and all of them have to be read: the
	// forwarder is still delivering when the last handler returns, and a test that
	// stopped at whatever had arrived would be comparing against the wrong event.
	var reported string
	for range pairs * 2 {
		select {
		case reported = <-last:
		case <-time.After(5 * time.Second):
			t.Fatal("the states published were never all delivered")
		}
	}
	if state := session.state(); state != reported {
		t.Fatalf("session.status says %q while the last event said %q", state, reported)
	}
}

// ConnectContext returns once the socket is up, and the handshake that authenticates
// the session runs on from there. A dial that clears its own flag on the way out leaves
// a window where nothing is dialing, nothing is connected, and the state machine falls
// through to `close` — which is what `session.connect` carries back as its reply for a
// resume that is going perfectly well.
func TestASocketThatIsUpButNotYetAuthenticatedStillReadsAsConnecting(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")

	session.setDialing(true)
	if state := session.state(); state != "connecting" {
		t.Fatalf("state = %q while dialling, want connecting", state)
	}

	// whatsmeow announcing the authenticated session is what ends the dial.
	session.handle(&waEvents.Connected{})
	if state := session.state(); state != "open" {
		t.Fatalf("state = %q after Connected, want open", state)
	}

	// And a connection that goes down ends it too, or a session that never came back
	// would report itself connecting for as long as it lived.
	session.setDialing(true)
	session.offline()
	if state := session.state(); state != "close" {
		t.Fatalf("state = %q after the connection was given up on, want close", state)
	}
}

// whatsmeow closes the socket when a pairing runs out of codes and publishes no
// Disconnected for a device that never paired, so nothing else takes the connection
// state down. The dial flag would stand and `session.status` would answer `connecting`
// for a pairing that ended minutes ago, which is a dashboard spinning on nothing.
func TestAPairingThatEndedTakesTheConnectionWithIt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	session.setDialing(true)
	if state := session.state(); state != "connecting" {
		t.Fatalf("state = %q while pairing, want connecting", state)
	}

	session.publishPairingFailure("timeout", nil)

	if emission := next(t, session); emission.Type != protocol.EventPairingError {
		t.Fatalf("published %q first, want the pairing error", emission.Type)
	}
	emission := next(t, session)
	if emission.Type != protocol.EventSessionState {
		t.Fatalf("published %q, want the state the pairing left behind", emission.Type)
	}
	var body struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(emission.Payload, &body); err != nil {
		t.Fatalf("unmarshal the state: %v", err)
	}
	if body.State != "close" {
		t.Fatalf("published state %q, want close", body.State)
	}
	if state := session.state(); state != "close" {
		t.Fatalf("state = %q after the pairing ended, want close", state)
	}
}

// A fleet member that has handed its sessions on never opens those accounts again, so
// an entry kept until the next Open of the same id is one kept forever: every whatsmeow
// client the instance ever ran, with the device credentials and channels behind it.
func TestAClosedSessionLeavesTheEngineCache(t *testing.T) {
	t.Parallel()
	container := openStore(t)
	waEngine := mustEngine(t, container, Options{DeviceName: "fazer.ai test"}, zerolog.Nop())
	t.Cleanup(func() {
		if err := waEngine.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	for _, sid := range []string{"sid-1", "sid-2", "sid-3"} {
		opened, err := waEngine.Open(t.Context(), sid)
		if err != nil {
			t.Fatalf("Open %s: %v", sid, err)
		}
		// The lease moved, so the layer above closes it and never asks for it again.
		if err := opened.Close(); err != nil {
			t.Fatalf("Close %s: %v", sid, err)
		}
	}

	waEngine.mu.Lock()
	held := len(waEngine.sessions)
	waEngine.mu.Unlock()
	if held != 0 {
		t.Fatalf("the engine still holds %d closed sessions", held)
	}
}

// The same rule must not reach past the session that ended: a closed session and the one
// that replaced it share an id, and the replacement is the one still running.
func TestClosingASessionDoesNotEvictItsReplacement(t *testing.T) {
	t.Parallel()
	container := openStore(t)
	waEngine := mustEngine(t, container, Options{DeviceName: "fazer.ai test"}, zerolog.Nop())
	t.Cleanup(func() {
		if err := waEngine.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	first, err := waEngine.Open(t.Context(), "sid-1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	replacement, err := waEngine.Open(t.Context(), "sid-1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if replacement == first {
		t.Fatal("Open handed back a closed session")
	}

	// Closing the first one a second time must not take the running one with it.
	if err := first.Close(); err != nil {
		t.Fatalf("Close again: %v", err)
	}
	again, err := waEngine.Open(t.Context(), "sid-1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if again != replacement {
		t.Fatal("the engine built a second client over the credentials a live session is using")
	}
}

// whatsmeow strips a phone number down to its digits before it pairs, so a number typed
// with a plus and spaces pairs fine. What it must not do is leave this session reporting
// the number it was handed: `pairing.code` promises digits, and a client that validates
// the contract drops the event over the plus it sent us itself.
func TestAPairedNumberIsReportedAsDigits(t *testing.T) {
	t.Parallel()

	compiler := jsonschema.NewCompiler()
	digits, err := compiler.Compile(
		filepath.Join("..", "..", "..", "contract", "schema", "protocol.schema.json") + "#/definitions/digits",
	)
	if err != nil {
		t.Fatalf("compile the contract's phone number: %v", err)
	}

	for name, test := range map[string]struct{ typed, want string }{
		"already digits":     {"5511999990001", "5511999990001"},
		"an international +": {"+5511999990001", "5511999990001"},
		"spaced and dashed":  {"+55 11 99999-0001", "5511999990001"},
		"in brackets":        {"+55 (11) 99999 0001", "5511999990001"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := digitsOf(test.typed)
			if got != test.want {
				t.Fatalf("%q would be reported as %q, want %q", test.typed, got, test.want)
			}
			if err := digits.Validate(got); err != nil {
				t.Fatalf("%q is not what the contract calls a phone number: %v", got, err)
			}
		})
	}
}

// whatsmeow can have queued a Connected before a disconnect landed, and that handler
// clears the hang-up guard before it records the connection. A hang-up settling in
// between is one whose `close` the handler then paints over with `open`: the socket is
// down, the session says it is up, and no later event corrects it, because whatsmeow
// publishes nothing more for a socket it already closed.
func TestAHangUpThatFinishedIsNeverPaintedOverByALateConnect(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	// Read as they arrive, or the inbox fills and the handlers stop being what is under
	// test.
	go func() {
		//nolint:revive // an empty body is the point: keep the forwarder moving
		for range session.Events() {
		}
	}()

	// The deterministic half: a hang-up has to wait for a socket transition already in
	// progress, the same way every other one does. Nothing outside the session can pin a
	// handler mid-transition, so this is what pins the rule itself.
	session.transition.Lock()
	settled := make(chan struct{})
	go func() { defer close(settled); session.settleHangUp() }()
	select {
	case <-settled:
		t.Fatal("a hang-up settled while another socket transition was in progress")
	case <-time.After(50 * time.Millisecond):
	}
	session.transition.Unlock()
	<-settled

	for round := range 500 {
		// Back to where a hang-up starts: a socket that is up and no guard standing.
		session.setConnected(true)
		session.mu.Lock()
		session.hungUp = false
		session.mu.Unlock()

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); session.handle(&waEvents.Connected{}) }()
		go func() { defer wg.Done(); session.settleHangUp() }()
		wg.Wait()

		if state := session.state(); state != "close" {
			t.Fatalf("round %d: a hang-up that finished left the session reporting %q", round, state)
		}
	}
}

// A dial that never reached WhatsApp produces no whatsmeow event, so a pairing that only
// answered its caller leaves the `connecting` it published standing for good.
func TestAPairingDialThatFailedIsReported(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	run := session.startPairing(t.Context(), func() {})
	session.setDialing(true)

	session.abandonPairing(run, session.current(), "connect_failed", errors.New("no route to host"))

	if emission := next(t, session); emission.Type != protocol.EventPairingError {
		t.Fatalf("published %q, want the pairing failure", emission.Type)
	}
	emission := next(t, session)
	if emission.Type != protocol.EventSessionState {
		t.Fatalf("published %q, want the connection going down with the attempt", emission.Type)
	}
	var body struct {
		State  string `json:"state"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(emission.Payload, &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body.State != "close" {
		t.Fatalf("the attempt ended with the session reporting %q", body.State)
	}
	if state := session.state(); state != "close" {
		t.Fatalf("session.status still answers %q for a pairing that ended", state)
	}
}

// The reader of the pairing channel reports WhatsApp's own outcomes through the same
// gate, so a run that ends here while a timeout is being published must not publish a
// second failure on top of it.
func TestAPairingIsGivenUpOnOnlyOnce(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	run := session.startPairing(t.Context(), func() {})

	session.abandonPairing(run, session.current(), "connect_failed", errors.New("no route to host"))
	session.abandonPairing(run, session.current(), "connect_failed", errors.New("no route to host"))

	if emission := next(t, session); emission.Type != protocol.EventPairingError {
		t.Fatalf("published %q, want the pairing failure", emission.Type)
	}
	if emission := next(t, session); emission.Type != protocol.EventSessionState {
		t.Fatalf("published %q, want the connection going down", emission.Type)
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("the second attempt to give up published %q as well", emission.Type)
	case <-time.After(100 * time.Millisecond):
	}
}

// whatsmeow keeps the device name in a package-level value it reads from the pairing
// handshake, so an engine assigning it is a write with no lock against every other one.
func TestBuildingEnginesAtOnceDoesNotRaceOverTheDeviceName(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Not mustEngine: a Fatalf from a goroutine that is not the test's own
			// ends that goroutine and leaves the test believing it passed.
			waEngine, err := New(container, Options{DeviceName: fmt.Sprintf("fazer.ai test %d", i)}, zerolog.Nop())
			if err != nil {
				t.Errorf("New: %v", err)
				return
			}
			if err := waEngine.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	wg.Wait()
}

// whatsmeow closes its pairing channel from the goroutine that emits codes, and that
// goroutine is started by the first QR event. An attempt whose dial failed before one
// arrived therefore leaves the channel open for good: a reader that only ranges over it
// waits there for the life of the process, holding the session and the client behind it,
// once for every failed attempt.
func TestAPairingReaderStopsWhenTheAttemptIsGivenUpOn(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	pairCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	run := session.startPairing(pairCtx, cancel)

	// Never closed, and never written to: whatsmeow's channel when the dial failed
	// before the first code.
	codes := make(chan wm.QRChannelItem)
	ready := make(chan bool, 1)
	reading := make(chan struct{})
	go func() {
		defer close(reading)
		session.readPairingWith(run, codes, func(arrived bool) { ready <- arrived }, false)
	}()

	session.abandonPairing(run, session.current(), "connect_failed", errors.New("no route to host"))

	select {
	case <-reading:
	case <-time.After(2 * time.Second):
		t.Fatal("the reader is still waiting on a channel whatsmeow is never going to close")
	}
	select {
	case arrived := <-ready:
		if arrived {
			t.Fatal("the reader reported a code that never came")
		}
	default:
		t.Fatal("nothing was told to the caller waiting for the connection")
	}
}

// The same for a session that is shut down while an attempt is open.
func TestAPairingReaderStopsWhenTheSessionCloses(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	pairCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	run := session.startPairing(pairCtx, cancel)

	codes := make(chan wm.QRChannelItem)
	reading := make(chan struct{})
	go func() {
		defer close(reading)
		session.readPairingWith(run, codes, nil, true)
	}()

	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-reading:
	case <-time.After(2 * time.Second):
		t.Fatal("a closed session left a pairing reader waiting for good")
	}
}

// WhatsApp revokes the device before anything local runs, so from the moment Logout
// returns the account is unlinked whatever this process manages to do next. Holding the
// news behind a database round trip means a store that stopped answering leaves the
// operator looking at a session that was logged out minutes ago, over credentials
// WhatsApp threw away.
func TestALogoutWhatsappAcceptedIsPublishedEvenWhenTheCleanupFails(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	session.logout = func(context.Context, *wm.Client) error { return nil }
	// The store stops answering the moment after WhatsApp accepted the logout.
	if err := container.Close(); err != nil {
		t.Fatalf("Close the store: %v", err)
	}

	if err := session.Logout(t.Context()); err == nil {
		t.Fatal("a cleanup that failed was reported as a logout that went through")
	}

	emission := next(t, session)
	if emission.Type != protocol.EventSessionLoggedOut {
		t.Fatalf("published %q, want the account being logged out", emission.Type)
	}
	if state := session.state(); state == "open" {
		t.Fatal("the session still reports itself open over credentials WhatsApp revoked")
	}
}

// And the ordinary logout still says so once, in front of the rebuild.
func TestALogoutThatWentThroughIsPublishedBeforeTheRebuild(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.logout = func(context.Context, *wm.Client) error { return nil }

	if err := session.Logout(t.Context()); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if emission := next(t, session); emission.Type != protocol.EventSessionLoggedOut {
		t.Fatalf("published %q, want the account being logged out", emission.Type)
	}
}

// whatsmeow unlinks the device, disconnects, and only then deletes the local one, so a
// logout that fails can be the deletion alone: WhatsApp has already revoked the device.
// A command that answers with an error and says nothing else leaves the client treating
// an account it no longer has as paired.
func TestALogoutThatFailedAfterTheRequestWentOutIsStillReported(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.logout = func(context.Context, *wm.Client) error {
		return errors.New("error deleting data from store: database is closed")
	}

	if err := session.Logout(t.Context()); err == nil {
		t.Fatal("a logout that failed was reported as one that went through")
	}
	if emission := next(t, session); emission.Type != protocol.EventSessionLoggedOut {
		t.Fatalf("published %q, want the account being logged out", emission.Type)
	}
}

// And a logout that never left this process says nothing: the device is untouched on
// both sides, and telling the operator it is gone costs them a pairing for a command
// that visibly failed.
func TestALogoutThatNeverLeftSaysNothing(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.logout = func(context.Context, *wm.Client) error { return wm.ErrNotLoggedIn }

	if err := session.Logout(t.Context()); err == nil {
		t.Fatal("a logout that never left was reported as one that went through")
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("published %q for a logout that never reached WhatsApp", emission.Type)
	case <-time.After(100 * time.Millisecond):
	}
}

// Authentication can have queued a Connected before the unlink landed. Left ungated, that
// handler sets the state back to connected and publishes `open` on top of
// session.logged_out, over credentials the account has revoked.
func TestALateConnectDoesNotReviveALoggedOutSession(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.logout = func(context.Context, *wm.Client) error { return nil }
	session.setConnected(true)

	if err := session.Logout(t.Context()); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if emission := next(t, session); emission.Type != protocol.EventSessionLoggedOut {
		t.Fatalf("published %q, want the account being logged out", emission.Type)
	}

	session.handle(&waEvents.Connected{})

	if state := session.state(); state == "open" {
		t.Fatal("a connection queued behind the unlink put a logged-out session back up")
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("published %q after the account was logged out", emission.Type)
	case <-time.After(100 * time.Millisecond):
	}
}

// whatsmeow dispatches from whichever goroutine produced the event, so authentication
// finishing can land after the thing that ended the connection. A terminal transition
// that does not refuse it is one the handler paints over: `open` on top of the event
// that just said the socket is gone, with nothing arriving later to correct it.
func TestATerminalTransitionRefusesTheConnectQueuedBehindIt(t *testing.T) {
	t.Parallel()

	for name, end := range map[string]func(*Session){
		"a stream somebody else took": func(session *Session) {
			session.handle(&waEvents.StreamReplaced{})
		},
		"a pairing that failed": func(session *Session) {
			session.publishPairingFailure("timeout", nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)

			end(session)
			drain(t, session)

			session.handle(&waEvents.Connected{})

			if state := session.state(); state == "open" {
				t.Fatal("a connection queued behind the end of the socket put the session back up")
			}
			select {
			case emission := <-session.Events():
				t.Fatalf("published %q after the socket was gone", emission.Type)
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}

// drain reads whatever a transition published, so what follows it is what a test is
// looking at.
func drain(t *testing.T, session *Session) {
	t.Helper()
	for {
		select {
		case _, open := <-session.Events():
			if !open {
				return
			}
		case <-time.After(100 * time.Millisecond):
			return
		}
	}
}

// A logout that never left this process is the commonest way to reach a session
// whatsmeow is already reconnecting. Treating it as a socket this session is done with
// would have the reconnect closed the moment it lands, for a command that failed and
// kept its credentials.
func TestALogoutThatNeverLeftLeavesTheReconnectAlone(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.logout = func(context.Context, *wm.Client) error { return wm.ErrNotConnected }

	if err := session.Logout(t.Context()); err == nil {
		t.Fatal("a logout that never left was reported as one that went through")
	}

	// whatsmeow's own reconnect finishes.
	session.handle(&waEvents.Connected{})

	if state := session.state(); state != "open" {
		t.Fatalf("the session reports %q after a reconnect it should have kept", state)
	}
	if emission := next(t, session); emission.Type != protocol.EventSessionState {
		t.Fatalf("published %q, want the session coming back up", emission.Type)
	}
}

// A terminal outcome takes the connection down and raises the guard that refuses a late
// connect. Retiring the run it belongs to has to happen first: a replacement attempt
// starting in between would have this one close the socket it just opened, and the
// operator's retry would fail for reasons of its own.
func TestAPairingIsRetiredBeforeItsOutcomeIsPublished(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	run := session.startPairing(t.Context(), func() {})

	// publishPairingFailure takes this to change the socket's state, so holding it pins
	// the publication where a replacement attempt would start.
	session.transition.Lock()
	published := make(chan struct{})
	go func() {
		defer close(published)
		session.publishPairing(run, wm.QRChannelTimeout, true)
	}()

	// The pairing error goes out before the state change, so waiting for it is what puts
	// the publication inside the window under test.
	if emission := next(t, session); emission.Type != protocol.EventPairingError {
		t.Fatalf("published %q, want the pairing failure", emission.Type)
	}
	if session.isCurrentPairing(run) {
		t.Fatal("a run publishing its terminal outcome is still the one a replacement would replace")
	}

	session.transition.Unlock()
	<-published
}

// whatsmeow publishes these three only from the branch that told the socket to stay down,
// so nothing is going to reconnect and nothing else is going to take the state with it. A
// Connected the socket produced on its way out would otherwise put the session back up
// over a connection whatsmeow will not make again, and a resume would then find nothing to
// do.
func TestAnEndedConnectionRefusesTheConnectQueuedBehindIt(t *testing.T) {
	t.Parallel()

	for name, arrive := range map[string]func(*Session){
		"a temporary ban": func(session *Session) {
			session.handle(&waEvents.TemporaryBan{Code: waEvents.TempBanReason(101)})
		},
		"a client WhatsApp will not talk to": func(session *Session) {
			session.handle(&waEvents.ClientOutdated{})
		},
		"a connection WhatsApp refused": func(session *Session) {
			session.handle(&waEvents.ConnectFailure{Reason: waEvents.ConnectFailureReason(400)})
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)

			arrive(session)
			drain(t, session)

			session.handle(&waEvents.Connected{})

			if state := session.state(); state == "open" {
				t.Fatal("a connection queued behind the end of the socket put the session back up")
			}
			select {
			case emission := <-session.Events():
				t.Fatalf("published %q for a socket whatsmeow will not make again", emission.Type)
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}

// The contract carries a standalone `pairing.request_code`, and it asks for the same
// thing a connect with `pairing: "code"` asks for. Refusing it as not supported by a
// build that advertises code pairing leaves a client holding a command the contract gave
// it and nothing to do with it.
func TestARequestForACodeReachesTheCodePairing(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")

	// Without a number, which is where the code flow stops before it touches the
	// network — and the message it stops with is the one only that flow produces, so it
	// says which path the command took and not merely that it was not refused outright.
	_, err := session.Execute(t.Context(), &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandPairingRequestCode,
		SID: "s1", TS: 1787000000000, Payload: json.RawMessage(`{"phone":"+++"}`),
	})

	if errors.Is(err, engine.ErrNotSupported) {
		t.Fatal("a command the contract carries was refused as one this connector does not know")
	}
	var coded *protocol.Error
	if !errors.As(err, &coded) || coded.Code != protocol.ErrorInvalidPayload {
		t.Fatalf("Execute answered %v, want invalid_payload from the code pairing", err)
	}
	if !strings.Contains(coded.Message, "code pairing") {
		t.Fatalf("the refusal reads %q, which is not the code flow answering", coded.Message)
	}
}

// A connect option is a thing the client asked the connector to do, and a build that does
// not do it must say so rather than answer `open`. The client is then waiting for a call
// to be refused, or for a backlog to arrive, with nothing on the stream to tell it that
// neither was ever going to happen — which is the same silence the proxy check exists to
// break. The canonical connect fixture sends both fields, so this is what a client really
// puts on the wire and not a shape invented for the test.
func TestConnectRefusesTheOptionsThisBuildDoesNotCarryOut(t *testing.T) {
	t.Parallel()

	for name, payload := range map[string]string{
		"auto-rejecting calls": `{"pairing":"resume","calls":{"auto_reject":true}}`,
		"importing history":    `{"pairing":"resume","history_sync":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")

			var request engine.ConnectRequest
			if err := json.Unmarshal([]byte(payload), &request); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}

			err := session.Connect(t.Context(), request)
			var coded *protocol.Error
			if !errors.As(err, &coded) || coded.Code != protocol.ErrorUnsupported {
				t.Fatalf("Connect answered %v, want unsupported", err)
			}
		})
	}
}

// A manual disconnect is followed by no Disconnected, so the guard that refuses the
// Connected whatsmeow had already queued has to come off for the next connect — and the
// state that decides whether that connect dials has to be read in the same breath.
// Read afterwards, the queued Connected lands in between: the guard is down, so it is
// announced rather than refused, and the resume is told the session is already open over
// a socket that is down for good, with nothing arriving later to say otherwise.
func TestDroppingTheHangUpGuardAndReadingTheStateIsOneStep(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.settleHangUp()
	if emission := next(t, session); emission.Type != protocol.EventSessionState {
		t.Fatalf("published %q, want the socket coming down", emission.Type)
	}

	// The Connected handler takes this for the length of the transition it announces, so
	// holding it here is the queued event caught mid-flight.
	session.transition.Lock()
	standing := make(chan string, 1)
	go func() { standing <- session.dropHangUp() }()

	select {
	case state := <-standing:
		t.Fatalf("the guard came down and answered %q without waiting for the transition in flight", state)
	case <-time.After(100 * time.Millisecond):
	}
	session.transition.Unlock()

	if state := <-standing; state != "close" {
		t.Fatalf("the connect was handed %q for a socket the operator closed, so it will not dial", state)
	}
}

// And the guard still does its job on the way through: a Connected the library had queued
// before the disconnect completed is answered by closing that socket, not by announcing
// it.
func TestAConnectQueuedBehindADisconnectIsStillRefused(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.settleHangUp()
	drain(t, session)

	session.handle(&waEvents.Connected{})
	if state := session.dropHangUp(); state != "close" {
		t.Fatalf("the connect was handed %q over a socket that is down", state)
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("published %q for a socket the operator had already closed", emission.Type)
	case <-time.After(100 * time.Millisecond):
	}
}

// whatsmeow dispatches Disconnected from a goroutine of its own, so a remote drop that
// landed just before a disconnect completed is handled just after it. Taking it at face
// value publishes `reconnecting` on top of the `close` that settled, for a socket
// whatsmeow was told to stay off and will not dial again — and the flag outlives the
// event, because the Connected that follows is answered by closing the socket rather than
// by clearing it. resume then reads the session as one whatsmeow is already recovering
// and returns without dialling, which is a session that never comes back.
func TestADropFromASocketAlreadyHungUpDoesNotAnnounceARetry(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	session.settleHangUp()
	if emission := next(t, session); emission.Type != protocol.EventSessionState {
		t.Fatalf("published %q, want the socket coming down", emission.Type)
	}

	// The remote drop whatsmeow had already queued.
	session.handle(&waEvents.Disconnected{})

	if state := session.state(); state != "close" {
		t.Fatalf("the session reports %q after a drop it had already closed over, want close", state)
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("published %q on top of the close the operator asked for", emission.Type)
	case <-time.After(100 * time.Millisecond):
	}

	// And the Connected that follows does not clear it either: that handler answers an
	// uninvited socket by closing it. So the state resume reads has to be right here and
	// not a moment later, because nothing after this corrects it.
	session.handle(&waEvents.Connected{})
	drain(t, session)
	if state := session.state(); state != "close" {
		t.Fatalf("the session reports %q, which resume takes as a recovery to leave alone", state)
	}
}

// A logout attempted while whatsmeow is reconnecting comes back before anything is sent,
// and the reconnect is still going. Calling the session offline there has `session.status`
// answer `close` for a connection that is coming back, and a resume start a second dial
// alongside the library's own.
func TestALogoutThatNeverLeftLeavesTheStateAsItWas(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.logout = func(context.Context, *wm.Client) error { return wm.ErrNotConnected }
	session.setConnected(false)
	session.setReconnecting(true)

	if err := session.Logout(t.Context()); err == nil {
		t.Fatal("a logout that never left was reported as one that went through")
	}
	if state := session.state(); state != "reconnecting" {
		t.Fatalf("the session reports %q for a reconnect the library is still running", state)
	}
}

// The cleanup and the rebuild both run before the event that tells a client WhatsApp
// revoked the account, and neither may take the session's whole lifetime: a database that
// stalls would hold the news for as long as the process runs.
func TestAnExternalLogoutIsPublishedEvenWhenTheStoreStalls(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	session.storeLimit = 100 * time.Millisecond

	// The store keeps one connection, so holding it is what makes every query after it
	// wait: a database that stalls rather than one that answers with an error.
	held, err := container.DB().Conn(t.Context())
	if err != nil {
		t.Fatalf("take the store's connection: %v", err)
	}
	t.Cleanup(func() { _ = held.Close() })

	before := time.Now()
	session.handle(&waEvents.LoggedOut{OnConnect: false, Reason: waEvents.ConnectFailureLoggedOut})

	if emission := next(t, session); emission.Type != protocol.EventSessionLoggedOut {
		t.Fatalf("published %q, want the account being logged out", emission.Type)
	}
	// The bound is what makes the news arrive at all: on the session's own lifetime, a
	// store that never answers holds it for as long as the process runs.
	if spent := time.Since(before); spent > 20*session.storeLimit {
		t.Fatalf("the account being logged out took %s to reach the client, on a store bound of %s",
			spent, session.storeLimit)
	}
}

// WhatsApp can revoke a device while authentication is still finishing, and whatsmeow
// expects that disconnect and publishes nothing more for it. A Connected it had already
// queued would then set the state back to connected and publish `open` after
// session.logged_out, over an account that is gone, with nothing arriving later to
// correct it.
func TestALateConnectDoesNotReviveAnAccountWhatsappRevoked(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.storeLimit = 100 * time.Millisecond
	session.setConnected(true)

	session.handle(&waEvents.LoggedOut{OnConnect: false, Reason: waEvents.ConnectFailureLoggedOut})
	if emission := next(t, session); emission.Type != protocol.EventSessionLoggedOut {
		t.Fatalf("published %q, want the account being logged out", emission.Type)
	}

	session.handle(&waEvents.Connected{})

	if state := session.state(); state == "open" {
		t.Fatal("a connection queued behind the revocation put the session back up")
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("published %q for an account WhatsApp has revoked", emission.Type)
	case <-time.After(100 * time.Millisecond):
	}
}

// A connect drops the guard on the way in, because a manual disconnect is followed by no
// event that would undo it. A request refused before anything is opened has to put it
// back: otherwise a Connected the library queued before that disconnect reports a socket
// that is down as open, and nothing arrives later to correct it.
func TestARefusedConnectPutsTheDisconnectGuardBack(t *testing.T) {
	t.Parallel()

	for name, request := range map[string]engine.ConnectRequest{
		"resuming a session that never paired": {Pairing: "resume"},
		"code pairing with no number":          {Pairing: "code"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "")
			session.setConnected(true)

			// The operator disconnects, and whatsmeow has a Connected already on its way.
			session.settleHangUp()
			drain(t, session)

			if err := session.Connect(t.Context(), request); err == nil {
				t.Fatal("the request was accepted, so there is no guard to put back")
			}

			session.handle(&waEvents.Connected{})

			if state := session.state(); state == "open" {
				t.Fatal("a connection queued before the disconnect put the session back up")
			}
			select {
			case emission := <-session.Events():
				t.Fatalf("published %q for a socket the operator had disconnected", emission.Type)
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}

// WhatsApp routes some accounts through a passkey handoff: it asks the operator's browser
// to sign a WebAuthn challenge, and the pairing carries on once it answers. Treating
// those items as a terminal outcome is a pairing that dies on every one of those accounts,
// under a reason that names none of it.
func TestAPasskeyHandoffIsRelayedInsteadOfEndingThePairing(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	run := session.startPairing(t.Context(), func() {})

	challenge := &waTypes.WebAuthnPublicKey{
		Challenge: []byte("a challenge"), Timeout: 60000, RelyingPartID: "web.whatsapp.com",
		UserVerification: "required",
	}
	session.publishPairing(run, wm.QRChannelItem{
		Event: wm.QRChannelEventPasskeyRequest, PasskeyRequest: &waEvents.PairPasskeyRequest{PublicKey: challenge},
	}, true)

	emission := next(t, session)
	if emission.Type != protocol.EventPairingPasskeyRequest {
		t.Fatalf("published %q, want the passkey challenge", emission.Type)
	}
	var request struct {
		RequestID string                     `json:"request_id"`
		PublicKey *waTypes.WebAuthnPublicKey `json:"public_key"`
	}
	if err := json.Unmarshal(emission.Payload, &request); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if request.RequestID != run.id {
		t.Fatalf("the challenge names %q, want the attempt's own %q", request.RequestID, run.id)
	}
	if request.PublicKey == nil || request.PublicKey.RelyingPartID != challenge.RelyingPartID {
		t.Fatalf("the challenge did not survive the trip: %+v", request.PublicKey)
	}
	if !session.isCurrentPairing(run) {
		t.Fatal("the attempt was retired over an item that is progress, not an outcome")
	}

	// And the code the operator checks against their phone is progress too.
	session.publishPairing(run, wm.QRChannelItem{
		Event:               wm.QRChannelEventPasskeyResponse,
		PasskeyConfirmation: &waEvents.PairPasskeyConfirmation{Code: "ABCD-1234"},
	}, true)

	emission = next(t, session)
	if emission.Type != protocol.EventPairingPasskeyConfirmation {
		t.Fatalf("published %q, want the passkey confirmation code", emission.Type)
	}
	if !session.isCurrentPairing(run) {
		t.Fatal("the attempt was retired over the confirmation code")
	}
}

// An answer naming an attempt this session has moved on from is refused rather than sent:
// WhatsApp would take it as the current one's, and the operator would be watching a
// pairing that fails for a reason belonging to the one before it.
func TestAPasskeyAnswerForAnotherAttemptIsRefused(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	stale := session.startPairing(t.Context(), func() {})
	session.startPairing(t.Context(), func() {})

	for name, command := range map[string]*protocol.Command{
		"a credential": {
			V: protocol.Version, ID: "c1", Type: protocol.CommandPairingPasskeyResponse,
			SID: "s1", TS: 1787000000000,
			Payload: json.RawMessage(`{"request_id":"` + stale.id + `","credential":{"id":"x","type":"public-key"}}`),
		},
		"a confirmation": {
			V: protocol.Version, ID: "c2", Type: protocol.CommandPairingPasskeyConfirm,
			SID: "s1", TS: 1787000000000,
			Payload: json.RawMessage(`{"request_id":"` + stale.id + `","confirmed":true}`),
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := session.Execute(t.Context(), command)
			var coded *protocol.Error
			if !errors.As(err, &coded) {
				t.Fatalf("Execute returned %v, want a protocol error", err)
			}
		})
	}
}

// An outdated build ends a pairing without a Disconnected behind it: whatsmeow tells the
// socket to stay down and publishes nothing more. The state has to come down with it, and
// it does because the general handler goes offline before it stands aside for the QR
// reader to publish the event. Pinning that here because the split reads as if the reader
// owned both halves: leave the state to it and `session.status` answers `connecting` for a
// connection nothing is going to make.
func TestAnOutdatedBuildDuringPairingLeavesTheSessionClosed(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	run := session.startPairing(t.Context(), func() {})
	session.setDialing(true)

	// The order whatsmeow dispatches in: our handler is registered when the client is
	// built, before GetQRChannel adds the reader's.
	session.handle(&waEvents.ClientOutdated{})
	session.publishPairing(run, wm.QRChannelClientOutdated, true)
	// whatsmeow closes the pairing channel behind a terminal item, which is what retires
	// the run in the reader.
	session.endPairing(run)

	if emission := next(t, session); emission.Type != protocol.EventSessionClientOutdated {
		t.Fatalf("published %q, want the outdated build", emission.Type)
	}
	if state := session.state(); state != "close" {
		t.Fatalf("the session reports %q after an outdated build ended its pairing, want close", state)
	}
}

// The contract requires `confirmed`, and the reason it does is that its two values mean
// opposite things to the operator: one carries the pairing on, the other ends it. Read
// into a plain bool an absent flag becomes the second one, so a client that sends a
// truncated payload ends the attempt and is told it succeeded, which is exactly what it
// is told when the operator genuinely refused.
func TestAConfirmationThatSaysNothingIsRefusedRatherThanReadAsNo(t *testing.T) {
	t.Parallel()

	for name, payload := range map[string]string{
		"an omitted flag": `{"request_id":%q}`,
		"a null flag":     `{"request_id":%q,"confirmed":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "")
			run := session.startPairing(t.Context(), func() {})

			_, err := session.Execute(t.Context(), &protocol.Command{
				V: protocol.Version, ID: "c1", Type: protocol.CommandPairingPasskeyConfirm,
				SID: "s1", TS: 1787000000000,
				Payload: json.RawMessage(fmt.Sprintf(payload, run.id)),
			})

			var coded *protocol.Error
			if !errors.As(err, &coded) || coded.Code != protocol.ErrorInvalidPayload {
				t.Fatalf("Execute answered %v, want invalid_payload", err)
			}
			// And the attempt it named is untouched: the operator's browser can still
			// answer once it sends a payload that says something.
			if !session.isCurrentPairing(run) {
				t.Fatal("a payload that said nothing ended the pairing the operator is watching")
			}
			select {
			case emission := <-session.Events():
				t.Fatalf("published %q for a confirmation that carried no answer", emission.Type)
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}

// Some terminal outcomes leave the socket up. A code scanned on a phone without
// multidevice is the one that matters: whatsmeow will not open a second pairing channel
// on a live socket, so the operator's corrected attempt is refused until WhatsApp's own
// codes run out, for a reason that has nothing to do with it.
func TestAPairingThatEndedPutsTheSocketBack(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	cancelled := make(chan struct{})
	run := session.startPairing(t.Context(), func() { close(cancelled) })

	session.publishPairing(run, wm.QRChannelScannedWithoutMultidevice, true)
	drain(t, session)

	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("the attempt kept its pairing channel open, so a corrected one cannot start")
	}
	if !session.isStale() {
		t.Fatal("the client was left for the next attempt to inherit the finished channel")
	}
}

// whatsmeow holds its socket lock for the length of a dial and Disconnect waits for the
// same lock, so a disconnect can outlive the command that asked for it: that command is
// answered with a failure and the socket goes down a moment later regardless. A connect
// answered `open` in between is one the older disconnect then closes underneath, which is
// the two commands taking effect in the opposite order to the one they were sent in.
func TestAConnectWaitsForADisconnectThatOutlivedItsCommand(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })
	session.disconnect = func(*wm.Client) { <-blocked }
	session.setConnected(true)

	brief, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := session.Disconnect(brief); err == nil {
		t.Fatal("a disconnect still holding the socket was reported as done")
	}

	// The socket is still up, and the disconnect that will close it has not landed.
	next, cancelNext := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancelNext()
	if err := session.Connect(next, engine.ConnectRequest{Pairing: "resume"}); err == nil {
		t.Fatal("a connect was answered while the disconnect before it was still going")
	}
}

// And once it lands, the next connect goes through rather than waiting for good.
func TestAConnectGoesAheadOnceTheDisconnectLanded(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	release := make(chan struct{})
	session.disconnect = func(*wm.Client) { <-release }
	session.setConnected(true)

	brief, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := session.Disconnect(brief); err == nil {
		t.Fatal("a disconnect still holding the socket was reported as done")
	}
	close(release)

	waited := make(chan error, 1)
	go func() {
		waited <- session.awaitHangUp(t.Context())
	}()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("waiting for a disconnect that landed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a disconnect that finished is still being waited on")
	}
}

// A count alone starts over whenever a session is rebuilt — a restart, a lease moving —
// so the first conversation of the new one would answer to the name the last one used. An
// answer still in flight from before would then be sent to WhatsApp against a challenge it
// was never meant for, and end the attempt the operator is watching.
func TestAPairingNameIsNotReusedByASessionRebuilt(t *testing.T) {
	t.Parallel()

	names := make(map[string]struct{})
	for range 8 {
		// A rebuilt session for the same account, which is what a restart or a lease
		// moving leaves behind.
		session, _ := newTestSession(t, "")
		run := session.startPairing(t.Context(), func() {})
		if _, seen := names[run.id]; seen {
			t.Fatalf("a rebuilt session answers to %q, the name one before it used", run.id)
		}
		names[run.id] = struct{}{}
	}
}

// And the confirmation carries the name the client answers with, or there is no way to
// address the reply.
func TestThePasskeyConfirmationCarriesTheNameToAnswerWith(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	run := session.startPairing(t.Context(), func() {})

	session.publishPairing(run, wm.QRChannelItem{
		Event:               wm.QRChannelEventPasskeyResponse,
		PasskeyConfirmation: &waEvents.PairPasskeyConfirmation{Code: "4821"},
	}, true)

	emission := next(t, session)
	var body struct {
		RequestID string `json:"request_id"`
		Code      string `json:"code"`
	}
	if err := json.Unmarshal(emission.Payload, &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body.RequestID != run.id {
		t.Fatalf("the confirmation names %q, want the attempt's own %q", body.RequestID, run.id)
	}
	validateAgainstContract(t, "event_pairing_passkey_confirmation", emission.Payload)
}

// validateAgainstContract checks a payload against the definition the contract names, and
// then that the contract names every field it carries.
//
// The second half is the one that matters here: the schema accepts extra properties, so a
// field the connector publishes and the schema does not declare validates perfectly and is
// invisible to a client generated from the contract — which for a `request_id` means no way
// to address the reply it exists to be answered with.
func validateAgainstContract(t *testing.T, definition string, payload json.RawMessage) {
	t.Helper()

	path := filepath.Join("..", "..", "..", "contract", "schema", "protocol.schema.json")
	compiler := jsonschema.NewCompiler()
	schema, err := compiler.Compile(path + "#/definitions/" + definition)
	if err != nil {
		t.Fatalf("compile %s: %v", definition, err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal the payload for %s: %v", definition, err)
	}
	if err := schema.Validate(decoded); err != nil {
		t.Fatalf("the payload does not match %s: %v", definition, err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the contract: %v", err)
	}
	var contract struct {
		Definitions map[string]struct {
			Properties map[string]json.RawMessage `json:"properties"`
		} `json:"definitions"`
	}
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatalf("read the contract: %v", err)
	}
	declared := contract.Definitions[definition].Properties
	for field := range decoded {
		if _, ok := declared[field]; !ok {
			t.Fatalf("%s carries %q and the contract does not name it", definition, field)
		}
	}
}

// Retiring a conversation clears it and only then publishes and puts its socket back. A
// replacement installed in between is one the attempt that just ended disconnects the
// socket of, and marks stale: the operator's corrected attempt fails for reasons belonging
// to the one before it.
func TestAReplacementPairingWaitsForTheOneItReplaces(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	run := session.startPairing(t.Context(), func() {})

	// publishPairingFailure takes this to change the socket's state, so holding it pins
	// the retirement exactly where a replacement would slip in.
	session.transition.Lock()
	retired := make(chan struct{})
	go func() {
		defer close(retired)
		session.publishPairing(run, wm.QRChannelTimeout, true)
	}()
	if emission := next(t, session); emission.Type != protocol.EventPairingError {
		t.Fatalf("published %q, want the pairing failure", emission.Type)
	}

	started := make(chan *pairingRun, 1)
	go func() { started <- session.startPairing(t.Context(), func() {}) }()
	select {
	case <-started:
		t.Fatal("a replacement started while the attempt before it was still being torn down")
	case <-time.After(100 * time.Millisecond):
	}

	session.transition.Unlock()
	<-retired
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the replacement never started once the attempt before it was done")
	}
}

// A session that has stopped writes nothing more, whatever context it is handed.
//
// The context it was connected with is cancelled on the way out, and that already refuses
// every write a node handler makes. This asks with a live context instead, which is what
// the work whatsmeow detaches from the connection context arrives with -- a history sync
// storing its secrets, a key share storing its app-state keys -- and is the half the fence
// exists for. Written by an instance that has handed the session on, those land on top of
// whatever the new owner has learned since.
func TestASessionThatStoppedWritesNothingMore(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	device := session.current().Store

	const address = "5511999990002.0"
	if err := device.Identities.PutIdentity(t.Context(), address, [32]byte{1}); err != nil {
		t.Fatalf("a session that is running could not write: %v", err)
	}

	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err := device.Identities.PutIdentity(t.Context(), address, [32]byte{2})
	if !errors.Is(err, store.ErrNotOwned) {
		t.Fatalf("a session that stopped wrote an identity anyway: %v", err)
	}
	// And a read still answers: what a peer cannot afford is this instance writing, not
	// this instance looking.
	if _, err := device.Identities.IsTrustedIdentity(t.Context(), address, [32]byte{1}); err != nil {
		t.Errorf("a stopped session cannot read its own keys either: %v", err)
	}
}

// A session builds more than one device over its life: a logout and a mapping that went
// stale both send it back for another, and the client it adopts then is the one doing the
// writing for the rest of the session. Fenced at Open and nowhere else, that replacement
// would run raw while Close went on dropping a fence in front of a device nobody uses.
func TestASessionThatRebuiltIsStillFenced(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	if err := session.rebuild(t.Context()); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	device := session.current().Store

	const address = "5511999990002.0"
	if err := device.Identities.PutIdentity(t.Context(), address, [32]byte{1}); err != nil {
		t.Fatalf("a session that had just rebuilt could not write: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := device.Identities.PutIdentity(t.Context(), address, [32]byte{2}); !errors.Is(err, store.ErrNotOwned) {
		t.Fatalf("the device a rebuild adopted wrote after the session stopped: %v", err)
	}
}

// Closed and fenced are one transition, not two. What an Open racing a Close reads to
// decide it may build the replacement is the closed flag, so a session that reports itself
// closed while its fence is still up is a session two clients can write through. The order
// is held by both living in one critical section; this is the half a test can see, which is
// that closing drops it at all.
func TestClosingASessionDropsItsFence(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	if session.fence.Dropped() {
		t.Fatal("a session that is running is already fenced")
	}
	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !session.Closed() {
		t.Fatal("Close returned and the session does not report itself closed")
	}
	if !session.fence.Dropped() {
		t.Error("a closed session still holds its fence up")
	}
}
