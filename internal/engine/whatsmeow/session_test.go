package whatsmeow

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"image/png"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/types"
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

	// M2 brings the sends. Until then a refusal is the honest answer: acknowledging a
	// send this build cannot make would lose the message and report success.
	if _, err := session.Execute(t.Context(), &protocol.Command{Type: protocol.CommandMessageSend}); !errors.Is(err, engine.ErrNotSupported) {
		t.Fatalf("message.send answered %v, want ErrNotSupported", err)
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
				ID:       types.JID{User: "5511999990001", Server: types.DefaultUserServer},
				LID:      types.JID{User: "192676662091991", Server: types.HiddenUserServer},
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
			event: &waEvents.PairError{Error: errors.New("the phone said no")},
			want:  protocol.EventPairingError,
			check: func(t *testing.T, payload map[string]any) {
				if payload["message"] != "the phone said no" {
					t.Fatalf("message=%v", payload["message"])
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
			session.startPairing(func() {})

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
			run := session.startPairing(func() {})

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
	run := session.startPairing(func() {})

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
	waEngine := New(container, "fazer.ai test", zerolog.Nop())
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
func newTestSession(t *testing.T, phone string) (*Session, *store.Container) {
	t.Helper()

	container := openStore(t)
	sid := "sid-" + t.Name()
	device, err := container.Device(t.Context(), sid)
	if err != nil {
		t.Fatalf("Device: %v", err)
	}
	if phone != "" {
		// A companion device JID, the way WhatsApp issues one.
		jid, err := types.ParseJID(phone + ":12@" + types.DefaultUserServer)
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
		if err := container.Devices().PutDevice(t.Context(), device); err != nil {
			t.Fatalf("PutDevice: %v", err)
		}
		if err := container.Bind(t.Context(), sid, jid); err != nil {
			t.Fatalf("Bind: %v", err)
		}
	}

	session := newSession(sid, wm.NewClient(device, nil), container, zerolog.Nop(), newLibraryLogger(zerolog.Nop(), sid))
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
	if written.Len() != 0 {
		t.Fatalf("the library logger wrote its debug output: %s", written.String())
	}

	// What an operator needs when a session will not connect still gets through.
	logger.Warnf("Failed to connect")
	if !strings.Contains(written.String(), "Failed to connect") {
		t.Fatalf("the library logger dropped a warning: %s", written.String())
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

	run := session.startPairing(func() {})
	session.cancelPairing()

	session.publishPairing(run, wm.QRChannelItem{Event: "code", Code: "2@a,b,c"}, true)

	select {
	case emission := <-session.Events():
		t.Fatalf("a cancelled run published %q", emission.Type)
	case <-time.After(200 * time.Millisecond):
	}
}
