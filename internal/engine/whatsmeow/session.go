package whatsmeow

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	qrcode "github.com/skip2/go-qrcode"
	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// inboxDepth is how many emissions may wait for the pump. WhatsApp's own event
// handlers run on the socket's goroutine, so an emission that blocked there would stop
// the socket reading; the buffer is what keeps a slow publisher off the wire.
const inboxDepth = 256

// qrSize is the pixel width of the pairing image. Large enough to scan off a screen
// at the size a dashboard renders it, small enough to travel inside an event.
const qrSize = 512

// Session is one WhatsApp account on a whatsmeow client.
type Session struct {
	sid    string
	client *wm.Client
	store  *store.Container
	log    zerolog.Logger

	// Producers send here and never close it; the forwarder owns `events` and is the
	// only thing that closes it. Two channels rather than one because whatsmeow's
	// handlers can still be running when Close is called, and a send on a closed
	// channel is a panic in a library goroutine we do not own.
	inbox  chan engine.Emission
	events chan engine.Emission
	done   chan struct{}

	handlerID uint32

	mu     sync.Mutex
	closed bool
	// pairing cancels the goroutine reading the QR channel, so a second Connect does
	// not leave a previous pairing conversation publishing codes nobody asked for.
	pairing context.CancelFunc
}

//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func newSession(sid string, client *wm.Client, container *store.Container, log zerolog.Logger) *Session {
	s := &Session{
		sid:    sid,
		client: client,
		store:  container,
		log:    log.With().Str("sid", sid).Logger(),
		inbox:  make(chan engine.Emission, inboxDepth),
		events: make(chan engine.Emission),
		done:   make(chan struct{}),
	}

	// WhatsApp itself demands a reconnect in the middle of pairing: the server closes
	// the stream with a 515 and expects the client back. Turning whatsmeow's reconnect
	// off would leave every pairing hanging one step from done, so the socket's own
	// recovery stays with the library. Ownership does not: the layer above closes this
	// session the moment the lease is gone, which is what keeps two instances off one
	// account.
	client.EnableAutoReconnect = true
	client.PrePairCallback = s.bind

	s.handlerID = client.AddEventHandler(s.handle)
	go s.forward()
	return s
}

// Events is the emission channel, closed once the session is done.
func (s *Session) Events() <-chan engine.Emission { return s.events }

// Connect starts pairing or resumes a stored session.
func (s *Session) Connect(ctx context.Context, req engine.ConnectRequest) error {
	if s.isClosed() {
		return errors.New("whatsmeow: the session is closed")
	}

	switch req.Pairing {
	case "resume":
		return s.resume()
	case "qr":
		return s.pairWithQR(ctx)
	case "code":
		return s.pairWithCode(ctx, req.Phone)
	default:
		return protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is not a pairing mode this connector knows", req.Pairing))
	}
}

// resume reconnects a session that has already paired. Asking to resume one that has
// not is a client bug rather than a reason to silently start a pairing the operator
// is not watching for.
func (s *Session) resume() error {
	if s.client.Store.ID == nil {
		return protocol.NewError(protocol.ErrorNotPaired,
			"this session has not paired, so there is nothing to resume")
	}
	if s.client.IsConnected() {
		return nil
	}
	if err := s.client.Connect(); err != nil {
		return fmt.Errorf("whatsmeow: resume %s: %w", s.sid, err)
	}
	s.emit(protocol.EventSessionState, map[string]any{"state": "connecting"})
	return nil
}

// pairWithQR connects and publishes the codes WhatsApp issues until one is scanned.
//
// The channel has to be taken before connecting: it is how whatsmeow reports the
// outcome of the pairing, and asking for it afterwards misses the first code.
func (s *Session) pairWithQR(ctx context.Context) error {
	if s.client.Store.ID != nil {
		// Already paired. A client asking for a QR code here means the operator hit
		// connect on an inbox that is simply disconnected, and resuming is what they
		// meant.
		return s.resume()
	}

	pairCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	codes, err := s.client.GetQRChannel(pairCtx)
	if err != nil {
		cancel()
		return fmt.Errorf("whatsmeow: open the pairing channel of %s: %w", s.sid, err)
	}
	s.setPairing(cancel)

	go s.readPairing(codes)

	if err := s.client.Connect(); err != nil {
		s.setPairing(nil)
		return fmt.Errorf("whatsmeow: connect %s: %w", s.sid, err)
	}
	s.emit(protocol.EventSessionState, map[string]any{"state": "connecting"})
	return nil
}

// pairWithCode connects and asks WhatsApp for a code the operator types on the phone.
//
// whatsmeow needs the socket up before it can ask, and it reports readiness by putting
// the first QR code on the pairing channel. Waiting for that is what makes the request
// land on an established connection rather than a sleep that is usually long enough.
func (s *Session) pairWithCode(ctx context.Context, phone string) error {
	if phone == "" {
		return protocol.NewError(protocol.ErrorInvalidPayload, "code pairing needs the phone number to pair")
	}
	if s.client.Store.ID != nil {
		return s.resume()
	}

	pairCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	codes, err := s.client.GetQRChannel(pairCtx)
	if err != nil {
		cancel()
		return fmt.Errorf("whatsmeow: open the pairing channel of %s: %w", s.sid, err)
	}
	s.setPairing(cancel)

	// The QR codes themselves are dropped: an operator who asked for a code is not
	// looking at an image, and publishing both would have the dashboard show two ways
	// to pair the same session.
	ready := make(chan struct{})
	go func() {
		s.readPairingWith(codes, func() { close(ready) }, false)
	}()

	if err := s.client.Connect(); err != nil {
		s.setPairing(nil)
		return fmt.Errorf("whatsmeow: connect %s: %w", s.sid, err)
	}
	s.emit(protocol.EventSessionState, map[string]any{"state": "connecting"})

	select {
	case <-ready:
	case <-ctx.Done():
		return fmt.Errorf("whatsmeow: %s did not reach the server in time: %w", s.sid, ctx.Err())
	case <-s.done:
		return errors.New("whatsmeow: the session closed before it could ask for a code")
	}

	code, err := s.client.PairPhone(ctx, phone, true, wm.PairClientChrome, "fazer.ai")
	if err != nil {
		return fmt.Errorf("whatsmeow: request a pairing code for %s: %w", s.sid, err)
	}
	s.emit(protocol.EventPairingCode, map[string]any{"code": code, "phone": phone})
	return nil
}

// Disconnect drops the socket and keeps the credentials.
func (s *Session) Disconnect(_ context.Context) error {
	s.setPairing(nil)
	s.client.Disconnect()
	s.emit(protocol.EventSessionState, map[string]any{"state": "close", "reason": "disconnect_requested"})
	return nil
}

// Logout ends the session on WhatsApp's side and forgets the credentials here, so the
// next connect has to pair again.
func (s *Session) Logout(ctx context.Context) error {
	s.setPairing(nil)
	if err := s.client.Logout(ctx); err != nil {
		return fmt.Errorf("whatsmeow: log %s out: %w", s.sid, err)
	}
	if err := s.store.Forget(ctx, s.sid); err != nil {
		return err
	}
	s.emit(protocol.EventSessionLoggedOut, map[string]any{"reason": "logout_requested"})
	return nil
}

// Execute carries out one command.
//
// M1 answers the one command that is about the session itself. Everything else is
// refused rather than answered with a plausible shape: a connector that acknowledged a
// send it cannot make would lose the message and report success.
func (s *Session) Execute(_ context.Context, command *protocol.Command) (json.RawMessage, error) {
	if command.Type == protocol.CommandSessionStatus {
		return json.Marshal(s.status())
	}
	return nil, engine.ErrNotSupported
}

// Close ends the session. Events is closed before it returns.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.drain()
		return nil
	}
	s.closed = true
	cancel := s.pairing
	s.pairing = nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	s.client.RemoveEventHandler(s.handlerID)
	s.client.Disconnect()

	close(s.done)
	s.drain()
	return nil
}

// drain waits for the forwarder to close Events, which is how Close keeps its promise
// that a reader of Events always terminates before Close returns.
func (s *Session) drain() {
	//nolint:revive // an empty body is the whole point: read until the forwarder closes it
	for range s.events {
	}
}

// Closed reports whether this session has been shut down.
func (s *Session) Closed() bool { return s.isClosed() }

func (s *Session) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Session) setPairing(cancel context.CancelFunc) {
	s.mu.Lock()
	previous := s.pairing
	s.pairing = cancel
	s.mu.Unlock()
	if previous != nil {
		previous()
	}
}

// status is the `connection_state` the contract answers session.status and
// session.connect with.
func (s *Session) status() map[string]any {
	state := map[string]any{"state": s.state()}
	if id := s.client.Store.ID; id != nil {
		state["phone"] = id.User
	}
	if lid := s.client.Store.LID; !lid.IsEmpty() {
		state["lid"] = lid.User
	}
	return state
}

func (s *Session) state() string {
	switch {
	case s.isClosed():
		return "close"
	case s.client.IsLoggedIn():
		return "open"
	case s.client.IsConnected():
		return "connecting"
	default:
		return "close"
	}
}

// forward moves emissions onto the channel the pump reads, and owns closing it.
func (s *Session) forward() {
	defer close(s.events)
	for {
		select {
		case <-s.done:
			return
		case emission := <-s.inbox:
			select {
			case s.events <- emission:
			case <-s.done:
				return
			}
		}
	}
}

func (s *Session) emit(eventType protocol.EventType, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		// Everything reaching this is built a few lines above, so a failure is a
		// programming error rather than something a session should carry on through.
		s.log.Error().Err(err).Str("type", string(eventType)).Msg("failed to render an event payload")
		return
	}
	select {
	case s.inbox <- engine.Emission{Type: eventType, Payload: body}:
	case <-s.done:
	}
}

// readPairing publishes the QR codes and the outcome of the pairing.
func (s *Session) readPairing(codes <-chan wm.QRChannelItem) {
	s.readPairingWith(codes, nil, true)
}

// readPairingWith drains the pairing channel. `onFirst` fires once the server has
// answered at all, which is what code pairing waits for, and `publishCodes` is false
// for the code flow, which has no image to show.
func (s *Session) readPairingWith(codes <-chan wm.QRChannelItem, onFirst func(), publishCodes bool) {
	first := true
	for item := range codes {
		if item.Event == "code" && first {
			first = false
			if onFirst != nil {
				onFirst()
			}
		}
		s.publishPairing(item, publishCodes)
	}
	if first && onFirst != nil {
		// The channel ended before a single code arrived, so whoever is waiting for the
		// connection is waiting for something that will never come.
		onFirst()
	}
}

func (s *Session) publishPairing(item wm.QRChannelItem, publishCodes bool) {
	switch item.Event {
	case "code":
		if !publishCodes {
			return
		}
		image, err := qrDataURL(item.Code)
		if err != nil {
			s.log.Error().Err(err).Msg("failed to render a pairing code")
			return
		}
		s.emit(protocol.EventPairingQR, map[string]any{
			"png_data_url":  image,
			"expires_in_ms": item.Timeout.Milliseconds(),
		})
	case "success":
		// pairing.success is published from the PairSuccess event, which is the one
		// that carries the address that was paired.
	case "err-client-outdated":
		s.emit(protocol.EventSessionClientOutdated, map[string]any{})
	case "timeout":
		s.emit(protocol.EventPairingError, map[string]any{
			"reason": "timeout", "message": "nobody scanned the code before it ran out",
		})
	case "error":
		s.emit(protocol.EventPairingError, map[string]any{
			"reason": "error", "message": errorText(item.Error),
		})
	default:
		s.emit(protocol.EventPairingError, map[string]any{"reason": item.Event, "message": errorText(item.Error)})
	}
}

// bind records the pairing before whatsmeow writes the device, which is what keeps a
// crash between the two from leaving credentials no session claims. Refusing here
// cancels the pairing, which is the right outcome: a device we cannot attribute is one
// no restart can find again.
func (s *Session) bind(jid types.JID, _, _ string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), bindTimeout)
	defer cancel()

	if err := s.store.Bind(ctx, s.sid, jid); err != nil {
		s.log.Error().Err(err).Msg("failed to record a pairing; refusing it")
		return false
	}
	return true
}

// bindTimeout bounds the write that stands between a scanned code and a paired
// session. It is short because WhatsApp is waiting on the other side of it.
const bindTimeout = 5 * time.Second

func qrDataURL(code string) (string, error) {
	png, err := qrcode.Encode(code, qrcode.Medium, qrSize)
	if err != nil {
		return "", fmt.Errorf("whatsmeow: render a pairing code: %w", err)
	}
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(png), nil
}

func errorText(err error) any {
	if err == nil {
		return nil
	}
	return err.Error()
}

// handle turns what whatsmeow reports into what the contract names. Anything with no
// canonical shape is left alone here rather than published as `raw`: M1 is the session
// lifecycle, and the message events arrive with M2.
func (s *Session) handle(rawEvent any) {
	switch event := rawEvent.(type) {
	case *waEvents.Connected:
		s.emit(protocol.EventSessionState, s.status())
	case *waEvents.Disconnected:
		s.emit(protocol.EventSessionState, map[string]any{"state": "reconnecting", "reason": "disconnected"})
	case *waEvents.LoggedOut:
		s.loggedOut(event)
	case *waEvents.StreamReplaced:
		s.emit(protocol.EventSessionStreamReplaced, map[string]any{})
	case *waEvents.TemporaryBan:
		s.emit(protocol.EventSessionTemporaryBan, map[string]any{"ban": map[string]any{
			"kind":       "temporary",
			"expires_at": time.Now().Add(event.Expire).UnixMilli(),
			"reason":     event.Code.String(),
		}})
	case *waEvents.ClientOutdated:
		s.emit(protocol.EventSessionClientOutdated, map[string]any{})
	case *waEvents.ConnectFailure:
		s.emit(protocol.EventSessionConnectFailure, map[string]any{
			"reason": event.Reason.String(), "code": int(event.Reason),
		})
	case *waEvents.PairSuccess:
		s.paired(event)
	case *waEvents.PairError:
		s.emit(protocol.EventPairingError, map[string]any{
			"reason": "pair_error", "message": errorText(event.Error),
		})
	}
}

func (s *Session) loggedOut(event *waEvents.LoggedOut) {
	// The credentials are gone on WhatsApp's side, so keeping them here would have
	// every reconnect fail with a session that looks resumable and is not.
	ctx, cancel := context.WithTimeout(context.Background(), bindTimeout)
	defer cancel()
	if err := s.store.Forget(ctx, s.sid); err != nil {
		s.log.Error().Err(err).Msg("failed to forget the device of a session that was logged out")
	}
	s.emit(protocol.EventSessionLoggedOut, map[string]any{
		"reason": event.Reason.String(), "on_connect": event.OnConnect,
	})
}

func (s *Session) paired(event *waEvents.PairSuccess) {
	payload := map[string]any{"phone": event.ID.User, "platform": event.Platform}
	if !event.LID.IsEmpty() {
		payload["lid"] = event.LID.User
	}
	s.emit(protocol.EventPairingSuccess, payload)
}
