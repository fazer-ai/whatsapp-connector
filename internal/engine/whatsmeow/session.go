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
	waLog "go.mau.fi/whatsmeow/util/log"

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

// clientDisplayName is what code pairing tells the server this client is. WhatsApp
// validates the `Browser (OS)` shape against a list of the ones it knows and answers
// 400 for anything else, so it cannot carry the operator's device name: what shows in
// the account's linked-devices list comes from the device properties, not from here.
const clientDisplayName = "Chrome (Linux)"

// Session is one WhatsApp account on a whatsmeow client.
type Session struct {
	sid   string
	store *store.Container
	log   zerolog.Logger
	waLog waLog.Logger

	// Producers send here and never close it; the forwarder owns `events` and is the
	// only thing that closes it. Two channels rather than one because whatsmeow's
	// handlers can still be running when Close is called, and a send on a closed
	// channel is a panic in a library goroutine we do not own.
	inbox  chan engine.Emission
	events chan engine.Emission
	done   chan struct{}

	// ctx is the session's own lifetime, cancelled by Close. Every socket the client
	// dials is dialled under it, so a Close during a handshake tears the dial down
	// instead of waiting behind it: whatsmeow's own Connect runs on a background
	// context, and a stale owner blocked in a dial is one that outlives its lease.
	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	client    *wm.Client
	handlerID uint32
	closed    bool
	// phone and lid are this session's copy of what it paired. whatsmeow assigns the
	// same fields on its pairing goroutine, so reading them off the client from a
	// command is a race; this is written from the event handler and read under the
	// lock.
	phone string
	lid   string
	// pairing is the conversation currently open, if any. It is a pointer rather than a
	// bare cancel func so a finished run can clear itself without clearing the one that
	// replaced it: functions are not comparable, and "is this still mine" is the whole
	// question.
	pairing *pairingRun
}

// pairingRun is one pairing conversation.
type pairingRun struct{ cancel context.CancelFunc }

//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func newSession(sid string, client *wm.Client, container *store.Container, log zerolog.Logger, wa waLog.Logger) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{
		sid:    sid,
		store:  container,
		log:    log.With().Str("sid", sid).Logger(),
		waLog:  wa,
		inbox:  make(chan engine.Emission, inboxDepth),
		events: make(chan engine.Emission),
		done:   make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}
	s.adopt(client)
	go s.forward()
	return s
}

// adopt takes a client over: it wires the callbacks, subscribes to its events, and
// copies out the identity it was built with. Nothing else assigns s.client, so the
// logout path can hand over a replacement without a second code path.
func (s *Session) adopt(client *wm.Client) {
	// WhatsApp itself demands a reconnect in the middle of pairing: the server closes
	// the stream with a 515 and expects the client back. Turning whatsmeow's reconnect
	// off would leave every pairing hanging one step from done, so the socket's own
	// recovery stays with the library. Ownership does not: the layer above closes this
	// session the moment the lease is gone, which is what keeps two instances off one
	// account.
	client.EnableAutoReconnect = true
	client.PrePairCallback = s.bind
	client.BackgroundEventCtx = s.ctx

	phone, lid := "", ""
	if id := client.Store.ID; id != nil {
		phone = id.User
	}
	if stored := client.Store.LID; !stored.IsEmpty() {
		lid = stored.User
	}

	s.mu.Lock()
	s.client = client
	s.phone = phone
	s.lid = lid
	s.mu.Unlock()

	// Subscribed after the fields are set, so the first event cannot find a session
	// halfway through adopting the client that produced it.
	handlerID := client.AddEventHandler(s.handle)
	s.mu.Lock()
	s.handlerID = handlerID
	s.mu.Unlock()
}

// current is the client this session is on right now.
func (s *Session) current() *wm.Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client
}

func (s *Session) identity() (phone, lid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.phone, s.lid
}

func (s *Session) setIdentity(phone, lid string) {
	s.mu.Lock()
	s.phone = phone
	s.lid = lid
	s.mu.Unlock()
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
		return s.resume(ctx)
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
func (s *Session) resume(ctx context.Context) error {
	if phone, _ := s.identity(); phone == "" {
		return protocol.NewError(protocol.ErrorNotPaired,
			"this session has not paired, so there is nothing to resume")
	}
	client := s.current()
	if client.IsConnected() {
		return nil
	}
	if err := s.dial(ctx, client); err != nil {
		return fmt.Errorf("whatsmeow: resume %s: %w", s.sid, err)
	}
	s.emit(protocol.EventSessionState, map[string]any{"state": "connecting"})
	return nil
}

// dial connects, and stops waiting when the command that asked for it does.
//
// The socket belongs to the session and not to the command, so a deadline that passes
// mid-handshake answers the caller and leaves the dial running: the alternative, handing
// whatsmeow a context that dies with the RPC, would also kill the reconnect loop it
// starts from the same one. What the deadline must not do is hold the session's command
// queue, which is single-file, behind a network round trip nobody is waiting for.
func (s *Session) dial(ctx context.Context, client *wm.Client) error {
	dialed := make(chan error, 1)
	go func() { dialed <- client.ConnectContext(s.ctx) }()

	select {
	case err := <-dialed:
		return err
	case <-ctx.Done():
		return fmt.Errorf("whatsmeow: %s was still connecting: %w", s.sid, ctx.Err())
	case <-s.done:
		return errors.New("whatsmeow: the session closed while it was connecting")
	}
}

// pairWithQR connects and publishes the codes WhatsApp issues until one is scanned.
//
// The channel has to be taken before connecting: it is how whatsmeow reports the
// outcome of the pairing, and asking for it afterwards misses the first code.
//
// The conversation itself outlives the command that started it, running on the
// session's lifetime until a code is scanned, the codes run out, or Close ends it. Only
// the wait for the socket honours the command's deadline.
func (s *Session) pairWithQR(ctx context.Context) error {
	if phone, _ := s.identity(); phone != "" {
		// Already paired. A client asking for a QR code here means the operator hit
		// connect on an inbox that is simply disconnected, and resuming is what they
		// meant.
		return s.resume(ctx)
	}

	client := s.current()
	pairCtx, cancel := context.WithCancel(s.ctx)
	codes, err := client.GetQRChannel(pairCtx)
	if err != nil {
		cancel()
		return fmt.Errorf("whatsmeow: open the pairing channel of %s: %w", s.sid, err)
	}
	run := s.startPairing(cancel)

	go s.readPairing(run, codes)

	if err := s.dial(ctx, client); err != nil {
		s.giveUpOn(ctx, client, err)
		return fmt.Errorf("whatsmeow: connect %s: %w", s.sid, err)
	}
	s.emit(protocol.EventSessionState, map[string]any{"state": "connecting"})
	return nil
}

// giveUpOn tears a pairing attempt down for a dial that failed, and leaves it alone for
// one the caller merely stopped waiting for.
//
// The distinction matters twice over: the conversation is documented to outlive the
// command, and tearing it down calls Disconnect, which would wait on the socket lock
// the still-running dial is holding — putting the executor right back behind the
// deadline it just escaped.
func (s *Session) giveUpOn(ctx context.Context, client *wm.Client, err error) {
	if errors.Is(err, ctx.Err()) && ctx.Err() != nil {
		return
	}
	s.abandonPairing(client)
}

// hangUp closes the socket without holding the session's command queue behind a dial.
//
// whatsmeow keeps its socket lock for the length of a connect, and Disconnect waits for
// that same lock, so a disconnect arriving mid-handshake would otherwise sit there long
// past its deadline and then report success. Cancelling the session context is what
// actually interrupts the dial; this is the part that stops waiting.
func (s *Session) hangUp(ctx context.Context, client *wm.Client) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.Disconnect()
	}()

	select {
	case <-done:
	case <-ctx.Done():
	case <-s.done:
	}
}

// abandonPairing gives up a pairing conversation and puts the socket back where a
// corrected connect can start a new one. Without the disconnect the next GetQRChannel
// refuses, because whatsmeow will not open a second pairing channel on a live socket,
// and the operator is stuck until the codes run out.
func (s *Session) abandonPairing(client *wm.Client) {
	s.cancelPairing()
	client.Disconnect()
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
	if paired, _ := s.identity(); paired != "" {
		return s.resume(ctx)
	}

	client := s.current()
	pairCtx, cancel := context.WithCancel(s.ctx)
	codes, err := client.GetQRChannel(pairCtx)
	if err != nil {
		cancel()
		return fmt.Errorf("whatsmeow: open the pairing channel of %s: %w", s.sid, err)
	}
	run := s.startPairing(cancel)

	// The QR codes themselves are dropped: an operator who asked for a code is not
	// looking at an image, and publishing both would have the dashboard show two ways
	// to pair the same session.
	ready := make(chan struct{})
	go func() {
		s.readPairingWith(run, codes, func() { close(ready) }, false)
	}()

	if err := s.dial(ctx, client); err != nil {
		// Off the executor when the dial is still running: Disconnect waits on the lock
		// that dial is holding, and this attempt is over either way.
		go s.abandonPairing(client)
		return fmt.Errorf("whatsmeow: connect %s: %w", s.sid, err)
	}
	s.emit(protocol.EventSessionState, map[string]any{"state": "connecting"})

	select {
	case <-ready:
	case <-ctx.Done():
		// Unlike a QR conversation, this one cannot carry on without its command:
		// nothing else will ever call PairPhone. Left up, the socket refuses the
		// operator's next attempt at GetQRChannel until WhatsApp's own codes run out.
		go s.abandonPairing(client)
		return fmt.Errorf("whatsmeow: %s did not reach the server in time: %w", s.sid, ctx.Err())
	case <-s.done:
		return errors.New("whatsmeow: the session closed before it could ask for a code")
	}

	code, err := client.PairPhone(ctx, phone, true, wm.PairClientChrome, clientDisplayName)
	if err != nil {
		// A number WhatsApp refuses is the ordinary case here, and the operator is about
		// to type a corrected one. Leaving the pairing socket up would refuse that next
		// attempt too, for reasons that have nothing to do with the number.
		s.abandonPairing(client)
		if coded := codeForPairPhone(err); coded != nil {
			return coded
		}
		return fmt.Errorf("whatsmeow: request a pairing code for %s: %w", s.sid, err)
	}
	s.emit(protocol.EventPairingCode, map[string]any{"code": code, "phone": phone})
	return nil
}

// Disconnect drops the socket and keeps the credentials.
func (s *Session) Disconnect(ctx context.Context) error {
	s.cancelPairing()
	s.hangUp(ctx, s.current())
	s.emit(protocol.EventSessionState, map[string]any{"state": "close", "reason": "disconnect_requested"})
	return nil
}

// Logout ends the session on WhatsApp's side and forgets the credentials here, so the
// next connect has to pair again.
func (s *Session) Logout(ctx context.Context) error {
	s.cancelPairing()
	if err := s.current().Logout(ctx); err != nil {
		return fmt.Errorf("whatsmeow: log %s out: %w", s.sid, err)
	}
	if err := s.store.Forget(ctx, s.sid); err != nil {
		return err
	}
	if err := s.rebuild(ctx); err != nil {
		return err
	}
	s.emit(protocol.EventSessionLoggedOut, map[string]any{"reason": "logout_requested"})
	return nil
}

// rebuild puts the session on a fresh client.
//
// whatsmeow's Logout marks the device deleted rather than emptying it, and every later
// call on that client answers ErrDeviceDeleted. Without this the documented next step,
// pairing again, fails on a session the manager is still perfectly happy to run, and
// the only way out is to release and re-adopt it.
func (s *Session) rebuild(ctx context.Context) error {
	if s.isClosed() {
		// Nothing left to pair with. A session that closed while this was on its way is
		// one the layer above has already given up.
		return nil
	}

	device, err := s.store.Device(ctx, s.sid)
	if err != nil {
		return fmt.Errorf("whatsmeow: rebuild %s: %w", s.sid, err)
	}

	previous := s.current()
	previous.RemoveEventHandler(s.handlerID)
	previous.Disconnect()

	s.adopt(wm.NewClient(device, s.waLog))
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
	run := s.pairing
	s.pairing = nil
	s.mu.Unlock()

	if run != nil {
		run.cancel()
	}
	// Cancelled first, and that order is the whole point: whatsmeow holds its socket
	// lock for the length of a dial, and Disconnect waits for the same lock. Cancelling
	// afterwards would never run, and a lease handover would wait out the handshake.
	s.cancel()

	client := s.current()
	client.RemoveEventHandler(s.handlerID)
	client.Disconnect()

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

// pairingActive reports whether a pairing conversation is open, which is what decides
// who publishes an outcome both paths are told about.
func (s *Session) pairingActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pairing != nil
}

// startPairing makes a run the current one and ends whatever it replaces.
func (s *Session) startPairing(cancel context.CancelFunc) *pairingRun {
	run := &pairingRun{cancel: cancel}
	s.mu.Lock()
	previous := s.pairing
	s.pairing = run
	s.mu.Unlock()
	if previous != nil {
		previous.cancel()
	}
	return run
}

// endPairing clears a run that has finished, and does nothing when another has already
// taken its place.
func (s *Session) endPairing(run *pairingRun) {
	s.mu.Lock()
	if s.pairing == run {
		s.pairing = nil
	}
	s.mu.Unlock()
}

// cancelPairing ends whatever conversation is open.
func (s *Session) cancelPairing() {
	s.mu.Lock()
	previous := s.pairing
	s.pairing = nil
	s.mu.Unlock()
	if previous != nil {
		previous.cancel()
	}
}

// status is the `connection_state` the contract answers session.status and
// session.connect with. Its key is `connection` and its number is `phone_number`: the
// event that reports the same change spells both differently, and answering an RPC
// with the event's shape leaves the caller without the one field the result requires.
func (s *Session) status() map[string]any {
	phone, lid := s.identity()
	state := map[string]any{"connection": s.state()}
	if phone != "" {
		state["phone_number"] = phone
	}
	if lid != "" {
		state["lid"] = lid
	}
	return state
}

// sessionState is the `session.state` event for the same connection, which spells the
// state under `state` and the number under `phone`.
func (s *Session) sessionState() map[string]any {
	phone, lid := s.identity()
	payload := map[string]any{"state": s.state()}
	if phone != "" {
		payload["phone"] = phone
	}
	if lid != "" {
		payload["lid"] = lid
	}
	return payload
}

func (s *Session) state() string {
	client := s.current()
	switch {
	case s.isClosed():
		return "close"
	case client.IsLoggedIn():
		return "open"
	case client.IsConnected():
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
func (s *Session) readPairing(run *pairingRun, codes <-chan wm.QRChannelItem) {
	s.readPairingWith(run, codes, nil, true)
}

// readPairingWith drains the pairing channel. `onFirst` fires once the server has
// answered at all, which is what code pairing waits for, and `publishCodes` is false
// for the code flow, which has no image to show.
func (s *Session) readPairingWith(run *pairingRun, codes <-chan wm.QRChannelItem, onFirst func(), publishCodes bool) {
	// Cleared on the way out, however it ends. A run left marked open after a successful
	// pairing goes on claiming every later outcome, and the events it claims are then
	// published by nobody: the QR channel's own handler is long gone.
	defer s.endPairing(run)

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
	// From the session's own lifetime: a pairing that lands after this instance lost
	// the account would otherwise overwrite the mapping the new owner is writing.
	ctx, cancel := context.WithTimeout(s.ctx, bindTimeout)
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

// codeForPairPhone names the refusals the caller can fix. Left as an internal error
// they reach the dashboard as "the connector could not carry out the command", which
// tells an operator nothing about the number they typed.
func codeForPairPhone(err error) error {
	switch {
	case errors.Is(err, wm.ErrPhoneNumberTooShort):
		return protocol.NewError(protocol.ErrorInvalidPayload, "that number is too short to pair")
	case errors.Is(err, wm.ErrPhoneNumberIsNotInternational):
		return protocol.NewError(protocol.ErrorInvalidPayload,
			"that number needs its country code and no leading zero")
	default:
		return nil
	}
}

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
		s.emit(protocol.EventSessionState, s.sessionState())
	case *waEvents.Disconnected:
		// whatsmeow only reconnects a device it has an id for, so a drop before pairing
		// finishes is the end of that attempt. Reporting it as reconnecting leaves the
		// dashboard waiting on something nothing is going to do.
		state := "reconnecting"
		if phone, _ := s.identity(); phone == "" {
			state = "close"
		}
		s.emit(protocol.EventSessionState, map[string]any{"state": state, "reason": "disconnected"})
	case *waEvents.LoggedOut:
		s.loggedOut(event)
	case *waEvents.StreamReplaced:
		s.emit(protocol.EventSessionStreamReplaced, map[string]any{})
	case *waEvents.TemporaryBan:
		ban := map[string]any{"kind": "temporary", "reason": event.Code.String()}
		if event.Expire > 0 {
			// A zero duration is whatsmeow saying it does not know when the ban lifts.
			// Publishing now+0 would read as one that already has.
			ban["expires_at"] = time.Now().Add(event.Expire).UnixMilli()
		}
		s.emit(protocol.EventSessionTemporaryBan, map[string]any{"ban": ban})
	case *waEvents.ClientOutdated:
		if s.pairingActive() {
			// The pairing reader publishes this one: whatsmeow delivers it here and to
			// the QR channel both, and two canonical events for one outcome is worse
			// than either.
			return
		}
		s.emit(protocol.EventSessionClientOutdated, map[string]any{})
	case *waEvents.ConnectFailure:
		s.emit(protocol.EventSessionConnectFailure, map[string]any{
			"reason": event.Reason.String(), "code": int(event.Reason),
		})
	case *waEvents.PairSuccess:
		s.paired(event)
	case *waEvents.PairError:
		if s.pairingActive() {
			return
		}
		s.emit(protocol.EventPairingError, map[string]any{
			"reason": "pair_error", "message": errorText(event.Error),
		})
	}
}

func (s *Session) loggedOut(event *waEvents.LoggedOut) {
	// The credentials are gone on WhatsApp's side, so keeping them here would have
	// every reconnect fail with a session that looks resumable and is not.
	// On the session's own lifetime: `Forget` unbinds the mapping, so a stale owner
	// finishing this after the account moved would erase what the new owner wrote.
	ctx, cancel := context.WithTimeout(s.ctx, bindTimeout)
	defer cancel()
	if err := s.store.Forget(ctx, s.sid); err != nil {
		s.log.Error().Err(err).Msg("failed to forget the device of a session that was logged out")
	}
	// Off this goroutine, and that is not a preference: whatsmeow dispatches events
	// holding its handler lock for read, and rebuilding takes the same lock for write.
	// Doing it here is a deadlock against ourselves, and the shutdown behind it.
	//
	// The event waits for the replacement. A client that reacts to session.logged_out
	// by pairing again is the expected next step, and reaching a session still holding
	// the deleted device answers it with an error for a state that had already passed.
	go func() {
		if err := s.rebuild(s.ctx); err != nil {
			s.log.Error().Err(err).Msg("failed to put a logged-out session back on a fresh client")
		}
		s.emit(protocol.EventSessionLoggedOut, map[string]any{
			"reason": event.Reason.String(), "on_connect": event.OnConnect,
		})
	}()
}

func (s *Session) paired(event *waEvents.PairSuccess) {
	lid := ""
	if !event.LID.IsEmpty() {
		lid = event.LID.User
	}
	s.setIdentity(event.ID.User, lid)

	payload := map[string]any{"phone": event.ID.User, "platform": event.Platform}
	if lid != "" {
		payload["lid"] = lid
	}
	s.emit(protocol.EventPairingSuccess, payload)
}
