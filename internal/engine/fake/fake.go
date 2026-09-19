// Package fake is an engine that answers without a WhatsApp socket.
//
// It is what every test above the engine runs against, and what `serve --engine fake`
// uses for the M0 end-to-end check: a fleet that pairs, publishes and answers commands
// with nothing behind it. It is deliberately not a simulator of WhatsApp; it does the
// smallest thing that exercises the layers above.
package fake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// QRData is the pairing image the fake issues. A real data URL rather than a
// placeholder, so a client rendering it shows something.
const QRData = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUg=="

// PairingCode is the code the fake issues for code pairing.
const PairingCode = "K7QP2M4X"

// PairedPhone is the shape of the number this fake pairs with: a Brazilian mobile,
// thirteen digits. The number a session actually gets is derived from its id, because one
// constant for a whole fleet is one account for a whole fleet. See PhoneFor.
const PairedPhone = "5511999990001"

// PhoneFor is the number this fake pairs a given session with.
//
// One per session, and that is not decoration. `wac_session_device` carries a UNIQUE
// index over `account`, and the store binds on the account rather than the device,
// because re-pairing issues a new device for the same number. A fake that paired every
// session to one constant would have each new session silently displace the last, and a
// bench that created ten accounts would be measuring one -- which is how #264's
// mass-adoption measurement was blocked before this existed.
func PhoneFor(sid string) string {
	sum := fnv.New32a()
	_, _ = sum.Write([]byte(sid))
	// Thirteen digits, the shape WhatsApp uses for a Brazilian mobile: country, area,
	// the ninth digit, and eight that vary.
	return fmt.Sprintf("55119%08d", sum.Sum32()%100000000)
}

// Engine hands out fake sessions and remembers them, so a test can reach into one it
// has already handed to the layer under test.
type Engine struct {
	mu       sync.Mutex
	sessions map[string]*Session
	store    *store.Container
	closed   bool
}

// Option configures an engine.
//
// Options rather than parameters so the hundred-odd `New()` calls in this repository's
// tests go on compiling: what they exercise has nothing to do with a store, and making
// them all name one would be a change to the suite bought for nothing.
type Option func(*Engine)

// WithStore gives this engine a store to record pairings in.
//
// Without it a session pairs and leaves nothing behind, which is what a deployment on
// this engine did until #266: the sweep that brings accounts back joins the desired row
// with the pairing, so an account with no pairing row is one nothing can resume. The
// whatsmeow engine has always written this from inside its pairing handshake, where the
// JID arrives, and that is a place no layer above can reach.
func WithStore(container *store.Container) Option {
	return func(e *Engine) { e.store = container }
}

// New returns an engine with no sessions open.
func New(opts ...Option) *Engine {
	e := &Engine{sessions: make(map[string]*Session)}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Open returns the session for an id, creating it the first time.
func (e *Engine) Open(_ context.Context, sid string) (engine.Session, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, errors.New("fake: engine is closed")
	}
	if existing, ok := e.sessions[sid]; ok && !existing.done() {
		return existing, nil
	}
	// A session that has been closed is not one to hand out again: its emission channel
	// is closed, so the reader on the other side has already stopped and everything the
	// new owner publishes would go nowhere. An account released and adopted again is an
	// ordinary sequence now, and the real engine opens a new session for it.
	session := newSession(sid)
	if e.store != nil {
		session.store = e.store.For(sid)
	}
	e.sessions[sid] = session
	return session, nil
}

// Session returns an already-opened session, for a test that wants to drive it.
func (e *Engine) Session(sid string) (*Session, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	session, ok := e.sessions[sid]
	return session, ok
}

// Close shuts every session down.
func (e *Engine) Close() error {
	e.mu.Lock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, session := range e.sessions {
		sessions = append(sessions, session)
	}
	e.closed = true
	e.mu.Unlock()

	for _, session := range sessions {
		_ = session.Close()
	}
	return nil
}

// Session is one fake WhatsApp account.
type Session struct {
	sid string
	// store is where this session records its pairing, or nil when nothing asked it to.
	store *store.Scoped

	mu           sync.Mutex
	events       chan engine.Emission
	closed       bool
	connected    bool
	finished     bool
	givenUp      uint64
	loggedOut    int
	deleted      int
	connects     int
	asked        engine.ConnectRequest
	refuseUnlink error
	failDelete   error
	failConnect  error
	failHangUp   error
	onDelete     func()
	commands     []protocol.Command
	bounds       []time.Time
	held         chan struct{}

	heldSucceeds bool
}

func newSession(sid string) *Session {
	// Buffered because the pairing burst is emitted from Connect, and a Connect that
	// blocked until somebody read would deadlock the caller that is about to start
	// reading.
	return &Session{sid: sid, events: make(chan engine.Emission, 32)}
}

// FailConnect makes every connect fail, which is how a test drives a session that cannot
// come back: an account WhatsApp refuses, a store that is away, credentials that no longer
// resume.
func (s *Session) FailConnect(err error) {
	s.mu.Lock()
	s.failConnect = err
	s.mu.Unlock()
}

// Connects counts the connect attempts, failed ones included. It is what a test waits on
// when what it is about is an attempt rather than its outcome.
func (s *Session) Connects() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connects
}

// Asked is the last connect this session was handed, and whether it has been handed one.
//
// Recorded because the pairing mode is not the whole of a connect: the subscription
// travels in the same request, and a caller that synthesises one -- the resume sweep does
// -- can carry the mode and drop the rest, which reaches the client as an account that is
// open and publishes no group conversation at all.
func (s *Session) Asked() (engine.ConnectRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.asked, s.connects > 0
}

// Connect walks the pairing conversation the type asks for and ends `open`.
func (s *Session) Connect(_ context.Context, req engine.ConnectRequest) error {
	s.mu.Lock()
	s.connects++
	// Before the refusal rather than after it: what was asked for is what was asked for,
	// and a test about an attempt reads this to find out what was attempted.
	s.asked = req
	failWith := s.failConnect
	s.mu.Unlock()
	// The same question the whatsmeow engine asks, asked the same way, because the answer
	// a client gets must not depend on which engine a deployment runs: before #266 this
	// engine refused an unknown pairing mode with a bare error, which reaches a client as
	// `internal`, while the other one answered `invalid_payload` for the same request.
	if err := req.Validate(); err != nil {
		return err
	}
	if failWith != nil {
		return failWith
	}
	switch req.Pairing {
	case "qr":
		s.emit(protocol.EventPairingQR, map[string]any{"png_data_url": QRData, "expires_in_ms": 20000})
	case "code":
		s.emit(protocol.EventPairingCode, map[string]any{"code": PairingCode, "phone": req.Phone})
	case "resume":
	default:
		// Unreachable: `Validate` has already refused every mode but these three. Loud
		// rather than silent all the same, because the silent version of this branch is a
		// connect that answers `open` having done nothing at all.
		return fmt.Errorf("fake: unknown pairing mode %q", req.Pairing)
	}

	if req.Pairing != "resume" {
		// Recorded before the success is announced, and the order is the promise rather
		// than an implementation detail: a client that sees `pairing.success` may act on
		// a paired account, and an instance that died between the announcement and the
		// write would leave one nothing can resume. The whatsmeow engine gets this for
		// free, because its write is the `PrePairCallback` and refusing it refuses the
		// pairing; here it has to be written down.
		if err := s.pair(); err != nil {
			return err
		}
		// `phone`, not an address: the schema requires the digits at the top level, and
		// a fake that publishes a shape the contract rejects is an end-to-end check that
		// proves the client would refuse the real thing.
		s.emit(protocol.EventPairingSuccess, map[string]any{
			"phone":    PhoneFor(s.sid),
			"platform": "fake",
		})
	}

	s.mu.Lock()
	s.connected = true
	// A socket that is up is a session with something left to try, which is what a
	// connect landing between a terminal emission and its publish leaves behind.
	s.finished = false
	s.mu.Unlock()
	s.emit(protocol.EventSessionState, map[string]any{"state": "open"})
	return nil
}

// pair records the account this session paired, the way an engine with a socket records
// the one WhatsApp handed it.
//
// A no-op without a store, which is every test above this package that does not care:
// they exercise what the layers do with a session, not what a deployment leaves behind.
// A deployment always has one, and the fence over this obligation is what says so.
func (s *Session) pair() error {
	if s.store == nil {
		return nil
	}
	jid, err := waTypes.ParseJID(PhoneFor(s.sid) + ":12@" + waTypes.DefaultUserServer)
	if err != nil {
		return fmt.Errorf("fake: build the jid of %s: %w", s.sid, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), bindTimeout)
	defer cancel()
	if err := s.store.Bind(ctx, jid); err != nil {
		return fmt.Errorf("fake: record the pairing of %s: %w", s.sid, err)
	}
	return nil
}

// unpair forgets the account this session paired, and what its client asked for with it.
//
// Both, through the one door that says both, because they are the same fact from two
// sides: the credentials are gone, so there is nothing for a resume to resume, and a row
// that outlived them is a sweep dialling an account that no longer exists.
func (s *Session) unpair(ctx context.Context) error {
	if s.store == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, bindTimeout)
	defer cancel()
	if err := s.store.ForgetCredentialsAndDesired(ctx); err != nil {
		return fmt.Errorf("fake: forget the pairing of %s: %w", s.sid, err)
	}
	return nil
}

// bindTimeout bounds the write that stands between a pairing and the event announcing it,
// the way the whatsmeow engine bounds its own: short, because nothing useful happens
// while it is outstanding and the announcement is waiting on it.
const bindTimeout = 5 * time.Second

// FailDisconnect makes dropping the socket fail, which is the case that separates "the
// operator asked for this" from "it happened". The record of the request is the first,
// and a disconnect that could not be carried out does not unask it.
func (s *Session) FailDisconnect(err error) {
	s.mu.Lock()
	s.failHangUp = err
	s.mu.Unlock()
}

// Disconnect drops the socket and says so.
func (s *Session) Disconnect(_ context.Context) error {
	s.mu.Lock()
	failWith := s.failHangUp
	if failWith != nil {
		s.mu.Unlock()
		return failWith
	}
	s.connected = false
	s.mu.Unlock()
	s.emit(protocol.EventSessionState, map[string]any{"state": "close", "reason": "disconnect_requested"})
	return nil
}

// Logout ends the session for good.
func (s *Session) Logout(ctx context.Context) error {
	s.mu.Lock()
	s.connected = false
	s.loggedOut++
	s.mu.Unlock()
	// The pairing goes with the logout, the way the real engine's does. An account whose
	// credentials are gone and whose row is still there is one the sweep tries to bring
	// back every pass, for ever, and that is this defect with the sign reversed: the
	// point of recording what was asked for is that something acts on it.
	if err := s.unpair(ctx); err != nil {
		return err
	}
	s.emit(protocol.EventSessionLoggedOut, map[string]any{"reason": "logout_requested"})
	return nil
}

// Delete unlinks and forgets, and counts the two separately so a test can tell a
// teardown that gave up from one that carried on: the whole point of the real one is
// that a refused unlink does not stop the deletion.
func (s *Session) Delete(ctx context.Context) error {
	s.mu.Lock()
	// The real engine drops the store's fence in Close, and every fenced write after
	// that is refused. Modelled here because a fake that deletes happily after its
	// session was closed hides the one ordering this teardown has to get right: the
	// engine is emptied while it is still open, and stopped afterwards.
	if s.closed {
		s.mu.Unlock()
		return errFenced
	}
	watch := s.onDelete
	failWith := s.failDelete
	refuse := s.refuseUnlink
	s.connected = false
	if refuse == nil {
		s.loggedOut++
	}
	s.deleted++
	s.mu.Unlock()
	if watch != nil {
		watch()
	}
	if failWith != nil {
		return failWith
	}
	// Same as the logout, and after the failure above rather than before it: a teardown
	// that did not happen must not take the record of the account with it.
	if err := s.unpair(ctx); err != nil {
		return err
	}
	// Marked, the way the real one marks it: the account is gone, so the session has
	// nothing left to try and the connector hands the lease back once this is out. A
	// refused unlink says it too -- what WhatsApp was told does not change the fact that
	// this connector is holding an account nothing addresses any more.
	s.EmitLast(protocol.EventSessionLoggedOut, map[string]any{"reason": "session_deleted"})
	// Nil even when WhatsApp refused the unlink, which is the real engine's answer and
	// the one thing a fake here must not soften: the teardown happened, and a failure
	// reported for it is a client republishing a delete over an account that is already
	// gone. What the refusal cost is a device still listed on somebody's phone, which
	// the real one names in its log and neither one can undo.
	return nil
}

// FailDelete makes the teardown itself fail, which is the case where nothing was
// deleted and the account is still addressable.
func (s *Session) FailDelete(err error) {
	s.mu.Lock()
	s.failDelete = err
	s.mu.Unlock()
}

// OnDelete runs at the moment the teardown does, for assertions about what else was
// still true then.
func (s *Session) OnDelete(watch func()) {
	s.mu.Lock()
	s.onDelete = watch
	s.mu.Unlock()
}

// errFenced is what a write refused by a dropped fence answers, which is what the real
// store does once the engine has been closed.
var errFenced = errors.New("fake: the store fence is down")

// Deleted counts the teardowns that ran to the end.
func (s *Session) Deleted() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleted
}

// RefuseUnlink makes WhatsApp turn the unlink down, which is the case Delete exists to
// answer: the credentials go anyway and the caller is told the device may still be
// listed on the phone.
func (s *Session) RefuseUnlink(err error) {
	s.mu.Lock()
	s.refuseUnlink = err
	s.mu.Unlock()
}

// LoggedOut counts how many times the account was unlinked, which is what a test
// asserting a command ran once rather than twice looks at.
func (s *Session) LoggedOut() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loggedOut
}

// Connected reports whether Connect has run and nothing has taken it back.
func (s *Session) Connected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connected
}

// Commands returns what Execute has been asked to do, in order.
func (s *Session) Commands() []protocol.Command {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]protocol.Command(nil), s.commands...)
}

// Bounds returns, per command in Commands, the deadline its context carried, zero when
// it carried none. It is how a test asks whether a command's own deadline reached the
// engine without waiting for one to pass: a wait long enough to observe an expiry is a
// wall clock deciding the order, which is what AGENTS.md rules out.
func (s *Session) Bounds() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.bounds...)
}

// Execute records the command and answers the shapes the contract's result table
// names. Anything it does not know is refused rather than answered with a guess: a
// fake that invents a result shape is a test that passes against a contract nobody
// implements.
func (s *Session) Execute(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	bound, _ := ctx.Deadline()
	s.mu.Lock()
	s.commands = append(s.commands, *command)
	s.bounds = append(s.bounds, bound)
	connected := s.connected
	held := s.held
	s.mu.Unlock()

	if held != nil {
		// In flight, which is the state a real send spends most of its time in. What
		// comes back when the context dies under it is the context's own error, the way
		// whatsmeow reports a call that was cut off rather than answered.
		select {
		case <-held:
		case <-ctx.Done():
			if !s.holdSucceeds() {
				return nil, ctx.Err()
			}
			// Held the other way: the send landed in the same instant the context died,
			// which is the race a caller cannot be left out of. Falls through to the
			// ordinary result.
		}
	}

	switch command.Type {
	case protocol.CommandSessionStatus:
		// A `connection_state`, whose key is `connection`. The `session.state` event
		// reporting the same change spells it `state`, and answering the RPC with the
		// event's shape leaves the caller without the field the result requires.
		state := map[string]any{"connection": "close"}
		if connected {
			state["connection"] = "open"
			// The number this session paired, not the package's, for the same reason
			// the pairing row carries one per session: a status that answered with a
			// constant would have every account in a fleet reporting the same phone.
			state["phone_number"] = PhoneFor(s.sid)
		}
		return marshal(state)
	case protocol.CommandMessageSend, protocol.CommandMessageEdit, protocol.CommandMessageReact:
		if !connected {
			return nil, errNotConnected
		}
		return marshal(map[string]any{
			"message_id": messageIDOf(command),
			"timestamp":  time.Now().UnixMilli(),
			"client_ref": nil,
		})
	case protocol.CommandMessageRevoke, protocol.CommandMessageMarkRead,
		protocol.CommandChatPresence, protocol.CommandPresenceSet, protocol.CommandPresenceSubscribe:
		if !connected {
			return nil, errNotConnected
		}
		return nil, nil
	default:
		return nil, engine.ErrNotSupported
	}
}

// Hold makes every later Execute wait before it does anything, until the returned
// function is called or the command's context ends. It is how a test puts a command in
// flight and then takes the session away underneath it.
func (s *Session) Hold() func() { return s.hold(false) }

// HoldUntilCanceled is Hold for the other side of that race: the command is in flight,
// the context dies, and the work turns out to have landed anyway. It is what a send
// WhatsApp accepted in the same instant the lease moved looks like from here.
func (s *Session) HoldUntilCanceled() func() { return s.hold(true) }

func (s *Session) hold(succeeds bool) func() {
	release := make(chan struct{})
	s.mu.Lock()
	s.held, s.heldSucceeds = release, succeeds
	s.mu.Unlock()
	return sync.OnceFunc(func() {
		s.mu.Lock()
		s.held = nil
		s.mu.Unlock()
		close(release)
	})
}

func (s *Session) holdSucceeds() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heldSucceeds
}

// Events is the emission channel. It is closed by Close.
func (s *Session) Events() <-chan engine.Emission { return s.events }

// Finished is what the fake was last told to say: EmitLast and EmitLastDurable put it
// up, and a Connect that succeeds takes it down, the way whatsmeow's own does.
func (s *Session) Finished() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.finished {
		return 0
	}
	return s.givenUp
}

// giveUp records another giving-up. The caller holds the lock.
func (s *Session) giveUp() uint64 {
	if !s.finished {
		s.finished = true
		s.givenUp++
	}
	return s.givenUp
}

// done reports that this session has been closed, which is the end of it: nothing it is
// asked afterwards reaches anybody.
func (s *Session) done() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Close ends the session. Safe to call twice, because both an operator command and
// the shutdown path reach it.
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.connected = false
	close(s.events)
	return nil
}

// Emit publishes an arbitrary emission, which is how a test drives an inbound message
// or a disconnection that nothing above asked for.
func (s *Session) Emit(eventType protocol.EventType, payload any) { s.emit(eventType, payload) }

// EmitLast publishes an emission after which the engine has nothing more to do for this
// session on its own, which is how a test drives a temporary ban or a connect WhatsApp
// refused without a socket to be banned from.
func (s *Session) EmitLast(eventType protocol.EventType, payload any) {
	body, err := marshal(payload)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.events <- engine.Emission{Type: eventType, Payload: body, Retires: true, Attempt: s.giveUp()}:
	default:
	}
}

// EmitLastRaced is EmitLast made in the instant a connect had already taken the session
// back: the mark is on the emission and the giving-up it named is gone, which is what
// whatsmeow produces when a connect clears the terminal state between the branch that
// marks it and the emission that reports it.
func (s *Session) EmitLastRaced(eventType protocol.EventType, payload any) {
	body, err := marshal(payload)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.events <- engine.Emission{Type: eventType, Payload: body, Retires: true}:
	default:
	}
}

// EmitLastDurable is EmitLast with the callback EmitDurable takes, which is how a test
// waits for the publish itself rather than for something after it.
func (s *Session) EmitLastDurable(eventType protocol.EventType, payload any, settle func(error)) {
	body, err := marshal(payload)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		settle(errors.New("fake: nobody is reading the emissions"))
		return
	}
	select {
	case s.events <- engine.Emission{Type: eventType, Payload: body, Retires: true, Settle: settle, Attempt: s.giveUp()}:
	default:
		settle(errors.New("fake: nobody is reading the emissions"))
	}
}

// EmitAt publishes an emission that says when the engine learned the thing it reports,
// which is what a frame's `ts` carries and the only way a reader can tell an event that
// waited from news of now.
func (s *Session) EmitAt(eventType protocol.EventType, payload any, at int64) {
	body, err := marshal(payload)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.events <- engine.Emission{Type: eventType, Payload: body, At: at}:
	default:
	}
}

// EmitDurable publishes an emission that wants to hear what became of it, which is the
// shape a real engine uses for anything WhatsApp is holding an acknowledgement on. The
// callback is what a test asserts against: it is the only place the layer above says
// out loud whether the client can be assumed to have the event.
func (s *Session) EmitDurable(eventType protocol.EventType, payload any, settle func(error)) {
	body, err := marshal(payload)
	if err != nil {
		settle(err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		settle(errors.New("fake: session is closed"))
		return
	}
	select {
	case s.events <- engine.Emission{Type: eventType, Payload: body, Settle: settle}:
	default:
		settle(errors.New("fake: nobody is reading the emissions"))
	}
}

// EmitStandIn publishes an emission that stands in for another one that may yet turn up,
// which is the shape a real engine uses for a placeholder. `claim` is asked by the
// publisher just before it writes, and a false drops the emission as a success.
func (s *Session) EmitStandIn(eventType protocol.EventType, payload any, claim func() bool, settle func(error)) {
	body, err := marshal(payload)
	if err != nil {
		settle(err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		settle(errors.New("fake: session is closed"))
		return
	}
	select {
	case s.events <- engine.Emission{Type: eventType, Payload: body, Claim: claim, Settle: settle}:
	default:
		settle(errors.New("fake: nobody is reading the emissions"))
	}
}

// EmitPerishable publishes an emission that stops being worth publishing, which is the
// shape a real engine uses for a moment rather than a fact. `fresh` is consulted by the
// publisher just before it writes.
func (s *Session) EmitPerishable(eventType protocol.EventType, payload any, expires func() time.Duration, settle func(error)) {
	body, err := marshal(payload)
	if err != nil {
		settle(err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		settle(errors.New("fake: session is closed"))
		return
	}
	select {
	case s.events <- engine.Emission{Type: eventType, Payload: body, Expires: expires, Settle: settle}:
	default:
		settle(errors.New("fake: nobody is reading the emissions"))
	}
}

func (s *Session) emit(eventType protocol.EventType, payload any) {
	body, err := marshal(payload)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.events <- engine.Emission{Type: eventType, Payload: body}:
	default:
		// A full buffer means nobody is reading, which for the fake means the test
		// stopped caring. Dropping is right here and would not be in a real engine,
		// where the reader is the publisher and blocking is what applies backpressure.
	}
}

var errNotConnected = errors.New("fake: session is not connected")

// NotConnected is the error the fake returns for a command that needs a live socket,
// exported so a test can assert on it rather than on a string.
func NotConnected() error { return errNotConnected }

func messageIDOf(command *protocol.Command) string {
	var payload struct {
		MessageID string `json:"message_id"`
	}
	if err := json.Unmarshal(command.Payload, &payload); err == nil && payload.MessageID != "" {
		return payload.MessageID
	}
	return "3EB0" + command.ID
}

func marshal(payload any) (json.RawMessage, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("fake: marshal payload: %w", err)
	}
	return body, nil
}
