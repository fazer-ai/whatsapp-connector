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
	"sync"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// QRData is the pairing image the fake issues. A real data URL rather than a
// placeholder, so a client rendering it shows something.
const QRData = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUg=="

// PairingCode is the code the fake issues for code pairing.
const PairingCode = "K7QP2M4X"

// PairedPhone is the number the fake reports having paired.
const PairedPhone = "5511999990001"

// Engine hands out fake sessions and remembers them, so a test can reach into one it
// has already handed to the layer under test.
type Engine struct {
	mu       sync.Mutex
	sessions map[string]*Session
	closed   bool
}

// New returns an engine with no sessions open.
func New() *Engine { return &Engine{sessions: make(map[string]*Session)} }

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

	mu           sync.Mutex
	events       chan engine.Emission
	closed       bool
	connected    bool
	finished     bool
	givenUp      uint64
	loggedOut    int
	deleted      int
	refuseUnlink error
	failDelete   error
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

// Connect walks the pairing conversation the type asks for and ends `open`.
func (s *Session) Connect(_ context.Context, req engine.ConnectRequest) error {
	switch req.Pairing {
	case "qr":
		s.emit(protocol.EventPairingQR, map[string]any{"png_data_url": QRData, "expires_in_ms": 20000})
	case "code":
		if req.Phone == "" {
			return errors.New("fake: code pairing needs a phone")
		}
		s.emit(protocol.EventPairingCode, map[string]any{"code": PairingCode, "phone": req.Phone})
	case "resume":
	default:
		return fmt.Errorf("fake: unknown pairing mode %q", req.Pairing)
	}

	if req.Pairing != "resume" {
		// `phone`, not an address: the schema requires the digits at the top level, and
		// a fake that publishes a shape the contract rejects is an end-to-end check that
		// proves the client would refuse the real thing.
		s.emit(protocol.EventPairingSuccess, map[string]any{
			"phone":    PairedPhone,
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

// Disconnect drops the socket and says so.
func (s *Session) Disconnect(_ context.Context) error {
	s.mu.Lock()
	s.connected = false
	s.mu.Unlock()
	s.emit(protocol.EventSessionState, map[string]any{"state": "close", "reason": "disconnect_requested"})
	return nil
}

// Logout ends the session for good.
func (s *Session) Logout(_ context.Context) error {
	s.mu.Lock()
	s.connected = false
	s.loggedOut++
	s.mu.Unlock()
	s.emit(protocol.EventSessionLoggedOut, map[string]any{"reason": "logout_requested"})
	return nil
}

// Delete unlinks and forgets, and counts the two separately so a test can tell a
// teardown that gave up from one that carried on: the whole point of the real one is
// that a refused unlink does not stop the deletion.
func (s *Session) Delete(_ context.Context) error {
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
			state["phone_number"] = PairedPhone
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
