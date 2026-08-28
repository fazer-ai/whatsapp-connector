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
	if existing, ok := e.sessions[sid]; ok {
		return existing, nil
	}
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

	mu        sync.Mutex
	events    chan engine.Emission
	closed    bool
	connected bool
	loggedOut int
	commands  []protocol.Command
	held      chan struct{}

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

// Execute records the command and answers the shapes the contract's result table
// names. Anything it does not know is refused rather than answered with a guess: a
// fake that invents a result shape is a test that passes against a contract nobody
// implements.
func (s *Session) Execute(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	s.mu.Lock()
	s.commands = append(s.commands, *command)
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
