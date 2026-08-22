// Package engine is the WhatsApp side, behind an interface.
//
// Everything above it is written against these two types and never against
// whatsmeow, which is what lets the whole connector be tested without a socket and
// what makes replacing the engine a change in one package.
package engine

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// ErrNotSupported is returned by an engine that does not implement a command. It is
// reported to the client as protocol.ErrorUnsupported.
var ErrNotSupported = errors.New("engine: command not supported")

// Emission is one thing the engine has to tell the client about. The engine names the
// type and renders the payload; stamping it (id, epoch, seq, instance) belongs to the
// session, which is the only thing that knows the ownership it is publishing under.
type Emission struct {
	Type    protocol.EventType
	Payload json.RawMessage
}

// Engine opens sessions. One process has one engine; a session is one WhatsApp
// account.
type Engine interface {
	// Open prepares the session for a device, resuming a stored one when there is
	// one. It does not connect: Session.Connect does, so that a session can be opened
	// and inspected without touching the network.
	Open(ctx context.Context, sid string) (Session, error)
	// Close releases whatever the engine holds process-wide.
	Close() error
}

// Session is one WhatsApp account as this connector holds it.
//
// Everything blocking takes a context. Nothing here retries: reconnection is a policy
// the session manager owns, because it is the only layer that knows whether this
// instance still holds the lease.
type Session interface {
	// Connect starts pairing or resumes a stored session. Progress arrives on Events,
	// not as a return value: pairing is a conversation, not a call.
	Connect(ctx context.Context, req ConnectRequest) error
	// Disconnect drops the socket and keeps the stored credentials.
	Disconnect(ctx context.Context) error
	// Logout ends the session on WhatsApp's side too, so resuming is impossible and
	// the next connect has to pair again.
	Logout(ctx context.Context) error
	// Execute carries out one command and returns the `result` half of the reply.
	// A nil result with a nil error is a command whose reply carries no data.
	Execute(ctx context.Context, command *protocol.Command) (json.RawMessage, error)
	// Events is closed when the session is done. Reading it is the only way to learn
	// what the engine has to say.
	Events() <-chan Emission
	// Close releases the session. Events is closed before Close returns, so a reader
	// draining it always terminates.
	Close() error
}

// ConnectRequest is `session.connect`, decoded.
type ConnectRequest struct {
	// Pairing is "qr", "code" or "resume".
	Pairing string `json:"pairing"`
	// Phone is required by code pairing and ignored otherwise.
	Phone string `json:"phone,omitempty"`
	Proxy string `json:"proxy,omitempty"`
}
