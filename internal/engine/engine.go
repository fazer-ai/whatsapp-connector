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
	// Settle, when it is set, is called exactly once with the outcome of publishing
	// this emission: nil once the client can be assumed to have it, an error when it
	// never reached the stream. It is what lets an engine hold WhatsApp's own
	// acknowledgement back until the event is durable, which is the whole difference
	// between losing Redis costing a redelivery and losing Redis costing a message.
	// An emission that leaves it nil is published like any other and nobody waits.
	Settle func(error)
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
// Everything blocking takes a context. An engine may recover its own socket, and the
// whatsmeow one has to: WhatsApp closes the stream in the middle of pairing and
// expects the client back, so a client that never reconnects never finishes pairing.
// What an engine must not do is decide whether this instance still owns the account.
// That is the layer above, which holds the lease and closes the session the moment it
// is gone.
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
	// DeviceName is what the account's linked-devices list shows. The whatsmeow engine
	// cannot honour it per session: the library keeps device properties process-wide,
	// so the deployment's own name is used instead.
	DeviceName string `json:"device_name,omitempty"`
	// Proxy is an object in the contract, not a string: decoding it as one made every
	// connect carrying a proxy fail to parse before it reached an engine.
	Proxy *ProxyRequest `json:"proxy,omitempty"`
	// Groups asks for group chats alongside direct ones. Nothing selects on it yet
	// because nothing is delivered yet: M2 is what brings conversation traffic, and
	// until then this chooses between nothing and nothing.
	Groups bool `json:"groups,omitempty"`
	// HistorySync asks for the backlog the phone holds. Honouring it is M6.
	HistorySync bool `json:"history_sync,omitempty"`
	// Calls is the call half of `session.connect`.
	Calls *CallsRequest `json:"calls,omitempty"`
}

// CallsRequest is what a session asks the connector to do about incoming calls.
// Honouring it is M3.
type CallsRequest struct {
	// AutoReject has the connector refuse an incoming call rather than let it ring.
	AutoReject bool `json:"auto_reject,omitempty"`
}

// ProxyRequest is the proxy half of `session.connect`. Honouring it is M5; parsing it
// is here so a client that sends one is not answered with invalid_payload.
type ProxyRequest struct {
	URL string `json:"url,omitempty"`
}
