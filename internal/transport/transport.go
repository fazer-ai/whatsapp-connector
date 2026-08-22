// Package transport moves frames between the connector and its clients.
//
// The interfaces are here and the wiring is in a sub-package so the rest of the
// connector never imports a Redis type. Redis Streams is the only transport in v1; the
// standalone HTTP mode planned for later is another implementation of these three,
// which is the whole reason they are interfaces this early.
package transport

import (
	"context"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// Publisher hands an event to the clients.
//
// Publishing is what makes an event real: an inbound message is acknowledged to
// WhatsApp only after this returns, so a failure here costs a redelivery rather than
// the message.
type Publisher interface {
	Publish(ctx context.Context, event *protocol.Event) error
}

// Delivery is one command as received, with the acknowledgement still owed.
type Delivery struct {
	Command protocol.Command
	// Ack removes the command from the pending list of the consumer group. It is
	// called after the command has been carried out (or definitively refused), never
	// before: an un-acked command is one another instance can claim after a crash.
	Ack func(context.Context) error
}

// CommandReader delivers the commands addressed to the sessions this instance owns,
// plus the ones addressed to no session in particular.
type CommandReader interface {
	// Read blocks until at least one command arrives, the context ends, or the block
	// interval elapses. An empty slice with a nil error means "nothing this round",
	// which is the ordinary case and not a failure.
	Read(ctx context.Context, sids []string) ([]Delivery, error)
	// Claim takes over commands left pending by an instance that stopped. It is what
	// makes a command survive the death of the instance that was about to run it.
	Claim(ctx context.Context, sids []string) ([]Delivery, error)
}

// Replier answers an RPC command.
type Replier interface {
	Reply(ctx context.Context, replyTo string, reply protocol.Reply) error
}

// Transport is the three of them together, which is what a running connector needs.
type Transport interface {
	Publisher
	CommandReader
	Replier
}
