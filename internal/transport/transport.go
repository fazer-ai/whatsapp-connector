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
	// Release says this instance is walking away from the command without having
	// carried it out, and without acknowledging it: a wake it could not act on, a
	// command for a session it does not own. The command stays pending and becomes
	// claimable again, by any instance including this one.
	//
	// It exists because "still being carried out here" and "left behind here" are
	// different states that a consumer group cannot tell apart: both are entries
	// pending under this instance's name. Reclaiming the first duplicates a command
	// that is still running; never reclaiming the second loses it for good in a fleet
	// of one.
	Release func()
}

// CommandReader delivers the commands addressed to the sessions this instance owns,
// plus the ones addressed to no session in particular.
type CommandReader interface {
	// Read blocks until at least one command arrives, the context ends, or the block
	// interval elapses. An empty slice with a nil error means "nothing this round",
	// which is the ordinary case and not a failure.
	Read(ctx context.Context, sids []string) ([]Delivery, error)
	// Claim takes over commands left pending on these sessions' own streams by an
	// instance that stopped. It is what makes a command survive the death of the
	// instance that was about to run it.
	Claim(ctx context.Context, sids []string) ([]Delivery, error)
	// ClaimControl is Claim over the stream addressed to no session in particular. It
	// is separate because a wake is what puts a session from a dead instance back on an
	// instance: sharing a call with the session streams has it taken last, under
	// whatever deadline they left, and released along with them when any one of them
	// fails.
	ClaimControl(ctx context.Context) ([]Delivery, error)
	// ClaimSessions is Claim over these sessions' own streams and nothing else. It is
	// what a session just adopted needs before anything newer is read for it, so that a
	// command its previous owner abandoned is not overtaken by one that arrived later.
	ClaimSessions(ctx context.Context, sids []string) ([]Delivery, error)
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
