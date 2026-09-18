// Package transport moves frames between the connector and its clients.
//
// The interfaces are here and the wiring is in a sub-package so the rest of the
// connector never imports a Redis type. Redis Streams is the only transport in v1; the
// standalone HTTP mode planned for later is another implementation of these three,
// which is the whole reason they are interfaces this early.
package transport

import (
	"context"
	"errors"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// ErrWindowSpent is what a read returns when the window it was given ran out with the
// answer still on its way: the deadline working, not the read failing.
//
// A sentinel of its own rather than `context.DeadlineExceeded`, because that value has
// other producers -- a dial that gives up on its own timeout carries it too -- and a
// caller that suppressed every one of them would swallow a Redis it never reached.
var ErrWindowSpent = errors.New("transport: the window ran out with the answer still on its way")

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
	// Forfeit is Release for a command this instance took its turn at and could not
	// carry out. Both leave it pending; what differs is where it comes back.
	//
	// A reclaimed entry has the age the claim erased put back by Release, so a peer can
	// take it at once rather than waiting the delay a second time. An entry that keeps
	// its age is the oldest one on every pass, though, so an instance that fails on it
	// takes it first again next pass, and again — and the entries behind it, which is
	// every other wake, never get their turn. Forfeit gives that place up: the entry
	// waits out the reclaim delay like a command whose instance died, which is what one
	// this instance tried and could not run has come to resemble.
	//
	// Nil where the delivery has no age to give up, and then Release is the whole story.
	Forfeit func()
	// Redelivered says this command was taken over rather than read for the first time:
	// its previous holder was killed, or lost the session, or its acknowledgement never
	// landed. A transport that cannot tell the two apart leaves it false.
	//
	// It exists because an answer that ends a command is only an answer while somebody
	// is still listening for it. A caller that sent a command a moment ago is waiting on
	// its reply; a caller whose command has been round the pending list since another
	// instance died is not, and its reply list may not even exist any more. Refusing the
	// first is backpressure the caller acts on. Refusing the second retires the only copy
	// of a command nobody ever ran and nobody hears about.
	Redelivered bool
	// DeliveredBefore says this entry came out of the consumer group's pending list
	// rather than arriving new, whatever its age. It is a fact about the entry, not a
	// judgement about it. Two things read it: the observability in internal/app counts a
	// delivery that came back, and `wake` declines to make the same adoption attempt again
	// while the fleet is leaving that account alone. Both want the same thing from it --
	// "has this been handed out before" -- and neither wants the narrower question below.
	//
	// Redelivered above is the narrower question and answers a different one: has this
	// been round long enough that its sender has probably stopped listening. The two
	// come apart exactly where it matters. A wake the fleet cannot act on is handed back
	// unrun and read out of the history again on the very next block, so its idle never
	// grows: measured on 2771941 it came round about two and a half times a heartbeat,
	// twenty times in forty-five seconds, with the idle never reaching two seconds
	// against a fifteen-second claim delay. Every one of those has Redelivered false, and
	// every one of them is the fleet doing the same work again.
	DeliveredBefore bool
	// TakenFrom names the consumer that was holding this entry when a claim took it
	// back, and is empty for one that was read rather than claimed. It is the difference
	// between "the fleet is busy" and "one instance took commands and stopped answering",
	// which is a question nothing else here can answer: the instance that went quiet is
	// precisely the one whose own metrics nobody is reading.
	TakenFrom string
	// Deliveries is how many times Redis said this entry had been handed out when the
	// claim listed it, not counting the claim itself, and is zero where that could not
	// be read. Only a claim can know it: it comes from XPENDING, and a read never asks.
	//
	// Up to the claim and no further, because past that the number would be a guess
	// about a command still on the wire. XCLAIM increments the delivery counter, so
	// adding one for the claim about to happen is right exactly when the claim happens
	// once -- and go-redis sends a command again when its answer never arrives, which is
	// the failure this instrument exists to make visible. Measured on a real Redis with
	// the claim's answer cut: XCLAIM ran twice, the counter went from 1 to 3, and the
	// caller was handed a successful claim with no sign that anything had been retried.
	Deliveries int64
	// Internal says this command came from the connector rather than from a client: the
	// resume sweep synthesises a `session.connect` for an account whose owner went away,
	// and nothing else does.
	//
	// What it changes is who is told when it fails. A command a client sent and is not
	// waiting on is answered with `command.failed`, which is the only way its sender ever
	// hears about it; one nobody sent has no sender, and publishing the event anyway
	// would put a command id no client has ever seen on the stream. The engine's own
	// events -- the connection state, the connect failure -- are what report a resume
	// either way, and those are addressed to the session rather than to a command.
	Internal bool
}

// CommandWaker puts a command on the control stream, which is the one stream this
// connector writes to that a client otherwise owns.
type CommandWaker interface {
	// Wake puts a `session.wake` for one session on the control stream, which is how an
	// instance that has just let a session go tells the fleet there is an account to pick
	// up. Releasing a lease only makes an account takeable: without this the peers find
	// out on their next resume pass, a whole interval later, and a rolling deploy leaves
	// every account it moves unowned for that interval (#268).
	Wake(ctx context.Context, sid string) error
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
	CommandWaker
	CommandReader
	Replier
}
