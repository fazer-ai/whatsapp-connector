package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// receipt publishes what became of messages that already exist, and reports whether
// WhatsApp may acknowledge the node.
//
// Through deliver rather than emit, on the same terms a message is: a receipt nobody
// published is a tick that never turns, and the client has no way to ask for it again.
// Withholding the acknowledgement costs a redelivery and a duplicate the client
// deduplicates; acknowledging one that was never published costs it for good.
func (s *Session) receipt(event *waEvents.Receipt) bool {
	published, ok := receiptOf(event)
	if !ok {
		// Not a name the contract has. Dropped rather than withheld, because withholding
		// buys a redelivery of a receipt no build will ever publish, and the node would
		// come back for as long as the session is up.
		s.log.Debug().Str("receipt", string(event.Type)).Int("messages", len(event.MessageIDs)).
			Msg("dropping a receipt the contract does not name")
		return true
	}
	return s.deliver(protocol.EventMessageReceipt, published)
}

// receiptOf maps whatsmeow's receipt onto the contract's four, and reports whether the
// contract has a name for it at all.
//
// The eleven types were read off types/presence.go rather than guessed at, and the
// mapping is the whole decision in this file:
//
//	""             -> delivered   the message reached the device
//	read           -> read        the chat was opened
//	read-self      -> read        this account read it from another of its devices
//	played         -> played      a view-once was opened, either way round
//	played-self    -> played      the same, from another of this account's devices
//	server-error   -> failed      WhatsApp could not deliver it
//	sender         -> dropped     one of this account's own devices got a copy of a send
//	retry          -> dropped     the recipient could not decrypt and whatsmeow resends
//	inactive       -> dropped     plumbing
//	peer_msg       -> dropped     plumbing
//	hist_sync      -> dropped     plumbing
//
// The two `-self` shapes are the ones worth arguing about, and they are published for
// what they cost either way: dropping them means a client can never learn that the
// person behind the account read the conversation on their phone, and there is nothing
// it could ask to find out. Publishing them is not ambiguous, because `participant`
// says whose device reported it and a client knows its own number.
//
// `retry` is not `failed`. It says the recipient's device could not decrypt the message
// and is asking for it again, which whatsmeow answers on its own; a client told the
// message failed would show an error for something that is about to arrive.
func receiptOf(event *waEvents.Receipt) (protocol.MessageReceipt, bool) {
	kind, named := receiptKinds[event.Type]
	if !named || len(event.MessageIDs) == 0 {
		return protocol.MessageReceipt{}, false
	}
	chat, addressable := addressOf(event.Chat)
	if !addressable {
		return protocol.MessageReceipt{}, false
	}

	published := protocol.MessageReceipt{
		Chat:       chat,
		MessageIDs: event.MessageIDs,
		Type:       kind,
		Timestamp:  event.Timestamp.UnixMilli(),
	}
	if participant, addressable := addressOf(event.Sender); addressable {
		published.Participant = &participant
	}
	if kind == protocol.ReceiptFailed {
		// Everything whatsmeow knows, which is that WhatsApp answered with an error and
		// nothing about which. Inventing a retryable flag out of that would be a claim
		// this connector cannot support.
		published.Error = &protocol.Error{
			Code:    protocol.ErrorWaError,
			Message: "whatsapp reported a server error for this message",
		}
	}
	return published, true
}

var receiptKinds = map[waTypes.ReceiptType]protocol.ReceiptKind{
	waTypes.ReceiptTypeDelivered:   protocol.ReceiptDelivered,
	waTypes.ReceiptTypeRead:        protocol.ReceiptRead,
	waTypes.ReceiptTypeReadSelf:    protocol.ReceiptRead,
	waTypes.ReceiptTypePlayed:      protocol.ReceiptPlayed,
	waTypes.ReceiptTypePlayedSelf:  protocol.ReceiptPlayed,
	waTypes.ReceiptTypeServerError: protocol.ReceiptFailed,
}

type markReadRequest struct {
	Chat       protocol.Address  `json:"chat"`
	MessageIDs []string          `json:"message_ids"`
	Sender     *protocol.Address `json:"sender"`
	Type       string            `json:"type"`
}

// markRead is `message.mark_read`: the account saying it has seen messages somebody
// else sent.
//
// The sender is what a group needs and a direct chat does not: WhatsApp routes the
// receipt to whoever wrote the message, and in a group that is not the chat. An empty
// JID is how whatsmeow is told there is nobody else to name.
func (s *Session) markRead(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req markReadRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a read mark has to say which messages it is on, and in which chat")
	}
	if len(req.MessageIDs) == 0 {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a read mark has to name at least one message")
	}
	if slices.Contains(req.MessageIDs, "") {
		// The length check above passes a list of empty strings, and whatsmeow builds
		// the receipt around ids[0] without looking at it. The node goes out naming no
		// message, sendNode reports no error, and the command is acknowledged and
		// remembered under its idempotency key -- so the retry that would have worked
		// never happens.
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a read mark cannot name a message with no id")
	}
	kind, named := markReadKinds[req.Type]
	if !named {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a read mark is either a read or a played, and nothing else")
	}
	chat, err := jidOf(req.Chat)
	if err != nil {
		return nil, err
	}
	sender := waTypes.EmptyJID
	if req.Sender != nil {
		if sender, err = jidOf(*req.Sender); err != nil {
			return nil, err
		}
	}
	if chat.Server == waTypes.GroupServer && sender.IsEmpty() {
		// whatsmeow puts `participant` on the node only when it has one, and a group
		// receipt without it names nobody's message. The write succeeds and the read
		// mark lands nowhere, which is the silent failure this whole path is written
		// against.
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a read mark in a group has to name who wrote the messages")
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}
	// A group answers in one namespace or the other, and a client holding the address
	// from an older event may well have the other one. Sent as it came, the participant
	// is one WhatsApp cannot resolve -- and, as above, nothing says so.
	if sender, err = s.asTheGroupAddresses(ctx, chat, sender); err != nil {
		return nil, err
	}

	if err := s.current().MarkRead(ctx, req.MessageIDs, time.Now(), chat, sender, kind); err != nil {
		s.log.Warn().Err(err).Str("chat", chat.String()).Msg("a read mark did not go out")
		return nil, markFailure(err)
	}
	return nil, nil
}

// markFailure names what went wrong in the contract's own words.
//
// The library's text does not cross into a reply: it is noise to whoever reads it and a
// description of this deployment's insides to whoever does not. And the codes are what a
// caller branches on -- told `wa_error` for a disconnect, a client retries against
// WhatsApp instead of waiting for the session to come back.
func markFailure(err error) error {
	switch {
	case errors.Is(err, wm.ErrNotLoggedIn):
		return protocol.NewError(protocol.ErrorNotPaired,
			"the session has no WhatsApp account to mark anything read from")
	case errors.Is(err, wm.ErrNotConnected):
		return protocol.NewError(protocol.ErrorNotConnected,
			"the session is not connected to WhatsApp")
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled),
		errors.Is(err, wm.ErrIQTimedOut):
		return protocol.NewError(protocol.ErrorTimeout,
			"the read mark did not go out before the command's deadline")
	default:
		return protocol.NewError(protocol.ErrorWaError, "WhatsApp did not take the read mark")
	}
}

// markReadKinds is the contract's two, with the empty string among them because `type`
// is optional and a read is what a mark with none means.
var markReadKinds = map[string]waTypes.ReceiptType{
	"":       waTypes.ReceiptTypeRead,
	"read":   waTypes.ReceiptTypeRead,
	"played": waTypes.ReceiptTypePlayed,
}
