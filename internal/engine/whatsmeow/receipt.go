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
	if published.Chat.Kind == protocol.AddressGroup && !s.wantsGroups() {
		// The same boundary the message path draws, and for the same reason: the client
		// asked for direct chats only, so it never got the messages these are about and
		// could not apply them if it did. Acknowledged rather than withheld, because
		// nobody is ever going to want this one and withholding would have WhatsApp
		// redeliver every group receipt the account gets for as long as it is up.
		s.log.Debug().Str("receipt", string(event.Type)).
			Msg("dropping a group receipt the client did not subscribe to")
		return true
	}
	if s.publisherStalled() {
		// A publisher that just took the whole budget and answered nothing. Waiting for
		// it again is what puts a grouped receipt past the node watchdog, and the answer
		// is the same either way: the acknowledgement is withheld and WhatsApp sends the
		// receipt again.
		s.log.Debug().Str("receipt", string(event.Type)).
			Msg("withholding a receipt while the publisher is not answering")
		return false
	}
	delivered := s.deliver(protocol.EventMessageReceipt, published)
	s.publisherAnswered(delivered)
	return delivered
}

// publisherStalled reports whether the last receipt was left unpublished recently enough
// that the next one is not worth waiting on.
//
// The window is the delivery budget itself, so a publisher that has stopped answering
// costs one wait per budget rather than one per receipt, and a publisher that comes back
// is asked again as soon as the window is out. Bounded that way rather than by a flag,
// because a flag only cleared by a success is never cleared: nothing would try again.
// Measured against a monotonic base rather than the wall clock. UnixNano drops Go's
// monotonic reading, so a host whose clock steps backwards -- an NTP correction, a
// suspend, a container's clock being set -- would leave the deadline ahead for as long
// as wall time took to catch up, and every receipt would be withheld until it did.
func (s *Session) publisherStalled() bool {
	until := s.stalledUntil.Load()
	return until != 0 && s.since() < time.Duration(until)
}

func (s *Session) publisherAnswered(delivered bool) {
	if delivered {
		s.stalledUntil.Store(0)
		return
	}
	s.stalledUntil.Store(int64(s.since() + s.deliverWait))
}

func (s *Session) since() time.Duration {
	if s.elapsed != nil {
		return s.elapsed()
	}
	return time.Since(sinceStart)
}

// sinceStart is what the window is measured from: a reading taken once, carrying the
// monotonic clock, so every elapsed time derived from it is monotonic too.
var sinceStart = time.Now()

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
// The library says played is a view-once being opened. Against a real phone it also
// arrives coalesced with a chat being opened, over messages of its own, so what it
// covers is wider than the doc says. That changes nothing here, since both are the
// contract's `played`, and it is written down because the reading was narrower than the
// thing -- and because `sender` and the coalescing were readings too until a phone sent
// them.
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
	chat, addressable := addressOf(receiptChatJID(event))
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

// receiptChatJID is the chat a receipt belongs to, which is not always the one on it.
//
// A message somebody sent through a broadcast list is published under the direct chat
// with them rather than under the list, because that is where the recipient's own phone
// shows it -- and a receipt published under the list would be about a message the client
// filed somewhere else, so it could never be applied.
//
// The rule is the same and the field is not, which is why this is not chatOf. On a
// message the other party is the sender; on a receipt the sender is this account's own
// device and whatsmeow puts the peer in BroadcastListOwner, a field that exists for
// exactly this case and is empty in every other.
func receiptChatJID(event *waEvents.Receipt) waTypes.JID {
	if !event.BroadcastListOwner.IsEmpty() {
		return event.BroadcastListOwner
	}
	return event.Chat
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
	if authored[req.Chat.Kind] && sender.IsEmpty() {
		// whatsmeow puts `participant` on the node only when it has one, and a receipt
		// in a chat whose messages have an author of their own names nobody's message
		// without it. The write succeeds and the read mark lands nowhere, which is the
		// silent failure this whole path is written against.
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a read mark here has to name who wrote the messages")
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

	// Asked for before the mark rather than during it. whatsmeow reads the account's
	// read-receipt setting inside MarkRead and, when that query fails, carries on with an
	// empty one -- which is not `none`, so the receipt goes out as an ordinary read. An
	// account that turned read receipts off would have the read disclosed to the person
	// on the other side, and a read receipt cannot be taken back.
	//
	// Which is the line between this and the group's addressing, where a failed query is
	// carried on through: that one fails closed, and the worst of it is that nothing
	// happens and the caller marks again. This one fails open, and there is no marking
	// again.
	//
	// Only where it could change the answer. whatsmeow downgrades a read and not a
	// played, and downgrades a newsletter whatever the setting says, so asking in those
	// cases would refuse over a query whose answer was never going to be used.
	if kind == waTypes.ReceiptTypeRead && chat.Server != waTypes.NewsletterServer {
		known := s.privacyKnown
		if known == nil {
			known = s.privacyOverSocket
		}
		if err := known(ctx); err != nil {
			s.log.Warn().Err(err).Msg("refusing a read mark whose privacy setting could not be read")
			return nil, markFailure(err,
				"this account's read-receipt setting could not be read, and marking without it could disclose the read")
		}
	}

	if err := s.current().MarkRead(ctx, req.MessageIDs, time.Now(), chat, sender, kind); err != nil {
		s.log.Warn().Err(err).Str("chat", chat.String()).Msg("a read mark did not go out")
		return nil, markFailure(err, "WhatsApp did not take the read mark")
	}
	return nil, nil
}

func (s *Session) privacyOverSocket(ctx context.Context) error {
	_, err := s.current().TryFetchPrivacySettings(ctx, false)
	return err
}

// markFailure names what went wrong in the contract's own words.
//
// The library's text does not cross into a reply: it is noise to whoever reads it and a
// description of this deployment's insides to whoever does not. And the codes are what a
// caller branches on -- told `wa_error` for a disconnect, a client retries against
// WhatsApp instead of waiting for the session to come back.
func markFailure(err error, refused string) error {
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
		return protocol.NewError(protocol.ErrorWaError, refused)
	}
}

// authored are the chats whose messages have an author the chat itself does not name, so
// a receipt in one is about somebody rather than about the chat.
//
// It is whatsmeow's own line, read off MarkRead: it puts `participant` on the node for
// every chat but a direct one, and drops it silently for a direct one whether or not a
// sender was given. A channel is left out on purpose -- a post's author is the channel,
// so the participant would repeat the chat and requiring it would refuse a mark that
// works.
var authored = map[protocol.AddressKind]bool{
	protocol.AddressGroup:     true,
	protocol.AddressBroadcast: true,
	protocol.AddressStatus:    true,
}

// markReadKinds is the contract's two, with the empty string among them because `type`
// is optional and a read is what a mark with none means.
var markReadKinds = map[string]waTypes.ReceiptType{
	"":       waTypes.ReceiptTypeRead,
	"read":   waTypes.ReceiptTypeRead,
	"played": waTypes.ReceiptTypePlayed,
}
