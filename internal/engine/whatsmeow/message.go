package whatsmeow

import (
	"encoding/json"
	"time"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// deliverTimeout is the default deliverWait: how long an inbound message waits for its
// event to be published before the session gives up and leaves it unacknowledged.
//
// whatsmeow hands one node to its handlers at a time and waits five minutes before it
// decides a handler is stuck and starts the next node alongside it. Staying well under
// that is what keeps a session's messages in the order WhatsApp sent them: the moment
// two node handlers run at once, the order they publish in is the scheduler's to
// decide. Giving up sooner costs a redelivery, which is the trade this whole path is
// built around.
const deliverTimeout = 60 * time.Second

// addressOf renders a JID the way the contract carries an address, and reports whether
// it is one the contract can name at all. Everything WhatsApp addresses that is not in
// the enum — a bot, an interop bridge, the Messenger servers — is refused here rather
// than guessed at, because a client cannot act on a kind it does not know and an
// invented one reads as a real account.
func addressOf(jid waTypes.JID) (protocol.Address, bool) {
	if jid.User == "" {
		return protocol.Address{}, false
	}
	if jid.IsBot() {
		// Meta's own assistants, which reach an account on the ordinary phone server
		// under a reserved range as well as on the bot one. Mapping the legacy form by
		// its server alone hands a client a phone number it can open a conversation
		// with and reply to, for something that is not a person.
		return protocol.Address{}, false
	}
	switch jid.Server {
	case waTypes.DefaultUserServer, waTypes.LegacyUserServer, waTypes.HostedServer:
		return protocol.Address{Kind: protocol.AddressPhone, ID: jid.User}, true
	case waTypes.HiddenUserServer, waTypes.HostedLIDServer:
		return protocol.Address{Kind: protocol.AddressLID, ID: jid.User}, true
	case waTypes.GroupServer:
		return protocol.Address{Kind: protocol.AddressGroup, ID: jid.User}, true
	case waTypes.NewsletterServer:
		return protocol.Address{Kind: protocol.AddressNewsletter, ID: jid.User}, true
	case waTypes.BroadcastServer:
		// The status feed is a broadcast list like any other as far as WhatsApp is
		// concerned, and nothing like one as far as a client is: it is the stories
		// timeline, not a conversation. The contract gives it its own kind so a client
		// can drop it without pattern-matching on a magic user.
		if jid.User == waTypes.StatusBroadcastJID.User {
			return protocol.Address{Kind: protocol.AddressStatus, ID: jid.User}, true
		}
		return protocol.Address{Kind: protocol.AddressBroadcast, ID: jid.User}, true
	default:
		return protocol.Address{}, false
	}
}

// partyOf names who sent a message, carrying both of WhatsApp's identifiers whenever
// both are on the event. Which one arrives as the sender and which as the alternative
// depends on the chat's addressing mode, so neither is the one to build on: a client
// that only ever stored the phone number still has to recognise the LID as the same
// person the first time a chat switches.
func partyOf(info *waTypes.MessageInfo) (*protocol.Party, bool) {
	party := protocol.Party{PushName: info.PushName}
	if info.VerifiedName != nil {
		party.VerifiedName = info.VerifiedName.Details.GetVerifiedName()
	}
	for _, jid := range []waTypes.JID{info.Sender, info.SenderAlt} {
		switch address, ok := addressOf(jid); {
		case !ok:
		case address.Kind == protocol.AddressPhone && party.Phone == "":
			party.Phone = address.ID
		case address.Kind == protocol.AddressLID && party.LID == "":
			party.LID = address.ID
		}
	}
	if party.Phone != "" || party.LID != "" {
		return &party, true
	}

	// Nobody to name, and the two reasons this happens could not be further apart.
	if info.Sender == info.Chat {
		// The chat posting as itself, which is what a newsletter is: whatsmeow sets the
		// sender to the channel's own JID, and a channel is not a person in a
		// conversation. The contract makes `sender` nullable for exactly this.
		return nil, true
	}
	// WhatsApp named somebody the contract has no way to describe. Leaving the field
	// off would publish the message anyway, and a client reading an unattributed
	// message in a direct chat takes it for the person on the other side of it, which
	// puts somebody else's words in their mouth.
	return nil, false
}

// inboundOf renders an inbound message the way the contract carries it, and reports
// whether this build can carry it at all. A false is not an error: it is this
// milestone saying the message is somebody else's to deliver, which upstream turns
// into a withheld acknowledgement, so WhatsApp keeps it and sends it again.
func inboundOf(event *waEvents.Message) (protocol.InboundMessage, bool) {
	if event.IsEdit || event.Info.Edit != waTypes.EditAttributeEmpty || newsletterEdit(event) {
		// whatsmeow unwraps an edit and hands back the corrected text in the shape a
		// new message arrives in, so nothing further down can tell the two apart. The
		// contract has `message.edited` for this and M2 has yet to reach it: publishing
		// it as a message received would either duplicate the original in the
		// conversation or be deduplicated away, and either way the correction is lost
		// and the acknowledgement spends the one redelivery it had.
		return protocol.InboundMessage{}, false
	}
	chatJID := event.Info.Chat
	if event.Info.IsIncomingBroadcast() {
		// Somebody sent this through a broadcast list, and WhatsApp shows it to the
		// recipient in the direct chat with whoever sent it, not under the list.
		// whatsmeow says so on the event, and addressing the list instead sends the
		// message to a chat the client does not open conversations for, after
		// acknowledging it: a message the recipient can see on their own phone and
		// nowhere else. The status feed is not a broadcast list and is not touched.
		chatJID = event.Info.Sender
	}
	chat, ok := addressOf(chatJID)
	if !ok || event.Info.ID == "" {
		return protocol.InboundMessage{}, false
	}
	var sender *protocol.Party
	if !event.Info.IsFromMe {
		// An echo carries no sender, which is what the contract's own fixture for one
		// says and what the client reads: `from_me` is the whole answer to who sent it,
		// and naming the account itself there files the operator's own number as the
		// party in a conversation with somebody else.
		var named bool
		if sender, named = partyOf(&event.Info); !named {
			return protocol.InboundMessage{}, false
		}
	}
	body, context, ok := textOf(event.Message)
	if !ok {
		return protocol.InboundMessage{}, false
	}

	message := protocol.InboundMessage{
		ID:        event.Info.ID,
		Chat:      chat,
		Sender:    sender,
		FromMe:    event.Info.IsFromMe,
		Timestamp: event.Info.Timestamp.UnixMilli(),
		Content:   protocol.Text(body),
		QuotedID:  context.GetStanzaID(),
		Mentions:  mentionsOf(context),
		Ephemeral: context.GetExpiration(),
	}
	return message, true
}

// newsletterEdit reports whether a newsletter post is a correction of an earlier one.
//
// A channel's edit does not arrive wrapped the way an ordinary one does: whatsmeow says
// so on the type itself, and what comes through is the new body under the original
// message's id, with the only sign of it being a timestamp off to the side. Published
// as a message received, a client that deduplicates on the id throws the correction
// away and the acknowledgement makes sure it is never sent again.
func newsletterEdit(event *waEvents.Message) bool {
	return event.NewsletterMeta != nil && !event.NewsletterMeta.EditTS.IsZero()
}

// textOf pulls the body and the context out of the two shapes WhatsApp sends text in.
// A message with neither is one of the many types M2 has yet to reach, and saying so
// is the caller's cue to leave it on the phone.
func textOf(message *waE2E.Message) (string, *waE2E.ContextInfo, bool) {
	if extended := message.GetExtendedTextMessage(); extended != nil {
		// The shape WhatsApp uses the moment a message has anything attached to it: a
		// quote, a mention, a link preview. The plain one below carries none of that.
		return extended.GetText(), extended.GetContextInfo(), true
	}
	if body := message.GetConversation(); body != "" {
		return body, nil, true
	}
	return "", nil, false
}

// mentionsOf renders the addresses a message tagged. An unparseable or unnameable one
// is dropped rather than refused: a mention is an annotation on the body, and losing
// one is not worth withholding the message it annotates.
func mentionsOf(context *waE2E.ContextInfo) []protocol.Address {
	mentioned := context.GetMentionedJID()
	if len(mentioned) == 0 {
		return nil
	}
	mentions := make([]protocol.Address, 0, len(mentioned))
	for _, raw := range mentioned {
		jid, err := waTypes.ParseJID(raw)
		if err != nil {
			continue
		}
		if address, ok := addressOf(jid); ok {
			mentions = append(mentions, address)
		}
	}
	if len(mentions) == 0 {
		return nil
	}
	return mentions
}

// receive publishes an inbound message and reports whether WhatsApp may be told the
// account has it. It is the one handler that waits: everything else this session
// emits describes something that already happened, while this one is the reason the
// message is allowed to leave the phone.
func (s *Session) receive(event *waEvents.Message) bool {
	if event.Info.Sender.IsBot() || event.Info.Chat.IsBot() {
		// Meta's assistants, either in a chat of their own or replying inline in
		// somebody else's. The contract has no kind for them on purpose, so no slice of
		// this milestone is going to deliver one: acknowledged rather than refused,
		// because a chat with an assistant would otherwise redeliver for good.
		s.log.Debug().Str("message_id", event.Info.ID).Msg("dropping a message from one of Meta's own bots")
		return true
	}

	if chat, named := addressOf(event.Info.Chat); named && chat.Kind == protocol.AddressGroup && !s.wantsGroups() {
		// Acknowledged and published nowhere, which is the opposite of what happens to
		// a message this build cannot render. The client asked for direct chats only,
		// so nobody is ever going to want this one, and withholding the acknowledgement
		// would have WhatsApp redeliver every group message the account ever receives
		// for as long as the session is up.
		s.log.Debug().Str("message_id", event.Info.ID).
			Msg("dropping a group message the client did not subscribe to")
		return true
	}

	message, ok := inboundOf(event)
	if !ok {
		s.log.Debug().Str("message_id", event.Info.ID).
			Msg("refusing to acknowledge an inbound message this build cannot publish")
		return false
	}
	return s.deliver(protocol.EventMessageReceived, map[string]any{"message": message})
}

// deliver emits and waits for the publisher to say what became of it.
//
// A failure here is reported as a failure to whatsmeow, which withholds the
// acknowledgement and keeps the decrypted plaintext buffered, so the redelivery can be
// read without the sender being asked to encrypt it again. The one thing it cannot
// promise is that a message published exactly once is acknowledged: a publish that
// succeeds after this gave up on it is delivered to the client and redelivered by
// WhatsApp afterwards. Which is why the client deduplicates on the message id, and why
// this way round is the right one — a duplicate is a nuisance, a lost message is not.
func (s *Session) deliver(eventType protocol.EventType, payload any) bool {
	body, err := json.Marshal(payload)
	if err != nil {
		s.log.Error().Err(err).Str("type", string(eventType)).Msg("failed to render an event payload")
		return false
	}

	// Buffered, because the settle callback runs on the pump's goroutine and must not
	// wait on a reader that has already given up.
	settled := make(chan error, 1)
	emission := engine.Emission{
		Type:    eventType,
		Payload: body,
		Settle:  func(err error) { settled <- err },
	}
	// Started before the emission is queued, not after: the inbox is bounded, and a
	// pump stalled behind a publisher that answers neither way fills it. Timing only
	// the publish would leave the handler waiting here with nothing to release it,
	// which is the state the bound exists to keep whatsmeow out of.
	timeout := time.NewTimer(s.deliverWait)
	defer timeout.Stop()
	select {
	case s.inbox <- emission:
	case <-timeout.C:
		s.log.Warn().Str("type", string(eventType)).Dur("waited", s.deliverWait).
			Msg("withholding an acknowledgement for an event that could not be queued")
		return false
	case <-s.done:
		return false
	}

	select {
	case err := <-settled:
		if err != nil {
			s.log.Warn().Err(err).Str("type", string(eventType)).
				Msg("withholding an acknowledgement for an event that was not published")
			return false
		}
		return true
	case <-timeout.C:
		s.log.Warn().Str("type", string(eventType)).Dur("waited", s.deliverWait).
			Msg("withholding an acknowledgement for an event that took too long to publish")
		return false
	case <-s.done:
		return false
	}
}
