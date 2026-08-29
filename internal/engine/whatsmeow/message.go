package whatsmeow

import (
	"context"
	"encoding/json"
	"time"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/reflect/protoreflect"

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
	naming(&party, info.Sender, info.SenderAlt)
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

// body is what a message says, in the terms the contract carries it in.
type body struct {
	content any
	context *waE2E.ContextInfo
	// failure is why this message's file is not coming, empty when there is nothing to
	// say. It is announced after the message, never instead of it.
	failure string
}

// renderBody turns the message WhatsApp sent into the body the contract carries, and
// reports whether this build can carry it at all.
//
// It is an argument to inboundOf rather than a call inside it because rendering the
// body of a media message means fetching the file over the network, and a message the
// envelope around it is going to refuse must not cost a download first.
//
// It takes the whole event and not just the message, because some of what decides how a
// body is rendered is what whatsmeow found around it rather than what is left inside:
// a view-once wrapper is unwrapped before a handler sees it, and only the flag on the
// event says it was ever there.
type renderBody func(*waEvents.Message) (body, bool)

// inboundOf renders an inbound message the way the contract carries it, and reports
// whether this build can carry it at all. A false is not an error: it is this
// milestone saying the message is somebody else's to deliver, which upstream turns
// into a withheld acknowledgement, so WhatsApp keeps it and sends it again.
//
// The second return is the failure to announce once the message itself is out, empty
// for a message with nothing missing.
func inboundOf(event *waEvents.Message, render renderBody) (protocol.InboundMessage, string, bool) {
	chat, ok := chatOf(event)
	if !ok || event.Info.ID == "" {
		return protocol.InboundMessage{}, "", false
	}
	var sender *protocol.Party
	if !event.Info.IsFromMe {
		// An echo carries no sender, which is what the contract's own fixture for one
		// says and what the client reads: `from_me` is the whole answer to who sent it,
		// and naming the account itself there files the operator's own number as the
		// party in a conversation with somebody else.
		var named bool
		if sender, named = partyOf(&event.Info); !named {
			return protocol.InboundMessage{}, "", false
		}
	}
	// Asked for last, once every question about the envelope has been answered.
	said, ok := render(event)
	if !ok {
		return protocol.InboundMessage{}, "", false
	}

	message := protocol.InboundMessage{
		ID:        event.Info.ID,
		Chat:      chat,
		Sender:    sender,
		FromMe:    event.Info.IsFromMe,
		Timestamp: event.Info.Timestamp.UnixMilli(),
		Content:   said.content,
		QuotedID:  said.context.GetStanzaID(),
		Mentions:  mentionsOf(said.context),
		Ephemeral: said.context.GetExpiration(),
	}
	return message, said.failure, true
}

// naming fills in whichever of WhatsApp's two identifiers the JIDs carry, first one
// wins per kind. Separate from partyOf because a presence names somebody with no
// message to read a push name off, and a second copy of this would drift.
func naming(party *protocol.Party, jids ...waTypes.JID) {
	for _, jid := range jids {
		switch address, ok := addressOf(jid); {
		case !ok:
		case address.Kind == protocol.AddressPhone && party.Phone == "":
			party.Phone = address.ID
		case address.Kind == protocol.AddressLID && party.LID == "":
			party.LID = address.ID
		}
	}
}

// chatOf is which chat a message belongs to, and it is one function because two places
// have to agree on the answer: the address the event is published under, and the one the
// file kept for that message is filed under. A second copy of this rule would drift, and
// the drift would file a message's file in a chat the message is not in.
func chatOf(event *waEvents.Message) (protocol.Address, bool) {
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
	return addressOf(chatJID)
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

// bodyOf renders what a message says: its text, or its file once that has been fetched.
// The download is the reason this is a method: it is the session that has the socket to
// pull the bytes over and the store to put them in.
func (s *Session) bodyOf(ctx context.Context) renderBody {
	return func(event *waEvents.Message) (body, bool) {
		if plain, ok := plainBody(event); ok {
			return plain, true
		}
		if shared, ok := sharedBody(event); ok {
			return shared, true
		}
		if _, isAFile := attachmentOf(event.Message); isAFile {
			// Asked before mediaBody rather than after it, and the difference is the
			// whole reason the placeholder below is safe. A false from mediaBody means
			// two opposite things -- this is not a file, or this is a file whose bytes
			// may arrive next time -- and a placeholder reached through the second one
			// would acknowledge a message whose download was worth retrying, spending
			// the redelivery the retry was for.
			return s.mediaBody(ctx, event)
		}
		return unreadableBody(event)
	}
}

// unreadableBody is the placeholder for a message this build cannot render.
//
// It is what closes the last of the redelivery loops. Refusing the acknowledgement is
// the right answer for an envelope that cannot be addressed, because a later build with
// the same message can still publish it -- but for a body with no arm it buys nothing: a
// poll is a poll on every redelivery, and the agent sees neither the message nor the
// reason nothing arrived. A bubble they cannot read still says somebody sent something,
// which is enough to ask.
//
// The cost is real and it is the trade being made: a message published as unsupported is
// stored as unsupported, so a build that later learns the type does not go back for it.
func unreadableBody(event *waEvents.Message) (body, bool) {
	// The annotation goes with it. A poll sent as a reply is still a reply, and a client
	// that never learns what it answered cannot thread it -- the placeholder is the one
	// renderer that cannot lose this, because it is the one that runs for every arm
	// nobody has written a renderer for.
	return body{content: protocol.Unsupported(whyUnreadable(event)), context: alongside(event.Message)}, true
}

// alongside is the annotation a message carries, whichever arm it arrived in: the quote
// it answers, the accounts it tags, the chat's disappearing timer.
//
// Every body WhatsApp defines keeps that in a `contextInfo` of its own rather than on the
// message, and each renderer above reads its own by name. The placeholder cannot: it
// exists for the arms nothing here knows, so naming them is the one thing it must not do.
// It reads the field that is set instead, which is the same answer for an arm WhatsApp
// adds tomorrow.
//
// In field order rather than through Range, whose order protobuf does not promise. A
// message carries one body, so the two agree today; a message that somehow carried two
// would otherwise answer differently on different runs.
func alongside(message *waE2E.Message) *waE2E.ContextInfo {
	return annotating(message, wrappersDeep)
}

// wrappersDeep bounds how far in a body may be wrapped before this stops looking. Two is
// what WhatsApp sends -- a spoiler inside a group mention -- and the bound is here
// because this runs on whatsmeow's node handler and a crafted stanza could nest forever.
const wrappersDeep = 4

// aMessage is the name of the message type itself, which is how a field that nests one
// is told from every other field that holds a message.
var aMessage = (&waE2E.Message{}).ProtoReflect().Descriptor().FullName()

// annotating is alongside, with the descent that finds it when a body arrives wrapped.
//
// Thirty-two of the message's arms nest another message, and whatsmeow unwraps six of
// them before a handler sees the event. What is left -- a spoiler, a group mention, a
// question -- keeps the annotation on what is inside rather than on the wrapper, so a
// placeholder that only looked at the top published a reply that answers nothing.
//
// The descent is on the nesting rather than on the wrapper's type, which is the whole
// point: twenty-seven of those arms are a FutureProofMessage and five are not -- a
// comment, a payment and a request for one carrying their note, a device's own send, and
// a protocol message carrying the body it corrects -- and matching the named type would
// have followed the twenty-seven and dropped the annotation of the other five.
//
// Only a nested message is followed. The quoted message hanging off a contextInfo is a
// message too, and descending into that one would answer with somebody else's
// annotation; it is never reached because a contextInfo is not itself a message arm.
func annotating(message *waE2E.Message, left int) *waE2E.ContextInfo {
	if message == nil || left == 0 {
		return nil
	}
	reflected := message.ProtoReflect()
	fields := reflected.Descriptor().Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		if field.Kind() != protoreflect.MessageKind || field.IsList() || field.IsMap() || !reflected.Has(field) {
			continue
		}
		arm := reflected.Get(field).Message()
		annotation := arm.Descriptor().Fields().ByName("contextInfo")
		if annotation != nil && arm.Has(annotation) {
			if annotated, ok := arm.Get(annotation).Message().Interface().(*waE2E.ContextInfo); ok {
				return annotated
			}
		}
		nested := arm.Descriptor().Fields()
		for j := range nested.Len() {
			wrapped := nested.Get(j)
			if wrapped.Kind() != protoreflect.MessageKind || wrapped.IsList() || wrapped.IsMap() {
				continue
			}
			if wrapped.Message().FullName() != aMessage || !arm.Has(wrapped) {
				continue
			}
			inner, _ := arm.Get(wrapped).Message().Interface().(*waE2E.Message)
			if annotated := annotating(inner, left-1); annotated != nil {
				return annotated
			}
		}
	}
	return nil
}

// whyUnreadable is which of the contract's reasons this message arrived with. It is what
// separates a poll from a stanza that carried nothing at all, and a client shows the
// difference.
func whyUnreadable(event *waEvents.Message) protocol.UnsupportedReason {
	switch {
	case bodyless(event.Message):
		return protocol.UnsupportedEmpty
	case event.Message.GetProtocolMessage() != nil:
		// One that is not the account's own plumbing, which changeOf already dropped.
		// What is left is somebody acting in the conversation in a way the contract does
		// not carry -- sharing their phone number, putting a timer on the chat -- and
		// this reason is what the contract has for saying so.
		return protocol.UnsupportedProtocol
	default:
		return protocol.UnsupportedUnknownType
	}
}

// bodyless reports whether a message is nothing but what rides along with one.
//
// Two things do. The context info is an annotation, and a group's sender key is what
// makes the group's messages readable; WhatsApp attaches both to stanzas whose body is
// genuinely absent. Counted as content, every one of those reads as a message of an
// unknown type rather than as the empty thing it is.
//
// A stanza that is nothing but the key is not empty, it is housekeeping, and changeOf
// takes it before this is ever asked. What is left here really did arrive carrying
// nothing, which is a reason the contract names.
func bodyless(message *waE2E.Message) bool {
	if message == nil {
		return true
	}
	if len(message.ProtoReflect().GetUnknown()) > 0 {
		// An arm WhatsApp added after this descriptor was generated. protobuf keeps it
		// here and Range never visits it, so a stanza made of nothing else reads as
		// empty -- and one that also carried a group's key would be dropped as key
		// material and never seen at all. What nobody here knows about is exactly what
		// the placeholder exists for.
		return false
	}
	said := false
	message.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		if _, along := ridesAlong[field.Name()]; along {
			return true
		}
		said = true
		return false
	})
	return !said
}

// ridesAlong is what is attached to a message rather than being one. The annotation is
// one; the rest is the key material that makes the conversation readable, which WhatsApp
// sends both on its own and bolted to the message it was sent for.
var ridesAlong = map[protoreflect.Name]struct{}{
	"messageContextInfo":                         {},
	"senderKeyDistributionMessage":               {},
	"fastRatchetKeySenderKeyDistributionMessage": {},
	"groupRootKeyShare":                          {},
	"rootSecretDistributeMessage":                {},
}

// plainBody renders a message whose whole body is text, which is the one kind that
// needs nothing fetched to render.
func plainBody(event *waEvents.Message) (body, bool) {
	text, info, ok := textOf(event.Message)
	if !ok {
		return body{}, false
	}
	return body{content: protocol.Text(text), context: info}, true
}

// textOf pulls the body and the context out of the two shapes WhatsApp sends text in.
// A message with neither is either a media message or one of the types M2 has yet to
// reach, and saying so is the caller's cue to try the next renderer.
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
func mentionsOf(info *waE2E.ContextInfo) []protocol.Address {
	mentioned := info.GetMentionedJID()
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

	switch what := s.changed(event); what.verdict {
	case publishChange:
		// Not a message this account received, but something done to one it already has.
		// It waits on the publisher exactly as a message does: WhatsApp redelivers what
		// is not acknowledged, and a correction or a deletion nobody published is one
		// the conversation never learns about.
		return s.deliver(what.kind, what.payload)
	case dropChange:
		s.log.Info().Err(what.err).Str("message_id", event.Info.ID).Msg(what.why)
		return true
	case withholdChange:
		s.log.Warn().Err(what.err).Str("message_id", event.Info.ID).Msg(what.why)
		return false
	}

	message, failure, ok := inboundOf(event, s.bodyOf(s.ctx))
	if !ok {
		s.log.Debug().Str("message_id", event.Info.ID).
			Msg("refusing to acknowledge an inbound message this build cannot publish")
		return false
	}
	if !s.deliver(protocol.EventMessageReceived, map[string]any{"message": message}) {
		return false
	}
	if failure == "" {
		return true
	}
	// After the message and never instead of it: the client looks the message up to
	// flag its bubble, and a failure that arrives first names one nobody has stored.
	// A failure that cannot be published leaves the acknowledgement withheld, so the
	// redelivery is what gets the pair out in order.
	return s.deliver(protocol.EventMediaDownloadFailed, protocol.MediaDownloadFailure{
		Chat: message.Chat, MessageID: message.ID, Reason: failure,
	})
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
	case s.inbox <- pending{event: emission}:
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
