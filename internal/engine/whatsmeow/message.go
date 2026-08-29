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
	"github.com/fazer-ai/whatsapp-connector/internal/store"
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
	message, ok := envelopeOf(&event.Info)
	if !ok {
		return protocol.InboundMessage{}, "", false
	}
	// Asked for last, once every question about the envelope has been answered.
	said, ok := render(event)
	if !ok {
		return protocol.InboundMessage{}, "", false
	}

	message.Content = said.content
	message.QuotedID = said.context.GetStanzaID()
	message.Mentions = mentionsOf(said.context)
	message.Ephemeral = said.context.GetExpiration()
	return message, said.failure, true
}

// envelopeOf is everything about an inbound message except what was said in it, which is
// the one part a message this device was never given has none of.
//
// Separate from `inboundOf` for that reason and no other: a stanza that arrived with no
// ciphertext in it still has a sender, a chat and a time, and the client needs all three
// to put a bubble where the message was.
func envelopeOf(info *waTypes.MessageInfo) (protocol.InboundMessage, bool) {
	chat, ok := chatOf(info)
	if !ok || info.ID == "" {
		return protocol.InboundMessage{}, false
	}
	var sender *protocol.Party
	if !info.IsFromMe {
		// An echo carries no sender, which is what the contract's own fixture for one
		// says and what the client reads: `from_me` is the whole answer to who sent it,
		// and naming the account itself there files the operator's own number as the
		// party in a conversation with somebody else.
		var named bool
		if sender, named = partyOf(info); !named {
			return protocol.InboundMessage{}, false
		}
	}
	return protocol.InboundMessage{
		ID:        info.ID,
		Chat:      chat,
		Sender:    sender,
		FromMe:    info.IsFromMe,
		Timestamp: info.Timestamp.UnixMilli(),
	}, true
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
func chatOf(info *waTypes.MessageInfo) (protocol.Address, bool) {
	chatJID := info.Chat
	if info.IsIncomingBroadcast() {
		// Somebody sent this through a broadcast list, and WhatsApp shows it to the
		// recipient in the direct chat with whoever sent it, not under the list.
		// whatsmeow says so on the event, and addressing the list instead sends the
		// message to a chat the client does not open conversations for, after
		// acknowledging it: a message the recipient can see on their own phone and
		// nowhere else. The status feed is not a broadcast list and is not touched.
		chatJID = info.Sender
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
	// Once for the node, and not once per event it turns into. A message whose media
	// failed publishes twice, and the second waits on the first: stamped when it is sent
	// rather than when it was learned, the failure carries a time up to the whole
	// publisher bound later than the message it is about, for something the session knew
	// before it published either.
	learned := s.learned()

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

	// Before the switch below, and not after it: a recovered edit or reaction leaves here
	// as a change to a message that already exists, and a placeholder still waiting behind
	// that return would go out as a message of its own.
	stanza := stanzaOf(event)
	if inTime, held := s.arrived(stanza); held {
		if inTime {
			// This one was unreadable a moment ago and the recovery worked: either the
			// sender encrypted it again or the phone forwarded it. The placeholder waiting
			// for it is called off rather than published and corrected, because a client
			// that deduplicates on the id would keep the placeholder and discard this.
			s.log.Info().Str("message_id", stanza).
				Msg("a message that could not be read arrived after all")
		}
		// Held without being in time is a bubble the forwarder already committed to, and
		// its row is as finished as one called off: the decision has been made either way,
		// and only an undecided row should outlive this process.
		s.dropHold(stanza)
	}

	switch what := s.changed(event); what.verdict {
	case publishChange:
		// Not a message this account received, but something done to one it already has.
		// It waits on the publisher exactly as a message does: WhatsApp redelivers what
		// is not acknowledged, and a correction or a deletion nobody published is one
		// the conversation never learns about.
		return s.deliver(what.kind, what.payload, learned)
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
	if !s.deliver(protocol.EventMessageReceived, map[string]any{"message": message}, learned) {
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
	}, learned)
}

// stanzaOf is the id of the stanza this event arrived in, which is what a placeholder for
// it is waiting under and is not always the id on the event.
//
// A recovery that comes back through the phone is parsed by ParseWebMessage, and that
// rewrites the id of an edit to the message the edit corrects, leaving the stanza's own id
// on the web message it came in. Asked for the id on the event, the carrier's placeholder
// would go unclaimed and go out behind the correction; asked for both, the correction would
// call off a placeholder belonging to the message it corrects, which is a different message
// and still missing.
func stanzaOf(event *waEvents.Message) string {
	if id := event.SourceWebMsg.GetKey().GetID(); id != "" {
		return id
	}
	return event.Info.ID
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
func (s *Session) deliver(eventType protocol.EventType, payload any, learned int64) bool {
	return s.deliverUnless(eventType, payload, learned, "")
}

// deliverUnless is deliver for an event to be dropped if the message named here has arrived
// by the time the publisher is about to write it.
//
// Which is the whole reason a placeholder goes through it. Queuing it and writing it are
// two steps with a queue, a forwarder and a pump between them, and the message it stands in
// for can land anywhere in there: published ahead of the real message, a client that
// deduplicates on the id keeps the placeholder for good. Asked where a moment is asked
// whether it is still true, the choice is made where it stops being reversible, and under
// the lock the arrival takes.
func (s *Session) deliverUnless(eventType protocol.EventType, payload any, learned int64, unless string) bool {
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
		At:      learned,
		Settle:  func(err error) { settled <- err },
	}
	if unless != "" {
		emission.Claim = func() bool { return s.commit(unless) }
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

// rerequestTimeout is how long a message that could not be read is left to arrive before
// the connector gives up and says so.
//
// Both recoveries are real and both deliver the message under the id the placeholder
// would take, so publishing one straight away is not early, it is final: the client
// deduplicates on that id and discards the real message as a repeat. whatsmeow waits
// `RequestFromPhoneDelay`, five seconds, before it even asks the phone, and the phone's
// answer is a round trip after that. This is well past both, and it is a guess at the
// only number that matters -- how long a recovery takes when it works -- which nobody
// here has measured. What it costs when it is too long is an agent waiting to be told a
// message exists; what it costs when it is too short is that message never arriving.
const rerequestTimeout = 45 * time.Second

// unreadable answers a message that arrived with nothing in it this device could read.
//
// Two different things reach here and they look almost alike. WhatsApp does not hand a
// view-once photo to a companion device, so what arrives is a stanza with no ciphertext
// at all and whatsmeow asks the primary phone to forward the real one. And a message
// whose ciphertext would not open -- a Signal session that has drifted, a group message
// with no sender key -- gets a retry receipt asking the sender to encrypt it again.
//
// Neither is published straight away, and that is the whole of this. Both recoveries
// deliver the message under the same id, and a client deduplicates on that id: a
// placeholder that goes out first is the only thing that chat will ever show, over a
// message that did arrive. So the placeholder is scheduled instead, and the message
// arriving is what calls it off.
//
// Acknowledged either way, because there is nothing to withhold: whatsmeow acknowledged
// the node before this ran, so a refusal here buys no redelivery and the stanza carries
// nothing to redeliver.
func (s *Session) unreadable(event *waEvents.UndecryptableMessage) bool {
	learned := s.learned()

	if event.Info.Sender.IsBot() || event.Info.Chat.IsBot() {
		s.log.Debug().Str("message_id", event.Info.ID).
			Msg("dropping an unreadable message from one of Meta's own bots")
		return true
	}
	if event.DecryptFailMode == waEvents.DecryptFailHide {
		// The sender marked the stanza `decrypt-fail: hide`, which WhatsApp sets on a
		// reaction, an edit, a revoke and a poll vote: things done to a message rather
		// than messages. A bubble is the wrong shape for all four, and the sender is
		// asking for nothing to be shown rather than something unreadable.
		s.log.Debug().Str("message_id", event.Info.ID).
			Msg("dropping an unreadable action the sender asked not to be shown")
		return true
	}
	message, addressed := envelopeOf(&event.Info)
	if !addressed {
		// No chat to put it in or nobody to attribute it to. There is no bubble to be
		// had, and the stanza carries nothing else worth an event.
		s.log.Debug().Str("message_id", event.Info.ID).
			Msg("dropping an unreadable message with no conversation to put it in")
		return true
	}
	if message.Chat.Kind == protocol.AddressGroup && !s.wantsGroups() {
		s.log.Debug().Str("message_id", event.Info.ID).
			Msg("dropping an unreadable group message the client did not subscribe to")
		return true
	}
	message.Content = protocol.Unsupported(whyUnopened(event, message.Chat.Kind))
	s.awaitOrPublish(&message, learned)
	return true
}

// whyUnopened is which of the contract's reasons a message nothing could read arrived
// with.
//
// Two paths in whatsmeow arrive here and they answer different questions. A stanza that
// carries an `<unavailable/>` node and no ciphertext at all is the server saying nothing
// was encrypted for this device, and the `type` on that node is optional: an untyped one is
// still a message that never arrived to be decrypted. A ciphertext that did arrive and
// would not open is the other, and whatsmeow reports one of those as unavailable too -- a
// group message with no sender key -- so neither field answers this on its own.
//
// What separates them from the event alone is the chat. The decryption path only raises the
// flag for `skmsg`, which is how a group message is encrypted, so the flag on a direct chat
// came from the other path. A group's own untyped unavailable is the case this gets wrong,
// and it reads as a failure to decrypt something that was withheld.
func whyUnopened(event *waEvents.UndecryptableMessage, chat protocol.AddressKind) protocol.UnsupportedReason {
	if event.UnavailableType != "" {
		return protocol.UnsupportedUnavailable
	}
	if event.IsUnavailable && chat != protocol.AddressGroup {
		return protocol.UnsupportedUnavailable
	}
	return protocol.UnsupportedUndecryptable
}

// awaitOrPublish gives the message that could not be read its window to arrive, and
// publishes the placeholder if it does not.
func (s *Session) awaitOrPublish(message *protocol.InboundMessage, learned int64) {
	due := learned + s.rerequestWait.Milliseconds()
	s.hold(message, learned, due)
	// Measured from the deadline that was written down, not from here. Holding the row
	// is a store call and can take up to the store bound, and a window started after it
	// would have this owner publish later than the deadline a successor reads out of the
	// same row -- so whether a bubble was on time would depend on whether a handoff
	// happened to occur.
	s.await(message, learned, due, s.until(due))
}

// until is what is left of a deadline, and never less than nothing. A window that has
// already run out is a bubble that is overdue, which is a reason to publish now rather
// than an error.
func (s *Session) until(due int64) time.Duration {
	return max(time.Duration(due-s.learned())*time.Millisecond, 0)
}

// hold writes the undecided bubble down, so a process that ends inside the window does
// not take the decision with it.
//
// A failure here is reported and not raised. What is lost is the row, and without it
// this path behaves exactly as it did before the row existed: the bubble lives in a
// timer, and a timer lives as long as the process. Refusing the message over it would
// trade a bubble that might go missing for one that certainly does.
func (s *Session) hold(message *protocol.InboundMessage, learned, due int64) {
	body, err := json.Marshal(message)
	if err != nil {
		s.log.Error().Err(err).Str("message_id", message.ID).
			Msg("failed to render a placeholder to hold on to")
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.storeLimit)
	defer cancel()
	if err := s.store.PutPlaceholder(ctx, &store.Placeholder{
		MessageID: message.ID, Message: string(body), LearnedAt: learned, DueAt: due,
	}); err != nil {
		s.log.Warn().Err(err).Str("message_id", message.ID).
			Msg("a placeholder could not be held; it will not survive this process ending")
	}
}

// dropHold forgets a bubble that has been decided, whichever way it went. Reported and
// not raised for the same reason as hold: a row left behind is armed again by whoever
// opens the session next, and the two endings are both idempotent -- the message
// arriving finds nothing waiting, and a second publish is deduplicated on the id.
func (s *Session) dropHold(id string) {
	ctx, cancel := context.WithTimeout(s.ctx, s.storeLimit)
	defer cancel()
	if err := s.store.DropPlaceholder(ctx, id); err != nil {
		s.log.Warn().Err(err).Str("message_id", id).Msg("a decided placeholder could not be released")
	}
}

// await puts a message on the waiting list and arms the timer that decides it.
//
// The wait is passed in rather than taken from the session, because the two callers
// measure it from different places: a message that just arrived gets the whole window,
// and one picked up from the store gets what is left of the window it was given. Both
// publish under the same learned time and against the same deadline.
func (s *Session) await(message *protocol.InboundMessage, learned, due int64, wait time.Duration) {
	ctx, cancel := context.WithCancel(s.ctx)

	s.awaitedMu.Lock()
	if _, waiting := s.awaited[message.ID]; waiting {
		// Already waiting on this one. whatsmeow repeats the event when a resend fails to
		// decrypt as well, and a second timer would publish a second bubble for one
		// message.
		s.awaitedMu.Unlock()
		cancel()
		return
	}
	s.awaited[message.ID] = &awaiting{cancel: cancel, due: due}
	s.awaitedMu.Unlock()

	go func() {
		defer cancel()
		timeout := time.NewTimer(wait)
		defer timeout.Stop()
		select {
		case <-timeout.C:
		case <-ctx.Done():
			// The message arrived, or the session ended. Either way this is not the thing
			// that should be in that chat.
			return
		}
		s.log.Info().Str("message_id", message.ID).
			Msg("giving up on a message arriving in a form this could read, and saying so")
		s.insist(ctx, message, learned)
	}()
}

// insist offers the placeholder to the publisher, and keeps offering for as long as the
// session is up. Whether it is published or dropped for the message arriving is the
// forwarder's to decide; both answer here as a publish that worked.
//
// Every other event here answers a failed publish by withholding the acknowledgement, and
// WhatsApp redelivering is the retry. This one has no such thing: whatsmeow acknowledged
// the stanza before it dispatched the failure, so a publisher that is down for a moment
// would otherwise cost the message its only bubble. The retry is what puts this path back
// on a par with the others.
//
// It ends with the session and needs no bound of its own. The publisher and the lease
// share a Redis: one down long enough to matter takes the lease with it, and a session
// that has lost its lease is cancelled.
func (s *Session) insist(ctx context.Context, message *protocol.InboundMessage, learned int64) {
	for {
		if s.deliverUnless(protocol.EventMessageReceived, map[string]any{"message": message}, learned, message.ID) {
			s.forgetAwaiting(message.ID)
			// After the publish and not before it. A crash in between leaves a row that
			// the next owner publishes a second time, which the client deduplicates on
			// the id and discards; dropping it first would leave a crash in between with
			// no bubble at all, which is the whole failure this row exists to close.
			s.dropHold(message.ID)
			return
		}
		s.log.Warn().Str("message_id", message.ID).
			Msg("an unreadable message could not be published, and WhatsApp has no copy left to send")
		// Back on the waiting list as one that has not been offered, so the next attempt
		// can be chosen the way this one was. A message that arrived in the meantime took
		// it off that list, and nothing here puts it back.
		s.released(message.ID)
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.rerequestRetry):
		}
	}
}

// awaiting is a placeholder's place on the waiting list: what stops the timer it is
// holding, and whether it has been handed to the pump.
//
// The second is what keeps the two endings exclusive. A placeholder chosen by the
// forwarder is going out and cannot be taken back, and one that has only been offered
// still can, which is the difference between a message arriving in time and arriving too
// late to help.
type awaiting struct {
	cancel    context.CancelFunc
	committed bool
	// due is the deadline this placeholder was given, in milliseconds. Kept so a
	// re-arm can tell a row it is already holding from one it has yet to see.
	due int64
}

// commit takes the placeholder for a message off the waiting list's undecided half, and
// reports whether it was still there to take. The forwarder's half of the choice: the
// message arriving is the other, and they contend for one lock.
func (s *Session) commit(id string) bool {
	s.awaitedMu.Lock()
	defer s.awaitedMu.Unlock()
	entry, waiting := s.awaited[id]
	if !waiting {
		return false
	}
	entry.committed = true
	return true
}

// released puts a placeholder the publisher would not take back among the undecided, so
// the next attempt is chosen rather than assumed. A message that arrived meanwhile is
// gone from the list entirely, and this leaves it that way.
func (s *Session) released(id string) {
	s.awaitedMu.Lock()
	defer s.awaitedMu.Unlock()
	if entry, waiting := s.awaited[id]; waiting {
		entry.committed = false
	}
}

// rerequestRetry is how long the placeholder waits before trying the publisher again.
// Short, because the message it stands for is already late by the whole rerequest window
// and the thing being waited on is a publisher that answered once and may answer now.
const rerequestRetry = time.Second

// arrived calls off the placeholder for a message that turned up after all.
//
// Two answers, because the caller needs both and they are not the same question. inTime
// is whether the message beat the bubble, which is what there is to log. held is whether
// there was anything on the list at all, which is what says there is a row to release --
// and a placeholder the forwarder already committed is held without being in time, so
// one answer cannot stand for the other.
func (s *Session) arrived(id string) (inTime, held bool) {
	s.awaitedMu.Lock()
	entry, waiting := s.awaited[id]
	delete(s.awaited, id)
	s.awaitedMu.Unlock()
	if !waiting {
		return false, false
	}
	entry.cancel()
	// Committed means the forwarder has already chosen it, and a placeholder chosen is a
	// placeholder published. Late rather than in time, and said so rather than logged as a
	// message this saved.
	return !entry.committed, true
}

// forgetAwaiting takes a message off the waiting list and reports whether it was still
// on it, which is what stops a placeholder racing the message that just arrived.
func (s *Session) forgetAwaiting(id string) bool {
	s.awaitedMu.Lock()
	defer s.awaitedMu.Unlock()
	_, waiting := s.awaited[id]
	delete(s.awaited, id)
	return waiting
}

// forgetAwaited lets go of every placeholder still waiting, which is what a session
// closing owes the goroutines holding them.
//
// Let go of in this process only. The row each one was written to is left exactly where
// it is, and whoever opens the session next arms it again for the remainder of the
// window it was given. That is the difference between this and giving up: the timer ends
// with the process, the decision does not.
//
// Still not published on the way out, and that has not changed. The client deduplicates
// on the id, so a placeholder written here would outrank the message that turns up a
// minute later at the next owner, permanently. Leaving the row lets the good ending stay
// possible; publishing would close it.
func (s *Session) forgetAwaited() {
	s.awaitedMu.Lock()
	waiting := s.awaited
	s.awaited = make(map[string]*awaiting)
	s.awaitedMu.Unlock()
	for _, entry := range waiting {
		entry.cancel()
	}
}

// rearm picks up the bubbles a previous owner of this session left undecided and puts
// them back on the clock.
//
// This is the other half of holding them. A process that ends inside the window leaves
// rows behind; without this they are only rows. Whoever opens the session next serves
// out the deadline each one was given rather than starting a fresh window, because a
// handoff more frequent than the window would otherwise hold a bubble back forever, and
// because the message it stands for is already as late as it is.
//
// A deadline that has already passed is not an error and not a reason to drop the row:
// the message never arrived, the chat has been missing the bubble since, and the event
// carries the time the message reached this connector, so it lands where it belongs in
// the history rather than at the top of it. A zero wait sends it straight to the
// publisher, which is what an overdue bubble deserves.
//
// Reported and not raised. A session that cannot read these is a session that works in
// every other respect, and refusing to open it would turn a bubble that might be missing
// into an account that certainly is.
func (s *Session) rearm(ctx context.Context) {
	waiting, err := s.store.Placeholders(ctx)
	if err != nil {
		s.log.Warn().Err(err).Msg("could not read the placeholders this session left waiting")
		return
	}
	if len(waiting) == 0 {
		return
	}

	for _, held := range waiting {
		var message protocol.InboundMessage
		if err := json.Unmarshal([]byte(held.Message), &message); err != nil {
			// A row nothing can read is a row that can never be published, and leaving it
			// would have every future owner of this session try and fail on it. Dropped
			// with the reason said out loud, which is the only thing left to do with it.
			s.log.Error().Err(err).Str("message_id", held.MessageID).
				Msg("dropping a held placeholder this build cannot read")
			s.dropHold(held.MessageID)
			continue
		}
		wait := s.until(held.DueAt)
		s.log.Info().Str("message_id", held.MessageID).Dur("wait", wait).
			Msg("picking up a placeholder a previous owner of this session left waiting")
		s.await(&message, held.LearnedAt, held.DueAt, wait)
	}
}
