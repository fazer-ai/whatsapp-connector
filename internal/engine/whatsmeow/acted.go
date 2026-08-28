package whatsmeow

import (
	"context"
	"errors"

	wm "go.mau.fi/whatsmeow"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// What arrives when somebody acts on a message that already exists.
//
// WhatsApp has no verb for any of this. An edit, a deletion and a reaction all travel as
// ordinary messages whose body says what they do to another one, which is why they land
// in the same handler a new message does and why telling them apart is the whole job
// here. Read as new messages they are worse than useless: an edit publishes the
// correction as a second bubble or is deduplicated away, a deletion has no body to
// render at all, and a reaction turns into a message the account never received.
//
// Each of the three is its own event on the wire because the client does something
// different with each: rewrite a bubble, flag or blank one, annotate one. The contract
// already names all three and this is the side that had yet to produce them.

// verdict is what a stanza that changes a message is worth doing about.
type verdict int

const (
	// notAChange is an ordinary message, and the caller carries on with it.
	notAChange verdict = iota
	// publishChange is one this build renders, and the payload is ready to go out.
	publishChange
	// dropChange is one nothing will ever publish -- no later milestone, no better
	// build. Acknowledged rather than refused, because withholding would leave WhatsApp
	// redelivering it for as long as the session is up.
	dropChange
	// withholdChange is one this build cannot publish and a later one could. The
	// acknowledgement is withheld so the account keeps it, which is the same trade the
	// message path makes.
	withholdChange
)

// change is what changeOf decided, with what to publish or what to say about not
// publishing it.
type change struct {
	verdict verdict
	kind    protocol.EventType
	payload any
	// why is the line the log carries for a drop or a withhold. It never names a body,
	// an emoji or a phone number: this is the log of the fact that something happened,
	// not of what was said.
	why string
	// err is what went wrong where something did. zerolog drops a nil one, so every
	// verdict logs the same way whether or not there was an error behind it.
	err error
}

// because attaches what went wrong to a drop or a withhold.
func (c change) because(err error) change {
	c.err = err
	return c
}

func publishing(kind protocol.EventType, payload any) change {
	return change{verdict: publishChange, kind: kind, payload: payload}
}

func dropping(why string) change { return change{verdict: dropChange, why: why} }

func withholding(why string) change { return change{verdict: withholdChange, why: why} }

// changed is changeOf plus the one arm that cannot be decided by reading the stanza.
//
// A correction arrives sealed under the secret of the message it corrects, and opening
// it takes the device store the socket writes rather than anything on the wire. That is
// not an exotic shape: a phone editing a message in a plain one-to-one chat sends this,
// and the wrapped protocol message the library also knows how to build is what arrives
// through other doors. Measured against a real phone, not read off a specification.
func (s *Session) changed(event *waEvents.Message) change {
	sealed := event.Message.GetSecretEncryptedMessage()
	// Comparing the type is safe on a nil message because MESSAGE_EDIT is not the zero
	// value of that enum -- UNKNOWN is. The other things sealed this way are a poll
	// edit, an event edit and a schedule, and the contract carries none of them.
	if sealed.GetSecretEncType() == waE2E.SecretEncryptedMessage_MESSAGE_EDIT {
		return s.unsealedCorrection(event, sealed)
	}
	return changeOf(event)
}

// unsealedCorrection opens a sealed correction and renders it, or says why it could not.
//
// The two ways it fails are not the same answer, and getting that backwards is the
// mistake this milestone has already made twice in both directions. A secret this
// session never had is final: it was stored when the message being corrected arrived,
// and a redelivery brings the same ciphertext and no key -- so it is acknowledged and
// the correction is lost, rather than kept and redelivered for as long as the session is
// up. A socket that is not up yet, or a store that ran out of time, is not final, and
// that one is kept.
//
// Anything else -- a payload that will not decrypt, a store that answered with an error
// of its own -- is treated as final. It costs one correction, loudly, against a
// redelivery loop that costs every one after it too.
func (s *Session) unsealedCorrection(event *waEvents.Message, sealed *waE2E.SecretEncryptedMessage) change {
	target := sealed.GetTargetMessageKey().GetID()
	if target == "" {
		return dropping("dropping a sealed correction that names no message to correct")
	}
	chat, sender, addressed := whereAndWho(event)
	if !addressed {
		return withholding("withholding an acknowledgement for a sealed correction this build cannot address")
	}

	open := s.unseal
	if open == nil {
		open = s.unsealOverStore
	}
	// Bounded, because this runs on whatsmeow's own node handler: a store that never
	// answers would hold the socket's next node behind it with nothing to release it.
	ctx, cancel := context.WithTimeout(s.ctx, s.storeLimit)
	defer cancel()

	plain, err := open(ctx, event)
	switch {
	case errors.Is(err, wm.ErrNotLoggedIn), errors.Is(err, wm.ErrClientIsNil),
		errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return withholding("withholding an acknowledgement for a correction this session could not reach its keys for").because(err)
	case err != nil:
		return dropping("dropping a correction this session has no key for").because(err)
	}

	corrected, at := theSealedBody(plain, event)
	content, renderable := correctedContent(corrected)
	if !renderable {
		return withholding("withholding an acknowledgement for a sealed correction whose new body this build cannot render")
	}
	return publishing(protocol.EventMessageEdited, protocol.MessageEdited{
		Chat:      chat,
		Sender:    sender,
		MessageID: target,
		FromMe:    event.Info.IsFromMe,
		Content:   content,
		Timestamp: at,
	})
}

// unsealOverStore is what opens a sealed change when nothing has replaced it: the
// socket's own device store, where the secret of every message this session has seen was
// put as it arrived.
func (s *Session) unsealOverStore(ctx context.Context, event *waEvents.Message) (*waE2E.Message, error) {
	return s.current().DecryptSecretEncryptedMessage(ctx, event)
}

// theSealedBody is what a sealed correction says the message now reads, and when.
//
// What a phone really sends is the wrapper: measured against one, the plaintext of a
// correction made in a direct chat is the protocol message an unsealed correction
// carries, and the clock inside it is what makes the published timestamp land on a
// millisecond rather than on the stanza's second. That is the branch that fires.
//
// The other is the fallback for a plaintext that is the new body on its own, which is
// what the field the payload is unmarshalled into allows and what nothing has been seen
// to send. It is here because the alternative to rendering it is refusing it, and the
// envelope carries no clock of its own to stamp it with.
func theSealedBody(plain *waE2E.Message, event *waEvents.Message) (corrected *waE2E.Message, at int64) {
	if inner := plain.GetProtocolMessage(); inner.GetType() == waE2E.ProtocolMessage_MESSAGE_EDIT {
		return inner.GetEditedMessage(), stampedAt(inner.GetTimestampMS(), event)
	}
	return plain, event.Info.Timestamp.UnixMilli()
}

// changeOf reads a stanza as something done to a message that already exists, and says
// so when it is not one.
//
// The order is what makes it readable: a reaction is recognised by what it carries, and
// everything else by the `edit` attribute WhatsApp puts on the stanza. The attribute is
// checked exhaustively rather than by exclusion, so an attribute a future WhatsApp
// introduces is refused with a name rather than published as whatever it happens to
// resemble.
func changeOf(event *waEvents.Message) change {
	switch {
	case event.Message.GetReactionMessage() != nil:
		return reactionOf(event, event.Message.GetReactionMessage())

	case event.Message.GetEncReactionMessage() != nil:
		// A community announcement group seals its reactions under the secret of the
		// message being reacted to. Opening one takes the device store the way a sealed
		// correction does, and the target it names is on the envelope rather than in the
		// plaintext -- but unlike a correction it is not the ordinary shape, and every
		// reaction in a direct chat arrives in the clear. Kept on the phone, because a
		// build that learns to read them can still publish it. Issue #38.
		return withholding("withholding an acknowledgement for a reaction sealed under a message secret this build does not read")

	case event.IsEdit,
		event.Info.Edit == waTypes.EditAttributeMessageEdit,
		event.Info.Edit == waTypes.EditAttributeAdminEdit,
		newsletterEdit(event),
		resentEdit(event):
		return editOf(event)

	case revokes(event):
		return revokeOf(event)

	case event.Info.Edit == waTypes.EditAttributePinInChat:
		// Pinning is not a change to the message, it is a change to where the chat shows
		// it, and the contract has no event for it in any milestone. Acknowledged, or
		// every pin in every chat would redeliver for good.
		return dropping("dropping a pin, which the contract does not carry")

	case event.Message.GetPollUpdateMessage() != nil:
		// A vote updates a poll that already exists, the way a reaction updates a
		// message. The contract has an event for the reaction and none for this, and a
		// vote published as a message received would put a bubble in the thread for
		// something that is not a bubble on the voter's phone either -- one per voter,
		// per poll. It belongs with the poll, whenever the contract carries one.
		return dropping("dropping a poll vote, which updates a message rather than adding one")

	case bodyless(event.Message) && event.Message.GetSenderKeyDistributionMessage() != nil:
		// A group handing out the key its messages are readable with. It usually rides
		// along with the message it was sent for, and then that message is what gets
		// rendered; alone, whatsmeow files it and nothing is left for anybody to read.
		// The placeholder the message path would otherwise give it is a bubble in an
		// agent's thread every time a group's membership changes.
		return dropping("dropping a sender key distribution, which is how a group stays readable rather than something somebody sent")

	case event.Message.GetProtocolMessage() != nil:
		// Machinery rather than something somebody sent: a history sync notification, an
		// app state key share, the answer to a peer request. whatsmeow acts on these
		// itself and none of them carries anything a conversation shows, so the
		// placeholder the message path would otherwise give this would put the account's
		// own housekeeping in an agent's thread, once per sync. Answered here because it
		// is the last thing a protocol message can be: the two that a client does act on
		// were taken above.
		return dropping("dropping a protocol message, which is machinery rather than something somebody sent")

	case event.Info.Edit != waTypes.EditAttributeEmpty:
		// WhatsApp added something. Loud rather than silent, and kept on the phone: a
		// build that learns what it is can still publish it, and guessing from the body
		// would file it as whichever of the three it happens to look like.
		return withholding("withholding an acknowledgement for a way of changing a message this build does not know")
	}
	return change{verdict: notAChange}
}

// revokes reports whether this stanza deletes a message rather than carrying one.
//
// The protobuf half is read only once there is a protocol message to read it from.
// REVOKE is the zero value of that enum, so GetType() on the nil an ordinary text
// message carries answers REVOKE, and reading it without the guard would classify every
// message in the account as a deletion of itself.
func revokes(event *waEvents.Message) bool {
	switch event.Info.Edit {
	case waTypes.EditAttributeSenderRevoke, waTypes.EditAttributeAdminRevoke:
		return true
	}
	deletion := event.Message.GetProtocolMessage()
	return deletion != nil && deletion.GetType() == waE2E.ProtocolMessage_REVOKE
}

// reactionOf renders `message.reaction`, which is also how a reaction is taken back: an
// empty emoji is the removal, and it is the one field here that is never omitted.
func reactionOf(event *waEvents.Message, reaction *waE2E.ReactionMessage) change {
	target := reaction.GetKey().GetID()
	switch {
	case target == "":
		return dropping("dropping a reaction that names no message to put it on")
	case event.Info.ID == "":
		// The client deduplicates on it and matches its own sends by it. Nothing arrives
		// later that supplies one, so a redelivery would be the same stanza again.
		return dropping("dropping a reaction with no id of its own")
	}
	chat, sender, addressed := whereAndWho(event)
	if !addressed {
		return withholding("withholding an acknowledgement for a reaction this build cannot address")
	}
	return publishing(protocol.EventMessageReaction, protocol.MessageReaction{
		ID:        event.Info.ID,
		Chat:      chat,
		Sender:    sender,
		FromMe:    event.Info.IsFromMe,
		TargetID:  target,
		Emoji:     reaction.GetText(),
		Timestamp: stampedAt(reaction.GetSenderTimestampMS(), event),
	})
}

// editOf renders `message.edited`.
func editOf(event *waEvents.Message) change {
	target, corrected, at := theCorrection(event)
	if target == "" {
		return dropping("dropping an edit that names no message to correct")
	}
	chat, sender, addressed := whereAndWho(event)
	if !addressed {
		return withholding("withholding an acknowledgement for an edit this build cannot address")
	}
	content, renderable := correctedContent(corrected)
	if !renderable {
		// A body this build has no arm for. Kept on the phone rather than published
		// empty, which would blank the bubble it was meant to correct.
		return withholding("withholding an acknowledgement for an edit whose new body this build cannot render")
	}
	return publishing(protocol.EventMessageEdited, protocol.MessageEdited{
		Chat:      chat,
		Sender:    sender,
		MessageID: target,
		FromMe:    event.Info.IsFromMe,
		Content:   content,
		Timestamp: at,
	})
}

// theCorrection pulls the three things an edit is: which message it corrects, what that
// message now says, and when the correction was made.
//
// An edit arrives in three shapes, and only the first is the one it was sent in.
//
// A channel's is shaped differently from everybody else's, and whatsmeow says so on the
// event rather than in the stanza: the new body arrives unwrapped, under the original
// post's id, with the edit's own clock off to the side. Read the ordinary way it is a
// fresh post under an id the client already has.
//
// The third is not WhatsApp's doing at all. A message that did not reach this device the
// first time comes back through another door -- the resend of a placeholder, a history
// sync -- and whatsmeow's parser for that door takes the edit apart for its caller: the
// corrected body becomes the message, the target becomes the id on the event, and the
// only thing left saying an edit was ever wrapped here is the flag. There is no protocol
// message left to find a key in, so read the ordinary way that correction names nothing
// and is dropped.
//
// The protocol message wins where there is one, because that shape is unambiguous and
// the wrapper does not have to have carried an edit to be there.
func theCorrection(event *waEvents.Message) (target string, corrected *waE2E.Message, at int64) {
	if newsletterEdit(event) {
		return event.Info.ID, event.Message, event.NewsletterMeta.EditTS.UnixMilli()
	}
	if edit := event.Message.GetProtocolMessage(); edit != nil {
		return edit.GetKey().GetID(), edit.GetEditedMessage(), stampedAt(edit.GetTimestampMS(), event)
	}
	if resentEdit(event) {
		// Taken apart already, and the clock went with the protocol message the parser
		// left out of the event. It is still on the raw message the parser kept, at the
		// millisecond the stanza's own second cannot hold -- and that difference is the
		// whole of the order between two corrections of one message made in the same
		// second. Truncated, the client is left with a tie and arrival to break it,
		// which is the thing that was out of order to begin with.
		return event.Info.ID, event.Message, stampedAt(theResentCorrection(event).GetTimestampMS(), event)
	}
	return "", nil, 0
}

// resentEdit reports whether the parser for the resend door took an edit apart on the
// way in.
//
// It is read off what the parser did rather than off a flag, because the flag is only
// half there: IsEdit is raised for the shape that arrived inside an EditedMessage
// envelope and not for the one that did not, while the id is rewritten for both. And
// that rewrite is the tell -- the parser replaces the event's id with the corrected
// message's only when what it unwrapped was an edit, so an id that no longer matches the
// one on the web message it was built from is the parser saying so. Without this, the
// envelope-less shape arrives looking like an ordinary message under the id of the
// message it was correcting, which is the id a client would then find and overwrite.
func resentEdit(event *waEvents.Message) bool {
	return event.SourceWebMsg != nil && event.Info.ID != event.SourceWebMsg.GetKey().GetID()
}

// theResentCorrection is the protocol message a correction was taken out of, read back
// off the raw message the parser kept.
//
// The unwrapping is the library's own, run again on a copy, because the parser unwraps
// before it looks and the envelopes it goes through are its list to grow: a correction
// the account made from another device arrives inside a DeviceSentMessage, one in a
// disappearing chat inside an EphemeralMessage. A hand-written copy of that list would
// drift, and what drifts off is the clock -- the correction still publishes, at the
// stanza's second instead of its own millisecond.
//
// The copy is what keeps this from touching the event: UnwrapRaw writes as it reads.
func theResentCorrection(event *waEvents.Message) *waE2E.ProtocolMessage {
	replayed := (&waEvents.Message{RawMessage: event.RawMessage}).UnwrapRaw()
	return replayed.Message.GetProtocolMessage()
}

// correctedContent renders what a message says now that it has been corrected.
//
// Deliberately not the renderer the message path uses. That one fetches the file of a
// media message, and an edit never changes the file: WhatsApp lets a caption be
// rewritten and nothing else. The bytes are the ones the client already holds a
// reference to, so fetching them again would mint a second blob for the same file and
// charge the account's bandwidth for a caption.
func correctedContent(corrected *waE2E.Message) (any, bool) {
	if corrected == nil {
		return nil, false
	}
	if text, _, ok := textOf(corrected); ok {
		return protocol.Text(text), true
	}
	if part, ok := attachmentOf(corrected); ok {
		// No Ref, on purpose: see protocol.MessageEdited.
		return part.content, true
	}
	return nil, false
}

// revokeOf renders `message.revoked`.
func revokeOf(event *waEvents.Message) change {
	chat, sender, addressed := whereAndWho(event)
	if !addressed {
		return withholding("withholding an acknowledgement for a deletion this build cannot address")
	}
	target := event.Message.GetProtocolMessage().GetKey().GetID()
	if target == "" && chat.Kind == protocol.AddressNewsletter {
		// A channel deletes a post by sending the deletion under the post's own id, with
		// no body at all to name a key in. It is the same asymmetry a channel's edit has,
		// and the library's own send path is where it is written down.
		target = event.Info.ID
	}
	if target == "" {
		return dropping("dropping a deletion that names no message to delete")
	}

	// Who performed the deletion, not who wrote what was deleted. A group admin removing
	// somebody else's message is the account's own act when the account is that admin,
	// and `sender` is what says who it was in every other case.
	//
	// A channel cannot answer it at all. whatsmeow does not work out `from_me` for a
	// newsletter stanza -- it says so, in as many words, where it fills the rest of the
	// source in -- so the account deleting its own post from another device is
	// indistinguishable here from the channel's owner deleting one. Both are published
	// as the contact's, because the two mistakes are not the same size: the client keeps
	// the text and the files of a contact's deletion and destroys them for its own, so
	// guessing `self` would take an agent's copy of a post nobody asked it to.
	by := protocol.RevokedByContact
	if event.Info.IsFromMe {
		by = protocol.RevokedBySelf
	}
	return publishing(protocol.EventMessageRevoked, protocol.MessageRevoked{
		Chat:      chat,
		Sender:    sender,
		MessageID: target,
		By:        by,
		Timestamp: event.Info.Timestamp.UnixMilli(),
	})
}

// whereAndWho is the half of these three events that does not depend on which one it is.
//
// The refusals are the ones inboundOf makes, for the same reasons: a chat the contract
// cannot name is one the client cannot open, and an unattributed change in a direct chat
// reads as the person on the other side of it, which puts somebody else's correction,
// deletion or reaction in their mouth. An echo carries no sender at all, because
// `from_me` is the whole answer to who did it.
func whereAndWho(event *waEvents.Message) (protocol.Address, *protocol.Party, bool) {
	chat, named := chatOf(event)
	if !named {
		return protocol.Address{}, nil, false
	}
	if event.Info.IsFromMe {
		return chat, nil, true
	}
	sender, attributed := partyOf(&event.Info)
	if !attributed {
		return protocol.Address{}, nil, false
	}
	return chat, sender, true
}

// stampedAt is the clock one of these is ordered by: the sender's own where they set
// one, and arrival where they did not.
//
// It matters more here than it does for a message. Two reactions from the same person,
// or two edits of the same message, are the same row on the client and the later one
// wins -- and arrival is the thing that is out of order, which is why it is only the
// fallback.
func stampedAt(stated int64, event *waEvents.Message) int64 {
	if stated != 0 {
		return stated
	}
	return event.Info.Timestamp.UnixMilli()
}
