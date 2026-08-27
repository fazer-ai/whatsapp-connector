package whatsmeow

import (
	"context"
	"encoding/json"
	"fmt"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// The three commands that act on a message that already exists.
//
// None of them is a verb of its own on the wire: WhatsApp carries an edit, a revoke and
// a reaction as ordinary messages whose body says what they do to another one. So all
// three end where a send ends, and what differs is the body they build, the key they
// build it around, and the answer the contract's table says they give back.
//
// What they have in common is that key. Every one of them names a message somebody
// already has, and naming it wrongly is the failure that matters: WhatsApp accepts a key
// that resolves to nothing, the send reports success, and the recipient sees no edit, no
// deletion and no reaction. There is no acknowledgement that says otherwise, so what
// cannot be built correctly is refused here instead.

// editRequest, revokeRequest and reactRequest are three shapes rather than one wide one.
//
// The contract gives each command its own payload, and they overlap only in naming a
// chat and a target. Decoded through a single struct, a field one of them adds later
// under a name another already reads differently would change what the other means --
// which is the compatibility the contract's additive rule exists to keep.
type editRequest struct {
	MessageID string           `json:"message_id"`
	To        protocol.Address `json:"to"`
	TargetID  string           `json:"target_id"`
	Content   json.RawMessage  `json:"content"`
}

type revokeRequest struct {
	To          protocol.Address  `json:"to"`
	TargetID    string            `json:"target_id"`
	Participant *protocol.Address `json:"participant"`
}

type reactRequest struct {
	MessageID    string           `json:"message_id"`
	To           protocol.Address `json:"to"`
	TargetID     string           `json:"target_id"`
	TargetFromMe bool             `json:"target_from_me"`
	// A pointer, because the two ways this can be missing mean opposite things: an empty
	// string is how the contract says "take the reaction off", and no field at all is a
	// caller that did not say what to react with. Decoded into a string both arrive as
	// "" and every malformed command would silently remove a reaction instead.
	Emoji             *string           `json:"emoji"`
	TargetParticipant *protocol.Address `json:"target_participant"`
}

// edit carries out `message.edit`.
func (s *Session) edit(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req editRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"an edit has to say which message it corrects, in which chat, and to what")
	}
	if req.TargetID == "" {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"an edit has to name the message it corrects")
	}
	to, err := jidOf(req.To)
	if err != nil {
		return nil, err
	}
	corrected, err := editedBody(req.Content)
	if err != nil {
		return nil, err
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	client := s.current()
	// The key BuildEdit puts together is `from_me: true` and nothing else, which is the
	// whole of what WhatsApp allows: a message is edited by whoever sent it. The
	// contract carries no way to say otherwise for exactly that reason.
	sent, err := s.putOnTheWire(ctx, to, s.orGenerated(req.MessageID),
		client.BuildEdit(to, req.TargetID, corrected))
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"message_id": sent.ID,
		"timestamp":  sent.Timestamp.UnixMilli(),
		"client_ref": nil,
	})
}

// revoke carries out `message.revoke`.
func (s *Session) revoke(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req revokeRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a revoke has to say which message it deletes and in which chat")
	}
	if req.TargetID == "" {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a revoke has to name the message it deletes")
	}
	to, err := jidOf(req.To)
	if err != nil {
		return nil, err
	}
	// Absent is the ordinary case and means the account's own message. Present is a
	// group admin deleting somebody else's, and it is passed on as it stands even in a
	// chat where WhatsApp will not act on it: the contract puts no condition on the
	// field, and a connector that added one would refuse a payload a client is entitled
	// to send.
	sender, err := jidOfMaybe(req.Participant)
	if err != nil {
		return nil, err
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	client := s.current()
	if _, err := s.putOnTheWire(ctx, to, "", client.BuildRevoke(to, sender, req.TargetID)); err != nil {
		return nil, err
	}
	// `null`, which is what the contract's table says a revoke answers. There is no id
	// worth handing back: the message this puts on the wire exists to make another one
	// disappear, and nothing ever refers to it again.
	return json.Marshal(nil)
}

// react carries out `message.react`.
func (s *Session) react(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req reactRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a reaction has to say which message it is on, in which chat, and with what")
	}
	if req.TargetID == "" {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a reaction has to name the message it is on")
	}
	if req.Emoji == nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a reaction has to say what to react with, and an empty one takes it off")
	}
	to, err := jidOf(req.To)
	if err != nil {
		return nil, err
	}
	sender, err := whoSentTheTarget(&req, to)
	if err != nil {
		return nil, err
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	client := s.current()
	sent, err := s.putOnTheWire(ctx, to, s.orGenerated(req.MessageID),
		client.BuildReaction(to, sender, req.TargetID, *req.Emoji))
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"message_id": sent.ID,
		"timestamp":  sent.Timestamp.UnixMilli(),
		"client_ref": nil,
	})
}

// whoSentTheTarget is the JID a reaction's key is built around, and getting it wrong is
// the silent failure this whole file is written against: WhatsApp accepts a reaction
// whose key resolves to no message, answers with a timestamp, and nobody ever sees it.
//
// An empty JID is how whatsmeow's BuildMessageKey is told the target is the account's
// own, so that is what a caller saying `target_from_me` gets back.
func whoSentTheTarget(req *reactRequest, chat waTypes.JID) (waTypes.JID, error) {
	if req.TargetFromMe {
		if req.TargetParticipant != nil {
			// Both, and they cannot both be true. Guessing which one the caller meant
			// puts the reaction on somebody else's message or on none at all, and the
			// send says it worked either way.
			return waTypes.EmptyJID, protocol.NewError(protocol.ErrorInvalidPayload,
				"a reaction cannot be on the account's own message and on somebody else's at once")
		}
		return waTypes.EmptyJID, nil
	}
	if req.TargetParticipant != nil {
		return jidOf(*req.TargetParticipant)
	}
	switch chat.Server {
	case waTypes.DefaultUserServer, waTypes.HiddenUserServer:
		// A chat with one other person: whoever it is with is who sent everything in it
		// that the account did not, so the chat names the sender on its own.
		return chat, nil
	default:
		// Anywhere else there are many, and the key needs the one. Refused rather than
		// sent with the chat in the sender's place: that builds a key naming a message
		// the group itself sent, which is no message, and the reaction lands nowhere.
		return waTypes.EmptyJID, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("a reaction on somebody else's message in a %s has to say whose", req.To.Kind))
	}
}

// jidOfMaybe is jidOf for a field the contract makes optional, where absent and an
// explicit null mean the same thing and neither is an error.
func jidOfMaybe(address *protocol.Address) (waTypes.JID, error) {
	if address == nil {
		return waTypes.EmptyJID, nil
	}
	return jidOf(*address)
}

// orGenerated is the id to put a message on the wire under.
//
// The contract requires one on a send and makes it optional on an edit and a reaction,
// so this fills in what a caller left out. It is worth knowing what that costs the
// caller: an id of their own is what makes a retry of a command whose reply was lost
// arrive under the same stanza id and be discarded by the receiving client. One
// generated here is new every time, so the retry lands a second edit or a second
// reaction instead.
func (s *Session) orGenerated(messageID string) string {
	if messageID != "" {
		return messageID
	}
	return s.current().GenerateMessageID()
}

// editedBody renders what a message is being corrected to.
//
// Text only, and that is a limitation rather than a reading of the contract: WhatsApp
// does let a caption be edited, but the message that carries the correction has to be
// the whole media message again -- upload coordinates, keys and hashes -- and nothing
// here keeps those once a send is done. Building one without them puts a message on the
// wire whose file resolves to nothing. See #32.
func editedBody(raw json.RawMessage) (*waE2E.Message, error) {
	var body struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &body); err != nil || body.Type == "" {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"an edit has to say what the message is being corrected to")
	}
	if body.Type != "text" {
		return nil, protocol.NewError(protocol.ErrorUnsupported,
			fmt.Sprintf("this connector cannot correct a message to %q yet", body.Type))
	}
	content, err := decodeBody[textContent](raw, body.Type)
	if err != nil {
		return nil, err
	}
	// No context alongside it. The contract's edit payload carries no quote and no
	// mentions, so there is nothing to put in one, and a correction that invented an
	// empty context would drop the quote the original was sent with.
	return textToSend(&content, nil)
}
