package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// sendRequest is `message.send`, decoded.
//
// The caller names the message. That is what makes a send idempotent: a command this
// connector already carried out and could not acknowledge is redelivered with the same
// id, goes out under the same id, and the recipient's own client shows one message
// rather than two. Generating an id here would turn every lost acknowledgement into a
// duplicate in somebody's conversation.
type sendRequest struct {
	MessageID string           `json:"message_id"`
	To        protocol.Address `json:"to"`
	Content   struct {
		Type string `json:"type"`
		Body string `json:"body"`
	} `json:"content"`
	Quoted *struct {
		ID          string            `json:"id"`
		Participant *protocol.Address `json:"participant"`
		FromMe      bool              `json:"from_me"`
	} `json:"quoted"`
	Mentions  []protocol.Address `json:"mentions"`
	Ephemeral uint32             `json:"ephemeral"`
	ClientRef *string            `json:"client_ref"`
}

// jidOf is the way back from a canonical address to the JID WhatsApp is addressed by.
// Refusing what it cannot build matters as much as building the rest: a bad server
// guessed at here is a message delivered to somebody the caller did not name.
func jidOf(address protocol.Address) (waTypes.JID, error) {
	if address.ID == "" {
		return waTypes.EmptyJID, protocol.NewError(protocol.ErrorInvalidPayload, "an address with no id names nobody")
	}
	switch address.Kind {
	case protocol.AddressPhone:
		return waTypes.NewJID(address.ID, waTypes.DefaultUserServer), nil
	case protocol.AddressLID:
		return waTypes.NewJID(address.ID, waTypes.HiddenUserServer), nil
	case protocol.AddressGroup:
		return waTypes.NewJID(address.ID, waTypes.GroupServer), nil
	case protocol.AddressNewsletter:
		return waTypes.NewJID(address.ID, waTypes.NewsletterServer), nil
	case protocol.AddressBroadcast, protocol.AddressStatus:
		return waTypes.NewJID(address.ID, waTypes.BroadcastServer), nil
	default:
		return waTypes.EmptyJID, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is not an address kind this connector knows", address.Kind))
	}
}

// send carries out `message.send` and answers the result the contract's table names.
func (s *Session) send(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req sendRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "a send has to say what to send and to whom")
	}
	if req.MessageID == "" {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "a send has to name the message it is sending")
	}
	if req.Content.Type != "text" {
		// Refused rather than sent as something else. A caller told its message went
		// out has no reason to send it again, so a media message quietly delivered as
		// its caption is one nobody finds out about.
		return nil, protocol.NewError(protocol.ErrorUnsupported,
			fmt.Sprintf("this connector cannot send %q yet", req.Content.Type))
	}
	if req.Content.Body == "" {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "a text message with no body is not a message")
	}
	to, err := jidOf(req.To)
	if err != nil {
		return nil, err
	}

	// Built before the session's own state is checked, and that order is deliberate: a
	// payload this connector can never send is the caller's bug whatever the socket is
	// doing, and answering `not_connected` to one sends it away to wait for a
	// connection that would not have helped.
	client := s.current()
	message, err := textToSend(&req, ownJID(client, to), to)
	if err != nil {
		return nil, err
	}

	if phone, _ := s.identity(); phone == "" {
		// Asked before the socket, because an unpaired session is never connected and
		// the two answers send a client down different roads: one pairs an account, the
		// other waits for a connection to come back. Answering `not_connected` to a
		// session that has no account leaves it waiting for something nothing is going
		// to do.
		return nil, protocol.NewError(protocol.ErrorNotPaired, "this session has no WhatsApp account to send from")
	}
	if !client.IsConnected() {
		return nil, protocol.NewError(protocol.ErrorNotConnected, "the session is not connected to WhatsApp")
	}

	sent, err := client.SendMessage(ctx, to, message, wm.SendRequestExtra{ID: req.MessageID})
	if err != nil {
		return nil, sendFailure(err)
	}

	return json.Marshal(map[string]any{
		"message_id": sent.ID,
		"timestamp":  sent.Timestamp.UnixMilli(),
		"client_ref": req.ClientRef,
	})
}

// textToSend renders the text and whatever rides along with it.
//
// A body on its own goes as the plain shape WhatsApp uses for one, because that is what
// a phone sends and what every client renders without thinking about it. The extended
// shape is for a message that carries something else: a quote, a mention, or the
// chat's disappearing-message timer.
func textToSend(req *sendRequest, own, to waTypes.JID) (*waE2E.Message, error) {
	alongside, err := contextToSend(req, own, to)
	if err != nil {
		return nil, err
	}
	if alongside == nil {
		return &waE2E.Message{Conversation: proto.String(req.Content.Body)}, nil
	}
	return &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
		Text:        proto.String(req.Content.Body),
		ContextInfo: alongside,
	}}, nil
}

// ownJID is this account's own address, in the form the chat is addressed by. WhatsApp
// names the same account by phone number in one chat and by LID in the next, and a
// quote attributed under the other one is a quote a client cannot match to anybody.
func ownJID(client *wm.Client, to waTypes.JID) waTypes.JID {
	if to.Server == waTypes.HiddenUserServer && !client.Store.LID.IsEmpty() {
		return client.Store.LID.ToNonAD()
	}
	if client.Store.ID != nil {
		return client.Store.ID.ToNonAD()
	}
	return waTypes.EmptyJID
}

// quotedParticipant names who wrote the message being answered.
//
// A caller only knows that for somebody else's message in a group; for its own it has
// nobody to name, and for a direct chat it does not send one at all. Left off, a client
// has a quote it cannot attribute. Both of the cases the caller cannot fill in are ones
// this session can: its own account wrote it, or the only other party in the chat did.
func quotedParticipant(req *sendRequest, own, to waTypes.JID) (waTypes.JID, error) {
	if req.Quoted.Participant != nil {
		return jidOf(*req.Quoted.Participant)
	}
	if req.Quoted.FromMe {
		return own, nil
	}
	if to.Server == waTypes.GroupServer {
		// Somebody else's message in a group, and the caller did not say whose. Guessing
		// would attribute it to the group itself.
		return waTypes.EmptyJID, nil
	}
	return to, nil
}

// contextToSend builds what goes alongside the body, and nil when nothing does.
//
// `own` is this account's own address and `to` is the chat, both of which the quote
// needs and the caller cannot always supply.
func contextToSend(req *sendRequest, own, to waTypes.JID) (*waE2E.ContextInfo, error) {
	var alongside *waE2E.ContextInfo
	ensure := func() *waE2E.ContextInfo {
		if alongside == nil {
			alongside = &waE2E.ContextInfo{}
		}
		return alongside
	}

	if req.Quoted != nil && req.Quoted.ID != "" {
		quote := ensure()
		quote.StanzaID = proto.String(req.Quoted.ID)
		participant, err := quotedParticipant(req, own, to)
		if err != nil {
			return nil, err
		}
		if !participant.IsEmpty() {
			quote.Participant = proto.String(participant.String())
		}
	}
	for _, mention := range req.Mentions {
		jid, err := jidOf(mention)
		if err != nil {
			return nil, err
		}
		mentioned := ensure()
		mentioned.MentionedJID = append(mentioned.MentionedJID, jid.String())
	}
	if req.Ephemeral > 0 {
		// Carried per message, not per chat: WhatsApp expires a message by what the
		// message itself says. A send that leaves it off in a chat on a timer is the one
		// message in that conversation that stays behind after the rest has gone.
		ensure().Expiration = proto.Uint32(req.Ephemeral)
	}
	return alongside, nil
}

// sendFailure turns whatsmeow's answer into a code a client can branch on. Anything
// this does not name degrades to wa_error rather than leaking a library's wording into
// somebody's dashboard.
func sendFailure(err error) error {
	switch {
	case errors.Is(err, wm.ErrNotLoggedIn):
		return protocol.NewError(protocol.ErrorNotPaired, "the session has no WhatsApp account to send from")
	case errors.Is(err, wm.ErrNotConnected):
		return protocol.NewError(protocol.ErrorNotConnected, "the session is not connected to WhatsApp")
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled),
		errors.Is(err, wm.ErrMessageTimedOut), errors.Is(err, wm.ErrIQTimedOut):
		// The message may well have gone out: what ran out is the answer, not the send,
		// whether the deadline was the command's or WhatsApp's. Saying so is what lets
		// the caller retry under the same id, which is the one retry that cannot
		// duplicate anything. A refusal here would have it give up on a message that is
		// already in somebody's chat.
		return protocol.NewError(protocol.ErrorTimeout, "WhatsApp did not answer whether the message went out")
	case errors.Is(err, wm.ErrUnknownServer), errors.Is(err, wm.ErrRecipientADJID):
		return protocol.NewError(protocol.ErrorInvalidPayload, "that is not an address a message can be sent to")
	default:
		return protocol.NewError(protocol.ErrorWaError, "WhatsApp refused the message")
	}
}
