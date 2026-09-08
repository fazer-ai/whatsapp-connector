package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	waCommon "go.mau.fi/whatsmeow/proto/waCommon"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// unreadRequest is `message.mark_unread`.
type unreadRequest struct {
	Chat protocol.Address `json:"chat"`
	// LastMessageID is optional in the contract, and it is what tells WhatsApp which
	// message the chat is unread from. Without one the patch carries a range with no
	// message in it, which is still a valid way to say "unread" and is what a caller that
	// does not track the last message can send.
	LastMessageID *string `json:"last_message_id"`
	FromMe        bool    `json:"from_me"`
}

// markUnread puts a chat back to unread on this account's other devices.
//
// Not a receipt, which is what its neighbour `message.mark_read` is: nothing goes to the
// people in the chat and nothing is disclosed to them. It is an app state patch, the same
// mechanism the phone uses when somebody long-presses a chat and marks it unread, and it
// travels to this account's own devices through WhatsApp's app state sync.
//
// That is also why it is answered as soon as the patch is sent rather than when anything
// is observed to have changed: what comes back is that WhatsApp took the patch. The
// devices apply it on their own schedule and there is no acknowledgement to wait for.
func (s *Session) markUnread(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req unreadRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"an unread mark has to say which chat it is on")
	}
	chat, err := jidOf(req.Chat)
	if err != nil {
		return nil, err
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	// Absent, null and empty all mean the same thing here: no message to anchor on. The
	// schema takes any string, so an empty one is a payload a conforming client may send,
	// and the contract is what decides that -- refusing it would be a rule this build
	// invented. What it must not do is reach the key: an id of "" names a message
	// WhatsApp cannot match, and the patch would go out, be acknowledged, and do nothing.
	var anchor *waCommon.MessageKey
	if req.LastMessageID != nil && *req.LastMessageID != "" {
		anchor = &waCommon.MessageKey{
			RemoteJID: proto.String(chat.String()),
			FromMe:    proto.Bool(req.FromMe),
			ID:        proto.String(*req.LastMessageID),
		}
		// A key for somebody else's message in a chat that does not name its own author
		// needs the author too, and this command's payload has no field for one --
		// `message.mark_read` takes a `sender` and this does not. Rather than send a key
		// naming a message WhatsApp cannot find, the anchor is dropped and the patch goes
		// out with the range alone, which is what a caller that named no message gets and
		// is what marks the chat unread. What is lost is the refinement, not the command.
		//
		// Which chats those are is `authored`, the same table the read mark reads, rather
		// than a list written again here: the question is the same one, and two lists
		// would answer it differently the first time either moved.
		//
		// A message this account sent is the exception everywhere: `from_me` identifies
		// it without an author.
		if authored[req.Chat.Kind] && !req.FromMe {
			s.log.Debug().Str("kind", string(req.Chat.Kind)).
				Msg("dropping the anchor of an unread mark: the contract carries no participant to name the message's author")
			anchor = nil
		}
	}

	// The timestamp is left at zero on purpose: whatsmeow fills in the moment the patch
	// is built, and the contract carries no timestamp for the caller to give a better
	// one. What it bounds is the range the patch describes, and a range ending now is
	// what "this chat is unread as of this command" means.
	patch := appstate.BuildMarkChatAsRead(chat, false, time.Time{}, anchor)
	if err := s.sendAppState(ctx, s.current(), patch); err != nil {
		return nil, appStateFailure(err, "unread mark")
	}
	return nil, nil
}

// appStateFailure is contactFailure plus the one refusal a patch has that a query does
// not.
//
// An app state update that the server answers with an error collection comes back as
// `ErrAppStateUpdate` and not as an `*IQError`: the IQ itself succeeded and the rejection
// is inside it. Left to the default that would read as this connector's own failure, and
// send an operator to these logs for something WhatsApp decided.
func appStateFailure(err error, subject string) error {
	// The shared failures first, and the order is load-bearing rather than tidy. A 409
	// conflict has whatsmeow parse and apply the patches it got back before retrying, and
	// a deadline that runs out in there comes back wrapping both this sentinel and the
	// context's own error. Read for the sentinel first, a command that timed out reports
	// that WhatsApp refused it -- and the two send a client down different roads.
	if named, coded := commandFailure(err, subject); named {
		return coded
	}
	if errors.Is(err, wm.ErrAppStateUpdate) {
		return protocol.NewError(protocol.ErrorWaError, "WhatsApp refused the "+subject)
	}
	return contactFailure(err, subject)
}

// sendAppStateOverClient is the default for the seam below.
func sendAppStateOverClient(ctx context.Context, client *wm.Client, patch appstate.PatchInfo) error {
	return client.SendAppState(ctx, patch) //nolint:wrapcheck // classified by its caller
}
