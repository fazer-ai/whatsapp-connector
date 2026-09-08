package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	wm "go.mau.fi/whatsmeow"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// inviteRequest is `group.invite.get`: the link that lets somebody join a group.
type inviteRequest struct {
	Group  protocol.Address `json:"group"`
	Revoke bool             `json:"revoke"`
}

// groupInvite is the answer, `{code, url}`.
//
// Both, because they are two things a client does two things with: the code is what it
// stores and compares, and the URL is what an operator sends somebody. Handing back only
// the link would have every client slice the prefix off it, and each one would have to
// know that prefix.
type groupInvite struct {
	Code string `json:"code"`
	URL  string `json:"url"`
}

// groupInviteOf carries out `group.invite.get`.
//
// `revoke` asks WhatsApp for a new code, which is the only way to take an old one out of
// circulation: whoever holds the previous link can no longer join, and there is no undo.
// It is a separate field rather than a separate command because WhatsApp answers both
// with the same IQ, and a caller that asked to revoke still needs the code it got.
func (s *Session) groupInviteOf(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req inviteRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"an invite request has to name the group it is asking about")
	}
	if req.Group.Kind != protocol.AddressGroup {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is not a group: only a group has an invite link", req.Group.Kind))
	}
	group, err := jidOf(req.Group)
	if err != nil {
		return nil, err
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	link, err := s.inviteLink(ctx, s.current(), group, req.Revoke)
	if err != nil {
		return nil, inviteFailure(err)
	}
	code, joined := strings.CutPrefix(link, wm.InviteLinkPrefix)
	if !joined || code == "" {
		// A link this build cannot take apart. Passing the whole of it back as the code
		// would have a client store something that is not a code and compare it against
		// ones that are, and there is no shape to guess at here: the prefix is whatsmeow's
		// own constant, so this is it having changed under us.
		s.log.Error().Msg("WhatsApp answered an invite link this build cannot read a code out of")
		return nil, protocol.NewError(protocol.ErrorInternal, "the invite link could not be read")
	}
	return json.Marshal(groupInvite{Code: code, URL: link})
}

// inviteFailure names why the link could not be read.
//
// The three whatsmeow separates are all `wa_error` -- the contract has no code for "you
// are not allowed to" -- and the message is what tells them apart, because the three send
// an operator somewhere different: ask an admin, check the group, or rejoin it.
func inviteFailure(err error) error {
	if named, coded := commandFailure(err, "invite request"); named {
		return coded
	}
	switch {
	case errors.Is(err, wm.ErrGroupInviteLinkUnauthorized):
		return protocol.NewError(protocol.ErrorWaError,
			"only an administrator of this group can read its invite link")
	case errors.Is(err, wm.ErrGroupNotFound):
		return protocol.NewError(protocol.ErrorWaError, "WhatsApp does not know this group")
	case errors.Is(err, wm.ErrNotInGroup):
		return protocol.NewError(protocol.ErrorWaError, "this account is not in the group it asked about")
	}
	return contactFailure(err, "invite request")
}
