package whatsmeow

import (
	"context"
	"encoding/json"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// groupTarget is the payload every command that names one group shares.
type groupTarget struct {
	Group protocol.Address `json:"group"`
}

// groupParticipant is one row of a group's membership, as the contract spells it.
type groupParticipant struct {
	Party protocol.Party `json:"party"`
	Role  string         `json:"role"`
}

// groupInfo is the contract's `group_info`.
//
// The three fields it does not carry are absent rather than null, and the difference is
// the contract's own: `picture_url` and `invite_code` each take a query of their own that
// this command does not make, and `has_picture` exists precisely so a client can tell a
// group with no photo from a snapshot that does not mention one. Leaving all three out is
// what the contract asks of a producer that does not answer them -- a `null` would claim
// this group has no photo and no invite, which is a different and untrue thing.
type groupInfo struct {
	Group         protocol.Address   `json:"group"`
	Subject       string             `json:"subject,omitempty"`
	Description   string             `json:"description,omitempty"`
	Owner         *protocol.Party    `json:"owner,omitempty"`
	CreatedAt     int64              `json:"created_at,omitempty"`
	Participants  []groupParticipant `json:"participants"`
	Size          int                `json:"size"`
	Announce      bool               `json:"announce"`
	Locked        bool               `json:"locked"`
	JoinApproval  bool               `json:"join_approval"`
	MemberAddMode string             `json:"member_add_mode,omitempty"`
}

// groupInfoOf reads a group's metadata.
//
// The size is `ParticipantCount` when WhatsApp gives one and the length of the list
// otherwise. The two disagree on a group whose membership was not sent in full, and the
// count is the one that answers "how many people are in this group" -- which is what a
// client shows next to the name, where a number that shrinks because a snapshot was
// partial is worse than no number at all.
func (s *Session) groupInfoOf(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req groupTarget
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a group info request has to name the group it is asking about")
	}
	if req.Group.Kind != protocol.AddressGroup {
		// A phone or a LID here is a direct chat, which has no subject, no participants
		// and no admins. WhatsApp answers the query with an error rather than nothing, so
		// refusing it names what is wrong instead of passing back `wa_error`.
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a group info request has to name a group, and that is not one")
	}
	group, err := jidOf(req.Group)
	if err != nil {
		return nil, err
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	info, err := s.groupInfo(ctx, s.current(), group)
	if err != nil {
		return nil, contactFailure(err, "group info request")
	}
	if info == nil {
		// whatsmeow returns an error for a group it cannot read, so nothing should reach
		// here with neither. Reading the fields off it would take the session's executor
		// down along with every command queued behind it.
		return nil, protocol.NewError(protocol.ErrorInternal,
			"the group info query came back empty without saying why")
	}
	return json.Marshal(s.describeGroup(ctx, info))
}

// describeGroup turns whatsmeow's own view of a group into the contract's.
func (s *Session) describeGroup(ctx context.Context, info *waTypes.GroupInfo) groupInfo {
	described := groupInfo{
		Subject:      info.Name,
		Description:  info.Topic,
		Announce:     info.IsAnnounce,
		Locked:       info.IsLocked,
		JoinApproval: info.IsJoinApprovalRequired,
		Size:         info.ParticipantCount,
		Participants: make([]groupParticipant, 0, len(info.Participants)),
	}
	if address, named := addressOf(info.JID); named {
		described.Group = address
	}
	if !info.GroupCreated.IsZero() {
		described.CreatedAt = info.GroupCreated.UnixMilli()
	}
	if owner := s.party(ctx, info.OwnerJID, info.OwnerPN); owner.Phone != "" || owner.LID != "" {
		described.Owner = &owner
	}
	if mode := memberAddModes[info.MemberAddMode]; mode != "" {
		described.MemberAddMode = mode
	}
	for i := range info.Participants {
		member := &info.Participants[i]
		// Both namespaces off the participant itself where it has them, and the mapping
		// only for what it does not: whatsmeow fills PhoneNumber and LID separately, so
		// most rows answer without a lookup at all.
		party := s.party(ctx, member.JID, member.PhoneNumber, member.LID)
		if party.Phone == "" && party.LID == "" {
			// Nobody this connector can name. An anonymous participant in an
			// announcement group is the case, and a row with no party is one the
			// contract does not allow.
			continue
		}
		described.Participants = append(described.Participants, groupParticipant{
			Party: party, Role: roleOf(member),
		})
	}
	if described.Size == 0 {
		// The list WhatsApp sent, not the rows that survived naming. Somebody this
		// connector cannot name is still somebody in the group, and counting only the
		// nameable ones would report an announcement group as smaller than it is.
		described.Size = len(info.Participants)
	}
	return described
}

// roleOf is the contract's three, in the order that matters: a superadmin is an admin
// too, so asking the narrower question first is what keeps the group's creator from
// being reported as an ordinary admin.
func roleOf(member *waTypes.GroupParticipant) string {
	switch {
	case member.IsSuperAdmin:
		return "superadmin"
	case member.IsAdmin:
		return "admin"
	}
	return "member"
}

// memberAddModes is whatsmeow's spelling onto the contract's. A mode this build does not
// know is left out rather than guessed: the field is optional, and an absent one says
// "this producer does not answer that" where a wrong one says the group is something it
// is not.
var memberAddModes = map[waTypes.GroupMemberAddMode]string{
	waTypes.GroupMemberAddModeAdmin:     "admin_add",
	waTypes.GroupMemberAddModeAllMember: "all_member_add",
}

// groupInfoOverClient is the default for the seam.
func groupInfoOverClient(ctx context.Context, client *wm.Client, group waTypes.JID) (*waTypes.GroupInfo, error) {
	return client.GetGroupInfo(ctx, group) //nolint:wrapcheck // classified by its caller
}
