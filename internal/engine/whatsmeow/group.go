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
	Description   string             `json:"description"`
	Owner         *protocol.Party    `json:"owner,omitempty"`
	CreatedAt     int64              `json:"created_at,omitempty"`
	Participants  []groupParticipant `json:"participants,omitempty"`
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

	// Read before the query, compared after it, the same as `groupModeCached` does and
	// for the same reason: a reconnection empties what is remembered so the first group
	// action on the new socket goes back to WhatsApp, and an answer already in flight
	// when that happened must not be written in behind it.
	on, _ := s.connection()

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
	// This query went to the wire, so its answer is the freshest reading of the group
	// there is. Filed for the paths that translate a participant into the group's own
	// namespace, which otherwise keep whatever the last one of them read -- a client
	// asking about a group is the one moment a migration becomes visible without anybody
	// paying a round trip for it. Deliberately not the other way round: `group.info` is a
	// client asking what the group *is*, and answering that out of a cache would report a
	// membership and a subject this session has not checked.
	s.rememberGroupMode(group, info.AddressingMode, on)

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
	if len(described.Participants) < described.Size {
		// A roster short of the group is left out rather than sent short. The client
		// reads any roster it is given as the whole of the group and deactivates every
		// membership missing from it, so an announcement group with one anonymous
		// participant would have every member it could not name removed from the
		// dashboard -- a sync that takes people out of a group they are still in. The
		// size still says how many there are, and a client that gets no roster leaves
		// the one it has alone.
		s.log.Info().Int("named", len(described.Participants)).Int("size", described.Size).
			Msg("left the roster out of a group description that could not account for every participant")
		described.Participants = nil
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

// joinedGroupsOverClient is the default for the seam.
func joinedGroupsOverClient(ctx context.Context, client *wm.Client) ([]*waTypes.GroupInfo, error) {
	return client.GetJoinedGroups(ctx) //nolint:wrapcheck // classified by its caller
}

// groupInfoOverClient is the default for the seam.
func groupInfoOverClient(ctx context.Context, client *wm.Client, group waTypes.JID) (*waTypes.GroupInfo, error) {
	return client.GetGroupInfo(ctx, group) //nolint:wrapcheck // classified by its caller
}

// listGroups carries out `group.list`: every group this account is in.
//
// Without the rosters, and that is the whole difference between this and asking about each
// group in turn. An account can be in hundreds of groups of hundreds of people, and a
// listing that carried every membership would answer with the entire address book of every
// conversation to say which conversations exist. `size` still says how big each one is,
// and `group.info` answers the roster for the group a caller actually opens.
//
// Absent, not empty: the contract's `participants` is optional and this is the same
// "not answered" that a partial roster is, which is what keeps a client from reading a
// listing as the whole of any group's membership and deactivating everybody missing.
func (s *Session) listGroups(ctx context.Context, _ *protocol.Command) (json.RawMessage, error) {
	if err := s.readyToSend(); err != nil {
		return nil, err
	}
	joined, err := s.joinedGroups(ctx, s.current())
	if err != nil {
		return nil, contactFailure(err, "group listing")
	}

	// Never nil: an empty list is the answer "this account is in no groups", and a client
	// reading `null` has to decide which of the two that is.
	listed := make([]groupInfo, 0, len(joined))
	for _, info := range joined {
		if info == nil {
			continue
		}
		described := s.describeGroup(ctx, info)
		described.Participants = nil
		listed = append(listed, described)
	}
	return json.Marshal(listed)
}
