package whatsmeow

import (
	"context"
	"encoding/json"
	"fmt"

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
	Group       protocol.Address `json:"group"`
	Subject     string           `json:"subject,omitempty"`
	Description string           `json:"description"`
	// TopicID is WhatsApp's own id for the description, passed on exactly as it arrives
	// and never interpreted here.
	//
	// It is what says a group's description can no longer be changed. A group whose
	// description was written by certain clients comes back with the literal string
	// `undefined` here, and from then on every edit is refused with a conflict, whatever
	// the stanza looks like -- measured on a group made for it, including a hand-built
	// stanza through DangerousInternals. Without the field a client can only offer "try
	// again", which in that group is false.
	//
	// Deliberately not an error code of its own. A conflict is genuinely ambiguous
	// between a frozen description and another admin writing between the read and the
	// write, and answering "refused permanently" would sell an inference as a fact. The
	// raw reading lets the client decide what to say, which is a product choice and not
	// this connector's to make.
	TopicID       string             `json:"topic_id,omitempty"`
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
	described := s.describeGroupItself(ctx, info)
	described.Participants = make([]groupParticipant, 0, len(info.Participants))
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

// describeGroupItself is everything about a group except who is in it.
//
// Split out because naming the members is what a listing must not pay for: `party` reads
// the LID mapping out of the device store for every namespace a participant row does not
// carry, so describing the roster of every group an account is in is thousands of
// sequential store reads -- on the one goroutine that owns the session, with every other
// command for it waiting behind. `group.list` needs none of them and answers `size`, which
// WhatsApp already counted.
func (s *Session) describeGroupItself(ctx context.Context, info *waTypes.GroupInfo) groupInfo {
	described := groupInfo{
		Subject:      info.Name,
		Description:  info.Topic,
		TopicID:      info.TopicID,
		Announce:     info.IsAnnounce,
		Locked:       info.IsLocked,
		JoinApproval: info.IsJoinApprovalRequired,
		Size:         info.ParticipantCount,
	}
	if address, named := addressOf(info.JID); named {
		described.Group = address
	}
	if !info.GroupCreated.IsZero() {
		described.CreatedAt = info.GroupCreated.UnixMilli()
	}
	// The owner is one party, not a roster, so it is worth the lookup it may cost.
	if owner := s.party(ctx, info.OwnerJID, info.OwnerPN); owner.Phone != "" || owner.LID != "" {
		described.Owner = &owner
	}
	if mode := memberAddModes[info.MemberAddMode]; mode != "" {
		described.MemberAddMode = mode
	}
	if described.Size == 0 {
		// The list WhatsApp sent, which is a count and not a translation: the fallback
		// holds for a listing as much as for one group.
		described.Size = len(info.Participants)
	}
	return described
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
		described := s.describeGroupItself(ctx, info)
		if described.Group.ID == "" {
			// A group WhatsApp named with something this connector cannot turn into an
			// address. whatsmeow keeps it: `parseGroupNode` answers a struct even when the
			// node was malformed, and `GetJoinedGroups` logs the parse error and appends
			// it anyway, so an entry with an empty JID reaches here.
			//
			// The whole listing fails rather than losing that one entry. A listing is a
			// statement about a set -- these are the groups -- and one silently short is
			// a statement that is false in a way no client can see: an answer of `[]` for
			// an account in one unreadable group says it is in none, and a client
			// reconciling against that removes a group it already knows about. The same
			// reasoning already keeps a partial roster off the wire.
			s.log.Error().Str("subject", described.Subject).
				Msg("WhatsApp listed a group with no address this build can read")
			return nil, protocol.NewError(protocol.ErrorInternal, "the group listing could not be read")
		}
		listed = append(listed, described)
	}
	return json.Marshal(listed)
}

// createRequest is `group.create`.
//
// `participants` is a pointer so that a payload which leaves it out can be told apart from
// one that sends an empty list. The two mean different things: an empty list is a group
// this account opens alone and fills in later, and a missing one is a payload the contract
// does not allow -- accepting it would create a group from a request that never said who
// was supposed to be in it.
type createRequest struct {
	Subject      string              `json:"subject"`
	Participants *[]protocol.Address `json:"participants"`
}

// createGroup carries out `group.create` and answers the group it made.
//
// The answer is a `group_info` like any other, with one difference that only a new group
// has: WhatsApp reports per participant whether it could add them, and somebody it refused
// is not in the group. Those rows are left out of the roster and out of `size`, because a
// caller reading them as members would show people a conversation they were never added
// to -- and the refusal is the ordinary case here, not the exception: a privacy setting
// that forbids being added to groups is exactly what a fresh group runs into.
func (s *Session) createGroup(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req createRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"creating a group has to say what to call it and who to put in it")
	}
	if req.Subject == "" {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "a group cannot be called nothing")
	}
	if req.Participants == nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"creating a group has to say who to put in it, even if that is nobody")
	}
	wanted := *req.Participants
	asked := make([]waTypes.JID, len(wanted))
	for i, party := range wanted {
		switch party.Kind {
		case protocol.AddressPhone, protocol.AddressLID:
		default:
			return nil, protocol.NewError(protocol.ErrorInvalidPayload,
				fmt.Sprintf("%q is not somebody who can be in a group", party.Kind))
		}
		var err error
		if asked[i], err = jidOf(party); err != nil {
			return nil, err
		}
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	made, err := s.createTheGroup(ctx, s.current(), wm.ReqCreateGroup{
		Name: req.Subject, Participants: asked,
	})
	if err != nil {
		// The context first, because this one call can lose it. `CreateGroup` reads the
		// LID mapping and a privacy token per participant before it sends anything, and
		// it wraps a failure there with `%v` rather than `%w` -- so a deadline that
		// expires mid-lookup arrives as text, `errors.Is` cannot see it, and a command
		// that ran out of time would be reported as a fault in this connector.
		if expired := ctx.Err(); expired != nil {
			return nil, contactFailure(expired, "group creation")
		}
		return nil, contactFailure(err, "group creation")
	}
	if made == nil {
		// whatsmeow answers an error for a group it could not make, so nothing should
		// reach here with neither. Reading the fields off it would take the session's
		// executor down along with every command queued behind it.
		return nil, protocol.NewError(protocol.ErrorInternal,
			"the group was created and WhatsApp said nothing about it")
	}
	return json.Marshal(s.describeGroup(ctx, withoutRefused(made)))
}

// withoutRefused drops the participants WhatsApp would not add, and the count with them.
//
// `GroupParticipant.Error` is filled in only here: an existing group's roster has nobody
// in it who is not in it, but a group just created reports the people whose privacy
// setting, block list or recent departure kept them out. They are on the list WhatsApp
// answered with, and they are not members.
func withoutRefused(made *waTypes.GroupInfo) *waTypes.GroupInfo {
	joined := make([]waTypes.GroupParticipant, 0, len(made.Participants))
	for i := range made.Participants {
		if member := &made.Participants[i]; member.Error == 0 {
			joined = append(joined, *member)
		}
	}
	if len(joined) == len(made.Participants) {
		return made
	}
	// A copy, because the caller's own struct is whatsmeow's and this is the connector's
	// reading of it.
	trimmed := *made
	trimmed.Participants = joined
	if trimmed.ParticipantCount > len(joined) {
		trimmed.ParticipantCount = len(joined)
	}
	return &trimmed
}
