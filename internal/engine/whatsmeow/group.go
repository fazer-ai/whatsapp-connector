package whatsmeow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	wm "go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
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

// keyedCreate is one group this connector asks WhatsApp to make, and the name it asks
// under.
//
// The key is the whole reason this is not `wm.ReqCreateGroup`: whatsmeow's own
// `CreateGroup` sends no key, and without one an answer that goes missing cannot be tied
// back to the command that asked for it.
type keyedCreate struct {
	Subject      string
	Participants []waTypes.JID
	// Key travels on the `create` stanza and comes back on the notification WhatsApp sends
	// about the group. whatsmeow reads it off that notification as `JoinedGroup.CreateKey`.
	Key string
}

// createKeyedGroupOverClient is the default for the seam: whatsmeow's own `CreateGroup`,
// with the key on it and without the settings this connector never asks for.
//
// Hand-built because the key has nowhere else to go. `ReqCreateGroup` has no field for one
// -- `JoinedGroup.CreateKey` documents a parameter that no longer exists on the request --
// so the alternative is a fork of the library to add it, which is a dependency this
// repository would then own. Everything the stanza needs is exported: the mapping and the
// privacy token per participant, the IQ, and the parser for the answer, so none of what
// whatsmeow knows about reading a group back is reimplemented here.
//
// What is copied is the shape of the request, and it is copied for one request shape only:
// a plain group with a subject and a list of people. `group.create` sends nothing else --
// no community, no lock, no announcement, no disappearing timer, no join approval -- so the
// three settings below are the defaults `CreateGroup` fills in, spelled out rather than
// inherited. A `group.create` that grows an option grows a node here, and the test that
// pins this stanza is what says so.
func createKeyedGroupOverClient(
	ctx context.Context, client *wm.Client, req keyedCreate,
) (*waTypes.GroupInfo, error) {
	// Deprecated as "dangerous", and taken deliberately: the four calls below are the
	// library's own, reached this way only because the key has no other route out. What the
	// warning is about is reaching past the library's contract, which is what the comment
	// above spells out the terms of.
	inside := client.DangerousInternals() //nolint:staticcheck // the only route a keyed create has
	asked := make([]waBinary.Node, 0, len(req.Participants)+3)
	for _, participant := range req.Participants {
		participant = participant.ToNonAD()
		attrs := waBinary.Attrs{"jid": participant}
		if participant.Server == waTypes.HiddenUserServer {
			// A participant named by LID goes out with the phone number too, the same as
			// whatsmeow sends it. A LID with no mapping on record is sent as it stands:
			// WhatsApp answers for it, and refusing here would turn a participant this
			// connector has never spoken to into a failed creation.
			phone, err := client.Store.LIDs.GetPNForLID(ctx, participant)
			if err != nil {
				return nil, fmt.Errorf("%w: read the phone number of %s: %w", errCreateUnsent, participant, err)
			}
			if !phone.IsEmpty() {
				attrs["phone_number"] = phone
			}
		}
		node := waBinary.Node{Tag: "participant", Attrs: attrs}
		token, err := inside.EnsureTCToken(ctx, participant)
		if err != nil {
			return nil, fmt.Errorf("%w: read the privacy token of %s: %w", errCreateUnsent, participant, err)
		}
		if len(token) > 0 {
			node.Content = []waBinary.Node{{Tag: "privacy", Content: token}}
		}
		asked = append(asked, node)
	}
	answer, err := inside.SendIQ(ctx, theCreateQuery(req.Subject, req.Key, asked))
	if err != nil {
		return nil, fmt.Errorf("create a group: %w", err)
	}
	group, found := answer.GetOptionalChildByTag("group")
	if !found {
		return nil, &wm.ElementMissingError{Tag: "group", In: "response to create group query"}
	}
	return inside.ParseGroupNode(&group) //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
}

// theCreateQuery is the whole IQ a group is asked for with.
//
// Its own function so a test can read it without a socket, and `NoRetry` is why that
// matters as much as the stanza does. whatsmeow retries an IQ whose socket died by waiting
// for the reconnection and sending the same frame again, and when that reconnection does
// not come it answers `ErrNotConnected` -- after the first frame has already gone out. That
// answer is indistinguishable from the one a request that never left gets, and this
// connector reads the latter as "no group was made" and forgets the intent. So a creation
// that WhatsApp had already accepted would be made a second time by the next delivery,
// which is the whole of #131 arriving through the library's own retry.
//
// Turned off rather than untangled: with it off, `ErrNotConnected` can only come from the
// socket check that runs before anything is written, and a socket that dies while the
// answer is outstanding comes back as a `DisconnectedError`, which this connector treats as
// what it is -- a creation that may well have happened. What is given up is one automatic
// retry of a request the caller retries anyway, under the same key and the same record.
func theCreateQuery(subject, key string, people []waBinary.Node) wm.DangerousInfoQuery {
	return wm.DangerousInfoQuery{
		Namespace: "w:g2",
		Type:      createIQ,
		To:        waTypes.GroupServerJID,
		NoRetry:   true,
		Content:   []waBinary.Node{theCreateStanza(subject, key, people)},
	}
}

// theCreateStanza is the `create` node a group is asked for with, people and all.
//
// The key is the attribute this whole mechanism turns on; the three settings after the
// participants are the ones whatsmeow's `CreateGroup` fills in for a request that asks for
// none of them, and they are spelled out here because nothing else is filling them in any
// more.
func theCreateStanza(subject, key string, people []waBinary.Node) waBinary.Node {
	content := make([]waBinary.Node, 0, len(people)+3)
	content = append(content, people...)
	content = append(content,
		waBinary.Node{Tag: "member_add_mode", Content: string(waTypes.GroupMemberAddModeAllMember)},
		waBinary.Node{Tag: "ephemeral", Attrs: waBinary.Attrs{"expiration": 0}},
		waBinary.Node{Tag: "membership_approval_mode", Content: []waBinary.Node{{
			Tag: "group_join", Attrs: waBinary.Attrs{"state": "off"},
		}}},
	)
	return waBinary.Node{
		Tag:     "create",
		Attrs:   waBinary.Attrs{"subject": subject, "key": key},
		Content: content,
	}
}

// createIQ is the `set` a creation is sent as. whatsmeow keeps the constant unexported and
// exports the type, so the string is spelled here.
const createIQ = wm.DangerousInfoQueryType("set")

// errCreateUnsent marks a creation that failed before the stanza could go out.
//
// It is what separates "this connector could not build the request" from "WhatsApp was
// asked and something went wrong": the first made no group and the record of it can be
// forgotten, and reading the second as the first is how a group that exists loses the only
// note saying it does.
var errCreateUnsent = errors.New("the creation was never sent")

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

	// Everything above this line refuses without touching WhatsApp, and nothing above it
	// writes: a payload this connector will not send, or a command nobody is waiting for
	// any more, must leave no trace that a later delivery would take for an attempt.
	attempt := attemptName(command)
	key, err := newCreateKey()
	if err != nil {
		return nil, protocol.NewError(protocol.ErrorInternal, "a group creation could not be named")
	}
	// Bounded, and derived from the caller's context rather than the session's, which is
	// the opposite of the abandon write thirty lines down. The two differ in when they
	// run: this one runs before any request, so a caller who has stopped waiting has
	// nothing to lose by it being dropped, and a ceiling shorter than this one is theirs
	// to set and should win. The abandon write runs after a creation failed, when the
	// caller's deadline may be exactly what expired, and it has to outlive that.
	//
	// The number is `storeLimit`, the same one every other store write in this package
	// uses, and the reason is not that it was picked for a write like this one: it was
	// picked for a bind. The question it answers here is the one it answers everywhere in
	// this package, which is how long this connector is willing to hold an account's
	// whole command queue on a single store call, and that answer does not depend on
	// which call it is. #284 measured the cost of having no answer at all: with the store
	// stalled, a `group.create` carrying neither ceiling field did not come back.
	//
	// What it bounds is the wait, not every way a store can be slow, and the difference is
	// a dialect's. On PostgreSQL the deadline reaches the server, which cancels the
	// statement in flight. On SQLite a write already blocked on another writer of the same
	// file sits out `busy_timeout` before it looks at the context at all: measured at
	// 10.09s against a three hundred millisecond deadline, on this repository's own driver
	// and pragmas. The error that comes back is the context's and the time is not, and it
	// is per call rather than per command -- a second bounded write that starts with a
	// live context gets its own window, so the worst case for a creation on that dialect
	// is two of them, against the two five second ceilings this function declares.
	// `busy_timeout` and `storeLimit` are two ceilings that do not know about each other
	// and the smaller does not win, which is #293 and is true of every `storeLimit` in
	// this package rather than anything #284 introduced. It is written here because "the
	// command comes back inside the ceiling" is otherwise a claim wider than the
	// measurement behind it.
	writing, written := context.WithTimeout(ctx, s.storeLimit)
	began, begun, err := s.store.BeginGroupCreate(writing, attempt, key, req.Subject, time.Now())
	expired := writing.Err()
	written()
	if err != nil {
		// The intent could not be written, so the cover is not there. Refused rather than
		// created: the caller's retry costs them a command, and creating anyway costs
		// them a group they cannot tell from the one a redelivery would make.
		return nil, s.intentFailure(ctx, expired, err, attempt)
	}
	if begun {
		// This command has been here before, so a group may already exist for it and this
		// delivery does not get to make another. It is answered with the group WhatsApp
		// named, or refused until WhatsApp names one.
		made, err := s.groupFromEarlierAttempt(ctx, attempt, began)
		if err != nil {
			return nil, err
		}
		return json.Marshal(s.describeGroup(ctx, made))
	}

	// Under the key that is on record rather than the one drawn above. The two are the same
	// on the only delivery that reaches here, and this is the value that has to go out: it
	// is what WhatsApp echoes on the notification, and what the record is found by.
	made, err := s.createTheGroup(ctx, s.current(), keyedCreate{
		Subject: req.Subject, Participants: asked, Key: began.Key,
	})
	if err != nil {
		// What the failure says about the group comes first, before what it says about the
		// caller's clock. A deadline that expired while the participants were being looked
		// up is still a creation that never left, and reading it as "ran out of time, so
		// who knows" would leave an intent behind for a group that was never asked for --
		// and every later delivery under that name waiting on a notification about it.
		if nothingWasMade(err) {
			// WhatsApp answered, and the answer was no -- or the stanza never left. Either
			// way there is no group, and an intent left open here would have every later
			// request under this name wait on a notification that is never coming, until
			// the sweep takes the row. Forgotten, so a refusal costs a command and no more.
			//
			// On a window of its own, because the caller's may be exactly what expired.
			forgetting, forgotten := context.WithTimeout(s.ctx, s.storeLimit)
			defer forgotten()
			if forget := s.store.AbandonGroupCreate(forgetting, attempt); forget != nil {
				s.log.Error().Err(forget).Str("sid", s.sid).Str("attempt", attempt).
					Msg("could not forget a creation that made nothing; retries of it will wait on a notification that is not coming")
			}
		} else if expired := ctx.Err(); expired != nil {
			// The request may have gone out and been answered after nobody was listening.
			// The intent stays, which is what a later delivery is answered from.
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
	// Named as soon as there is a name, and before the answer goes back. A failure here is
	// logged rather than returned: the group exists, and reporting a failure would have the
	// client retry a creation that happened -- the very duplicate this exists to prevent.
	// It is also not the only chance to write it down: WhatsApp's own notification about
	// this group carries the key, and `joinedAGroup` records the pair when it arrives.
	//
	// On a window of its own, and off the session's context rather than the caller's, for
	// the reason the abandon write above has: the group exists now, so this write has to
	// happen whether or not whoever asked is still waiting. Bounded all the same, because
	// a store that stopped answering would otherwise hold this command, and every command
	// queued behind it for that account, for as long as the process runs -- which #284
	// measured happening here as well as at the intent write, so closing only the first
	// would have left the account held at the second.
	naming, named := context.WithTimeout(s.ctx, s.storeLimit)
	defer named()
	if err := s.store.FinishGroupCreate(naming, attempt, made.JID.String()); err != nil {
		s.log.Error().Err(err).Str("sid", s.sid).Str("attempt", attempt).
			Msg("made a group and could not record which one; a redelivery will have to look for it")
	}
	return json.Marshal(s.describeGroup(ctx, withoutRefused(made)))
}

// intentFailure names what went wrong writing a creation's intent, and it exists because
// the shared answer is wrong here in both halves.
//
// `commandFailure` turns a `context.DeadlineExceeded` into `timeout` with "did not go out
// before the command's deadline". Over this write that says two false things at once:
// nothing went out, because the intent is written before any request, and the deadline
// that passed is this connector's own rather than the caller's. A client told `timeout`
// reads it as WhatsApp not answering and retries against WhatsApp; what actually happened
// is that the local store did not answer this connector.
//
// So the caller's clock keeps its word and this connector's ceiling gets the one the
// package already uses for a store that will not answer: `internal`, which
// `TestACreationThatCouldNotRecordItsIntentIsStillInternal` pins for a store that is
// down, and which says the same thing about a store that is merely too slow to be waited
// on -- this connector could not carry the command out, and it settles when somebody
// fixes the store. #291 is where the two stop being distinguishable only by their
// message; the log line below is what separates them until then.
//
// Which ceiling fired is read off the contexts and not off the error, and that is the
// whole reason this takes `expired`. The first version of it asked
// `errors.Is(err, context.DeadlineExceeded)`, which is true when the wait was for a free
// connection in the pool and false when the query was already in flight: PostgreSQL
// cancels the statement and `lib/pq` reports `canceling statement due to user request`,
// which wraps nothing. Measured against a second pool holding the row's lock, that is
// exactly what comes back, so the version that read the error would have taken the
// deployment's own path -- contention on the row -- down the branch for an ordinary store
// failure, with the suite green because the suite's stall is the pool one.
func (s *Session) intentFailure(ctx context.Context, expired, err error, attempt string) error {
	switch {
	case ctx.Err() != nil:
		// The caller's own clock, which is the case `commandFailure` was written for.
		// Asked first because the two are not exclusive: `expired` is the derived
		// context's error and a dead caller kills the child with it, so asking the other
		// way round would call every caller ceiling ours.
		return contactFailure(context.Cause(ctx), "group creation")
	case expired == nil:
		// Not a ceiling at all: the store answered, and what it answered was a failure.
		return contactFailure(err, "group creation")
	}
	s.log.Error().Err(err).Str("attempt", attempt).
		Dur("store_limit", s.storeLimit).
		Msg("the store did not answer in time to record a group creation; refused before anything was asked of WhatsApp")
	return protocol.NewError(protocol.ErrorInternal,
		"the store did not answer in time to record the group creation")
}

// nothingWasMade reports whether a failed creation is one that certainly made no group.
//
// An answer from WhatsApp is one: an IQ error is a reply, and a reply saying no is a group
// that does not exist. So is a request that never left this process, whether because there
// was no socket to send it on (`sentNothing`, which the teardowns already name) or because
// the stanza could not be built (`errCreateUnsent`). Everything else -- a socket that went
// while the request was in flight, a deadline that passed -- is the ambiguous case this
// whole mechanism is for, and saying "nothing was made" about one of those is how the record
// that covers a duplicate gets thrown away.
func nothingWasMade(err error) bool {
	var refused *wm.IQError
	return errors.As(err, &refused) || sentNothing(err) || errors.Is(err, errCreateUnsent)
}

// newCreateKey is the name one creation travels under.
//
// Random rather than derived from the command, and it does not need to be either: the key
// is written with the intent, before anything is sent, so a redelivery reads the key its
// first delivery used instead of recomputing it. Random is what keeps two sessions, or two
// deployments, from ever sending the same one -- a key WhatsApp echoes onto a notification
// is only useful while it names exactly one attempt.
func newCreateKey() (string, error) {
	var drawn [12]byte
	if _, err := rand.Read(drawn[:]); err != nil {
		return "", fmt.Errorf("draw a name for a group creation: %w", err)
	}
	return "WAC" + strings.ToUpper(hex.EncodeToString(drawn[:])), nil
}

// attemptName is what a creation is filed under, and it is the name the caller gave the
// command, not one of this connector's making.
//
// The same two names the ledger in `internal/session` keys by, and for the same reason: an
// `idempotency_key` is the caller saying "this is one request however many times you see
// it", and a command that carries none still arrives with the id the transport redelivers
// it under. Prefixed apart so a key and an id that happen to read the same are not one
// attempt.
func attemptName(command *protocol.Command) string {
	if command.IdempotencyKey != "" {
		return "idem:" + command.IdempotencyKey
	}
	return "cmd:" + command.ID
}

// groupFromEarlierAttempt answers the group an earlier delivery of this command made.
//
// One thing answers it, and it is WhatsApp: the creation went out carrying a key, the
// notification announcing the group comes back with that key, and `joinedAGroup` writes the
// pair down. The notification survives the instance that asked for it -- WhatsApp redelivers
// what nobody acknowledged, key intact, which is what `probe131b` measured -- so it reaches
// whoever holds the session next, which is exactly the instance a redelivered command lands
// on. An attempt with no group on record has not been told yet, so it waits to be.
//
// Nothing here reads the group listing, and that is deliberate. A listing can be made to
// say "no group of mine by that name exists after that instant", which reads like proof
// that nothing was made, and it is not: a participant renaming the group, or a clock that
// disagrees with WhatsApp's by a second, turns a group this attempt made into one the
// listing does not account for, and the command makes a second one. The only thing that
// says a group was made is WhatsApp saying so, and the only thing that says none was is a
// request that certainly never left.
//
// What that costs, stated rather than implied: a creation that did go out and that WhatsApp
// never answered leaves the command refused until the record is swept. Every delivery of it
// waits and refuses, which converges the moment the notification arrives and does not
// converge at all if there is nothing to arrive. It is the ambiguous case this whole
// mechanism is for, and the alternative to refusing it is the second group.
func (s *Session) groupFromEarlierAttempt(
	ctx context.Context, attempt string, began store.GroupCreation,
) (*waTypes.GroupInfo, error) {
	if !began.Done() {
		var err error
		if began, err = s.waitForTheGroupItMade(ctx, attempt, began); err != nil {
			return nil, err
		}
	}
	jid, err := waTypes.ParseJID(began.JID)
	if err != nil {
		return nil, protocol.NewError(protocol.ErrorInternal,
			"the group this command already made is on record under a name that is not a group")
	}
	made, err := s.groupInfo(ctx, s.current(), jid)
	if err != nil {
		return nil, contactFailure(err, "group creation")
	}
	if made == nil {
		// whatsmeow answers an error for a group it cannot read, so nothing should reach
		// here with neither. Reading the fields off it would take the session's executor
		// down along with every command queued behind it.
		return nil, protocol.NewError(protocol.ErrorInternal,
			"the group this command made could not be read back")
	}
	return made, nil
}

// waitForTheGroupItMade waits for WhatsApp to say which group an attempt made.
//
// Only ever reached with a group that could be this attempt's already on WhatsApp, so what
// is being waited for is the notification that names it -- a message already sent, not one
// that has to be asked for. It is short: the notification is redelivered as the socket
// comes back, ahead of the command in all but a race, and the session's own goroutine is
// what is being held.
//
// Refuses when it runs out, because the alternative is worse in both directions: creating
// would make the second group this exists to prevent, and answering with a group picked out
// of the listing would hand this request whatever else has that name. The refusal converges
// -- the notification lands and the next delivery is answered from the record.
func (s *Session) waitForTheGroupItMade(
	ctx context.Context, attempt string, began store.GroupCreation,
) (store.GroupCreation, error) {
	waited, giveUp := context.WithTimeout(ctx, s.createWait)
	defer giveUp()
	asking := time.NewTicker(createNoticePoll)
	defer asking.Stop()
	for {
		select {
		case <-waited.Done():
			if expired := ctx.Err(); expired != nil {
				// The caller's own deadline, not this window: the command ran out of time,
				// which is a `timeout` on the wire and not the connector failing to tell
				// two requests apart. `waited` descends from it, so without this the two
				// are one case and which error the client sees depends on whether the
				// clock ran out during a read or between them.
				return began, contactFailure(expired, "group creation")
			}
			return began, s.notSettledYet(attempt, began)
		case <-asking.C:
			if s.lookingForNotice != nil {
				// A test's one chance to act while this command is demonstrably inside the
				// wait, which no sleep can establish.
				s.lookingForNotice()
			}
			// On the waiting window rather than the caller's, so a store that has stopped
			// answering ends the wait instead of holding the session's goroutine: a
			// `group.create` whose caller named no deadline has no other ceiling, and every
			// command queued behind this one waits with it.
			told, found, err := s.store.GroupCreation(waited, attempt)
			if err != nil {
				if ctx.Err() == nil && waited.Err() != nil {
					// The window ran out mid-read, which is the window doing its job and
					// not a fault. Answered by ending the loop rather than here, so the
					// window has one exit however the clock falls: a read that straddles
					// the deadline and one that finishes just inside it must not be two
					// different answers to the caller.
					continue
				}
				return began, contactFailure(err, "group creation")
			}
			if found && told.Done() {
				s.log.Info().Str("sid", s.sid).Str("attempt", attempt).Str("group", told.JID).
					Msg("WhatsApp named the group a redelivered creation had already made")
				return told, nil
			}
		}
	}
}

// notSettledYet is what a delivery is answered with while WhatsApp has not said which group
// its creation made.
//
// `not_settled` and not `internal`: the outcome exists, the connector will know which one
// it is as soon as WhatsApp's notification names the group, and a client that asks again
// in a moment gets a real answer. `internal` read as this connector having a bug, which
// this is not, and it gave a client no way to tell a retry worth making from one worth
// paging somebody about (#214).
func (s *Session) notSettledYet(attempt string, began store.GroupCreation) error {
	s.log.Warn().Str("sid", s.sid).Str("attempt", attempt).Str("subject", began.Subject).
		Msg("a creation is on record and WhatsApp has not said yet which group it made")
	return protocol.NewError(protocol.ErrorNotSettled,
		"a group by this name was made and which request made it is not settled yet")
}

// How long a redelivered creation waits for WhatsApp to name the group it made, and how
// often it looks. The wait is bounded by the caller's own deadline as well.
//
// Sized for the case the mechanism rests on rather than for a local race. What it waits for
// is a notification WhatsApp held because nobody acknowledged it, and that one arrives with
// the rest of what the socket missed while it was down: after the reconnection, alongside
// the redelivered command itself, in an order nothing here guarantees. A window sized for
// two local deliveries crossing would expire in the middle of that sync and refuse a
// command whose answer was seconds away.
//
// What it costs is the session's goroutine, and the ceiling to measure that against is this
// command's own: whatsmeow gives a creation IQ 75 seconds before giving up, so a
// `group.create` can already hold that goroutine for longer than this. It is also only ever
// paid by a redelivery of a creation nothing has settled, which is the rare path.
const (
	createNoticeWait = 30 * time.Second
	createNoticePoll = 50 * time.Millisecond
)

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
