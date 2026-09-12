package whatsmeow

import (
	"context"

	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// groupChanges is the contract's `changes`, and every field in it is a pointer or a slice
// for one reason: the contract distinguishes a setting that was turned off from one this
// notification did not mention, and a plain `bool` with `omitempty` renders both as
// absent. A client reading an absent field as "unchanged" is right; one reading `false`
// where nothing changed turns every notification about anything into a settings change.
//
// whatsmeow already models it this way -- `Name`, `Topic`, `Locked`, `Announce` and
// `MembershipApprovalMode` are pointers on the event, nil when the notification is silent
// about them -- so the rule here is only to carry that across rather than flatten it.
type groupChanges struct {
	Subject     *string          `json:"subject,omitempty"`
	Description *string          `json:"description,omitempty"`
	Announce    *bool            `json:"announce,omitempty"`
	Locked      *bool            `json:"locked,omitempty"`
	Join        []protocol.Party `json:"join,omitempty"`
	Leave       []protocol.Party `json:"leave,omitempty"`
	Promote     []protocol.Party `json:"promote,omitempty"`
	Demote      []protocol.Party `json:"demote,omitempty"`
}

// empty reports whether this carries nothing the client can act on.
func (c *groupChanges) empty() bool {
	return c.Subject == nil && c.Description == nil && c.Announce == nil &&
		c.Locked == nil &&
		len(c.Join) == 0 && len(c.Leave) == 0 && len(c.Promote) == 0 && len(c.Demote) == 0
}

// groupUpdate is the contract's `group.updated`.
type groupUpdate struct {
	Group     protocol.Address `json:"group"`
	Actor     *protocol.Party  `json:"actor,omitempty"`
	Timestamp int64            `json:"timestamp,omitempty"`
	Changes   groupChanges     `json:"changes"`
}

// groupActivity is the contract's `group.activity`: this group changed and the connector
// cannot say how.
type groupActivity struct {
	Groups []protocol.Address `json:"groups"`
}

// joinedGroup is the contract's `group.joined`.
type joinedGroup struct {
	Info groupInfo `json:"info"`
}

// joinedAGroup publishes `group.joined` for a group the account was added to.
//
// The whole description, roster included, because this is the client's first sight of a
// group it is now in and there is nothing on its side to merge into. It costs what
// `group.info` costs, which is a mapping read per participant this notification did not
// name both namespaces for -- bounded by one group, and paid once.
func (s *Session) joinedAGroup(event *waEvents.JoinedGroup) {
	if !s.wantsGroups() {
		// The client asked for direct chats only, and this is group traffic like any
		// other: `session.connect` decides whether groups reach it at all, and a session
		// that publishes a group it was not asked about has the client opening a
		// conversation for a chat it will never receive a message in.
		return
	}
	ctx, cancel := s.looking()
	defer cancel()

	if _, named := addressOf(event.JID); !named {
		s.log.Warn().Msg("dropping a group the account joined that has no address to publish it under")
		return
	}
	// Through the same filter the create command answers through, and for the same
	// reason: a group just created reports the people WhatsApp would not add -- a privacy
	// setting, a block list, a recent departure -- as rows on its own participant list,
	// with `Error` filled in. They are on the list WhatsApp sent and they are not members,
	// and this roster is the one a client builds the group's membership out of, so
	// publishing them adds people to a group they were kept out of.
	s.emit(protocol.EventGroupJoined, joinedGroup{
		Info: s.describeGroup(ctx, withoutRefused(&event.GroupInfo)),
	})
}

// groupChanged publishes what WhatsApp says changed about a group.
//
// Two events come out of one notification, and which one depends on whether anything in
// it fits the contract. What fits is published as `group.updated`; a notification that
// reported something and none of it fits is published as `group.activity`, which is the
// contract's way of saying a group moved without saying how, and which a client answers
// with a metadata query of its own. That second branch is not a nicety: WhatsApp reports
// changes this contract has no field for -- disappearing messages, community links,
// invite link resets, suspension -- and whatsmeow reports ones it does not recognise at
// all, and without it every one of them is silence indistinguishable from nothing having
// happened.
//
// One notification can produce both, and only when it reported something on each side.
// whatsmeow's parser walks every child of the `w:gp2` node into one `GroupInfo`, so a
// rename and a disappearing-message timer arriving in the same breath are one event with
// `Name` and `Ephemeral` both set. Publishing only `group.updated` there would carry the
// rename and swallow the timer, and nothing afterwards would tell the client to go and
// look: a `group.updated` is a statement about what changed, so a client that got one has
// no reason to suspect the rest.
//
// What is not done is emitting the pair whenever `group.updated` goes out. A soft sync
// alongside a change the client has just been handed is a metadata query for what it
// already has, and that is the whole of why the second event is conditioned on residue
// rather than on there having been an update at all.
func (s *Session) groupChanged(event *waEvents.GroupInfo) {
	if !s.wantsGroups() {
		return
	}
	ctx, cancel := s.looking()
	defer cancel()

	group, named := addressOf(event.JID)
	if !named {
		s.log.Warn().Msg("dropping a group change for a group with no address to publish it under")
		return
	}

	changes := s.describeChanges(ctx, event)
	if changes.empty() {
		if reportsNothing(event) {
			// A participant version bump on its own, which says the roster has a new
			// version and not what it is. Nothing to publish and nothing to go and read.
			return
		}
		s.emit(protocol.EventGroupActivity, groupActivity{Groups: []protocol.Address{group}})
		return
	}

	update := groupUpdate{Group: group, Changes: changes}
	if !event.Timestamp.IsZero() {
		update.Timestamp = event.Timestamp.UnixMilli()
	}
	if actor := s.actorOf(ctx, event); actor != nil {
		update.Actor = actor
	}
	s.emit(protocol.EventGroupUpdated, update)

	if reportsResidue(event) {
		// The update goes first and the sync second, because the two say different things
		// about the same instant: one is the line an operator reads, the other is a
		// prompt to go and read state. Reversed, a client that syncs on the prompt can
		// land its query before the update it already had the answer to.
		s.emit(protocol.EventGroupActivity, groupActivity{Groups: []protocol.Address{group}})
	}
}

// actorOf names whoever made the change, when the notification says who.
//
// `notify=invite` is the case that does not: WhatsApp reports somebody joining by invite
// link with no participant on the node, and the contract has `actor` optional precisely
// so a producer can say it does not know rather than guess.
func (s *Session) actorOf(ctx context.Context, event *waEvents.GroupInfo) *protocol.Party {
	jids := make([]waTypes.JID, 0, 2)
	if event.Sender != nil {
		jids = append(jids, *event.Sender)
	}
	if event.SenderPN != nil {
		jids = append(jids, *event.SenderPN)
	}
	// One guard for both ways of having no actor: a notification that named nobody, and one
	// that named somebody this connector cannot address. `party` with nothing to go on
	// returns an empty one, so the two arrive here the same way and leave it the same way.
	actor := s.party(ctx, jids...)
	if actor.Phone == "" && actor.LID == "" {
		return nil
	}
	return &actor
}

// describeChanges maps the notification onto the contract, field by field, and carries
// across only what the notification actually spoke about.
func (s *Session) describeChanges(ctx context.Context, event *waEvents.GroupInfo) groupChanges {
	var changes groupChanges
	if event.Name != nil {
		subject := event.Name.Name
		changes.Subject = &subject
	}
	if event.Topic != nil {
		// A deleted description is the empty string rather than an absent field: the
		// contract's absent means "this notification did not mention the description",
		// and a group whose description was cleared did have it mentioned.
		description := event.Topic.Topic
		if event.Topic.TopicDeleted {
			description = ""
		}
		changes.Description = &description
	}
	if event.Announce != nil {
		announce := event.Announce.IsAnnounce
		changes.Announce = &announce
	}
	if event.Locked != nil {
		locked := event.Locked.IsLocked
		changes.Locked = &locked
	}
	// `MembershipApprovalMode` is deliberately not carried, and it is the one field on
	// this event that is left out rather than mapped. whatsmeow's notification parser has
	// a case for `membership_approval_mode` and none for its negation -- no
	// `not_membership_approval_mode` beside it, the way `not_announcement` sits beside
	// `announcement` and `unlocked` beside `locked` -- so it fills the field with `true`
	// whenever the tag is present at all. Whether WhatsApp sends that tag only when
	// approval is switched on, or sends it in both directions with the state in a child
	// (which is how whatsmeow's own `SetGroupJoinApprovalMode` writes it), decides whether
	// `true` here is the truth or its opposite, and nothing available offline tells them
	// apart.
	//
	// So it goes out as `group.activity` instead: the client answers that with a metadata
	// query, and the snapshot parser reads approval by the presence of the element, which
	// is right in both directions. Being wrong this way costs one query per change,
	// throttled by the client; being wrong the other way writes "approval turned on" into
	// an operator's timeline at the moment somebody turned it off, and nothing afterwards
	// contradicts it.
	changes.Join = s.parties(ctx, event.Join)
	changes.Leave = s.parties(ctx, event.Leave)
	changes.Promote = s.parties(ctx, event.Promote)
	changes.Demote = s.parties(ctx, event.Demote)
	return changes
}

// parties names a list of participants, dropping whoever this connector cannot name.
//
// The contract has no party without an address, so a row that would be empty is left out
// rather than sent hollow. Unlike the roster in a group description, a short list here is
// not read as the whole of anything: `join` says who joined, not who is in the group, so
// leaving somebody out understates the change instead of removing them from it.
func (s *Session) parties(ctx context.Context, jids []waTypes.JID) []protocol.Party {
	if len(jids) == 0 {
		return nil
	}
	named := make([]protocol.Party, 0, len(jids))
	for _, jid := range jids {
		party := s.party(ctx, jid)
		if party.Phone == "" && party.LID == "" {
			s.log.Info().Msg("left a participant out of a group change that could not be named")
			continue
		}
		named = append(named, party)
	}
	return named
}

// reportsResidue is a notification carrying something `groupChanges` has no field for.
//
// These are the changes WhatsApp reports and this contract does not spell out --
// disappearing messages, join approval, community links and unlinks, an invite link
// reset, deletion, suspension -- plus whatever whatsmeow could not parse at all. None of
// them can be said in a `group.updated`, so the only way to say a group moved is
// `group.activity` and a metadata query on the client's side.
//
// It is deliberately the complement of what `describeChanges` maps rather than a list
// that happens to sit next to it: every field of `waEvents.GroupInfo` is on exactly one
// of the two sides, and `TestEveryFieldOfAGroupNotificationIsMappedOrResidue` is what
// keeps it that way when whatsmeow grows a field.
func reportsResidue(event *waEvents.GroupInfo) bool {
	return event.Ephemeral != nil || event.MembershipApprovalMode != nil ||
		event.Delete != nil || event.Link != nil || event.Unlink != nil ||
		event.NewInviteLink != nil || event.Suspended || event.Unsuspended ||
		len(event.UnknownChanges) > 0
}

// reportsNothing is a notification that changed nothing anybody can act on: no setting,
// no membership, and nothing whatsmeow failed to parse either.
func reportsNothing(event *waEvents.GroupInfo) bool {
	return event.Name == nil && event.Topic == nil && event.Locked == nil &&
		event.Announce == nil &&
		len(event.Join) == 0 && len(event.Leave) == 0 &&
		len(event.Promote) == 0 && len(event.Demote) == 0 &&
		!reportsResidue(event)
}
