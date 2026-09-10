package whatsmeow

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	waBinary "go.mau.fi/whatsmeow/binary"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

const theGroup = "120363041234567890"

func groupSession(t *testing.T) *Session {
	t.Helper()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	// The subscription the client asks for on `session.connect`. Every case below is
	// about what a session that wanted groups publishes; the one below that is about
	// what a session that did not want them must not.
	session.setGroups(true)
	return session
}

func groupJID() waTypes.JID { return waTypes.NewJID(theGroup, waTypes.GroupServer) }

func someone(phone string) waTypes.JID {
	return waTypes.NewJID(phone, waTypes.DefaultUserServer)
}

// contractShape compiles one definition out of the contract, so a payload is held to what
// a client validates rather than to what this test expects to see.
func contractShape(t *testing.T, definition string) *jsonschema.Schema {
	t.Helper()

	compiled, err := jsonschema.NewCompiler().Compile(
		filepath.Join("..", "..", "..", "contract", "schema", "protocol.schema.json") +
			"#/definitions/" + definition,
	)
	if err != nil {
		t.Fatalf("compile %s: %v", definition, err)
	}
	return compiled
}

// published reads what the session put out and holds it to the contract.
func published(t *testing.T, session *Session, want protocol.EventType, definition string) map[string]any {
	t.Helper()

	emission := next(t, session)
	if emission.Type != want {
		t.Fatalf("published %q, want %q", emission.Type, want)
	}
	var payload any
	if err := json.Unmarshal(emission.Payload, &payload); err != nil {
		t.Fatalf("unmarshal the payload: %v", err)
	}
	if err := contractShape(t, definition).Validate(payload); err != nil {
		t.Fatalf("%s does not validate against the contract: %v", want, err)
	}
	return decode(t, emission.Payload)
}

// nothingPublished fails when anything comes out, which is what half the cases here are
// about: a notification carrying nothing the contract can say must not turn into an event
// saying nothing.
func nothingPublished(t *testing.T, session *Session) {
	t.Helper()

	select {
	case emission := <-session.Events():
		t.Fatalf("published %q for a notification with nothing in it", emission.Type)
	case <-time.After(200 * time.Millisecond):
	}
}

func changesIn(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()

	changes, ok := payload["changes"].(map[string]any)
	if !ok {
		t.Fatalf("the payload carries no changes object: %v", payload)
	}
	return changes
}

// `session.connect` decides whether group traffic reaches a client at all, and these
// three events are group traffic like the messages, the receipts and the typing
// indicators that already select on it. A client that asked for direct chats only and is
// handed a `group.joined` opens a conversation for a chat no message will ever arrive
// in, because the very next group message is acknowledged and dropped by the path beside
// this one.
func TestAGroupIsNotPublishedToASessionThatDidNotAskForGroups(t *testing.T) {
	t.Parallel()

	for name, event := range map[string]any{
		"being added to one": &waEvents.JoinedGroup{
			GroupInfo: waTypes.GroupInfo{JID: groupJID(), GroupName: waTypes.GroupName{Name: "Equipe fazer.ai"}},
		},
		"one changing": &waEvents.GroupInfo{
			JID: groupJID(), Name: &waTypes.GroupName{Name: "Equipe fazer.ai"},
		},
		"one changing in a way the contract cannot carry": &waEvents.GroupInfo{
			JID: groupJID(), Ephemeral: &waTypes.GroupEphemeral{IsEphemeral: true},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			// Deliberately not `setGroups`: false is the default a client gets by not
			// asking, which is the case this is about.

			session.handle(event)
			nothingPublished(t, session)
		})
	}
}

// The whole point of the issue: a group changing produces an event at all. whatsmeow
// delivers the notification and, before this, `handle` had no case for it, so it fell
// through to the acknowledgement at the bottom and a native inbox never learned a thing.
func TestAGroupChangeIsPublished(t *testing.T) {
	t.Parallel()

	session := groupSession(t)
	actor := someone("5511999990002")
	session.handle(&waEvents.GroupInfo{
		JID: groupJID(), Sender: &actor, Timestamp: time.UnixMilli(1755440000123),
		Name: &waTypes.GroupName{Name: "Equipe fazer.ai (2026)"},
	})

	payload := published(t, session, protocol.EventGroupUpdated, "event_group_updated")
	group, _ := payload["group"].(map[string]any)
	if group["id"] != theGroup || group["kind"] != "group" {
		t.Errorf("published the change under %v", payload["group"])
	}
	if ts, _ := payload["timestamp"].(float64); int64(ts) != 1755440000123 {
		t.Errorf("published %v as the moment the change happened", payload["timestamp"])
	}
	who, _ := payload["actor"].(map[string]any)
	if who["phone"] != "5511999990002" {
		t.Errorf("named %v as who made the change", payload["actor"])
	}
	if subject := changesIn(t, payload)["subject"]; subject != "Equipe fazer.ai (2026)" {
		t.Errorf("published %v as the new subject", subject)
	}
}

// A setting that was turned off and a setting nobody touched are different things, and a
// plain bool renders them the same: `false` with `omitempty` is indistinguishable from
// absent, and a client that reads an absent field as "unchanged" -- which is what the
// other side does -- would see every rename as also turning announce off.
func TestASettingNobodyTouchedIsNotReported(t *testing.T) {
	t.Parallel()

	session := groupSession(t)
	session.handle(&waEvents.GroupInfo{
		JID: groupJID(), Name: &waTypes.GroupName{Name: "Equipe fazer.ai"},
	})

	changes := changesIn(t, published(t, session, protocol.EventGroupUpdated, "event_group_updated"))
	for _, quiet := range []string{"announce", "locked", "join_approval", "description"} {
		if _, mentioned := changes[quiet]; mentioned {
			t.Errorf("a rename reported %q as %v", quiet, changes[quiet])
		}
	}
}

// The other half of the same rule: a setting that really was turned off is reported as
// `false`, not left out. Leaving it out would be the producer saying nothing changed.
func TestASettingTurnedOffIsReportedAsFalse(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		event *waEvents.GroupInfo
		field string
		want  bool
	}{
		"announce on":  {&waEvents.GroupInfo{Announce: &waTypes.GroupAnnounce{IsAnnounce: true}}, "announce", true},
		"announce off": {&waEvents.GroupInfo{Announce: &waTypes.GroupAnnounce{IsAnnounce: false}}, "announce", false},
		"locked":       {&waEvents.GroupInfo{Locked: &waTypes.GroupLocked{IsLocked: true}}, "locked", true},
		"unlocked":     {&waEvents.GroupInfo{Locked: &waTypes.GroupLocked{IsLocked: false}}, "locked", false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session := groupSession(t)
			test.event.JID = groupJID()
			session.handle(test.event)

			changes := changesIn(t, published(t, session, protocol.EventGroupUpdated, "event_group_updated"))
			got, mentioned := changes[test.field]
			if !mentioned {
				t.Fatalf("the change to %q was not reported at all", test.field)
			}
			if got != test.want {
				t.Errorf("reported %q as %v, want %v", test.field, got, test.want)
			}
		})
	}
}

// A description that was cleared is the empty string rather than an absent field: absent
// is this producer saying the notification did not mention the description, and a group
// whose description was deleted did have it mentioned.
func TestAClearedDescriptionIsReportedAsEmptyRatherThanAbsent(t *testing.T) {
	t.Parallel()

	session := groupSession(t)
	session.handle(&waEvents.GroupInfo{
		JID:   groupJID(),
		Topic: &waTypes.GroupTopic{Topic: "canal do time", TopicDeleted: true},
	})

	changes := changesIn(t, published(t, session, protocol.EventGroupUpdated, "event_group_updated"))
	description, mentioned := changes["description"]
	if !mentioned {
		t.Fatal("a deleted description was not reported at all")
	}
	if description != "" {
		t.Errorf("a deleted description was reported as %q", description)
	}
}

func TestMembershipChangesAreNamedOnBothSides(t *testing.T) {
	t.Parallel()

	session := groupSession(t)
	session.handle(&waEvents.GroupInfo{
		JID:     groupJID(),
		Join:    []waTypes.JID{someone("5511999990003")},
		Leave:   []waTypes.JID{someone("5511999990004")},
		Promote: []waTypes.JID{someone("5511999990005")},
		Demote:  []waTypes.JID{someone("5511999990006")},
	})

	changes := changesIn(t, published(t, session, protocol.EventGroupUpdated, "event_group_updated"))
	for field, phone := range map[string]string{
		"join": "5511999990003", "leave": "5511999990004",
		"promote": "5511999990005", "demote": "5511999990006",
	} {
		list, ok := changes[field].([]any)
		if !ok || len(list) != 1 {
			t.Errorf("%q came out as %v", field, changes[field])
			continue
		}
		party, _ := list[0].(map[string]any)
		if party["phone"] != phone {
			t.Errorf("%q named %v", field, list[0])
		}
	}
}

// Somebody this connector cannot put an address on is left out of the list rather than
// sent as an empty party the contract does not allow. Unlike a group's roster, a short
// list here understates a change instead of removing anybody from anything: `join` says
// who joined, not who is in the group.
func TestAParticipantWithNoAddressIsLeftOutOfTheChange(t *testing.T) {
	t.Parallel()

	session := groupSession(t)
	session.handle(&waEvents.GroupInfo{
		JID:  groupJID(),
		Join: []waTypes.JID{someone("5511999990003"), {Server: waTypes.DefaultUserServer}},
	})

	changes := changesIn(t, published(t, session, protocol.EventGroupUpdated, "event_group_updated"))
	list, _ := changes["join"].([]any)
	if len(list) != 1 {
		t.Fatalf("a nameless participant came through as %v", changes["join"])
	}
}

// WhatsApp reports a join by invite link with no participant on the node. `actor` is
// optional in the contract exactly so a producer can say it does not know, and inventing
// one would name whoever happened to be handy as the person who did it.
func TestAChangeWithNobodyBehindItNamesNoActor(t *testing.T) {
	t.Parallel()

	session := groupSession(t)
	session.handle(&waEvents.GroupInfo{
		JID: groupJID(), Notify: "invite",
		Join: []waTypes.JID{someone("5511999990003")},
	})

	payload := published(t, session, protocol.EventGroupUpdated, "event_group_updated")
	if actor, named := payload["actor"]; named && actor != nil {
		t.Errorf("named %v as the actor of a change nobody was reported for", actor)
	}
}

// A notification that names somebody this connector cannot put an address on. The
// contract has no party without one -- `anyOf` requires phone or lid -- so an actor built
// out of it would be an empty object the client's own validator rejects, taking the whole
// frame down with it. Saying nothing about who made the change is what the optional field
// is for.
func TestAnActorWithNoAddressIsLeftOutRatherThanSentHollow(t *testing.T) {
	t.Parallel()

	session := groupSession(t)
	nobody := waTypes.JID{Server: waTypes.DefaultUserServer}
	session.handle(&waEvents.GroupInfo{
		JID: groupJID(), Sender: &nobody,
		Name: &waTypes.GroupName{Name: "Equipe fazer.ai"},
	})

	payload := published(t, session, protocol.EventGroupUpdated, "event_group_updated")
	if actor, named := payload["actor"]; named && actor != nil {
		t.Errorf("named %v as the actor of a change nobody nameable was reported for", actor)
	}
}

// The changes the contract has no field for. Publishing nothing for them is silence a
// client cannot tell from nothing having happened, so they go out as `group.activity`,
// which is the contract's way of saying a group moved without saying how.
func TestAChangeTheContractCannotCarryIsPublishedAsActivity(t *testing.T) {
	t.Parallel()

	for name, event := range map[string]*waEvents.GroupInfo{
		"disappearing messages": {Ephemeral: &waTypes.GroupEphemeral{IsEphemeral: true, DisappearingTimer: 86400}},
		"the group deleted":     {Delete: &waTypes.GroupDelete{Deleted: true, DeleteReason: "admin"}},
		"a new invite link":     {NewInviteLink: ptr("ABCDEF")},
		"suspended":             {Suspended: true},
		"linked to a community": {Link: &waTypes.GroupLinkChange{Type: waTypes.GroupLinkChangeTypeSub}},
		// whatsmeow could not read it at all, which is every change WhatsApp adds after
		// this build. Enumerating them is what this branch exists not to have to do.
		"something unparsed": {UnknownChanges: []*waBinary.Node{{Tag: "whatever"}}},
		// The one that is not about a missing field: whatsmeow fills approval in with
		// `true` whenever the tag is there, having no case for the negation, so the value
		// may be the opposite of what happened. A metadata query reads it right in both
		// directions and writes nothing into an operator's timeline on the way.
		"membership approval": {MembershipApprovalMode: &waTypes.GroupMembershipApprovalMode{IsJoinApprovalRequired: true}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			session := groupSession(t)
			event.JID = groupJID()
			session.handle(event)

			payload := published(t, session, protocol.EventGroupActivity, "event_group_activity")
			groups, _ := payload["groups"].([]any)
			if len(groups) != 1 {
				t.Fatalf("named %v as the groups that moved", payload["groups"])
			}
			named, _ := groups[0].(map[string]any)
			if named["id"] != theGroup {
				t.Errorf("named %v as the group that moved", groups[0])
			}
		})
	}
}

// A notification that says the roster has a new version and not what it is. There is
// nothing to publish and nothing for a client to go and read: the version bump rides
// along with the change it belongs to, and on its own it is bookkeeping.
func TestAVersionBumpOnItsOwnPublishesNothing(t *testing.T) {
	t.Parallel()

	session := groupSession(t)
	session.handle(&waEvents.GroupInfo{
		JID:                      groupJID(),
		PrevParticipantVersionID: "17",
		ParticipantVersionID:     "18",
	})

	nothingPublished(t, session)
}

// An event with no group on it has nowhere to be published to, and the contract has no
// address for it. Falling back to something would file the change under the wrong
// conversation.
func TestAChangeWithNoGroupPublishesNothing(t *testing.T) {
	t.Parallel()

	session := groupSession(t)
	session.handle(&waEvents.GroupInfo{Name: &waTypes.GroupName{Name: "Equipe fazer.ai"}})

	nothingPublished(t, session)
}

// Being added to a group is the client's first sight of it, so the whole description
// goes: there is nothing on its side to merge a partial one into.
func TestJoiningAGroupIsPublished(t *testing.T) {
	t.Parallel()

	session := groupSession(t)
	session.handle(&waEvents.JoinedGroup{
		Reason: "invite",
		GroupInfo: waTypes.GroupInfo{
			JID:              groupJID(),
			GroupName:        waTypes.GroupName{Name: "Equipe fazer.ai"},
			GroupTopic:       waTypes.GroupTopic{Topic: "canal do time"},
			ParticipantCount: 2,
			Participants: []waTypes.GroupParticipant{
				{JID: someone("5511999990001")},
				{JID: someone("5511999990002"), IsAdmin: true},
			},
		},
	})

	payload := published(t, session, protocol.EventGroupJoined, "event_group_joined")
	info, ok := payload["info"].(map[string]any)
	if !ok {
		t.Fatalf("group.joined carried %v", payload)
	}
	if info["subject"] != "Equipe fazer.ai" {
		t.Errorf("published %v as the subject of a group the account joined", info["subject"])
	}
	group, _ := info["group"].(map[string]any)
	if group["id"] != theGroup {
		t.Errorf("published the group as %v", info["group"])
	}
	roster, _ := info["participants"].([]any)
	if len(roster) != 2 {
		t.Errorf("published %d participants for a group of two", len(roster))
	}
}

func TestJoiningAGroupWithNoAddressPublishesNothing(t *testing.T) {
	t.Parallel()

	session := groupSession(t)
	session.handle(&waEvents.JoinedGroup{
		GroupInfo: waTypes.GroupInfo{GroupName: waTypes.GroupName{Name: "Equipe fazer.ai"}},
	})

	nothingPublished(t, session)
}

// Two notifications for one group, in the order WhatsApp sent them. Group events go on
// the session's own shard like everything else, so what the client reads is the order the
// account experienced.
func TestGroupChangesKeepTheOrderTheyArrivedIn(t *testing.T) {
	t.Parallel()

	session := groupSession(t)
	session.handle(&waEvents.GroupInfo{JID: groupJID(), Name: &waTypes.GroupName{Name: "first"}})
	session.handle(&waEvents.GroupInfo{JID: groupJID(), Name: &waTypes.GroupName{Name: "second"}})

	for _, want := range []string{"first", "second"} {
		payload := published(t, session, protocol.EventGroupUpdated, "event_group_updated")
		if subject := changesIn(t, payload)["subject"]; subject != want {
			t.Errorf("read %v where %q was published", subject, want)
		}
	}
}

func ptr[T any](v T) *T { return &v }
