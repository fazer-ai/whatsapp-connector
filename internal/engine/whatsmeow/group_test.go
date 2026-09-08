package whatsmeow

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

func groupCommand(t *testing.T, payload string) *protocol.Command {
	t.Helper()
	return &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandGroupInfo,
		SID: "s1", Payload: json.RawMessage(payload),
	}
}

const aGroup = `{"group":{"kind":"group","id":"120363041234567890"}}`

func TestGroupInfoRefusesAPayloadThatNamesNoGroup(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name    string
		payload string
	}{
		{name: "no group at all", payload: `{}`},
		// A direct chat has no subject, no participants and no admins. WhatsApp answers
		// the query with an error, which would reach the caller as `wa_error` and say
		// nothing about what it did wrong.
		{name: "a phone number", payload: `{"group":{"kind":"phone","id":"5511999990002"}}`},
		{name: "a lid", payload: `{"group":{"kind":"lid","id":"123456789012345"}}`},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
				t.Error("a payload that names no group was asked about anyway")
				return nil, nil
			}

			_, err := session.Execute(t.Context(), groupCommand(t, refused.payload))
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

func TestGroupInfoDescribesAGroupTheWayTheContractSpellsIt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	created := time.UnixMilli(1755000000000)
	session.groupInfo = func(_ context.Context, _ *wm.Client, group waTypes.JID) (*waTypes.GroupInfo, error) {
		if group.User != "120363041234567890" {
			t.Errorf("the query names %q, want the group the caller asked about", group.User)
		}
		return &waTypes.GroupInfo{
			JID:          waTypes.NewJID("120363041234567890", waTypes.GroupServer),
			OwnerJID:     waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
			GroupName:    waTypes.GroupName{Name: "Equipe"},
			GroupTopic:   waTypes.GroupTopic{Topic: "o grupo da equipe"},
			GroupLocked:  waTypes.GroupLocked{IsLocked: true},
			GroupCreated: created,
			GroupMembershipApprovalMode: waTypes.GroupMembershipApprovalMode{
				IsJoinApprovalRequired: true,
			},
			MemberAddMode:    waTypes.GroupMemberAddModeAllMember,
			ParticipantCount: 3,
			Participants: []waTypes.GroupParticipant{
				{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer), IsSuperAdmin: true, IsAdmin: true},
				{JID: waTypes.NewJID("5511999990003", waTypes.DefaultUserServer), IsAdmin: true},
				{JID: waTypes.NewJID("5511999990004", waTypes.DefaultUserServer)},
			},
		}, nil
	}

	result, err := session.Execute(t.Context(), groupCommand(t, aGroup))
	if err != nil {
		t.Fatalf("group.info: %v", err)
	}
	var described groupInfo
	if err := json.Unmarshal(result, &described); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}

	if described.Group.Kind != protocol.AddressGroup || described.Group.ID != "120363041234567890" {
		t.Errorf("the answer is about %+v, want the group asked for", described.Group)
	}
	if described.Subject != "Equipe" || described.Description != "o grupo da equipe" {
		t.Errorf("subject/description came back as %q/%q", described.Subject, described.Description)
	}
	if !described.Locked || !described.JoinApproval || described.Announce {
		t.Errorf("the flags came back locked=%v join_approval=%v announce=%v", described.Locked, described.JoinApproval, described.Announce)
	}
	if described.MemberAddMode != "all_member_add" {
		t.Errorf("member_add_mode is %q, want all_member_add", described.MemberAddMode)
	}
	// Epoch milliseconds, which is what every timestamp on this wire is. Seconds here
	// would put the group's creation in 1970 and a client would render it.
	if described.CreatedAt != created.UnixMilli() {
		t.Errorf("created_at is %d, want %d", described.CreatedAt, created.UnixMilli())
	}
	if described.Owner == nil || described.Owner.Phone != "5511999990002" {
		t.Errorf("the owner came back as %+v", described.Owner)
	}
	if len(described.Participants) != 3 {
		t.Fatalf("the group has %d participants, want 3", len(described.Participants))
	}
	// A superadmin is an admin too, so a check in the wrong order reports the group's
	// creator as an ordinary admin.
	for i, want := range []string{"superadmin", "admin", "member"} {
		if described.Participants[i].Role != want {
			t.Errorf("participant %d is a %q, want %q", i, described.Participants[i].Role, want)
		}
	}
	// The three the contract says a producer that cannot answer leaves out. A `null`
	// would claim this group has no photo and no invite, which is a different thing from
	// not having been asked.
	var raw map[string]any
	if err := json.Unmarshal(result, &raw); err != nil {
		t.Fatalf("unmarshal the answer again: %v", err)
	}
	for _, absent := range []string{"picture_url", "has_picture", "invite_code"} {
		if _, present := raw[absent]; present {
			t.Errorf("%q is in the answer, and this command does not ask for it", absent)
		}
	}
}

// The two disagree on a group whose membership did not arrive in full, and the count is
// what answers "how many people are in this group". A size taken from a partial list is a
// number that shrinks for a reason nobody can see.
func TestGroupInfoPrefersWhatsAppsOwnCountToTheListItSent(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
		return &waTypes.GroupInfo{
			JID:              waTypes.NewJID("120363041234567890", waTypes.GroupServer),
			ParticipantCount: 42,
			Participants: []waTypes.GroupParticipant{
				{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)},
			},
		}, nil
	}

	result, err := session.Execute(t.Context(), groupCommand(t, aGroup))
	if err != nil {
		t.Fatalf("group.info: %v", err)
	}
	var described groupInfo
	if err := json.Unmarshal(result, &described); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}
	if described.Size != 42 {
		t.Errorf("size is %d, want the count WhatsApp gave (42) rather than the list's length", described.Size)
	}
}

// A participant nobody can be named by is a row the contract does not allow -- `party`
// requires one of the two identifiers. An anonymous member of an announcement group is
// how one arrives.
func TestGroupInfoLeavesOutAParticipantItCannotName(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
		return &waTypes.GroupInfo{
			JID: waTypes.NewJID("120363041234567890", waTypes.GroupServer),
			Participants: []waTypes.GroupParticipant{
				{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)},
				{DisplayName: "somebody"},
			},
		}, nil
	}

	result, err := session.Execute(t.Context(), groupCommand(t, aGroup))
	if err != nil {
		t.Fatalf("group.info: %v", err)
	}
	var described groupInfo
	if err := json.Unmarshal(result, &described); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}
	if len(described.Participants) != 1 {
		t.Fatalf("the answer has %d participants, want only the one that can be named", len(described.Participants))
	}
	for _, member := range described.Participants {
		if member.Party.Phone == "" && member.Party.LID == "" {
			t.Error("a participant came back with neither a phone nor a lid, which no client can address")
		}
	}
}

func TestGroupInfoThatComesBackEmptyWithoutAReasonIsAFailureRatherThanAPanic(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
		return nil, nil
	}

	_, err := session.Execute(t.Context(), groupCommand(t, aGroup))
	assertCode(t, err, protocol.ErrorInternal)
}
