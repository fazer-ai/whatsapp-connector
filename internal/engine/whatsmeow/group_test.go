package whatsmeow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
// A roster that cannot account for every participant is left out rather than sent short.
// The client reads any roster it is given as the whole of the group -- `sync_members`
// deactivates every membership missing from it -- so half a roster takes people out of a
// group they are still in, and the half it takes out are the ones nothing here could name
// and nothing there can put back.
func TestGroupInfoLeavesOutARosterItCannotAccountFor(t *testing.T) {
	t.Parallel()

	for _, partial := range []struct {
		name string
		info *waTypes.GroupInfo
		size int
	}{
		{
			// An anonymous participant in an announcement group: WhatsApp names them by
			// an obfuscated display name, which is not an address any client can hold.
			name: "somebody this connector cannot name",
			info: &waTypes.GroupInfo{
				JID: waTypes.NewJID("120363041234567890", waTypes.GroupServer),
				Participants: []waTypes.GroupParticipant{
					{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)},
					{DisplayName: "somebody"},
				},
			},
			size: 2,
		},
		{
			// WhatsApp's own count is larger than the list it sent, which says the list
			// is not the whole group whatever this connector does with it.
			name: "a list shorter than the count WhatsApp reported",
			info: &waTypes.GroupInfo{
				JID:              waTypes.NewJID("120363041234567890", waTypes.GroupServer),
				ParticipantCount: 40,
				Participants: []waTypes.GroupParticipant{
					{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)},
				},
			},
			size: 40,
		},
	} {
		t.Run(partial.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
				return partial.info, nil
			}

			result, err := session.Execute(t.Context(), groupCommand(t, aGroup))
			if err != nil {
				t.Fatalf("group.info: %v", err)
			}
			var described groupInfo
			if err := json.Unmarshal(result, &described); err != nil {
				t.Fatalf("unmarshal the answer: %v", err)
			}
			if len(described.Participants) != 0 {
				t.Errorf("the answer carries %d of %d participants, want no roster at all",
					len(described.Participants), partial.size)
			}
			// The field is absent, not an empty array: an empty roster is still a roster
			// to whoever reads one, and the count is what says the group is not empty.
			if bytes.Contains(result, []byte(`"participants"`)) {
				t.Errorf("a roster that could not be accounted for went out anyway: %s", result)
			}
			if described.Size != partial.size {
				t.Errorf("size is %d, want %d: a participant that could not be named is still in the group",
					described.Size, partial.size)
			}
		})
	}
}

// A group that removed its description says so with an empty one. The client leaves an
// absent field alone on purpose -- `invite_code` and `owner` are not always readable, and
// treating either absence as a removal would throw away what it legitimately has -- so a
// description dropped from the reply keeps the deleted text on a dashboard forever.
func TestGroupInfoAnswersAnEmptyDescriptionRatherThanNone(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
		return &waTypes.GroupInfo{
			JID:        waTypes.NewJID("120363041234567890", waTypes.GroupServer),
			GroupTopic: waTypes.GroupTopic{TopicDeleted: true},
		}, nil
	}

	result, err := session.Execute(t.Context(), groupCommand(t, aGroup))
	if err != nil {
		t.Fatalf("group.info: %v", err)
	}
	if !bytes.Contains(result, []byte(`"description":""`)) {
		t.Errorf("a group with no description answered %s, want an empty description", result)
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

// A `group.info` query goes to the wire, so its answer is the freshest reading of the
// group there is. The paths that translate a participant into the group's own namespace
// otherwise keep whatever the last one of them read, and a client asking about a group is
// the one moment a migration becomes visible without anybody paying a round trip for it.
func TestGroupInfoFilesTheAddressingItJustRead(t *testing.T) {
	t.Parallel()

	group := waTypes.NewJID("120363041234567890", waTypes.GroupServer)

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	// A stale reading of the same group, which is what this is meant to correct.
	session.rememberGroupMode(group, waTypes.AddressingModePN, mustConnection(t, session))
	session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
		return &waTypes.GroupInfo{
			JID:            group,
			OwnerJID:       waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
			GroupName:      waTypes.GroupName{Name: "Equipe"},
			AddressingMode: waTypes.AddressingModeLID,
		}, nil
	}

	if _, err := session.groupInfoOf(t.Context(), &protocol.Command{
		Type:    protocol.CommandGroupInfo,
		Payload: json.RawMessage(`{"group":{"kind":"group","id":"120363041234567890"}}`),
	}); err != nil {
		t.Fatalf("group.info: %v", err)
	}

	session.mu.Lock()
	filed := session.groupModes[group]
	session.mu.Unlock()
	if filed != waTypes.AddressingModeLID {
		t.Fatalf("the group answered %q and %q is still what a reaction would be built on",
			waTypes.AddressingModeLID, filed)
	}
}

// mustConnection is the counter a reading is filed against, read the way the production
// path reads it.
func mustConnection(t *testing.T, session *Session) int64 {
	t.Helper()

	on, _ := session.connection()
	return on
}

func listCommand(t *testing.T) *protocol.Command {
	t.Helper()
	return &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandGroupList,
		SID: "s1", Payload: json.RawMessage(`{}`),
	}
}

func listedGroups(t *testing.T, result json.RawMessage) []groupInfo {
	t.Helper()
	var listed []groupInfo
	if err := json.Unmarshal(result, &listed); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}
	return listed
}

// The listing says which groups exist and how big each one is, and leaves the rosters to
// `group.info`. An account can be in hundreds of groups of hundreds of people, and a
// listing that carried every membership would answer with the whole address book of every
// conversation to say which conversations exist.
func TestAGroupListingAnswersTheGroupsWithoutTheirRosters(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return []*waTypes.GroupInfo{
			{
				JID:              waTypes.NewJID("120363041234567890", waTypes.GroupServer),
				GroupName:        waTypes.GroupName{Name: "Turma da tarde"},
				ParticipantCount: 2,
				Participants: []waTypes.GroupParticipant{
					{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)},
					{JID: waTypes.NewJID("5511999990003", waTypes.DefaultUserServer)},
				},
			},
			{
				JID:       waTypes.NewJID("120363041234567891", waTypes.GroupServer),
				GroupName: waTypes.GroupName{Name: "Obras"},
			},
		}, nil
	}

	result, err := session.Execute(t.Context(), listCommand(t))
	if err != nil {
		t.Fatalf("group.list: %v", err)
	}
	listed := listedGroups(t, result)
	if len(listed) != 2 {
		t.Fatalf("the answer has %d groups, want the two this account is in", len(listed))
	}
	if listed[0].Group.Kind != protocol.AddressGroup || listed[0].Group.ID != "120363041234567890" {
		t.Errorf("the first group came back as %+v, want the one WhatsApp named", listed[0].Group)
	}
	if listed[0].Subject != "Turma da tarde" {
		t.Errorf("the first group is called %q, want its subject", listed[0].Subject)
	}
	// The count survives; the membership does not.
	if listed[0].Size != 2 {
		t.Errorf("the first group has size %d, want 2", listed[0].Size)
	}
	if len(listed[0].Participants) != 0 {
		t.Errorf("the listing carries %d participants, want the roster left to group.info",
			len(listed[0].Participants))
	}
	// Absent rather than empty: an empty roster is still a roster to whoever reads one,
	// and a client that deactivates every membership missing from it would empty the group.
	if bytes.Contains(result, []byte(`"participants"`)) {
		t.Errorf("the listing carries a participants field: %s", result)
	}
}

// An empty list is the answer "this account is in no groups". `null` would leave a client
// deciding whether that means the same thing.
func TestAGroupListingAnswersAnEmptyListRatherThanNull(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return nil, nil
	}

	result, err := session.Execute(t.Context(), listCommand(t))
	if err != nil {
		t.Fatalf("group.list: %v", err)
	}
	if string(result) != "[]" {
		t.Errorf("an account in no groups answered %s, want an empty list", result)
	}
}

// A listing is a statement about a set -- these are the groups -- so one silently short is
// false in a way no client can see. whatsmeow keeps a malformed group node rather than
// dropping it: `parseGroupNode` answers a struct even on a parse error and
// `GetJoinedGroups` appends it anyway, so an entry with no JID does reach this code, and an
// answer of `[]` for an account in one unreadable group would say it is in none.
func TestAGroupListingFailsRatherThanAnswerAGroupShort(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return []*waTypes.GroupInfo{
			{JID: waTypes.NewJID("120363041234567890", waTypes.GroupServer)},
			// Parsed far enough to be a struct and not far enough to have an id, which is
			// what a malformed group node leaves behind.
			{GroupName: waTypes.GroupName{Name: "Sem endereço"}},
		}, nil
	}

	_, err := session.Execute(t.Context(), listCommand(t))
	assertCode(t, err, protocol.ErrorInternal)
}

// The listing must not pay for naming members it is about to throw away. `party` reads the
// device store for every namespace a participant row does not carry, so describing the
// roster of every group an account is in is thousands of sequential reads on the one
// goroutine that owns the session, with every other command for it waiting behind.
//
// What holds that is structural rather than measured: `describeGroupItself` has no loop
// over the participants at all, so a listing built on it cannot walk them. This pins the
// half a test can see -- it describes a group of fifty and names none of them, while still
// answering how many there are.
func TestDescribingAGroupItselfNamesNobodyInIt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	roster := make([]waTypes.GroupParticipant, 0, 50)
	for i := range 50 {
		roster = append(roster, waTypes.GroupParticipant{
			JID: waTypes.NewJID(fmt.Sprintf("55119999%05d", i), waTypes.DefaultUserServer),
		})
	}

	described := session.describeGroupItself(t.Context(), &waTypes.GroupInfo{
		JID:          waTypes.NewJID("120363041234567890", waTypes.GroupServer),
		Participants: roster,
	})
	if described.Participants != nil {
		t.Errorf("describing the group itself named %d participants, want none", len(described.Participants))
	}
	// The count still comes from the list WhatsApp sent, which is a length and not a
	// translation: the fallback holds for a listing as much as for one group.
	if described.Size != len(roster) {
		t.Errorf("the group is %d big, want %d", described.Size, len(roster))
	}
}

func TestAGroupListingNeedsAConnection(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		t.Error("a disconnected session asked WhatsApp for its groups anyway")
		return nil, nil
	}

	_, err := session.Execute(t.Context(), listCommand(t))
	assertCode(t, err, protocol.ErrorNotConnected)
}

func TestAGroupListingAnswersWhatsAppsRefusal(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.joinedGroups = func(context.Context, *wm.Client) ([]*waTypes.GroupInfo, error) {
		return nil, &wm.IQError{Code: 429}
	}

	_, err := session.Execute(t.Context(), listCommand(t))
	assertCode(t, err, protocol.ErrorRateLimited)
}

func createCommand(t *testing.T, payload string) *protocol.Command {
	t.Helper()
	return &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandGroupCreate,
		SID: "s1", Payload: json.RawMessage(payload),
	}
}

func TestCreatingAGroupRefusesAPayloadItCannotCarryOut(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name    string
		payload string
	}{
		{name: "no payload at all", payload: `{}`},
		{name: "no subject", payload: `{"participants":[{"kind":"phone","id":"5511999990002"}]}`},
		// WhatsApp has no nameless group and answers the empty string by refusing the IQ.
		{name: "a name of nothing", payload: `{"subject":"","participants":[]}`},
		// A group cannot hold a group, and sending one anyway has WhatsApp refuse the
		// whole request, which loses the participants that were named correctly.
		{
			name:    "a group as a participant",
			payload: `{"subject":"Obras","participants":[{"kind":"group","id":"120363000000000002"}]}`,
		},
		{
			name:    "a participant with no id",
			payload: `{"subject":"Obras","participants":[{"kind":"phone","id":""}]}`,
		},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.createTheGroup = func(
				context.Context, *wm.Client, wm.ReqCreateGroup,
			) (*waTypes.GroupInfo, error) {
				t.Error("a payload that names no group to create made one anyway")
				return nil, nil
			}

			_, err := session.Execute(t.Context(), createCommand(t, refused.payload))
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

func TestCreatingAGroupSendsTheSubjectAndTheParticipants(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.createTheGroup = func(
		_ context.Context, _ *wm.Client, req wm.ReqCreateGroup,
	) (*waTypes.GroupInfo, error) {
		if req.Name != "Obras" {
			t.Errorf("the group was called %q, want the subject that was asked", req.Name)
		}
		if len(req.Participants) != 2 ||
			req.Participants[0].User != "5511999990002" || req.Participants[1].User != "77777777777777" {
			t.Errorf("whatsmeow was given %v, want the two participants that were asked", req.Participants)
		}
		if req.Participants[1].Server != waTypes.HiddenUserServer {
			t.Errorf("the LID was sent as %s, want it on the LID server", req.Participants[1])
		}
		return &waTypes.GroupInfo{
			JID:       waTypes.NewJID("120363041234567890", waTypes.GroupServer),
			GroupName: waTypes.GroupName{Name: "Obras"},
		}, nil
	}

	result, err := session.Execute(t.Context(), createCommand(t,
		`{"subject":"Obras","participants":[{"kind":"phone","id":"5511999990002"},{"kind":"lid","id":"77777777777777"}]}`))
	if err != nil {
		t.Fatalf("group.create: %v", err)
	}
	var described groupInfo
	if err := json.Unmarshal(result, &described); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}
	if described.Group.ID != "120363041234567890" || described.Subject != "Obras" {
		t.Errorf("the answer describes %+v, want the group that was made", described)
	}
}

// A group with nobody else in it is a group: WhatsApp adds this account itself, and an
// operator opening a group to fill in later is a real thing to do.
func TestCreatingAGroupTakesAnEmptyGuestList(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	asked := false
	session.createTheGroup = func(
		_ context.Context, _ *wm.Client, req wm.ReqCreateGroup,
	) (*waTypes.GroupInfo, error) {
		asked = true
		if len(req.Participants) != 0 {
			t.Errorf("whatsmeow was given %v, want nobody", req.Participants)
		}
		return &waTypes.GroupInfo{JID: waTypes.NewJID("120363041234567890", waTypes.GroupServer)}, nil
	}

	if _, err := session.Execute(t.Context(),
		createCommand(t, `{"subject":"Obras","participants":[]}`)); err != nil {
		t.Fatalf("group.create: %v", err)
	}
	if !asked {
		t.Error("a group with nobody in it was refused rather than created")
	}
}

// The one thing only a new group's roster has: WhatsApp reports per participant whether it
// could add them, and somebody it refused is not in the group. A caller reading those rows
// as members would show people a conversation they were never added to.
func TestCreatingAGroupLeavesOutTheParticipantsWhatsAppRefused(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.createTheGroup = func(
		context.Context, *wm.Client, wm.ReqCreateGroup,
	) (*waTypes.GroupInfo, error) {
		return &waTypes.GroupInfo{
			JID:              waTypes.NewJID("120363041234567890", waTypes.GroupServer),
			ParticipantCount: 3,
			Participants: []waTypes.GroupParticipant{
				{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)},
				{JID: waTypes.NewJID("5511999990003", waTypes.DefaultUserServer), Error: 403},
				{JID: waTypes.NewJID("5511999990004", waTypes.DefaultUserServer)},
			},
		}, nil
	}

	result, err := session.Execute(t.Context(), createCommand(t,
		`{"subject":"Obras","participants":[{"kind":"phone","id":"5511999990002"},`+
			`{"kind":"phone","id":"5511999990003"},{"kind":"phone","id":"5511999990004"}]}`))
	if err != nil {
		t.Fatalf("group.create: %v", err)
	}
	var described groupInfo
	if err := json.Unmarshal(result, &described); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}
	if len(described.Participants) != 2 {
		t.Fatalf("the answer has %d participants, want the two WhatsApp added", len(described.Participants))
	}
	for _, member := range described.Participants {
		if member.Party.Phone == "5511999990003" {
			t.Error("somebody WhatsApp refused to add came back as a member of the group")
		}
	}
	// The count goes with them: a size of three over a roster of two reads as a roster
	// that could not be accounted for, and the roster would be dropped entirely.
	if described.Size != 2 {
		t.Errorf("the group has size %d, want the two who are in it", described.Size)
	}
}

func TestCreatingAGroupNeedsAConnection(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.createTheGroup = func(
		context.Context, *wm.Client, wm.ReqCreateGroup,
	) (*waTypes.GroupInfo, error) {
		t.Error("a disconnected session created a group anyway")
		return nil, nil
	}

	_, err := session.Execute(t.Context(), createCommand(t, `{"subject":"Obras","participants":[]}`))
	assertCode(t, err, protocol.ErrorNotConnected)
}

func TestCreatingAGroupAnswersWhatsAppsRefusal(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.createTheGroup = func(
		context.Context, *wm.Client, wm.ReqCreateGroup,
	) (*waTypes.GroupInfo, error) {
		// The name is longer than WhatsApp allows, which it answers with 406.
		return nil, &wm.IQError{Code: 406, Text: "not-acceptable"}
	}

	_, err := session.Execute(t.Context(), createCommand(t, `{"subject":"Obras","participants":[]}`))
	assertCode(t, err, protocol.ErrorWaError)
}

// A payload that leaves `participants` out is not the same as one that sends an empty
// list. The contract requires the field, and a group created from a request that never
// said who was supposed to be in it is a group nobody asked for in that shape.
func TestCreatingAGroupTellsAMissingGuestListFromAnEmptyOne(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name    string
		payload string
	}{
		{name: "no participants field", payload: `{"subject":"Obras"}`},
		{name: "a null participants field", payload: `{"subject":"Obras","participants":null}`},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.createTheGroup = func(
				context.Context, *wm.Client, wm.ReqCreateGroup,
			) (*waTypes.GroupInfo, error) {
				t.Error("a payload that never said who to add created a group anyway")
				return nil, nil
			}

			_, err := session.Execute(t.Context(), createCommand(t, refused.payload))
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// whatsmeow's `CreateGroup` reads a LID mapping and a privacy token per participant before
// it sends anything, and wraps a failure there with `%v` rather than `%w`. A deadline that
// expires mid-lookup therefore arrives as text: `errors.Is` cannot see it, and a command
// that ran out of time would be answered as a fault in this connector.
func TestCreatingAGroupAnswersTimeoutWhenItRanOutOfTime(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.createTheGroup = func(
		ctx context.Context, _ *wm.Client, _ wm.ReqCreateGroup,
	) (*waTypes.GroupInfo, error) {
		<-ctx.Done()
		// Exactly what whatsmeow answers: the sentinel flattened into a string.
		return nil, fmt.Errorf("failed to get phone number for participant: %v", ctx.Err()) //nolint:errorlint // the point is the lost sentinel
	}

	ran, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := session.Execute(ran, createCommand(t,
		`{"subject":"Obras","participants":[{"kind":"lid","id":"77777777777777"}]}`))
	assertCode(t, err, protocol.ErrorTimeout)
}
