package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

func participantsCommand(t *testing.T, payload string) *protocol.Command {
	t.Helper()
	return &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandGroupParticipantsUpdate,
		SID: "s1", Payload: json.RawMessage(payload),
	}
}

// spelled is a row's code as the client reads it, because a failure that prints the
// pointer instead names no code at all.
func spelled(code *protocol.ErrorCode) string {
	if code == nil {
		return "no code"
	}
	return string(*code)
}

// updated is the answer, read back the way a client reads it.
func updated(t *testing.T, result json.RawMessage) []participantOutcome {
	t.Helper()
	var rows []participantOutcome
	if err := json.Unmarshal(result, &rows); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}
	return rows
}

func TestAParticipantsUpdateRefusesAPayloadItCannotCarryOut(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name    string
		payload string
	}{
		{name: "no payload at all", payload: `{}`},
		{name: "nobody to change", payload: `{"group":{"kind":"group","id":"120363000000000001"},"participants":[],"action":"add"}`},
		{
			name:    "an action this build does not know",
			payload: `{"group":{"kind":"group","id":"120363000000000001"},"participants":[{"kind":"phone","id":"5511999990002"}],"action":"ban"}`,
		},
		// A direct chat has no participants to update. Sending it on would have WhatsApp
		// refuse a group IQ addressed to a person, which reaches the client as a
		// `wa_error` it can do nothing with.
		{
			name:    "a chat that is not a group",
			payload: `{"group":{"kind":"phone","id":"5511999990002"},"participants":[{"kind":"phone","id":"5511999990003"}],"action":"add"}`,
		},
		// Nobody a group can hold. WhatsApp refuses the whole IQ on the one bad entry,
		// which loses the verdict for everybody named correctly alongside it.
		{
			name:    "a group as a participant",
			payload: `{"group":{"kind":"group","id":"120363000000000001"},"participants":[{"kind":"group","id":"120363000000000002"}],"action":"add"}`,
		},
		{
			name:    "a participant with no id",
			payload: `{"group":{"kind":"group","id":"120363000000000001"},"participants":[{"kind":"phone","id":""}],"action":"add"}`,
		},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.updateParticipants = func(
				context.Context, *wm.Client, waTypes.JID, []waTypes.JID, wm.ParticipantChange,
			) ([]waTypes.GroupParticipant, error) {
				t.Error("a payload that names no change to make was sent to WhatsApp anyway")
				return nil, nil
			}

			_, err := session.Execute(t.Context(), participantsCommand(t, refused.payload))
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// Every action in the contract's enum reaches whatsmeow as the change it names. A promote
// carried out as a demote is the failure this covers: both are accepted, both answer
// success, and the group is left with the opposite of what was asked.
func TestAParticipantsUpdateCarriesOutTheActionItWasAsked(t *testing.T) {
	t.Parallel()

	for _, action := range []struct {
		asked string
		want  wm.ParticipantChange
	}{
		{asked: "add", want: wm.ParticipantChangeAdd},
		{asked: "remove", want: wm.ParticipantChangeRemove},
		{asked: "promote", want: wm.ParticipantChangePromote},
		{asked: "demote", want: wm.ParticipantChangeDemote},
	} {
		t.Run(action.asked, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.updateParticipants = func(
				_ context.Context, _ *wm.Client, group waTypes.JID,
				participants []waTypes.JID, change wm.ParticipantChange,
			) ([]waTypes.GroupParticipant, error) {
				if change != action.want {
					t.Errorf("whatsmeow was asked to %q, want %q", change, action.want)
				}
				if group.Server != waTypes.GroupServer || group.User != "120363000000000001" {
					t.Errorf("the change was addressed to %s, want the group that was asked", group)
				}
				if len(participants) != 1 || participants[0].User != "5511999990002" {
					t.Errorf("whatsmeow was given %v, want the one participant that was asked", participants)
				}
				return []waTypes.GroupParticipant{
					{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)},
				}, nil
			}

			result, err := session.Execute(t.Context(), participantsCommand(t,
				`{"group":{"kind":"group","id":"120363000000000001"},`+
					`"participants":[{"kind":"phone","id":"5511999990002"}],"action":"`+action.asked+`"}`))
			if err != nil {
				t.Fatalf("group.participants.update: %v", err)
			}
			rows := updated(t, result)
			if len(rows) != 1 || rows[0].Status != "success" {
				t.Fatalf("the answer is %+v, want one successful row", rows)
			}
		})
	}
}

// One row per participant asked, in the order asked, under the address that was asked.
// WhatsApp answers a participant under whichever namespace that account is addressed by,
// leaves out whoever it has nothing to say about, and does not promise an order -- so a
// caller lining the two lists up by position would read one person's verdict under
// another's name, and could not tell that it had.
func TestAParticipantsUpdateAnswersOneRowPerParticipantAskedInTheOrderAsked(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.updateParticipants = func(
		context.Context, *wm.Client, waTypes.JID, []waTypes.JID, wm.ParticipantChange,
	) ([]waTypes.GroupParticipant, error) {
		return []waTypes.GroupParticipant{
			// Reordered, and answered under the LID of an account that was asked for by
			// phone: both are WhatsApp's to choose.
			{
				JID:         waTypes.NewJID("77777777777777", waTypes.HiddenUserServer),
				LID:         waTypes.NewJID("77777777777777", waTypes.HiddenUserServer),
				PhoneNumber: waTypes.NewJID("5511999990004", waTypes.DefaultUserServer),
			},
			{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer), Error: 403},
		}, nil
	}

	result, err := session.Execute(t.Context(), participantsCommand(t,
		`{"group":{"kind":"group","id":"120363000000000001"},"participants":[`+
			`{"kind":"phone","id":"5511999990002"},`+
			`{"kind":"phone","id":"5511999990003"},`+
			`{"kind":"phone","id":"5511999990004"}],"action":"add"}`))
	if err != nil {
		t.Fatalf("group.participants.update: %v", err)
	}
	rows := updated(t, result)
	asked := []string{"5511999990002", "5511999990003", "5511999990004"}
	if len(rows) != len(asked) {
		t.Fatalf("the answer has %d rows, want one per participant asked (%d)", len(rows), len(asked))
	}
	for i, id := range asked {
		if rows[i].Address.ID != id || rows[i].Address.Kind != protocol.AddressPhone {
			t.Errorf("row %d is about %+v, want the phone %q that was asked", i, rows[i].Address, id)
		}
	}
	if rows[0].Status != "failed" || rows[0].Code == nil || *rows[0].Code != protocol.ErrorGroupParticipantNotAllowed {
		t.Errorf("the participant WhatsApp would not add came back as %s/%s, want failed and not allowed",
			rows[0].Status, spelled(rows[0].Code))
	}
	// The one WhatsApp said nothing about. Silence is not consent: a caller told this
	// succeeded would believe somebody is in a group they were never added to.
	if rows[1].Status != "failed" || rows[1].Code == nil || *rows[1].Code != protocol.ErrorWaError {
		t.Errorf("the participant WhatsApp left out came back as %s/%s, want failed",
			rows[1].Status, spelled(rows[1].Code))
	}
	// Asked for by phone and answered under a LID. Matching on the asked-for namespace
	// alone reads this as the silence above.
	if rows[2].Status != "success" || rows[2].Code != nil {
		t.Errorf("the participant WhatsApp answered under a LID came back as %s/%s, want success",
			rows[2].Status, spelled(rows[2].Code))
	}
}

// Which refusals get a name of their own, and which stay `wa_error` on purpose. A client
// branches on the code, so a number translated into a meaning nobody confirmed sends it
// down a road that was never checked.
func TestAParticipantsUpdateNamesOnlyTheRefusalItCanAccountFor(t *testing.T) {
	t.Parallel()

	for _, refusal := range []struct {
		name   string
		action string
		code   int
		want   protocol.ErrorCode
	}{
		{name: "an add WhatsApp would not authorize", action: "add", code: 403, want: protocol.ErrorGroupParticipantNotAllowed},
		// The same number on another action is a different sentence. Nothing here has
		// confirmed what it means, and a client acting on the privacy code would offer to
		// invite somebody it was trying to demote.
		{name: "a remove WhatsApp would not authorize", action: "remove", code: 403, want: protocol.ErrorWaError},
		{name: "a promote WhatsApp would not authorize", action: "promote", code: 403, want: protocol.ErrorWaError},
		{name: "a demote WhatsApp would not authorize", action: "demote", code: 403, want: protocol.ErrorWaError},
		{name: "already in the group", action: "add", code: 409, want: protocol.ErrorWaError},
		{name: "left recently", action: "add", code: 408, want: protocol.ErrorWaError},
		{name: "something this build has never seen", action: "add", code: 500, want: protocol.ErrorWaError},
	} {
		t.Run(refusal.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.updateParticipants = func(
				context.Context, *wm.Client, waTypes.JID, []waTypes.JID, wm.ParticipantChange,
			) ([]waTypes.GroupParticipant, error) {
				return []waTypes.GroupParticipant{
					{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer), Error: refusal.code},
				}, nil
			}

			result, err := session.Execute(t.Context(), participantsCommand(t,
				`{"group":{"kind":"group","id":"120363000000000001"},`+
					`"participants":[{"kind":"phone","id":"5511999990002"}],"action":"`+refusal.action+`"}`))
			if err != nil {
				t.Fatalf("group.participants.update: %v", err)
			}
			rows := updated(t, result)
			if len(rows) != 1 {
				t.Fatalf("the answer has %d rows, want one", len(rows))
			}
			if rows[0].Status != "failed" {
				t.Errorf("a participant WhatsApp refused came back as %q", rows[0].Status)
			}
			if rows[0].Code == nil || *rows[0].Code != refusal.want {
				t.Errorf("WhatsApp's %d on a %s came back as %s, want %q",
					refusal.code, refusal.action, spelled(rows[0].Code), refusal.want)
			}
		})
	}
}

// A refusal of the request itself, which is a different thing from a refusal of one
// participant in it: nothing was carried out, so there is no row to answer with.
func TestAParticipantsUpdateFailsWhenWhatsAppRefusesTheRequestItself(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name string
		err  error
		want protocol.ErrorCode
	}{
		{name: "the connection went", err: wm.ErrNotConnected, want: protocol.ErrorNotConnected},
		{name: "WhatsApp said no", err: &wm.IQError{Code: 403, Text: "forbidden"}, want: protocol.ErrorWaError},
		{name: "this account is being rate limited", err: &wm.IQError{Code: 429}, want: protocol.ErrorRateLimited},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.updateParticipants = func(
				context.Context, *wm.Client, waTypes.JID, []waTypes.JID, wm.ParticipantChange,
			) ([]waTypes.GroupParticipant, error) {
				return nil, refused.err
			}

			_, err := session.Execute(t.Context(), participantsCommand(t,
				`{"group":{"kind":"group","id":"120363000000000001"},`+
					`"participants":[{"kind":"phone","id":"5511999990002"}],"action":"add"}`))
			assertCode(t, err, refused.want)
			if errors.Is(err, refused.err) {
				t.Error("WhatsApp's own error reached the client instead of a code from the contract")
			}
		})
	}
}

// Asked before the socket, and before anything is sent: an unpaired or disconnected
// session has no group to change anybody in.
func TestAParticipantsUpdateNeedsAConnection(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.updateParticipants = func(
		context.Context, *wm.Client, waTypes.JID, []waTypes.JID, wm.ParticipantChange,
	) ([]waTypes.GroupParticipant, error) {
		t.Error("a disconnected session asked WhatsApp to change a group anyway")
		return nil, nil
	}

	_, err := session.Execute(t.Context(), participantsCommand(t,
		`{"group":{"kind":"group","id":"120363000000000001"},`+
			`"participants":[{"kind":"phone","id":"5511999990002"}],"action":"add"}`))
	assertCode(t, err, protocol.ErrorNotConnected)
}

// A LID and a phone number are separate namespaces written the same way, so the same
// digits under both kinds name two different people. Matching a verdict by the digits
// alone hands one person's answer to the other, and the caller reads two rows that agree
// about a group only one of them is in.
func TestAParticipantsUpdateKeepsTwoNamespacesWithTheSameDigitsApart(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.updateParticipants = func(
		context.Context, *wm.Client, waTypes.JID, []waTypes.JID, wm.ParticipantChange,
	) ([]waTypes.GroupParticipant, error) {
		// Only the phone was added. WhatsApp says nothing about the LID of the same
		// digits, which belongs to somebody else entirely.
		return []waTypes.GroupParticipant{
			{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)},
		}, nil
	}

	result, err := session.Execute(t.Context(), participantsCommand(t,
		`{"group":{"kind":"group","id":"120363000000000001"},"participants":[`+
			`{"kind":"phone","id":"5511999990002"},`+
			`{"kind":"lid","id":"5511999990002"}],"action":"add"}`))
	if err != nil {
		t.Fatalf("group.participants.update: %v", err)
	}
	rows := updated(t, result)
	if len(rows) != 2 {
		t.Fatalf("the answer has %d rows, want one per participant asked", len(rows))
	}
	if rows[0].Address.Kind != protocol.AddressPhone || rows[0].Status != "success" {
		t.Errorf("the phone that was added came back as %+v/%s, want success", rows[0].Address, rows[0].Status)
	}
	if rows[1].Address.Kind != protocol.AddressLID || rows[1].Status != "failed" {
		t.Errorf("a LID WhatsApp never answered for came back as %+v/%s, want failed",
			rows[1].Address, rows[1].Status)
	}
}
