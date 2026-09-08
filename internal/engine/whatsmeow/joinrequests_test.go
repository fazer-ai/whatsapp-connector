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

func joinsCommand(t *testing.T, payload string) *protocol.Command {
	t.Helper()
	return &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandGroupJoinRequestsList,
		SID: "s1", Payload: json.RawMessage(payload),
	}
}

const aGatedGroup = `{"group":{"kind":"group","id":"120363000000000001"}}`

func listed(t *testing.T, result json.RawMessage) []joinRequest {
	t.Helper()
	var rows []joinRequest
	if err := json.Unmarshal(result, &rows); err != nil {
		t.Fatalf("unmarshal the answer: %v", err)
	}
	return rows
}

func TestAJoinRequestListingRefusesAPayloadThatNamesNoGroup(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name    string
		payload string
	}{
		{name: "no payload at all", payload: `{}`},
		{name: "a group with no id", payload: `{"group":{"kind":"group","id":""}}`},
		{name: "a chat that is not a group", payload: `{"group":{"kind":"phone","id":"5511999990002"}}`},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.joinRequests = func(
				context.Context, *wm.Client, waTypes.JID,
			) ([]waTypes.GroupParticipantRequest, error) {
				t.Error("a payload that names no group was asked about anyway")
				return nil, nil
			}

			_, err := session.Execute(t.Context(), joinsCommand(t, refused.payload))
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

func TestAJoinRequestListingAnswersWhoIsWaiting(t *testing.T) {
	t.Parallel()

	asked := time.Date(2026, 9, 7, 12, 30, 0, 0, time.UTC)
	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.joinRequests = func(
		_ context.Context, _ *wm.Client, group waTypes.JID,
	) ([]waTypes.GroupParticipantRequest, error) {
		if group.Server != waTypes.GroupServer || group.User != "120363000000000001" {
			t.Errorf("the listing was asked of %s, want the group that was named", group)
		}
		return []waTypes.GroupParticipantRequest{
			{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer), RequestedAt: asked},
		}, nil
	}

	result, err := session.Execute(t.Context(), joinsCommand(t, aGatedGroup))
	if err != nil {
		t.Fatalf("group.join_requests.list: %v", err)
	}
	rows := listed(t, result)
	if len(rows) != 1 {
		t.Fatalf("the answer has %d rows, want the one request that is waiting", len(rows))
	}
	if rows[0].Party.Phone != "5511999990002" {
		t.Errorf("the request came back from %+v, want the number that asked", rows[0].Party)
	}
	// Milliseconds, which is what the contract carries. Seconds reach a dashboard as
	// January 1970 and order wrong against every other row on the screen.
	if rows[0].RequestedAt == nil || *rows[0].RequestedAt != asked.UnixMilli() {
		t.Errorf("the request is dated %v, want %d", rows[0].RequestedAt, asked.UnixMilli())
	}
}

// An empty answer is an answer: nobody is waiting. `null` would leave a client deciding
// whether that means the same thing as an empty list, and the two are worth telling apart
// everywhere else in this contract.
func TestAJoinRequestListingAnswersAnEmptyListRatherThanNull(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.joinRequests = func(
		context.Context, *wm.Client, waTypes.JID,
	) ([]waTypes.GroupParticipantRequest, error) {
		return nil, nil
	}

	result, err := session.Execute(t.Context(), joinsCommand(t, aGatedGroup))
	if err != nil {
		t.Fatalf("group.join_requests.list: %v", err)
	}
	if string(result) != "[]" {
		t.Errorf("a group nobody is waiting to join answered %s, want an empty list", result)
	}
}

// A request WhatsApp dated as nothing carries a null date rather than a made-up one. The
// zero `time.Time` is year 1, whose UnixMilli is a large negative number a client reads as
// a date and sorts by.
func TestAJoinRequestWithNoDateCarriesNone(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.joinRequests = func(
		context.Context, *wm.Client, waTypes.JID,
	) ([]waTypes.GroupParticipantRequest, error) {
		return []waTypes.GroupParticipantRequest{
			{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)},
		}, nil
	}

	result, err := session.Execute(t.Context(), joinsCommand(t, aGatedGroup))
	if err != nil {
		t.Fatalf("group.join_requests.list: %v", err)
	}
	rows := listed(t, result)
	if len(rows) != 1 {
		t.Fatalf("the answer has %d rows, want the one request that is waiting", len(rows))
	}
	if rows[0].RequestedAt != nil {
		t.Errorf("a request WhatsApp gave no date for is dated %d, want no date at all", *rows[0].RequestedAt)
	}
	// Present and null, not absent: that is how the rest of this contract writes "there
	// is none", and a client reading the key without checking for it finds nothing rather
	// than a date from year 1.
	if !bytes.Contains(result, []byte(`"requested_at":null`)) {
		t.Errorf("a request with no date answered %s, want a null date", result)
	}
}

// Somebody the connector has no address for is left out. A row with neither a phone nor a
// LID is one the contract does not allow, and nothing is lost that a client could act on:
// approving a request means naming who is approved.
func TestAJoinRequestListingLeavesOutSomebodyItCannotName(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.joinRequests = func(
		context.Context, *wm.Client, waTypes.JID,
	) ([]waTypes.GroupParticipantRequest, error) {
		return []waTypes.GroupParticipantRequest{
			{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)},
			{JID: waTypes.NewJID("", waTypes.DefaultUserServer)},
		}, nil
	}

	result, err := session.Execute(t.Context(), joinsCommand(t, aGatedGroup))
	if err != nil {
		t.Fatalf("group.join_requests.list: %v", err)
	}
	rows := listed(t, result)
	if len(rows) != 1 {
		t.Fatalf("the answer has %d rows, want only the request that can be named", len(rows))
	}
	if rows[0].Party.Phone == "" && rows[0].Party.LID == "" {
		t.Error("a request came back naming nobody, which no client can approve")
	}
}

func TestAJoinRequestListingNeedsAConnection(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.joinRequests = func(
		context.Context, *wm.Client, waTypes.JID,
	) ([]waTypes.GroupParticipantRequest, error) {
		t.Error("a disconnected session asked WhatsApp who is waiting anyway")
		return nil, nil
	}

	_, err := session.Execute(t.Context(), joinsCommand(t, aGatedGroup))
	assertCode(t, err, protocol.ErrorNotConnected)
}

func decideCommand(t *testing.T, payload string) *protocol.Command {
	t.Helper()
	return &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandGroupJoinRequestsUpdate,
		SID: "s1", Payload: json.RawMessage(payload),
	}
}

const twoWaiting = `{"group":{"kind":"group","id":"120363000000000001"},"participants":[` +
	`{"kind":"phone","id":"5511999990002"},{"kind":"phone","id":"5511999990003"}],"action":"approve"}`

func TestAJoinRequestUpdateRefusesAPayloadItCannotCarryOut(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name    string
		payload string
	}{
		{name: "no payload at all", payload: `{}`},
		{name: "nobody to decide about", payload: `{"group":{"kind":"group","id":"120363000000000001"},"participants":[],"action":"approve"}`},
		{
			name:    "an action this build does not know",
			payload: `{"group":{"kind":"group","id":"120363000000000001"},"participants":[{"kind":"phone","id":"5511999990002"}],"action":"maybe"}`,
		},
		{
			name:    "a chat that is not a group",
			payload: `{"group":{"kind":"phone","id":"5511999990002"},"participants":[{"kind":"phone","id":"5511999990003"}],"action":"approve"}`,
		},
		{
			name:    "a group as somebody waiting",
			payload: `{"group":{"kind":"group","id":"120363000000000001"},"participants":[{"kind":"group","id":"120363000000000002"}],"action":"approve"}`,
		},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.decideJoinRequests = func(
				context.Context, *wm.Client, waTypes.JID, []waTypes.JID, wm.ParticipantRequestChange,
			) ([]waTypes.GroupParticipant, error) {
				t.Error("a payload that decides nothing was sent to WhatsApp anyway")
				return nil, nil
			}

			_, err := session.Execute(t.Context(), decideCommand(t, refused.payload))
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// An approval carried out as a rejection is the failure this covers: both are accepted,
// both answer success, and somebody is turned away who was meant to be let in.
func TestAJoinRequestUpdateCarriesOutTheActionItWasAsked(t *testing.T) {
	t.Parallel()

	for _, action := range []struct {
		asked string
		want  wm.ParticipantRequestChange
	}{
		{asked: "approve", want: wm.ParticipantChangeApprove},
		{asked: "reject", want: wm.ParticipantChangeReject},
	} {
		t.Run(action.asked, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.decideJoinRequests = func(
				_ context.Context, _ *wm.Client, group waTypes.JID,
				participants []waTypes.JID, decided wm.ParticipantRequestChange,
			) ([]waTypes.GroupParticipant, error) {
				if decided != action.want {
					t.Errorf("whatsmeow was asked to %q, want %q", decided, action.want)
				}
				if group.Server != waTypes.GroupServer || group.User != "120363000000000001" {
					t.Errorf("the decision was addressed to %s, want the group that was asked", group)
				}
				if len(participants) != 1 || participants[0].User != "5511999990002" {
					t.Errorf("whatsmeow was given %v, want the one request that was decided", participants)
				}
				return []waTypes.GroupParticipant{
					{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)},
				}, nil
			}

			result, err := session.Execute(t.Context(), decideCommand(t,
				`{"group":{"kind":"group","id":"120363000000000001"},`+
					`"participants":[{"kind":"phone","id":"5511999990002"}],"action":"`+action.asked+`"}`))
			if err != nil {
				t.Fatalf("group.join_requests.update: %v", err)
			}
			var rows []participantOutcome
			if err := json.Unmarshal(result, &rows); err != nil {
				t.Fatalf("unmarshal the answer: %v", err)
			}
			if len(rows) != 1 || rows[0].Status != "success" {
				t.Fatalf("the answer is %+v, want one successful row", rows)
			}
		})
	}
}

// Every refusal stays `wa_error`: WhatsApp answers with a number, and none of them has
// been confirmed to mean anything in particular here. A client branches on the code, so a
// guess spelled as a contract code sends it somewhere nobody checked.
func TestAJoinRequestUpdateLeavesEveryRefusalOpaque(t *testing.T) {
	t.Parallel()

	for _, code := range []int{403, 406, 409, 500} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.decideJoinRequests = func(
				context.Context, *wm.Client, waTypes.JID, []waTypes.JID, wm.ParticipantRequestChange,
			) ([]waTypes.GroupParticipant, error) {
				return []waTypes.GroupParticipant{
					{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer), Error: code},
					{JID: waTypes.NewJID("5511999990003", waTypes.DefaultUserServer)},
				}, nil
			}

			result, err := session.Execute(t.Context(), decideCommand(t, twoWaiting))
			if err != nil {
				t.Fatalf("group.join_requests.update: %v", err)
			}
			var rows []participantOutcome
			if err := json.Unmarshal(result, &rows); err != nil {
				t.Fatalf("unmarshal the answer: %v", err)
			}
			if len(rows) != 2 {
				t.Fatalf("the answer has %d rows, want one per request decided", len(rows))
			}
			if rows[0].Status != "failed" || rows[0].Code == nil || *rows[0].Code != protocol.ErrorWaError {
				t.Errorf("WhatsApp's %d came back as %+v, want a failed row with wa_error", code, rows[0])
			}
			if rows[1].Status != "success" {
				t.Errorf("the request WhatsApp did decide came back as %q", rows[1].Status)
			}
		})
	}
}

// A request nothing was carried out of is the command failing. The client drops the
// request from its own list right after the call and only a raised error stops it, so an
// `ok` carrying a refusal it does not read loses somebody who is still waiting.
func TestAJoinRequestUpdateFailsWhenNothingWasDecided(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.decideJoinRequests = func(
		context.Context, *wm.Client, waTypes.JID, []waTypes.JID, wm.ParticipantRequestChange,
	) ([]waTypes.GroupParticipant, error) {
		return []waTypes.GroupParticipant{
			{JID: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer), Error: 403},
		}, nil
	}

	_, err := session.Execute(t.Context(), decideCommand(t, twoWaiting))
	assertCode(t, err, protocol.ErrorWaError)
}

func TestAJoinRequestUpdateNeedsAConnection(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.decideJoinRequests = func(
		context.Context, *wm.Client, waTypes.JID, []waTypes.JID, wm.ParticipantRequestChange,
	) ([]waTypes.GroupParticipant, error) {
		t.Error("a disconnected session asked WhatsApp to decide a join request anyway")
		return nil, nil
	}

	_, err := session.Execute(t.Context(), decideCommand(t, twoWaiting))
	assertCode(t, err, protocol.ErrorNotConnected)
}
