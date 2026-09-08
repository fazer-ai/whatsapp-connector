package whatsmeow

import (
	"bytes"
	"context"
	"encoding/json"
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
	if rows[0].RequestedAt != asked.UnixMilli() {
		t.Errorf("the request is dated %d, want %d", rows[0].RequestedAt, asked.UnixMilli())
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

// A request WhatsApp dated as nothing carries no date rather than a made-up one. The zero
// `time.Time` is year 1, whose UnixMilli is a large negative number a client reads as a
// date and sorts by.
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
	if rows[0].RequestedAt != 0 {
		t.Errorf("a request WhatsApp gave no date for is dated %d, want no date at all", rows[0].RequestedAt)
	}
	if bytes.Contains(result, []byte(`"requested_at"`)) {
		t.Errorf("a request with no date carried the field anyway: %s", result)
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
