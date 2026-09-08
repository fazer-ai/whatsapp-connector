package whatsmeow

import (
	"encoding/json"
	"testing"

	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

func resolveCommand(t *testing.T, payload string) *protocol.Command {
	t.Helper()
	return setCommand(t, protocol.CommandContactResolve, payload)
}

// resolved is the party a resolve answered with, read the way a client reads it.
func resolved(t *testing.T, result json.RawMessage) map[string]any {
	t.Helper()

	var party map[string]any
	if err := json.Unmarshal(result, &party); err != nil {
		t.Fatalf("the result is not a party: %v", err)
	}
	validateAgainstContract(t, "party", result)
	return party
}

// The mapping is what a client cannot do for itself: a contact stored by number and a
// LID-only party are the same person, and nothing on the wire says so unless this
// answers it.
func TestAResolveAnswersBothNamespacesFromEitherOne(t *testing.T) {
	t.Parallel()

	const (
		phone = "5541988887777"
		lid   = "998877665544332"
	)

	for _, tc := range []struct {
		name    string
		payload string
	}{
		{"asked by number", `{"party":{"kind":"phone","id":"` + phone + `"}}`},
		{"asked by LID", `{"party":{"kind":"lid","id":"` + lid + `"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			if err := session.current().Store.LIDs.PutLIDMapping(t.Context(),
				waTypes.NewJID(lid, waTypes.HiddenUserServer),
				waTypes.NewJID(phone, waTypes.DefaultUserServer)); err != nil {
				t.Fatalf("PutLIDMapping: %v", err)
			}

			result, err := session.Execute(t.Context(), resolveCommand(t, tc.payload))
			if err != nil {
				t.Fatalf("contact.resolve: %v", err)
			}
			party := resolved(t, result)
			if party["phone"] != phone || party["lid"] != lid {
				t.Errorf("the resolve answered %v, want both namespaces of one person", party)
			}
		})
	}
}

// A party nobody has learned the other half of is still an answer: the half in hand is
// what the caller asked about, and an error would have a client treat a contact it has as
// one it cannot address.
func TestAResolveAnswersWhatItHasWhenTheOtherHalfIsNotKnown(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")

	result, err := session.Execute(t.Context(), resolveCommand(t, `{"party":{"kind":"phone","id":"5541988887777"}}`))
	if err != nil {
		t.Fatalf("contact.resolve: %v", err)
	}
	party := resolved(t, result)
	if party["phone"] != "5541988887777" {
		t.Errorf("the resolve answered %v, want the number it was asked about", party)
	}
	if _, invented := party["lid"]; invented {
		t.Errorf("the resolve answered %v, want no LID at all rather than a guess", party)
	}
}

// The name is why a client asks a second time. Both namespaces are looked up because a
// push name is filed under whichever address the message carrying it arrived on, which is
// not necessarily the one the caller asked about.
func TestAResolveCarriesTheNameTheDeviceLearned(t *testing.T) {
	t.Parallel()

	const (
		phone = "5541988887777"
		lid   = "998877665544332"
	)

	session, _ := newTestSession(t, "5511999990001")
	client := session.current()
	if err := client.Store.LIDs.PutLIDMapping(t.Context(),
		waTypes.NewJID(lid, waTypes.HiddenUserServer),
		waTypes.NewJID(phone, waTypes.DefaultUserServer)); err != nil {
		t.Fatalf("PutLIDMapping: %v", err)
	}
	// Filed under the LID, asked for by number.
	if _, _, err := client.Store.Contacts.PutPushName(t.Context(),
		waTypes.NewJID(lid, waTypes.HiddenUserServer), "Bruno Lima"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}

	result, err := session.Execute(t.Context(), resolveCommand(t, `{"party":{"kind":"phone","id":"`+phone+`"}}`))
	if err != nil {
		t.Fatalf("contact.resolve: %v", err)
	}
	if party := resolved(t, result); party["push_name"] != "Bruno Lima" {
		t.Errorf("the resolve answered %v, want the name the device learned", party)
	}
}

func TestAResolveRefusesAPayloadItCannotCarryOut(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name    string
		payload string
	}{
		{name: "no payload at all", payload: `{}`},
		{name: "an address with no id", payload: `{"party":{"kind":"phone","id":""}}`},
		// A group parses as an address and is not somebody: answering would hand back a
		// party naming a group as though it were a person.
		{name: "a group", payload: `{"party":{"kind":"group","id":"120363000000000000"}}`},
		{name: "a channel", payload: `{"party":{"kind":"newsletter","id":"120363111111111111"}}`},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			_, err := session.Execute(t.Context(), resolveCommand(t, refused.payload))
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// Local does not mean accountless: the mapping lives in a device store that exists once
// an account is paired, and there is nothing to read before that.
func TestAResolveNeedsAnAccountButNotAConnection(t *testing.T) {
	t.Parallel()

	unpaired, _ := newTestSession(t, "")
	_, err := unpaired.Execute(t.Context(), resolveCommand(t, `{"party":{"kind":"phone","id":"5541988887777"}}`))
	assertCode(t, err, protocol.ErrorNotPaired)

	// Paired and never connected, which is what every other command here refuses.
	paired, _ := newTestSession(t, "5511999990001")
	if _, err := paired.Execute(t.Context(), resolveCommand(t, `{"party":{"kind":"phone","id":"5541988887777"}}`)); err != nil {
		t.Fatalf("a resolve on a disconnected session: %v", err)
	}
}
