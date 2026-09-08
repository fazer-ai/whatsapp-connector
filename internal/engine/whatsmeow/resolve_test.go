package whatsmeow

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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
			learn(t, session, phone, lid)

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
	// Filed under the LID, asked for by number. The row is also what says this account
	// has met them, which is what lets the mapping out.
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
		// The contract lets an address carry any non-empty id and a party carry only
		// digits, so this is a payload the schema accepts whose answer the schema would
		// refuse.
		{name: "a person whose id is not a number", payload: `{"party":{"kind":"phone","id":"abc"}}`},
		// Meta's own assistants answer on the phone server under a reserved range, and
		// the addressing layer refuses to name one as a party at all.
		{name: "a number that belongs to a bot", payload: `{"party":{"kind":"phone","id":"13135550002"}}`},
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

	// An account WhatsApp has revoked, on credentials this session is still holding. The
	// identity is copied at pairing and nothing clears it, so the number is in hand and
	// means nothing. Settling the unlink is what says so, and it says it before the
	// cleanup behind it: forgetting the device and rebuilding are a store round trip
	// each, and this command reads local state rather than the socket, so it would answer
	// all the way through them.
	paired.settleLogout()
	if !paired.isStale() {
		t.Fatal("a session whose account was unlinked did not say so until its cleanup finished")
	}
	_, err = paired.Execute(t.Context(), resolveCommand(t, `{"party":{"kind":"phone","id":"5511999990001"}}`))
	assertCode(t, err, protocol.ErrorNotPaired)
}

// A mapping that could not be read is not a mapping that does not exist. Answering the
// input address for both would tell a client the other namespace is unknown, and a client
// told that stops asking; told the read failed, it asks again.
func TestAResolveSaysWhenTheMappingCouldNotBeRead(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	stopped, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := session.resolveContact(stopped, resolveCommand(t, `{"party":{"kind":"phone","id":"5541988887777"}}`))
	assertCode(t, err, protocol.ErrorTimeout)
}

// The two namespaces are written by different paths: an app-state contact sync files one,
// a message's push name the other. A row for the address that was asked about can exist
// and hold neither name, so finding it is not the end of the search.
func TestAResolveKeepsLookingWhenTheFirstRowHasNoName(t *testing.T) {
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
	// A row under the number with a contact name and no push name, which is what an
	// address-book sync leaves behind.
	if err := client.Store.Contacts.PutContactName(t.Context(),
		waTypes.NewJID(phone, waTypes.DefaultUserServer), "Bruno Lima", "Bruno"); err != nil {
		t.Fatalf("PutContactName: %v", err)
	}
	if _, _, err := client.Store.Contacts.PutPushName(t.Context(),
		waTypes.NewJID(lid, waTypes.HiddenUserServer), "Bruninho"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}

	result, err := session.Execute(t.Context(), resolveCommand(t, `{"party":{"kind":"phone","id":"`+phone+`"}}`))
	if err != nil {
		t.Fatalf("contact.resolve: %v", err)
	}
	if party := resolved(t, result); party["push_name"] != "Bruninho" {
		t.Errorf("the resolve answered %v, want the push name filed under the other namespace", party)
	}
}

// The cache mirrors a table in the device store, and a logout deletes that device. What
// is paired next may be another account, and a pairing between a LID and a number is what
// one account was shown rather than a fact about the world: answered out of the cache, the
// new account is handed a number nobody gave it.
func TestAResolveForgetsWhatThePreviousAccountLearned(t *testing.T) {
	t.Parallel()

	const (
		phone = "5541988887777"
		lid   = "998877665544332"
	)

	session, _ := newTestSession(t, "5511999990001")
	learn(t, session, phone, lid)
	asked := resolveCommand(t, `{"party":{"kind":"lid","id":"`+lid+`"}}`)
	result, err := session.Execute(t.Context(), asked)
	if err != nil {
		t.Fatalf("contact.resolve: %v", err)
	}
	if party := resolved(t, result); party["phone"] != phone {
		t.Fatalf("the first resolve answered %v, want the mapping the account was shown", party)
	}

	// The device the next pairing produces, with none of what the last one learned.
	fresh, _ := newTestSession(t, "5511999990002")
	if !session.adopt(fresh.current()) {
		t.Fatal("the session would not take the new client")
	}

	result, err = session.Execute(t.Context(), asked)
	if err != nil {
		t.Fatalf("contact.resolve after the account changed: %v", err)
	}
	party := resolved(t, result)
	if _, remembered := party["phone"]; remembered {
		t.Errorf("the resolve answered %v after the account changed, want nothing the previous one learned", party)
	}
}

// The account's own two names were copied out of the device at pairing, and the mapping
// table is a separate write that whatsmeow logs rather than fails on. Resolved through the
// table alone, the account can be the one party this command cannot answer for.
func TestAResolveAnswersTheAccountOutOfItsOwnIdentity(t *testing.T) {
	t.Parallel()

	const lid = "111222333444555"

	session, _ := newTestSession(t, "5511999990001")
	client := session.current()
	client.Store.LID = waTypes.NewJID(lid, waTypes.HiddenUserServer)
	// Adopted again so the session copies the identity back out of the device, which is
	// the only thing that reads it. Nothing in this test publishes an event, so the
	// second handler the re-adoption registers has nothing to double up on.
	if !session.adopt(client) {
		t.Fatal("the session would not take its own client back")
	}

	// Nothing in the mapping table, which is the case this is about.
	for _, payload := range []string{
		`{"party":{"kind":"phone","id":"5511999990001"}}`,
		`{"party":{"kind":"lid","id":"` + lid + `"}}`,
	} {
		result, err := session.Execute(t.Context(), resolveCommand(t, payload))
		if err != nil {
			t.Fatalf("contact.resolve: %v", err)
		}
		party := resolved(t, result)
		if party["phone"] != "5511999990001" || party["lid"] != lid {
			t.Errorf("resolving the account itself answered %v, want both names the session holds", party)
		}
	}
}

// learn records a pairing and the contact row that says this account met them.
func learn(t *testing.T, session *Session, phone, lid string) {
	t.Helper()

	client := session.current()
	if err := client.Store.LIDs.PutLIDMapping(t.Context(),
		waTypes.NewJID(lid, waTypes.HiddenUserServer),
		waTypes.NewJID(phone, waTypes.DefaultUserServer)); err != nil {
		t.Fatalf("PutLIDMapping: %v", err)
	}
	if _, _, err := client.Store.Contacts.PutPushName(t.Context(),
		waTypes.NewJID(phone, waTypes.DefaultUserServer), "Bruno Lima"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}
}

// The mapping table has no `our_jid`: every account on a deployment writes into one, so a
// row in it may be one another operator's account was shown. Enriching an event with it is
// one thing -- the event is about somebody this account is already talking to -- and
// answering a question about an address nobody here has met is handing a client a number
// another operator was given.
func TestAResolveWithholdsAMappingThisAccountNeverLearned(t *testing.T) {
	t.Parallel()

	const (
		phone = "5541988887777"
		lid   = "998877665544332"
	)

	session, _ := newTestSession(t, "5511999990001")
	// The pairing alone, with no contact row: what another account on the same
	// deployment leaves behind.
	if err := session.current().Store.LIDs.PutLIDMapping(t.Context(),
		waTypes.NewJID(lid, waTypes.HiddenUserServer),
		waTypes.NewJID(phone, waTypes.DefaultUserServer)); err != nil {
		t.Fatalf("PutLIDMapping: %v", err)
	}

	result, err := session.Execute(t.Context(), resolveCommand(t, `{"party":{"kind":"lid","id":"`+lid+`"}}`))
	if err != nil {
		t.Fatalf("contact.resolve: %v", err)
	}
	party := resolved(t, result)
	if _, disclosed := party["phone"]; disclosed {
		t.Errorf("the resolve answered %v for a party this account has no record of meeting", party)
	}
	if party["lid"] != lid {
		t.Errorf("the resolve answered %v, want the half the caller already had", party)
	}
}

// The mapping can come out of the cache while the contact record still has to be read, so
// the check that authorises it has a failure of its own. Reported as "not met", it answers
// the same one-sided party an unknown mapping answers, and a client told the other
// namespace is unknown stops asking.
func TestAResolveSaysWhenTheContactRecordCouldNotBeRead(t *testing.T) {
	t.Parallel()

	const (
		phone = "5541988887777"
		lid   = "998877665544332"
	)

	session, container := newTestSession(t, "5511999990001")
	// The mapping put straight into the session's cache, so the lookup answers without a
	// read and the contact record is the first thing that touches the database.
	session.aliases.remember(
		waTypes.NewJID(lid, waTypes.HiddenUserServer).String(),
		waTypes.NewJID(phone, waTypes.DefaultUserServer),
		session.aliases.learning())
	if err := container.Close(); err != nil {
		t.Fatalf("Close the store: %v", err)
	}

	_, err := session.Execute(t.Context(), resolveCommand(t, `{"party":{"kind":"lid","id":"`+lid+`"}}`))
	assertCode(t, err, protocol.ErrorInternal)
}

// A command need not carry a deadline, and this one runs on the session's executor: a
// database call left with the session's own context behind it holds every later command
// for that session for as long as it lasts. The bound is the store's, the same one every
// event handler reads under.
func TestAResolveBoundsItsReadsWithoutACallerDeadline(t *testing.T) {
	t.Parallel()

	const (
		phone = "5541988887777"
		lid   = "998877665544332"
	)

	session, _ := newTestSession(t, "5511999990001")
	learn(t, session, phone, lid)
	session.storeLimit = time.Nanosecond

	// No deadline on the command, so the bound has to come from here.
	_, err := session.Execute(t.Context(), resolveCommand(t, `{"party":{"kind":"lid","id":"`+lid+`"}}`))
	assertCode(t, err, protocol.ErrorTimeout)
}
