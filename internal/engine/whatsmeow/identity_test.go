package whatsmeow

import (
	"testing"

	waSyncAction "go.mau.fi/whatsmeow/proto/waSyncAction"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// A device stored before its account had a LID learns one from the connection, not from
// the record it resumed: whatsmeow writes `Store.LID` while handling connect success. A
// resumed session emits no `PairSuccess`, which is the only other thing that ever set
// this, so without the connection the session runs with half an identity for good.
func TestASessionTakesTheLIDTheConnectionBrought(t *testing.T) {
	t.Parallel()

	const lid = "111222333444555"

	session, _ := newTestSession(t, "5511999990001")
	if _, learned := session.identity(); learned != "" {
		t.Fatalf("the session started with a LID of %q, want none for this test to mean anything", learned)
	}

	// What whatsmeow leaves behind before it dispatches the event.
	session.current().Store.LID = waTypes.NewJID(lid, waTypes.HiddenUserServer)
	session.handle(&waEvents.Connected{})
	drain(t, session)

	if phone, learned := session.identity(); learned != lid || phone != "5511999990001" {
		t.Errorf("the session says it is %q/%q, want the LID the connection brought", phone, learned)
	}
}

// The account can rename itself from another of its devices. The name arrives on the
// event, which is what makes it readable at all: whatsmeow writes `Store.PushName` on
// this same path, so going to read it there is a data race.
func TestASessionTakesTheNameTheAccountChangedTo(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Atendimento")},
	})

	if named, _ := session.names(); named != "Atendimento" {
		t.Errorf("the session calls itself %q, want the name the account changed to", named)
	}
}

// The names are on the device record, where the contact table has nothing: that table is
// the people this account has met, and it is not one of them.
func TestAResolveAnswersTheAccountsOwnNames(t *testing.T) {
	t.Parallel()

	const lid = "111222333444555"

	session, _ := newTestSession(t, "5511999990001")
	client := session.current()
	client.Store.LID = waTypes.NewJID(lid, waTypes.HiddenUserServer)
	client.Store.PushName = "Atendimento"
	client.Store.BusinessName = "Loja do Bruno"
	// Adopted again so the session copies the record out, the way it does for the client
	// it is built with.
	if !session.adopt(client) {
		t.Fatal("the session would not take its own client back")
	}

	result, err := session.Execute(t.Context(), resolveCommand(t, `{"party":{"kind":"phone","id":"5511999990001"}}`))
	if err != nil {
		t.Fatalf("contact.resolve: %v", err)
	}
	party := resolved(t, result)
	if party["push_name"] != "Atendimento" || party["verified_name"] != "Loja do Bruno" {
		t.Errorf("resolving the account itself answered %v, want the names on its own record", party)
	}
	if party["lid"] != lid {
		t.Errorf("resolving the account itself answered %v, want both of its names", party)
	}
}

// Connected orders the LID write and orders nothing about the display names: an app-state
// sync writes those from its own goroutine and may be writing one now. Reading them there
// would be a data race, and it would also lose a name the account had just changed to.
func TestAConnectionDoesNotTakeTheNamesBackOffTheDevice(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Atendimento")},
	})
	// What the device record said before the rename, which is what a read here would
	// find.
	session.current().Store.PushName = "Antigo"

	session.handle(&waEvents.Connected{})
	drain(t, session)

	if named, _ := session.names(); named != "Atendimento" {
		t.Errorf("after connecting the session calls itself %q, want the name it was told", named)
	}
}

// The first app-state sync of a device is a full one, and whatsmeow suppresses its events
// by default: the account's own push name would be updated with nobody told, and a freshly
// paired session would answer with no name until the account renamed itself.
func TestASessionAsksForTheEventsOfAFullSync(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	if !session.current().EmitAppStateEventsOnFullSync {
		t.Error("the client suppresses the events of a full sync, so the push name it learns there is never published")
	}
}

// The verified name arrives with the pairing and nowhere else until a reconnect: the
// client the session was built with had no account on it, so there was nothing to copy.
func TestAPairingCarriesTheVerifiedName(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	session.handle(&waEvents.PairSuccess{
		ID:           waTypes.NewJID("5511999990001", waTypes.DefaultUserServer),
		LID:          waTypes.NewJID("111222333444555", waTypes.HiddenUserServer),
		BusinessName: "Loja do Bruno",
		Platform:     "android",
	})
	drain(t, session)

	if _, verified := session.names(); verified != "Loja do Bruno" {
		t.Errorf("after pairing the session is verified as %q, want the name the pairing carried", verified)
	}
}
