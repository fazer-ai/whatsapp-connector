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
