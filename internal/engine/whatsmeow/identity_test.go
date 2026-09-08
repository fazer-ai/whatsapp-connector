package whatsmeow

import (
	"fmt"
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

	if named, _, _ := session.names(); named != "Atendimento" {
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

	if named, _, _ := session.names(); named != "Atendimento" {
		t.Errorf("after connecting the session calls itself %q, want the name it was told", named)
	}
}

// Asking for the events of a full sync looks like the way to hear the push name it learns,
// and it is not worth what it costs: whatsmeow's mass insert of the contact snapshot is
// conditional on those events being suppressed, so turning them on turns a few batch
// inserts into a round trip per contact, for an address book of any size. What it would
// buy is one display name on the account itself, which arrives anyway the next time the
// session is built or the account renames itself.
func TestASessionLeavesTheEventsOfAFullSyncSuppressed(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	if session.current().EmitAppStateEventsOnFullSync {
		t.Error("a full sync emits its events, which costs the contact snapshot its mass insert")
	}
}

// Reading a field and throwing the value away is the same race as reading it and using it.
// A probe rather than a proof -- the detector reports what it happens to observe -- on the
// one arrangement that matters: a connection landing while an app-state sync writes.
func TestConnectingDoesNotReadWhatAnAppStateSyncIsWriting(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	client := session.current()

	writing := make(chan struct{})
	go func() {
		defer close(writing)
		for i := range 200 {
			client.Store.PushName = fmt.Sprintf("nome %d", i)
		}
	}()
	for range 200 {
		session.relearn(client)
	}
	<-writing
}

// A verified name change reaches the contact table and never the device record, so the
// account's own is the one nothing else here would hear about: the copy taken at pairing
// would stand for the life of the session, and it is the copy a resolve answers with.
func TestASessionTakesItsOwnVerifiedNameChange(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.handle(&waEvents.PairSuccess{
		ID:           waTypes.NewJID("5511999990001", waTypes.DefaultUserServer),
		BusinessName: "Loja do Bruno",
	})
	drain(t, session)

	session.handle(&waEvents.BusinessName{
		JID:             waTypes.NewJID("5511999990001", waTypes.DefaultUserServer),
		OldBusinessName: "Loja do Bruno",
		NewBusinessName: "Loja do Bruno LTDA",
	})
	if _, verified, _ := session.names(); verified != "Loja do Bruno LTDA" {
		t.Errorf("the session is verified as %q, want the name the account changed to", verified)
	}

	// Somebody else renaming their business says nothing about this account.
	session.handle(&waEvents.BusinessName{
		JID:             waTypes.NewJID("5541988887777", waTypes.DefaultUserServer),
		NewBusinessName: "Outra Loja",
	})
	if _, verified, _ := session.names(); verified != "Loja do Bruno LTDA" {
		t.Errorf("the session is verified as %q after somebody else was renamed", verified)
	}

	// The same digits in the other namespace are somebody else: a LID and a phone number
	// are drawn from two spaces, and nothing stops one reading like the other.
	session.handle(&waEvents.BusinessName{
		JID:             waTypes.NewJID("5511999990001", waTypes.HiddenUserServer),
		NewBusinessName: "Loja Homonima",
	})
	if _, verified, _ := session.names(); verified != "Loja do Bruno LTDA" {
		t.Errorf("the session is verified as %q after a LID that only looks like its number", verified)
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

	if _, verified, _ := session.names(); verified != "Loja do Bruno" {
		t.Errorf("after pairing the session is verified as %q, want the name the pairing carried", verified)
	}
}

// A verified name change is written to the contact table and not to the device record, so
// after a restart the record is the stale copy of the two: the session takes its own names
// from the device, and answering out of that would report a name the account left behind.
func TestAResolvePrefersTheNewerVerifiedName(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	client := session.current()
	// The device record as a restart finds it: what the pairing wrote.
	client.Store.BusinessName = "Loja do Bruno"
	if !session.adopt(client) {
		t.Fatal("the session would not take its own client back")
	}
	// The table as the rename left it.
	if _, _, err := client.Store.Contacts.PutBusinessName(t.Context(),
		waTypes.NewJID("5511999990001", waTypes.DefaultUserServer), "Loja do Bruno LTDA"); err != nil {
		t.Fatalf("PutBusinessName: %v", err)
	}

	result, err := session.Execute(t.Context(), resolveCommand(t, `{"party":{"kind":"phone","id":"5511999990001"}}`))
	if err != nil {
		t.Fatalf("contact.resolve: %v", err)
	}
	if party := resolved(t, result); party["verified_name"] != "Loja do Bruno LTDA" {
		t.Errorf("the account is verified as %v, want the name the table was left with", party)
	}
}

// Which copy of a name is the fresher one differs per field, and it differs because of
// where each change is written: a rename updates the device record and the session and
// leaves any contact row the account has for itself alone.
func TestAResolvePrefersTheNewerPushName(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	own := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
	// A row for the account itself, as a from-me message leaves one.
	if _, _, err := session.current().Store.Contacts.PutPushName(t.Context(), own, "Antigo"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Atendimento")},
	})

	result, err := session.Execute(t.Context(), resolveCommand(t, `{"party":{"kind":"phone","id":"5511999990001"}}`))
	if err != nil {
		t.Fatalf("contact.resolve: %v", err)
	}
	if party := resolved(t, result); party["push_name"] != "Atendimento" {
		t.Errorf("the account is called %v, want the name it renamed itself to", party)
	}
}

// A connection that arrives while an explicit disconnect still stands is closed, and it
// still authenticated: whatsmeow wrote and saved the LID before the event existed, and no
// second connection comes to learn it from.
func TestASessionTakesTheLIDOffASocketItIsAboutToClose(t *testing.T) {
	t.Parallel()

	const lid = "111222333444555"

	session, _ := newTestSession(t, "5511999990001")
	session.current().Store.LID = waTypes.NewJID(lid, waTypes.HiddenUserServer)
	// An explicit disconnect still standing, which is what makes this socket uninvited.
	session.mu.Lock()
	session.hungUp = true
	session.mu.Unlock()

	session.handle(&waEvents.Connected{})

	if _, learned := session.identity(); learned != lid {
		t.Errorf("the session says its LID is %q, want the one the refused connection brought", learned)
	}
}

// A verified name from an event is the newest copy there is. The contact rows for the two
// namespaces can disagree -- whatsmeow writes the LID row first and logs a failure on the
// phone one -- and reading the stale row would undo a change this session was told about.
func TestAResolveKeepsTheVerifiedNameAnEventBrought(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	own := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
	if _, _, err := session.current().Store.Contacts.PutBusinessName(t.Context(), own, "Loja do Bruno"); err != nil {
		t.Fatalf("PutBusinessName: %v", err)
	}
	session.handle(&waEvents.BusinessName{JID: own, NewBusinessName: "Loja do Bruno LTDA"})

	result, err := session.Execute(t.Context(), resolveCommand(t, `{"party":{"kind":"phone","id":"5511999990001"}}`))
	if err != nil {
		t.Fatalf("contact.resolve: %v", err)
	}
	if party := resolved(t, result); party["verified_name"] != "Loja do Bruno LTDA" {
		t.Errorf("the account is verified as %v, want the name the event brought", party)
	}
}

// A rename can reach this connector as the notify on a message the account sent from
// another device, which whatsmeow reports as a contact's push name changing. For this
// account it is the same rename, and it may be the only notice there is.
func TestASessionTakesItsOwnNameOffAMessageItSent(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Antigo")},
	})

	session.handle(&waEvents.PushName{
		JID:         waTypes.NewJID("5511999990001", waTypes.DefaultUserServer),
		OldPushName: "Antigo",
		NewPushName: "Atendimento",
	})
	if named, _, _ := session.names(); named != "Atendimento" {
		t.Errorf("the session calls itself %q, want the name its own message carried", named)
	}

	// Somebody else's push name is not this account's.
	session.handle(&waEvents.PushName{
		JID:         waTypes.NewJID("5541988887777", waTypes.DefaultUserServer),
		NewPushName: "Bruno",
	})
	if named, _, _ := session.names(); named != "Atendimento" {
		t.Errorf("the session calls itself %q after somebody else was renamed", named)
	}
}
