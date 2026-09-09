package whatsmeow

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// plain is a session with no socket and no store behind it, for the tests that read an
// address out of an event and nothing else. The resolution falls back to what the event
// carried, which is what those tests are about.
func plain(t *testing.T) *Session {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Session{
		ctx: ctx, storeLimit: time.Second, aliases: newAlias(), log: zerolog.Nop(),
	}
}

// The case the issue was opened on, measured on a real account: a direct chat's
// `chatstate` arrives LID-only, with no alternative on the event to prefer. Before this,
// what went out was what came in -- so the typing indicator for a person the client knew
// only by number named a conversation it could not find, and never showed.
//
// The pairing was on the message that came before it, and this is the one place that
// remembers.
func TestAnAddressIsResolvedFromWhatTheAccountWasShown(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	looking, done := session.looking()
	defer done()

	lid := waTypes.NewJID("167392323834034", waTypes.HiddenUserServer)
	phone := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	// The message that named both, which is how this account came to know the two are one
	// person.
	session.party(looking, phone, lid)

	// Only the number on the event, which is the shape that used to publish a chat no
	// LID-keyed contact matched.
	named := session.party(looking, phone)
	if named.LID != "167392323834034" || named.Phone != "5511999990002" {
		t.Fatalf("the party is %+v, want both halves of the pairing it was shown", named)
	}

	// And the conversation goes out under the address every other path uses.
	chat, ok := session.address(looking, phone)
	if !ok || chat.Kind != protocol.AddressLID || chat.ID != "167392323834034" {
		t.Fatalf("the chat went out as %+v (ok=%v), want the LID the pairing named", chat, ok)
	}
}

// A pair, once known, names the same two people for good, so the answer is remembered.
// What is deliberately not remembered is the absence: the pairing arrives later, on a
// message or a group listing, and a miss kept in memory would pin "this person has no
// LID" for as long as the session ran.
func TestOnlyAMappingThatWasFoundIsRemembered(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	looking, done := session.looking()
	defer done()

	lid := waTypes.NewJID("167392323834035", waTypes.HiddenUserServer)
	phone := waTypes.NewJID("5511999990003", waTypes.DefaultUserServer)

	// Asked before anything showed this account the pairing, which must not settle the
	// question.
	if named := session.party(looking, phone); named.LID != "" {
		t.Fatalf("a party was given a LID nothing had paired yet: %+v", named)
	}
	session.party(looking, phone, lid)
	if named := session.party(looking, phone); named.LID != "167392323834035" {
		t.Fatalf("the party is %+v, want the pairing learned after the first miss", named)
	}

	// And it stays what it was shown. A pair names the same two people for good, so a
	// later event naming this number beside a different handle is a spelling of somebody
	// else, not a correction.
	if named := session.party(looking, phone); named.LID != "167392323834035" {
		t.Fatalf("the party is %+v, want the pair this session had already learned", named)
	}
}

// A lookup that started under one account must not write its answer into the map that
// replaced it. The store read is a round trip taken outside the lock, so a logout and the
// pairing after it can land in the middle of one, and what comes back is the previous
// account's -- a pairing between a LID and a number is what one account was shown.
func TestAnAliasLearnedBeforeTheAccountChangedIsNotKept(t *testing.T) {
	t.Parallel()

	const key = "998877665544332@lid"
	learned := waTypes.NewJID("5541988887777", waTypes.DefaultUserServer)

	kept := newAlias()
	kept.remember(key, learned, kept.learning())
	if _, held := kept.seen[key]; !held {
		t.Fatal("an alias learned under the current account was dropped")
	}

	dropped := newAlias()
	learning := dropped.learning()
	dropped.forget()
	dropped.remember(key, learned, learning)
	if _, held := dropped.seen[key]; held {
		t.Error("an alias learned under the previous account was written back after the change")
	}
}

// The pairing an event carried is one this account was shown, so it needs no table and no
// permission: WhatsApp addressed this account with both halves. Remembering it is what
// keeps the later events for the same person whole, because the wire carries both halves
// on a message and one half on a receipt or a typing indicator.
func TestAPairingTheEventCarriedIsLearnedFirstHand(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	lid := waTypes.NewJID("167392323834036", waTypes.HiddenUserServer)
	phone := waTypes.NewJID("5511999990004", waTypes.DefaultUserServer)

	// The context an event handler runs under, which is where the account a pairing is
	// being learned for is stamped.
	looking, done := session.looking()
	defer done()

	// Nothing in the shared table and no contact row: the event is the only source.
	if named := session.party(looking, phone, lid); named.Phone != "5511999990004" || named.LID != "167392323834036" {
		t.Fatalf("the party is %+v, want both halves the event named", named)
	}

	// The half a receipt would carry, answered whole.
	if named := session.party(looking, lid); named.Phone != "5511999990004" {
		t.Errorf("the party is %+v, want the number the message had already named", named)
	}
	if chat, ok := session.address(looking, phone); !ok || chat.ID != "167392323834036" {
		t.Errorf("the chat went out as %+v (ok=%v), want the LID the message had already named", chat, ok)
	}
}

// A command spends a round trip at WhatsApp between being asked and naming anybody, and a
// logout landing in that window rebuilds the session on another account. What comes back
// belongs to the account that asked, so it is dropped rather than written into the map
// that replaced it -- which is what the next account would otherwise be answered from.
func TestAPairingLearnedUnderOneAccountIsNotKeptForTheNext(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	lid := waTypes.NewJID("167392323834038", waTypes.HiddenUserServer)
	phone := waTypes.NewJID("5511999990006", waTypes.DefaultUserServer)

	// The account that asked, stamped before the round trip.
	asking := session.aliases.stamp(t.Context())
	// The logout, and the account that took its place.
	session.aliases.forget()
	// And the answer, arriving late.
	session.aliases.observe(asking, phone, lid)

	if named := session.party(t.Context(), lid); named.Phone != "" {
		t.Errorf("the party is %+v, carrying a number the previous account was shown", named)
	}
}

// `whatsmeow_lid_map` is `(lid, pn)` with no `our_jid`, so one table serves every session
// on a deployment: a pairing operator A's account learned is readable by operator B's. A
// LID exists so somebody can take part without handing over their number, and which
// accounts get to see the number behind one is granted per account. B is not one of them.
func TestAPairingAnotherAccountLearnedIsNotPublished(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	lid := waTypes.NewJID("167392323834037", waTypes.HiddenUserServer)
	phone := waTypes.NewJID("5511999990005", waTypes.DefaultUserServer)
	// The row as the other account's session left it, with nothing saying this one was
	// ever shown the pairing.
	if err := session.current().Store.LIDs.PutLIDMapping(t.Context(), lid, phone); err != nil {
		t.Fatalf("PutLIDMapping: %v", err)
	}

	// And a contact row under the number, which is what a row proves nothing: whatsmeow
	// fills in the address a LID-only message did not carry out of that same shared table
	// and files a row under it, so this account's record of the number can be a copy of
	// the mapping it would be authorising.
	if _, _, err := session.current().Store.Contacts.PutPushName(t.Context(), phone, "Bruno"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}

	if named := session.party(t.Context(), lid); named.Phone != "" {
		t.Errorf("the party is %+v, carrying a number this account was never given", named)
	}
	if chat, ok := session.address(t.Context(), lid); !ok || chat.Kind != protocol.AddressLID {
		t.Errorf("the chat went out as %+v (ok=%v), want the half the event carried", chat, ok)
	}

	// The other direction links the same two people, and links it for an account that
	// holds neither side by having met them.
	if named := session.party(t.Context(), phone); named.LID != "" {
		t.Errorf("the party is %+v, linking a number to a handle this account never met", named)
	}
}

// The account's own pairing is the one nothing had to show it: the session holds both
// halves, off the device it paired and off the connection that brought the LID. It is also
// the one the shared table can be missing, because whatsmeow logs that write rather than
// failing on it -- and a receipt for the account's own send would then go out under the
// number while every other path names the conversation by LID.
func TestTheAccountsOwnPairingNeedsNothingToHaveShownIt(t *testing.T) {
	t.Parallel()

	const lid = "111222333444555"

	session, _ := newTestSession(t, "5511999990001")
	session.current().Store.LID = waTypes.NewJID(lid, waTypes.HiddenUserServer)
	session.handle(&waEvents.Connected{})
	drain(t, session)
	session.setConnected(true)

	own := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
	if named := session.party(t.Context(), own); named.LID != lid {
		t.Errorf("the account is named %+v, want the LID the connection brought", named)
	}
	if chat, ok := session.address(t.Context(), own); !ok || chat.Kind != protocol.AddressLID || chat.ID != lid {
		t.Errorf("the account's own chat went out as %+v (ok=%v), want its LID", chat, ok)
	}
}
