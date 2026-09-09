package whatsmeow

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	waTypes "go.mau.fi/whatsmeow/types"

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
// The mapping was there the whole time, in the device store, and this is the one place
// that asks for it.
func TestAnAddressIsResolvedFromTheStoreWhenTheEventNamesOnlyOne(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	lid := waTypes.NewJID("167392323834034", waTypes.HiddenUserServer)
	phone := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	if err := session.current().Store.LIDs.PutLIDMapping(t.Context(), lid, phone); err != nil {
		t.Fatalf("PutLIDMapping: %v", err)
	}
	// The shared table answers for a number this account already holds, and holding it is
	// what a contact row records.
	met(t, session, phone)

	// Only the number on the event, which is the shape that used to publish a chat no
	// LID-keyed contact matched.
	named := session.party(t.Context(), phone)
	if named.LID != "167392323834034" || named.Phone != "5511999990002" {
		t.Fatalf("the party is %+v, want both halves filled in from the mapping", named)
	}

	// And the conversation goes out under the address every other path uses.
	chat, ok := session.address(t.Context(), phone)
	if !ok || chat.Kind != protocol.AddressLID || chat.ID != "167392323834034" {
		t.Fatalf("the chat went out as %+v (ok=%v), want the LID the mapping named", chat, ok)
	}
}

// A pair, once known, names the same two people for good, so the answer is remembered and
// the store is asked once. What is deliberately not remembered is the absence: the
// mapping is learned later, from a message or an app-state sync, and a miss kept in
// memory would pin "this person has no LID" for as long as the session ran.
func TestOnlyAMappingThatWasFoundIsRemembered(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	lid := waTypes.NewJID("167392323834035", waTypes.HiddenUserServer)
	phone := waTypes.NewJID("5511999990003", waTypes.DefaultUserServer)
	met(t, session, phone)

	// Asked before the mapping exists, which must not settle the question.
	if named := session.party(t.Context(), phone); named.LID != "" {
		t.Fatalf("a party was given a LID nothing had mapped yet: %+v", named)
	}
	if err := session.current().Store.LIDs.PutLIDMapping(t.Context(), lid, phone); err != nil {
		t.Fatalf("PutLIDMapping: %v", err)
	}
	if named := session.party(t.Context(), phone); named.LID != "167392323834035" {
		t.Fatalf("the party is %+v, want the mapping learned after the first miss", named)
	}

	// And now it is remembered, which the store no longer having it is what proves: a
	// pair that has been learned names the same two people for good, so asking again is a
	// round trip to the device store -- the one connection everything the session writes
	// shares -- for an answer that cannot have changed.
	// Repointed at somebody else rather than deleted, because the store has no delete:
	// either way the answer this session already has is no longer the store's.
	other := waTypes.NewJID("5511999990099", waTypes.DefaultUserServer)
	if err := session.current().Store.LIDs.PutLIDMapping(t.Context(), lid, other); err != nil {
		t.Fatalf("repointing the mapping: %v", err)
	}
	if named := session.party(t.Context(), phone); named.LID != "167392323834035" {
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

// met gives this account a contact row for somebody, which is what the device store keeps
// per account and what the shared mapping is answered against.
func met(t *testing.T, session *Session, jid waTypes.JID) {
	t.Helper()

	if _, _, err := session.current().Store.Contacts.PutPushName(t.Context(), jid, "conhecido"); err != nil {
		t.Fatalf("PutPushName: %v", err)
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
