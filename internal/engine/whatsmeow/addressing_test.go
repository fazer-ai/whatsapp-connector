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
