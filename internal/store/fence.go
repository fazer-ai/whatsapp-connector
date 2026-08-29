package store

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
)

// ErrNotOwned is what every write answers with once the session it belongs to is no
// longer this instance's.
//
// An error rather than a silent drop: whatsmeow decides what to do with a store it
// cannot write, and for the Signal paths that decision is to fail the message rather
// than carry on with state it could not save. Which is right. A message that fails here
// is one WhatsApp sends again, to whoever owns the session now.
var ErrNotOwned = errors.New("store: the session is no longer owned by this instance")

// Fence is what an instance holds while it owns a session, and drops when it stops.
//
// `AGENTS.md` invariant 1 says losing the lease disconnects the socket and fences the
// store writes. The socket half is the layer above; this is the other one.
//
// Cancelling the session's context does most of it already, and cheaply: whatsmeow hands
// that context to every node handler, and `database/sql` refuses a statement on a
// cancelled one. What it does not do is cover the work whatsmeow deliberately detaches --
// `context.WithoutCancel` appears five times in the library, and two of those write:
// everything a history sync stores, and the app-state sync keys a key share brings. Those
// are the writes that cost the most when they land late, because state a peer has moved
// on from is state the next message is decrypted against.
//
// So the fence is asked per write rather than per context, and the two overlap on
// purpose. What it does not answer is the window between a lease running out and this
// instance learning it: inside that, this instance still believes it owns the session and
// this still says yes. Closing that needs the database to arbitrate, which is
// https://github.com/fazer-ai/whatsapp-connector/issues/55.
type Fence struct {
	dropped atomic.Bool
}

// Drop fences every write from here on. It is idempotent: a session can be closed by its
// owner and by a lease it lost at the same time.
func (f *Fence) Drop() { f.dropped.Store(true) }

// Dropped reports whether the fence is down, for a caller that would rather not start
// work it cannot finish.
func (f *Fence) Dropped() bool { return f.dropped.Load() }

// held is what every fenced write asks first.
func (f *Fence) held() error {
	if f.dropped.Load() {
		return ErrNotOwned
	}
	return nil
}

// Fenced puts the fence in front of every write the device can make, and returns the same
// device.
//
// The same one, mutated, and not a copy: whatsmeow fills a device in as pairing proceeds
// -- the JID, the account signature, the push name -- and hands the same pointer back to
// `PutDevice`. A copy would take those writes somewhere nobody reads.
//
// Reads are left alone. A session that has lost its lease reading its own keys costs
// nothing and stops nothing; it is the writing that lands on a peer.
func Fenced(device *store.Device, fence *Fence) *store.Device {
	device.Identities = fencedIdentities{device.Identities, fence}
	device.Sessions = fencedSessions{device.Sessions, fence}
	device.PreKeys = fencedPreKeys{device.PreKeys, fence}
	device.SenderKeys = fencedSenderKeys{device.SenderKeys, fence}
	device.AppStateKeys = fencedAppStateKeys{device.AppStateKeys, fence}
	device.AppState = fencedAppState{device.AppState, fence}
	device.Contacts = fencedContacts{device.Contacts, fence}
	device.ChatSettings = fencedChatSettings{device.ChatSettings, fence}
	device.MsgSecrets = fencedMsgSecrets{device.MsgSecrets, fence}
	device.PrivacyTokens = fencedPrivacyTokens{device.PrivacyTokens, fence}
	device.NCTSalt = fencedNCTSalt{device.NCTSalt, fence}
	device.EventBuffer = fencedEventBuffer{device.EventBuffer, fence}
	device.LIDs = fencedLIDs{device.LIDs, fence}
	device.Container = fencedContainer{device.Container, fence}
	return device
}

// Each of the below embeds the store it stands in front of, so a read passes straight
// through, and overrides every method that writes. Embedding is what keeps this to the
// writes; `TestEveryWriteIsFenced` is what stops a write added by a later whatsmeow
// passing through with them.

type fencedIdentities struct {
	store.IdentityStore
	fence *Fence
}

func (f fencedIdentities) PutIdentity(ctx context.Context, address string, key [32]byte) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.IdentityStore.PutIdentity(ctx, address, key)
}

func (f fencedIdentities) DeleteAllIdentities(ctx context.Context, phone string) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.IdentityStore.DeleteAllIdentities(ctx, phone)
}

func (f fencedIdentities) DeleteIdentity(ctx context.Context, address string) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.IdentityStore.DeleteIdentity(ctx, address)
}

type fencedSessions struct {
	store.SessionStore
	fence *Fence
}

func (f fencedSessions) PutSession(ctx context.Context, address string, session []byte) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.SessionStore.PutSession(ctx, address, session)
}

func (f fencedSessions) PutManySessions(ctx context.Context, sessions map[string][]byte) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.SessionStore.PutManySessions(ctx, sessions)
}

func (f fencedSessions) DeleteAllSessions(ctx context.Context, phone string) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.SessionStore.DeleteAllSessions(ctx, phone)
}

func (f fencedSessions) DeleteSession(ctx context.Context, address string) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.SessionStore.DeleteSession(ctx, address)
}

func (f fencedSessions) MigratePNToLID(ctx context.Context, pn, lid types.JID) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.SessionStore.MigratePNToLID(ctx, pn, lid)
}

type fencedPreKeys struct {
	store.PreKeyStore
	fence *Fence
}

// GetOrGenPreKeys and GenOnePreKey read their name and write anyway: what is not there is
// generated and stored. Fenced with the rest, because a key this instance minted and a
// peer never sees is a key the next handshake is answered with and cannot be.

func (f fencedPreKeys) GetOrGenPreKeys(ctx context.Context, count uint32) ([]*keys.PreKey, error) {
	if err := f.fence.held(); err != nil {
		return nil, err
	}
	return f.PreKeyStore.GetOrGenPreKeys(ctx, count)
}

func (f fencedPreKeys) GenOnePreKey(ctx context.Context) (*keys.PreKey, error) {
	if err := f.fence.held(); err != nil {
		return nil, err
	}
	return f.PreKeyStore.GenOnePreKey(ctx)
}

func (f fencedPreKeys) RemovePreKey(ctx context.Context, id uint32) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.PreKeyStore.RemovePreKey(ctx, id)
}

func (f fencedPreKeys) MarkPreKeysAsUploaded(ctx context.Context, upToID uint32) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.PreKeyStore.MarkPreKeysAsUploaded(ctx, upToID)
}

type fencedSenderKeys struct {
	store.SenderKeyStore
	fence *Fence
}

func (f fencedSenderKeys) PutSenderKey(ctx context.Context, group, user string, session []byte) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.SenderKeyStore.PutSenderKey(ctx, group, user, session)
}

type fencedAppStateKeys struct {
	store.AppStateSyncKeyStore
	fence *Fence
}

func (f fencedAppStateKeys) PutAppStateSyncKey(ctx context.Context, id []byte, key store.AppStateSyncKey) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.AppStateSyncKeyStore.PutAppStateSyncKey(ctx, id, key)
}

type fencedAppState struct {
	store.AppStateStore
	fence *Fence
}

//nolint:gocritic // the signature is whatsmeow's AppStateStore; a pointer would not implement it
func (f fencedAppState) PutAppStateVersion(ctx context.Context, name string, version uint64, hash [128]byte) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.AppStateStore.PutAppStateVersion(ctx, name, version, hash)
}

func (f fencedAppState) DeleteAppStateVersion(ctx context.Context, name string) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.AppStateStore.DeleteAppStateVersion(ctx, name)
}

func (f fencedAppState) PutAppStateMutationMACs(ctx context.Context, name string, version uint64, mutations []store.AppStateMutationMAC) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.AppStateStore.PutAppStateMutationMACs(ctx, name, version, mutations)
}

func (f fencedAppState) DeleteAppStateMutationMACs(ctx context.Context, name string, indexMACs [][]byte) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.AppStateStore.DeleteAppStateMutationMACs(ctx, name, indexMACs)
}

type fencedContacts struct {
	store.ContactStore
	fence *Fence
}

func (f fencedContacts) PutPushName(ctx context.Context, user types.JID, pushName string) (changed bool, previous string, err error) {
	if err := f.fence.held(); err != nil {
		return false, "", err
	}
	return f.ContactStore.PutPushName(ctx, user, pushName)
}

func (f fencedContacts) PutBusinessName(ctx context.Context, user types.JID, businessName string) (changed bool, previous string, err error) {
	if err := f.fence.held(); err != nil {
		return false, "", err
	}
	return f.ContactStore.PutBusinessName(ctx, user, businessName)
}

func (f fencedContacts) PutContactName(ctx context.Context, user types.JID, fullName, firstName string) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.ContactStore.PutContactName(ctx, user, fullName, firstName)
}

func (f fencedContacts) PutAllContactNames(ctx context.Context, contacts []store.ContactEntry) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.ContactStore.PutAllContactNames(ctx, contacts)
}

func (f fencedContacts) PutManyRedactedPhones(ctx context.Context, entries []store.RedactedPhoneEntry) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.ContactStore.PutManyRedactedPhones(ctx, entries)
}

type fencedChatSettings struct {
	store.ChatSettingsStore
	fence *Fence
}

func (f fencedChatSettings) PutMutedUntil(ctx context.Context, chat types.JID, mutedUntil time.Time) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.ChatSettingsStore.PutMutedUntil(ctx, chat, mutedUntil)
}

func (f fencedChatSettings) PutPinned(ctx context.Context, chat types.JID, pinned bool) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.ChatSettingsStore.PutPinned(ctx, chat, pinned)
}

func (f fencedChatSettings) PutArchived(ctx context.Context, chat types.JID, archived bool) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.ChatSettingsStore.PutArchived(ctx, chat, archived)
}

type fencedMsgSecrets struct {
	store.MsgSecretStore
	fence *Fence
}

func (f fencedMsgSecrets) PutMessageSecrets(ctx context.Context, inserts []store.MessageSecretInsert) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.MsgSecretStore.PutMessageSecrets(ctx, inserts)
}

func (f fencedMsgSecrets) PutMessageSecret(ctx context.Context, chat, sender types.JID, id types.MessageID, secret []byte) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.MsgSecretStore.PutMessageSecret(ctx, chat, sender, id, secret)
}

type fencedPrivacyTokens struct {
	store.PrivacyTokenStore
	fence *Fence
}

func (f fencedPrivacyTokens) PutPrivacyTokens(ctx context.Context, tokens ...store.PrivacyToken) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.PrivacyTokenStore.PutPrivacyTokens(ctx, tokens...)
}

func (f fencedPrivacyTokens) DeleteExpiredPrivacyTokens(ctx context.Context, cutoff time.Time) (int64, error) {
	if err := f.fence.held(); err != nil {
		return 0, err
	}
	return f.PrivacyTokenStore.DeleteExpiredPrivacyTokens(ctx, cutoff)
}

type fencedNCTSalt struct {
	store.NCTSaltStore
	fence *Fence
}

func (f fencedNCTSalt) PutNCTSalt(ctx context.Context, salt []byte) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.NCTSaltStore.PutNCTSalt(ctx, salt)
}

func (f fencedNCTSalt) DeleteNCTSalt(ctx context.Context) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.NCTSaltStore.DeleteNCTSalt(ctx)
}

type fencedEventBuffer struct {
	store.EventBuffer
	fence *Fence
}

func (f fencedEventBuffer) PutBufferedEvent(ctx context.Context, ciphertextHash [32]byte, plaintext []byte, serverTimestamp time.Time) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.EventBuffer.PutBufferedEvent(ctx, ciphertextHash, plaintext, serverTimestamp)
}

// DoDecryptionTxn runs its callback inside a transaction, and what that callback does is
// write. Refused whole rather than entered and rolled back: whatsmeow reads a failure here
// as a message it could not decrypt, which is the right answer for a session this instance
// no longer runs.
func (f fencedEventBuffer) DoDecryptionTxn(ctx context.Context, fn func(context.Context) error) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.EventBuffer.DoDecryptionTxn(ctx, fn)
}

func (f fencedEventBuffer) ClearBufferedEventPlaintext(ctx context.Context, ciphertextHash [32]byte) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.EventBuffer.ClearBufferedEventPlaintext(ctx, ciphertextHash)
}

func (f fencedEventBuffer) DeleteOldBufferedHashes(ctx context.Context) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.EventBuffer.DeleteOldBufferedHashes(ctx)
}

func (f fencedEventBuffer) AddOutgoingEvent(ctx context.Context, chatJID types.JID, id types.MessageID, format string, plaintext []byte) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.EventBuffer.AddOutgoingEvent(ctx, chatJID, id, format, plaintext)
}

func (f fencedEventBuffer) DeleteOldOutgoingEvents(ctx context.Context) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.EventBuffer.DeleteOldOutgoingEvents(ctx)
}

type fencedLIDs struct {
	store.LIDStore
	fence *Fence
}

func (f fencedLIDs) PutManyLIDMappings(ctx context.Context, mappings []store.LIDMapping) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.LIDStore.PutManyLIDMappings(ctx, mappings)
}

func (f fencedLIDs) PutLIDMapping(ctx context.Context, lid, jid types.JID) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.LIDStore.PutLIDMapping(ctx, lid, jid)
}

type fencedContainer struct {
	store.DeviceContainer
	fence *Fence
}

// PutDevice is also where the fence would take itself off, if it did not put itself back.
//
// A device that has never been saved carries no stores at all, and whatsmeow installs them
// the first time it is written: `sqlstore.Container.PutDevice` calls `initializeDevice`,
// which sets every store field, the LID map and the container itself, over whatever was
// there. So the save that ends a pairing replaces the whole fence with the raw stores, and
// the account that just paired spends the rest of the session unfenced -- the one kind of
// session where losing it would be least noticed, since nothing about it looks different.
//
// Only when the initialisation actually happened. Every later save finds the device already
// initialised and changes none of them, and re-wrapping there would put a fence in front of
// a fence on every write the device ever makes.
func (f fencedContainer) PutDevice(ctx context.Context, device *store.Device) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	fresh := !device.Initialized
	err := f.DeviceContainer.PutDevice(ctx, device)
	if fresh && device.Initialized {
		Fenced(device, f.fence)
	}
	return err
}

func (f fencedContainer) DeleteDevice(ctx context.Context, device *store.Device) error {
	if err := f.fence.held(); err != nil {
		return err
	}
	return f.DeviceContainer.DeleteDevice(ctx, device)
}
