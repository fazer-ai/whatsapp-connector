package store

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"go.mau.fi/whatsmeow/store"
)

// fenced is one of whatsmeow's stores, the decorator this package puts in front of it,
// and which of its methods write.
//
// The two lists are the point. Reflection compares them against the interface as the
// library actually declares it, so a method added upstream fails this test until somebody
// says which it is -- and a write that nobody classified is a write that would otherwise
// reach the database from a session this instance stopped owning.
var fenced = []struct {
	name   string
	iface  reflect.Type
	build  func(*Fence) any
	reads  []string
	writes []string
}{
	{
		name:   "IdentityStore",
		iface:  reflect.TypeOf((*store.IdentityStore)(nil)).Elem(),
		build:  func(f *Fence) any { return fencedIdentities{fence: f} },
		reads:  []string{"IsTrustedIdentity"},
		writes: []string{"PutIdentity", "DeleteAllIdentities", "DeleteIdentity"},
	},
	{
		name:   "SessionStore",
		iface:  reflect.TypeOf((*store.SessionStore)(nil)).Elem(),
		build:  func(f *Fence) any { return fencedSessions{fence: f} },
		reads:  []string{"GetSession", "HasSession", "GetManySessions"},
		writes: []string{"PutSession", "PutManySessions", "DeleteAllSessions", "DeleteSession", "MigratePNToLID"},
	},
	{
		name:   "PreKeyStore",
		iface:  reflect.TypeOf((*store.PreKeyStore)(nil)).Elem(),
		build:  func(f *Fence) any { return fencedPreKeys{fence: f} },
		reads:  []string{"GetPreKey", "UploadedPreKeyCount"},
		writes: []string{"GetOrGenPreKeys", "GenOnePreKey", "RemovePreKey", "MarkPreKeysAsUploaded"},
	},
	{
		name:   "SenderKeyStore",
		iface:  reflect.TypeOf((*store.SenderKeyStore)(nil)).Elem(),
		build:  func(f *Fence) any { return fencedSenderKeys{fence: f} },
		reads:  []string{"GetSenderKey"},
		writes: []string{"PutSenderKey"},
	},
	{
		name:   "AppStateSyncKeyStore",
		iface:  reflect.TypeOf((*store.AppStateSyncKeyStore)(nil)).Elem(),
		build:  func(f *Fence) any { return fencedAppStateKeys{fence: f} },
		reads:  []string{"GetAppStateSyncKey", "GetLatestAppStateSyncKeyID", "GetAllAppStateSyncKeys"},
		writes: []string{"PutAppStateSyncKey"},
	},
	{
		name:   "AppStateStore",
		iface:  reflect.TypeOf((*store.AppStateStore)(nil)).Elem(),
		build:  func(f *Fence) any { return fencedAppState{fence: f} },
		reads:  []string{"GetAppStateVersion", "GetAppStateMutationMAC"},
		writes: []string{"PutAppStateVersion", "DeleteAppStateVersion", "PutAppStateMutationMACs", "DeleteAppStateMutationMACs"},
	},
	{
		name:   "ContactStore",
		iface:  reflect.TypeOf((*store.ContactStore)(nil)).Elem(),
		build:  func(f *Fence) any { return fencedContacts{fence: f} },
		reads:  []string{"GetContact", "GetAllContacts"},
		writes: []string{"PutPushName", "PutBusinessName", "PutContactName", "PutAllContactNames", "PutManyRedactedPhones"},
	},
	{
		name:   "ChatSettingsStore",
		iface:  reflect.TypeOf((*store.ChatSettingsStore)(nil)).Elem(),
		build:  func(f *Fence) any { return fencedChatSettings{fence: f} },
		reads:  []string{"GetChatSettings"},
		writes: []string{"PutMutedUntil", "PutPinned", "PutArchived"},
	},
	{
		name:   "MsgSecretStore",
		iface:  reflect.TypeOf((*store.MsgSecretStore)(nil)).Elem(),
		build:  func(f *Fence) any { return fencedMsgSecrets{fence: f} },
		reads:  []string{"GetMessageSecret"},
		writes: []string{"PutMessageSecrets", "PutMessageSecret"},
	},
	{
		name:   "PrivacyTokenStore",
		iface:  reflect.TypeOf((*store.PrivacyTokenStore)(nil)).Elem(),
		build:  func(f *Fence) any { return fencedPrivacyTokens{fence: f} },
		reads:  []string{"GetPrivacyToken"},
		writes: []string{"PutPrivacyTokens", "DeleteExpiredPrivacyTokens"},
	},
	{
		name:   "NCTSaltStore",
		iface:  reflect.TypeOf((*store.NCTSaltStore)(nil)).Elem(),
		build:  func(f *Fence) any { return fencedNCTSalt{fence: f} },
		reads:  []string{"GetNCTSalt"},
		writes: []string{"PutNCTSalt", "DeleteNCTSalt"},
	},
	{
		name:  "EventBuffer",
		iface: reflect.TypeOf((*store.EventBuffer)(nil)).Elem(),
		build: func(f *Fence) any { return fencedEventBuffer{fence: f} },
		reads: []string{"GetBufferedEvent", "GetOutgoingEvent"},
		writes: []string{
			"PutBufferedEvent", "DoDecryptionTxn", "ClearBufferedEventPlaintext",
			"DeleteOldBufferedHashes", "AddOutgoingEvent", "DeleteOldOutgoingEvents",
		},
	},
	{
		name:   "LIDStore",
		iface:  reflect.TypeOf((*store.LIDStore)(nil)).Elem(),
		build:  func(f *Fence) any { return fencedLIDs{fence: f} },
		reads:  []string{"GetPNForLID", "GetLIDForPN", "GetManyLIDsForPNs"},
		writes: []string{"PutManyLIDMappings", "PutLIDMapping"},
	},
	{
		name:   "DeviceContainer",
		iface:  reflect.TypeOf((*store.DeviceContainer)(nil)).Elem(),
		build:  func(f *Fence) any { return fencedContainer{fence: f} },
		reads:  nil,
		writes: []string{"PutDevice", "DeleteDevice"},
	},
}

// Every method whatsmeow declares is either a read this package lets through or a write it
// fences. A library that grows a method fails here rather than quietly gaining a way past
// the fence.
func TestEveryStoreMethodIsClassified(t *testing.T) {
	t.Parallel()

	for _, group := range fenced {
		t.Run(group.name, func(t *testing.T) {
			t.Parallel()

			classified := slices.Concat(group.reads, group.writes)
			slices.Sort(classified)
			declared := make([]string, 0, group.iface.NumMethod())
			for i := range group.iface.NumMethod() {
				declared = append(declared, group.iface.Method(i).Name)
			}
			slices.Sort(declared)
			if !slices.Equal(classified, declared) {
				t.Errorf("whatsmeow declares %v; this package classifies %v", declared, classified)
			}
		})
	}
}

// And every write actually goes through the fence.
//
// The delegate is nil on purpose. A write this package overrides answers the dropped fence
// and never reaches it; one that is only promoted from the embedded interface calls
// straight through and panics on the nil, which is what makes a missing override a failure
// here rather than a silent hole.
func TestEveryWriteIsFenced(t *testing.T) {
	t.Parallel()

	for _, group := range fenced {
		t.Run(group.name, func(t *testing.T) {
			t.Parallel()

			for _, name := range group.writes {
				t.Run(name, func(t *testing.T) {
					t.Parallel()

					fence := &Fence{}
					fence.Drop()

					method := reflect.ValueOf(group.build(fence)).MethodByName(name)
					if !method.IsValid() {
						t.Fatalf("%s has no %s at all", group.name, name)
					}

					defer func() {
						if panicked := recover(); panicked != nil {
							t.Fatalf("%s.%s reached the store it stands in front of: %v", group.name, name, panicked)
						}
					}()
					returned := method.Call(zeroArgs(method.Type()))
					last := returned[len(returned)-1]
					err, _ := last.Interface().(error)
					if !errors.Is(err, ErrNotOwned) {
						t.Errorf("%s.%s answered %v, want ErrNotOwned", group.name, name, err)
					}
				})
			}
		})
	}
}

// zeroArgs is one zero value per parameter. What the write is called with does not matter:
// the fence is asked before anything looks at them. A variadic method is called with none
// of its variadic half, which is the same nothing by another spelling.
func zeroArgs(signature reflect.Type) []reflect.Value {
	fixed := signature.NumIn()
	if signature.IsVariadic() {
		fixed--
	}
	args := make([]reflect.Value, 0, fixed)
	for i := range fixed {
		args = append(args, reflect.Zero(signature.In(i)))
	}
	return args
}

// A fence that is up lets a write through, which is the other half of the same claim: this
// fences a session that lost its lease, not every session.
func TestAHeldFenceLetsAWriteThrough(t *testing.T) {
	t.Parallel()

	if err := (&Fence{}).held(); err != nil {
		t.Fatalf("a fence nobody dropped refuses a write: %v", err)
	}
	fence := &Fence{}
	fence.Drop()
	fence.Drop()
	if !errors.Is(fence.held(), ErrNotOwned) {
		t.Error("a fence dropped twice stopped refusing")
	}
}
