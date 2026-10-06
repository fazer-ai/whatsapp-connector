package store

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"slices"
	"strings"
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

					fence := NewFence(held)
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

// Every write a fence refuses is seen by the witness of the context it was made under, and
// by no other. A write that skipped the witness would be one whose failure a history dump
// could not see, and would be receipted without (#350).
func TestEveryRefusedWriteIsWitnessed(t *testing.T) {
	t.Parallel()

	for _, group := range fenced {
		t.Run(group.name, func(t *testing.T) {
			t.Parallel()

			for _, name := range group.writes {
				t.Run(name, func(t *testing.T) {
					t.Parallel()

					fence := NewFence(held)
					fence.Drop()
					watched, writes := WatchWrites(t.Context())
					_, elsewhere := WatchWrites(t.Context())

					method := reflect.ValueOf(group.build(fence)).MethodByName(name)
					args := zeroArgs(method.Type())
					args[0] = reflect.ValueOf(watched)
					method.Call(args)

					if !errors.Is(writes.Err(), ErrNotOwned) {
						t.Errorf("%s.%s was refused and its witness saw %v", group.name, name, writes.Err())
					}
					if elsewhere.Err() != nil {
						t.Errorf("%s.%s was seen by the witness of another context", group.name, name)
					}
				})
			}
		})
	}
}

type derivedKey struct{}

// A witness keeps the first failure, and a write that went through is not one.
func TestAWitnessKeepsTheFirstFailure(t *testing.T) {
	t.Parallel()

	ctx, writes := WatchWrites(t.Context())
	if err := witnessed(ctx, nil); err != nil || writes.Err() != nil {
		t.Fatalf("a write that went through was witnessed as %v", writes.Err())
	}
	first, second := errors.New("first"), errors.New("second")
	if err := witnessed(ctx, first); !errors.Is(err, first) {
		t.Fatalf("the error came back as %v", err)
	}
	witnessed(context.WithValue(ctx, derivedKey{}, 1), second)
	if !errors.Is(writes.Err(), first) {
		t.Fatalf("the witness kept %v, want the first failure", writes.Err())
	}
	if err := witnessed(t.Context(), second); !errors.Is(err, second) {
		t.Fatalf("an unwatched write's error came back as %v", err)
	}
}

// zeroArgs is one zero value per parameter, except the context, which no caller leaves nil
// and which a refused write still hands its witness. What the write is called with does not
// matter otherwise: the fence is asked before anything looks at them. A variadic method is
// called with none of its variadic half, which is the same nothing by another spelling.
func zeroArgs(signature reflect.Type) []reflect.Value {
	fixed := signature.NumIn()
	if signature.IsVariadic() {
		fixed--
	}
	contextType := reflect.TypeFor[context.Context]()
	args := make([]reflect.Value, 0, fixed)
	for i := range fixed {
		if signature.In(i) == contextType {
			args = append(args, reflect.ValueOf(context.Background()))
			continue
		}
		args = append(args, reflect.Zero(signature.In(i)))
	}
	return args
}

// A fence that is up lets a write through, which is the other half of the same claim: this
// fences a session that lost its lease, not every session.
func TestAHeldFenceLetsAWriteThrough(t *testing.T) {
	t.Parallel()

	if err := NewFence(held).held(); err != nil {
		t.Fatalf("a fence nobody dropped refuses a write: %v", err)
	}
	fence := NewFence(held)
	fence.Drop()
	fence.Drop()
	if !errors.Is(fence.held(), ErrNotOwned) {
		t.Error("a fence dropped twice stopped refusing")
	}
}

// held is the arbiter for a test that is not about losing a lease: the session is this
// instance's and stays that way, so what the fence refuses is what Drop refused.
func held() bool { return true }

// Every answer a fenced write gives goes through its witness: the refusal, which the test
// above exercises for every write, and the store's own answer behind it, which no fake
// here can make fail for all of them at once. A return that skipped `witnessed` is a write
// whose failure a history dump would not see (#350).
func TestEveryFencedAnswerGoesThroughTheWitness(t *testing.T) {
	t.Parallel()

	files := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("list the package: %v", err)
	}
	methods := 0
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(files, entry.Name(), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		for _, decl := range file.Decls {
			method, ok := decl.(*ast.FuncDecl)
			if !ok || method.Recv == nil || !strings.HasPrefix(receiverName(method), "fenced") || !method.Name.IsExported() {
				continue
			}
			methods++
			ast.Inspect(method.Body, func(node ast.Node) bool {
				if _, nested := node.(*ast.FuncLit); nested {
					return false
				}
				ret, ok := node.(*ast.ReturnStmt)
				if !ok {
					return true
				}
				if !answersThroughTheWitness(ret) {
					t.Errorf("%s.%s answers at %s without telling the witness",
						receiverName(method), method.Name.Name, files.Position(ret.Pos()))
				}
				return true
			})
		}
	}
	if methods != len(slices.Concat(writesOf()...)) {
		t.Errorf("found %d fenced writes in the source, the table lists %d", methods, len(slices.Concat(writesOf()...)))
	}
}

func answersThroughTheWitness(ret *ast.ReturnStmt) bool {
	if len(ret.Results) == 0 {
		return false
	}
	last, ok := ret.Results[len(ret.Results)-1].(*ast.CallExpr)
	if !ok {
		return false
	}
	name, ok := last.Fun.(*ast.Ident)
	return ok && name.Name == "witnessed"
}

func receiverName(method *ast.FuncDecl) string {
	if ident, ok := method.Recv.List[0].Type.(*ast.Ident); ok {
		return ident.Name
	}
	return ""
}

func writesOf() [][]string {
	all := make([][]string, 0, len(fenced))
	for _, group := range fenced {
		all = append(all, group.writes)
	}
	return all
}
