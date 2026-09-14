package whatsmeow

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waAdv"
	waSyncAction "go.mau.fi/whatsmeow/proto/waSyncAction"
	waStore "go.mau.fi/whatsmeow/store"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
	"github.com/fazer-ai/whatsapp-connector/internal/store/storetest"
)

const (
	ownPhone = "5511999990001"
	ownLID   = "111222333444555"
)

// The account renamed itself, the device record took the new name and the contact table
// refused it. The session that saw the rename answers with the name the account is
// actually using, which is what #139 left; a process that never saw the event has only the
// row, and the row is the one the write was supposed to level.
func TestARestartAnswersWithTheNameTheTableWouldNotTake(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-a", ownPhone, ownLID)
	theTableSays(t, client, "Antigo")
	theRecordSays(t, client, "Antigo")

	// Calibration: with both copies agreeing there is nothing to decide.
	if party := resolvedBy(t, session, ownPhone); party["push_name"] != "Antigo" {
		t.Fatalf("before anything the account is called %v, want the name both copies hold", party)
	}

	// whatsmeow's half of an app-state rename is the record; the connector's half is the
	// filing, and only that one fails.
	theRecordSays(t, client, "Atendimento")
	client.Store.Contacts = shutContacts{ContactStore: client.Store.Contacts}
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Atendimento")},
	})
	if names := session.names(); !names.pushUnfiled {
		t.Fatal("the session says its name is filed after a table that took nothing")
	}
	if party := resolvedBy(t, session, ownPhone); party["push_name"] != "Atendimento" {
		t.Fatalf("before the restart the account is called %v, want the name it renamed itself to", party)
	}

	restarted, _ := live.restart(t, session, "sid-a", ownPhone, ownLID)
	restarted.handle(&waEvents.Connected{})
	drain(t, restarted)
	if party := resolvedBy(t, restarted, ownPhone); party["push_name"] != "Atendimento" {
		t.Errorf("after the restart the account is called %v, want the name it renamed itself to", party)
	}
}

// The window between a process coming back and its socket opening is exactly when a client
// resynchronises its contacts, and `contact.resolve` is the one command written to answer
// without a socket at all. A fix that reconciles on the connection leaves the same wrong
// answer inside that window.
func TestARestartAnswersBeforeItsSocketEverOpens(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-b", ownPhone, ownLID)
	theTableSays(t, client, "Antigo")
	theRecordSays(t, client, "Atendimento")
	client.Store.Contacts = shutContacts{ContactStore: client.Store.Contacts}
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Atendimento")},
	})

	// No Connected, no socket, nothing but the session that has just been built.
	restarted, _ := live.restart(t, session, "sid-b", ownPhone, ownLID)
	if party := resolvedBy(t, restarted, ownPhone); party["push_name"] != "Atendimento" {
		t.Errorf("before connecting the account is called %v, want the name it renamed itself to", party)
	}
}

// The verified name is the harder half. whatsmeow files it under the address the change
// arrived on and never writes it to the device record, so between the row it left behind
// and the session there is no third copy: a restart that only remembered a marker would
// have nothing to put the marker beside.
func TestARestartAnswersWithTheVerifiedNameARowWouldNotTake(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-c", ownPhone, ownLID)
	phoneJID := waTypes.NewJID(ownPhone, waTypes.DefaultUserServer)
	lidJID := waTypes.NewJID(ownLID, waTypes.HiddenUserServer)
	client.Store.LID = lidJID
	session.handle(&waEvents.Connected{})
	drain(t, session)
	for _, jid := range []waTypes.JID{phoneJID, lidJID} {
		if _, _, err := client.Store.Contacts.PutBusinessName(t.Context(), jid, "Loja do Bruno"); err != nil {
			t.Fatalf("PutBusinessName: %v", err)
		}
	}

	// whatsmeow wrote the row the change arrived on before the event existed, and left the
	// other one -- the one a read goes to first -- behind.
	if _, _, err := client.Store.Contacts.PutBusinessName(t.Context(), lidJID, "Loja do Bruno LTDA"); err != nil {
		t.Fatalf("PutBusinessName: %v", err)
	}
	client.Store.Contacts = &refusingContacts{ContactStore: client.Store.Contacts, refuse: phoneJID}
	session.handle(&waEvents.BusinessName{
		JID: lidJID, OldBusinessName: "Loja do Bruno", NewBusinessName: "Loja do Bruno LTDA",
	})
	if names := session.names(); !names.verifiedUnfiled {
		t.Fatal("the session says its verified name is filed after the row a read prefers refused it")
	}
	if party := resolvedBy(t, session, ownPhone); party["verified_name"] != "Loja do Bruno LTDA" {
		t.Fatalf("before the restart the account is verified as %v, want the name it changed to", party)
	}

	restarted, _ := live.restart(t, session, "sid-c", ownPhone, ownLID)
	restarted.handle(&waEvents.Connected{})
	drain(t, restarted)
	if party := resolvedBy(t, restarted, ownPhone); party["verified_name"] != "Loja do Bruno LTDA" {
		t.Errorf("after the restart the account is verified as %v, want the name it changed to", party)
	}
}

// The commoner case, and the price this must not charge for the one above: a rename that
// only ever reached the table is answered by the table, and the row is not rewritten from
// the record. Reconciling the other way round would put an older name over a newer row,
// which is the hole the issue names.
func TestARestartTakesTheTableWhenNoWriteEverFailed(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-d", ownPhone, ownLID)
	// The record as the pairing wrote it, the table as the notify on a message the account
	// sent left it. No event reached this process and no write failed.
	theRecordSays(t, client, "Antigo")
	theTableSays(t, client, "Atendimento")

	restarted, restartedClient := live.restart(t, session, "sid-d", ownPhone, ownLID)
	restarted.handle(&waEvents.Connected{})
	drain(t, restarted)
	if party := resolvedBy(t, restarted, ownPhone); party["push_name"] != "Atendimento" {
		t.Errorf("after the restart the account is called %v, want the copy the table holds", party)
	}
	for _, jid := range bothAddresses() {
		contact, err := restartedClient.Store.Contacts.GetContact(t.Context(), jid)
		if err != nil {
			t.Fatalf("GetContact: %v", err)
		}
		if contact.PushName != "Atendimento" {
			t.Errorf("the %s row was left saying %q, want the name nothing here had reason to touch",
				jid.Server, contact.PushName)
		}
	}
}

// What is kept has to stop being true when it stops being true. Once the write lands, the
// table is level again and a copy that went on answering over it would have traded one
// wrong name for another.
func TestAKeptNameLosesToARowThatMovedOnAfterIt(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-e", ownPhone, ownLID)
	theTableSays(t, client, "Antigo")
	theRecordSays(t, client, "Atendimento")
	client.Store.Contacts = shutContacts{ContactStore: client.Store.Contacts}
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Atendimento")},
	})
	session, client = live.restart(t, session, "sid-e", ownPhone, ownLID)
	session.handle(&waEvents.Connected{})
	drain(t, session)
	if party := resolvedBy(t, session, ownPhone); party["push_name"] != "Atendimento" {
		t.Fatalf("after the first restart the account is called %v, want the name the table would not take", party)
	}

	// The table takes writes again and the account renames itself. Both rows move, and
	// whatever was being kept stops being true here.
	theRecordSays(t, client, "Recepcao")
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Recepcao")},
	})
	if names := session.names(); names.pushUnfiled {
		t.Fatal("the session says its name is unfiled after a write that took")
	}
	for _, jid := range bothAddresses() {
		contact, err := client.Store.Contacts.GetContact(t.Context(), jid)
		if err != nil {
			t.Fatalf("GetContact: %v", err)
		}
		if contact.PushName != "Recepcao" {
			t.Fatalf("the %s row says %q after a write that took, want the new name", jid.Server, contact.PushName)
		}
	}

	// And then the table alone moves again: the notify on a message the account sent from
	// another device, which writes the row, does not write the record and does not reach
	// this process at all.
	theTableSays(t, client, "Triagem")

	restarted, _ := live.restart(t, session, "sid-e", ownPhone, ownLID)
	restarted.handle(&waEvents.Connected{})
	drain(t, restarted)
	if party := resolvedBy(t, restarted, ownPhone); party["push_name"] != "Triagem" {
		t.Errorf("after the second restart the account is called %v, want the name the row moved on to", party)
	}
}

// Invariant 1: losing the lease closes the socket and fences the writes. A durable record
// written from outside that fence is a dead instance writing over what the live one has
// just written, and the whole point of the fence is that it cannot.
func TestAnInstanceThatLostItsLeaseWritesNothingDown(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-f", ownPhone, ownLID)
	theTableSays(t, client, "Antigo")
	pool := live.target.Pool(t)
	before := wholeDatabase(t, live.target, pool)

	session.store.Drop()
	// The field and not a save: whatsmeow's own write of the record goes through the same
	// fence, so a session in this state is not persisting that half either.
	client.Store.PushName = "Atendimento"
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Atendimento")},
	})
	drain(t, session)

	if after := wholeDatabase(t, live.target, pool); after != before {
		t.Errorf("a session that lost its lease wrote to the database:\nbefore %s\nafter  %s", before, after)
	}
	names := session.names()
	if !names.pushUnfiled || names.push != "Atendimento" {
		t.Errorf("the session calls itself %q (unfiled %v), want the new name held in memory", names.push, names.pushUnfiled)
	}
}

// What one session keeps is that session's, under its own key. A database is shared by a
// whole fleet, so a copy filed under the wrong one would answer for another operator's
// account, and one that leaked into a third party's answer would put the account's own
// name on somebody else's contact.
func TestAKeptNameAnswersForItsOwnSessionAndNobodyElse(t *testing.T) {
	t.Parallel()

	const (
		otherPhone = "5511999990002"
		knownThird = "5511988887777"
		emptyThird = "5511988886666"
	)

	live := aStoreThatOutlivesItsProcess(t)
	mine, myClient := live.session(t, "sid-g-a", ownPhone, ownLID)
	theirs, theirClient := live.session(t, "sid-g-b", otherPhone, "111222333444666")
	unpaired, _ := live.session(t, "sid-g-c", "", "")

	theTableSays(t, myClient, "Antigo")
	if _, _, err := theirClient.Store.Contacts.PutPushName(t.Context(),
		waTypes.NewJID(otherPhone, waTypes.DefaultUserServer), "Comercial"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}
	if _, _, err := myClient.Store.Contacts.PutPushName(t.Context(),
		waTypes.NewJID(knownThird, waTypes.DefaultUserServer), "Cliente"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}

	theRecordSays(t, myClient, "Atendimento")
	myClient.Store.Contacts = shutContacts{ContactStore: myClient.Store.Contacts}
	mine.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Atendimento")},
	})

	if err := theirs.Close(); err != nil {
		t.Fatalf("Close the other session: %v", err)
	}
	if err := unpaired.Close(); err != nil {
		t.Fatalf("Close the unpaired session: %v", err)
	}
	mine, _ = live.restart(t, mine, "sid-g-a", ownPhone, ownLID)
	theirs, _ = live.session(t, "sid-g-b", otherPhone, "111222333444666")
	unpaired, _ = live.session(t, "sid-g-c", "", "")

	if party := resolvedBy(t, mine, ownPhone); party["push_name"] != "Atendimento" {
		t.Errorf("the session that was renamed calls itself %v, want its own new name", party)
	}
	if party := resolvedBy(t, theirs, otherPhone); party["push_name"] != "Comercial" {
		t.Errorf("the other account is called %v, want the name that is actually its own", party)
	}
	if party := resolvedBy(t, mine, knownThird); party["push_name"] != "Cliente" {
		t.Errorf("a third party is called %v, want the name the table holds for them", party)
	}
	if party := resolvedBy(t, mine, emptyThird); party["push_name"] != nil {
		t.Errorf("a third party with no row is called %v, want no name at all", party)
	}
	_, err := unpaired.Execute(t.Context(),
		resolveCommand(t, `{"party":{"kind":"phone","id":"`+ownPhone+`"}}`))
	assertCode(t, err, protocol.ErrorNotPaired)
}

// aStoreThatOutlivesItsProcess is one database and whatever container is open over it. The
// container is what a restart replaces: reopening it is deliberate, because a marker kept
// in a cache of the process would survive rebuilding only the session and would not
// survive this.
type liveStore struct {
	target storetest.Target
	open   *store.Container
	// wrap stands between a session and the contact table from the moment the session is
	// built. It has to be in place before that, not after: what a session does with what
	// was left unfiled, it does while it is being built.
	wrap func(waStore.ContactStore) waStore.ContactStore
}

func aStoreThatOutlivesItsProcess(t *testing.T) *liveStore {
	t.Helper()

	live := &liveStore{target: storetest.New(t)}
	live.reopen(t)
	t.Cleanup(func() {
		if live.open != nil {
			_ = live.open.Close()
		}
	})
	return live
}

func (l *liveStore) reopen(t *testing.T) {
	t.Helper()

	if l.open != nil {
		if err := l.open.Close(); err != nil {
			t.Fatalf("Close the store: %v", err)
		}
		l.open = nil
	}
	container, err := store.Open(t.Context(), l.target.URL, store.AlwaysOwned, zerolog.Nop())
	if err != nil {
		t.Fatalf("Open the store: %v", err)
	}
	l.open = container
}

// restart is the whole of one: the session closed, the container closed, another opened
// over the same database, and a session built from the device record that was left there.
// Nothing of the process before it survives.
func (l *liveStore) restart(t *testing.T, session *Session, sid, phone, lid string) (*Session, *wm.Client) {
	t.Helper()

	if err := session.Close(); err != nil {
		t.Fatalf("Close the session: %v", err)
	}
	l.reopen(t)
	return l.session(t, sid, phone, lid)
}

// session builds one over whatever container is open, pairing the device the first time
// and reading it back every time after.
func (l *liveStore) session(t *testing.T, sid, phone, lid string) (*Session, *wm.Client) {
	t.Helper()

	scoped := l.open.For(sid)
	device, err := scoped.Device(t.Context())
	if err != nil {
		t.Fatalf("Device: %v", err)
	}
	if device.ID == nil && phone != "" {
		jid, err := waTypes.ParseJID(phone + ":12@" + waTypes.DefaultUserServer)
		if err != nil {
			t.Fatalf("ParseJID: %v", err)
		}
		device.ID = &jid
		device.LID = waTypes.NewJID(lid, waTypes.HiddenUserServer)
		device.Account = &waAdv.ADVSignedDeviceIdentity{
			Details:             make([]byte, 32),
			AccountSignature:    make([]byte, 64),
			AccountSignatureKey: make([]byte, 32),
			DeviceSignature:     make([]byte, 64),
		}
		if err := device.Save(t.Context()); err != nil {
			t.Fatalf("Save the device: %v", err)
		}
		if err := scoped.Bind(t.Context(), jid); err != nil {
			t.Fatalf("Bind: %v", err)
		}
	}
	client := wm.NewClient(device, nil)
	if l.wrap != nil {
		client.Store.Contacts = l.wrap(client.Store.Contacts)
	}
	session := newSession(t.Context(), sid, client, scoped, MediaOptions{}, zerolog.Nop(), newLibraryLogger(zerolog.Nop(), sid))
	t.Cleanup(func() { _ = session.Close() })
	return session, client
}

// theTableSays puts a push name on both of the account's rows, which is where a change
// that reached the table leaves it.
func theTableSays(t *testing.T, client *wm.Client, name string) {
	t.Helper()

	for _, jid := range bothAddresses() {
		if _, _, err := client.Store.Contacts.PutPushName(t.Context(), jid, name); err != nil {
			t.Fatalf("PutPushName: %v", err)
		}
	}
}

// theRecordSays writes the device record and saves it, which is whatsmeow's half of an
// app-state rename. Without the save nothing of it crosses a restart.
func theRecordSays(t *testing.T, client *wm.Client, name string) {
	t.Helper()

	client.Store.PushName = name
	if err := client.Store.Save(t.Context()); err != nil {
		t.Fatalf("Save the device: %v", err)
	}
}

func bothAddresses() []waTypes.JID {
	return []waTypes.JID{
		waTypes.NewJID(ownPhone, waTypes.DefaultUserServer),
		waTypes.NewJID(ownLID, waTypes.HiddenUserServer),
	}
}

// resolvedBy asks a session to resolve one number.
func resolvedBy(t *testing.T, session *Session, phone string) map[string]any {
	t.Helper()

	result, err := session.Execute(t.Context(), resolveCommand(t, `{"party":{"kind":"phone","id":"`+phone+`"}}`))
	if err != nil {
		t.Fatalf("contact.resolve: %v", err)
	}
	return resolved(t, result)
}

// shutContacts is a contact store that files no name at all, which is the shape of a
// failure that is about the write rather than about one row.
type shutContacts struct {
	waStore.ContactStore
}

func (shutContacts) PutPushName(_ context.Context, _ waTypes.JID, _ string) (changed bool, previous string, err error) {
	return false, "", errors.New("this table is not taking writes")
}

func (shutContacts) PutBusinessName(_ context.Context, _ waTypes.JID, _ string) (changed bool, previous string, err error) {
	return false, "", errors.New("this table is not taking writes")
}

// wholeDatabase is every row of every table, as text, in an order that does not depend on
// the storage engine. A count would catch a row that was added and miss one that was
// rewritten, and both are writes a fenced instance must not be making.
func wholeDatabase(t *testing.T, target storetest.Target, pool *sql.DB) string {
	t.Helper()

	listing := `SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`
	if target.Postgres() {
		listing = `SELECT table_name FROM information_schema.tables
			WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name`
	}
	tables, err := pool.QueryContext(t.Context(), listing)
	if err != nil {
		t.Fatalf("list the tables: %v", err)
	}
	var named []string
	for tables.Next() {
		var name string
		if err := tables.Scan(&name); err != nil {
			t.Fatalf("read a table name: %v", err)
		}
		named = append(named, name)
	}
	if err := tables.Err(); err != nil {
		t.Fatalf("list the tables: %v", err)
	}
	if err := tables.Close(); err != nil {
		t.Fatalf("close the listing: %v", err)
	}

	var whole strings.Builder
	for _, table := range named {
		fmt.Fprintf(&whole, "%s:%s\n", table, tableContents(t, pool, table))
	}
	return whole.String()
}

func tableContents(t *testing.T, pool *sql.DB, table string) string {
	t.Helper()

	// The name comes from the database's own catalogue, not from anything a caller passed.
	rows, err := pool.QueryContext(t.Context(), `SELECT * FROM `+table)
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("read the columns of %s: %v", table, err)
	}
	var printed []string
	for rows.Next() {
		cells := make([]sql.RawBytes, len(columns))
		into := make([]any, len(columns))
		for i := range cells {
			into[i] = &cells[i]
		}
		if err := rows.Scan(into...); err != nil {
			t.Fatalf("read a row of %s: %v", table, err)
		}
		printed = append(printed, fmt.Sprintf("%q", cells))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	// Sorted here rather than in SQL: a row order the engine chooses is not a difference,
	// and asking the database to order by every column is a different statement per table.
	slices.Sort(printed)
	sum := sha256.Sum256([]byte(strings.Join(printed, "\n")))
	return fmt.Sprintf("%d rows %s", len(printed), hex.EncodeToString(sum[:8]))
}

// A write this connector still owes is retried when the session comes back, which is what
// ends this rather than leaving the row behind until the next rename -- and what is kept is
// dropped as it stops being needed, instead of outliving its own truth in the database.
func TestARestartFilesWhatTheLastProcessCouldNot(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-h", ownPhone, ownLID)
	theTableSays(t, client, "Antigo")
	theRecordSays(t, client, "Atendimento")
	client.Store.Contacts = shutContacts{ContactStore: client.Store.Contacts}
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Atendimento")},
	})
	if _, found, err := live.open.For("sid-h").UnfiledName(t.Context(), store.UnfiledPushName); err != nil {
		t.Fatalf("UnfiledName: %v", err)
	} else if !found {
		t.Fatal("nothing was written down about a name the table would not take")
	}

	restarted, restartedClient := live.restart(t, session, "sid-h", ownPhone, ownLID)
	for _, jid := range bothAddresses() {
		contact, err := restartedClient.Store.Contacts.GetContact(t.Context(), jid)
		if err != nil {
			t.Fatalf("GetContact: %v", err)
		}
		if contact.PushName != "Atendimento" {
			t.Errorf("the %s row still says %q, want the write the last process owed", jid.Server, contact.PushName)
		}
	}
	if _, found, err := live.open.For("sid-h").UnfiledName(t.Context(), store.UnfiledPushName); err != nil {
		t.Fatalf("UnfiledName: %v", err)
	} else if found {
		t.Error("what was kept is still kept after the write it was waiting on landed")
	}
	if names := restarted.names(); names.pushUnfiled {
		t.Error("the session still says its name is unfiled after the write landed")
	}
}

// A push name learned from a message the account sent is the case with no third copy on
// that half either: whatsmeow writes the table and dispatches, and the device record is
// never touched. Remembering only that a row was behind would leave nothing to answer with.
func TestARestartAnswersAPushNameTheRecordNeverHeardOf(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-i", ownPhone, ownLID)
	lidJID := waTypes.NewJID(ownLID, waTypes.HiddenUserServer)
	client.Store.LID = lidJID
	session.handle(&waEvents.Connected{})
	drain(t, session)
	theTableSays(t, client, "Antigo")
	theRecordSays(t, client, "Antigo")

	// whatsmeow wrote the row the notify arrived on and dispatched. The record is not on
	// this path at all, so it is still holding what the pairing left.
	if _, _, err := client.Store.Contacts.PutPushName(t.Context(), lidJID, "Atendimento"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}
	client.Store.Contacts = &refusingContacts{
		ContactStore: client.Store.Contacts, refuse: waTypes.NewJID(ownPhone, waTypes.DefaultUserServer),
	}
	session.handle(&waEvents.PushName{
		JID: lidJID, OldPushName: "Antigo", NewPushName: "Atendimento",
	})
	if names := session.names(); !names.pushUnfiled {
		t.Fatal("the session says its name is filed after the row a read prefers refused it")
	}

	restarted, _ := live.restart(t, session, "sid-i", ownPhone, ownLID)
	if party := resolvedBy(t, restarted, ownPhone); party["push_name"] != "Atendimento" {
		t.Errorf("after the restart the account is called %v, want the name the notify brought", party)
	}
}

// Each name is compared against its own column. A row carries both, and they move on their
// own: comparing the verified name against the push name's column would drop a kept
// verified name because somebody's push name changed, and keep one because it did not.
func TestAKeptVerifiedNameIsComparedAgainstItsOwnColumn(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-j", ownPhone, ownLID)
	phoneJID := waTypes.NewJID(ownPhone, waTypes.DefaultUserServer)
	lidJID := waTypes.NewJID(ownLID, waTypes.HiddenUserServer)
	client.Store.LID = lidJID
	session.handle(&waEvents.Connected{})
	drain(t, session)
	// The row carries both names, which is the ordinary shape of one.
	theTableSays(t, client, "Antigo")
	for _, jid := range bothAddresses() {
		if _, _, err := client.Store.Contacts.PutBusinessName(t.Context(), jid, "Loja do Bruno"); err != nil {
			t.Fatalf("PutBusinessName: %v", err)
		}
	}
	if _, _, err := client.Store.Contacts.PutBusinessName(t.Context(), lidJID, "Loja do Bruno LTDA"); err != nil {
		t.Fatalf("PutBusinessName: %v", err)
	}
	table := client.Store.Contacts
	client.Store.Contacts = &refusingContacts{ContactStore: table, refuse: phoneJID}
	session.handle(&waEvents.BusinessName{
		JID: lidJID, OldBusinessName: "Loja do Bruno", NewBusinessName: "Loja do Bruno LTDA",
	})

	// The push name of the same row moves afterwards, and says nothing about the verified
	// one: a notify writes that column and leaves this one where it was. Written through
	// the table itself, because what refused the verified name is not what writes this.
	if _, _, err := table.PutPushName(t.Context(), phoneJID, "Atendimento"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}

	restarted, _ := live.restart(t, session, "sid-j", ownPhone, ownLID)
	if party := resolvedBy(t, restarted, ownPhone); party["verified_name"] != "Loja do Bruno LTDA" {
		t.Errorf("after the restart the account is verified as %v, want the name a push name change says nothing about", party)
	}
}

// Without what the row was holding there is nothing to compare against later, and a copy
// that answers unconditionally is the one that outlives its own truth. Writing it down
// half-known is worse than not writing it down: the session's memory already covers this
// process, and the next one would inherit a comparison it cannot make.
func TestANameIsNotKeptWhenTheRowCouldNotBeRead(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-k", ownPhone, ownLID)
	theTableSays(t, client, "Antigo")
	theRecordSays(t, client, "Atendimento")
	client.Store.Contacts = blindContacts{ContactStore: client.Store.Contacts}
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Atendimento")},
	})

	if names := session.names(); !names.pushUnfiled || names.push != "Atendimento" {
		t.Errorf("the session calls itself %q (unfiled %v), want the new name held in memory",
			names.push, names.pushUnfiled)
	}
	if _, found, err := live.open.For("sid-k").UnfiledName(t.Context(), store.UnfiledPushName); err != nil {
		t.Fatalf("UnfiledName: %v", err)
	} else if found {
		t.Error("a name was written down beside a row nothing could read")
	}
}

// blindContacts takes no name and answers no question about one, which is the store that
// has gone away entirely rather than refused one row.
type blindContacts struct {
	waStore.ContactStore
}

func (blindContacts) PutPushName(_ context.Context, _ waTypes.JID, _ string) (changed bool, previous string, err error) {
	return false, "", errors.New("this table is not taking writes")
}

func (blindContacts) PutBusinessName(_ context.Context, _ waTypes.JID, _ string) (changed bool, previous string, err error) {
	return false, "", errors.New("this table is not taking writes")
}

func (blindContacts) GetContact(_ context.Context, _ waTypes.JID) (waTypes.ContactInfo, error) {
	return waTypes.ContactInfo{}, errors.New("this table is not answering reads")
}

// A row that moved on after the failure is the newer copy of the two, so what was kept is
// answered over and then dropped: leaving it would have every later restart asking the
// same settled question, and a row that never goes is a row that outlives its pairing.
func TestAKeptNameIsDroppedWhenTheRowMovedOnWithoutIt(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-l", ownPhone, ownLID)
	theTableSays(t, client, "Antigo")
	theRecordSays(t, client, "Atendimento")
	table := client.Store.Contacts
	client.Store.Contacts = shutContacts{ContactStore: table}
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Atendimento")},
	})

	// The row moves without this process filing anything: the notify on a message the
	// account sent from another device, arriving at whoever holds the session next.
	for _, jid := range bothAddresses() {
		if _, _, err := table.PutPushName(t.Context(), jid, "Triagem"); err != nil {
			t.Fatalf("PutPushName: %v", err)
		}
	}

	restarted, _ := live.restart(t, session, "sid-l", ownPhone, ownLID)
	if party := resolvedBy(t, restarted, ownPhone); party["push_name"] != "Triagem" {
		t.Errorf("after the restart the account is called %v, want the name the row moved on to", party)
	}
	if _, found, err := live.open.For("sid-l").UnfiledName(t.Context(), store.UnfiledPushName); err != nil {
		t.Fatalf("UnfiledName: %v", err)
	} else if found {
		t.Error("a copy that lost to the row it was compared against is still kept")
	}
}

// A read takes the phone row and falls back to the LID row for what that one does not
// hold, so an account whose phone row carries no name is answered from its LID row. What
// is kept has to be compared against the answer, not against one row: otherwise a name
// that arrived on the LID after the failure is invisible, and gets replayed over.
func TestAKeptNameLosesToTheRowThatIsActuallyAnswering(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-m", ownPhone, ownLID)
	lidJID := waTypes.NewJID(ownLID, waTypes.HiddenUserServer)
	client.Store.LID = lidJID
	session.handle(&waEvents.Connected{})
	drain(t, session)
	// The phone row holds no push name at all, so the LID row is the one answering.
	table := client.Store.Contacts
	if _, _, err := table.PutPushName(t.Context(), lidJID, "Antigo"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}
	theRecordSays(t, client, "Atendimento")
	client.Store.Contacts = shutContacts{ContactStore: table}
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Atendimento")},
	})

	// And then a newer name lands on the LID row, which this process never hears about.
	if _, _, err := table.PutPushName(t.Context(), lidJID, "Triagem"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}

	restarted, restartedClient := live.restart(t, session, "sid-m", ownPhone, ownLID)
	if party := resolvedBy(t, restarted, ownPhone); party["push_name"] != "Triagem" {
		t.Errorf("after the restart the account is called %v, want the name that arrived after the failure", party)
	}
	contact, err := restartedClient.Store.Contacts.GetContact(t.Context(), lidJID)
	if err != nil {
		t.Fatalf("GetContact: %v", err)
	}
	if contact.PushName != "Triagem" {
		t.Errorf("the LID row was left saying %q, want the name nothing here was newer than", contact.PushName)
	}
}

// The verified half of the same ending: the write the last process owed is made, the rows
// are level again, and what was kept is dropped rather than outliving the question it was
// there to answer.
func TestARestartFilesTheVerifiedNameTheLastProcessCouldNot(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-n", ownPhone, ownLID)
	phoneJID := waTypes.NewJID(ownPhone, waTypes.DefaultUserServer)
	lidJID := waTypes.NewJID(ownLID, waTypes.HiddenUserServer)
	client.Store.LID = lidJID
	session.handle(&waEvents.Connected{})
	drain(t, session)
	for _, jid := range bothAddresses() {
		if _, _, err := client.Store.Contacts.PutBusinessName(t.Context(), jid, "Loja do Bruno"); err != nil {
			t.Fatalf("PutBusinessName: %v", err)
		}
	}
	if _, _, err := client.Store.Contacts.PutBusinessName(t.Context(), lidJID, "Loja do Bruno LTDA"); err != nil {
		t.Fatalf("PutBusinessName: %v", err)
	}
	client.Store.Contacts = &refusingContacts{ContactStore: client.Store.Contacts, refuse: phoneJID}
	session.handle(&waEvents.BusinessName{
		JID: lidJID, OldBusinessName: "Loja do Bruno", NewBusinessName: "Loja do Bruno LTDA",
	})
	if _, found, err := live.open.For("sid-n").UnfiledName(t.Context(), store.UnfiledVerifiedName); err != nil {
		t.Fatalf("UnfiledName: %v", err)
	} else if !found {
		t.Fatal("nothing was written down about a verified name the row would not take")
	}

	restarted, restartedClient := live.restart(t, session, "sid-n", ownPhone, ownLID)
	contact, err := restartedClient.Store.Contacts.GetContact(t.Context(), phoneJID)
	if err != nil {
		t.Fatalf("GetContact: %v", err)
	}
	if contact.BusinessName != "Loja do Bruno LTDA" {
		t.Errorf("the phone row still says %q, want the write the last process owed", contact.BusinessName)
	}
	if _, found, err := live.open.For("sid-n").UnfiledName(t.Context(), store.UnfiledVerifiedName); err != nil {
		t.Fatalf("UnfiledName: %v", err)
	} else if found {
		t.Error("what was kept is still kept after the write it was waiting on landed")
	}
	if names := restarted.names(); names.verifiedUnfiled {
		t.Error("the session still says its verified name is unfiled after the write landed")
	}
}

// A deadline that ran out is one of the ways the filing fails, and it is the one a loaded
// store produces: this connector gives the filing its own budget while whatsmeow saves the
// device record under a deadline of its own. Recording it on the deadline that ran out
// would lose exactly the failure the record is for.
func TestANameTheWriteRanOutOfTimeForIsStillWrittenDown(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-o", ownPhone, ownLID)
	theTableSays(t, client, "Antigo")
	theRecordSays(t, client, "Atendimento")
	// Short enough that the test does not wait on the real one, which is five seconds.
	session.storeLimit = 150 * time.Millisecond
	client.Store.Contacts = stalledContacts{ContactStore: client.Store.Contacts}
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Atendimento")},
	})

	kept, found, err := live.open.For("sid-o").UnfiledName(t.Context(), store.UnfiledPushName)
	if err != nil {
		t.Fatalf("UnfiledName: %v", err)
	}
	if !found {
		t.Fatal("nothing was written down about a name whose write ran out of time")
	}
	if kept.Name != "Atendimento" {
		t.Errorf("what was kept reads %+v, want the name the write ran out of time for", kept)
	}

	restarted, _ := live.restart(t, session, "sid-o", ownPhone, ownLID)
	if party := resolvedBy(t, restarted, ownPhone); party["push_name"] != "Atendimento" {
		t.Errorf("after the restart the account is called %v, want the name it renamed itself to", party)
	}
}

// stalledContacts is a table that answers reads and never finishes a write, which is what
// one under load looks like from here: the deadline decides, not the store.
type stalledContacts struct {
	waStore.ContactStore
}

func (stalledContacts) PutPushName(ctx context.Context, _ waTypes.JID, _ string) (changed bool, previous string, err error) {
	<-ctx.Done()
	return false, "", ctx.Err()
}

func (stalledContacts) PutBusinessName(ctx context.Context, _ waTypes.JID, _ string) (changed bool, previous string, err error) {
	<-ctx.Done()
	return false, "", ctx.Err()
}

// Two rows must not be able to spell what one row spells. Without a length beside each
// value, a name moving from one row to the other leaves the same string behind, the
// comparison sees nothing move, and a kept name is replayed over a newer one.
func TestTwoRowsCannotSpellWhatOneRowSpells(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-p", ownPhone, ownLID)
	phoneJID := waTypes.NewJID(ownPhone, waTypes.DefaultUserServer)
	lidJID := waTypes.NewJID(ownLID, waTypes.HiddenUserServer)
	client.Store.LID = lidJID
	session.handle(&waEvents.Connected{})
	drain(t, session)
	table := client.Store.Contacts
	// The whole of it on the phone row, nothing on the LID row.
	if _, _, err := table.PutPushName(t.Context(), phoneJID, "AnaBia"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}
	theRecordSays(t, client, "Atendimento")
	client.Store.Contacts = shutContacts{ContactStore: table}
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Atendimento")},
	})

	// And then the same letters, split across the two rows. Concatenated with nothing
	// between them the two states are the same string; they are not the same state, and
	// the second one is newer.
	if _, _, err := table.PutPushName(t.Context(), phoneJID, "Ana"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}
	if _, _, err := table.PutPushName(t.Context(), lidJID, "Bia"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}

	restarted, _ := live.restart(t, session, "sid-p", ownPhone, ownLID)
	if party := resolvedBy(t, restarted, ownPhone); party["push_name"] != "Ana" {
		t.Errorf("after the restart the account is called %v, want the name the rows moved on to", party)
	}
}

// A retry that lands on one address and not on the other moves a row without anybody else
// having written it. Read as somebody's newer rename, it would have the process that comes
// next throw away the only copy of the name and answer from the row the retry never
// reached -- and permanently, because nothing would be left to try again.
func TestAPartlyLandedRetryIsNotMistakenForSomebodyElsesRename(t *testing.T) {
	t.Parallel()

	live := aStoreThatOutlivesItsProcess(t)
	session, client := live.session(t, "sid-q", ownPhone, ownLID)
	phoneJID := waTypes.NewJID(ownPhone, waTypes.DefaultUserServer)
	lidJID := waTypes.NewJID(ownLID, waTypes.HiddenUserServer)
	client.Store.LID = lidJID
	session.handle(&waEvents.Connected{})
	drain(t, session)
	theTableSays(t, client, "Antigo")
	theRecordSays(t, client, "Atendimento")
	table := client.Store.Contacts
	client.Store.Contacts = shutContacts{ContactStore: table}
	session.handle(&waEvents.PushNameSetting{
		Action: &waSyncAction.PushNameSetting{Name: proto.String("Atendimento")},
	})

	// The retry of a process that is no longer here: the LID row took the name, the phone
	// row did not, and nothing got as far as writing the new state down.
	if _, _, err := table.PutPushName(t.Context(), lidJID, "Atendimento"); err != nil {
		t.Fatalf("PutPushName: %v", err)
	}

	// And this process cannot file it either, so the answer rests on what was kept.
	live.wrap = func(table waStore.ContactStore) waStore.ContactStore {
		return shutContacts{ContactStore: table}
	}
	restarted, restartedClient := live.restart(t, session, "sid-q", ownPhone, ownLID)
	if party := resolvedBy(t, restarted, ownPhone); party["push_name"] != "Atendimento" {
		t.Errorf("after the restart the account is called %v, want the name its own retry was writing", party)
	}
	if _, found, err := live.open.For("sid-q").UnfiledName(t.Context(), store.UnfiledPushName); err != nil {
		t.Fatalf("UnfiledName: %v", err)
	} else if !found {
		t.Error("the only copy of the name was thrown away over this session's own retry")
	}
	if contact, err := restartedClient.Store.Contacts.GetContact(t.Context(), phoneJID); err != nil {
		t.Fatalf("GetContact: %v", err)
	} else if contact.PushName != "Antigo" {
		t.Fatalf("the phone row says %q, want the row the retry never reached", contact.PushName)
	}
}
