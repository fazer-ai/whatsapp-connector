package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// deviceCounter gives every pairing its own device part, the way WhatsApp does.
var deviceCounter atomic.Uint32

func TestOpenRejectsAnUnknownURL(t *testing.T) {
	t.Parallel()

	for _, url := range []string{"", "mysql://localhost/wa", "/var/lib/wa.db"} {
		if _, err := store.Open(t.Context(), url, zerolog.Nop()); err == nil {
			t.Fatalf("opened %q, want an error", url)
		}
	}
}

func TestDeviceIsFreshUntilSomethingIsBound(t *testing.T) {
	t.Parallel()
	container := open(t)

	device, err := container.For("sid-1").Device(t.Context())
	if err != nil {
		t.Fatalf("Device: %v", err)
	}
	if device.ID != nil {
		t.Fatalf("a session that never paired came back with a jid: %s", device.ID)
	}
	if device.NoiseKey == nil || device.IdentityKey == nil {
		t.Fatal("a fresh device came back without its keys")
	}
}

func TestDeviceResumesWhatWasBound(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	jid := pair(t, container, "sid-1", "5511999990001")

	device, err := container.For("sid-1").Device(ctx)
	if err != nil {
		t.Fatalf("Device: %v", err)
	}
	if device.ID == nil || device.ID.User != jid.User {
		t.Fatalf("resumed %v, want the device of %s", device.ID, jid)
	}

	// A different session must not be handed the same credentials.
	other, err := container.For("sid-2").Device(ctx)
	if err != nil {
		t.Fatalf("Device: %v", err)
	}
	if other.ID != nil {
		t.Fatalf("a second session resumed %s", other.ID)
	}
}

func TestBindMovesADeviceOffItsPreviousSession(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	jid := pair(t, container, "sid-old", "5511999990001")
	if err := container.For("sid-new").Bind(ctx, jid); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	if _, bound, err := container.For("sid-old").JID(ctx); err != nil || bound {
		t.Fatalf("the old session is still bound (bound=%v, err=%v)", bound, err)
	}
	bound, ok, err := container.For("sid-new").JID(ctx)
	if err != nil || !ok || bound.User != jid.User {
		t.Fatalf("the new session holds %v (ok=%v, err=%v)", bound, ok, err)
	}
}

func TestDeviceForgetsAMappingWhoseDeviceIsGone(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	// A mapping written for a device that was never stored is what a crash between
	// Bind and whatsmeow's own write leaves behind.
	jid, err := types.ParseJID("5511999990002@s.whatsapp.net")
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	if err := container.For("sid-1").Bind(ctx, jid); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	device, err := container.For("sid-1").Device(ctx)
	if err != nil {
		t.Fatalf("Device: %v", err)
	}
	if device.ID != nil {
		t.Fatalf("resumed %s from a device that is not stored", device.ID)
	}
	if _, bound, err := container.For("sid-1").JID(ctx); err != nil || bound {
		t.Fatalf("the dangling mapping survived (bound=%v, err=%v)", bound, err)
	}
}

func TestForgetRemovesTheDeviceAndTheMapping(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	jid := pair(t, container, "sid-1", "5511999990001")
	if err := container.For("sid-1").Forget(ctx); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	if _, bound, err := container.For("sid-1").JID(ctx); err != nil || bound {
		t.Fatalf("the mapping survived a logout (bound=%v, err=%v)", bound, err)
	}
	stored, err := container.Devices().GetDevice(ctx, jid)
	if err != nil {
		t.Fatalf("GetDevice: %v", err)
	}
	if stored != nil {
		t.Fatal("the device survived a logout")
	}

	// Forgetting a session that never paired is what a delete on an unpaired inbox
	// does, and it is not an error.
	if err := container.For("sid-never").Forget(ctx); err != nil {
		t.Fatalf("Forget on an unpaired session: %v", err)
	}
}

func TestBindRefusesAnIncompleteCall(t *testing.T) {
	t.Parallel()
	container := open(t)

	jid, err := types.ParseJID("5511999990001@s.whatsapp.net")
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	if err := container.For("").Bind(t.Context(), jid); err == nil {
		t.Fatal("bound a device to no session")
	}
	if err := container.For("sid-1").Bind(t.Context(), types.EmptyJID); err == nil {
		t.Fatal("bound a session to no device")
	}
}

// open returns a container on its own SQLite file, which is deleted with the test.
func open(t *testing.T) *store.Container {
	t.Helper()

	address := "sqlite:" + filepath.Join(t.TempDir(), "wac.db")
	container, err := store.Open(t.Context(), address, zerolog.Nop())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return container
}

// pair stores a device for a phone number and binds it to a session, which is what
// whatsmeow and the engine between them do on a successful pairing.
func pair(t *testing.T, container *store.Container, sid, phone string) types.JID {
	t.Helper()
	ctx := context.Background()
	// WhatsApp issues a companion device JID, device part and all, and that is the key
	// whatsmeow files the device under. Each pairing gets its own, the way a real one
	// does.
	jid, err := types.ParseJID(fmt.Sprintf("%s:%d@s.whatsapp.net", phone, deviceCounter.Add(1)))
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	device := container.Devices().NewDevice()
	device.ID = &jid
	// whatsmeow fills the account identity during pairing and refuses to store a
	// device without one, so the test stands in for that half.
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             make([]byte, 32),
		AccountSignature:    make([]byte, 64),
		AccountSignatureKey: make([]byte, 32),
		DeviceSignature:     make([]byte, 64),
	}
	if err := container.Devices().PutDevice(ctx, device); err != nil {
		t.Fatalf("PutDevice: %v", err)
	}
	if err := container.For(sid).Bind(ctx, jid); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return jid
}

// WhatsApp pairs a companion device and names it with the device part attached. That
// full JID is the key whatsmeow files the device under, so a mapping that drops it
// finds nothing on the next boot, reads the session as unpaired, and asks the operator
// to scan another code for a session that never stopped being paired.
func TestDeviceResumesACompanionDeviceJID(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	jid := pair(t, container, "sid-1", "5511999990001")
	if jid.Device == 0 {
		t.Fatalf("the test paired %s, which carries no device part", jid)
	}

	device, err := container.For("sid-1").Device(ctx)
	if err != nil {
		t.Fatalf("Device: %v", err)
	}
	if device.ID == nil {
		t.Fatal("a paired companion device came back unpaired")
	}
	if device.ID.String() != jid.String() {
		t.Fatalf("resumed %s, want %s", device.ID, jid)
	}
}

// Re-pairing an account issues a new device rather than reusing the old one, so the
// rule is one session per account and the credentials it displaces are deleted: they
// are unreachable, and WhatsApp has already unlinked them.
func TestBindDeletesTheDeviceItDisplaces(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	old := pair(t, container, "sid-old", "5511999990001")
	fresh := pair(t, container, "sid-new", "5511999990001")
	if old.String() == fresh.String() {
		t.Fatal("the test paired the same device twice")
	}

	if _, bound, err := container.For("sid-old").JID(ctx); err != nil || bound {
		t.Fatalf("the displaced session is still bound (bound=%v, err=%v)", bound, err)
	}
	stored, err := container.Devices().GetDevice(ctx, old)
	if err != nil {
		t.Fatalf("GetDevice: %v", err)
	}
	if stored != nil {
		t.Fatal("the displaced device survived, holding credentials nothing can reach")
	}
}

// whatsmeow refuses to bring its schema up on SQLite with foreign keys off, and the
// driver leaves them off. An operator writing the documented url should not have to
// know that.
func TestSQLiteOpensWithoutSpellingOutThePragma(t *testing.T) {
	t.Parallel()

	address := "sqlite:" + filepath.Join(t.TempDir(), "wac.db")
	container, err := store.Open(t.Context(), address, zerolog.Nop())
	if err != nil {
		t.Fatalf("Open %q: %v", address, err)
	}
	if err := container.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// whatsmeow writes its own tables from every path it has at once: a receipt saves a
// session while a decrypt saves an identity while the connector records a pairing. The
// first live pairing turned that into a wall of `database is locked (5)`, which is not
// cosmetic: an identity key that cannot be saved is a message that cannot be decrypted,
// and the app state key share inside one of those is why the account's contacts and
// chats never synced either.
//
// The test writes through both halves of the container at once, which is the shape that
// failed. It is not a stress test and a passing run is not a promise about a busy
// deployment; it is a floor, and the floor is that ordinary concurrent use does not
// return an error.
func TestConcurrentWritersDoNotCollide(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	const writers = 8
	const rounds = 12

	failures := make(chan error, writers*rounds*2)
	var wg sync.WaitGroup
	for writer := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := range rounds {
				jid, err := types.ParseJID(fmt.Sprintf("55119999%05d:%d@s.whatsapp.net", writer, round+1))
				if err != nil {
					failures <- err
					return
				}
				device := container.Devices().NewDevice()
				device.ID = &jid
				device.Account = &waAdv.ADVSignedDeviceIdentity{
					Details:             make([]byte, 32),
					AccountSignature:    make([]byte, 64),
					AccountSignatureKey: make([]byte, 32),
					DeviceSignature:     make([]byte, 64),
				}
				if err := container.Devices().PutDevice(ctx, device); err != nil {
					failures <- fmt.Errorf("PutDevice: %w", err)
					return
				}
				if err := container.For(fmt.Sprintf("sid-%d", writer)).Bind(ctx, jid); err != nil {
					failures <- fmt.Errorf("Bind: %w", err)
					return
				}
				if _, _, err := container.For(fmt.Sprintf("sid-%d", writer)).JID(ctx); err != nil {
					failures <- fmt.Errorf("JID: %w", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(failures)

	for err := range failures {
		t.Errorf("a concurrent writer failed: %v", err)
	}
}

// The rule is one session per account, and Bind used to enforce it by reading the
// competing mappings and then writing. Two pairings of the same number landing together
// both read nothing and both write, so the account ends up on two sessions and the
// displacement never happens: the second session's socket works while the first's
// credentials are still on file, and a restart hands the account to whichever row is
// found. A constraint is the only version of that check two writers cannot both pass.
func TestOneAccountKeepsOneMappingUnderConcurrentPairings(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	const account = "5511999990001"
	const pairings = 8

	var wg sync.WaitGroup
	failures := make(chan error, pairings)
	for i := range pairings {
		wg.Add(1)
		go func() {
			defer wg.Done()
			jid, err := types.ParseJID(fmt.Sprintf("%s:%d@s.whatsapp.net", account, deviceCounter.Add(1)))
			if err != nil {
				failures <- err
				return
			}
			// A pairing that loses the race is a pairing refused, which is a correct
			// outcome; a pairing that wins and leaves a second mapping behind is not.
			if err := container.For(fmt.Sprintf("sid-%d", i)).Bind(ctx, jid); err != nil {
				failures <- err
			}
		}()
	}
	wg.Wait()
	close(failures)

	var bound int
	for i := range pairings {
		if _, ok, err := container.For(fmt.Sprintf("sid-%d", i)).JID(ctx); err != nil {
			t.Fatalf("JID: %v", err)
		} else if ok {
			bound++
		}
	}
	if bound != 1 {
		t.Fatalf("%d sessions hold the same account, want exactly 1", bound)
	}
}

// The account index shipped non-unique, and `CREATE UNIQUE INDEX IF NOT EXISTS` under
// the same name is a no-op against a database that already has it. An upgraded store
// "There is no device" and "the device could not be read" are the same value out of a
// lookup and opposite facts. Read as absence, a row this store cannot parse costs its
// mapping the contest, and the winner is then the half-written one — so the upgrade
// deletes the mapping of a session that was paired, over a device that was there all
// along. The migration stops instead: the duplicates keep, and so does the account.
func TestAnUpgradeStopsRatherThanGuessAtCredentialsItCannotRead(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "wac.db")
	address := "sqlite:" + path

	old, err := store.Open(t.Context(), address, zerolog.Nop())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db := old.DB()
	if _, err := db.ExecContext(t.Context(), `DROP INDEX IF EXISTS wac_session_device_one_per_account`); err != nil {
		t.Fatalf("drop the new index: %v", err)
	}
	if _, err := db.ExecContext(t.Context(),
		`CREATE INDEX IF NOT EXISTS wac_session_device_account ON wac_session_device (account)`); err != nil {
		t.Fatalf("recreate the old index: %v", err)
	}

	paired, err := types.ParseJID("5511999990001:1@s.whatsapp.net")
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	device := old.Devices().NewDevice()
	device.ID = &paired
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             make([]byte, 32),
		AccountSignature:    make([]byte, 64),
		AccountSignatureKey: make([]byte, 32),
		DeviceSignature:     make([]byte, 64),
	}
	if err := old.Devices().PutDevice(t.Context(), device); err != nil {
		t.Fatalf("PutDevice: %v", err)
	}
	// The row is there and holds the credentials; what it will not do is come back out.
	if _, err := db.ExecContext(t.Context(),
		`UPDATE whatsmeow_device SET facebook_uuid = 'not-a-uuid' WHERE jid = ?`, paired.String()); err != nil {
		t.Fatalf("make the device unreadable: %v", err)
	}
	if _, err := db.ExecContext(t.Context(),
		`INSERT INTO wac_session_device (sid, jid, account, bound_at) VALUES (?, ?, ?, ?)`,
		"sid-paired", paired.String(), "5511999990001", int64(1)); err != nil {
		t.Fatalf("write the paired mapping: %v", err)
	}
	if _, err := db.ExecContext(t.Context(),
		`INSERT INTO wac_session_device (sid, jid, account, bound_at) VALUES (?, ?, ?, ?)`,
		"sid-halfway", "5511999990001:2@s.whatsapp.net", "5511999990001", int64(2)); err != nil {
		t.Fatalf("write the half-written mapping: %v", err)
	}
	if err := old.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	upgraded, err := store.Open(t.Context(), address, zerolog.Nop())
	if err == nil {
		t.Cleanup(func() { _ = upgraded.Close() })
		t.Fatal("the upgrade chose a winner while it could not tell whether the other had credentials")
	}

	// And it chose nothing on the way out: the mapping that names the device is still
	// there, so a start once the store reads again finds the account where it left it.
	check, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("reopen the file: %v", err)
	}
	t.Cleanup(func() { _ = check.Close() })
	var mappings int
	if err := check.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM wac_session_device WHERE sid = ?`, "sid-paired").Scan(&mappings); err != nil {
		t.Fatalf("count the mappings: %v", err)
	}
	if mappings != 1 {
		t.Fatalf("the paired mapping was deleted on the way out (%d rows)", mappings)
	}
}

// Bind writes the mapping before whatsmeow writes the device, so an instance that died
// mid-pairing leaves the newest mapping of all pointing at a device that never existed.
// Reading `bound_at` alone as "this is the pairing that took" then deletes the working
// device to preserve the empty row, Device unbinds the survivor for having no
// credentials, and an upgrade has made a paired account pair again.
func TestAnUpgradeKeepsTheMappingThatStillHasItsCredentials(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "wac.db")
	address := "sqlite:" + path

	old, err := store.Open(t.Context(), address, zerolog.Nop())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db := old.DB()
	if _, err := db.ExecContext(t.Context(), `DROP INDEX IF EXISTS wac_session_device_one_per_account`); err != nil {
		t.Fatalf("drop the new index: %v", err)
	}
	if _, err := db.ExecContext(t.Context(),
		`CREATE INDEX IF NOT EXISTS wac_session_device_account ON wac_session_device (account)`); err != nil {
		t.Fatalf("recreate the old index: %v", err)
	}

	// The pairing that took, and the credentials whatsmeow wrote beside it.
	paired, err := types.ParseJID("5511999990001:1@s.whatsapp.net")
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	device := old.Devices().NewDevice()
	device.ID = &paired
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             make([]byte, 32),
		AccountSignature:    make([]byte, 64),
		AccountSignatureKey: make([]byte, 32),
		DeviceSignature:     make([]byte, 64),
	}
	if err := old.Devices().PutDevice(t.Context(), device); err != nil {
		t.Fatalf("PutDevice: %v", err)
	}
	if _, err := db.ExecContext(t.Context(),
		`INSERT INTO wac_session_device (sid, jid, account, bound_at) VALUES (?, ?, ?, ?)`,
		"sid-paired", paired.String(), "5511999990001", int64(1)); err != nil {
		t.Fatalf("write the paired mapping: %v", err)
	}
	// And the one that did not: newer, and with nothing behind it.
	if _, err := db.ExecContext(t.Context(),
		`INSERT INTO wac_session_device (sid, jid, account, bound_at) VALUES (?, ?, ?, ?)`,
		"sid-halfway", "5511999990001:2@s.whatsapp.net", "5511999990001", int64(2)); err != nil {
		t.Fatalf("write the half-written mapping: %v", err)
	}
	if err := old.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	upgraded, err := store.Open(t.Context(), address, zerolog.Nop())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = upgraded.Close() })

	if _, bound, err := upgraded.For("sid-halfway").JID(t.Context()); err != nil || bound {
		t.Fatalf("the mapping with no device behind it survived (bound=%v, err=%v)", bound, err)
	}
	if jid, bound, err := upgraded.For("sid-paired").JID(t.Context()); err != nil || !bound || jid != paired {
		t.Fatalf("the paired mapping is gone (jid=%v, bound=%v, err=%v)", jid, bound, err)
	}

	// And its credentials with it, which is what makes the session resumable rather than
	// a mapping Device unbinds on the next connect.
	if stored, err := upgraded.Devices().GetDevice(t.Context(), paired); err != nil {
		t.Fatalf("GetDevice: %v", err)
	} else if stored == nil {
		t.Fatal("the upgrade deleted the only device the account had")
	}
	resumed, err := upgraded.For("sid-paired").Device(t.Context())
	if err != nil {
		t.Fatalf("Device: %v", err)
	}
	if resumed.ID == nil || *resumed.ID != paired {
		t.Fatalf("the session came back unpaired (%v), want %v", resumed.ID, paired)
	}
}

// would keep the permissive index, never gain the constraint, and go on letting two
// sessions hold one account while every test on a fresh database passed.
func TestAnOlderStoreGainsTheAccountConstraint(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "wac.db")
	address := "sqlite:" + path

	// The shape the first version left behind: a permissive index, and two mappings for
	// one account that it allowed through.
	old, err := store.Open(t.Context(), address, zerolog.Nop())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db := old.DB()
	if _, err := db.ExecContext(t.Context(), `DROP INDEX IF EXISTS wac_session_device_one_per_account`); err != nil {
		t.Fatalf("drop the new index: %v", err)
	}
	if _, err := db.ExecContext(t.Context(),
		`CREATE INDEX IF NOT EXISTS wac_session_device_account ON wac_session_device (account)`); err != nil {
		t.Fatalf("recreate the old index: %v", err)
	}
	var stale types.JID
	for i, sid := range []string{"sid-old", "sid-newer"} {
		raw := fmt.Sprintf("5511999990001:%d@s.whatsapp.net", i+1)
		jid, err := types.ParseJID(raw)
		if err != nil {
			t.Fatalf("ParseJID: %v", err)
		}
		if sid == "sid-old" {
			stale = jid
		}
		// With the credentials whatsmeow keeps beside it, so the upgrade has something
		// to leave behind if it only deletes the mapping.
		device := old.Devices().NewDevice()
		device.ID = &jid
		device.Account = &waAdv.ADVSignedDeviceIdentity{
			Details:             make([]byte, 32),
			AccountSignature:    make([]byte, 64),
			AccountSignatureKey: make([]byte, 32),
			DeviceSignature:     make([]byte, 64),
		}
		if err := old.Devices().PutDevice(t.Context(), device); err != nil {
			t.Fatalf("PutDevice: %v", err)
		}
		if _, err := db.ExecContext(t.Context(),
			`INSERT INTO wac_session_device (sid, jid, account, bound_at) VALUES (?, ?, ?, ?)`,
			sid, raw, "5511999990001", int64(i+1)); err != nil {
			t.Fatalf("write the duplicate mapping: %v", err)
		}
	}
	if err := old.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Opened again by this version, which is what an upgrade is.
	upgraded, err := store.Open(t.Context(), address, zerolog.Nop())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = upgraded.Close() })

	if _, bound, err := upgraded.For("sid-old").JID(t.Context()); err != nil || bound {
		t.Fatalf("the older of two mappings survived (bound=%v, err=%v)", bound, err)
	}
	if _, bound, err := upgraded.For("sid-newer").JID(t.Context()); err != nil || !bound {
		t.Fatalf("the mapping that should have been kept is gone (bound=%v, err=%v)", bound, err)
	}

	// The device the dropped mapping named goes with it. Nothing can reach it once its
	// mapping is gone, so leaving it is leaving authentication material on disk that
	// nothing will ever use or clean up.
	if stored, err := upgraded.Devices().GetDevice(t.Context(), stale); err != nil {
		t.Fatalf("GetDevice: %v", err)
	} else if stored != nil {
		t.Fatal("the device of the dropped mapping survived, unreachable and unclaimed")
	}

	// And the constraint is really there now, not merely named.
	_, err = upgraded.DB().ExecContext(t.Context(),
		`INSERT INTO wac_session_device (sid, jid, account, bound_at) VALUES (?, ?, ?, ?)`,
		"sid-third", "5511999990001:9@s.whatsapp.net", "5511999990001", 99)
	if err == nil {
		t.Fatal("a second mapping for one account was accepted after the upgrade")
	}
}

// The save that ends a pairing is where whatsmeow installs the stores a fresh device does
// not have yet, and it installs them over whatever was there. A fence that did not put
// itself back would come off exactly once per account, at pairing, and stay off -- on the
// sessions where nothing would look wrong.
func TestPairingDoesNotTakeTheFenceOff(t *testing.T) {
	t.Parallel()
	container := open(t)

	fence := &store.Fence{}
	device := store.Fenced(container.Devices().NewDevice(), fence)
	if device.Initialized {
		t.Fatal("a device that has never been saved is already initialised, so this proves nothing")
	}
	jid := types.NewJID("5511999990001", types.DefaultUserServer)
	jid.Device = 1
	device.ID = &jid
	// The half of pairing this test does not run: whatsmeow fills the account identity
	// before it saves, and refuses to store a device without one.
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             make([]byte, 32),
		AccountSignature:    make([]byte, 64),
		AccountSignatureKey: make([]byte, 32),
		DeviceSignature:     make([]byte, 64),
	}

	if err := device.Save(t.Context()); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if !device.Initialized {
		t.Fatal("the save left the device uninitialised, so whatsmeow no longer installs its stores here")
	}

	fence.Drop()
	if err := device.Identities.PutIdentity(t.Context(), "5511999990002.0", [32]byte{}); !errors.Is(err, store.ErrNotOwned) {
		t.Errorf("an identity was written after the fence came down: %v", err)
	}
	if err := device.Save(t.Context()); !errors.Is(err, store.ErrNotOwned) {
		t.Errorf("the device itself was written after the fence came down: %v", err)
	}
}
