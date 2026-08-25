package store_test

import (
	"context"
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

	device, err := container.Device(t.Context(), "sid-1")
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

	device, err := container.Device(ctx, "sid-1")
	if err != nil {
		t.Fatalf("Device: %v", err)
	}
	if device.ID == nil || device.ID.User != jid.User {
		t.Fatalf("resumed %v, want the device of %s", device.ID, jid)
	}

	// A different session must not be handed the same credentials.
	other, err := container.Device(ctx, "sid-2")
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
	if err := container.Bind(ctx, "sid-new", jid); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	if _, bound, err := container.JID(ctx, "sid-old"); err != nil || bound {
		t.Fatalf("the old session is still bound (bound=%v, err=%v)", bound, err)
	}
	bound, ok, err := container.JID(ctx, "sid-new")
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
	if err := container.Bind(ctx, "sid-1", jid); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	device, err := container.Device(ctx, "sid-1")
	if err != nil {
		t.Fatalf("Device: %v", err)
	}
	if device.ID != nil {
		t.Fatalf("resumed %s from a device that is not stored", device.ID)
	}
	if _, bound, err := container.JID(ctx, "sid-1"); err != nil || bound {
		t.Fatalf("the dangling mapping survived (bound=%v, err=%v)", bound, err)
	}
}

func TestForgetRemovesTheDeviceAndTheMapping(t *testing.T) {
	t.Parallel()
	container := open(t)
	ctx := t.Context()

	jid := pair(t, container, "sid-1", "5511999990001")
	if err := container.Forget(ctx, "sid-1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	if _, bound, err := container.JID(ctx, "sid-1"); err != nil || bound {
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
	if err := container.Forget(ctx, "sid-never"); err != nil {
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
	if err := container.Bind(t.Context(), "", jid); err == nil {
		t.Fatal("bound a device to no session")
	}
	if err := container.Bind(t.Context(), "sid-1", types.EmptyJID); err == nil {
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
	if err := container.Bind(ctx, sid, jid); err != nil {
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

	device, err := container.Device(ctx, "sid-1")
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

	if _, bound, err := container.JID(ctx, "sid-old"); err != nil || bound {
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
				if err := container.Bind(ctx, fmt.Sprintf("sid-%d", writer), jid); err != nil {
					failures <- fmt.Errorf("Bind: %w", err)
					return
				}
				if _, _, err := container.JID(ctx, fmt.Sprintf("sid-%d", writer)); err != nil {
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
			if err := container.Bind(ctx, fmt.Sprintf("sid-%d", i), jid); err != nil {
				failures <- err
			}
		}()
	}
	wg.Wait()
	close(failures)

	var bound int
	for i := range pairings {
		if _, ok, err := container.JID(ctx, fmt.Sprintf("sid-%d", i)); err != nil {
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

	if _, bound, err := upgraded.JID(t.Context(), "sid-old"); err != nil || bound {
		t.Fatalf("the older of two mappings survived (bound=%v, err=%v)", bound, err)
	}
	if _, bound, err := upgraded.JID(t.Context(), "sid-newer"); err != nil || !bound {
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
