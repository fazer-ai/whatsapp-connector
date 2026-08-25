package store_test

import (
	"context"
	"fmt"
	"path/filepath"
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
