package store_test

import (
	"context"
	"path/filepath"
	"testing"

	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

func TestOpenRejectsAnUnknownURL(t *testing.T) {
	t.Parallel()

	for _, url := range []string{"", "mysql://localhost/wa", "/var/lib/wa.db"} {
		if _, err := store.Open(t.Context(), url); err == nil {
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

	address := "file:" + filepath.Join(t.TempDir(), "wac.db") + "?_pragma=foreign_keys(1)"
	container, err := store.Open(t.Context(), address)
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

	jid, err := types.ParseJID(phone + "@s.whatsapp.net")
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
