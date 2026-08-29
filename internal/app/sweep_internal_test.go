package app

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow/proto/waAdv"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/store"
	"github.com/fazer-ai/whatsapp-connector/internal/store/storetest"
)

// The cadence runs to an hour on the default retention, and every restart builds a new
// ticker. A fleet that ships several times a day would reach the first tick on none of
// its instances, and the retention an operator configured would be a setting nothing
// enforces.
func TestTheRefetchSweepRunsBeforeItsFirstTick(t *testing.T) {
	t.Parallel()

	container := openTestStore(t)
	seedExpiredPart(t, container, "sid-1", "3EB0OLD")

	passes := make(chan struct{}, 1)
	connector := &Connector{
		cfg:       Config{MediaRefetch: DefaultMediaRefetch},
		log:       zerolog.Nop(),
		store:     container,
		partSwept: passes,
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	swept := connector.sweepMediaParts(ctx)

	// Awaited rather than polled for: the first tick of this sweeper is an hour away, so
	// the only pass that can arrive here is the one the loop makes on its way in.
	select {
	case <-passes:
	case <-time.After(5 * time.Second):
		t.Fatal("the sweeper made no pass at all, and its first tick is an hour away")
	}
	_, found, err := container.For("sid-1").MediaPart(t.Context(), "3EB0OLD")
	if err != nil {
		t.Fatalf("MediaPart: %v", err)
	}
	if found {
		t.Fatal("the pass the loop makes on its way in left a row past its retention behind")
	}

	cancel()
	select {
	case <-swept:
	case <-time.After(5 * time.Second):
		t.Fatal("the sweeper never came back")
	}
}

// And an instance with no store of its own has nothing to sweep, which is the fake
// engine and every test that runs one.
func TestTheRefetchSweepStopsAtOnceWithNoStore(t *testing.T) {
	t.Parallel()

	connector := &Connector{cfg: Config{MediaRefetch: DefaultMediaRefetch}, log: zerolog.Nop()}
	select {
	case <-connector.sweepMediaParts(t.Context()):
	case <-time.After(5 * time.Second):
		t.Fatal("a sweeper with nothing to sweep did not come back")
	}
}

func openTestStore(t *testing.T) *store.Container {
	t.Helper()

	container, err := store.Open(t.Context(), storetest.New(t).URL, zerolog.Nop())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return container
}

// seedExpiredPart writes a part old enough that any retention drops it, for a session
// that is paired: the table refuses a row whose session is not.
func seedExpiredPart(t *testing.T, container *store.Container, sid, messageID string) {
	t.Helper()

	jid, err := waTypes.ParseJID("5511999990001:12@" + waTypes.DefaultUserServer)
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	device := container.Devices().NewDevice()
	device.ID = &jid
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details: make([]byte, 32), AccountSignature: make([]byte, 64),
		AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64),
	}
	if err := container.Devices().PutDevice(t.Context(), device); err != nil {
		t.Fatalf("PutDevice: %v", err)
	}
	if err := container.For(sid).Bind(t.Context(), jid); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	part := store.MediaPart{
		SID: sid, MessageID: messageID, Kind: "image",
		DirectPath: "/v/t62.7118-24/file.enc", Mime: "image/jpeg",
	}
	if err := container.For(part.SID).PutMediaPart(t.Context(), &part, time.Now().Add(-365*24*time.Hour)); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
}
