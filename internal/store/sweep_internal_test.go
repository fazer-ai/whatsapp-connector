package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/store/storetest"
)

// The sweep deletes a batch at a time so the rest of the store is not held behind it,
// and a backlog is more than one batch by definition: an instance that comes back after
// an outage has a whole retention window to drop, and it drops it on its way in, which
// is when sessions are being adopted on a budget. A loop that stopped at the first
// statement would leave the table growing for the life of the deployment while reporting
// a sweep that worked.
//
// In-package because the boundary being crossed is mediaSweepBatch itself: pinned to a
// number instead, the test would stop crossing anything the day somebody raises it.
func TestASweepCrossesItsBatchBoundary(t *testing.T) {
	t.Parallel()

	container := openInternal(t)
	bindOne(t, container, "sid-1", "5511999990001")

	const extra = 3
	expired := time.Now().Add(-30 * 24 * time.Hour)
	for i := range mediaSweepBatch + extra {
		part := MediaPart{
			SID: "sid-1", MessageID: fmt.Sprintf("3EB0%04d", i), Kind: "image",
			DirectPath: "/v/t62.7118-24/file.enc", Mime: "image/jpeg",
		}
		if err := container.putMediaPart(t.Context(), &part, expired); err != nil {
			t.Fatalf("PutMediaPart(%d): %v", i, err)
		}
	}

	dropped, err := container.SweepMediaParts(t.Context(), time.Now().Add(-7*24*time.Hour))
	if err != nil {
		t.Fatalf("SweepMediaParts: %v", err)
	}
	if want := int64(mediaSweepBatch + extra); dropped != want {
		t.Fatalf("the sweep dropped %d rows and there were %d past their retention", dropped, want)
	}

	var left int
	if err := container.db.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM wac_media_part`).Scan(&left); err != nil {
		t.Fatalf("count what is left: %v", err)
	}
	if left != 0 {
		t.Fatalf("%d row(s) past their retention survived a sweep that reported dropping everything", left)
	}
}

// A sweep given a context that is already done deletes nothing and says so. The pass the
// loop makes on its way in runs under the connector's own context, so this is the shape
// of a shutdown landing mid-backlog.
func TestASweepGivenNoTimeDropsNothing(t *testing.T) {
	t.Parallel()

	container := openInternal(t)
	bindOne(t, container, "sid-1", "5511999990001")
	part := MediaPart{SID: "sid-1", MessageID: "3EB0OLD", Kind: "image", DirectPath: "/v/f.enc"}
	if err := container.putMediaPart(t.Context(), &part, time.Now().Add(-30*24*time.Hour)); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	stopped, cancel := context.WithCancel(t.Context())
	cancel()

	dropped, err := container.SweepMediaParts(stopped, time.Now())
	if err == nil {
		t.Fatal("a sweep with no time left reported success")
	}
	if dropped != 0 {
		t.Fatalf("a sweep with no time left reported dropping %d rows", dropped)
	}
	if _, found, err := container.mediaPart(t.Context(), "sid-1", "3EB0OLD"); err != nil || !found {
		t.Fatalf("the row went with a sweep that never ran (found=%v err=%v)", found, err)
	}
}

func openInternal(t *testing.T) *Container {
	t.Helper()

	container, err := Open(t.Context(), storetest.New(t).URL, zerolog.Nop())
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

// bindOne pairs a session, because the table refuses a row whose session is not.
func bindOne(t *testing.T, container *Container, sid, phone string) {
	t.Helper()

	jid, err := types.ParseJID(phone + ":11@s.whatsapp.net")
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
	if err := container.bind(t.Context(), sid, jid); err != nil {
		t.Fatalf("Bind: %v", err)
	}
}
