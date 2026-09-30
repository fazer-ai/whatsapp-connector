package store_test

import (
	"reflect"
	"testing"

	"go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

func sampleDump(device types.JID, messageID string, learnedAt int64) store.PendingHistory {
	return store.PendingHistory{
		MessageID: messageID, Device: device.String(),
		Notice: []byte{0x08, 0x01, 0x12, 0x03, 'a', 'b', 'c'}, LearnedAt: learnedAt,
	}
}

// The notice is what the download is made from, so it has to come back byte for byte.
func TestAPendingDumpComesBackExactly(t *testing.T) {
	t.Parallel()
	container := open(t)
	device := pair(t, container, "sid-1", "5511999990001")

	held := sampleDump(device, "NOTIF1", 1755000000000)
	if err := container.For("sid-1").PutPendingHistory(t.Context(), &held); err != nil {
		t.Fatalf("PutPendingHistory: %v", err)
	}
	back, err := container.For("sid-1").PendingHistory(t.Context())
	if err != nil {
		t.Fatalf("PendingHistory: %v", err)
	}
	want := held
	want.SID = "sid-1"
	if len(back) != 1 || !reflect.DeepEqual(back[0], want) {
		t.Fatalf("what came back is %+v, and what went in was %+v", back, want)
	}
}

// The same notification twice is the same dump, and the first arrival is where it belongs.
func TestHoldingTheSameDumpTwiceKeepsTheFirst(t *testing.T) {
	t.Parallel()
	container := open(t)
	device := pair(t, container, "sid-1", "5511999990001")

	for _, held := range []store.PendingHistory{sampleDump(device, "NOTIF2", 1755000000000), sampleDump(device, "NOTIF2", 1755000009000)} {
		if err := container.For("sid-1").PutPendingHistory(t.Context(), &held); err != nil {
			t.Fatalf("PutPendingHistory: %v", err)
		}
	}
	back, err := container.For("sid-1").PendingHistory(t.Context())
	if err != nil {
		t.Fatalf("PendingHistory: %v", err)
	}
	if len(back) != 1 || back[0].LearnedAt != 1755000000000 {
		t.Fatalf("one dump is pending as %+v", back)
	}
}

// In the order the phone sent them, which is what the next owner works through.
func TestPendingDumpsComeBackInTheOrderTheyArrived(t *testing.T) {
	t.Parallel()
	container := open(t)
	device := pair(t, container, "sid-1", "5511999990001")

	for _, held := range []store.PendingHistory{sampleDump(device, "NOTIFLATER", 1755000020000), sampleDump(device, "NOTIFSOONER", 1755000000000)} {
		if err := container.For("sid-1").PutPendingHistory(t.Context(), &held); err != nil {
			t.Fatalf("PutPendingHistory: %v", err)
		}
	}
	back, err := container.For("sid-1").PendingHistory(t.Context())
	if err != nil {
		t.Fatalf("PendingHistory: %v", err)
	}
	if len(back) != 2 || back[0].MessageID != "NOTIFSOONER" || back[1].MessageID != "NOTIFLATER" {
		t.Fatalf("the dumps came back as %+v", back)
	}
}

// One account's history is not another's.
func TestAPendingDumpOfOneSessionIsNotReadByAnother(t *testing.T) {
	t.Parallel()
	container := open(t)
	device := pair(t, container, "sid-1", "5511999990001")
	pair(t, container, "sid-2", "5511999990002")

	held := sampleDump(device, "NOTIF3", 1755000000000)
	if err := container.For("sid-1").PutPendingHistory(t.Context(), &held); err != nil {
		t.Fatalf("PutPendingHistory: %v", err)
	}
	if back, err := container.For("sid-2").PendingHistory(t.Context()); err != nil || len(back) != 0 {
		t.Fatalf("the other session has %d of these (err=%v)", len(back), err)
	}
}

// Both writes are the owner's alone: the row is read by whoever owns the session next.
func TestAPendingDumpIsWrittenAndDroppedOnlyByTheOwner(t *testing.T) {
	t.Parallel()
	container := open(t)
	device := pair(t, container, "sid-1", "5511999990001")

	owner := container.For("sid-1")
	held := sampleDump(device, "NOTIF4", 1755000000000)
	if err := owner.PutPendingHistory(t.Context(), &held); err != nil {
		t.Fatalf("PutPendingHistory: %v", err)
	}
	owner.Drop()

	stale := sampleDump(device, "NOTIF5", 1755000001000)
	if err := owner.PutPendingHistory(t.Context(), &stale); err == nil {
		t.Error("a session that no longer owns this one wrote a dump for its successor")
	}
	if err := owner.DropPendingHistory(t.Context(), "NOTIF4"); err == nil {
		t.Error("a session that no longer owns this one took its successor's dump away")
	}
	back, _ := container.For("sid-1").PendingHistory(t.Context())
	if len(back) != 1 || back[0].MessageID != "NOTIF4" {
		t.Fatalf("what the next owner finds is %+v", back)
	}
}

// Finished with, a dump goes, and the rest stay.
func TestDroppingAPendingDumpLeavesTheRest(t *testing.T) {
	t.Parallel()
	container := open(t)
	device := pair(t, container, "sid-1", "5511999990001")

	for _, held := range []store.PendingHistory{sampleDump(device, "NOTIFDONE", 1755000000000), sampleDump(device, "NOTIFLEFT", 1755000001000)} {
		if err := container.For("sid-1").PutPendingHistory(t.Context(), &held); err != nil {
			t.Fatalf("PutPendingHistory: %v", err)
		}
	}
	if err := container.For("sid-1").DropPendingHistory(t.Context(), "NOTIFDONE"); err != nil {
		t.Fatalf("DropPendingHistory: %v", err)
	}
	back, _ := container.For("sid-1").PendingHistory(t.Context())
	if len(back) != 1 || back[0].MessageID != "NOTIFLEFT" {
		t.Fatalf("what is left is %+v", back)
	}
}

// A logged-out account's history is not published by whoever picks the session up next.
func TestForgettingASessionForgetsItsPendingDumps(t *testing.T) {
	t.Parallel()
	container := open(t)
	device := pair(t, container, "sid-1", "5511999990001")

	held := sampleDump(device, "NOTIF6", 1755000000000)
	if err := container.For("sid-1").PutPendingHistory(t.Context(), &held); err != nil {
		t.Fatalf("PutPendingHistory: %v", err)
	}
	if err := container.For("sid-1").ForgetCredentialsAndDesired(t.Context()); err != nil {
		t.Fatalf("ForgetCredentialsAndDesired: %v", err)
	}
	if back, _ := container.For("sid-1").PendingHistory(t.Context()); len(back) != 0 {
		t.Fatalf("%d dump(s) outlived the account they belong to", len(back))
	}
}

// A dump is written only for the device the session is bound to, in the same statement: a
// logout and a new pairing between the arrival and the write would otherwise file the old
// account's dump under the new one.
func TestADumpForADeviceTheSessionIsNotBoundToIsNotWritten(t *testing.T) {
	t.Parallel()
	container := open(t)
	old := pair(t, container, "sid-1", "5511999990001")
	pair(t, container, "sid-1", "5511999990009")

	held := sampleDump(old, "NOTIF7", 1755000000000)
	if err := container.For("sid-1").PutPendingHistory(t.Context(), &held); err != nil {
		t.Fatalf("PutPendingHistory: %v", err)
	}
	if back, _ := container.For("sid-1").PendingHistory(t.Context()); len(back) != 0 {
		t.Fatalf("the previous device's dump was written for the new one: %+v", back)
	}
}

// A new pairing takes with it what the previous device left: those dumps are the previous
// account's, and the bond is updated in place, so no cascade clears them.
func TestPairingAgainForgetsThePreviousDevicesDumps(t *testing.T) {
	t.Parallel()
	container := open(t)
	device := pair(t, container, "sid-1", "5511999990001")

	held := sampleDump(device, "NOTIF8", 1755000000000)
	if err := container.For("sid-1").PutPendingHistory(t.Context(), &held); err != nil {
		t.Fatalf("PutPendingHistory: %v", err)
	}
	next := pair(t, container, "sid-1", "5511999990009")
	if back, _ := container.For("sid-1").PendingHistory(t.Context()); len(back) != 0 {
		t.Fatalf("the new pairing (%s) inherited the previous device's dumps: %+v", next, back)
	}
}
