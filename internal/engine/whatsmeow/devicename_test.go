package whatsmeow

import (
	"testing"

	"github.com/rs/zerolog"
	waStore "go.mau.fi/whatsmeow/store"
	"google.golang.org/protobuf/proto"
)

// The device name is a package-level value in whatsmeow, and its pairing handshake reads
// it while marshalling the registration payload. New writes it, and a sync.Once orders
// that write against another New but not against a read: nothing connects the two, so a
// pairing anywhere in the process races an engine being built anywhere else.
//
// One engine is built before any session exists in production, so the write is done long
// before there is a client to read it. A test binary builds engines throughout the run,
// some of them while another test is pairing, which is where the pair actually meets --
// on Linux under -race, not reliably on a developer's machine.
//
// Consumed here instead. init runs before any test goroutine, so the write happens-before
// everything the binary goes on to do, and New finds the Once spent and never writes
// again: one side of the racing pair stops existing rather than being scheduled around.
func init() {
	deviceNameOnce.Do(func() { waStore.DeviceProps.Os = proto.String("fazer.ai test") })
}

// And the fix is only worth as much as that claim: if New could still write, init would
// have moved the race rather than removed it.
func TestBuildingAnEngineNoLongerWritesTheDeviceName(t *testing.T) {
	t.Parallel()

	before := waStore.DeviceProps.GetOs()
	mustEngine(t, openStore(t), Options{DeviceName: "somebody else's name"}, zerolog.Nop())
	if after := waStore.DeviceProps.GetOs(); after != before {
		t.Fatalf("building an engine wrote the device name (%q -> %q), which is the write "+
			"a pairing handshake races", before, after)
	}
}
