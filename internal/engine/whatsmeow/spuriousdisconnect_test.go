package whatsmeow_test

import (
	"context"
	"testing"
	"testing/synctest"

	wm "go.mau.fi/whatsmeow"
	waSocket "go.mau.fi/whatsmeow/socket"
	waStore "go.mau.fi/whatsmeow/store"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
)

// The half of #207 that can be exercised rather than read.
//
// The race lets `FrameSocket.Close` read a stale, non-nil `fs.OnDisconnect` and call it, so
// `Client.onDisconnect` runs for a socket nobody is asking about. Whether that is harmless is
// decided by `cli.socket == ns`, and this test is that decision under load-bearing conditions
// rather than a reading of the source: build a NoiseSocket the client has never heard of, hand
// it to `onDisconnect` the way the race would, and require that nothing is dispatched.
//
// `remote` is true because that is what the race produces: the reader is the `go fs.Close(0)`
// the read pump defers, `Close` passes `code == 0` to the callback, and `code == 0` is the
// argument. Calling it with false would be testing a case the race cannot create.
//
// The client is deliberately NOT disconnected first, and that is the whole design of the test.
// Measured on the pinned module with `cli.socket == ns` forced true: with a preceding
// `Disconnect()` the dispatch stays at zero anyway, because `isExpectedDisconnect()` catches
// it, so a test shaped like production would pass with the guard deleted. The variant that can
// tell the difference is this one -- a live client that asked for nothing.
//
// Deterministic through synctest rather than through a wait: the two goroutines the unguarded
// path would start (`go cli.dispatchEvent` and `go cli.autoReconnect`) are started by this
// call, so they are inside the bubble and `synctest.Wait()` covers them. Three things keep the
// bubble honest. Nothing here touches `database/sql` -- the device is assembled in memory over
// `NoopStore`, because a bubble around anything that opens a pool aborts the whole package
// under PostgreSQL, which is a pass `make test` never runs. `AutoReconnectErrors` is set high
// enough that an autoreconnect, if one started, parks on a long fake-time timer and counts as
// durably blocked instead of blocking on a real dial the bubble cannot see. And the context is
// cancelled on the way out so the frame consumer and any parked goroutine finish before the
// bubble closes.
//
// What this does NOT cover: `!cli.isExpectedDisconnect()`, the second guard, whose own false
// branch needs a drop that SHOULD dispatch. A test device is refused by the server on a path
// that arms `expectDisconnect` itself, so producing one means driving the transport. That half
// stays a reading, and #207 says so.
func TestASpuriousDisconnectCallbackDispatchesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		jid := waTypes.JID{User: "5511999990207", Device: 12, Server: waTypes.DefaultUserServer}
		device := &waStore.Device{ID: &jid}
		device.SetAllStores(&waStore.NoopStore{})

		client := wm.NewClient(device, nil)
		var dispatched int
		client.AddEventHandler(func(event any) {
			if _, ok := event.(*waEvents.Disconnected); ok {
				dispatched++
			}
		})
		// High enough that autoReconnect's backoff is minutes of fake time: it parks on a
		// timer, which the bubble sees as durably blocked, instead of reaching a real dial.
		client.AutoReconnectErrors = 7

		handshake := waSocket.NewNoiseHandshake()
		handshake.Start(waSocket.NoiseStartPattern, waSocket.WAConnHeader)
		orphan, err := handshake.Finish(
			ctx,
			waSocket.NewFrameSocket(nil, nil),
			func(context.Context, []byte) {},
			//nolint:staticcheck // SA1019: "dangerous" is a warning about production callers, and
			// reaching Client.onDisconnect is the point here -- it is the code under test, it is
			// unexported, and this is the only door to it. The same door whatsmeow itself uses in
			// the handshake, which is why passing it here reproduces the real wiring.
			client.DangerousInternals().OnDisconnect,
		)
		if err != nil {
			t.Fatalf("build a socket this client never adopted: %v", err)
		}

		//nolint:staticcheck // SA1019: as above -- the deprecation warns production code off an
		// internal path, and exercising that path is what this test exists for.
		client.DangerousInternals().OnDisconnect(ctx, orphan, true)
		synctest.Wait()

		if dispatched != 0 {
			t.Errorf("a callback about a socket this client never adopted dispatched %d "+
				"Disconnected event(s), want 0.\n"+
				"That is the spurious callback of #207 stopping being inert: the connector "+
				"publishes session.state reconnecting and whatsmeow redials an account whose "+
				"lease may already be gone, which is operational invariant 1 going false.",
				dispatched)
		}
	})
}
