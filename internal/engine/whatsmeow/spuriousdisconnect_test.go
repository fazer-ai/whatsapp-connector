package whatsmeow_test

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"

	wm "go.mau.fi/whatsmeow"
	waSocket "go.mau.fi/whatsmeow/socket"
	waStore "go.mau.fi/whatsmeow/store"
	waTypes "go.mau.fi/whatsmeow/types"
)

// The half of #207 that can be exercised rather than read.
//
// The race lets `FrameSocket.Close` read a stale, non-nil `fs.OnDisconnect` and call it, so
// `Client.onDisconnect` runs for a socket nobody is asking about. Whether that is harmless is
// decided by `cli.socket == ns`, and this test is that decision under load-bearing conditions
// rather than a reading of the source: build a NoiseSocket the client has never heard of, hand
// it to `onDisconnect` with the arguments the race produces, on a socket this client never
// adopted, and require that nothing is dispatched.
//
// That distinction is not pedantry. In production the race hands over the socket the client DID
// adopt, and the branch is false because `cli.socket` has already been cleared. Here the branch
// is false because the socket was never there to begin with. Same branch, different reason, and
// the reason production relies on is fenced next door rather than here.
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
// Two things this does NOT cover, and the second is the subtler one.
//
// It does not cover `!cli.isExpectedDisconnect()`, the second guard, whose own false branch
// needs a drop that SHOULD dispatch. A test device is refused by the server on a path that
// arms `expectDisconnect` itself, so producing one means driving the transport. The fence
// next door covers that guard's existence; its behaviour stays a reading, and #207 says so.
//
// And it is sensitive to the PRESENCE of the first guard, not to the reason that guard holds
// in production. Here the branch is structural: no NoiseSocket built from outside can ever be
// `cli.socket`, because the only assignment is in handshake.go from a value nothing exported
// hands back. In production the same branch is taken for a different reason -- `Disconnect`
// clears `cli.socket` inside the `cli.socketLock` critical section, and the spurious callback
// is parked on that same Lock until after it. Move that `nil` out of the critical section and
// production starts taking the other branch while this test stays green. That ordering is
// fenced in upstream_guards_test.go rather than here, because this test cannot build the
// state that would distinguish it.
func TestASpuriousDisconnectOnASocketTheClientNoLongerHoldsDispatchesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		jid := waTypes.JID{User: "5511999990207", Device: 12, Server: waTypes.DefaultUserServer}
		device := &waStore.Device{ID: &jid}
		device.SetAllStores(&waStore.NoopStore{})

		client := wm.NewClient(device, nil)
		// Every event, not just Disconnected: the claim is that this callback produces
		// nothing, and a counter for one type would let the name outrun the assertion.
		var dispatched []string
		client.AddEventHandler(func(event any) {
			dispatched = append(dispatched, fmt.Sprintf("%T", event))
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

		if len(dispatched) != 0 {
			t.Errorf("a callback about a socket this client never adopted dispatched %d "+
				"event(s), %v, want none.\n"+
				"That is the spurious callback of #207 stopping being inert: the connector "+
				"publishes session.state reconnecting and whatsmeow redials an account whose "+
				"lease may already be gone, which is operational invariant 1 going false.",
				len(dispatched), dispatched)
		}
	})
}
