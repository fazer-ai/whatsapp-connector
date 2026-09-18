package whatsmeow

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The other half of #207, and the direction of the news is the opposite of upstream_test.go's.
//
// That file asserts a whatsmeow DEFECT is still there, so a failure means somebody fixed it
// upstream and the good move is to delete the entry. This file asserts a whatsmeow GUARD is
// still there, and a failure means somebody REMOVED it. That is bad news, and it is bad news
// about this connector rather than about the library.
//
// What the guards are load-bearing for: the race in #207 lets `FrameSocket.Close` read a
// stale, non-nil `fs.OnDisconnect` and call it, which lands in `Client.onDisconnect` as a
// callback nobody asked for. Today that callback is inert, and these two lines are the whole
// reason -- `cli.socket == ns` is false because `Disconnect` already cleared `cli.socket`
// under the same lock the callback waits on, and `!cli.isExpectedDisconnect()` is false
// because `Disconnect` set the flag before releasing it. Take either away and the spurious
// callback dispatches `events.Disconnected` and starts an autoreconnect, which is a socket
// coming back up after this connector deliberately closed it. That is operational invariant 1
// of this repository going false -- losing the lease disconnects the socket immediately -- and
// without this fence nothing anywhere would turn red when it did.
//
// This is a fence on upstream SOURCE, not an exercise of the guards. It does not run the
// callback and it proves nothing about behaviour; what it buys is that the reading behind
// "the consequence is a suppressed debug line" cannot quietly stop being true across a pin
// bump. The reading itself is recorded in #207.
//
// Not exercising them is a choice and not a dead end, and #207 records which half is which.
// Three of the four things an exercise would need are reachable today: a `NoiseSocket` can be
// built with no network at all (`NoiseHandshake.Finish` is exported and installs the callback),
// `cli.socket == ns` happens by itself in the tests here because they complete a real
// handshake, and the suppressing branch is observable on a real drop. What is missing is the
// positive control for the second guard -- a drop that SHOULD dispatch -- because a test
// device is refused by the server on a path that arms `expectDisconnect` itself. Getting one
// means driving the transport, which is test infrastructure and a round of its own.
//
// Scoped to the body of the enclosing func on purpose, and that is not defensive: at this pin
// `!cli.isExpectedDisconnect()` also appears at client.go:855, outside `onDisconnect`
// entirely. A fence matching the file would stay green with the guard deleted from the one
// place it protects, which is the failure a fence cannot afford.
var onDisconnectGuards = []struct {
	fragment string // the guard, as it reads inside the func
	what     string // what stops being true when it goes
}{
	{
		fragment: "if cli.socket == ns {",
		what: "a callback about a socket this client already let go would be treated as " +
			"news about the live one",
	},
	{
		fragment: "!cli.isExpectedDisconnect()",
		what: "a disconnect this connector asked for would be reported as one it did not, " +
			"which is the half that dispatches the event and starts the autoreconnect",
	},
}

func TestTheGuardsThatKeepASpuriousDisconnectHarmlessAreStillThere(t *testing.T) {
	t.Parallel()

	root := whatsmeowRoot(t)
	source, err := os.ReadFile(filepath.Join(root, "client.go"))
	if err != nil {
		t.Fatalf("read client.go of the pinned whatsmeow: %v", err)
	}
	body := funcBody(t, string(source), "func (cli *Client) onDisconnect(")

	for _, guard := range onDisconnectGuards {
		if !strings.Contains(body, guard.fragment) {
			t.Errorf("Client.onDisconnect no longer holds %q.\n"+
				"This is not a fix and it is not good news: that guard is why the spurious "+
				"callback #207 can produce is inert. Without it, %s.\n"+
				"Re-read fazer-ai/whatsapp-connector#207 before bumping the pin any further: "+
				"the limitation it documents may have stopped being harmless.",
				guard.fragment, guard.what)
		}
	}
}
