package whatsmeow

import (
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
)

// The value is a count of failed pings dressed as a duration, and this is the arithmetic
// that turns one into the other. Written against whatsmeow's own constants rather than
// against 40s and 60s: the library sets the ping interval and the response deadline, and
// a release that changes either moves the window this has to sit in.
func TestTheSocketIsGivenUpOnTheSecondFailedPingAndNotTheFirst(t *testing.T) {
	// The latest the first failed ping can be seen: a full interval, then the whole
	// deadline spent waiting for an answer that does not come.
	first := wm.KeepAliveIntervalMax + wm.KeepAliveResponseDeadline
	// The earliest the second can, which is the same wait twice at its shortest.
	second := 2 * (wm.KeepAliveIntervalMin + wm.KeepAliveResponseDeadline)

	if wm.KeepAliveMaxFailTime <= first {
		t.Fatalf("a socket is given up after %s, which one failed ping can reach by %s, "+
			"so a single lost ping takes a healthy session down",
			wm.KeepAliveMaxFailTime, first)
	}
	if wm.KeepAliveMaxFailTime >= second {
		t.Fatalf("a socket is given up after %s, which the second failed ping reaches by %s, "+
			"so the account waits for a third and spends half again as long behind a dead transport",
			wm.KeepAliveMaxFailTime, second)
	}
}

// And the init is what puts it there. Asserted apart from the arithmetic above so a
// removed init is not reported as a badly chosen number.
func TestTheKeepAliveSettingIsInstalled(t *testing.T) {
	if wm.KeepAliveMaxFailTime != keepAliveGiveUp {
		t.Fatalf("whatsmeow gives up after %s, and this package means to set %s",
			wm.KeepAliveMaxFailTime, keepAliveGiveUp)
	}
}

// The default this replaces, named so the test says what changed rather than only that
// something did. Three minutes is what an account spends behind a quiet socket without
// the init above.
func TestTheLibraryDefaultIsTheOneThisReplaces(t *testing.T) {
	if was := 3 * time.Minute; keepAliveGiveUp >= was {
		t.Fatalf("this package sets %s, which is no sooner than the %s it exists to shorten",
			keepAliveGiveUp, was)
	}
}
