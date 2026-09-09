package app

import (
	"strings"
	"testing"
)

// The default is what an operator who changed nothing is pairing under, and it is read in
// the account's linked-devices list beside rows that all say Chrome, Edge, Firefox on
// somebody's machine. Naming the software here is the one part of the handshake that
// announces itself, so the property pinned is that it does not: not the library's own
// default, and not this product's name.
//
// Asserted as a property rather than as the literal, because the literal is the thing
// somebody will legitimately change: another browser is fine, going back to a name that
// identifies the software is not.
func TestTheDefaultDeviceNameNamesNoSoftware(t *testing.T) {
	t.Parallel()

	for _, giveaway := range []string{"whatsmeow", "fazer", "chatwoot", "connector", "bot", "api"} {
		if strings.Contains(strings.ToLower(DefaultDeviceName), giveaway) {
			t.Errorf("DefaultDeviceName = %q, which carries %q: the linked-devices list is read by people, "+
				"and every other row in it is a browser", DefaultDeviceName, giveaway)
		}
	}
	if DefaultDeviceName == "" {
		t.Error("DefaultDeviceName is empty, which leaves whatsmeow's own default in place")
	}
}
