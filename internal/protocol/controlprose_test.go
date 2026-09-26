package protocol_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// What the contract says happens to a `wa:control` entry for a session another connector
// runs, by command, because the answer is not the same for all of them (#316).
//
// It used to say one thing for every entry -- given up and reclaimed later -- which is true
// of `session.delete` and not of `session.wake`: a wake for an owned account is
// acknowledged, and its owner never sees it. The `session.wake` bullet said the second
// after #314, and the two paragraphs then contradicted each other. A client building a
// retry on the general rule would be publishing wakes into a stream that retires them.
//
// Each clause is checked on its own paragraph, and each one was deleted from it to see red:
// the handback exception is also described in the key table, and "acknowledged" appears
// all over the file, so a file-wide search would stay green on a paragraph that lost them.
func TestTheContractSaysWhatBecomesOfAControlEntryForAnOwnedSession(t *testing.T) {
	t.Parallel()

	prose, err := os.ReadFile(filepath.Join("..", "..", "contract", "PROTOCOL.md"))
	if err != nil {
		t.Fatalf("read the contract's prose: %v", err)
	}
	var paragraph string
	for _, block := range strings.Split(string(prose), "\n\n") {
		if strings.HasPrefix(block, "What `wa:control` guarantees") {
			paragraph = strings.Join(strings.Fields(block), " ")
		}
	}
	if paragraph == "" {
		t.Fatal("contract/PROTOCOL.md has no paragraph saying what `wa:control` guarantees")
	}

	for _, clause := range []struct{ says, why string }{
		{"depends on the command",
			"the paragraph has to say the answer differs by command, or it reads as one rule for every entry"},
		{"A `session.delete` is given up by the one that read it and reclaimed later",
			"a teardown for an owned account waits for its owner, which is what makes it reach one"},
		{"A `session.wake` is not given up",
			"a wake for an owned account is retired where it lands, and a client must not build a retry on it"},
		{"acknowledged and never reaches that owner",
			"what happens to the wake instead, in words a client can act on"},
		{"`wa:handback:<sid>`",
			"the one window where a wake for an owned account is kept: its owner is giving it back"},
		{"the wake is left pending",
			"what the window does to the wake"},
	} {
		if !strings.Contains(paragraph, clause.says) {
			t.Errorf("the `wa:control` paragraph no longer says %q: %s", clause.says, clause.why)
		}
	}

	// The sentence #316 was about, which made the delete's behaviour everybody's.
	for _, block := range strings.Split(string(prose), "\n\n") {
		flat := strings.Join(strings.Fields(block), " ")
		if strings.Contains(flat, "so an entry naming a session another connector is running is given up") {
			t.Errorf("contract/PROTOCOL.md says again that every entry for an owned session is given up and "+
				"reclaimed later, which a wake is not:\n%s", flat)
		}
	}
}
