package protocol_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The contract tells a client where the number of event streams comes from, and what to
// do with it (#286). It used to publish `event_shards` in the key table, as something
// connectors agree on, and say nothing to the side that consumes the streams: a client
// that assumed a count followed the contract as written and lost the events of every
// session on the streams it did not read.
//
// Read from the two paragraphs that carry it and not from the file, and each clause was
// deleted from them to see red: `wa:meta` and `event_shards` are also in the key table,
// so a file-wide search stays green on a contract that lost the obligation.
func TestTheContractTellsAClientToReadTheShardCount(t *testing.T) {
	t.Parallel()

	prose, err := os.ReadFile(filepath.Join("..", "..", "contract", "PROTOCOL.md"))
	if err != nil {
		t.Fatalf("read the contract's prose: %v", err)
	}
	blocks := strings.Split(string(prose), "\n\n")
	var obligation string
	for i, block := range blocks {
		if strings.HasPrefix(block, "**A client must read the number of event streams from `wa:meta`") && i+1 < len(blocks) {
			obligation = strings.Join(strings.Fields(block+" "+blocks[i+1]), " ")
		}
	}
	if obligation == "" {
		t.Fatal("contract/PROTOCOL.md does not tell a client to read the number of event streams from `wa:meta`")
	}
	// In the register the fence on the unvendored half reads. Written as a description ("a
	// client reads"), the same paragraphs pasted into contract/README.md passed that fence,
	// so the obligation could move to the half the client never receives with the suite
	// green, which is #222 again.
	if !clientObligation.MatchString(obligation) {
		t.Error("the shard count paragraphs state the obligation in a form the contract/README.md fence does not see; " +
			"say what a client must do, not what it does")
	}

	for _, clause := range []struct{ says, why string }{
		{"The count is the `event_shards` field of the `wa:meta` hash", "where the number comes from"},
		{"and consume every one of them", "that a client reads all of them"},
		{"`wa:events:0` up to `wa:events:<event_shards - 1>`", "which streams exist"},
		{"`fnv1a32(sid) % event_shards`", "the mapping, for a client that locates one session's stream"},
		{"the 32-bit FNV-1a hash of the sid's UTF-8 bytes", "which hash, in words a client can implement"},
		{"A client that reads every stream does not need the hash", "that the hash is not what decides which streams to read"},
		{"one that wants to locate a single session's stream does", "who does need it"},
		{"A client must read it when it starts, and can keep it", "when to read it"},
		{"never reads fewer streams than that", "what a client that starts first may do meanwhile, and the one thing it may not"},
		{"only changes with the whole fleet stopped", "why keeping it is safe"},
		{"not to follow it on the fly", "what a count that changed under a running client calls for"},
	} {
		if !strings.Contains(obligation, clause.says) {
			t.Errorf("the shard count paragraphs no longer say %q: %s", clause.says, clause.why)
		}
	}
}
