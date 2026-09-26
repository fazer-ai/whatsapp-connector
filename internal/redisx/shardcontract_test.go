package redisx_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// The hash the contract spells out for clients is the one ShardOf computes (#286).
//
// A client locating a session's stream computes it on its own side, from the prose, and
// the two ends only agree if the prose and this code name the same function. The vectors
// in PROTOCOL.md were computed outside this repository from the description written above
// them, so this compares the code against an independent reading of the words: a change of
// hash, of width or of modulus here turns it red, and so does a vector edited by hand.
func TestTheShardVectorsInTheContractAreWhatShardOfComputes(t *testing.T) {
	t.Parallel()

	prose, err := os.ReadFile(filepath.Join("..", "..", "contract", "PROTOCOL.md"))
	if err != nil {
		t.Fatalf("read the contract's prose: %v", err)
	}
	row := regexp.MustCompile("(?m)^\\| (`[^`]*`|\\(empty string\\)) \\| `0x([0-9a-f]{8})` \\| (\\d+) \\| (\\d+) \\|$")
	rows := row.FindAllStringSubmatch(string(prose), -1)
	if len(rows) < 5 {
		t.Fatalf("contract/PROTOCOL.md has %d shard vectors, want the table a client checks its hash against", len(rows))
	}

	eight, sixteen := redisx.NewKeys("wa:", 8), redisx.NewKeys("wa:", 16)
	// A prime count reads the hash almost whole. The two powers of two only see its low four
	// bits, and those are the same for FNV-1a at 32 bits and at 64 bits truncated to 32: the
	// offset bases and the primes end in the same nibbles, so a table of those alone passes
	// the wrong function.
	const prime = 2147483647
	wide := redisx.NewKeys("wa:", prime)
	var disagree, upper bool
	for _, vector := range rows {
		sid := strings.Trim(vector[1], "`")
		if vector[1] == "(empty string)" {
			sid = ""
		}
		hash, _ := strconv.ParseUint(vector[2], 16, 32)
		of8, _ := strconv.Atoi(vector[3])
		of16, _ := strconv.Atoi(vector[4])
		if uint64(of8) != hash%8 || uint64(of16) != hash%16 {
			t.Errorf("the contract's row for %q does not follow from its own hash 0x%08x", sid, hash)
		}
		if got := wide.ShardOf(sid); uint64(got) != hash%prime {
			t.Errorf("the contract hashes %q to 0x%08x, and ShardOf disagrees at %d streams: %d, want %d",
				sid, hash, prime, got, hash%prime)
		}
		if got := eight.ShardOf(sid); got != of8 {
			t.Errorf("the contract puts %q on shard %d of 8, and ShardOf on %d", sid, of8, got)
		}
		if got := sixteen.ShardOf(sid); got != of16 {
			t.Errorf("the contract puts %q on shard %d of 16, and ShardOf on %d", sid, of16, got)
		}
		disagree = disagree || of8 != of16
		upper = upper || of16 >= 8
	}
	// Vectors that agree at every count prove nothing about the modulus, which is the half
	// a client guessing a count gets wrong.
	if !disagree || !upper {
		t.Error("no vector lands on a different shard at 8 and at 16, or none lands on shard 8 or above at 16: " +
			"the table no longer shows what a wrong count costs")
	}
}
