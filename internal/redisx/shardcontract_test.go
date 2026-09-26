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
	row := regexp.MustCompile("(?m)^\\| (`[^`]*`|\\(empty string\\)) \\| (\\d+) \\| (\\d+) \\|$")
	rows := row.FindAllStringSubmatch(string(prose), -1)
	if len(rows) < 5 {
		t.Fatalf("contract/PROTOCOL.md has %d shard vectors, want the table a client checks its hash against", len(rows))
	}

	eight, sixteen := redisx.NewKeys("wa:", 8), redisx.NewKeys("wa:", 16)
	var disagree, upper bool
	for _, vector := range rows {
		sid := strings.Trim(vector[1], "`")
		if vector[1] == "(empty string)" {
			sid = ""
		}
		of8, _ := strconv.Atoi(vector[2])
		of16, _ := strconv.Atoi(vector[3])
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
