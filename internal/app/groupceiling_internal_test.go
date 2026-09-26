package app

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// contract/PROTOCOL.md tells a client how long a stranded group creation lasts, in hours,
// and a client plans its escalation on that number (#277). It is this constant's, and a
// number written in prose does not move when the constant does.
func TestTheContractNamesTheGroupCreateCeiling(t *testing.T) {
	t.Parallel()

	source, err := os.ReadFile(filepath.Join("..", "..", "contract", "PROTOCOL.md"))
	if err != nil {
		t.Fatalf("read the contract: %v", err)
	}
	var paragraph string
	for _, block := range strings.Split(string(source), "\n\n") {
		if strings.HasPrefix(block, "**`not_settled` is not a promise") {
			paragraph = strings.Join(strings.Fields(block), " ")
		}
	}
	if paragraph == "" {
		t.Fatal("contract/PROTOCOL.md has no `not_settled` paragraph to read the ceiling from")
	}
	if groupCreateCeiling%time.Hour != 0 {
		t.Fatalf("groupCreateCeiling is %v, which the contract cannot state in whole hours", groupCreateCeiling)
	}
	want := strconv.Itoa(int(groupCreateCeiling/time.Hour)) + " hours after its first delivery"
	if !strings.Contains(paragraph, want) {
		t.Fatalf("the `not_settled` paragraph does not say %q, which is what groupCreateCeiling is", want)
	}
}
