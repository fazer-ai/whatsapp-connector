package protocol_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The contract's normative prose has to live where a vendoring client receives it.
//
// `contract/README.md` is NOT vendored: the Chatwoot side lists it in `UNVENDORED` and its
// sync deletes it outright, deliberately, because that file describes the directory to
// somebody reading it in this repository and counting it would report drift forever. The
// side effect is that anything written there is invisible to the client the obligation is
// addressed to, and #222 is the worked example: a client obligation went in on line 59 with
// the suite and CI green, because nothing reads that file.
//
// So this is the thing that reads it. A sentence in the unvendored half that tells the
// consuming side what it must do fails here, and belongs in `contract/PROTOCOL.md`, which
// travels and is checksummed.
//
// What it can and cannot see, said out loud because a fence nobody trusts gets deleted.
//
// The variable is an obligation modal whose subject is the consuming side, and the two
// narrowings are what make it the least bad one available. Both were chosen against the
// prose as it stands, not in the abstract:
//
//   - Only obligation modals. `may not` and `cannot` are out: in this file they say "might
//     not happen" and "is unable to" far more often than they forbid anything, and they
//     accounted for two of the five sentences a looser version flagged, both descriptive.
//   - No comma between the subject and the modal. A comma opens a new clause, and the
//     obligation in a new clause belongs to that clause's subject: "the same thing to a
//     client, so a field that has to distinguish" is a rule about a field. That was the
//     third false positive.
//
// It does not parse English, so an obligation phrased without either token slips through.
// That residual is bounded by keeping README.md short and orientation-only; it is not
// bounded by this regexp, and pretending otherwise would be the worse error.
var clientObligation = regexp.MustCompile(
	`(?i)\b(?:a |the |every |any )?(?:client|clients|consumer|consumers)\b[^.!?,]{0,60}?\b` +
		`(?:must|must not|has to|have to|is required to|are required to|shall)\b`,
)

var sentenceEnd = regexp.MustCompile(`[.!?]\s`)

func TestTheUnvendoredHalfCarriesNoClientObligation(t *testing.T) {
	t.Parallel()
	path := filepath.Join("..", "..", "contract", "README.md")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the contract README: %v", err)
	}
	var offenders []string
	for _, sentence := range sentenceEnd.Split(string(source), -1) {
		flat := strings.Join(strings.Fields(sentence), " ")
		if flat == "" || !clientObligation.MatchString(flat) {
			continue
		}
		offenders = append(offenders, flat)
	}
	if len(offenders) == 0 {
		return
	}
	t.Fatalf("contract/README.md is not vendored, and the Chatwoot sync deletes it, so a client "+
		"reading the contract never sees these %d sentence(s). Move them to contract/PROTOCOL.md, "+
		"which travels and is counted in the client's checksum:\n\n  - %s",
		len(offenders), strings.Join(offenders, "\n  - "))
}
