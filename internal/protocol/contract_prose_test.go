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
// What it can and cannot see, said out loud because a fence nobody trusts gets deleted, and
// measured adversarially rather than asserted: the holdout verifier was asked to write client
// obligations this misses, and the first draft of it missed eight out of eight.
//
// What that measurement changed. The first draft could not cross a period, which sounds
// harmless and is not: this contract is made of `message.id`, `chat.presence`, `session.wake`,
// so any frame name between the subject and the modal blinded it, and putting the frame name
// there is the natural way to write the obligation. It also knew neither `should`, which
// PROTOCOL.md already uses for a client obligation, nor "the consuming side", which is how the
// README beside this test names the very subject it tells people to write about. Four of the
// eight misses were in the delivery's own register, not artificial.
//
// What it still cannot see, measured on the same corpus rather than guessed:
//
//   - A sentence with no client subject at all: "Deduplicate on `message.id`" in the
//     imperative, or "Unknown event types must be ignored" in the passive. No keyword fence
//     reaches those, and saying so is better than implying otherwise.
//   - `never` and `always`, and the reason is a prediction rather than a measurement, which
//     is worth saying because the first version of this comment claimed otherwise. Measured:
//     neither word occurs in contract/README.md at all, and adding both to the alternation
//     flags nothing there today. The false positive that argued against them lives in the
//     repository's root README ("the client never sees a JID"), which this fence never opens,
//     so that evidence was collected in the wrong condition. They stay out because the word
//     does both jobs in protocol prose -- it forbids and it describes -- and this file's one
//     substantive section is about what a client does and does not receive, which is exactly
//     where the descriptive use turns up. Including them would take the fence from five of
//     the holdout's eight misses to six. A false positive is the worse failure here, because
//     it teaches whoever edits to write less, and that comes back as a rule nobody writes
//     anywhere; whoever disagrees has the number and the reason to reverse this.
//   - More than 120 characters between the subject and its modal.
//
// So the residual is real and this comment does not pretend it away. What bounds it is that
// README.md is short and orientation-only, which is a discipline and not a mechanism.
var clientObligation = regexp.MustCompile(
	`(?i)\b(?:a |the |every |any )?(?:client|clients|consumer|consumers|consuming side)\b` +
		// Crosses a `.` inside an identifier but never a comma: a new clause has a new
		// subject, and "the same thing to a client, so a field that has to distinguish" is a
		// rule about a field. Sentence-ending periods are already gone, split off below.
		`[^!?,]{0,120}?\b` +
		`(?:must|must not|has to|have to|need to|needs to|is required to|are required to|shall|should)\b`,
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
