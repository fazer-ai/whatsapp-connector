package protocol_test

import (
	"os"
	"strings"
	"testing"
)

// #165 asked which commands should be given a ceiling. Measured, the answer was that none
// needs one: the WhatsApp library bounds every request it sends, and several paths are held
// to something tighter this connector chooses. That makes the sentence the contract used to
// carry -- a command with neither field "has no ceiling at all" -- false, and false in the
// direction that costs a client something: it reads as an invitation to set a ceiling on
// every command to avoid one hanging, when what the client actually has to do is resend
// correctly after a `timeout`.
//
// So the paragraph that replaced it carries an obligation, and this reads the paragraph
// rather than the file. The difference is not pedantry: `timeout`, `message_id` and
// `idempotency_key` all appear elsewhere in `PROTOCOL.md`, so a whole-file search would go
// green on a paragraph that had been cut down to its first sentence.
func TestTheContractSaysWhatBoundsACommandWithNoCeilingOfItsOwn(t *testing.T) {
	t.Parallel()

	body, err := os.ReadFile("../../contract/PROTOCOL.md")
	if err != nil {
		t.Fatalf("read PROTOCOL.md: %v", err)
	}
	prose := string(body)

	// The sentence this replaced, kept as an assertion of its own: it was true when it was
	// written and the measurement in #165 made it false, and nothing else would notice it
	// coming back.
	if strings.Contains(prose, "has no ceiling at all") {
		t.Error("PROTOCOL.md still says a command with neither field has no ceiling at all, " +
			"which #165 measured to be false: the library bounds every request it sends")
	}

	paragraphs := relevantParagraphs(prose, "neither field")
	if len(paragraphs) == 0 {
		t.Fatal("no paragraph in PROTOCOL.md talks about a command with neither field, so " +
			"the obligation this test is about is not stated anywhere a client reads")
	}
	found := strings.Join(paragraphs, "\n\n")

	for _, clause := range []struct {
		what   string
		phrase string
		why    string
	}{
		{"whose bound it is", "not this connector's",
			"a client that thinks the seventy five seconds is ours will expect us to change it"},
		{"the number", "seventy five seconds",
			"without it the paragraph says a bound exists but not what to expect"},
		// Not the bare word: `timeout` is said three more times in this same paragraph and
		// the next, so asserting on it alone went green with the sentence that names it
		// deleted. The clause has to be the sentence.
		{"the code that comes back", "runs out is `timeout`",
			"a client branches on the code, and this is the one it gets"},
		{"that resending is allowed", "may resend",
			"the whole point of the paragraph is what to do next"},
		{"that the identifier is not what makes it safe", "the identifier is not what makes that safe",
			"review round 1 caught this promising more than it delivers: a failed first " +
				"attempt leaves no record, so the same key answers nothing and the work runs again"},
		{"why a failed attempt leaves nothing", "records only what succeeded",
			"it is the reason the same key answers nothing, and without it the paragraph " +
				"asserts the conclusion without the fact under it"},
		{"what the identifier does buy", "after the first attempt *succeeded*",
			"the narrow thing it is actually good for, which is a redelivery after a success"},
		{"the mutation that is not covered", "rotates the link",
			"a named example, because `may already have happened` reads as hypothetical"},
		{"reading the state back", "reads the state back before resending",
			"it is the only thing a client can actually do about it"},
		{"the socket write it does not cover", "not interruptible",
			"a client would otherwise read `timeout` as proof WhatsApp was asked"},
		{"the retried send it does not cover", "retried once the connection is back",
			"round 2 of the review measured this one: whatsmeow retries an interrupted " +
				"send with a zero timeout, which switches its timer off, so the claim that " +
				"nothing runs forever was false as written"},
		{"that both are unbounded, not merely slow", "not bounded",
			"`covered` and `slower` are what this would degrade into"},
	} {
		if !strings.Contains(found, clause.phrase) {
			t.Errorf("the paragraph about a command with neither field does not say %s (%q): %s",
				clause.what, clause.phrase, clause.why)
		}
	}
}

// relevantParagraphs returns the paragraphs holding the phrase, plus the one after each,
// because the obligation here runs past its own first paragraph and a client reads both.
func relevantParagraphs(prose, phrase string) []string {
	all := strings.Split(prose, "\n\n")
	var found []string
	for i, paragraph := range all {
		if !strings.Contains(paragraph, phrase) {
			continue
		}
		found = append(found, paragraph)
		if i+1 < len(all) {
			found = append(found, all[i+1])
		}
	}
	return found
}
