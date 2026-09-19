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

	// The whole ceiling section, from the sentence that introduces the two caller fields to
	// the one telling a client to time the reply out itself. Starting at "neither field"
	// was the first cut, and it left the sentence introducing the section outside the run,
	// where it went on contradicting the paragraphs under it with nothing reading it.
	paragraphs := theCeilingRun(t, prose)
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
		// And the sentence above the run, which introduces it. #287 fixed the bold lead and
		// left this one saying the connector bounds the command, so the contradiction just
		// moved up a paragraph. The clause is here rather than in a run of its own because
		// the sentence's whole job is to hand off to what follows.
		{"the sentence introducing the run", "something the caller did not\nchoose",
			"it said the connector bounds the command while the paragraph under it says the " +
				"ceiling is the library's, which is the same contradiction #287 removed one " +
				"sentence lower"},
		// The bold lead specifically, not just the body. It said "bounded by the connector"
		// while the sentence under it explained the ceiling is the library's, and the
		// verifier's report on #281 pointed out that the lead is what a client skimming
		// the contract keeps. Two true sentences can still leave a false impression, and
		// the one in bold is the one that does it.
		{"the lead not handing the bound to this connector", "mostly not by this connector",
			"a reader who takes only the bold sentence would expect us to own the number"},
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
		{"the socket write it does not cover", "already being written to the socket",
			"a client would otherwise read `timeout` as proof WhatsApp was asked"},
		{"what a client can actually do about that one", "its own timeout on the reply",
			"it is the only remedy left once no field on the command reaches the wait"},
		{"that the list of unbounded paths is not claimed complete",
			"does not claim the list is complete",
			"round 4 found a third one in this connector's own bookkeeping, so an " +
				"exhaustive-sounding paragraph is a promise that keeps turning out false"},
		{"the group.create carve-out", "writes down what it is about to do before it asks",
			"round 4 measured that `left no record at all` is false for this one command, " +
				"and it is the command `not_settled` was added for"},
		{"keeping the key for that recovery", "Retrying with a fresh key instead makes a second group",
			"the actionable half of the carve-out"},
		{"that the send retry is bounded now", "still ends",
			"#283 put a ceiling on it, so the paragraph naming it as unbounded became false"},
		{"the two waits no ceiling reaches", "does not look at the context at all",
			"they are what stops a ceiling being a promise of a reply inside it"},
		{"what the ceiling buys instead", "ends the commands queued behind it",
			"round 1 of #283's holdout asked for exactly this distinction: the held call is " +
				"not freed, the ones behind it are"},
	} {
		if !strings.Contains(found, clause.phrase) {
			t.Errorf("the paragraph about a command with neither field does not say %s (%q): %s",
				clause.what, clause.phrase, clause.why)
		}
	}
}

// theCeilingRun returns the run of paragraphs the obligation is written across, anchored at
// both ends. Anchoring only at the start, and taking a fixed number of paragraphs after it,
// is what this did first: the run grew from two paragraphs to four as the review found
// exceptions, and the clauses that landed in the new ones went green because nothing was
// reading them. Both anchors are required, so a paragraph moved out of the run fails here
// rather than stopping being checked.
func theCeilingRun(t *testing.T, prose string) []string {
	t.Helper()

	all := strings.Split(prose, "\n\n")
	first, last := -1, -1
	for i, paragraph := range all {
		if first < 0 && strings.Contains(paragraph, "carries two different ceilings") {
			first = i
		}
		if strings.Contains(paragraph, "its own timeout on the reply") {
			last = i
		}
	}
	if first < 0 {
		t.Fatal("the paragraph that opens the ceiling section is gone, so the run has no " +
			"start anchor and none of the clauses below would be read")
	}
	if last < 0 {
		t.Fatal("the paragraph that ends the run, the one telling a client to time out the " +
			"reply itself, is gone: the run has no end anchor and the clauses after the " +
			"first paragraph would stop being read")
	}
	if last < first {
		t.Fatalf("the run's anchors are out of order: start at paragraph %d, end at %d", first, last)
	}
	return all[first : last+1]
}
