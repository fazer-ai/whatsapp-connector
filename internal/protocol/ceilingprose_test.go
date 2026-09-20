package protocol_test

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
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
// waitsNoCeilingReaches is the list the contract names and the list this file checks, and
// it is one list because it used to be two.
//
// The count read one of them and the clauses read the other, and the second was missing an
// entry: deleting the send lock from `PROTOCOL.md` left the suite green while deleting
// either of the other two went red (#292). That is the wait a client is most likely to
// meet, because every send takes it.
var waitsNoCeilingReaches = []struct {
	what   string
	phrase string
	why    string
}{
	{"the socket write it does not cover", "already being written to the socket",
		"a client would otherwise read `timeout` as proof WhatsApp was asked"},
	{"the send lock the library takes", "one send per connection at a time",
		"every send takes it, so it is the one of the three a client actually meets, and a " +
			"command held by it comes back no sooner for having named a ceiling"},
	{"the third wait no ceiling reaches", "the read lock on the socket",
		"a reconnection holds the write half for the length of its dial, and autoReconnect " +
			"is what calls it, so this one is held precisely during the reconnection #283 " +
			"is about"},
}

// namesOf lists a set of commands for a failure message, in a stable order.
func namesOf(commands map[protocol.CommandType]bool) string {
	var names []string
	for command := range commands {
		names = append(names, string(command))
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// sentenceIntroducing is the sentence that presents the list of waits, with the waits
// themselves taken out of it.
//
// The sentence and not the run, because the run opens with "A command carries two different
// ceilings" and the waits themselves say "one send per connection at a time": a check over
// the whole run that trips on any numeral trips on those, and a fence that cries wolf gets
// turned off. Taking the phrases out is what lets the rest of the sentence be read plainly.
func sentenceIntroducing(run string) string {
	first := strings.Index(run, waitsNoCeilingReaches[0].phrase)
	if first < 0 {
		return ""
	}
	opening := strings.LastIndex(run[:first], ". ")
	if opening < 0 {
		opening = -2
	}
	sentence := run[opening+2:]
	if end := strings.Index(sentence, ". "); end >= 0 {
		sentence = sentence[:end]
	}
	for _, wait := range waitsNoCeilingReaches {
		sentence = strings.ReplaceAll(sentence, wait.phrase, "")
	}
	return sentence
}

// countsIn reads the counts a sentence fixes.
//
// By the number rather than by the phrasing around it, which is the hole #292 names: the
// old check matched two constructions and its own comment promised all of them, so `holds
// exactly two:`, `holds two, and no more:`, `there are two such waits...` and `holds a pair
// of them:` all passed green on a list of three. A lower bound is not a count and is meant
// to pass -- the contract says two paragraphs up that it does not claim the list complete,
// and a lower bound agrees with that while an exact count is a promise the next pin bump
// can break.
//
// It does not catch every way English can fix a number, and that is worth saying rather
// than promising otherwise: a sentence that spells the count some way this does not know
// passes. What stops that from being the whole defence is the clause requiring the contract
// to say the list is not claimed complete, which is checked separately and is the sentence
// that makes any count a lower bound.
func countsIn(sentence string) []struct {
	written string
	is      int
} {
	words := map[string]int{
		"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
		"a pair": 2, "a couple": 2, "both": 2,
	}
	var found []struct {
		written string
		is      int
	}
	for word, value := range words {
		spelling := regexp.MustCompile(`(?i)(\w+\s+\w+\s+)?\b` + regexp.QuoteMeta(word) + `\b`)
		for _, hit := range spelling.FindAllString(sentence, -1) {
			// "more than one" and "at least two" are lower bounds, which the paragraph is
			// entitled to give: they stay true when the list grows.
			lower := strings.ToLower(hit)
			if strings.Contains(lower, "more than") || strings.Contains(lower, "at least") ||
				strings.Contains(lower, "no fewer") {
				continue
			}
			found = append(found, struct {
				written string
				is      int
			}{hit, value})
		}
	}
	return found
}

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

	// The count of the waits no ceiling reaches has been wrong twice, and the second time it
	// was wrong in a paragraph the fix to the first one did not touch: the sentence counting
	// them moved up by one and went on counting, while the paragraph below it grew a third.
	// So the check is the one the command count already uses -- read the number and compare
	// it against the things it stands for, in the same document -- rather than a literal
	// match on the two sentences that happened to carry it last time.
	//
	// A phrase that does not fix a number ("more than one", "named below") parses as no
	// count and passes, which is the intent: the contract says two paragraphs up that it
	// does not claim the list is complete, and a lower bound agrees with that while an exact
	// count is a promise the next pin bump can break.
	run := strings.Join(theCeilingRun(t, prose), "\n\n")
	named := 0
	for _, wait := range waitsNoCeilingReaches {
		if strings.Contains(run, wait.phrase) {
			named++
		}
	}
	for _, hit := range countsIn(sentenceIntroducing(run)) {
		if hit.is != named {
			t.Errorf("PROTOCOL.md says %q while the run names %d waits no ceiling reaches: "+
				"the count was two, then three when the socket read lock turned up, and a "+
				"number written out does not move when the list under it does. Say it "+
				"without fixing a number, or move both", hit.written, named)
		}
	}
	if !strings.Contains(run, "does not claim the list is complete") {
		t.Error("PROTOCOL.md no longer says the list of paths that escape every ceiling is " +
			"not claimed complete, and it has been wrong about that twice")
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

	// The count in that lead is a word, and a word does not move when the list under it
	// does. #285's fence was written after "Four of these" survived a list that had gone
	// down to three, so this compares the two rather than trusting either.
	// Which commands those are comes from the code, not from a list here. A fifth command
	// reaching the gate would leave a literal saying four, and so would a literal moved
	// into a constant or a table: what separates a derived list from a relocated one is
	// that a command leaving the gate moves it too (#292).
	reaching := commandsThatReach(t, "putOnTheWire")
	countedCommands := map[string]int{"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5, "Six": 6}
	for word, want := range countedCommands {
		if !strings.Contains(found, word+" commands now carry a ceiling") {
			continue
		}
		if len(reaching) != want {
			t.Errorf("the lead says %q but %d commands reach putOnTheWire (%s): a count "+
				"written as a word does not move when the code does",
				word+" commands", len(reaching), namesOf(reaching))
		}
		named := 0
		for command := range reaching {
			if strings.Contains(found, "`"+string(command)+"`") {
				named++
			}
		}
		if named != len(reaching) {
			t.Errorf("the run names %d of the %d commands that reach putOnTheWire (%s): a "+
				"client reads the paragraph, not the switch", named, len(reaching), namesOf(reaching))
		}
	}

	clauses := []struct {
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
		{"the waits no ceiling reaches", "does not look at the context at all",
			"they are what stops a ceiling being a promise of a reply inside it"},
		{"that a ceiling does not expire what is queued behind it",
			"it does not end the commands queued behind it either",
			"round 1 of #283's holdout asked for the distinction between the held call and " +
				"the queue, and the sentence that answered it promised the wrong half: " +
				"`bound` starts a max_runtime_ms budget when a command begins, not when it " +
				"arrives, so a queued command runs with a full one however long it waited"},
		{"what a ceiling does buy the queue", "returns the instant it is released",
			"the honest version of the same distinction: nothing frees the held call, and " +
				"what the ceiling saves is the rest of a reply wait after it is released"},
		{"the field that does drop a queued command", "names a `deadline`, which is the field checked before the work starts",
			"a client told that a runtime ceiling clears the queue would use the wrong field; " +
				"`expired` reads the absolute deadline and nothing else"},
		// The bold lead of the paragraph #283 added. #281 and #287 were both about a bold
		// lead saying something the paragraph under it did not, and neither was caught by a
		// clause, because no clause read a lead. This one does.
		{"the lead saying how many commands the ceiling reaches", "Four commands now carry a ceiling",
			"the lead used to say a send, and the gate the ceiling sits on is left by four " +
				"commands; a client sending message.edit with no ceiling of its own now gets " +
				"timeout and was told nowhere"},
		{"which four they are", "`message.edit`, `message.revoke` and `message.react`",
			"naming the count without naming the members is a tally nobody can check, and the " +
				"count is checked against these names below"},
		{"why the other three are safe to resend", "an identifier the receiving side already holds",
			"the argument that makes a resend safe is the same one the send has, and it was " +
				"written for the send alone"},
	}
	// The three waits are checked as clauses too, from the same list the count reads, so
	// the two cannot disagree about which waits exist the way they did before #292.
	clauses = append(clauses, waitsNoCeilingReaches...)
	for _, clause := range clauses {
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
