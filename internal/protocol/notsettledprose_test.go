package protocol_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// The word is only worth adding if the contract says what to do with it, and the client is
// held to `PROTOCOL.md` rather than to a doc comment in this repository.
//
// Each phrase below is a separate half of the obligation, checked separately so a rewrite
// that keeps the paragraph and drops one of them fails saying which: that the answer is
// `not_settled`, that the client asks again rather than giving up, that the same
// idempotency key is the right one on that retry, and that this is not the connector
// having a bug. A paragraph that named the code and left a client guessing what to do
// would satisfy a single substring check and teach nobody anything.
func TestTheContractSaysWhatAClientDoesWithNotSettled(t *testing.T) {
	t.Parallel()

	prose, err := os.ReadFile(filepath.Join("..", "..", "contract", "PROTOCOL.md"))
	if err != nil {
		t.Fatalf("read the contract's prose: %v", err)
	}

	// The paragraph and not the whole file, which is the difference between a fence and a
	// coincidence: "same `idempotency_key`" and "`internal`" both appear elsewhere in
	// PROTOCOL.md already, so a file-wide search stays green on a paragraph that dropped
	// them. Validated the only way a prose fence can be: by deleting each clause from this
	// paragraph and requiring red.
	var paragraph string
	for _, block := range strings.Split(string(prose), "\n\n") {
		if strings.Contains(block, "`not_settled`") {
			paragraph = block
			break
		}
	}
	if paragraph == "" {
		t.Fatal("contract/PROTOCOL.md has no paragraph about `not_settled`: a client that never reads the word cannot branch on it")
	}

	for _, clause := range []struct {
		phrase string
		why    string
	}{
		{"ask again", "the whole difference from `timeout` is that asking again settles this one"},
		{"same `idempotency_key`", "a client that generates a new key on the retry makes the second group"},
		{"`internal`", "without the contrast a client pages a human for a case that settles itself"},
		{"`timeout`", "the word it is carved out of is what tells a client which of the two it got"},
		{"`not_attempted`", "the third answer about time, and a client has to be able to place this one against it"},
	} {
		if !strings.Contains(paragraph, clause.phrase) {
			t.Errorf("the `not_settled` paragraph of contract/PROTOCOL.md never says %q: %s", clause.phrase, clause.why)
		}
	}
}

// The two prose halves that count the codes nobody sends have to agree with the list, and
// with each other. They did not: the doc comment over the catalogue said four while
// `errorCodesWithNoProducer` held three and `PROTOCOL.md` said three, and nothing noticed,
// because a number written in words is invisible to every check in this repository.
//
// It matters more than a typo. That sentence is how a reader decides whether a code they
// are about to branch on is ever sent, and the reader on the other side is a client. This
// round added a code and had to know which number to leave alone; the next one should not
// have to count by hand.
func TestBothProseHalvesCountTheSameCodesNobodySends(t *testing.T) {
	t.Parallel()

	words := map[string]int{
		"Zero": 0, "One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5, "Six": 6,
		"Seven": 7, "Eight": 8, "Nine": 9, "Ten": 10,
	}
	claim := func(t *testing.T, where, text, pattern string) (int, bool) {
		t.Helper()

		found := regexp.MustCompile(pattern).FindStringSubmatch(text)
		if found == nil {
			t.Errorf("%s no longer says how many codes are published and never sent, "+
				"and that sentence is how a client decides whether a branch it is about to "+
				"write ever runs", where)
			return 0, false
		}
		n, ok := words[found[1]]
		if !ok {
			t.Errorf("%s counts them as %q, which is not a number this test can read", where, found[1])
			return 0, false
		}
		return n, true
	}

	catalogue, err := os.ReadFile("errors.go")
	if err != nil {
		t.Fatalf("read the catalogue: %v", err)
	}
	prose, err := os.ReadFile(filepath.Join("..", "..", "contract", "PROTOCOL.md"))
	if err != nil {
		t.Fatalf("read the contract's prose: %v", err)
	}

	inGo, okGo := claim(t, "internal/protocol/errors.go", string(catalogue),
		`(?m)^// (\w+) of these are published and never sent`)
	inContract, okContract := claim(t, "contract/PROTOCOL.md", string(prose),
		`(?m)^- (\w+) codes are published and never sent`)

	// The list is the fact; both sentences are claims about it.
	want := len(errorCodesWithNoProducer)
	if okGo && inGo != want {
		t.Errorf("internal/protocol/errors.go says %d codes are published and never sent, "+
			"and errorCodesWithNoProducer holds %d", inGo, want)
	}
	if okContract && inContract != want {
		t.Errorf("contract/PROTOCOL.md says %d codes are published and never sent, "+
			"and errorCodesWithNoProducer holds %d", inContract, want)
	}
}

// The value is reachable, which is not something the catalogue entry alone says.
//
// `NewError` degrades a code it does not know to `internal`, which is the guard that keeps
// a typo in one package from putting an unknown word in front of a client. It also means a
// value declared and never added to `AllErrorCodes` is silently replaced on its way out:
// the constant exists, the code reads as if it worked, and every client sees `internal`.
func TestNotSettledSurvivesBeingTurnedIntoAnError(t *testing.T) {
	t.Parallel()

	if !protocol.ErrorNotSettled.Valid() {
		t.Fatal("not_settled is not in AllErrorCodes, so NewError replaces it with internal and no client ever sees it")
	}
	err := protocol.NewError(protocol.ErrorNotSettled, "a group by this name was made and which request made it is not settled yet")
	if got := string(err.Code); got != "not_settled" {
		t.Fatalf("NewError turned not_settled into %q", got)
	}
	if err.Message != "a group by this name was made and which request made it is not settled yet" {
		t.Errorf("the message came back as %q", err.Message)
	}
}
