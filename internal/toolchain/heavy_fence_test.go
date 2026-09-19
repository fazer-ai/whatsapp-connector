package toolchain_test

import (
	"os"
	"strings"
	"testing"
)

// Targets that exist, that `make check` deliberately does not reach, and the reason.
//
// The fence beside this one runs in the other direction: it asks that every gate CI
// enforces be reachable from `check`, so that "CI will accept this" is not said over a
// pass `check` never ran. This one covers the case that direction cannot see. A target
// that CI does not run either is invisible to it: nothing says it exists, nothing says
// why it is not in `check`, and the reason lives in a comment no test opens.
//
// It matters because the reason is a claim that goes stale. "Too heavy for every change"
// stops being true the day somebody makes the target cheap, or wires it into `check` and
// leaves the sentence behind. The assertion is that the exemption still describes the
// Makefile: the target is there, and `check` still does not reach it.
var deliberatelyOutsideCheck = map[string]string{
	"bench-fleet": "it builds a binary, starts connector processes and waits on real clocks. " +
		"That is minutes against the seconds `check` is allowed on every change, and it needs a " +
		"PostgreSQL and a Redis of its own, which it refuses to run without. Issue #264, decision 3.",
}

// Every exemption still describes the Makefile it is about.
//
// Two directions, because each catches a different way of going stale. A target that
// vanished leaves an exemption for nothing, and the next reader takes the sentence as a
// fact about the repository. A target that became reachable from `check` leaves an
// exemption that is now a lie in the most expensive direction: it says the heavy thing is
// kept out of every change while `check` runs it on every change.
func TestEveryDeliberateExemptionStillDescribesTheMakefile(t *testing.T) {
	t.Parallel()

	if len(deliberatelyOutsideCheck) == 0 {
		t.Fatal("no exemptions are listed, so this test read nothing and would pass over any Makefile")
	}

	raw, err := os.ReadFile("../../Makefile")
	if err != nil {
		t.Fatalf("read the Makefile: %v", err)
	}
	reachable, declared := reachableFromCheck(t, string(raw))
	if len(declared) == 0 {
		t.Fatal("the Makefile parsed into no targets at all, so this fence read nothing")
	}

	for target, why := range deliberatelyOutsideCheck {
		if strings.TrimSpace(why) == "" {
			t.Errorf("%s is listed as deliberately outside `make check` with no reason.\n"+
				"The list exists for the reason; an entry without one is the comment this replaced.", target)
		}
		if !declared[target] {
			t.Errorf("%s is listed as deliberately outside `make check` and the Makefile has no such target.\n"+
				"The exemption now describes nothing, and its reason reads as a fact about this repository: %s",
				target, why)
			continue
		}
		if reachable[target] {
			t.Errorf("%s is listed as deliberately outside `make check`, and `check` reaches it.\n"+
				"The reason on record is %q, so either the target stopped being heavy and the entry has to "+
				"go, or it was wired into `check` by accident and every change is now paying for it.",
				target, why)
		}
	}
}

// reachableFromCheck answers what `make check` pulls in, reading the Makefile the way make
// does, through the same parser the other direction uses: two parsers would be two answers
// about one file, and the day they disagreed the disagreement would be silent.
func reachableFromCheck(t *testing.T, raw string) (reachable, declared map[string]bool) {
	t.Helper()

	prereqs := parseMakefile(raw)
	declared = map[string]bool{}
	for name := range prereqs {
		declared[name] = true
	}
	reachable = map[string]bool{}
	for queue := []string{"check"}; len(queue) > 0; queue = queue[1:] {
		name := queue[0]
		if reachable[name] {
			continue
		}
		reachable[name] = true
		queue = append(queue, prereqs[name]...)
	}
	delete(reachable, "check")
	return reachable, declared
}
