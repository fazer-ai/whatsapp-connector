package toolchain_test

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const (
	workflowPath = "../../.github/workflows/ci.yml"
	makefilePath = "../../Makefile"

	// The target whose help text says it is the whole of what CI enforces. Every gate in
	// the workflow has to be reachable from here, or named below with the reason it is not.
	promise = "check"
)

// An exemption from `make check`, and the local target it leans on. A reason in prose is
// not an assertion: the exemption for the lint action said "`make lint` is reachable from
// check", and `check-offline: tidy` would have left that sentence in place while the target
// stopped running lint and the SQLite pass at all. standsFor is what makes the premise
// fail with it.
type exemption struct {
	standsFor string // the target `make check` has to keep reaching; empty when there is no local counterpart
	why       string
}

// Steps that gate nothing: they move bytes into or out of the runner. Keyed by the action
// without its version, because the version moves and the reason does not.
var notAGate = map[string]string{
	"actions/checkout":           "puts the tree on the runner",
	"actions/setup-go":           "installs the toolchain make would otherwise find on PATH",
	"actions/upload-artifact":    "publishes coverage.txt after the pass that wrote it",
	"docker/setup-buildx-action": "installs the builder the image job runs on",
}

// Gates CI enforces from outside `make check`, each with the reason it is outside. A gate
// that is neither here nor reachable from `check` fails this test, and that is the whole
// point: these were two hand-written lists that had already drifted apart once, with the
// target reporting "CI will accept this" over a pass CI ran and it did not.
//
// Keyed by the action for a `uses:` step and by the step's name for a `run:` step.
var outsideCheck = map[string]exemption{
	"golangci/golangci-lint-action": {standsFor: "lint", why: "the same binary, version and config as `make lint`. " +
		"The action is kept for its own caching and for turning findings into annotations on the diff, neither of which make can do from here."},
	"docker/build-push-action": {why: "builds the release image. It needs buildx and a layer cache, so `make check` does not promise it; " +
		"what it protects is that a change cannot break the release build without CI saying so on the pull request that made it."},
	"It comes up with its own volume attached": {why: "starts the built image with its volume attached, which needs the image the step above built. " +
		"Nothing a developer runs locally has that image, so this one is CI's alone."},
}

// Targets the workflow runs that `check` does not reach, with the reason. The list exists
// so that adding a target to CI is a decision somebody writes down, rather than a line
// that lands and leaves `check` quietly behind.
var targetsOutsideCheck = map[string]exemption{
	"test-cover": {standsFor: "test", why: "`make test` under a coverage profile: the same packages and the same cleared variables, " +
		"plus coverage.txt for the artifact. What `check` does not reach is the profile, and a profile gates nothing."},
}

type workflow struct {
	Jobs map[string]struct {
		Steps []struct {
			Name string `yaml:"name"`
			Uses string `yaml:"uses"`
			Run  string `yaml:"run"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

// `make foo`, `make -s foo`, `make foo bar`, and the same after a shell separator.
//
// Anchored to a command position rather than to the word, because the word appears in
// English: a step whose script says "# make sure the volume is attached" would otherwise
// report four targets the Makefile does not have, and a fence that fails on prose is a
// fence somebody deletes.
var makeCall = regexp.MustCompile(`(?m)(?:^|[;&|(])[ \t]*make\b((?:[ \t]+-{1,2}[^\s]+)*)((?:[ \t]+[a-zA-Z0-9][a-zA-Z0-9_.-]*)+)`)

func TestEveryGateCIEnforcesIsReachableFromMakeCheck(t *testing.T) {
	t.Parallel()

	steps := workflowSteps(t)
	reachable, targets := reachableFrom(t, promise)
	used := map[string]bool{}

	var gates int
	for _, step := range steps {
		called, only := targetsCalledBy(step.Run)
		switch {
		// A script that calls make and then does something else is the drift this fence
		// exists to catch, hidden inside a step that looks compliant: `make tidy` followed
		// by `go vet ./...` adds a gate nobody local runs. Such a step is judged as the
		// script it is, not as the make call it starts with.
		case len(called) > 0 && only:
			gates++
			for _, target := range called {
				used[target] = true
				if !targets[target] {
					t.Errorf("the workflow step %q runs `make %s`, and the Makefile has no such target:\n"+
						"\tthe step fails on every run, or the target was renamed and this side was not", step.label(), target)
					continue
				}
				if reachable[target] {
					continue
				}
				if x, ok := targetsOutsideCheck[target]; ok {
					standsUp(t, reachable, "the target "+target, x)
					continue
				}
				t.Errorf("the workflow step %q runs `make %s`, which `make %s` does not reach:\n"+
					"\tCI enforces it and the target that promises to be everything CI enforces does not run it.\n"+
					"\tEither add it to the dependencies of %s, or add it to targetsOutsideCheck with the reason it belongs only to CI.",
					step.label(), target, promise, promise)
			}
		case step.Uses != "":
			action := strings.SplitN(step.Uses, "@", 2)[0]
			if notAGate[action] != "" {
				used[action] = true
				continue
			}
			gates++
			used[action] = true
			x, ok := outsideCheck[action]
			if !ok {
				t.Errorf("the workflow uses %q and nothing here says what it is:\n"+
					"\tif it enforces something, `make %s` has to run it too; if it does not, say so in notAGate.\n"+
					"\tEither way it goes in one of the lists in this file, with a reason.",
					action, promise)
				continue
			}
			standsUp(t, reachable, "the action "+action, x)
		default:
			gates++
			used[step.label()] = true
			x, ok := outsideCheck[step.label()]
			if !ok {
				what := "runs a script of its own instead of a make target"
				if len(called) > 0 {
					what = "runs make and then commands that are not make"
				}
				t.Errorf("the workflow step %q %s:\n"+
					"\ta command here is a gate `make %s` cannot run and developers cannot reproduce.\n"+
					"\tMove it behind a target, or add it to outsideCheck with the reason it is CI's alone.\n"+
					"\tThe script:\n\t\t%s",
					step.label(), what, promise, strings.ReplaceAll(strings.TrimSpace(step.Run), "\n", "\n\t\t"))
				continue
			}
			standsUp(t, reachable, "the step "+step.label(), x)
		}
	}

	// An exemption nobody reaches is a decision about a workflow that has moved on. It is
	// how these lists rot into a record of what CI used to do, which is the state the
	// Makefile and this workflow were already in when the issue was filed.
	for _, list := range []struct {
		name string
		of   map[string]exemption
	}{{"outsideCheck", outsideCheck}, {"targetsOutsideCheck", targetsOutsideCheck}} {
		for key := range list.of {
			if !used[key] {
				t.Errorf("%s exempts %q and no step in %s uses it:\n"+
					"\tthe step was renamed or removed, and the exemption outlived it", list.name, key, workflowPath)
			}
		}
	}
	for key := range notAGate {
		if !used[key] {
			t.Errorf("notAGate lists %q and no step in %s uses it:\n"+
				"\tthe step was renamed or removed, and the entry outlived it", key, workflowPath)
		}
	}

	t.Logf("classified %d gate(s) across %d workflow step(s); `make %s` reaches %v", gates, len(steps), promise, sorted(reachable))

	// Anti-vacuity: a fence that walked an empty list reports exactly what a clean one
	// reports. Everything above is a loop over something parsed out of a file, and a
	// parse that came back empty is the failure this whole issue was about.
	if gates == 0 {
		t.Fatalf("read %d step(s) from %s and classified none of them as a gate: the workflow moved, or the parse is silently empty", len(steps), workflowPath)
	}
	if len(reachable) < 2 {
		t.Fatalf("`make %s` reaches %v in %s: a promise with no dependencies is the defect this fence exists to catch", promise, sorted(reachable), makefilePath)
	}
}

// standsUp fails when an exemption's own premise has stopped being true. Each one is
// allowed because `make check` still reaches the local equivalent; when it stops reaching
// it, the exemption is a sentence about a target that no longer runs.
func standsUp(t *testing.T, reachable map[string]bool, what string, x exemption) {
	t.Helper()

	if x.standsFor == "" || reachable[x.standsFor] {
		return
	}
	t.Errorf("%s is exempt from `make %s` because `make %s` covers it locally, and `make %s` no longer reaches `%s`:\n"+
		"\tthe exemption reads: %s\n"+
		"\tCI enforces it, nothing local does, and the list still says otherwise.",
		what, promise, x.standsFor, promise, x.standsFor, x.why)
}

// The fence reads scripts written for a shell, and a script carries prose: comments,
// echoed messages, names of things. Every prose case here was a target this file claimed
// the Makefile was missing before the pattern was anchored to a command position.
//
// onlyMake is the other half, and it is what a mixed script turns on: a step that runs a
// target and then something else is a gate hiding behind a compliant-looking line.
func TestTargetsCalledByReadsCommandsAndNotProse(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		script string
		want   []string
		only   bool
	}{
		{script: "make test-postgres", want: []string{"test-postgres"}, only: true},
		{script: "make -s check", want: []string{"check"}, only: true},
		{script: "make lint test", want: []string{"lint", "test"}, only: true},
		{script: "make tidy\nmake lint", want: []string{"tidy", "lint"}, only: true},
		{script: "make tidy && make lint", want: []string{"tidy", "lint"}, only: true},
		{script: "# only a comment here\nmake tidy", want: []string{"tidy"}, only: true},
		// Mixed: make plus a gate nobody local runs.
		{script: "make tidy\ngo vet ./...", want: []string{"tidy"}},
		{script: "make tidy && go vet ./...", want: []string{"tidy"}},
		{script: "go build ./... && make lint", want: []string{"lint"}},
		{script: "docker ps; make tidy", want: []string{"tidy"}},
		// Prose: not a call at all.
		{script: "# make sure the volume is attached\ndocker run smoke"},
		{script: "echo 'this will make things slow'"},
		{script: "echo \"nothing here to make of it\""},
	} {
		got, only := targetsCalledBy(tc.script)
		if !equal(got, tc.want) || only != tc.only {
			t.Errorf("targetsCalledBy(%q) = %v, %t; want %v, %t", tc.script, got, only, tc.want, tc.only)
		}
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type step struct {
	Name string
	Uses string
	Run  string
}

func (s step) label() string {
	if s.Name != "" {
		return s.Name
	}
	return s.Uses
}

func workflowSteps(t *testing.T) []step {
	t.Helper()

	raw, err := os.ReadFile(workflowPath)
	if err != nil {
		t.Fatalf("read %s: %v", workflowPath, err)
	}
	var wf workflow
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", workflowPath, err)
	}

	var steps []step
	for _, name := range sortedKeys(wf.Jobs) {
		for _, s := range wf.Jobs[name].Steps {
			steps = append(steps, step{Name: s.Name, Uses: s.Uses, Run: s.Run})
		}
	}
	if len(steps) == 0 {
		t.Fatalf("%s parsed into %d job(s) and no steps at all", workflowPath, len(wf.Jobs))
	}
	return steps
}

// targetsCalledBy returns the targets a script invokes, and whether the script is make
// calls and nothing else. The second half is what stops a compliant-looking step from
// carrying an extra gate: `make tidy` on one line and `go vet ./...` on the next.
func targetsCalledBy(script string) (called []string, onlyMake bool) {
	for _, m := range makeCall.FindAllStringSubmatch(script, -1) {
		called = append(called, strings.Fields(m[2])...)
	}
	return called, len(called) > 0 && everyCommandIsMake(script)
}

// everyCommandIsMake splits a script the way a shell separates commands, ignoring comments
// and blank lines, and asks whether each one starts with make. A continuation line or a
// heredoc reads as "not make", which sends the step to the branch that demands a written
// exemption: the wrong answer there costs a sentence, and the wrong answer the other way
// costs an unnoticed gate.
func everyCommandIsMake(script string) bool {
	for _, line := range strings.Split(script, "\n") {
		line = strings.TrimSpace(comment.ReplaceAllString(line, ""))
		if line == "" {
			continue
		}
		for _, cmd := range shellSeparator.Split(line, -1) {
			if cmd = strings.TrimSpace(cmd); cmd != "" && !strings.HasPrefix(cmd, "make ") && cmd != "make" {
				return false
			}
		}
	}
	return true
}

// reachableFrom walks the Makefile's own dependency graph from a target, and also returns
// every target it declares.
func reachableFrom(t *testing.T, root string) (reachable, declared map[string]bool) {
	t.Helper()

	raw, err := os.ReadFile(makefilePath)
	if err != nil {
		t.Fatalf("read %s: %v", makefilePath, err)
	}
	prereqs := parseMakefile(string(raw))
	if len(prereqs) == 0 {
		t.Fatalf("%s parsed into no targets at all", makefilePath)
	}

	declared = map[string]bool{}
	for name := range prereqs {
		declared[name] = true
	}
	reachable = map[string]bool{}
	for queue := []string{root}; len(queue) > 0; queue = queue[1:] {
		name := queue[0]
		if reachable[name] {
			continue
		}
		reachable[name] = true
		queue = append(queue, prereqs[name]...)
	}
	delete(reachable, root)
	return reachable, declared
}

// parseMakefile reads the rules as make reads them. Variables are expanded because `check`
// names its server passes through one: read literally, the prerequisites would say `check`
// runs neither.
func parseMakefile(raw string) map[string][]string {
	vars := map[string]string{}
	prereqs := map[string][]string{}
	for _, line := range strings.Split(raw, "\n") {
		if line == "" || line[0] == '\t' || line[0] == '#' || line[0] == '.' {
			continue
		}
		if name, value, ok := assignment(line); ok {
			vars[name] = value
			continue
		}
		name, rest, ok := strings.Cut(line, ":")
		if !ok || strings.ContainsAny(name, " \t$") || name == "" {
			continue
		}
		// `check: UNDER_CHECK := yes` is a target-specific variable, not a rule. Read as
		// prerequisites it hands `check` three that do not exist, and whether that is
		// harmless depends only on which side of the real rule the line sits.
		if _, _, isVar := assignment(strings.TrimSpace(rest)); isVar {
			continue
		}
		prereqs[name] = strings.Fields(expand(vars, comment.ReplaceAllString(rest, "")))
	}
	return prereqs
}

// A target-specific variable shares a rule's shape and is not one. The Makefile puts it
// above the rule it belongs to, which is where it reads best and also where getting this
// wrong does no harm, so the order is pinned here rather than left to the file.
func TestParseMakefileReadsRulesAndNotTargetVariables(t *testing.T) {
	t.Parallel()

	for _, order := range []string{
		"check: UNDER_CHECK := yes\ncheck: check-servers check-offline\n",
		"check: check-servers check-offline\ncheck: UNDER_CHECK := yes\n",
	} {
		got := parseMakefile("PASSES := test-postgres\n" + order + "check-offline: lint test $(PASSES)\n")
		if want := []string{"check-servers", "check-offline"}; !equal(got["check"], want) {
			t.Errorf("parseMakefile(%q)[\"check\"] = %v, want %v", order, got["check"], want)
		}
		if want := []string{"lint", "test", "test-postgres"}; !equal(got["check-offline"], want) {
			t.Errorf("parseMakefile(%q)[\"check-offline\"] = %v, want %v", order, got["check-offline"], want)
		}
	}
}

var (
	comment = regexp.MustCompile(`#.*$`)
	// What a shell reads as the end of one command and the start of the next.
	shellSeparator = regexp.MustCompile(`&&|\|\||[;|]`)
	assignRe       = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*[:?+]?=\s*(.*)$`)
	referenceRe    = regexp.MustCompile(`\$\(([A-Za-z_][A-Za-z0-9_]*)\)`)
)

func assignment(line string) (name, value string, ok bool) {
	m := assignRe.FindStringSubmatch(line)
	if m == nil {
		return "", "", false
	}
	return m[1], strings.TrimSpace(comment.ReplaceAllString(m[2], "")), true
}

func expand(vars map[string]string, s string) string {
	return referenceRe.ReplaceAllStringFunc(s, func(ref string) string {
		return vars[referenceRe.FindStringSubmatch(ref)[1]]
	})
}

func sorted(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for name := range m {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
