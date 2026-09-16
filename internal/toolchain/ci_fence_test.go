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
var outsideCheck = map[string]string{
	"golangci/golangci-lint-action": "the same binary, version and config as `make lint`, which is reachable from check. " +
		"The action is kept for its own caching and for turning findings into annotations on the diff, neither of which make can do from here.",
	"docker/build-push-action": "builds the release image. It needs buildx and a layer cache, so `make check` does not promise it; " +
		"what it protects is that a change cannot break the release build without CI saying so on the pull request that made it.",
	"It comes up with its own volume attached": "starts the built image with its volume attached, which needs the image the step above built. " +
		"Nothing a developer runs locally has that image, so this one is CI's alone.",
}

// Targets the workflow runs that `check` does not reach, with the reason. The list exists
// so that adding a target to CI is a decision somebody writes down, rather than a line
// that lands and leaves `check` quietly behind.
var targetsOutsideCheck = map[string]string{
	"test-cover": "`make test` under a coverage profile: the same packages and the same cleared variables, plus coverage.txt for the artifact. " +
		"`check` reaches `test`, so the gate is covered; what it does not reach is the profile, and a profile gates nothing.",
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

	var gates int
	for _, step := range steps {
		called := targetsCalledBy(step.Run)
		switch {
		case len(called) > 0:
			gates++
			for _, target := range called {
				if !targets[target] {
					t.Errorf("the workflow step %q runs `make %s`, and the Makefile has no such target:\n"+
						"\tthe step fails on every run, or the target was renamed and this side was not", step.label(), target)
					continue
				}
				if reachable[target] || targetsOutsideCheck[target] != "" {
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
				continue
			}
			gates++
			if outsideCheck[action] == "" {
				t.Errorf("the workflow uses %q and nothing here says what it is:\n"+
					"\tif it enforces something, `make %s` has to run it too; if it does not, say so in notAGate.\n"+
					"\tEither way it goes in one of the two lists in this file, with a reason.",
					action, promise)
			}
		default:
			gates++
			if outsideCheck[step.label()] == "" {
				t.Errorf("the workflow step %q runs a script of its own instead of a make target:\n"+
					"\ta script here is a gate `make %s` cannot run and developers cannot reproduce.\n"+
					"\tMove it behind a target, or add it to outsideCheck with the reason it is CI's alone.",
					step.label(), promise)
			}
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

// The fence reads scripts written for a shell, and a script carries prose: comments,
// echoed messages, names of things. Every one of these was a target this file claimed the
// Makefile was missing before the pattern was anchored to a command position.
func TestTargetsCalledByReadsCommandsAndNotProse(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		script string
		want   []string
	}{
		{"make test-postgres", []string{"test-postgres"}},
		{"make -s check", []string{"check"}},
		{"make lint test", []string{"lint", "test"}},
		{"go build ./... && make lint", []string{"lint"}},
		{"docker ps; make tidy", []string{"tidy"}},
		{"# make sure the volume is attached\ndocker run smoke", nil},
		{"echo 'this will make things slow'", nil},
		{"echo \"nothing here to make of it\"", nil},
	} {
		got := targetsCalledBy(tc.script)
		if len(got) != len(tc.want) {
			t.Errorf("targetsCalledBy(%q) = %v, want %v", tc.script, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("targetsCalledBy(%q) = %v, want %v", tc.script, got, tc.want)
				break
			}
		}
	}
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

func targetsCalledBy(script string) []string {
	var called []string
	for _, m := range makeCall.FindAllStringSubmatch(script, -1) {
		called = append(called, strings.Fields(m[2])...)
	}
	return called
}

// reachableFrom walks the Makefile's own dependency graph from a target, and also returns
// every target it declares. Variables are expanded because `check` names its server passes
// through one: reading the prerequisites literally would report that `check` runs neither.
func reachableFrom(t *testing.T, root string) (reachable, declared map[string]bool) {
	t.Helper()

	raw, err := os.ReadFile(makefilePath)
	if err != nil {
		t.Fatalf("read %s: %v", makefilePath, err)
	}

	vars := map[string]string{}
	prereqs := map[string][]string{}
	for _, line := range strings.Split(string(raw), "\n") {
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
		prereqs[name] = strings.Fields(expand(vars, comment.ReplaceAllString(rest, "")))
	}
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

var (
	comment     = regexp.MustCompile(`#.*$`)
	assignRe    = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*[:?+]?=\s*(.*)$`)
	referenceRe = regexp.MustCompile(`\$\(([A-Za-z_][A-Za-z0-9_]*)\)`)
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
