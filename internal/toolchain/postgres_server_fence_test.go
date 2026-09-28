package toolchain_test

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The server flags that make the PostgreSQL pass affordable, and the reason each one is
// here. Every test creates a database and drops it again, and against a durable server
// those two statements are the pass: `internal/engine/whatsmeow` took 148s on a default
// server and 15s on one started with these, same machine, nothing else running (#342).
// All three belong to the server; the one of them a session may set on its own was tried
// in the url and left the package at 112s.
var notDurable = []string{
	"-c fsync=off",
	"-c synchronous_commit=off",
	"-c full_page_writes=off",
}

var (
	makeVar = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_-]*)\s*[:?]?=\s*(.*)$`)
	makeRef = regexp.MustCompile(`\$\(([A-Za-z_][A-Za-z0-9_-]*)\)`)
)

const (
	// The variable the Makefile prints when the pass has no server, and runs to start one.
	serverVar = "test-postgres_RUN"
	// The target that starts that server and waits for it, which is how CI gets one.
	serverTarget = "test-postgres-server"
	// The pass the server is for.
	pgPass = "test-postgres"
)

// One command, and everybody runs it. Held in three places before this, the flags would
// reach the line a developer copies and not the server CI starts, or the other way round,
// and the pass would go back to costing ten times the SQLite one with nothing red to say so.
func TestThePostgreSQLTheTestsRunAgainstIsNotDurable(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(makefilePath)
	if err != nil {
		t.Fatalf("read %s: %v", makefilePath, err)
	}
	// Read here rather than through `assignment`, whose names are identifiers: the server
	// variables carry the target in theirs, hyphen included, and read that way this
	// reported the variable as gone.
	vars := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		if m := makeVar.FindStringSubmatch(line); m != nil {
			vars[m[1]] = makeRef.ReplaceAllStringFunc(strings.TrimSpace(m[2]), func(ref string) string {
				return vars[makeRef.FindStringSubmatch(ref)[1]]
			})
		}
	}
	command, ok := vars[serverVar]
	if !ok {
		t.Fatalf("%s no longer defines %s, the command that starts the server the PostgreSQL pass runs against", makefilePath, serverVar)
	}
	// After the image, because before it they are flags to docker and not to postgres:
	// `docker run -c` is a CPU share, and the server would start durable with no error.
	_, server, found := strings.Cut(command, "postgres:18-alpine")
	if !found {
		t.Fatalf("%s is %q, which starts no postgres:18-alpine: the image CI and the Makefile agree on", serverVar, command)
	}
	for _, flag := range notDurable {
		if !strings.Contains(server, flag) {
			t.Errorf("%s does not pass %q to the server:\n\t%s\n"+
				"\tevery test creates and drops a database, and a durable server spends the pass on exactly that",
				serverVar, flag, command)
		}
	}

	recipe, _ := recipeOf(t, serverTarget)
	if !strings.Contains(recipe, "$("+serverVar+")") {
		t.Errorf("%s does not run $(%s):\n%s\n\tspelled out a second time, the server CI starts and the one the Makefile prints drift apart",
			serverTarget, serverVar, recipe)
	}
}

// CI starts the server through the target above, because a `services:` container cannot be
// given a command, which is where the flags go. A job that keeps a PostgreSQL service runs
// the pass against a durable server, and one that starts nothing runs it against no server.
func TestCIStartsThePostgreSQLThroughTheMakefile(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(workflowsDir)
	if err != nil {
		t.Fatalf("read %s: %v", workflowsDir, err)
	}
	var passes int
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || (!strings.HasSuffix(name, ".yml") && !strings.HasSuffix(name, ".yaml")) {
			continue
		}
		raw, err := os.ReadFile(workflowsDir + "/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		var wf struct {
			Jobs map[string]struct {
				Services map[string]struct {
					Image string `yaml:"image"`
				} `yaml:"services"`
				Steps []struct {
					Name string `yaml:"name"`
					Run  string `yaml:"run"`
				} `yaml:"steps"`
			} `yaml:"jobs"`
		}
		if err := yaml.Unmarshal(raw, &wf); err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, job := range sortedKeys(wf.Jobs) {
			spec := wf.Jobs[job]
			started := -1
			for i, s := range spec.Steps {
				called, _ := targetsCalledBy(s.Run)
				for _, target := range called {
					switch target {
					case serverTarget:
						if started < 0 {
							started = i
						}
					case pgPass:
						passes++
						if started < 0 {
							t.Errorf("%s job %q runs `make %s` in step %q with no `make %s` before it:\n"+
								"\tthe pass has no server to run against, or it runs against one that was not started with the flags",
								name, job, pgPass, s.Name, serverTarget)
						}
					}
				}
			}
			if started < 0 {
				continue
			}
			for service, spec := range spec.Services {
				if strings.HasPrefix(spec.Image, "postgres") {
					t.Errorf("%s job %q still declares the service %q (%s) next to `make %s`:\n"+
						"\ta service takes no command, so it is a durable server, and two servers on one port is a start that fails",
						name, job, service, spec.Image, serverTarget)
				}
			}
		}
	}
	if passes == 0 {
		t.Fatalf("no workflow in %s runs `make %s`: the pass left CI, or this reads nothing", workflowsDir, pgPass)
	}
}
