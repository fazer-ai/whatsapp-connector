package main

import (
	"os/exec"
	"slices"
	"strings"
	"syscall"
	"testing"
)

// Every WAC_ variable a connector of this run sees comes from the run, and never from the
// shell that started the bench.
//
// This is not tidiness. `app.New` opens the media store whenever `WAC_MEDIA_ROOT` is set,
// and its blob sweeper then deletes expired files under that directory once a minute,
// knowing only about writes from its own process. A shell with that variable exported for
// a connector somebody is running by hand would hand this run four processes sweeping
// somebody else's blobs -- and this bench isolates everything else by identifier precisely
// so it cannot touch another fleet.
func TestTheFleetNeverInheritsAWACVariable(t *testing.T) {
	t.Parallel()

	environ := []string{
		"PATH=/usr/bin",
		"HOME=/Users/alguem",
		"WAC_MEDIA_ROOT=/var/lib/outro-conector/media",
		"WAC_MEDIA_TOKEN=segredo-de-outra-frota",
		"WAC_ENGINE=whatsmeow",
		"WAC_TEST_DATABASE_URL=postgres://nao-e-desta-corrida",
	}
	own := map[string]string{"WAC_ENGINE": "fake", "WAC_REDIS_PREFIX": "wacbench1:"}

	got := fleetEnv(environ, own, "bench-1-a", "127.0.0.1:5000")

	for _, entry := range got {
		if !strings.HasPrefix(entry, "WAC_") {
			continue
		}
		name, value, _ := strings.Cut(entry, "=")
		switch name {
		case "WAC_INSTANCE", "WAC_HTTP_ADDR":
		default:
			if own[name] != value {
				t.Errorf("o conector receberia %s, que nao veio desta corrida", entry)
			}
		}
	}
	// What is not WAC_ has to survive: a connector still needs its PATH.
	for _, want := range []string{"PATH=/usr/bin", "HOME=/Users/alguem"} {
		if !slices.Contains(got, want) {
			t.Errorf("%s nao chegou ao conector, e ele precisa dela", want)
		}
	}
	for _, want := range []string{"WAC_INSTANCE=bench-1-a", "WAC_HTTP_ADDR=127.0.0.1:5000", "WAC_ENGINE=fake"} {
		if !slices.Contains(got, want) {
			t.Errorf("%s nao chegou ao conector", want)
		}
	}
	// The run's own value has to be the one that wins, and not merely be present next to
	// an inherited one: `exec` takes the last of a repeated name, so a check that only
	// asked "is it there" would pass with both.
	last := ""
	for _, entry := range got {
		if name, value, _ := strings.Cut(entry, "="); name == "WAC_ENGINE" {
			last = value
		}
	}
	if last != "fake" {
		t.Errorf("o WAC_ENGINE que vale e %q, e esta corrida pediu fake", last)
	}
}

// A pid that has been waited for names nothing, and signalling it again is signalling
// whatever the kernel handed the number to next.
//
// This bench kills two instances in the middle of a run and shuts the fleet down minutes
// later. On a machine running several of these at once -- which is the machine this was
// written on -- the number is very likely somebody else's process group by then, and a
// SIGTERM to it arrives with no warning and no way to trace it back here.
func TestAKilledInstanceIsNeverSignalledAgain(t *testing.T) {
	t.Parallel()

	// A real process of this test's own, so the reaping is the real reaping and not a
	// struct filled in by hand to agree with the code under test.
	cmd := exec.CommandContext(t.Context(), "sleep", "60")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("nao consegui subir o processo da prova: %v", err)
	}
	live := &instance{name: "prova", pid: cmd.Process.Pid, cmd: cmd}

	if live.gone() {
		t.Fatal("a instancia se diz enterrada antes de qualquer sinal")
	}
	if err := live.kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	if !live.gone() {
		t.Fatal("depois do kill e do wait, a instancia nao se diz enterrada, entao o shutdown " +
			"mandaria SIGCONT e SIGTERM para um pid que o kernel ja pode ter reciclado")
	}

	// And the later calls take the early return rather than reaching a signal: a second
	// kill of a reaped pid would otherwisecome back as an error from the kernel, or worse,
	// land on somebody else.
	if err := live.kill(); err != nil {
		t.Errorf("um segundo kill devia nao fazer nada: %v", err)
	}
	if err := live.freeze(); err != nil {
		t.Errorf("freeze depois do enterro devia nao fazer nada: %v", err)
	}
	if err := live.thaw(); err != nil {
		t.Errorf("thaw depois do enterro devia nao fazer nada: %v", err)
	}
	live.stop() // não pode travar nem sinalizar
}
