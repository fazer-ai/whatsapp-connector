package main

import (
	"slices"
	"strings"
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
