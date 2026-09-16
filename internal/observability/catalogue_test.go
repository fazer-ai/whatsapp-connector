package observability_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/observability"
)

// writtenBy names, for every metric this build registers, the package that writes to it.
//
// The same shape the contract uses for its enums, and for the same reason: a metric can
// be registered and left unwired without anything failing, and three of the six were
// (#226). A registered metric reads as `0` on a panel whether it means "nothing
// happened" or "nobody is counting", and an operator cannot tell those apart from the
// outside -- `wac_events_published_total` sat at zero through every event this connector
// ever published, while its own help text offered to say whether events were being
// published.
//
// So a new metric goes in this table with the package that writes it, or in
// `notWrittenYet` with the reason. `TestEveryMetricIsAccountedFor` fails until it is one
// or the other, which is the point: the next one cannot be added and quietly left
// counting nothing.
var writtenBy = map[string]string{
	"SessionsRunning":        "internal/app",
	"CommandReadsFailed":     "internal/app",
	"CommandReadLastSuccess": "internal/app",
	"EmissionWait":           "internal/app",
	"InboxDepth":             "internal/app",
	"EventsPublished":        "internal/app",
	"CommandDuration":        "internal/app",
	"LeasesLost":             "internal/app",
}

// notWrittenYet is the metrics that are registered and known to count nothing, each with
// what is missing. Being on this list is not permission to stay on it: it is a statement
// that somebody looked, which is exactly what was absent when #226 was written. It is
// empty now, and the test above is what keeps it honest when it stops being.
var notWrittenYet = map[string]string{}

// Keyed by field and not by metric name on purpose. A Vec with no children gathers
// nothing at all, so `wac_events_published_total` and `wac_command_duration_seconds` do
// not read zero on `/metrics` -- they are absent, and an alert written on a series that
// never appears never fires. Walking the struct is what sees them; walking a scrape is
// what missed them for as long as #226 went unnoticed.

func TestEveryMetricIsAccountedFor(t *testing.T) {
	t.Parallel()

	set := reflect.TypeOf(*observability.New())
	for i := range set.NumField() {
		name := set.Field(i).Name
		if name == "Registry" {
			continue
		}
		_, written := writtenBy[name]
		_, known := notWrittenYet[name]
		switch {
		case written && known:
			t.Errorf("%s is in both tables: say which one is true", name)
		case !written && !known:
			t.Errorf("%s is registered and in neither table.\n"+
				"Add it to writtenBy with the package that writes it, or to notWrittenYet "+
				"with what is missing. A metric nobody writes reads as 0 on a panel, and a "+
				"labelled one does not appear at all (#226).", name)
		}
	}

	fields := map[string]bool{}
	for i := range set.NumField() {
		fields[set.Field(i).Name] = true
	}
	for name := range writtenBy {
		if !fields[name] {
			t.Errorf("writtenBy names %s, which is not a field of Metrics", name)
		}
	}
	for name := range notWrittenYet {
		if !fields[name] {
			t.Errorf("notWrittenYet names %s, which is not a field of Metrics", name)
		}
	}
}

// The table above is a claim about the code, and a claim nobody checks is how #226
// happened in the first place. This reads the tree and fails when a metric said to be
// written by a package has no mention of its field there.
func TestAMetricSaidToBeWrittenIsMentionedWhereItIsSaidToBe(t *testing.T) {
	t.Parallel()

	for field, pkg := range writtenBy {
		if !mentions(t, filepath.Join("..", "..", pkg), field) {
			t.Errorf("writtenBy says %s writes %s, and nothing in %s mentions it", pkg, field, pkg)
		}
	}

	for field := range notWrittenYet {
		for _, pkg := range []string{"internal/app", "internal/session", "internal/cluster"} {
			if mentions(t, filepath.Join("..", "..", pkg), field) {
				t.Errorf("%s is listed as not written, but %s mentions it: move it to writtenBy",
					field, pkg)
			}
		}
	}
}

// mentions reports whether any non-test Go file directly in dir names the identifier.
func mentions(t *testing.T, dir, identifier string) bool {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if strings.Contains(string(body), "."+identifier) {
			return true
		}
	}
	return false
}
