package app_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

// The stop hands every session's lease back so a peer can take the account now rather than
// a lease TTL from now, and the log has to say whether it did: "connector is down" alone
// reads the same whether the leases went back or were left to expire (#360).

type logLine struct {
	Level    string `json:"level"`
	Message  string `json:"message"`
	Sessions *int   `json:"sessions"`
}

func handBackLines(t *testing.T, logged string) []logLine {
	t.Helper()

	var lines []logLine
	for _, raw := range strings.Split(strings.TrimSpace(logged), "\n") {
		var line logLine
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("a log line is not JSON: %q: %v", raw, err)
		}
		if strings.Contains(line.Message, "hand") {
			lines = append(lines, line)
		}
	}
	return lines
}

func seededInstance(t *testing.T, server *miniredis.Miniredis, sids ...string) (stop func(), logged *syncBuffer, sessions func() int) {
	t.Helper()

	dsn := "sqlite:" + filepath.Join(t.TempDir(), "wa.db")
	for i, sid := range sids {
		seedWantedConnected(t, dsn, sid, "551199999036"+string(rune('0'+i)))
	}
	connector, stop, logged := startStoppableLogged(t, server.Addr(), "inst-360",
		map[string]string{"WAC_DATABASE_URL": dsn})
	waitFor(t, "the instance to be running every seeded account", func() bool {
		return connector.Sessions() == len(sids)
	})
	return stop, logged, connector.Sessions
}

func TestAShutdownSaysHowManySessionsItHandedBack(t *testing.T) {
	server := miniredis.RunT(t)
	stop, logged, _ := seededInstance(t, server,
		"2f1c6f0e-0000-4000-8000-000000036001", "2f1c6f0e-0000-4000-8000-000000036002")

	stop()

	lines := handBackLines(t, logged.String())
	if len(lines) != 1 || lines[0].Level != "info" || lines[0].Message != "handed the sessions back" ||
		lines[0].Sessions == nil || *lines[0].Sessions != 2 {
		t.Fatalf("want one info line handing back 2 sessions, got %+v", lines)
	}
	if keys := server.Keys(); containsPrefix(keys, "wa:lease:") {
		t.Errorf("the stop said it handed the leases back and left %v", keys)
	}
}

func TestAShutdownWithNoSessionsSaysThereWasNothingToHandBack(t *testing.T) {
	server := miniredis.RunT(t)
	stop, logged, _ := seededInstance(t, server)

	stop()

	lines := handBackLines(t, logged.String())
	if len(lines) != 1 || lines[0].Level != "info" || lines[0].Message != "no sessions to hand back" {
		t.Fatalf("want one info line saying there was nothing to hand back, got %+v", lines)
	}
}

// The arm the unit test pins against counts, run for real: Redis gone by the time the stop
// hands back, so every release fails and the stop has to say the leases will expire
// rather than claim a hand-back that never landed.
func TestAShutdownThatCannotReachRedisSaysTheLeasesWillExpire(t *testing.T) {
	server := miniredis.RunT(t)
	stop, logged, _ := seededInstance(t, server,
		"2f1c6f0e-0000-4000-8000-000000036003", "2f1c6f0e-0000-4000-8000-000000036004")

	server.Close()
	stop()

	var warned bool
	for _, line := range handBackLines(t, logged.String()) {
		if line.Message == "handed the sessions back" {
			t.Errorf("the stop claims a hand-back Redis never received: %+v", line)
		}
		if line.Level == "warn" && strings.HasPrefix(line.Message, "could not hand every session back") &&
			line.Sessions != nil && *line.Sessions == 2 {
			warned = true
		}
	}
	if !warned {
		t.Fatalf("want a warning that 2 leases were not handed back, got:\n%s", logged.String())
	}
}

func containsPrefix(keys []string, prefix string) bool {
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}
