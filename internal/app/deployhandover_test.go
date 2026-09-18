package app_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// What a rolling deploy costs, measured on the deployment at chat.fazer.ai and reproduced
// here: the new container came up at 13:15:50, the old one released and said `connector is
// down` at 13:15:51, and the account was only picked up at 13:16:20. Thirty seconds of an
// account being nobody's, on every release.
//
// The mechanism is ordering, and it is deterministic. The incoming instance makes its one
// early resume pass on the way in, and at that instant the outgoing one still holds the
// lease -- it exits a second later. The pass after that is a whole `resumeInterval` away.
// So the only early pass happens early by about a second, and the price is thirty.
//
// Three comments in this repository already promise the opposite. `shutdown` says giving
// the sessions back is "the difference between a peer picking them up now and one TTL from
// now"; `Manager.StopAll` says "a released lease is one a peer can take immediately"; and
// `Keys.HandBack` is built around a wake arriving while an owner is letting go. All three
// describe a hand-over that nothing completes, because a lease being *takeable* is not a
// lease being *taken* and no instance is told to come and take it.
//
// The instance that is leaving is the one that knows, exactly, which accounts it is
// dropping and when. So it says so, on the stream that exists for saying it.
func TestAnInstanceGoingDownTellsTheFleetWhatItIsGivingUp(t *testing.T) {
	server := miniredis.RunT(t)
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "wa.db")

	const sid = "2f1c6f0e-0000-4000-8000-000000000268"
	// Seeded rather than woken, and that is what makes the assertion mean anything: a
	// session this test woke would leave its own `session.wake` on the control stream, and
	// then finding one there afterwards would prove nothing. The instance picks this
	// account up through the sweep, so the stream starts empty and anything on it at the
	// end was put there by the shutdown.
	seedWantedConnected(t, dsn, sid, "5511999990268")

	connector, stop := startStoppable(t, server.Addr(), "inst-a", map[string]string{"WAC_DATABASE_URL": dsn})
	waitFor(t, "the account to be picked up", func() bool { return connector.Sessions() == 1 })

	stop()

	wakes := wakesOnControl(t, server.Addr(), sid)
	if len(wakes) == 0 {
		t.Fatal("an instance released an account on its way out and told nobody.\n" +
			"The lease is free, no peer knows it, and the account waits a whole resume " +
			"interval to be picked up -- which is the thirty seconds every deploy costs.")
	}
}

// The same thing from the other end, in the shape production has: the successor is already
// up and has already made its one early pass before the outgoing instance lets go.
//
// Without the hand-over this cannot pass inside the deadline `waitFor` allows, and not by a
// narrow margin: the successor's next pass is `resumeInterval` away, thirty seconds against
// ten. With it, the wake reaches a live instance and the adoption is immediate -- measured
// at 0.1s between the XADD and `adopted a session`, against two connector processes.
func TestTheSuccessorPicksUpWhatItsPredecessorGaveUpWithoutWaitingForASweep(t *testing.T) {
	server := miniredis.RunT(t)
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "wa.db")

	const sid = "2f1c6f0e-0000-4000-8000-000000000269"
	seedWantedConnected(t, dsn, sid, "5511999990269")

	outgoing, stopOutgoing := startStoppable(t, server.Addr(), "inst-out", map[string]string{"WAC_DATABASE_URL": dsn})
	waitFor(t, "the outgoing instance to be running the account", func() bool {
		return outgoing.Sessions() == 1
	})

	// Started while the account is still owned, which is the whole point: its one early
	// pass finds the lease taken and comes back with nothing, exactly as the new container
	// does a second before the old one exits.
	incoming := start(t, server.Addr(), "inst-in", map[string]string{"WAC_DATABASE_URL": dsn})
	waitFor(t, "the incoming instance to have made its early pass and found nothing", func() bool {
		return incoming.Sessions() == 0
	})

	stopOutgoing()

	waitFor(t, "the successor to pick the account up", func() bool { return incoming.Sessions() == 1 })
}

// wakesOnControl is every `session.wake` on the control stream naming this session.
func wakesOnControl(t *testing.T, addr, sid string) []protocol.Command {
	t.Helper()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()

	entries, err := rdb.XRange(context.Background(), redisx.NewKeys("wa:", 8).Control(), "-", "+").Result()
	if err != nil {
		t.Fatalf("read the control stream: %v", err)
	}
	var wakes []protocol.Command
	for _, entry := range entries {
		fields := make(map[string]string, len(entry.Values))
		for key, value := range entry.Values {
			if text, ok := value.(string); ok {
				fields[key] = text
			}
		}
		command, parseErr := protocol.ParseCommand(fields)
		if parseErr != nil {
			t.Fatalf("parse a control frame: %v", parseErr)
		}
		if command.Type == protocol.CommandSessionWake && command.SID == sid {
			wakes = append(wakes, command)
		}
	}
	return wakes
}
