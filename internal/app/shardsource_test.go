package app_test

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// The test client and the connector have to agree on which stream a session's events land
// on, and until this test existed nothing made them.
//
// `ShardOf(sid) = fnv32a(sid) % shards`, so a client counting eight streams and a fleet
// publishing to sixteen agree only when `fnv32a(sid) % 16 < 8`: about half the sids, and
// for the other half the client reads a stream nobody wrote to. There is no error in that
// and no warning, just an empty list, which is the worst shape a wrong answer can take.
//
// The sid here is chosen to land on the disagreeing side and is not decoration: measured
// on the base, it hashes to stream 4 with eight and to stream 12 with sixteen. Of the
// eight sids this package already used, four were on each side, and all three tests that
// read events happened to hold one from the agreeing half. Green by luck, and the next
// test written with a sid from the other half would have inherited the luck of the draw.
func TestTheClientReadsTheSameShardTheFleetPublishesTo(t *testing.T) {
	server := miniredis.RunT(t)
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "wa.db")

	// fnv32a % 8 = 4, fnv32a % 16 = 12.
	const sid = "2f1c6f0e-0000-4000-8000-000000000002"
	seedWantedConnected(t, dsn, sid, "5511999990002")

	// No WAC_EVENT_SHARDS: the connector runs on the count a deployment runs on, which
	// is the whole point. Pinning it here is the workaround this test exists to remove.
	start(t, server.Addr(), "inst-a", map[string]string{"WAC_DATABASE_URL": dsn})

	// First that the connector published at all, asked without naming a count: any stream
	// under the events prefix will do. Without this the test below is red whenever the
	// account fails to come up, which is a different defect wearing this one's clothes.
	client := newClient(t, server.Addr())
	waitFor(t, "the connector to publish this account's state to some event stream", func() bool {
		return len(eventStreamsWritten(t, server)) > 0
	})

	// And only then that the client looks at the stream it was written to.
	var seen []string
	for _, event := range client.events(context.Background(), sid) {
		seen = append(seen, string(event.Type))
	}
	if !slices.Contains(seen, string(protocol.EventSessionState)) {
		stream, counted := client.eventsOf(context.Background(), sid)
		t.Fatalf("the client counted %d event streams, picked %s for %s and read %v from "+
			"it; the fleet recorded %s in wa:meta and wrote to %v.\n"+
			"The client and the connector disagree about how many event streams there are, "+
			"so the client reads a stream nobody wrote to: no error, no warning, an empty "+
			"list.",
			counted, stream, sid, seen, shardsRecordedBy(server), eventStreamsWritten(t, server))
	}
}

// shardsRecordedBy is the count the fleet wrote in `wa:meta`, read off the server instead
// of through the client, so a failure can print both numbers side by side even when the
// client's is the wrong one. Going through the client would print the same number twice
// and hide the disagreement that is the whole defect.
func shardsRecordedBy(server *miniredis.Miniredis) string {
	if recorded := server.HGet("wa:meta", "event_shards"); recorded != "" {
		return recorded
	}
	return "nothing"
}

// eventStreamsWritten is which event streams have anything in them, named without this
// side claiming to know how many there are. The point of the test above is that guessing
// that number is what goes wrong, so the diagnostic cannot guess it either.
func eventStreamsWritten(t *testing.T, server *miniredis.Miniredis) []string {
	t.Helper()
	var written []string
	for _, key := range server.Keys() {
		if strings.HasPrefix(key, "wa:events:") && !strings.HasSuffix(key, ":lease") {
			if entries, err := server.Stream(key); err == nil && len(entries) > 0 {
				written = append(written, key)
			}
		}
	}
	return written
}

// The count has to come from the instance being watched, not from this side's idea of
// what instances usually run on, and those two are indistinguishable while the instance
// runs on the default.
//
// That is the trap the first version of this fix walked into: reading `DefaultEventShards`
// instead of `wa:meta` passes every test in this package, because nothing here runs an
// instance at any other count. It breaks on the first deployment that sets
// `WAC_EVENT_SHARDS`, which is a supported thing to set and the reason the number is
// published in the first place.
//
// So the instance here runs at four, and the sid is picked to expose the difference:
// measured, it lands on stream 1 with four and on stream 5 with sixteen.
func TestTheCountComesFromTheInstanceBeingWatchedAndNotFromTheDefault(t *testing.T) {
	server := miniredis.RunT(t)
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "wa.db")

	const sid = "2f1c6f0e-0000-4000-8000-00000000000a"
	seedWantedConnected(t, dsn, sid, "551199999000a")

	start(t, server.Addr(), "inst-a", map[string]string{
		"WAC_DATABASE_URL": dsn, "WAC_EVENT_SHARDS": "4",
	})

	client := newClient(t, server.Addr())
	waitFor(t, "the connector to publish this account's state to some event stream", func() bool {
		return len(eventStreamsWritten(t, server)) > 0
	})

	var seen []string
	for _, event := range client.events(context.Background(), sid) {
		seen = append(seen, string(event.Type))
	}
	if !slices.Contains(seen, string(protocol.EventSessionState)) {
		stream, counted := client.eventsOf(context.Background(), sid)
		t.Fatalf("the instance recorded %s in wa:meta as its event stream count and wrote "+
			"to %v; the client counted %d, picked %s and read %v from it.\n"+
			"It is taking the count from somewhere other than this instance, which agrees "+
			"with it only while the instance runs on the default.",
			shardsRecordedBy(server), eventStreamsWritten(t, server), counted, stream, seen)
	}
}

// What the client does when the fleet has not said, which is the case that decides
// whether this is a fix or a second copy of the number wearing a disguise.
//
// A fallback here would pass every other test in this package: `wa:meta` is written by
// the first instance to start, so in a test that starts one there is always an answer.
// The cost lands in the shape the fallback was meant to avoid, a client reading a stream
// nobody wrote to and reporting an empty list, which is the defect this fixes.
//
// The last cell is the one that keeps the table honest: an answer that is there is used,
// so a `fleetShards` that failed on everything would not pass either.
func TestAFleetThatHasNotSaidHowManyStreamsItHasGetsNoAnswerInvented(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, server *miniredis.Miniredis)
		want  int
	}{
		{
			name:  "no meta at all, which is a fleet nothing has started in",
			setup: func(*testing.T, *miniredis.Miniredis) {},
		},
		{
			name: "meta without the field",
			setup: func(_ *testing.T, server *miniredis.Miniredis) {
				server.HSet("wa:meta", "protocol_min", "1")
			},
		},
		{
			name: "a count that is not a number",
			setup: func(_ *testing.T, server *miniredis.Miniredis) {
				server.HSet("wa:meta", "event_shards", "plenty")
			},
		},
		{
			name: "a count of zero, which names no stream",
			setup: func(_ *testing.T, server *miniredis.Miniredis) {
				server.HSet("wa:meta", "event_shards", "0")
			},
		},
		{
			name: "a count the fleet really recorded",
			setup: func(_ *testing.T, server *miniredis.Miniredis) {
				server.HSet("wa:meta", "event_shards", "4")
			},
			want: 4,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			tc.setup(t, server)
			client := newClient(t, server.Addr())

			shards, err := client.fleetShards(context.Background())
			if tc.want == 0 {
				if err == nil {
					t.Fatalf("the client answered %d streams where the fleet said nothing "+
						"usable. An invented count sends a session's events to a stream "+
						"nobody wrote to, and that reads as an empty list rather than an "+
						"error, which is exactly what #269 was.", shards)
				}
				return
			}
			if err != nil {
				t.Fatalf("the fleet recorded %d and the client refused it: %v", tc.want, err)
			}
			if shards != tc.want {
				t.Errorf("the client counts %d streams, the fleet records %d", shards, tc.want)
			}
		})
	}
}

// The answer a client keeps is the answer its own fleet gave.
//
// `fleetShards` caches, because without it every read of a session's events pays an
// `HGET` on top of the `XRANGE` it used to cost alone. The cache is on the client, and
// this is what says so: a package variable would pass every other test that starts one
// instance, and quietly hand this client's count to the next test's client. That is #269
// again, with the wrong number arriving from a neighbouring fleet instead of a literal,
// and it would land on whichever test happened to run second.
//
// Both fleets are planted by hand rather than started: the number under test is what the
// client reads out of `wa:meta`, and a connector would only be a slower way to write it.
// A is asked again at the end, because a cache that is merely last-write-wins passes the
// first two assertions and fails only when the earlier client is used after the later one.
func TestOneClientsCountDoesNotBecomeAnothersCount(t *testing.T) {
	ctx := context.Background()

	fleetA := miniredis.RunT(t)
	fleetA.HSet("wa:meta", "event_shards", "4")
	fleetB := miniredis.RunT(t)
	fleetB.HSet("wa:meta", "event_shards", "8")

	clientA := newClient(t, fleetA.Addr())
	clientB := newClient(t, fleetB.Addr())

	const sid = "2f1c6f0e-0000-4000-8000-000000000002"
	for _, step := range []struct {
		who    string
		client *client
		fleet  *miniredis.Miniredis
		want   int
	}{
		{who: "A", client: clientA, fleet: fleetA, want: 4},
		{who: "B", client: clientB, fleet: fleetB, want: 8},
		{who: "A again, after B has asked", client: clientA, fleet: fleetA, want: 4},
	} {
		got, err := step.client.fleetShards(ctx)
		if err != nil {
			t.Fatalf("client %s could not read its own fleet's count: %v", step.who, err)
		}
		if got != step.want {
			t.Errorf("client %s counts %d event streams; its fleet recorded %s.\n"+
				"The count is being kept somewhere both clients can see, so one fleet's "+
				"answer reaches a client watching the other.",
				step.who, got, shardsRecordedBy(step.fleet))
		}
		// `step.want` and not a literal on purpose, and not a hole in the fence either:
		// this is the oracle, built from what this test itself wrote into that fleet's
		// `wa:meta`, and it is red the moment the client names a different stream.
		stream, counted := step.client.eventsOf(ctx, sid)
		if want := redisx.NewKeys("wa:", step.want).EventsOf(sid); stream != want {
			t.Errorf("client %s picked %s with a count of %d; its fleet's %d streams put "+
				"%s on %s", step.who, stream, counted, step.want, sid, want)
		}
	}
}

// Asking the fleet its stream count is paid once, not once per read.
//
// Before #269 a read of a session's events was one `XRANGE` and nothing else, because the
// count was a literal in this file. Taking it from `wa:meta` instead is the fix, and the
// obvious way of writing it puts an `HGET` in front of every single read: correct, and a
// cost that multiplies with nothing to show for it, since the count is written once per
// fleet and `ClaimMeta` refuses to start an instance that disagrees with it.
//
// Nothing else in this package would notice that regression -- the reads all pass either
// way -- so this counts the commands the server actually served. The fleet is planted by
// hand and no connector runs, so every command counted belongs to the client.
func TestAskingTheFleetItsStreamCountIsPaidOnce(t *testing.T) {
	server := miniredis.RunT(t)
	server.HSet("wa:meta", "event_shards", "16")
	client := newClient(t, server.Addr())
	ctx := context.Background()

	const sid = "2f1c6f0e-0000-4000-8000-000000000002"
	client.events(ctx, sid)
	afterFirst := server.Server().TotalCommands()
	client.events(ctx, sid)
	second := server.Server().TotalCommands() - afterFirst

	// One: the `XRANGE` the read was always made of. Two means the `HGET` came with it.
	if second > 1 {
		t.Errorf("a second read of the same session's events cost %d commands; before "+
			"#269 it cost one.\n"+
			"The fleet's stream count is being re-read on every call. It is written once "+
			"per fleet and an instance that disagrees with it is refused at startup, so "+
			"there is nothing for the extra round trip to catch.", second)
	}
}
