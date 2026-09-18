package app_test

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
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
		t.Fatalf("the client read %v from the stream it picked for %s, and the fleet wrote "+
			"to %v.\nThe client and the connector disagree about how many event streams "+
			"there are, so the client reads a stream nobody wrote to: no error, no warning, "+
			"an empty list.",
			seen, sid, eventStreamsWritten(t, server))
	}
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
// measured, it lands on stream 1 with four and on stream 9 with sixteen.
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
		t.Fatalf("the instance runs on four event streams and wrote to %v, and the client "+
			"read %v from the stream it picked.\n"+
			"It is taking the count from somewhere other than this instance, which agrees "+
			"with it only while the instance runs on the default.",
			eventStreamsWritten(t, server), seen)
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
