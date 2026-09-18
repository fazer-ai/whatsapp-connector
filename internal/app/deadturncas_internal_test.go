package app

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// The takeover is a compare-and-set, and this is the table that makes it one.
//
// Between reading who holds a turn and replacing it, the mark can expire and be taken by a
// third instance that is alive and working. Stamping over that one is the collision the
// mark exists to prevent, arrived at by the code written to repair it -- so the write only
// lands while the turn still reads as the name that was found holding it.
//
// Driven against the script rather than through a sweep, because what separates the two
// cells is an interleaving no test can schedule: the second cell is the state that race
// leaves behind, written down directly.
func TestATurnIsOnlyTakenWhileItStillReadsAsTheOneThatWasFound(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		holdsNow string
		want     int
		wantMark string
	}{
		{
			name:     "still the instance that was found gone",
			holdsNow: "inst-dead",
			want:     1,
			wantMark: "inst-taking",
		},
		{
			name: "taken by somebody else in between",
			// The state the race leaves: the mark expired after the read, and a live
			// instance took the turn and is bringing the account up behind it.
			holdsNow: "inst-third",
			want:     0,
			wantMark: "inst-third",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := miniredis.RunT(t)
			client, err := redisx.New(redisx.Config{
				URL: "redis://" + server.Addr(), Prefix: "wa:", Shards: 8,
			})
			if err != nil {
				t.Fatalf("dial the test redis: %v", err)
			}
			t.Cleanup(func() { _ = client.Close() })

			const sid = "sid-1"
			mark := client.Keys().Resume(sid)
			if err := server.Set(mark, tc.holdsNow); err != nil {
				t.Fatalf("set the turn: %v", err)
			}

			took, err := takeTurnFromGone.Run(
				t.Context(), client, []string{mark}, "inst-dead", "inst-taking", int64(60000),
			).Int()
			if err != nil {
				t.Fatalf("run the takeover: %v", err)
			}
			if took != tc.want {
				t.Errorf("takeover returned %d, want %d", took, tc.want)
			}
			held, err := server.Get(mark)
			if err != nil {
				t.Fatalf("read the turn back: %v", err)
			}
			if held != tc.wantMark {
				t.Errorf("the turn now reads %q, want %q", held, tc.wantMark)
			}
		})
	}
}

// The whole table the predicate decides, including the cells that answer "no", because
// three quite different states come out as "leave it alone" and only one of them is the
// one a sweep meets most often.
//
// The cell that nothing else reaches is "mine, and my own registry entry is not there".
// The early return on one's own name looks like a shortcut for the liveness read that
// follows it, and on a healthy instance it is: the entry is there, the instance reads as
// alive, and both paths answer no. It stops being a shortcut the moment a beat has not
// landed yet -- a instance that has just come up, a Redis that blinked, a heartbeat the
// scheduler has not run. Without the early return, such an instance reads ITSELF as gone,
// takes its own turn, and dials an account it decided a moment ago to leave alone: the
// retry floor removed by the code written to repair a different hole in it.
func TestWhoseTurnItIsDecidesWhetherItCanBeTaken(t *testing.T) {
	t.Parallel()

	const me = "inst-mine"
	for _, tc := range []struct {
		name string
		// holder is the name on the turn; empty means no turn at all.
		holder string
		// announced are the instances with a registry entry.
		announced []string
		want      bool
		wantMark  string
	}{
		{
			name:      "nobody holds it any more",
			holder:    "",
			announced: []string{me},
			want:      false,
		},
		{
			name:      "mine, and I am announced",
			holder:    me,
			announced: []string{me},
			want:      false,
			wantMark:  me,
		},
		{
			name: "mine, and my own beat has not landed",
			// Not a licence to take it: a turn in this instance's own name is its own
			// previous attempt far more often than it is a predecessor under a pinned
			// WAC_INSTANCE, and taking it is the cool-off gone.
			holder:    me,
			announced: nil,
			want:      false,
			wantMark:  me,
		},
		{
			name:      "a peer's, and the peer is announced",
			holder:    "inst-peer",
			announced: []string{me, "inst-peer"},
			want:      false,
			wantMark:  "inst-peer",
		},
		{
			name:      "a peer's, and the peer is gone from the fleet",
			holder:    "inst-peer",
			announced: []string{me},
			want:      true,
			wantMark:  me,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := miniredis.RunT(t)
			client, err := redisx.New(redisx.Config{
				URL: "redis://" + server.Addr(), Prefix: "wa:", Shards: 8,
			})
			if err != nil {
				t.Fatalf("dial the test redis: %v", err)
			}
			t.Cleanup(func() { _ = client.Close() })

			const sid = "sid-1"
			mark := client.Keys().Resume(sid)
			if tc.holder != "" {
				if err := server.Set(mark, tc.holder); err != nil {
					t.Fatalf("set the turn: %v", err)
				}
			}
			for _, instance := range tc.announced {
				server.HSet(client.Keys().Instance(instance), "version", "test")
			}

			connector := &Connector{
				cfg: Config{Instance: me}, log: zerolog.Nop(), client: client,
			}
			if got := connector.tookTurnFromAnInstanceThatIsGone(t.Context(), sid); got != tc.want {
				t.Errorf("took the turn = %v, want %v", got, tc.want)
			}

			held, err := server.Get(mark)
			if tc.wantMark == "" {
				if err == nil {
					t.Errorf("a turn was written where there was none, reading %q", held)
				}
				return
			}
			if err != nil {
				t.Fatalf("read the turn back: %v", err)
			}
			if held != tc.wantMark {
				t.Errorf("the turn now reads %q, want %q", held, tc.wantMark)
			}
		})
	}
}
