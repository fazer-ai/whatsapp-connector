package app

import (
	"testing"

	"github.com/alicebob/miniredis/v2"

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
