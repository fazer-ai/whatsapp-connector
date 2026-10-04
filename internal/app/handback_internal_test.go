package app

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// What the stop says about its sessions, for every mix of hand-backs that landed and ones
// that did not. The end-to-end tests cover a stop with Redis up; a stop that cannot reach
// Redis waits out the whole grace period, so its arm is pinned here, against the counts.
func TestReportHandBackSaysWhatTheStopDidWithTheSessions(t *testing.T) {
	t.Parallel()

	type line struct {
		Level    string
		Message  string
		Sessions int
	}
	cases := []struct {
		name                string
		stopped, unreturned int
		want                []line
	}{
		{"nothing to hand back", 0, 0, []line{{"info", "no sessions to hand back", 0}}},
		{"all handed back", 2, 0, []line{{"info", "handed the sessions back", 2}}},
		{"none reached redis", 2, 2, []line{
			{"warn", "could not hand every session back; those leases expire on their own", 2},
		}},
		{"some reached redis", 3, 1, []line{
			{"info", "handed the sessions back", 2},
			{"warn", "could not hand every session back; those leases expire on their own", 1},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			said := &bytes.Buffer{}
			c := &Connector{log: zerolog.New(said)}
			c.reportHandBack(tc.stopped, tc.unreturned)

			var got []line
			for _, raw := range strings.Split(strings.TrimSpace(said.String()), "\n") {
				var l line
				if err := json.Unmarshal([]byte(raw), &l); err != nil {
					t.Fatalf("not JSON: %q: %v", raw, err)
				}
				got = append(got, l)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("line %d: got %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}
