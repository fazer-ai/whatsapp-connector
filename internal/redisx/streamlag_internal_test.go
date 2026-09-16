package redisx

import "testing"

// The lag Redis could not compute is the one an operator must not be told is zero: a
// panel reading "up to date" is the answer somebody stops looking after.
func TestAnUncomputableLagIsNotReportedAsZero(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		reported int64
		want     float64
		known    bool
	}{
		{name: "a client that is up to date", reported: 0, want: 0, known: true},
		{name: "a client that is behind", reported: 41, want: 41, known: true},
		{name: "a lag redis could not work out", reported: -1, known: false},
		{name: "any other negative redis might answer", reported: -7, known: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, known := lagOf(tc.reported)
			if known != tc.known {
				t.Fatalf("lagOf(%d) known = %v, want %v", tc.reported, known, tc.known)
			}
			if known && got != tc.want {
				t.Fatalf("lagOf(%d) = %v, want %v", tc.reported, got, tc.want)
			}
		})
	}
}
