package redisx

import "testing"

// The lag Redis did not give is the one an operator must not be told is zero: a panel
// reading "up to date" is the answer somebody stops looking after.
func TestAnUncomputableLagIsNotReportedAsZero(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		fields map[string]any
		want   float64
		known  bool
	}{
		{name: "a client that is up to date", fields: map[string]any{"lag": int64(0)}, want: 0, known: true},
		{name: "a client that is behind", fields: map[string]any{"lag": int64(41)}, want: 41, known: true},
		{name: "a lag redis could not work out", fields: map[string]any{"lag": nil}, known: false},
		{name: "any negative redis might answer", fields: map[string]any{"lag": int64(-7)}, known: false},
		// Redis before 7.0 has no lag field at all (#320).
		{name: "a server that does not report a lag", fields: map[string]any{"pending": int64(0)}, known: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, known := lagOf(tc.fields)
			if known != tc.known {
				t.Fatalf("lagOf(%v) known = %v, want %v", tc.fields, known, tc.known)
			}
			if known && got != tc.want {
				t.Fatalf("lagOf(%v) = %v, want %v", tc.fields, got, tc.want)
			}
		})
	}
}

// A group's fields arrive as a map under RESP3 and as a flat list under RESP2, and a
// reader of one shape finds no lag in the other: every group would read as unknown.
func TestAGroupsFieldsAreReadUnderBothProtocols(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		item any
	}{
		{name: "resp3", item: map[any]any{"name": "chatwoot", "lag": int64(3)}},
		{name: "resp2", item: []any{"name", "chatwoot", "lag", int64(3)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fields, ok := fieldsOf(tc.item)
			if !ok {
				t.Fatalf("fieldsOf(%v) refused the reply", tc.item)
			}
			if fields["name"] != "chatwoot" || fields["lag"] != int64(3) {
				t.Fatalf("fieldsOf(%v) = %v", tc.item, fields)
			}
		})
	}
	if _, ok := fieldsOf("OK"); ok {
		t.Fatal("a reply that is not a group was read as one")
	}
}
