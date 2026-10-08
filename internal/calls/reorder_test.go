package calls

import (
	"slices"
	"testing"

	"github.com/pion/rtp"
)

func TestTheBrowsersRTPIsPutBackInSequence(t *testing.T) {
	t.Parallel()

	// -1 in the output is a packet given up as lost.
	for name, test := range map[string]struct {
		in   []uint16
		want []int
	}{
		"in order":            {[]uint16{10, 11, 12}, []int{10, 11, 12}},
		"two swapped":         {[]uint16{10, 12, 11, 13}, []int{10, 11, 12, 13}},
		"a late repeat":       {[]uint16{10, 11, 10, 12}, []int{10, 11, 12}},
		"one late, in time":   {[]uint16{10, 12, 13, 14, 15, 11}, []int{10, 11, 12, 13, 14, 15}},
		"one lost":            {[]uint16{10, 12, 13, 14, 15, 16}, []int{10, -1, 12, 13, 14, 15, 16}},
		"lost, then it comes": {[]uint16{10, 12, 13, 14, 15, 16, 11}, []int{10, -1, 12, 13, 14, 15, 16}},
		"across the wrap":     {[]uint16{65534, 0, 65535, 1}, []int{65534, 65535, 0, 1}},
		// Late ones are dropped, not held: held, five of them would read as a gap ahead.
		"late ones do not pile up": {[]uint16{10, 11, 12, 5, 6, 7, 8, 9, 13}, []int{10, 11, 12, 13}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var order reorder
			var got []int
			for _, seq := range test.in {
				for _, ready := range order.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: seq}}) {
					if ready == nil {
						got = append(got, -1)
						continue
					}
					got = append(got, int(ready.SequenceNumber))
				}
			}
			if !slices.Equal(got, test.want) {
				t.Fatalf("pushed %v, got %v, want %v", test.in, got, test.want)
			}
		})
	}
}

// What the browser says reaches WhatsApp in the order it was said, whatever order the
// packets arrived in, and a lost one leaves its silence.
func TestWhatTheBrowserSaysIsQueuedInSequence(t *testing.T) {
	t.Parallel()
	leg := &Leg{}
	var order reorder
	// Each packet is one µ-law value, so where each one landed can be read back.
	packet := func(seq uint16, value byte) *rtp.Packet {
		return &rtp.Packet{Header: rtp.Header{SequenceNumber: seq}, Payload: []byte{value, value}}
	}
	for _, p := range []*rtp.Packet{packet(1, 0x10), packet(3, 0x30), packet(2, 0x20)} {
		leg.take(&order, &pcmuDecoder{}, p)
	}
	// Each two-byte payload decodes to four samples, the second and fourth the value.
	var got []int16
	for i := 3; i < len(leg.toWA); i += 4 {
		got = append(got, leg.toWA[i])
	}
	want := []int16{ulawDecode(0x10), ulawDecode(0x20), ulawDecode(0x30)}
	if !slices.Equal(got, want) {
		t.Fatalf("queued %v, want %v in the order they were said", got, want)
	}
}
