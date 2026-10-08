package calls

import "github.com/pion/rtp"

// reorderDepth is how many packets from the browser may wait for one that is missing,
// 80 ms at 20 ms a packet. Past that the missing one is taken as lost: waiting longer
// delays every word after it for a packet that is not coming.
const reorderDepth = 4

// reorder puts the browser's RTP back in sequence before it is decoded. G.722 decodes
// each packet from the state the previous one left, so a packet decoded out of turn
// corrupts the ones after it as well as playing in the wrong place, and a packet that
// never arrives has to leave its gap in the audio rather than close it up.
type reorder struct {
	next    uint16
	started bool
	held    map[uint16]*rtp.Packet
}

// push takes one packet as it arrived and returns, in sequence, the packets now ready to
// decode. A nil in the result is a packet given up as lost, whose place is silence. A
// packet older than the sequence already played, a late or repeated one, is dropped.
func (r *reorder) push(packet *rtp.Packet) []*rtp.Packet {
	if !r.started {
		r.started, r.next = true, packet.SequenceNumber
		r.held = make(map[uint16]*rtp.Packet, reorderDepth+1)
	}
	if behind(packet.SequenceNumber, r.next) {
		return nil
	}
	r.held[packet.SequenceNumber] = packet
	var ready []*rtp.Packet
	for {
		if next, ok := r.held[r.next]; ok {
			delete(r.held, r.next)
			ready = append(ready, next)
			r.next++
			continue
		}
		if len(r.held) <= reorderDepth {
			return ready
		}
		ready = append(ready, nil)
		r.next++
	}
}

// behind is whether sequence number a comes before b, across the wrap at 65535.
func behind(a, b uint16) bool { return int16(a-b) < 0 } //nolint:gosec // the wrap is the point
