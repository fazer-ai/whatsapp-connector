package calls

import (
	"errors"
	"sync"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// audioTrack is the connector's voice to the browser, in whichever of the two codecs the
// negotiation settled on.
//
// Its own TrackLocal rather than pion's static sample track, because that one is built for
// one codec and refuses a negotiation that chose another: a track made for G.722 cannot
// bind to a browser that answered with PCMU only, and which one the browser picks is only
// known once it has answered.
type audioTrack struct {
	mu      sync.Mutex
	bound   bool
	mime    string
	pt      uint8
	ssrc    uint32
	stream  webrtc.TrackLocalWriter
	seq     uint16
	tick    uint32
	started bool
}

var errNoCodecInCommon = errors.New("calls: the browser accepted neither G.722 nor PCMU")

func (t *audioTrack) Bind(ctx webrtc.TrackLocalContext) (webrtc.RTPCodecParameters, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// The negotiated list, in the order the answer put it: the first one this side can
	// speak is the one both ends agreed to prefer.
	for _, codec := range ctx.CodecParameters() {
		if _, _, ok := codecFor(codec.MimeType); !ok {
			continue
		}
		mime, _ := canonical(codec.MimeType)
		t.bound, t.mime, t.pt, t.ssrc, t.stream = true, mime, uint8(codec.PayloadType), uint32(ctx.SSRC()), ctx.WriteStream()
		return codec, nil
	}
	return webrtc.RTPCodecParameters{}, errNoCodecInCommon
}

func (t *audioTrack) Unbind(webrtc.TrackLocalContext) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.bound, t.stream = false, nil
	return nil
}

func (t *audioTrack) ID() string       { return "audio" }
func (t *audioTrack) RID() string      { return "" }
func (t *audioTrack) StreamID() string { return "whatsapp" }
func (t *audioTrack) Kind() webrtc.RTPCodecType {
	return webrtc.RTPCodecTypeAudio
}

// codec is the negotiated codec, empty until the track is bound.
func (t *audioTrack) codec() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mime
}

// write sends one 20 ms packet. The timestamp advances by one packet whether or not the
// previous write reached the wire, so the browser's clock follows wall time.
func (t *audioTrack) write(payload []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.bound {
		return nil
	}
	header := &rtp.Header{
		Version:        2,
		PayloadType:    t.pt,
		SequenceNumber: t.seq,
		Timestamp:      t.tick,
		SSRC:           t.ssrc,
		// The first packet of a talkspurt, which for a stream that never stops is only
		// the first one.
		Marker: !t.started,
	}
	t.seq++
	t.tick += packetTicks
	t.started = true
	_, err := t.stream.WriteRTP(header, payload)
	return err
}
