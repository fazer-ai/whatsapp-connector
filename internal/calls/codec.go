package calls

import (
	"strings"

	"github.com/gotranspile/g722"
	"github.com/pion/webrtc/v4"
)

const (
	// sampleRate is meowcaller's PCM rate on the WhatsApp side, and G.722's real rate.
	sampleRate = 16000
	// frameSamples is one meowcaller frame, 60 ms.
	frameSamples = 960
	// packetSamples is one packet to the browser, 20 ms at 16 kHz.
	packetSamples = sampleRate / 50
	// rtpClock is the RTP clock of both browser codecs. G.722 samples at 16 kHz and
	// still runs an 8 kHz RTP clock, by an error RFC 3551 kept for compatibility, so 20 ms
	// is 160 ticks for either of them.
	rtpClock     = 8000
	packetTicks  = rtpClock / 50
	g722BitRate  = 64000
	payloadG722  = 9
	payloadPCMU  = 0
	maxAmplitude = 32767
)

// browserCodecs is what the connector offers and accepts, in the order it prefers them.
// G.722 first: it is wideband at the WhatsApp side's own rate, so nothing is resampled.
// PCMU is the fallback every browser has, at half the bandwidth.
var browserCodecs = []webrtc.RTPCodecParameters{
	{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeG722, ClockRate: rtpClock}, PayloadType: payloadG722},
	{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMU, ClockRate: rtpClock}, PayloadType: payloadPCMU},
}

// encoder turns 20 ms of 16 kHz PCM into one packet's payload.
type encoder interface {
	encode(pcm []int16) []byte
}

// decoder turns one packet's payload into 16 kHz PCM.
type decoder interface {
	decode(payload []byte) []int16
}

type g722Encoder struct{ enc *g722.Encoder }

func (e g722Encoder) encode(pcm []int16) []byte {
	out := make([]byte, len(pcm)/2)
	return out[:e.enc.Encode(out, pcm)]
}

type g722Decoder struct {
	dec *g722.Decoder
	buf []int16
}

func (d *g722Decoder) decode(payload []byte) []int16 {
	// Two samples per byte at 64 kbit/s.
	if need := len(payload) * 2; cap(d.buf) < need {
		d.buf = make([]int16, need)
	}
	return d.buf[:d.dec.Decode(d.buf[:cap(d.buf)], payload)]
}

// pcmuEncoder narrows 16 kHz to 8 kHz by averaging each pair, which is a crude low-pass
// and enough for a fallback whose band ends at 4 kHz anyway.
type pcmuEncoder struct{}

func (pcmuEncoder) encode(pcm []int16) []byte {
	out := make([]byte, len(pcm)/2)
	for i := range out {
		out[i] = ulawEncode(int16((int32(pcm[2*i]) + int32(pcm[2*i+1])) / 2)) //nolint:gosec // the mean of two int16 is an int16
	}
	return out
}

// pcmuDecoder widens 8 kHz to 16 kHz by putting the midpoint between each pair.
type pcmuDecoder struct{ last int16 }

func (d *pcmuDecoder) decode(payload []byte) []int16 {
	out := make([]int16, 0, len(payload)*2)
	for _, u := range payload {
		v := ulawDecode(u)
		out = append(out, int16((int32(d.last)+int32(v))/2), v) //nolint:gosec // the mean of two int16 is an int16
		d.last = v
	}
	return out
}

// canonical is a codec's MIME type as this package spells it. SDP compares them without
// case, pion keeps whatever spelling the other side wrote, and a browser writing `pcmu` is
// still speaking PCMU.
func canonical(mime string) (string, bool) {
	for _, codec := range browserCodecs {
		if strings.EqualFold(mime, codec.MimeType) {
			return codec.MimeType, true
		}
	}
	return "", false
}

func codecFor(mime string) (encoder, decoder, bool) {
	mime, _ = canonical(mime)
	switch mime {
	case webrtc.MimeTypeG722:
		return g722Encoder{g722.NewEncoder(g722BitRate, 0)}, &g722Decoder{dec: g722.NewDecoder(g722BitRate, 0)}, true
	case webrtc.MimeTypePCMU:
		return pcmuEncoder{}, &pcmuDecoder{}, true
	}
	return nil, nil, false
}

// toPCM16 converts meowcaller's float samples, clipping what is out of range.
func toPCM16(dst []int16, frame []float32) []int16 {
	for _, f := range frame {
		f = max(-1, min(1, f))
		dst = append(dst, int16(f*maxAmplitude))
	}
	return dst
}

// ulawDecode is G.711 µ-law expansion.
func ulawDecode(u byte) int16 {
	u = ^u
	t := (int16(u&0x0f) << 3) + 0x84
	t <<= (u & 0x70) >> 4
	if u&0x80 != 0 {
		return 0x84 - t
	}
	return t - 0x84
}

// ulawEncode is G.711 µ-law compression.
func ulawEncode(s int16) byte {
	const bias, clip = 0x84, 32635
	sign := byte(0)
	v := int(s)
	if v < 0 {
		v, sign = -v, 0x80
	}
	v = min(v, clip) + bias
	exp := byte(7)
	for mask := 0x4000; v&mask == 0 && exp > 0; mask >>= 1 {
		exp--
	}
	mant := byte((v >> (exp + 3)) & 0x0f)
	return ^(sign | exp<<4 | mant)
}
