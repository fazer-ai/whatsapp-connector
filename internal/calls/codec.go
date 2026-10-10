package calls

import (
	"math"

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

// pcmuEncoder narrows 16 kHz to 8 kHz: the voice is low-passed below 4 kHz first, or
// what WhatsApp carries above it folds back into the band as a tone that was never said.
type pcmuEncoder struct{ filter halfBand }

func (e *pcmuEncoder) encode(pcm []int16) []byte {
	out := make([]byte, 0, len(pcm)/2)
	for i, s := range pcm {
		y := e.filter.push(float32(s))
		if i%2 == 1 {
			out = append(out, ulawEncode(clip16(y)))
		}
	}
	return out
}

// pcmuDecoder widens 8 kHz to 16 kHz: a zero between each pair of samples, low-passed.
// Without the filter, or with a straight line between neighbours, a mirror of the voice
// stays above 4 kHz and a person hears it as a whistle over everything.
type pcmuDecoder struct{ filter halfBand }

func (d *pcmuDecoder) decode(payload []byte) []int16 {
	out := make([]int16, 0, len(payload)*2)
	for _, u := range payload {
		// Doubled, since half the samples going into the filter are the zeros.
		out = append(out, clip16(d.filter.push(2*float32(ulawDecode(u)))), clip16(d.filter.push(0)))
	}
	return out
}

// lowPassTaps is a windowed-sinc low-pass at 16 kHz with its cutoff at 3.7 kHz, under
// the 4 kHz that 8 kHz can carry. Sixty-three taps put the stopband past 4.4 kHz, at
// two milliseconds of delay.
const lowPassLen = 63

var lowPassTaps = func() []float32 {
	const n, cutoff = lowPassLen, 3700.0 / sampleRate
	taps := make([]float32, n)
	var sum float64
	for i := range taps {
		x := float64(i) - (n-1)/2.0
		h := 2 * cutoff
		if x != 0 {
			h = math.Sin(2*math.Pi*cutoff*x) / (math.Pi * x)
		}
		// Blackman window.
		w := 0.42 - 0.5*math.Cos(2*math.Pi*float64(i)/(n-1)) + 0.08*math.Cos(4*math.Pi*float64(i)/(n-1))
		taps[i] = float32(h * w)
		sum += h * w
	}
	for i := range taps {
		taps[i] /= float32(sum)
	}
	return taps
}()

// halfBand runs lowPassTaps over a stream of 16 kHz samples, one at a time. It keeps its
// history between packets, so the filter does not restart on every 20 ms.
type halfBand struct {
	history [lowPassLen]float32
	at      int
}

func (f *halfBand) push(x float32) float32 {
	f.history[f.at] = x
	var y float32
	for k, tap := range lowPassTaps {
		y += tap * f.history[(f.at-k+len(f.history))%len(f.history)]
	}
	f.at = (f.at + 1) % len(f.history)
	return y
}

func clip16(x float32) int16 {
	return int16(max(-maxAmplitude-1, min(maxAmplitude, x)))
}

func codecFor(mime string) (encoder, decoder, bool) {
	switch mime {
	case webrtc.MimeTypeG722:
		return g722Encoder{g722.NewEncoder(g722BitRate, 0)}, &g722Decoder{dec: g722.NewDecoder(g722BitRate, 0)}, true
	case webrtc.MimeTypePCMU:
		return &pcmuEncoder{}, &pcmuDecoder{}, true
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
