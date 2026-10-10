package calls

import (
	"math"
	"testing"
)

// power is the energy of pcm at freq, by Goertzel, at the given sample rate.
func power(pcm []float64, freq, rate float64) float64 {
	w := 2 * math.Pi * freq / rate
	var s1, s2 float64
	for _, x := range pcm {
		s1, s2 = x+2*math.Cos(w)*s1-s2, s1
	}
	return s1*s1 + s2*s2 - 2*math.Cos(w)*s1*s2
}

// sine is n samples of a sine at freq, at the given rate and amplitude.
func sine(n int, freq, rate, amplitude float64) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = amplitude * math.Sin(2*math.Pi*freq*float64(i)/rate)
	}
	return out
}

func decibels(signal, other float64) float64 { return 10 * math.Log10(signal/other) }

// PCMU runs at 8 kHz and the WhatsApp side at 16 kHz, so a PCMU call is converted both
// ways. A conversion that does not filter leaves a mirror of the voice above 4 kHz on the
// way up, which a person hears as a whistle over everything (measured on a real phone:
// "estridente"), and folds what is above 4 kHz back into the band on the way down. Each
// direction is held to keeping what it did not filter out at least 50 dB below the tone.
func TestAPCMUCallIsConvertedWithoutAMirrorOrAFold(t *testing.T) {
	t.Parallel()

	t.Run("8 kHz to 16 kHz leaves no mirror of the tone", func(t *testing.T) {
		t.Parallel()
		in := sine(8000, 440, rtpClock, 12000)
		payload := make([]byte, len(in))
		for i, x := range in {
			payload[i] = ulawEncode(int16(x))
		}
		_, dec, _ := codecFor("audio/PCMU")
		var out []float64
		for i := 0; i < len(payload); i += packetTicks {
			for _, s := range dec.decode(payload[i : i+packetTicks]) {
				out = append(out, float64(s))
			}
		}
		if len(out) != 2*len(in) {
			t.Fatalf("decoded %d samples from %d, want twice as many", len(out), len(in))
		}
		out = out[sampleRate/10:] // past whatever the filter takes to settle
		assertSameLevel(t, out, sine(len(out), 440, sampleRate, 12000), 440, sampleRate)
		mirror := sampleRate - rtpClock - 440.0
		if got := decibels(power(out, 440, sampleRate), power(out, mirror, sampleRate)); got < 50 {
			t.Fatalf("the mirror at %.0f Hz is %.1f dB below the tone, want at least 50", mirror, got)
		}
	})

	t.Run("16 kHz to 8 kHz does not fold what is above 4 kHz into the band", func(t *testing.T) {
		t.Parallel()
		const high = 7000.0 // folds onto 1 kHz at 8 kHz
		run := func(freq float64) []float64 {
			enc, _, _ := codecFor("audio/PCMU")
			in := sine(sampleRate, freq, sampleRate, 12000)
			var out []float64
			for i := 0; i < len(in); i += packetSamples {
				pcm := make([]int16, packetSamples)
				for j := range pcm {
					pcm[j] = int16(in[i+j])
				}
				for _, u := range enc.encode(pcm) {
					out = append(out, float64(ulawDecode(u)))
				}
			}
			return out[rtpClock/10:]
		}
		inBand, folded := run(1000), run(high)
		assertSameLevel(t, inBand, sine(len(inBand), 1000, rtpClock, 12000), 1000, rtpClock)
		if got := decibels(power(inBand, 1000, rtpClock), power(folded, rtpClock-high, rtpClock)); got < 50 {
			t.Fatalf("a %.0f Hz tone comes out at 1 kHz %.1f dB below a 1 kHz tone, want at least 50", high, got)
		}
	})
}

// assertSameLevel fails when the tone does not come through at the level it went in at,
// within 1 dB: a conversion that loses the voice has no mirror to measure either.
func assertSameLevel(t *testing.T, got, want []float64, freq, rate float64) {
	t.Helper()
	if diff := math.Abs(decibels(power(got, freq, rate), power(want, freq, rate))); !(diff <= 1) {
		t.Fatalf("the %.0f Hz tone came through %.1f dB off the level it went in at", freq, diff)
	}
}
