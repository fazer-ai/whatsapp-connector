package calls

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/testwait"
)

// browser is a second pion peer in the same process, standing in for the agent's browser:
// it speaks a tone and records what it hears.
type browser struct {
	t     *testing.T
	pc    *webrtc.PeerConnection
	track *webrtc.TrackLocalStaticSample

	mu       sync.Mutex
	heard    []int16
	packets  int
	codec    string
	arrivals chan struct{}
}

func newBrowser(t *testing.T, mimes ...string) *browser {
	t.Helper()
	codecs := &webrtc.MediaEngine{}
	for _, codec := range browserCodecs {
		for _, mime := range mimes {
			if codec.MimeType == mime {
				if err := codecs.RegisterCodec(codec, webrtc.RTPCodecTypeAudio); err != nil {
					t.Fatalf("register %s: %v", mime, err)
				}
			}
		}
	}
	settings := webrtc.SettingEngine{}
	settings.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	api := webrtc.NewAPI(webrtc.WithMediaEngine(codecs), webrtc.WithSettingEngine(settings))
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("browser peer: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	track, err := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: mimes[0], ClockRate: rtpClock}, "mic", "browser")
	if err != nil {
		t.Fatalf("browser track: %v", err)
	}
	if _, err := pc.AddTrack(track); err != nil {
		t.Fatalf("add browser track: %v", err)
	}
	b := &browser{t: t, pc: pc, track: track, arrivals: make(chan struct{}, 1024)}
	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		_, dec, ok := codecFor(remote.Codec().MimeType)
		if !ok {
			return
		}
		b.mu.Lock()
		b.codec = remote.Codec().MimeType
		b.mu.Unlock()
		for {
			packet, _, err := remote.ReadRTP()
			if err != nil {
				return
			}
			pcm := dec.decode(packet.Payload)
			b.mu.Lock()
			b.heard = append(b.heard, pcm...)
			b.packets++
			b.mu.Unlock()
			select {
			case b.arrivals <- struct{}{}:
			default:
			}
		}
	})
	return b
}

func (b *browser) gathered() string {
	b.t.Helper()
	select {
	case <-webrtc.GatheringCompletePromise(b.pc):
	case <-time.After(gatherTimeout):
		b.t.Fatal("the browser peer never finished gathering")
	}
	return b.pc.LocalDescription().SDP
}

// answer takes the connector's offer and returns the browser's answer.
func (b *browser) answer(offer string) string {
	b.t.Helper()
	if err := b.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}); err != nil {
		b.t.Fatalf("browser applies the offer: %v", err)
	}
	answer, err := b.pc.CreateAnswer(nil)
	if err == nil {
		err = b.pc.SetLocalDescription(answer)
	}
	if err != nil {
		b.t.Fatalf("browser answers: %v", err)
	}
	return b.gathered()
}

// offer is the browser's offer for a call it places.
func (b *browser) offer() string {
	b.t.Helper()
	offer, err := b.pc.CreateOffer(nil)
	if err == nil {
		err = b.pc.SetLocalDescription(offer)
	}
	if err != nil {
		b.t.Fatalf("browser offers: %v", err)
	}
	return b.gathered()
}

func (b *browser) accept(answer string) {
	b.t.Helper()
	if err := b.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		b.t.Fatalf("browser applies the answer: %v", err)
	}
}

// speak sends a 440 Hz tone for as long as ctx lives, 20 ms at a time, the way a
// browser's microphone does.
func (b *browser) speak(ctx context.Context) {
	enc, _, _ := codecFor(b.track.Codec().MimeType)
	go func() {
		ticker := time.NewTicker(packetInterval)
		defer ticker.Stop()
		for n := 0; ; n++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			_ = b.track.WriteSample(media.Sample{Data: enc.encode(tone(n*packetSamples, packetSamples)), Duration: packetInterval})
		}
	}()
}

// waitPackets blocks until the browser has received n packets from the connector.
func (b *browser) waitPackets(n int) {
	b.t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		b.mu.Lock()
		got := b.packets
		b.mu.Unlock()
		if got >= n {
			return
		}
		select {
		case <-b.arrivals:
		case <-deadline:
			b.t.Fatalf("the browser received %d packets from the connector, want %d", got, n)
		}
	}
}

func (b *browser) heardSoFar() []int16 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int16(nil), b.heard...)
}

// tone is n samples of 440 Hz at half scale, starting at sample offset.
func tone(offset, n int) []int16 {
	out := make([]int16, n)
	for i := range out {
		out[i] = int16(16000 * math.Sin(2*math.Pi*440*float64(offset+i)/sampleRate))
	}
	return out
}

func rms(pcm []int16) float64 {
	if len(pcm) == 0 {
		return 0
	}
	var sum float64
	for _, v := range pcm {
		sum += float64(v) * float64(v)
	}
	return math.Sqrt(sum / float64(len(pcm)))
}

func openMedia(t *testing.T) *Media {
	t.Helper()
	m, err := Open(Config{}, zerolog.Nop())
	if err != nil {
		t.Fatalf("open media: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// waitConnected blocks until both peers report connected.
func waitConnected(t *testing.T, pcs ...*webrtc.PeerConnection) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for _, pc := range pcs {
		for pc.ConnectionState() != webrtc.PeerConnectionStateConnected {
			if time.Now().After(deadline) {
				t.Fatalf("peer state %s, want connected", pc.ConnectionState())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// feed plays a tone into the leg as meowcaller would, one 60 ms frame at a time, until ctx
// ends.
func feed(ctx context.Context, sink Sink) {
	go func() {
		ticker := time.NewTicker(60 * time.Millisecond)
		defer ticker.Stop()
		for n := 0; ; n++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			pcm := tone(n*frameSamples, frameSamples)
			frame := make([]float32, len(pcm))
			for i, v := range pcm {
				frame[i] = float32(v) / (maxAmplitude + 1)
			}
			_ = sink.WriteFrame(frame)
		}
	}()
}

// heardFromBrowser drains the leg's Source until a frame carries the browser's tone.
func heardFromBrowser(t *testing.T, source Source) float64 {
	t.Helper()
	deadline := time.Now().Add(testwait.Budget)
	for time.Now().Before(deadline) {
		// A whole frame queued before it is read: one read short of it would come back
		// padded with silence and measure the tone as quieter than it is.
		source.l.mu.Lock()
		queued := len(source.l.toWA)
		source.l.mu.Unlock()
		if queued < frameSamples {
			time.Sleep(testwait.Poll)
			continue
		}
		frame, err := source.ReadFrame()
		if err != nil {
			t.Fatalf("read from the browser: %v", err)
		}
		if len(frame) != frameSamples {
			t.Fatalf("frame of %d samples, want %d", len(frame), frameSamples)
		}
		pcm := toPCM16(nil, frame)
		if level := rms(pcm); level > 3000 {
			return level
		}
	}
	t.Fatal("nothing the browser said reached the WhatsApp side")
	return 0
}

// A call this account receives: the connector offers, the browser answers, and voice
// crosses both ways in G.722.
func TestAReceivedCallCarriesVoiceBothWaysInG722(t *testing.T) {
	m := openMedia(t)
	leg, offer, err := m.Offer(t.Context(), zerolog.Nop())
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	t.Cleanup(func() { _ = leg.Close() })
	if !strings.Contains(offer, "G722/8000") || !strings.Contains(offer, "PCMU/8000") {
		t.Fatalf("the offer does not carry both codecs:\n%s", offer)
	}

	b := newBrowser(t, webrtc.MimeTypeG722, webrtc.MimeTypePCMU)
	if err := leg.Accept(b.answer(offer)); err != nil {
		t.Fatalf("accept: %v", err)
	}
	waitConnected(t, leg.pc, b.pc)
	if got := leg.Codec(); got != webrtc.MimeTypeG722 {
		t.Fatalf("negotiated %q, want G.722", got)
	}

	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	leg.Start()
	feed(ctx, leg.Sink())
	b.speak(ctx)

	b.waitPackets(50)
	if level := rms(b.heardSoFar()); level < 3000 {
		t.Fatalf("the browser heard rms %.0f, want the tone", level)
	}
	heardFromBrowser(t, leg.Source())
}

// A call the client places: the browser offers, the connector answers. A browser that
// offers PCMU only still gets voice, in PCMU.
func TestAPlacedCallFallsBackToPCMU(t *testing.T) {
	m := openMedia(t)
	b := newBrowser(t, webrtc.MimeTypePCMU)
	leg, err := m.Answer(b.offer(), zerolog.Nop())
	if err != nil {
		t.Fatalf("answer: %v", err)
	}
	t.Cleanup(func() { _ = leg.Close() })
	answer, err := leg.Answer(t.Context())
	if err != nil {
		t.Fatalf("apply the answer: %v", err)
	}
	if strings.Contains(answer, "G722") {
		t.Fatalf("the answer names G.722 to a browser that did not offer it:\n%s", answer)
	}
	b.accept(answer)
	waitConnected(t, leg.pc, b.pc)
	if got := leg.Codec(); got != webrtc.MimeTypePCMU {
		t.Fatalf("negotiated %q, want PCMU", got)
	}

	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	leg.Start()
	feed(ctx, leg.Sink())
	b.speak(ctx)

	b.waitPackets(50)
	if level := rms(b.heardSoFar()); level < 3000 {
		t.Fatalf("the browser heard rms %.0f, want the tone", level)
	}
	heardFromBrowser(t, leg.Source())
}

// WhatsApp says nothing while the other person is silent, and the browser is still sent a
// packet on every tick: silence, not a gap.
func TestTheBrowserIsSentSilenceWhenWhatsAppSaysNothing(t *testing.T) {
	m := openMedia(t)
	leg, offer, err := m.Offer(t.Context(), zerolog.Nop())
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	t.Cleanup(func() { _ = leg.Close() })
	b := newBrowser(t, webrtc.MimeTypeG722)
	if err := leg.Accept(b.answer(offer)); err != nil {
		t.Fatalf("accept: %v", err)
	}
	waitConnected(t, leg.pc, b.pc)

	leg.Start()
	b.waitPackets(25)
	if level := rms(b.heardSoFar()); level > 100 {
		t.Fatalf("with nothing from WhatsApp the browser heard rms %.0f, want silence", level)
	}
}

func TestAnSDPTheConnectorCannotUseIsRefused(t *testing.T) {
	m := openMedia(t)

	leg, _, err := m.Offer(t.Context(), zerolog.Nop())
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	t.Cleanup(func() { _ = leg.Close() })
	if err := leg.Accept("this is not an sdp"); !errors.Is(err, ErrBadSDP) {
		t.Fatalf("accept of garbage = %v, want ErrBadSDP", err)
	}

	if _, err := m.Answer("this is not an sdp", zerolog.Nop()); !errors.Is(err, ErrBadSDP) {
		t.Fatalf("answer to garbage = %v, want ErrBadSDP", err)
	}

	// An offer with only a codec this side does not speak.
	opus := &webrtc.MediaEngine{}
	if err := opus.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		PayloadType:        111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		t.Fatal(err)
	}
	pc, err := webrtc.NewAPI(webrtc.WithMediaEngine(opus)).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio); err != nil {
		t.Fatal(err)
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if leg, err := m.Answer(offer.SDP, zerolog.Nop()); err == nil {
		_ = leg.Close()
		t.Fatal("an Opus-only offer was answered, and no voice could cross it")
	} else if !errors.Is(err, ErrBadSDP) {
		t.Fatalf("answer to an Opus-only offer = %v, want ErrBadSDP", err)
	}
}

// The socket is bound when the process starts, on the port asked for.
func TestTheMediaSocketIsTheOneAskedFor(t *testing.T) {
	m := openMedia(t)
	port := m.Port()
	if port == 0 {
		t.Fatal("the media socket has no port")
	}
	if _, err := Open(Config{UDPPort: port}, zerolog.Nop()); err == nil {
		t.Fatalf("a second socket bound port %d, which the first one holds", port)
	}
	if _, err := Open(Config{PublicIPs: []string{"not-an-ip"}}, zerolog.Nop()); err == nil {
		t.Fatal("a public IP that is not an address was accepted")
	}
}

// A placed call rings for as long as the callee takes, and ICE on this side does not start
// until the callee picks up: started with the call, pion's first checking deadline (~30 s)
// would fail the leg of a call still ringing, since the browser gets the answer and starts
// checking only then.
func TestAPlacedCallStartsICEOnlyWhenAnswered(t *testing.T) {
	m := openMedia(t)
	b := newBrowser(t, webrtc.MimeTypeG722)
	leg, err := m.Answer(b.offer(), zerolog.Nop())
	if err != nil {
		t.Fatalf("answer: %v", err)
	}
	t.Cleanup(func() { _ = leg.Close() })
	if leg.pc.LocalDescription() != nil {
		t.Fatal("the answer was applied while the callee was still being rung")
	}
	if got := leg.pc.ICEGatheringState(); got != webrtc.ICEGatheringStateNew {
		t.Fatalf("ICE gathering is %s before the callee answered, want new", got)
	}

	answer, err := leg.Answer(t.Context())
	if err != nil {
		t.Fatalf("apply the answer: %v", err)
	}
	if !strings.Contains(answer, "a=candidate") {
		t.Fatalf("the answer carries no candidate:\n%s", answer)
	}
	b.accept(answer)
	waitConnected(t, leg.pc, b.pc)
	if _, err := leg.Answer(t.Context()); err == nil {
		t.Fatal("a second Answer was taken")
	}
}

// The browser hanging up closes its peer connection, and the leg reports itself lost; the
// connector closing the leg does not, since whoever closed it is already ending the call.
func TestABrowserThatClosesItsPeerIsLost(t *testing.T) {
	for _, closer := range []string{"browser", "connector"} {
		t.Run(closer, func(t *testing.T) {
			m := openMedia(t)
			leg, offer, err := m.Offer(t.Context(), zerolog.Nop())
			if err != nil {
				t.Fatalf("offer: %v", err)
			}
			t.Cleanup(func() { _ = leg.Close() })
			lost := make(chan struct{}, 1)
			leg.OnLost(func() { lost <- struct{}{} })
			b := newBrowser(t, webrtc.MimeTypeG722)
			if err := leg.Accept(b.answer(offer)); err != nil {
				t.Fatalf("accept: %v", err)
			}
			waitConnected(t, leg.pc, b.pc)

			if closer == "browser" {
				_ = b.pc.Close()
				select {
				case <-lost:
				case <-time.After(10 * time.Second):
					t.Fatal("the browser closed its peer and the leg was not reported lost")
				}
				return
			}
			_ = leg.Close()
			_ = b.pc.Close()
			select {
			case <-lost:
				t.Fatal("the connector closed the leg and it reported itself lost")
			case <-time.After(500 * time.Millisecond):
			}
		})
	}
}
