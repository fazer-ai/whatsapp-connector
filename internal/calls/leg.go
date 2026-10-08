package calls

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
)

const (
	// packetInterval is how often the browser is sent a packet, voice or silence.
	packetInterval = 20 * time.Millisecond
	// maxToBrowser is how much of what WhatsApp said may wait for the clock. More than
	// this is a stall somewhere upstream, and playing it late is worse than dropping it.
	maxToBrowser = sampleRate / 5
	// maxToWhatsApp is the same bound in the other direction.
	maxToWhatsApp = sampleRate * 3 / 10
)

// Leg is the browser half of one call: a WebRTC peer, and the two queues between it and
// meowcaller.
type Leg struct {
	pc    *webrtc.PeerConnection
	track *audioTrack
	log   zerolog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	toBrowser []int16
	toWA      []int16
	started   bool
	// pending is the browser's offer for a call the client placed, held from the moment
	// the call was placed until the callee picks up.
	pending string
	onLost  func()

	closeOnce sync.Once
}

//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func (m *Media) newLeg(log zerolog.Logger) (*Leg, error) {
	pc, err := m.api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return nil, fmt.Errorf("calls: new peer: %w", err)
	}
	track := &audioTrack{}
	if _, err := pc.AddTransceiverFromTrack(track, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendrecv}); err != nil {
		_ = pc.Close()
		return nil, fmt.Errorf("calls: add the audio track: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	leg := &Leg{pc: pc, track: track, log: log, ctx: ctx, cancel: cancel}
	pc.OnTrack(leg.listen)
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		leg.log.Debug().Str("state", state.String()).Msg("browser peer")
		switch state {
		case webrtc.PeerConnectionStateFailed:
		case webrtc.PeerConnectionStateClosed:
			// Closed by the browser, which ends DTLS with a close_notify and leaves pion
			// to close this side: the browser hung up. Closed by this side, the leg's
			// context went first, and whoever closed it is already ending the call.
			if leg.ctx.Err() != nil {
				return
			}
		default:
			return
		}
		leg.mu.Lock()
		lost := leg.onLost
		leg.mu.Unlock()
		if lost != nil {
			lost()
		}
	})
	return leg, nil
}

// gathered is the local description once every candidate is in it. The browser gets one
// SDP and no trickle: neither side of the contract has a frame for a late candidate.
func (l *Leg) gathered(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, gatherTimeout)
	defer cancel()
	select {
	case <-webrtc.GatheringCompletePromise(l.pc):
	case <-ctx.Done():
		return "", fmt.Errorf("calls: gather candidates: %w", ctx.Err())
	}
	return l.pc.LocalDescription().SDP, nil
}

// Answer applies the browser's offer that Media.Answer took, answers it, and returns the
// answer with its candidates, for the browser. Only once the callee has picked up; see
// Media.Answer for why.
func (l *Leg) Answer(ctx context.Context) (string, error) {
	l.mu.Lock()
	offer := l.pending
	l.pending = ""
	l.mu.Unlock()
	if offer == "" {
		return "", errors.New("calls: this leg has no offer to answer")
	}
	if err := l.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}); err != nil {
		return "", fmt.Errorf("%w: %w", ErrBadSDP, err)
	}
	answer, err := l.pc.CreateAnswer(nil)
	if err == nil {
		err = l.pc.SetLocalDescription(answer)
	}
	if err != nil {
		return "", fmt.Errorf("calls: answer the browser: %w", err)
	}
	return l.gathered(ctx)
}

// Accept applies the browser's answer to the connector's offer.
func (l *Leg) Accept(answer string) error {
	if err := usableAudio(answer); err != nil {
		return err
	}
	if err := l.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		return fmt.Errorf("%w: %w", ErrBadSDP, err)
	}
	return nil
}

// OnLost is called once the browser peer is gone for good: ICE gave up, so nothing the
// connector sends reaches the browser and nothing comes back, or the browser closed its
// peer connection.
func (l *Leg) OnLost(fn func()) {
	l.mu.Lock()
	l.onLost = fn
	l.mu.Unlock()
}

// Codec is the codec the browser negotiated, empty before it answered.
func (l *Leg) Codec() string { return l.track.codec() }

// Start begins sending to the browser on a clock of its own. WhatsApp sends nothing while
// the other person is silent, and RTP that only moves when there is voice makes the
// browser's jitter buffer grow across every silence: in the spike it reached ~940 ms, and
// a packet every 20 ms, silence when there is nothing to say, kept it at ~30 ms.
func (l *Leg) Start() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started {
		return
	}
	l.started = true
	go l.clock()
}

func (l *Leg) clock() {
	enc, _, ok := codecFor(l.track.codec())
	if !ok {
		// Started before the browser answered, or on a negotiation that found nothing in
		// common, which Accept and Answer would have refused already.
		l.log.Warn().Msg("browser leg started with no negotiated codec; nothing is sent")
		return
	}
	ticker := time.NewTicker(packetInterval)
	defer ticker.Stop()
	chunk := make([]int16, packetSamples)
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
		}
		l.mu.Lock()
		n := copy(chunk, l.toBrowser)
		l.toBrowser = l.toBrowser[n:]
		l.mu.Unlock()
		clear(chunk[n:])
		if err := l.track.write(enc.encode(chunk)); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			l.log.Debug().Err(err).Msg("write to the browser")
		}
	}
}

// listen decodes what the browser says into the queue WhatsApp is played from.
func (l *Leg) listen(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
	_, dec, ok := codecFor(remote.Codec().MimeType)
	if !ok {
		l.log.Warn().Str("codec", remote.Codec().MimeType).Msg("the browser sent a track in a codec this side does not speak")
		return
	}
	var order reorder
	for {
		packet, _, err := remote.ReadRTP()
		if err != nil {
			return
		}
		l.take(&order, dec, packet)
	}
}

// lostPacket is the silence a packet given up as lost leaves: 20 ms, which is what a
// browser sends per packet.
var lostPacket = make([]int16, packetSamples)

// take queues one packet from the browser for WhatsApp, in sequence.
func (l *Leg) take(order *reorder, dec decoder, packet *rtp.Packet) {
	for _, ready := range order.push(packet) {
		pcm := lostPacket
		if ready != nil {
			pcm = dec.decode(ready.Payload)
		}
		l.mu.Lock()
		l.toWA = append(l.toWA, pcm...)
		if over := len(l.toWA) - maxToWhatsApp; over > 0 {
			l.toWA = l.toWA[over:]
		}
		l.mu.Unlock()
	}
}

// Sink is what meowcaller decodes WhatsApp's audio into, on its way to the browser.
func (l *Leg) Sink() Sink { return Sink{l} }

// Source is what meowcaller plays into the call, read from the browser.
func (l *Leg) Source() Source { return Source{l} }

// Close tears the browser peer down. Safe to call more than once.
func (l *Leg) Close() error {
	var err error
	l.closeOnce.Do(func() {
		l.cancel()
		err = l.pc.Close()
	})
	return err
}

// Sink matches meowcaller's AudioSink. Its Close does nothing: the leg's life is the
// call's, and is ended by whoever ends the call.
type Sink struct{ l *Leg }

// WriteFrame queues one frame of WhatsApp's audio for the browser.
func (s Sink) WriteFrame(frame []float32) error {
	s.l.mu.Lock()
	defer s.l.mu.Unlock()
	s.l.toBrowser = toPCM16(s.l.toBrowser, frame)
	if over := len(s.l.toBrowser) - maxToBrowser; over > 0 {
		s.l.toBrowser = s.l.toBrowser[over:]
	}
	return nil
}

// Close is a no-op; see Sink.
func (Sink) Close() error { return nil }

// Source matches meowcaller's AudioSource.
type Source struct{ l *Leg }

// ReadFrame is one 60 ms frame of what the browser said, padded with silence when it has
// said less. It never reports the end: the call ends by being hung up, not by the browser
// running out of audio.
func (s Source) ReadFrame() ([]float32, error) {
	frame := make([]float32, frameSamples)
	s.l.mu.Lock()
	n := min(len(s.l.toWA), frameSamples)
	for i, v := range s.l.toWA[:n] {
		frame[i] = float32(v) / (maxAmplitude + 1)
	}
	s.l.toWA = s.l.toWA[n:]
	s.l.mu.Unlock()
	return frame, nil
}

// Close is a no-op; see Sink.
func (Source) Close() error { return nil }
