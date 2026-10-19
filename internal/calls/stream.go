package calls

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/rs/zerolog"
)

const (
	// streamDialTimeout bounds the dial and the `start` frame: the call rings while it
	// runs, and one whose server cannot be reached is better left ringing.
	streamDialTimeout = 5 * time.Second
	// maxStreamToWhatsApp is how much of what the media server said may wait to be
	// played. A voice agent sends a whole sentence faster than real time, so this is far
	// more than the browser's bound; a `clear` empties it when the person talks over it.
	maxStreamToWhatsApp = sampleRate * 120
	// streamOutQueue is how many frames for the media server may wait for the socket.
	streamOutQueue = 64
	// streamStopTimeout bounds the `stop` frame, on a server that may have stopped reading.
	streamStopTimeout = time.Second
	// streamReadLimit is the largest frame read from the media server: a few seconds of
	// audio, base64 inside JSON.
	streamReadLimit = 1 << 22
)

// StreamTarget is where a call's voice goes when a media server, not a browser, is the
// other half of it.
type StreamTarget struct {
	URL string `json:"url"`
}

// StreamStart is what the `start` frame says about the call, next to the audio format.
type StreamStart struct {
	CallID    string `json:"call_id,omitempty"`
	Direction string `json:"direction"`
	Peer      string `json:"peer,omitempty"`
}

// StreamEnd is how a media stream ended from the server's side: it hung up, with who and
// why, or the socket went without a word.
type StreamEnd struct {
	Hangup bool
	By     string
	Reason string
}

type streamFormat struct {
	Encoding   string `json:"encoding"`
	SampleRate int    `json:"sample_rate"`
	Channels   int    `json:"channels"`
}

type startFrame struct {
	Event string `json:"event"`
	StreamStart
	Format streamFormat `json:"format"`
}

// streamFrame is one text frame on a media stream, either way. Decoded into a struct, so
// a field this build does not know is ignored.
type streamFrame struct {
	Event   string `json:"event"`
	Payload string `json:"payload,omitempty"`
	CallID  string `json:"call_id,omitempty"`
	By      string `json:"by,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

// Stream is the half of a call a media server carries over a WebSocket: raw PCM, 16 kHz
// mono, little endian, base64 in JSON text frames. The connector sends `start` once
// dialled, `answered` once the voice is wired, a `media` frame per 60 ms of what the
// person says, and `stop` when the call ends. The server sends `media` frames of any
// length, `clear` to drop what it sent and was not played yet, and `hangup`, or closes
// the socket, to end the call.
type Stream struct {
	conn *websocket.Conn
	log  zerolog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	out    chan []byte
	onEnd  func(StreamEnd)
	// stopping is closed by Close, to wake the writer for the `stop`; written is closed
	// once the writer is done.
	stopping chan struct{}
	written  chan struct{}

	mu       sync.Mutex
	toWA     []int16
	toServer []int16
	started  bool

	// closing is a stream this side is closing: what the server does from then on is
	// not the server ending it.
	closing   atomic.Bool
	closeOnce sync.Once
}

// DialStream connects to a media server over client, nil for net/http's default, and
// announces the call to it. onEnd is called
// when the server ends the stream: it hung up, closed the socket, or the socket broke; not
// for a stream this side closed. It is in place before anything is read, so a server that
// hangs up as soon as it reads `start` is not missed.
//
//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func DialStream(ctx context.Context, target StreamTarget, start StreamStart, onEnd func(StreamEnd), client *http.Client, log zerolog.Logger) (*Stream, error) {
	dialCtx, cancelDial := context.WithTimeout(ctx, streamDialTimeout)
	defer cancelDial()
	conn, _, err := websocket.Dial(dialCtx, target.URL, &websocket.DialOptions{HTTPClient: client}) //nolint:bodyclose // the response body is the library's
	if err != nil {
		return nil, fmt.Errorf("calls: dial the media stream at %s: %w", streamHost(target.URL), withoutURL(err))
	}
	conn.SetReadLimit(streamReadLimit)
	body, err := json.Marshal(startFrame{
		Event:       "start",
		StreamStart: start,
		Format:      streamFormat{Encoding: "pcm_s16le", SampleRate: sampleRate, Channels: 1},
	})
	if err == nil {
		err = conn.Write(dialCtx, websocket.MessageText, body)
	}
	if err != nil {
		_ = conn.CloseNow()
		return nil, fmt.Errorf("calls: announce the call to the media stream: %w", err)
	}
	streamCtx, cancel := context.WithCancel(context.Background())
	s := &Stream{
		conn: conn, log: log, ctx: streamCtx, cancel: cancel, onEnd: onEnd,
		out: make(chan []byte, streamOutQueue), stopping: make(chan struct{}), written: make(chan struct{}),
	}
	go s.read()
	go s.write()
	return s, nil
}

// streamHost is where a stream URL points, without its path and query: a voice agent's
// URL carries the token that admits the connector, and an error is logged.
func streamHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "an unparsable url"
	}
	return u.Scheme + "://" + u.Host
}

// withoutURL is a dial error without the request URL net/http wraps it in, for the same
// reason.
func withoutURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

func (s *Stream) ended(end StreamEnd) {
	if s.closing.Load() || s.onEnd == nil {
		return
	}
	s.onEnd(end)
}

// read is the one goroutine that reads the socket, and it returns once the stream ended,
// so the stream ends once.
func (s *Stream) read() {
	for {
		_, data, err := s.conn.Read(s.ctx)
		if err != nil {
			if s.ctx.Err() == nil {
				s.log.Debug().Err(err).Msg("media stream closed")
			}
			s.ended(StreamEnd{})
			return
		}
		var frame streamFrame
		if err := json.Unmarshal(data, &frame); err != nil {
			s.log.Debug().Err(err).Msg("media stream sent a frame that is not json")
			continue
		}
		switch frame.Event {
		case "media":
			raw, err := base64.StdEncoding.DecodeString(frame.Payload)
			if err != nil {
				continue
			}
			s.mu.Lock()
			for i := 0; i+1 < len(raw); i += 2 {
				s.toWA = append(s.toWA, int16(binary.LittleEndian.Uint16(raw[i:]))) //nolint:gosec // reinterpreting the bits is the decoding
			}
			if over := len(s.toWA) - maxStreamToWhatsApp; over > 0 {
				s.toWA = s.toWA[over:]
			}
			s.mu.Unlock()
		case "clear":
			s.mu.Lock()
			s.toWA = s.toWA[:0]
			s.mu.Unlock()
		case "hangup":
			s.ended(StreamEnd{Hangup: true, By: frame.By, Reason: frame.Reason})
			return
		}
	}
}

// write is the one goroutine that writes to the socket once the stream is dialled, so the
// `stop` it ends with is the last frame the server reads: whatever audio was still queued
// when the stream was closed is dropped.
func (s *Stream) write() {
	defer close(s.written)
	for {
		if s.closing.Load() {
			body, _ := json.Marshal(streamFrame{Event: "stop"})
			ctx, cancel := context.WithTimeout(s.ctx, streamStopTimeout)
			_ = s.conn.Write(ctx, websocket.MessageText, body)
			cancel()
			return
		}
		select {
		case <-s.ctx.Done():
			return
		case <-s.stopping:
		case body := <-s.out:
			if err := s.conn.Write(s.ctx, websocket.MessageText, body); err != nil {
				if s.ctx.Err() == nil {
					s.log.Debug().Err(err).Msg("write to the media stream")
				}
				return
			}
		}
	}
}

// send queues one frame for the media server, dropping it when the socket is behind:
// audio that arrives late is worse than audio that does not.
func (s *Stream) send(frame *streamFrame) {
	if s.closing.Load() {
		return
	}
	body, err := json.Marshal(frame)
	if err != nil {
		return
	}
	select {
	case s.out <- body:
	default:
		s.log.Debug().Msg("media stream is behind; dropped a frame")
	}
}

// Start tells the media server the voice is wired. Before it the server should not
// speak, since nothing it says is played.
func (s *Stream) Start(callID string) {
	s.mu.Lock()
	first := !s.started
	s.started = true
	s.mu.Unlock()
	if first {
		s.send(&streamFrame{Event: "answered", CallID: callID})
		go s.clock()
	}
}

// clock sends the media server a frame every 60 ms, silence when the person said nothing:
// WhatsApp sends nothing while the other side is silent, and a voice agent that detects
// the end of a turn by hearing silence would otherwise never hear it.
func (s *Stream) clock() {
	ticker := time.NewTicker(frameSamples * time.Second / sampleRate)
	defer ticker.Stop()
	chunk := make([]int16, frameSamples)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
		s.mu.Lock()
		n := copy(chunk, s.toServer)
		s.toServer = s.toServer[n:]
		s.mu.Unlock()
		clear(chunk[n:])
		raw := make([]byte, 0, len(chunk)*2)
		for _, v := range chunk {
			raw = binary.LittleEndian.AppendUint16(raw, uint16(v)) //nolint:gosec // reinterpreting the bits is the encoding
		}
		s.send(&streamFrame{Event: "media", Payload: base64.StdEncoding.EncodeToString(raw)})
	}
}

// Close tells the media server the call is over and closes the socket. It returns at
// once: nothing more is sent from here on, and the `stop` and the closing handshake run on
// their own goroutine, since a server that stopped reading would hold them for seconds and
// the call is ended from paths that a session's shutdown waits on. Safe to call more than
// once, and after the server ended the stream.
func (s *Stream) Close() error {
	s.closeOnce.Do(func() {
		s.closing.Store(true)
		close(s.stopping)
		go func() {
			// The writer's `stop` before the cancel: a context cancelled under a read or
			// a write closes the connection, and the frame would never go out. Bounded,
			// for a writer stuck on a server that stopped reading.
			select {
			case <-s.written:
			case <-time.After(2 * streamStopTimeout):
			}
			s.cancel()
			_ = s.conn.Close(websocket.StatusNormalClosure, "call ended")
		}()
	})
	return nil
}

// StreamSink is what meowcaller decodes the person's voice into, on its way to the media
// server.
type StreamSink struct{ s *Stream }

// Sink is the meowcaller AudioSink of this stream.
func (s *Stream) Sink() StreamSink { return StreamSink{s} }

// WriteFrame queues one frame of the person's voice for the media server's clock.
func (k StreamSink) WriteFrame(frame []float32) error {
	k.s.mu.Lock()
	defer k.s.mu.Unlock()
	k.s.toServer = toPCM16(k.s.toServer, frame)
	if over := len(k.s.toServer) - maxToBrowser; over > 0 {
		k.s.toServer = k.s.toServer[over:]
	}
	return nil
}

// Close is a no-op: the stream's life is the call's.
func (StreamSink) Close() error { return nil }

// StreamSource is what meowcaller plays into the call, read from the media server.
type StreamSource struct{ s *Stream }

// Source is the meowcaller AudioSource of this stream.
func (s *Stream) Source() StreamSource { return StreamSource{s} }

// ReadFrame is one 60 ms frame of what the media server said, padded with silence.
func (r StreamSource) ReadFrame() ([]float32, error) {
	frame := make([]float32, frameSamples)
	r.s.mu.Lock()
	n := min(len(r.s.toWA), frameSamples)
	for i, v := range r.s.toWA[:n] {
		frame[i] = float32(v) / (maxAmplitude + 1)
	}
	r.s.toWA = r.s.toWA[n:]
	r.s.mu.Unlock()
	return frame, nil
}

// Close is a no-op; see StreamSink.
func (StreamSource) Close() error { return nil }
