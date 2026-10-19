package calls

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/testwait"
)

// streamServer is a media server for one connection: what the connector sends arrives on
// got, decoded, and what a test puts on send goes to the connector.
type streamServer struct {
	url  string
	got  chan map[string]any
	send chan string
	drop chan struct{}
}

func newStreamServer(t *testing.T) *streamServer {
	t.Helper()
	m := &streamServer{got: make(chan map[string]any, 256), send: make(chan string, 16), drop: make(chan struct{})}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		ctx := r.Context()
		go func() {
			for {
				select {
				case body := <-m.send:
					_ = conn.Write(ctx, websocket.MessageText, []byte(body))
				case <-m.drop:
					_ = conn.CloseNow()
					return
				case <-ctx.Done():
					return
				}
			}
		}()
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var frame map[string]any
			_ = json.Unmarshal(data, &frame)
			m.got <- frame
		}
	}))
	t.Cleanup(srv.Close)
	m.url = "ws" + strings.TrimPrefix(srv.URL, "http")
	return m
}

func (m *streamServer) next(t *testing.T) map[string]any {
	t.Helper()
	select {
	case frame := <-m.got:
		return frame
	case <-time.After(testwait.Budget):
		t.Fatal("no frame from the connector")
		return nil
	}
}

// dialed is a stream to m, with every end it reports.
func dialed(t *testing.T, m *streamServer) (*Stream, chan StreamEnd) {
	t.Helper()
	end := make(chan StreamEnd, 4)
	s, err := DialStream(context.Background(), StreamTarget{URL: m.url}, StreamStart{CallID: "c1", Direction: "inbound", Peer: "5511988887777"},
		func(e StreamEnd) { end <- e }, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, end
}

func pcmFrame(samples int, first int16) string {
	raw := make([]byte, samples*2)
	binary.LittleEndian.PutUint16(raw, uint16(first))
	return `{"event":"media","payload":"` + base64.StdEncoding.EncodeToString(raw) + `","x_future":{"a":1}}`
}

func queuedToWA(s *Stream) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.toWA)
}

func waitQueued(t *testing.T, s *Stream, want int) {
	t.Helper()
	deadline := time.Now().Add(testwait.Budget)
	for queuedToWA(s) != want {
		if time.Now().After(deadline) {
			t.Fatalf("%d samples queued for WhatsApp, want %d", queuedToWA(s), want)
		}
		time.Sleep(testwait.Poll)
	}
}

func endOf(t *testing.T, ch chan StreamEnd) StreamEnd {
	t.Helper()
	select {
	case end := <-ch:
		return end
	case <-time.After(testwait.Budget):
		t.Fatal("the stream did not end")
		return StreamEnd{}
	}
}

// The connector announces the call with its format, says when it is answered, and from
// then on sends a frame on every tick, silence until the person speaks.
func TestAStreamAnnouncesTheCallAndCarriesThePersonsVoice(t *testing.T) {
	t.Parallel()
	m := newStreamServer(t)
	s, _ := dialed(t, m)

	start := m.next(t)
	format, _ := start["format"].(map[string]any)
	if start["event"] != "start" || start["call_id"] != "c1" || start["direction"] != "inbound" || start["peer"] != "5511988887777" ||
		format["encoding"] != "pcm_s16le" || format["sample_rate"] != float64(sampleRate) || format["channels"] != 1.0 {
		t.Fatalf("start = %v", start)
	}
	s.Start("c1")
	s.Start("c1")
	if answered := m.next(t); answered["event"] != "answered" || answered["call_id"] != "c1" {
		t.Fatalf("after Start = %v", answered)
	}
	silence := m.next(t)
	raw, err := base64.StdEncoding.DecodeString(silence["payload"].(string))
	if silence["event"] != "media" || err != nil || len(raw) != frameSamples*2 {
		t.Fatalf("clock frame = %v (%d bytes)", silence["event"], len(raw))
	}

	frame := make([]float32, frameSamples)
	frame[0] = 0.5
	if err := s.Sink().WriteFrame(frame); err != nil {
		t.Fatal(err)
	}
	for {
		f := m.next(t)
		if f["event"] == "answered" {
			t.Fatal("answered was sent twice")
		}
		raw, _ := base64.StdEncoding.DecodeString(f["payload"].(string))
		if v := int16(binary.LittleEndian.Uint16(raw)); v != 0 {
			if v != 16383 || len(raw) != frameSamples*2 {
				t.Fatalf("the person's frame: first sample %d, %d bytes", v, len(raw))
			}
			break
		}
	}
	after := m.next(t)
	raw, _ = base64.StdEncoding.DecodeString(after["payload"].(string))
	for i := 0; i+1 < len(raw); i += 2 {
		if raw[i] != 0 || raw[i+1] != 0 {
			t.Fatal("the frame after the person's was not silence")
		}
	}
}

// What the server says is played in frames padded with silence, and a `clear` drops what
// was not played yet. Fields this build does not know are ignored.
func TestAStreamPlaysWhatTheServerSaysUntilItClears(t *testing.T) {
	t.Parallel()
	m := newStreamServer(t)
	s, end := dialed(t, m)

	m.send <- pcmFrame(frameSamples*3/2, 16384)
	waitQueued(t, s, frameSamples*3/2)
	first, _ := s.Source().ReadFrame()
	if first[0] != 0.5 {
		t.Fatalf("first sample = %v, want 0.5", first[0])
	}
	half, _ := s.Source().ReadFrame()
	if len(half) != frameSamples || half[frameSamples-1] != 0 {
		t.Fatal("half a frame was not padded with silence")
	}

	m.send <- pcmFrame(frameSamples*4, 16384)
	waitQueued(t, s, frameSamples*4)
	m.send <- `{"event":"clear","x_future":true}`
	waitQueued(t, s, 0)
	m.send <- pcmFrame(frameSamples, 16384)
	waitQueued(t, s, frameSamples)

	select {
	case got := <-end:
		t.Fatalf("the stream ended on audio: %+v", got)
	default:
	}
}

// What the server says waits to be played up to a bound, past which the oldest goes.
func TestAStreamBoundsWhatWaitsToBePlayed(t *testing.T) {
	t.Parallel()
	m := newStreamServer(t)
	s, _ := dialed(t, m)

	s.mu.Lock()
	s.toWA = make([]int16, maxStreamToWhatsApp)
	s.mu.Unlock()
	m.send <- pcmFrame(frameSamples, 16384)
	deadline := time.Now().Add(testwait.Budget)
	for {
		s.mu.Lock()
		n, last := len(s.toWA), s.toWA[len(s.toWA)-frameSamples]
		s.mu.Unlock()
		if last == 16384 {
			if n != maxStreamToWhatsApp {
				t.Fatalf("%d samples queued, want the bound %d", n, maxStreamToWhatsApp)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the server's frame never reached the queue")
		}
		time.Sleep(testwait.Poll)
	}
}

// A hangup ends the stream once, with who hung up and why.
func TestAHangupEndsTheStreamOnce(t *testing.T) {
	t.Parallel()
	m := newStreamServer(t)
	s, end := dialed(t, m)

	m.send <- `{"event":"hangup","by":"agent","reason":"farewell","x_future":{"a":1}}`
	m.send <- `{"event":"hangup","by":"agent","reason":"again"}`
	if got := endOf(t, end); !got.Hangup || got.By != "agent" || got.Reason != "farewell" {
		t.Fatalf("end = %+v", got)
	}
	_ = s.Close()
	select {
	case got := <-end:
		t.Fatalf("the stream ended twice: %+v", got)
	default:
	}
}

// A socket that drops ends the stream, and says it was not a hangup.
func TestASocketThatDropsEndsTheStream(t *testing.T) {
	t.Parallel()
	m := newStreamServer(t)
	_, end := dialed(t, m)

	m.next(t)
	close(m.drop)
	if got := endOf(t, end); got.Hangup {
		t.Fatalf("end = %+v, want a socket that went", got)
	}
}

// Closing tells the server the call is over and is not the server ending the stream.
func TestClosingAStreamSendsStop(t *testing.T) {
	t.Parallel()
	m := newStreamServer(t)
	s, end := dialed(t, m)

	m.next(t)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if stop := m.next(t); stop["event"] != "stop" {
		t.Fatalf("frame on close = %v, want stop", stop)
	}
	select {
	case got := <-end:
		t.Fatalf("closing reported the server ending the stream: %+v", got)
	default:
	}
}

// A stream without a url, or whose server is not there, is not dialled.
func TestAStreamThatCannotBeDialled(t *testing.T) {
	t.Parallel()
	for _, url := range []string{"", "ws://127.0.0.1:1"} {
		if _, err := DialStream(context.Background(), StreamTarget{URL: url}, StreamStart{Direction: "inbound"}, nil, zerolog.Nop()); err == nil {
			t.Fatalf("dialled %q", url)
		}
	}
}

// A dial error names where the stream is and not the rest of its url, which carries the
// token a voice agent admits the connector with.
func TestADialErrorDoesNotCarryTheStreamsToken(t *testing.T) {
	t.Parallel()
	for _, url := range []string{"ws://127.0.0.1:1/api/v1/voice/stream/secret-token?k=secret-key", "ws://no-such-host.invalid/voice/secret-token"} {
		_, err := DialStream(context.Background(), StreamTarget{URL: url}, StreamStart{Direction: "inbound"}, nil, zerolog.Nop())
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("dial of %q: %v", url, err)
		}
	}
}

// A server that hangs up the moment it reads `start` still ends the stream: the end is
// not lost to a callback set after the reader started.
func TestAHangupRightAfterStartIsNotMissed(t *testing.T) {
	t.Parallel()
	m := newStreamServer(t)
	m.send <- `{"event":"hangup","by":"agent","reason":"busy"}`
	_, end := dialed(t, m)
	if got := endOf(t, end); !got.Hangup || got.Reason != "busy" {
		t.Fatalf("end = %+v", got)
	}
}
