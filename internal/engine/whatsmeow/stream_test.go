package whatsmeow

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/calls"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/testwait"
)

// mediaServer stands in for a voice agent's media server: it records every frame the
// connector sends it, sends what a test puts on `send`, and can hold the handshake or
// drop the socket.
type mediaServer struct {
	url string
	// conns counts the WebSockets the connector opened.
	conns atomic.Int32
	// requested receives one value per upgrade request, before the handshake.
	requested chan struct{}
	// gate, when set, holds every handshake until release.
	gate    chan struct{}
	release func()
	// frames is every text frame from the connector, decoded, then `closed` once the
	// socket is gone.
	frames chan map[string]any
	send   chan string
	// drop closes the TCP connection with no close frame.
	drop chan struct{}
}

const socketGone = "socket gone"

func newMediaServer(t *testing.T, gate chan struct{}) *mediaServer {
	t.Helper()
	m := &mediaServer{
		requested: make(chan struct{}, 8),
		gate:      gate,
		frames:    make(chan map[string]any, 256),
		send:      make(chan string, 16),
		drop:      make(chan struct{}),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.requested <- struct{}{}
		if m.gate != nil {
			<-m.gate
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		m.conns.Add(1)
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
				m.frames <- map[string]any{"event": socketGone}
				return
			}
			var frame map[string]any
			_ = json.Unmarshal(data, &frame)
			m.frames <- frame
		}
	}))
	t.Cleanup(srv.Close)
	var once sync.Once
	m.release = func() {
		if gate != nil {
			once.Do(func() { close(gate) })
		}
	}
	// Before the server closes, which waits on every handler: a test that failed with a
	// handshake held would otherwise never end.
	t.Cleanup(m.release)
	m.url = "ws" + strings.TrimPrefix(srv.URL, "http")
	return m
}

// dialled waits for the connector to ask for the stream.
func (m *mediaServer) dialled(t *testing.T) {
	t.Helper()
	select {
	case <-m.requested:
	case <-time.After(testwait.Budget):
		t.Fatal("the connector did not dial the stream")
	}
}

// finished is the result of a command run on its own goroutine.
func finished(t *testing.T, done chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(testwait.Budget):
		t.Fatal("the command did not return")
		return nil
	}
}

// next is the next frame from the connector that is not audio.
func (m *mediaServer) next(t *testing.T) map[string]any {
	t.Helper()
	deadline := time.After(testwait.Budget)
	for {
		select {
		case frame := <-m.frames:
			if frame["event"] == "media" {
				continue
			}
			return frame
		case <-deadline:
			t.Fatal("the media server got nothing more from the connector")
			return nil
		}
	}
}

func streamAccept(t *testing.T, callID, url string) *protocol.Command {
	t.Helper()
	return command(protocol.CommandCallAccept, `{"call_id":"`+callID+`","stream":{"url":`+mustJSON(t, url)+`}}`)
}

// acceptedOnStream is a received call accepted with a media stream, and the `start` the
// server read.
func acceptedOnStream(t *testing.T) (*Session, *fakeCall, *mediaServer) {
	t.Helper()
	session := newCallSession(t)
	call, _ := ringCall(t, session, "call-1")
	server := newMediaServer(t, nil)
	if _, err := session.Execute(t.Context(), streamAccept(t, "call-1", server.url)); err != nil {
		t.Fatalf("call.accept with a stream: %v", err)
	}
	if start := server.next(t); start["event"] != "start" {
		t.Fatalf("first frame = %v, want start", start)
	}
	return session, call, server
}

func hungUp(call *fakeCall) int {
	_, _, n, _ := call.counts()
	return n
}

func waitHungUp(t *testing.T, call *fakeCall) {
	t.Helper()
	deadline := time.Now().Add(testwait.Budget)
	for hungUp(call) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the call was not hung up on WhatsApp")
		}
		time.Sleep(testwait.Poll)
	}
}

// A call accepted with a media stream is announced to the server, answered on WhatsApp,
// wired to the stream, and only then is the server told it was answered.
func TestAnAcceptWithAStreamAnswersTheCallOnTheStream(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call, _ := ringCall(t, session, "call-1")
	server := newMediaServer(t, nil)

	if _, err := session.Execute(t.Context(), streamAccept(t, "call-1", server.url)); err != nil {
		t.Fatalf("call.accept with a stream: %v", err)
	}
	start := server.next(t)
	format, _ := start["format"].(map[string]any)
	if start["event"] != "start" || start["call_id"] != "call-1" || start["direction"] != "inbound" ||
		start["peer"] != "5511988887777" || format["encoding"] != "pcm_s16le" || format["sample_rate"] != 16000.0 {
		t.Fatalf("start = %v", start)
	}
	if answered := server.next(t); answered["event"] != "answered" || answered["call_id"] != "call-1" {
		t.Fatalf("after start = %v, want answered", answered)
	}
	if answered, _, _, wired := call.counts(); answered != 1 || !wired {
		t.Fatalf("answered %d times, wired %v; want once, wired", answered, wired)
	}
	if n := server.conns.Load(); n != 1 {
		t.Fatalf("opened %d streams, want 1", n)
	}
}

// The call is not answered on WhatsApp before the stream is open: the person would hear a
// connected call with nobody behind it.
func TestACallIsAnsweredOnlyOnceItsStreamIsOpen(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call, _ := ringCall(t, session, "call-1")
	gate := make(chan struct{})
	server := newMediaServer(t, gate)

	done := make(chan error, 1)
	go func() {
		_, err := session.Execute(t.Context(), streamAccept(t, "call-1", server.url))
		done <- err
	}()
	server.dialled(t)
	if answered, _, _, _ := call.counts(); answered != 0 {
		t.Fatal("the call was answered before its stream was open")
	}
	server.release()
	if err := finished(t, done); err != nil {
		t.Fatalf("call.accept with a stream: %v", err)
	}
	if answered, _, _, _ := call.counts(); answered != 1 {
		t.Fatalf("answered %d times once the stream opened, want 1", answered)
	}
}

// A stream that cannot be reached fails the command and leaves the call ringing, so it can
// still be answered another way.
func TestAStreamThatDoesNotOpenLeavesTheCallRinging(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call, offer := ringCall(t, session, "call-1")
	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	nobody := "ws://" + listener.Addr().String()
	_ = listener.Close()

	_, err = session.Execute(t.Context(), streamAccept(t, "call-1", nobody))
	assertCode(t, err, protocol.ErrorInvalidPayload)
	if answered, _, hung, _ := call.counts(); answered != 0 || hung != 0 {
		t.Fatalf("answered %d, hung up %d; want the call left ringing", answered, hung)
	}

	accept := `{"call_id":"call-1","sdp":` + mustJSON(t, browserAnswer(t, offer)) + `}`
	if _, err := session.Execute(t.Context(), command(protocol.CommandCallAccept, accept)); err != nil {
		t.Fatalf("call.accept from the browser after the stream failed: %v", err)
	}
	if answered, _, _, _ := call.counts(); answered != 1 {
		t.Fatal("the call could not be answered after its stream failed to open")
	}
}

// A hangup from the media server ends the call on WhatsApp, and the end says the stream
// hung up. Fields this build does not know are ignored, and a second hangup is nothing new.
func TestAHangupFromTheStreamEndsTheCallOnce(t *testing.T) {
	t.Parallel()
	session, call, server := acceptedOnStream(t)
	if answered := server.next(t); answered["event"] != "answered" {
		t.Fatalf("frame = %v, want answered", answered)
	}

	hangup := `{"event":"hangup","by":"agent","reason":"farewell","x_future":{"a":1}}`
	server.send <- hangup
	server.send <- hangup
	waitHungUp(t, call)
	end := published(t, session, protocol.EventCallTerminate, "event_call_terminate")
	if end["call_id"] != "call-1" || end["reason"] != "stream_hangup" {
		t.Fatalf("call.terminate = %v, want reason stream_hangup", end)
	}
	if stop := server.next(t); stop["event"] != "stop" {
		t.Fatalf("frame after the hangup = %v, want stop", stop)
	}
	if gone := server.next(t); gone["event"] != socketGone {
		t.Fatalf("frame after stop = %v, want the socket closed", gone)
	}
	if n := hungUp(call); n != 1 {
		t.Fatalf("hung up %d times, want once", n)
	}
}

// A stream that drops without a word ends the call, with a reason of its own: a call left
// up would keep the person talking to silence.
func TestAStreamThatDropsEndsTheCall(t *testing.T) {
	t.Parallel()
	session, call, server := acceptedOnStream(t)
	if answered := server.next(t); answered["event"] != "answered" {
		t.Fatalf("frame = %v, want answered", answered)
	}

	close(server.drop)
	waitHungUp(t, call)
	end := published(t, session, protocol.EventCallTerminate, "event_call_terminate")
	if end["call_id"] != "call-1" || end["reason"] != "stream_lost" {
		t.Fatalf("call.terminate = %v, want reason stream_lost", end)
	}
}

// The caller hanging up tells the media server the call is over, and closes the socket.
func TestTheCallerHangingUpStopsTheStream(t *testing.T) {
	t.Parallel()
	session, call, server := acceptedOnStream(t)
	if answered := server.next(t); answered["event"] != "answered" {
		t.Fatalf("frame = %v, want answered", answered)
	}

	call.remoteEnds("timeout")
	if stop := server.next(t); stop["event"] != "stop" {
		t.Fatalf("frame after the caller hung up = %v, want stop", stop)
	}
	if gone := server.next(t); gone["event"] != socketGone {
		t.Fatalf("frame after stop = %v, want the socket closed", gone)
	}
	end := published(t, session, protocol.EventCallTerminate, "event_call_terminate")
	if end["reason"] != "timeout" {
		t.Fatalf("call.terminate = %v, want the caller's reason", end)
	}
	if n := hungUp(call); n != 0 {
		t.Fatalf("hung up a call the caller already ended: %d", n)
	}
}

// An accept redelivered while its stream is being dialled opens no second stream and
// answers the call once.
func TestAStreamAcceptRepeatedWhileDiallingIsNothingNew(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call, _ := ringCall(t, session, "call-1")
	gate := make(chan struct{})
	server := newMediaServer(t, gate)

	done := make(chan error, 1)
	go func() {
		_, err := session.Execute(t.Context(), streamAccept(t, "call-1", server.url))
		done <- err
	}()
	server.dialled(t)
	repeat := make(chan error, 1)
	go func() {
		_, err := session.Execute(t.Context(), streamAccept(t, "call-1", server.url))
		repeat <- err
	}()
	// Before the handshake is let through: a repeat that dials again waits on it too.
	if err := finished(t, repeat); err != nil {
		t.Fatalf("a repeated call.accept: %v", err)
	}
	server.release()
	if err := finished(t, done); err != nil {
		t.Fatalf("call.accept with a stream: %v", err)
	}
	if _, err := session.Execute(t.Context(), streamAccept(t, "call-1", server.url)); err != nil {
		t.Fatalf("a call.accept after the answer: %v", err)
	}
	if answered, _, _, _ := call.counts(); answered != 1 {
		t.Fatalf("answered %d times, want once", answered)
	}
	if n := server.conns.Load(); n != 1 {
		t.Fatalf("opened %d streams, want 1", n)
	}
}

// A call that ends while its stream is being dialled is not answered, and the stream that
// opened for it is told the call is over.
func TestACallThatEndsWhileItsStreamIsDialledClosesTheStream(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call, _ := ringCall(t, session, "call-1")
	gate := make(chan struct{})
	server := newMediaServer(t, gate)

	done := make(chan error, 1)
	go func() {
		_, err := session.Execute(t.Context(), streamAccept(t, "call-1", server.url))
		done <- err
	}()
	server.dialled(t)
	call.remoteEnds("timeout")
	published(t, session, protocol.EventCallTerminate, "event_call_terminate")
	server.release()
	if err := finished(t, done); err != nil {
		t.Fatalf("an accept of a call that ended while it was dialled = %v, want nothing new", err)
	}
	if answered, _, _, _ := call.counts(); answered != 0 {
		t.Fatal("a call that had ended was answered")
	}
	for {
		frame := server.next(t)
		if frame["event"] == socketGone {
			break
		}
		if frame["event"] == "answered" {
			t.Fatal("the stream was told a call that had ended was answered")
		}
	}
}

// A call offered without sdp has no browser offer to answer, and can still be answered by
// a media stream, which needs none.
func TestACallOfferedWithoutSDPCanBeAcceptedOnAStream(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call := &fakeCall{id: "call-1"}
	session.ringing(call)
	server := newMediaServer(t, nil)

	if _, err := session.Execute(t.Context(), streamAccept(t, "call-1", server.url)); err != nil {
		t.Fatalf("call.accept with a stream: %v", err)
	}
	if answered, _, _, wired := call.counts(); answered != 1 || !wired {
		t.Fatalf("answered %d times, wired %v; want once, wired", answered, wired)
	}
}

// An answer WhatsApp does not take tells the media server the call is over, rather than
// leaving it waiting on a call nobody answered, and a retry dials it again.
func TestAStreamAcceptWhatsAppRefusesStopsTheStream(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call, _ := ringCall(t, session, "call-1")
	call.fail = errors.New("no ack")
	server := newMediaServer(t, nil)

	_, err := session.Execute(t.Context(), streamAccept(t, "call-1", server.url))
	assertCode(t, err, protocol.ErrorWaError)
	if start := server.next(t); start["event"] != "start" {
		t.Fatalf("first frame = %v, want start", start)
	}
	if stop := server.next(t); stop["event"] != "stop" {
		t.Fatalf("frame after the refused answer = %v, want stop", stop)
	}

	call.mu.Lock()
	call.fail = nil
	call.mu.Unlock()
	if _, err := session.Execute(t.Context(), streamAccept(t, "call-1", server.url)); err != nil {
		t.Fatalf("a retried call.accept: %v", err)
	}
	if n := server.conns.Load(); n != 2 {
		t.Fatalf("opened %d streams, want the retry to open a second", n)
	}
}

// A call offered without sdp has no browser leg, and an accept redelivered while its
// stream is dialled still opens no second one.
func TestAStreamAcceptOfACallWithoutSDPRepeatedWhileDiallingIsNothingNew(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call := &fakeCall{id: "call-1"}
	session.ringing(call)
	gate := make(chan struct{})
	server := newMediaServer(t, gate)

	done := make(chan error, 1)
	go func() {
		_, err := session.Execute(t.Context(), streamAccept(t, "call-1", server.url))
		done <- err
	}()
	server.dialled(t)
	repeat := make(chan error, 1)
	go func() {
		_, err := session.Execute(t.Context(), streamAccept(t, "call-1", server.url))
		repeat <- err
	}()
	// Before the handshake is let through: a repeat that dials again waits on it too.
	if err := finished(t, repeat); err != nil {
		t.Fatalf("a repeated call.accept: %v", err)
	}
	server.release()
	if err := finished(t, done); err != nil {
		t.Fatalf("call.accept with a stream: %v", err)
	}
	if answered, _, _, _ := call.counts(); answered != 1 {
		t.Fatalf("answered %d times, want once", answered)
	}
	if n := server.conns.Load(); n != 1 {
		t.Fatalf("opened %d streams, want 1", n)
	}
}

// A stream URL is the client's, and is dialled the way a media URL is fetched: never to
// an address a cloud's metadata service answers on, whether named directly or reached
// through a redirect.
func TestAStreamIsNeverDialledToAMetadataAddress(t *testing.T) {
	t.Parallel()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data/", http.StatusFound)
	}))
	t.Cleanup(redirect.Close)
	for _, url := range []string{"ws://169.254.169.254/latest/meta-data/", "ws" + strings.TrimPrefix(redirect.URL, "http")} {
		_, err := dialStream(t.Context(), calls.StreamTarget{URL: url}, calls.StreamStart{Direction: "inbound"}, nil, zerolog.Nop())
		if !errors.Is(err, errMetadataAddress) {
			t.Fatalf("dial of %s = %v, want it refused as a metadata address", url, err)
		}
	}
}

// Without a stream, an accept still has to carry the browser's answer.
func TestAnAcceptWithNeitherAnswerNorStreamIsRefused(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	call, _ := ringCall(t, session, "call-1")

	for _, payload := range []string{`{"call_id":"call-1"}`, `{"call_id":"call-1","stream":{"url":""}}`, `{"call_id":"call-1","stream":{}}`} {
		_, err := session.Execute(t.Context(), command(protocol.CommandCallAccept, payload))
		assertCode(t, err, protocol.ErrorInvalidPayload)
	}
	if answered, _, _, _ := call.counts(); answered != 0 {
		t.Fatal("an accept with nothing to answer with answered the call")
	}
}
