package whatsmeow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/coder/websocket"
	wm "go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	waSocket "go.mau.fi/whatsmeow/socket"
	waTypes "go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// wsWire stands where WhatsApp's websocket does, for the one thing #180 is about: how many
// times a write reaches it. Every frame whatsmeow writes is one websocket message, so a
// message counted here is a frame that left the client -- encrypted, which does not matter,
// since the question is how many and not what.
//
// It is not a WhatsApp server. Nothing answers the handshake, and a real client would never
// adopt a socket this side opens: the server's certificate is checked against a key only
// WhatsApp holds. So the test adopts it for the client, by writing the two fields the
// handshake would have written (see adopt), and the rest of the path is whatsmeow's own:
// sendIQ, the response waiters, onDisconnect clearing them, retryFrame waiting for the
// connection and resending. That is the path the issue measured duplicating on the real
// service, and the reason a seam double is not evidence here: the resend lives inside the
// library, below every seam this package has.
type wsWire struct {
	t      *testing.T
	server *httptest.Server

	mu     sync.Mutex
	conns  []*websocket.Conn
	frames []int
	landed chan int // the connection each frame arrived on, in order
}

func newWire(t *testing.T) *wsWire {
	t.Helper()
	w := &wsWire{t: t, landed: make(chan int, 64)}
	w.server = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		// whatsmeow sends the Origin WhatsApp expects, which is not this server.
		conn, err := websocket.Accept(rw, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		w.mu.Lock()
		index := len(w.conns)
		w.conns = append(w.conns, conn)
		w.frames = append(w.frames, 0)
		w.mu.Unlock()
		for {
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
			w.mu.Lock()
			w.frames[index]++
			w.mu.Unlock()
			w.landed <- index
		}
	}))
	t.Cleanup(w.server.Close)
	return w
}

// framesOn is how many frames have arrived on the index'th connection.
func (w *wsWire) framesOn(index int) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if index >= len(w.frames) {
		return 0
	}
	return w.frames[index]
}

// nextFrame waits for the next frame and reports which connection it came on.
func (w *wsWire) nextFrame(within time.Duration) (int, bool) {
	select {
	case index := <-w.landed:
		return index, true
	case <-time.After(within):
		return 0, false
	}
}

// cut drops the index'th connection from the server's side, the way WhatsApp does when the
// path goes: the client's read pump fails, and whatsmeow takes that for a remote drop.
func (w *wsWire) cut(index int) {
	w.mu.Lock()
	conn := w.conns[index]
	w.mu.Unlock()
	_ = conn.CloseNow()
}

// adopt opens a connection to the wsWire and makes it the client's socket, logged in.
//
// The handshake a real connection goes through ends by assigning `cli.socket` and, once
// the server says so, `isLoggedIn`; those are written here instead, under the lock whatsmeow
// writes them under, and then the socket wait is woken the way a reconnect wakes it. Through
// reflection, because neither has a setter: this is the only part of the path that is not
// the library's own, and it is the part a real server would have done.
func (w *wsWire) adopt(ctx context.Context, client *wm.Client) {
	w.t.Helper()
	frames := waSocket.NewFrameSocket(waLog.Noop, nil)
	frames.URL = "ws" + strings.TrimPrefix(w.server.URL, "http")
	if err := frames.Connect(ctx); err != nil {
		w.t.Fatalf("connect to the wsWire: %v", err)
	}
	handshake := waSocket.NewNoiseHandshake()
	handshake.Start(waSocket.NoiseStartPattern, waSocket.WAConnHeader)
	//nolint:staticcheck // SA1019: the disconnect handler a real handshake installs is this one
	noise, err := handshake.Finish(ctx, frames, func(context.Context, []byte) {}, client.DangerousInternals().OnDisconnect)
	if err != nil {
		w.t.Fatalf("finish a handshake with the wsWire: %v", err)
	}

	value := reflect.ValueOf(client).Elem()
	lock := (*sync.RWMutex)(unsafe.Pointer(value.FieldByName("socketLock").UnsafeAddr()))
	socket := (**waSocket.NoiseSocket)(unsafe.Pointer(value.FieldByName("socket").UnsafeAddr()))
	loggedIn := (*atomic.Bool)(unsafe.Pointer(value.FieldByName("isLoggedIn").UnsafeAddr()))
	lock.Lock()
	*socket = noise
	loggedIn.Store(true)
	lock.Unlock()
	//nolint:staticcheck // SA1019: what a reconnect does once the socket is up, as above
	client.DangerousInternals().CloseSocketWaitChan()
}

// wiredSession is a paired session whose client talks to the wsWire, with the session
// listening to it the way adopt makes it listen to a real one.
func wiredSession(t *testing.T) (*Session, *wsWire) {
	t.Helper()
	session, _ := newTestSession(t, "5511999990180")
	client := session.current()
	// Nothing here may dial WhatsApp: the reconnect is the test's to make, at the moment it
	// chooses, which is the whole of what is being measured.
	client.EnableAutoReconnect = false
	client.AddEventHandlerWithSuccessStatus(session.handle)
	// Whatever a write reads before writing is not under test, and a read sent to the wsWire
	// would be counted as a write and never answered.
	session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
		// With a description id, so that whatsmeow writes the description instead of reading
		// the group again first.
		info := &waTypes.GroupInfo{JID: waTypes.NewJID("120363041234567890", waTypes.GroupServer)}
		info.TopicID = "3EB0PREVIOUS"
		return info, nil
	}
	w := newWire(t)
	w.adopt(t.Context(), client)
	// Registered after the wire, so it runs before the wire closes: the client lets go of the
	// socket first. The other order has the server's close reach the read pump while the
	// session's own Close stops the socket, which is whatsmeow's race of #207 and nothing to
	// do with this test.
	t.Cleanup(client.Disconnect)
	session.setConnected(true)
	return session, w
}

// groupWrites is every group command that writes to WhatsApp through one of whatsmeow's
// helpers, and so through sendIQ's resend. group.create is not here: it builds its own query
// with NoRetry and was never resent.
var groupWrites = []struct {
	name    string
	kind    protocol.CommandType
	payload string
}{
	{"participants add", protocol.CommandGroupParticipantsUpdate, `{"group":{"kind":"group","id":"120363041234567890"},"participants":[{"kind":"phone","id":"5541977776666"}],"action":"add"}`},
	{"participants remove", protocol.CommandGroupParticipantsUpdate, `{"group":{"kind":"group","id":"120363041234567890"},"participants":[{"kind":"phone","id":"5541977776666"}],"action":"remove"}`},
	{"invite revoked", protocol.CommandGroupInviteGet, `{"group":{"kind":"group","id":"120363041234567890"},"revoke":true}`},
	{"photo", protocol.CommandGroupPhotoSet, `{"group":{"kind":"group","id":"120363041234567890"},"image":"/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAA=="}`},
	{"name", protocol.CommandGroupNameSet, `{"group":{"kind":"group","id":"120363041234567890"},"subject":"wac180"}`},
	{"description", protocol.CommandGroupDescriptionSet, `{"group":{"kind":"group","id":"120363041234567890"},"description":"wac180"}`},
	{"announce", protocol.CommandGroupSettingsSet, `{"group":{"kind":"group","id":"120363041234567890"},"setting":"announce","value":true}`},
	{"locked", protocol.CommandGroupSettingsSet, `{"group":{"kind":"group","id":"120363041234567890"},"setting":"locked","value":true}`},
	{"join approval", protocol.CommandGroupSettingsSet, `{"group":{"kind":"group","id":"120363041234567890"},"setting":"join_approval","value":true}`},
	{"member add mode", protocol.CommandGroupSettingsSet, `{"group":{"kind":"group","id":"120363041234567890"},"setting":"member_add_mode","value":"admin_add"}`},
	{"join requests", protocol.CommandGroupJoinRequestsUpdate, `{"group":{"kind":"group","id":"120363041234567890"},"participants":[{"kind":"phone","id":"5541977776666"}],"action":"approve"}`},
	{"leave", protocol.CommandGroupLeave, `{"group":{"kind":"group","id":"120363041234567890"}}`},
}

type answered struct {
	result []byte
	err    error
}

// interrupted runs one group write on a wired session, drops the connection while the write
// waits for its answer, and gives the client a new one after back (or never, when back is
// zero). It reports the frames each connection received and what the command answered.
func interrupted(t *testing.T, kind protocol.CommandType, payload string, back time.Duration) (first, second int, answer answered) {
	t.Helper()
	session, w := wiredSession(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	done := make(chan answered, 1)
	go func() {
		result, err := session.Execute(ctx, setCommand(t, kind, payload))
		done <- answered{result, err}
	}()
	if index, ok := w.nextFrame(5 * time.Second); !ok || index != 0 {
		t.Fatalf("the write never reached the wsWire (frame on %d, %v)", index, ok)
	}

	before := session.transitions.Load()
	w.cut(0)
	// The drop reaches the session before anything can reconnect: a reconnect is a dial and
	// two round trips to WhatsApp, and the drop is one goroutine away. Waited for rather than
	// slept on, so that the reconnect below is never the one that wins.
	for deadline := time.Now().Add(5 * time.Second); session.transitions.Load() == before; {
		if time.Now().After(deadline) {
			t.Fatal("the session never heard of the drop")
		}
		time.Sleep(time.Millisecond)
	}
	if back > 0 {
		time.Sleep(back)
		w.adopt(t.Context(), session.current())
	}

	select {
	case answer = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the command never answered")
	}
	// Anything the client was going to resend has been sent by the time the command answers,
	// but a frame in flight to the server may not have been counted yet.
	if _, late := w.nextFrame(200 * time.Millisecond); late {
		t.Log("a frame arrived after the answer")
	}
	return w.framesOn(0), w.framesOn(1), answer
}

// A group write that was on the wsWire when the connection went is not written again on the
// connection that replaces it. WhatsApp does not deduplicate an IQ across connections, so a
// second frame is a second group, a second rotation of the link, a second system message.
// What the client is told is `timeout`: the frame went, and whether WhatsApp applied it is
// what nobody here can say.
func TestAGroupWriteCaughtByADropIsNotWrittenAgain(t *testing.T) {
	t.Parallel()

	for _, write := range groupWrites {
		t.Run(write.name, func(t *testing.T) {
			t.Parallel()
			first, second, answer := interrupted(t, write.kind, write.payload, 300*time.Millisecond)
			if first != 1 || second != 0 {
				t.Errorf("the write reached WhatsApp %d time(s) on the first connection and %d on the "+
					"one that replaced it, want once in all", first, second)
			}
			if code := codeOf(answer.err); code != protocol.ErrorTimeout {
				t.Errorf("a write the drop caught answered %v (%q), want timeout", answer.err, code)
			}
			// The mark is what has the ledger answer a redelivery instead of writing again.
			// A departure is the one write the ledger does not hold.
			if marked := errors.Is(answer.err, engine.ErrMayHaveLanded); marked != (write.kind != protocol.CommandGroupLeave) {
				t.Errorf("a write the drop caught is marked as one that may have landed: %v", marked)
			}
		})
	}
}

// Past whatsmeow's five seconds the library gives up on its own and never resends; the
// frame still went, so the answer is the same.
func TestAGroupWriteCaughtByALongDropAnswersTimeout(t *testing.T) {
	t.Parallel()

	for _, write := range groupWrites[:5] {
		t.Run(write.name, func(t *testing.T) {
			t.Parallel()
			first, second, answer := interrupted(t, write.kind, write.payload, 0)
			if first != 1 || second != 0 {
				t.Errorf("the write reached WhatsApp %d+%d times, want once", first, second)
			}
			if code := codeOf(answer.err); code != protocol.ErrorTimeout {
				t.Errorf("a write the drop caught answered %v (%q), want timeout", answer.err, code)
			}
		})
	}
}

// answer hands every query waiting on the client the node built for its id, the way
// WhatsApp's reply would reach it, and reports how many it answered.
func (w *wsWire) answer(client *wm.Client, reply func(id string) *waBinary.Node) int {
	value := reflect.ValueOf(client).Elem()
	lock := (*sync.Mutex)(unsafe.Pointer(value.FieldByName("responseWaitersLock").UnsafeAddr()))
	waiters := value.FieldByName("responseWaiters")
	lock.Lock()
	ids := make([]string, 0, waiters.Len())
	for _, key := range waiters.MapKeys() {
		ids = append(ids, key.String())
	}
	lock.Unlock()
	for _, id := range ids {
		//nolint:staticcheck // SA1019: where a reply read off the socket is delivered
		client.DangerousInternals().ReceiveResponse(context.Background(), reply(id))
	}
	return len(ids)
}

// answered runs one write on a wired session and gives it the reply WhatsApp would, with
// no drop anywhere, and reports the frames it took and what the command answered.
func replied(t *testing.T, kind protocol.CommandType, payload string, reply func(id string) *waBinary.Node) (int, answered) {
	t.Helper()
	session, w := wiredSession(t)
	done := make(chan answered, 1)
	go func() {
		result, err := session.Execute(t.Context(), setCommand(t, kind, payload))
		done <- answered{result, err}
	}()
	if _, ok := w.nextFrame(5 * time.Second); !ok {
		t.Fatal("the write never reached the wire")
	}
	if w.answer(session.current(), reply) != 1 {
		t.Fatal("not exactly one query was waiting for its answer")
	}
	select {
	case got := <-done:
		return w.framesOn(0), got
	case <-time.After(10 * time.Second):
		t.Fatal("the command never answered")
		return 0, answered{}
	}
}

func resultFor(id string) *waBinary.Node {
	return &waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": id, "type": "result", "from": waTypes.GroupServerJID}}
}

// Nothing changes for a write nobody interrupts: one frame, and the answer it always had.
func TestAnUninterruptedGroupWriteIsWrittenOnceAndAnswered(t *testing.T) {
	t.Parallel()

	for _, write := range groupWrites {
		switch write.kind {
		case protocol.CommandGroupParticipantsUpdate, protocol.CommandGroupJoinRequestsUpdate,
			protocol.CommandGroupInviteGet, protocol.CommandGroupPhotoSet:
			// Their answers carry content a bare result does not; what they make of it is
			// covered next to each of them, and is not what a drop changes.
			continue
		}
		t.Run(write.name, func(t *testing.T) {
			t.Parallel()
			frames, got := replied(t, write.kind, write.payload, resultFor)
			if frames != 1 || got.err != nil {
				t.Errorf("an uninterrupted write took %d frame(s) and answered %v, want one and ok", frames, got.err)
			}
		})
	}
}

// A refusal is WhatsApp's answer, and an answer is not a drop: it is reported as the
// refusal, once.
func TestARefusedGroupWriteIsReportedAsTheRefusal(t *testing.T) {
	t.Parallel()

	frames, got := replied(t, protocol.CommandGroupNameSet, groupWrites[4].payload, func(id string) *waBinary.Node {
		return &waBinary.Node{
			Tag: "iq", Attrs: waBinary.Attrs{"id": id, "type": "error", "from": waTypes.GroupServerJID},
			Content: []waBinary.Node{{Tag: "error", Attrs: waBinary.Attrs{"code": 403, "text": "forbidden"}}},
		}
	})
	if frames != 1 {
		t.Errorf("a refused write took %d frames, want one", frames)
	}
	if code := codeOf(got.err); code != protocol.ErrorWaError {
		t.Errorf("a refused write answered %v (%q), want wa_error", got.err, code)
	}
}

// Two sessions dropped at once each keep their own write to one frame, and a write that
// arrives while its session is reconnecting is refused before it is written.
func TestDropsOnTwoSessionsAtOnceKeepEachWriteToOneFrame(t *testing.T) {
	t.Parallel()

	type wired struct {
		session *Session
		wire    *wsWire
		done    chan answered
	}
	sessions := []*wired{}
	for _, write := range []int{4, 0} { // a name, and a participant added
		session, w := wiredSession(t)
		one := &wired{session: session, wire: w, done: make(chan answered, 1)}
		go func() {
			result, err := session.Execute(t.Context(), setCommand(t, groupWrites[write].kind, groupWrites[write].payload))
			one.done <- answered{result, err}
		}()
		if _, ok := w.nextFrame(5 * time.Second); !ok {
			t.Fatal("a write never reached the wire")
		}
		sessions = append(sessions, one)
	}

	before := make([]int64, len(sessions))
	for i, one := range sessions {
		before[i] = one.session.transitions.Load()
	}
	var cuts sync.WaitGroup
	for _, one := range sessions {
		cuts.Go(func() { one.wire.cut(0) })
	}
	cuts.Wait()
	for i, one := range sessions {
		for deadline := time.Now().Add(5 * time.Second); one.session.transitions.Load() == before[i]; {
			if time.Now().After(deadline) {
				t.Fatal("a session never heard of its drop")
			}
			time.Sleep(time.Millisecond)
		}
	}

	// Delivered while the first session is between connections.
	_, err := sessions[0].session.Execute(t.Context(), setCommand(t, groupWrites[6].kind, groupWrites[6].payload))
	if code := codeOf(err); code != protocol.ErrorNotConnected {
		t.Errorf("a write delivered during the reconnect answered %v (%q), want not_connected", err, code)
	}

	for _, one := range sessions {
		one.wire.adopt(t.Context(), one.session.current())
	}
	for i, one := range sessions {
		select {
		case got := <-one.done:
			if code := codeOf(got.err); code != protocol.ErrorTimeout {
				t.Errorf("session %d's write answered %v (%q), want timeout", i, got.err, code)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("session %d's write never answered", i)
		}
		if first, second := one.wire.framesOn(0), one.wire.framesOn(1); first != 1 || second != 0 {
			t.Errorf("session %d's connections took %d and %d frames, want 1 and 0", i, first, second)
		}
	}
}

// A write the caller stopped waiting for is the caller's to answer for, not the drop's: it
// is not marked as one that may have landed, because the contract does not hold a key for a
// command its own deadline ended -- the deadline can end it before the write as easily as
// after.
func TestAGroupWriteTheCallerAbandonedIsNotHeldForTheDrop(t *testing.T) {
	t.Parallel()

	session, w := wiredSession(t)
	ctx, abandon := context.WithCancel(t.Context())
	done := make(chan answered, 1)
	go func() {
		result, err := session.Execute(ctx, setCommand(t, groupWrites[4].kind, groupWrites[4].payload))
		done <- answered{result, err}
	}()
	if _, ok := w.nextFrame(5 * time.Second); !ok {
		t.Fatal("the write never reached the wire")
	}
	abandon()
	select {
	case got := <-done:
		if errors.Is(got.err, errUnanswered) || errors.Is(got.err, engine.ErrMayHaveLanded) {
			t.Errorf("a write the caller abandoned answered as one the drop caught: %v", got.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("an abandoned write never answered")
	}
}
