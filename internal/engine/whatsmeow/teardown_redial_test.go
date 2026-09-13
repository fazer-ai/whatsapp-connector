package whatsmeow

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// holdTheDial puts the session's client where a reconnect puts it: inside a dial, holding
// whatsmeow's socket lock for as long as the dial lasts. The proxy takes the connection and
// never answers it, which is what a network that swallows the CONNECT looks like from here,
// and whatsmeow puts no ceiling of its own on that wait (fazer-ai/whatsapp-connector#187
// measured the lock held for as long as the proxy held the dial).
//
// The real library and the real lock, not a seam: what these tests are about is a call
// into whatsmeow that does not look at its context while the lock is taken, and a stand-in
// would only prove the stand-in.
func holdTheDial(t *testing.T, session *Session) {
	t.Helper()

	var listening net.ListenConfig
	listener, err := listening.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for the dial: %v", err)
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()

	client := session.current()
	client.SetProxy(http.ProxyURL(&url.URL{Scheme: "http", Host: listener.Addr().String()}))
	dialling, stop := context.WithCancel(context.Background())
	dialled := make(chan error, 1)
	go func() { dialled <- client.ConnectContext(dialling) }()

	select {
	case conn := <-accepted:
		// The lock is taken before the dial starts, so a connection reaching the proxy is a
		// dial holding it. Registered after the session's own cleanup, so it runs first: the
		// session closes on a client whose dial has already given up.
		t.Cleanup(func() {
			stop()
			<-dialled
			_ = conn.Close()
			_ = listener.Close()
		})
	case err := <-dialled:
		stop()
		_ = listener.Close()
		t.Fatalf("the dial ended before it reached the proxy: %v", err)
	case <-time.After(10 * time.Second):
		stop()
		_ = listener.Close()
		t.Fatal("the dial never reached the proxy")
	}
}

// answeredWithin runs a teardown on a caller's deadline and fails if it is still waiting
// long after that deadline. The bound is generous on purpose: what it separates is an
// answer that honours the caller from one that waits on something else -- the dial, which
// here lasts the whole test, or a bound of the session's own set far above this one.
func answeredWithin(t *testing.T, tearDown func(context.Context) error) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	answered := make(chan error, 1)
	go func() { answered <- tearDown(ctx) }()
	select {
	case err := <-answered:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the teardown was still waiting on the dial seconds after the caller's deadline")
		return nil
	}
}

// A logout that arrives while whatsmeow is redialling reaches the library, because the
// lifecycle commands do not pass readyToSend, and there the request waits for the socket
// under a lock that takes no context. The client's deadline has to end that wait: the
// dial can outlast it by any amount, and the client is owed an answer when its time is up.
//
// And the answer is the one a logout that never reached WhatsApp gets. Nothing was sent
// while the dial held the lock, so nothing was revoked: the pairing, the state and the
// reconnect all stay exactly as they were.
func TestALogoutWaitingOnARedialAnswersWithinTheCallersTime(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	session.setConnected(false)
	session.setReconnecting(true, time.Now())
	holdTheDial(t, session)

	err := answeredWithin(t, session.Logout)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Logout failed with %v, want the caller's deadline", err)
	}
	saysNothing(t, session, "for a logout that never got a socket to go out on")
	if state := session.state(); state != "reconnecting" {
		t.Fatalf("the session reports %q for a reconnect the library is still running", state)
	}
	if _, bound, err := container.For(session.sid).JID(t.Context()); err != nil || !bound {
		t.Fatalf("the pairing was forgotten anyway (bound=%v, err=%v)", bound, err)
	}
	if session.isStale() {
		t.Fatal("the session was marked stale for a logout that never left")
	}
	if resolvesAsUnpaired(t, session) {
		t.Fatal("the session answers not_paired for an account nothing revoked")
	}
}

// A delete in the same place gets the same bound, and then does what a delete whose unlink
// could not be made does: forgets the account anyway and says so. The client destroyed the
// inbox before sending it, so there is nobody left to keep the credentials for.
//
// Two waits stand in its way, not one. The unlink waits on the lock to send, and the
// rebuild after it waits on the same lock to close the client being thrown away; answering
// the first and then sitting in the second is the same hang one step later. Nor may the
// second end on the bound the local cleanup has for the store: closing a client nothing
// listens to any more is not store work, and spending that bound on it answers the caller
// a whole bound after its time. Set far above the test's own bound, so it cannot be what
// ends the wait here.
func TestADeleteWaitingOnARedialAnswersWithinTheCallersTime(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990002")
	session.storeLimit = time.Minute
	session.setConnected(false)
	session.setReconnecting(true, time.Now())
	holdTheDial(t, session)
	deleted := session.current()

	if err := answeredWithin(t, session.Delete); err != nil {
		t.Fatalf("a delete whose unlink ran out of time answered failure (%v); the teardown is finished, and a client that republishes on failure would retry it forever", err)
	}
	if _, bound, err := container.For(session.sid).JID(t.Context()); err != nil || bound {
		t.Fatalf("the delete kept the pairing (bound=%v, err=%v); the session goes on being adopted for an inbox that no longer exists", bound, err)
	}
	// Not waiting for the old client to close is not the same as not replacing it: a session
	// left on the deleted client answers every later command with a device that is gone.
	if session.current() == deleted {
		t.Fatal("the session is still on the client that was being dialled when it was deleted")
	}
	emission := next(t, session)
	if emission.Type != protocol.EventSessionLoggedOut {
		t.Fatalf("a delete published %q, want %q", emission.Type, protocol.EventSessionLoggedOut)
	}
	if reason := decode(t, emission.Payload)["reason"]; reason != "session_deleted" {
		t.Fatalf("a delete published logged_out with reason %v, want session_deleted", reason)
	}
}
