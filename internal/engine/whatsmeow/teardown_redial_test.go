package whatsmeow

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
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
//
// The returned function ends the dial, the way a network that comes back does; it also runs
// at cleanup, so a test that does not care can ignore it.
func holdTheDial(t *testing.T, session *Session) func() {
	t.Helper()

	return holdTheDialOf(t, session.current())
}

// holdTheDialOf is holdTheDial for a client the caller names, for the tests where the
// session is about to be put on a different one.
func holdTheDialOf(t *testing.T, client *wm.Client) func() {
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

	client.SetProxy(http.ProxyURL(&url.URL{Scheme: "http", Host: listener.Addr().String()}))
	dialling, stop := context.WithCancel(context.Background())
	dialled := make(chan error, 1)
	go func() { dialled <- client.ConnectContext(dialling) }()

	select {
	case conn := <-accepted:
		// The lock is taken before the dial starts, so a connection reaching the proxy is a
		// dial holding it. Registered after the session's own cleanup, so it runs first: the
		// session closes on a client whose dial has already given up.
		var once sync.Once
		release := func() {
			once.Do(func() {
				stop()
				<-dialled
				_ = conn.Close()
				_ = listener.Close()
			})
		}
		t.Cleanup(release)
		return release
	case err := <-dialled:
		stop()
		_ = listener.Close()
		t.Fatalf("the dial ended before it reached the proxy: %v", err)
	case <-time.After(10 * time.Second):
		stop()
		_ = listener.Close()
		t.Fatal("the dial never reached the proxy")
	}
	return func() {}
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

// The wait for the socket cannot be called off: a dial that outlives a teardown's deadline
// outlives the probe waiting on it too. So there is one probe and not one each, or a client
// retrying a logout every few seconds through an outage parks a goroutine for every attempt
// and none of them come back until the dial does.
func TestTeardownsWaitingOnTheSameDialShareOneProbe(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990003")
	session.setConnected(false)
	session.setReconnecting(true, time.Now())
	holdTheDial(t, session)

	var first chan struct{}
	for attempt := range 5 {
		if err := answeredWithin(t, session.Logout); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("logout %d failed with %v, want the caller's deadline", attempt+1, err)
		}
		// The probe itself, rather than a count of goroutines: the package's other tests
		// park probes of their own, and a process-wide stack dump cannot tell them apart.
		// One goroutine is started per probe, so the same probe is the same goroutine.
		session.mu.Lock()
		probe := session.probing
		session.mu.Unlock()
		if probe == nil {
			t.Fatalf("no probe is waiting on the socket after logout %d, so the next teardown starts another", attempt+1)
		}
		if first == nil {
			first = probe
		}
		if probe != first {
			t.Fatalf("logout %d started a probe of its own; a client retrying through an outage parks a goroutine for every attempt", attempt+1)
		}
	}
}

// And the sharing lasts as long as the dial it was about. A probe kept past the dial that
// ended answers the next teardown that the socket is free, on the strength of a lock that
// was free a minute ago -- and the teardown goes back to waiting for the new dial with no
// deadline over it, which is the whole of #187.
func TestAProbeIsNotReusedAfterTheDialItWaitedForEnded(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990004")
	session.setConnected(false)
	session.setReconnecting(true, time.Now())

	release := holdTheDial(t, session)
	if err := answeredWithin(t, session.Logout); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the logout during the first dial failed with %v, want the caller's deadline", err)
	}
	session.mu.Lock()
	probe := session.probing
	session.mu.Unlock()
	if probe == nil {
		t.Fatal("no probe is waiting on the first dial")
	}
	release()
	// The probe, not just the dial: it forgets itself before it closes, so waiting for it to
	// close is what makes "the next teardown finds no probe" true rather than likely.
	<-probe

	// The dial the first probe was waiting for is over, and another one starts, the way a
	// reconnect that fails and is tried again does.
	holdTheDial(t, session)
	if err := answeredWithin(t, session.Logout); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the logout during the second dial failed with %v, want the caller's deadline", err)
	}
}

// Shared between the teardowns of one client, and not across a replacement: a probe is
// about the lock of the client it was started on. Handed to a teardown on the client that
// took its place, it is an answer about somebody else's socket, and the teardown waits out
// its whole deadline for a dial that has nothing to do with it.
func TestAProbeIsNotSharedWithATeardownOnAnotherClient(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990005")
	session.storeLimit = time.Minute
	holdTheDial(t, session)

	if err := answeredWithin(t, session.Logout); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the logout during the dial failed with %v, want the caller's deadline", err)
	}

	// The session moves to a fresh client while that probe is still parked, which is what a
	// teardown's own rebuild does.
	spent, giveUp := context.WithCancel(t.Context())
	giveUp()
	if err := session.rebuild(t.Context(), spent); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	asked := make(chan struct{}, 1)
	session.logout = func(context.Context, *wm.Client) error {
		asked <- struct{}{}
		return wm.ErrNotConnected
	}

	if err := answeredWithin(t, session.Logout); !errors.Is(err, wm.ErrNotConnected) {
		t.Fatalf("the logout on the fresh client failed with %v, want the answer from the library", err)
	}
	select {
	case <-asked:
	default:
		t.Fatal("the logout never reached the library: it waited on a probe about the client it replaced")
	}
}

// The other half of not waiting for the old client to close: a dial the caller stopped
// waiting for ends whenever the network lets it, and by then a teardown has put the session
// on a client of its own. The close that dial reports is about a connection nobody has any
// more, and published anyway it lands on top of whatever replaced it -- a terminal state
// over a session that is pairing again, or over the logged_out that retired this one.
func TestADialThatEndsAfterItsClientWasReplacedSaysNothing(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990006")
	session.storeLimit = time.Minute

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
	t.Cleanup(func() { _ = listener.Close() })
	session.current().SetProxy(http.ProxyURL(&url.URL{Scheme: "http", Host: listener.Addr().String()}))

	// The connect the session itself makes, which is the one that leaves a watcher behind
	// to report how the dial ended.
	connecting, giveUpConnecting := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer giveUpConnecting()
	if err := session.Connect(connecting, engine.ConnectRequest{Pairing: "resume"}); err == nil {
		t.Fatal("a connect that never got past the proxy reported success")
	}
	var parked net.Conn
	select {
	case parked = <-accepted:
	case <-time.After(10 * time.Second):
		t.Fatal("the dial never reached the proxy")
	}
	if emission := next(t, session); emission.Type != protocol.EventSessionState {
		t.Fatalf("the connect published %q, want the session connecting", emission.Type)
	}

	dialling := session.current()
	spent, giveUp := context.WithCancel(t.Context())
	giveUp()
	if err := session.rebuild(t.Context(), spent); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	// The replacement is dialling by the time the old one gives up, which is the case that
	// matters: the state the old failure would clear is this dial's.
	session.setDialing(true)

	_ = parked.Close()
	// The old dial's own end, taken from the client that was dialling: its socket lock comes
	// free only once the dial is over, so everything the failure does has been done by the
	// time this returns.
	dialling.IsConnected()

	saysNothing(t, session, "for a dial that ended on a client the session had already replaced")
	session.mu.Lock()
	stillDialling := session.dialing
	session.mu.Unlock()
	if !stillDialling {
		// The same event arriving on the wrong session by another door: `session.status`
		// would answer `close` over a dial in flight, and a resume landing there would start
		// a second one alongside it.
		t.Fatal("a dial that ended on the replaced client cleared the flag of the one that took its place")
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
