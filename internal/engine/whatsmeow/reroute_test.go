package whatsmeow

import (
	"context"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// publishedState reads the next emission as a session state, or fails.
func publishedState(t *testing.T, session *Session) (state, reason string) {
	t.Helper()

	emission := next(t, session)
	if emission.Type != protocol.EventSessionState {
		t.Fatalf("published %q, want a session state", emission.Type)
	}
	body := decode(t, emission.Payload)
	state, _ = body["state"].(string)
	reason, _ = body["reason"].(string)
	return state, reason
}

// A paired session moved to another path is published as reconnecting, and says why.
//
// The socket goes down and comes back within the same connect, with nobody asking for
// anything more. Published as the `close` a `session.disconnect` publishes, the client
// could only read it as a connection ended on request, one that comes back when somebody
// connects it again: Chatwoot told the operator to do exactly that, for the two seconds
// the move took (fazer-ai/chatwoot#747).
func TestAMoveOfAPairedSessionIsPublishedAsAReconnect(t *testing.T) {
	t.Parallel()

	before, after := listenAsProxy(t), listenAsProxy(t)
	session, _ := newTestSession(t, "5511999990001")
	session.disconnect = func(*wm.Client) {}
	standOn(t, session, "socks5://"+before.addr)
	session.handle(&waEvents.Connected{})
	next(t, session)

	// The redial fails -- the listener hangs up -- and that is the connect's own outcome.
	// What this is about is what the hang-up said.
	_ = session.Connect(t.Context(), engine.ConnectRequest{
		Pairing: "resume", Proxy: &engine.ProxyRequest{URL: "http://" + after.addr},
	})
	if state, reason := publishedState(t, session); state != "reconnecting" || reason != reasonProxyChanged {
		t.Fatalf("the move published %s/%s, want reconnecting/%s", state, reason, reasonProxyChanged)
	}
}

// A session that has not paired has no connection to come back to: the connect that
// follows the move starts a pairing, not a reconnect, so the hang-up is the plain close.
func TestAMoveOfAnUnpairedSessionIsStillAClose(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "")
	session.disconnect = func(*wm.Client) {}
	standOn(t, session, "socks5://127.0.0.1:1")
	session.handle(&waEvents.Connected{})
	next(t, session)

	// Refused after the move, for want of a pairing to resume.
	_ = session.Connect(t.Context(), engine.ConnectRequest{
		Pairing: "resume", Proxy: &engine.ProxyRequest{URL: "socks5://127.0.0.1:2"},
	})
	if state, reason := publishedState(t, session); state != "close" || reason != "disconnect_requested" {
		t.Fatalf("the move published %s/%s, want close/disconnect_requested", state, reason)
	}
}

// A move whose hang-up outlived its command is not followed by any redial: the connect
// was answered as failed and stopped there. The socket that goes down afterwards is then
// down for good, and reporting it as a reconnect would leave the client waiting on one
// nobody is making.
func TestAMoveWhoseHangUpOutlivedItsCommandIsAClose(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	release := make(chan struct{})
	session.disconnect = func(*wm.Client) { <-release }
	standOn(t, session, "socks5://127.0.0.1:1")
	session.handle(&waEvents.Connected{})
	next(t, session)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := session.Connect(ctx, engine.ConnectRequest{
		Pairing: "resume", Proxy: &engine.ProxyRequest{URL: "socks5://127.0.0.1:2"},
	}); err == nil {
		t.Fatal("a move whose hang-up never finished answered success")
	}
	close(release)
	if state, reason := publishedState(t, session); state != "close" || reason != "disconnect_requested" {
		t.Fatalf("the late hang-up published %s/%s, want close/disconnect_requested", state, reason)
	}
}

// A hang-up that finished as the command gave up is the move's, not a late one.
//
// The two can meet: the socket is down and the hang-up has already decided to publish the
// reconnect when the deadline runs out. Answering the connect as failed then would stop
// the redial that reconnect promised, and the client would wait on it for good. Held
// here by the transition lock, which the hang-up takes to publish after it has decided.
func TestAMoveDecidedAsTheDeadlineRunsOutGoesOn(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	disconnected := make(chan struct{})
	session.disconnect = func(*wm.Client) { close(disconnected) }
	standOn(t, session, "socks5://127.0.0.1:1")
	session.handle(&waEvents.Connected{})
	next(t, session)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	session.transition.Lock()
	answered := make(chan error, 1)
	go func() {
		answered <- session.Connect(ctx, engine.ConnectRequest{
			Pairing: "resume", Proxy: &engine.ProxyRequest{URL: "socks5://127.0.0.1:2"},
		})
	}()
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		session.transition.Unlock()
		t.Fatal("the move never hung up")
	}
	// The hang-up is past its decision and waiting on the lock; the command gives up now.
	waitFor(t, func() bool { return hangUpDecided(session) }, "the hang-up never decided how to report itself")
	cancel()
	session.transition.Unlock()

	if state, reason := publishedState(t, session); state != "reconnecting" || reason != reasonProxyChanged {
		t.Fatalf("the move published %s/%s, want reconnecting/%s", state, reason, reasonProxyChanged)
	}
	// And the redial it promised follows. Carried on before the hang-up had finished, the
	// connect would still read the session as open and leave the socket down.
	if state, _ := publishedState(t, session); state != "connecting" {
		t.Fatalf("the move was followed by %s, want the redial's connecting", state)
	}
	if got := session.proxyURL(); got != "socks5://127.0.0.1:2" {
		t.Fatalf("the session records %q after a move whose socket did go down", got)
	}
	<-answered
}

// A disconnect is not a move, however the session got there: nothing redials after it.
func TestADisconnectOfAPairedSessionIsStillAClose(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.disconnect = func(*wm.Client) {}
	standOn(t, session, "socks5://127.0.0.1:1")
	session.handle(&waEvents.Connected{})
	next(t, session)

	if err := session.Disconnect(t.Context()); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if state, reason := publishedState(t, session); state != "close" || reason != "disconnect_requested" {
		t.Fatalf("the disconnect published %s/%s, want close/disconnect_requested", state, reason)
	}
}

// hangUpDecided reports whether the latest hang-up has claimed how it will be published.
func hangUpDecided(session *Session) bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.hangUpClaim != nil && session.hangUpClaim.Load() == hangUpSettled
}

// A hang-up that is already over is not waited on, even by a command out of time.
//
// The move above reaches here with its deadline gone and the reconnect published, and
// only the redial after this makes that reconnect true. Asked of a select that has both
// ready, the answer would be a coin toss; repeated so a coin toss cannot pass.
func TestAHangUpAlreadyOverIsNotAnErrorForACommandOutOfTime(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	over := make(chan struct{})
	close(over)
	session.mu.Lock()
	session.hangingUp = over
	session.mu.Unlock()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for range 64 {
		if err := session.awaitHangUp(ctx); err != nil {
			t.Fatalf("a hang-up already over answered %v", err)
		}
	}
}
