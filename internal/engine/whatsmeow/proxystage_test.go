package whatsmeow

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/testwait"
)

// deadProxy is an address nothing listens on any more: a dial to it is refused at once,
// which is what a proxy whose process went away looks like from here.
func deadProxy(t *testing.T) string {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return addr
}

// answeringProxy is an HTTP proxy that answers every CONNECT with the status line it is
// given and then hangs up. `502 Bad Gateway` is a proxy refusing the tunnel; `200 Connection
// established` is a proxy that opened it and a far end that never spoke through it.
func answeringProxy(t *testing.T, status string) string {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				_ = conn.SetDeadline(time.Now().Add(testwait.Budget))
				reader := bufio.NewReader(conn)
				for {
					line, err := reader.ReadString('\n')
					if err != nil || strings.TrimSpace(line) == "" {
						break
					}
				}
				_, _ = conn.Write([]byte("HTTP/1.1 " + status + "\r\n\r\n"))
			}()
		}
	}()
	return listener.Addr().String()
}

// reconnectingOn is a paired session on the given proxy whose socket just dropped, with
// the `open` and the `reconnecting` that tells the client so already read off it.
func reconnectingOn(t *testing.T, proxyURL string) *Session {
	t.Helper()

	session, _ := newTestSession(t, "5511999990001")
	standOn(t, session, proxyURL)
	session.handle(&waEvents.Connected{})
	if state := decode(t, next(t, session).Payload)["state"]; state != "open" {
		t.Fatalf("the session published %v before its socket dropped", state)
	}
	session.handle(&waEvents.Disconnected{})
	if got := decode(t, next(t, session).Payload); got["state"] != "reconnecting" {
		t.Fatalf("the drop published %v", got)
	}
	return session
}

// redial is one of whatsmeow's own attempts to bring the socket back, through the route.
func redial(t *testing.T, session *Session) {
	t.Helper()

	_ = reach(t, &http.Client{Transport: &session.route.websocket})
}

// nextState is the next thing the session published, which has to be a session.state.
func nextState(t *testing.T, session *Session) map[string]any {
	t.Helper()

	emission := next(t, session)
	if emission.Type != protocol.EventSessionState {
		t.Fatalf("the session published %s where a session.state was due", emission.Type)
	}
	return decode(t, emission.Payload)
}

// publishesNothing fails if the session publishes anything within the window.
func publishesNothing(t *testing.T, session *Session, window time.Duration) {
	t.Helper()

	select {
	case emission := <-session.Events():
		t.Fatalf("the session published %s %s when nothing had changed", emission.Type, emission.Payload)
	case <-time.After(window):
	}
}

// A socket that cannot get back because the proxy is gone says so, once.
//
// Measured on the bench before this (#335): one `reconnecting` with `disconnected` and then
// silence, while whatsmeow redialled every few seconds into a refused proxy that only the
// connector's log mentioned. A client could not tell that from a network outage.
func TestADroppedSessionWhoseProxyIsGoneSaysSoOnce(t *testing.T) {
	t.Parallel()

	addr := deadProxy(t)
	session := reconnectingOn(t, "http://user:secret@"+addr)

	for range 3 {
		redial(t, session)
	}
	got := nextState(t, session)
	if got["state"] != "reconnecting" || got["reason"] != reasonProxyUnreachable {
		t.Fatalf("a redial refused by the proxy published %v", got)
	}
	for key, value := range got {
		text, _ := value.(string)
		for _, leak := range []string{addr, "secret", "127.0.0.1", "proxyconnect", "refused"} {
			if strings.Contains(text, leak) {
				t.Fatalf("the state carried %q in %s: %v", leak, key, got)
			}
		}
	}
	publishesNothing(t, session, 300*time.Millisecond)
}

// A proxy that is up and refuses the tunnel is the proxy too: the account cannot leave
// through it, whatever the reason it gives.
func TestAProxyThatRefusesTheTunnelIsNamed(t *testing.T) {
	t.Parallel()

	session := reconnectingOn(t, "http://"+answeringProxy(t, "502 Bad Gateway"))

	redial(t, session)
	if got := nextState(t, session); got["reason"] != reasonProxyUnreachable {
		t.Fatalf("a CONNECT answered 502 published %v", got)
	}
}

// Past the proxy is not the proxy's fault. A tunnel that opens and carries nothing back is
// WhatsApp or the network behind the proxy, and blaming the proxy would send the operator
// to change something that works.
func TestAFailurePastTheProxyIsNotBlamedOnIt(t *testing.T) {
	t.Parallel()

	session := reconnectingOn(t, "http://"+answeringProxy(t, "200 Connection established"))

	redial(t, session)
	redial(t, session)
	publishesNothing(t, session, 300*time.Millisecond)
}

// Once the proxy answers again, the blame goes with it: a session still down after that is
// down for another reason, and a client left showing the proxy as the cause would be
// showing the wrong thing for as long as the outage lasts.
func TestTheBlameLeavesTheProxyOnceItAnswersAgain(t *testing.T) {
	t.Parallel()

	addr := answeringProxy(t, "200 Connection established")
	session := reconnectingOn(t, "http://"+addr)
	session.route.reported(session.route.generation.Load(), false)
	if got := nextState(t, session); got["reason"] != reasonProxyUnreachable {
		t.Fatalf("a proxy failure published %v", got)
	}

	redial(t, session)
	got := nextState(t, session)
	if got["state"] != "reconnecting" || got["reason"] == reasonProxyUnreachable {
		t.Fatalf("a redial the proxy let through published %v", got)
	}
	publishesNothing(t, session, 300*time.Millisecond)
}

// Every outage is its own: a proxy that fails, comes back, and fails again is named again.
func TestAProxyThatFailsAgainAfterTheSessionCameBackIsNamedAgain(t *testing.T) {
	t.Parallel()

	session := reconnectingOn(t, "http://"+deadProxy(t))
	redial(t, session)
	if got := nextState(t, session); got["reason"] != reasonProxyUnreachable {
		t.Fatalf("the first outage published %v", got)
	}

	session.handle(&waEvents.Connected{})
	if got := nextState(t, session); got["state"] != "open" {
		t.Fatalf("the session came back publishing %v", got)
	}
	session.handle(&waEvents.Disconnected{})
	_ = nextState(t, session)

	redial(t, session)
	if got := nextState(t, session); got["reason"] != reasonProxyUnreachable {
		t.Fatalf("the second outage published %v", got)
	}
}

// A socket that is up has nothing to say about a failed dial: the media client dials
// through the same proxy, and a download that could not start is not the session down.
func TestAProxyFailureWhileTheSocketIsUpChangesNothing(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	standOn(t, session, "http://"+deadProxy(t))
	session.handle(&waEvents.Connected{})
	_ = nextState(t, session)

	redial(t, session)
	publishesNothing(t, session, 300*time.Millisecond)
}

// A session that goes out directly has no proxy to blame, and its dials report nothing.
func TestTheDirectRouteReportsNothing(t *testing.T) {
	t.Parallel()

	calls := 0
	transport, err := egressTransportReporting("", func(bool) { calls++ })
	if err != nil {
		t.Fatalf("egressTransportReporting: %v", err)
	}
	if conn, err := transport.DialContext(t.Context(), "tcp", deadProxy(t)); err == nil {
		_ = conn.Close()
		t.Fatal("a dial to an address nothing listens on succeeded")
	}
	if calls != 0 {
		t.Fatalf("the direct route reported %d proxy outcomes", calls)
	}
}

// A SOCKS5 proxy that cannot be reached is reported like an HTTP one.
func TestASOCKS5ProxyThatCannotBeReachedIsReported(t *testing.T) {
	t.Parallel()

	var outcomes []bool
	transport, err := egressTransportReporting("socks5://"+deadProxy(t), func(reached bool) {
		outcomes = append(outcomes, reached)
	})
	if err != nil {
		t.Fatalf("egressTransportReporting: %v", err)
	}
	if conn, err := transport.DialContext(t.Context(), "tcp", "web.whatsapp.com:443"); err == nil {
		_ = conn.Close()
		t.Fatal("a dial through a proxy nothing listens on succeeded")
	}
	if len(outcomes) != 1 || outcomes[0] {
		t.Fatalf("the SOCKS5 route reported %v", outcomes)
	}
}

// A resume that cannot get past its proxy says so too. whatsmeow goes on retrying the dial
// the connector started, so the session does not close: it names the proxy from
// `connecting`, or from `reconnecting` if whatsmeow's own Disconnected for the failed dial
// is handled first. Which of the two is scheduling, and both are the documented states.
func TestAResumeThatCannotReachItsProxyNamesIt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	// The answer is not the point: the resume answers once its dial is under way.
	_ = session.Connect(t.Context(), engine.ConnectRequest{
		Pairing: "resume", Proxy: &engine.ProxyRequest{URL: "http://" + deadProxy(t)},
	})
	for {
		got := nextState(t, session)
		if got["reason"] == reasonProxyUnreachable {
			if got["state"] != "connecting" && got["state"] != "reconnecting" {
				t.Fatalf("the resume named the proxy on %v", got)
			}
			return
		}
	}
}

// Another state going out in between is what the client now reads, so the next failure at
// the proxy names it again rather than being taken for a repeat. Measured by review before
// this: a Disconnected published after the proxy was named left it counted as named, and
// every later failure was held back for good.
func TestAStatePublishedInBetweenLetsTheProxyBeNamedAgain(t *testing.T) {
	t.Parallel()

	session := reconnectingOn(t, "http://"+deadProxy(t))
	redial(t, session)
	if got := nextState(t, session); got["reason"] != reasonProxyUnreachable {
		t.Fatalf("the failure published %v", got)
	}
	session.handle(&waEvents.Disconnected{})
	if got := nextState(t, session); got["reason"] == reasonProxyUnreachable {
		t.Fatalf("the drop published %v", got)
	}

	redial(t, session)
	if got := nextState(t, session); got["reason"] != reasonProxyUnreachable {
		t.Fatalf("the failure after the drop published %v", got)
	}
}

// A proxy reached and gone before it answered the CONNECT -- hung up, or a TLS handshake
// with an HTTPS proxy that failed -- is a failure at the proxy, though neither the dial nor
// the CONNECT answer says so on its own.
func TestAProxyThatHangsUpBeforeAnsweringIsNamed(t *testing.T) {
	t.Parallel()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	session := reconnectingOn(t, "http://"+listener.Addr().String())

	redial(t, session)
	if got := nextState(t, session); got["reason"] != reasonProxyUnreachable {
		t.Fatalf("a proxy that hung up on the CONNECT published %v", got)
	}
}

// A dial still under way on a path the session has left is about a proxy it no longer
// uses: blaming it would name the wrong cause for a session now going out another way.
func TestAnOutcomeFromAPathTheSessionLeftIsDropped(t *testing.T) {
	t.Parallel()

	session := reconnectingOn(t, "http://"+deadProxy(t))
	left := session.route.websocket.current.Load()
	if err := session.route.set(""); err != nil {
		t.Fatalf("route.set: %v", err)
	}

	_ = reach(t, &http.Client{Transport: &swappedAt{left}})
	publishesNothing(t, session, 300*time.Millisecond)
}

// swappedAt is a swappedTransport pinned to one transport, the way a request that started
// before a move goes on with the one it started on.
type swappedAt struct{ transport *http.Transport }

func (s *swappedAt) RoundTrip(request *http.Request) (*http.Response, error) {
	var pinned swappedTransport
	pinned.current.Store(s.transport)
	return pinned.RoundTrip(request)
}

// The media client dials the same proxy, but a download that could not start says nothing
// about whether the account can get back online, so it does not name the proxy either.
func TestAMediaDialThatFailsAtTheProxyNamesNothing(t *testing.T) {
	t.Parallel()

	session := reconnectingOn(t, "http://"+deadProxy(t))
	_ = reach(t, &http.Client{Transport: &session.route.media})
	publishesNothing(t, session, 300*time.Millisecond)
}

// Outcomes are judged off the dial's goroutine, so two can be judged out of order. A
// failure that lands after the success that followed it is old news: publishing it would
// name the proxy over a proxy that already answered.
func TestAnOutcomeJudgedLateIsDropped(t *testing.T) {
	t.Parallel()

	session := reconnectingOn(t, "http://"+deadProxy(t))
	session.judgeProxy(session.route.generation.Load(), 1, false)
	if got := nextState(t, session); got["reason"] != reasonProxyUnreachable {
		t.Fatalf("the failure published %v", got)
	}
	session.judgeProxy(session.route.generation.Load(), 3, true)
	if got := nextState(t, session); got["reason"] == reasonProxyUnreachable {
		t.Fatalf("the answer published %v", got)
	}

	session.judgeProxy(session.route.generation.Load(), 2, false)
	publishesNothing(t, session, 300*time.Millisecond)
}

// A dial its caller gave up on failed because somebody stopped waiting, not because of the
// proxy, and says nothing about it.
func TestADialItsCallerGaveUpOnReportsNothing(t *testing.T) {
	t.Parallel()

	calls := 0
	transport, err := egressTransportReporting("http://"+deadProxy(t), func(bool) { calls++ })
	if err != nil {
		t.Fatalf("egressTransportReporting: %v", err)
	}
	gaveUp, cancel := context.WithCancel(t.Context())
	cancel()
	if conn, err := transport.DialContext(gaveUp, "tcp", deadProxy(t)); err == nil {
		_ = conn.Close()
		t.Fatal("a dial whose caller had given up succeeded")
	}
	if calls != 0 {
		t.Fatalf("a dial its caller gave up on reported %d proxy outcomes", calls)
	}
}

// The route drops an outcome from a path it has left before telling anybody: the check at
// the session covers only the judgements already queued when the route moved.
func TestTheRouteDropsAnOutcomeFromAPathItLeft(t *testing.T) {
	t.Parallel()

	route := newEgressRoute()
	told := 0
	route.notify = func(uint64, uint64, bool) { told++ }
	if err := route.set("http://" + deadProxy(t)); err != nil {
		t.Fatalf("route.set: %v", err)
	}
	left := route.generation.Load()
	if err := route.set(""); err != nil {
		t.Fatalf("route.set: %v", err)
	}

	route.reported(left, false)
	if told != 0 {
		t.Fatalf("an outcome from the path the route left was passed on %d times", told)
	}
}

// A judgement queued before the route moved runs after it, and is dropped then.
func TestAJudgementQueuedBeforeAMoveIsDropped(t *testing.T) {
	t.Parallel()

	session := reconnectingOn(t, "http://"+deadProxy(t))
	left := session.route.generation.Load()
	if err := session.route.set(""); err != nil {
		t.Fatalf("route.set: %v", err)
	}

	session.judgeProxy(left, 1, false)
	publishesNothing(t, session, 300*time.Millisecond)
}

// A dial this connector started, and whatsmeow went on retrying, is `connecting`: the proxy
// is named from there too. Set by hand, because which of `connecting` and `reconnecting` a
// real failed resume shows first is whatsmeow's scheduling.
func TestAProxyFailureWhileConnectingIsNamedFromConnecting(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	standOn(t, session, "http://"+deadProxy(t))
	session.setDialing(true)

	session.judgeProxy(session.route.generation.Load(), 1, false)
	got := nextState(t, session)
	if got["state"] != "connecting" || got["reason"] != reasonProxyUnreachable {
		t.Fatalf("a proxy failure while connecting published %v", got)
	}
}
