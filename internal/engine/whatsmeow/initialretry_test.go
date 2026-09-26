package whatsmeow

import (
	"testing"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/testwait"
)

// A resume whose dial fails for a reason that is not terminal is tried again, without
// anybody asking.
//
// It used to be the end of it: whatsmeow retries the first dial only when told to, and
// nothing told it, so the error came back, `close` went out with `connect_failed`, and
// the account stayed adopted on this instance holding its lease. The sweep does not ask
// about an account somebody runs and nothing retires a failure that is not terminal, so
// nothing came back for it, ever (#280).
//
// The proxy here takes the connection and hangs up in the middle of the SOCKS handshake,
// which reaches whatsmeow as a network error -- the kind it calls retryable. The second
// connection is the statement: nothing on this side dialled it.
func TestAResumeWhoseDialFailedIsTriedAgain(t *testing.T) {
	t.Parallel()

	proxy := listenAsProxy(t)
	session, _ := newTestSession(t, "5511999990001")
	onProxy := "socks5://" + proxy.addr
	standOn(t, session, onProxy)

	err := session.Connect(t.Context(), engine.ConnectRequest{
		Pairing: "resume", Proxy: &engine.ProxyRequest{URL: onProxy},
	})
	if said := proxy.next(t); said != "socks5" {
		t.Fatalf("the resume reached the proxy saying %q", said)
	}
	if said := proxy.next(t); said != "socks5" {
		t.Fatalf("the retry reached the proxy saying %q", said)
	}
	if err != nil {
		t.Fatalf("the connect answered %v for a dial whatsmeow is retrying", err)
	}

	// And the client is told the account is on its way back, not that it is down.
	deadline := time.After(testwait.Budget)
	for {
		select {
		case emission := <-session.Events():
			if emission.Type != protocol.EventSessionState {
				continue
			}
			switch stateOf(t, &emission) {
			case "reconnecting":
				return
			case "close":
				t.Fatalf("a dial whatsmeow is retrying was published as close: %s", emission.Payload)
			}
		case <-deadline:
			t.Fatal("nothing said the account was reconnecting")
		}
	}
}

// A pairing whose first dial fails still fails. whatsmeow's reconnect loop does nothing
// for a client with no device id yet, so a pairing handed to it would answer nil, announce
// a drop, and never dial again: the operator would be left on a QR screen waiting for a
// code that is not coming, told nothing about the network that was away.
func TestAPairingWhoseDialFailedStillFails(t *testing.T) {
	t.Parallel()

	for _, pairing := range []string{"qr", "code"} {
		t.Run(pairing, func(t *testing.T) {
			t.Parallel()

			proxy := listenAsProxy(t)
			session, _ := newTestSession(t, "")
			onProxy := "socks5://" + proxy.addr
			standOn(t, session, onProxy)

			request := engine.ConnectRequest{Pairing: pairing, Proxy: &engine.ProxyRequest{URL: onProxy}}
			if pairing == "code" {
				request.Phone = "5511999990002"
			}
			err := session.Connect(t.Context(), request)
			if said := proxy.next(t); said != "socks5" {
				t.Fatalf("the pairing reached the proxy saying %q", said)
			}
			if err == nil {
				t.Fatal("a pairing whose dial failed answered as though it had started")
			}
		})
	}
}
