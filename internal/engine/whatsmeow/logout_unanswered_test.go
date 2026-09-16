package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// A logout whose request was written to the socket and never answered has not revoked
// anything anyone knows of. whatsmeow's Logout returns from the request step before it
// disconnects or touches the store, so the credentials are exactly as good as they were,
// and the device is still listed on the phone. Answering that the account is gone there
// takes it off the air for good while it stays linked: what fazer-ai/whatsapp-connector#203
// measured, on a logout sent in the window WhatsApp closes the stream after a pairing.
//
// Each case is the error as whatsmeow builds it, wrapped the way its Logout wraps it.
var unansweredLogouts = map[string]func(context.Context) error{
	"the socket dropped before the answer": func(context.Context) error {
		return fmt.Errorf("error sending logout request: %w", &wm.DisconnectedError{Action: "info query"})
	},
	// retryFrame names the action differently, and DisconnectedError.Is compares the name,
	// so this one is not ErrIQDisconnected as far as errors.Is is concerned.
	"the socket dropped again on the retry": func(context.Context) error {
		return fmt.Errorf("error sending logout request: %w", &wm.DisconnectedError{Action: "info query (retry)"})
	},
	"the answer never came in time": func(context.Context) error {
		return fmt.Errorf("error sending logout request: %w", wm.ErrIQTimedOut)
	},
	"the command ran out of time waiting": func(ctx context.Context) error {
		<-ctx.Done()
		return fmt.Errorf("error sending logout request: %w", ctx.Err())
	},
	"the command was called off while waiting": func(context.Context) error {
		return fmt.Errorf("error sending logout request: %w", context.Canceled)
	},
}

// logoutWithin runs a logout on a context of its own, short enough for the case that
// waits on it and long enough for every other one.
func logoutWithin(t *testing.T, session *Session) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	return session.Logout(ctx)
}

// saysNothing fails on anything the session publishes within the window.
func saysNothing(t *testing.T, session *Session, why string) {
	t.Helper()

	select {
	case emission := <-session.Events():
		t.Fatalf("published %q %s", emission.Type, why)
	case <-time.After(300 * time.Millisecond):
	}
}

// loggedOutReasons collects the reason of every session.logged_out published within the
// window, and nothing else.
func loggedOutReasons(t *testing.T, session *Session) []any {
	t.Helper()

	var reasons []any
	window := time.After(300 * time.Millisecond)
	for {
		select {
		case emission, open := <-session.Events():
			if !open {
				return reasons
			}
			if emission.Type == protocol.EventSessionLoggedOut {
				reasons = append(reasons, decode(t, emission.Payload)["reason"])
			}
		case <-window:
			return reasons
		}
	}
}

// resolvesAsUnpaired reports whether a contact.resolve of the account's own number is
// refused as an account this session no longer has.
func resolvesAsUnpaired(t *testing.T, session *Session) bool {
	t.Helper()

	_, err := session.Execute(t.Context(), resolveCommand(t, `{"party":{"kind":"phone","id":"5511999990001"}}`))
	var coded *protocol.Error
	return errors.As(err, &coded) && coded.Code == protocol.ErrorNotPaired
}

func TestALogoutThatLostItsAnswerLeavesTheSessionAsItWas(t *testing.T) {
	t.Parallel()

	for name, logout := range unansweredLogouts {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.logout = func(ctx context.Context, _ *wm.Client) error { return logout(ctx) }
			session.setConnected(false)
			session.setReconnecting(true, time.Now())

			err := logoutWithin(t, session)
			if err == nil {
				t.Fatal("a logout nobody answered was reported as one that went through")
			}
			saysNothing(t, session, "for a logout WhatsApp never answered")
			if state := session.state(); state != "reconnecting" {
				t.Fatalf("the session reports %q for a reconnect the library is still running", state)
			}
			if resolvesAsUnpaired(t, session) {
				t.Fatal("the session answers not_paired for an account nothing revoked")
			}
		})
	}
}

// The error the client gets is the one whatsmeow gave, so a caller deciding whether to
// retry can still tell a dropped socket apart.
func TestALogoutThatLostItsAnswerSaysWhy(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.logout = func(context.Context, *wm.Client) error {
		return fmt.Errorf("error sending logout request: %w", &wm.DisconnectedError{Action: "info query"})
	}

	if err := logoutWithin(t, session); !errors.Is(err, wm.ErrIQDisconnected) {
		t.Fatalf("Logout failed with %v, want the disconnect whatsmeow reported", err)
	}
}

// Nothing a later step reads may say the account is gone: the pairing, and the record the
// resume sweep brings the account back from, both stay.
func TestALogoutThatLostItsAnswerKeepsThePairing(t *testing.T) {
	t.Parallel()

	for name, logout := range unansweredLogouts {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			session, container := newTestSession(t, "5511999990001")
			if err := session.store.PutDesiredConnected(t.Context(), store.Wants{}); err != nil {
				t.Fatalf("PutDesiredConnected: %v", err)
			}
			session.logout = func(ctx context.Context, _ *wm.Client) error { return logout(ctx) }

			if err := logoutWithin(t, session); err == nil {
				t.Fatal("a logout nobody answered was reported as one that went through")
			}
			if session.isStale() {
				t.Fatal("the session was marked stale, so the next connect would delete credentials that still resume")
			}
			if _, bound, err := container.For(session.sid).JID(t.Context()); err != nil || !bound {
				t.Fatalf("the pairing was forgotten anyway (bound=%v, err=%v)", bound, err)
			}
			wanted, err := container.Wanted(t.Context())
			if err != nil {
				t.Fatalf("Wanted: %v", err)
			}
			if len(wanted) != 1 || wanted[0].SID != session.sid {
				t.Fatalf("the store wants %v after the logout, want %s: no sweep would bring this account back", wanted, session.sid)
			}
		})
	}
}

// The reconnect that was already under way lands on a session still willing to take it.
// A guard raised by the logout would have it close the socket the moment it came back.
func TestAReconnectAfterALogoutThatLostItsAnswerComesBackUp(t *testing.T) {
	t.Parallel()

	for name, logout := range unansweredLogouts {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.logout = func(ctx context.Context, _ *wm.Client) error { return logout(ctx) }
			session.setConnected(false)
			session.setReconnecting(true, time.Now())

			if err := logoutWithin(t, session); err == nil {
				t.Fatal("a logout nobody answered was reported as one that went through")
			}
			drain(t, session)

			session.handle(&waEvents.Connected{})

			if state := session.state(); state != "open" {
				t.Fatalf("the session reports %q after a reconnect it should have kept", state)
			}
			emission := next(t, session)
			if emission.Type != protocol.EventSessionState {
				t.Fatalf("published %q, want the session coming back up", emission.Type)
			}
			if state := decode(t, emission.Payload)["state"]; state != "open" {
				t.Fatalf("published the session as %v, want open", state)
			}
		})
	}
}

// The client's retry is the other half: once WhatsApp does answer, the logout is a logout,
// said once.
func TestARetriedLogoutAfterALostAnswerLogsOutOnce(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	if err := session.store.PutDesiredConnected(t.Context(), store.Wants{}); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
	}
	var calls atomic.Int32
	var answered atomic.Bool
	session.logout = func(context.Context, *wm.Client) error {
		calls.Add(1)
		if answered.Load() {
			return nil
		}
		return fmt.Errorf("error sending logout request: %w", &wm.DisconnectedError{Action: "info query"})
	}
	session.setConnected(true)

	if err := logoutWithin(t, session); err == nil {
		t.Fatal("a logout nobody answered was reported as one that went through")
	}
	saysNothing(t, session, "for a logout WhatsApp never answered")

	answered.Store(true)
	if err := session.Logout(t.Context()); err != nil {
		t.Fatalf("the retried logout: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("the unlink was attempted %d times, want the retry to reach WhatsApp again", got)
	}
	if reasons := loggedOutReasons(t, session); len(reasons) != 1 || reasons[0] != "logout_requested" {
		t.Fatalf("published logged_out with reasons %v, want logout_requested exactly once", reasons)
	}
	if _, bound, err := container.For(session.sid).JID(t.Context()); err != nil || bound {
		t.Fatalf("the device survived the logout WhatsApp accepted (bound=%v, err=%v)", bound, err)
	}
	var desired int
	if err := container.DB().QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM wac_session_desired WHERE sid = '`+session.sid+`'`).Scan(&desired); err != nil {
		t.Fatalf("read the desired state back: %v", err)
	}
	if desired != 0 {
		t.Fatal("the desired state outlived the logout; a sweep would go on trying to bring the account back")
	}
	if state := session.state(); state == "open" {
		t.Fatal("the session reports open after WhatsApp accepted the logout")
	}
}

// And when WhatsApp did process the request that lost its answer, it says so on the
// reconnect, and that is the path that ends the session: on a fact, once.
func TestWhatsappConfirmingALogoutThatLostItsAnswerStillLogsOut(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	session.logout = func(context.Context, *wm.Client) error {
		return fmt.Errorf("error sending logout request: %w", &wm.DisconnectedError{Action: "info query"})
	}
	session.setConnected(false)
	session.setReconnecting(true, time.Now())

	if err := logoutWithin(t, session); err == nil {
		t.Fatal("a logout nobody answered was reported as one that went through")
	}

	session.handle(&waEvents.LoggedOut{OnConnect: true, Reason: waEvents.ConnectFailureLoggedOut})

	if reasons := loggedOutReasons(t, session); len(reasons) != 1 {
		t.Fatalf("published logged_out %d times (reasons %v), want once", len(reasons), reasons)
	}
	deadline := time.Now().Add(2 * session.storeLimit)
	for {
		_, bound, err := container.For(session.sid).JID(t.Context())
		if err == nil && !bound {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the device survived WhatsApp logging the account out (bound=%v, err=%v)", bound, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	session.handle(&waEvents.Connected{})
	if state := session.state(); state == "open" {
		t.Fatal("a late connect put a logged-out session back up")
	}
}

// What did not change: a logout whatsmeow got past the request on is one WhatsApp
// accepted, whatever the local cleanup did afterwards, including a store that stalled
// until the command's time ran out.
func TestALogoutWhoseCleanupFailedIsStillALogout(t *testing.T) {
	t.Parallel()

	for name, failure := range map[string]error{
		"the store refused the deletion":     fmt.Errorf("error deleting data from store: %w", errors.New("disk full")),
		"the store stalled past the command": fmt.Errorf("error deleting data from store: %w", context.DeadlineExceeded),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.logout = func(context.Context, *wm.Client) error { return failure }
			session.setConnected(true)

			if err := session.Logout(t.Context()); err == nil {
				t.Fatal("a logout that failed was reported as one that went through")
			}
			if reasons := loggedOutReasons(t, session); len(reasons) != 1 || reasons[0] != "logout_requested" {
				t.Fatalf("published logged_out with reasons %v, want logout_requested exactly once", reasons)
			}
			if state := session.state(); state == "open" {
				t.Fatal("the session reports open over a device WhatsApp revoked")
			}
			if !resolvesAsUnpaired(t, session) {
				t.Fatal("the session goes on resolving against an account WhatsApp revoked")
			}
		})
	}
}

// Nor did this: a logout that never left keeps everything, whichever of the three ways it
// never left.
func TestALogoutThatNeverLeftStillKeepsEverything(t *testing.T) {
	t.Parallel()

	for name, failure := range map[string]error{
		"not connected":    wm.ErrNotConnected,
		"not logged in":    fmt.Errorf("wrapped: %w", wm.ErrNotLoggedIn),
		"no client at all": wm.ErrClientIsNil,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			session, container := newTestSession(t, "5511999990001")
			session.logout = func(context.Context, *wm.Client) error { return failure }
			session.setConnected(false)
			session.setReconnecting(true, time.Now())

			err := session.Logout(t.Context())
			if !errors.Is(err, failure) {
				t.Fatalf("Logout failed with %v, want %v", err, failure)
			}
			saysNothing(t, session, "for a logout that never reached WhatsApp")
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
		})
	}
}

// An open session that loses the answer stays open: the socket is up, and nothing on it
// said the account is gone.
func TestAnOpenSessionThatLostALogoutAnswerStaysOpen(t *testing.T) {
	t.Parallel()

	for name, logout := range unansweredLogouts {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			session, container := newTestSession(t, "5511999990001")
			session.logout = func(ctx context.Context, _ *wm.Client) error { return logout(ctx) }
			session.setConnected(true)

			if err := logoutWithin(t, session); err == nil {
				t.Fatal("a logout nobody answered was reported as one that went through")
			}
			if reasons := loggedOutReasons(t, session); len(reasons) != 0 {
				t.Fatalf("published logged_out (reasons %v) for a logout WhatsApp never answered", reasons)
			}
			if state := session.state(); state != "open" {
				t.Fatalf("the session reports %q over a socket nothing took down", state)
			}
			if _, bound, err := container.For(session.sid).JID(t.Context()); err != nil || !bound {
				t.Fatalf("the pairing was forgotten anyway (bound=%v, err=%v)", bound, err)
			}
			if session.isStale() {
				t.Fatal("the session was marked stale, so the next connect would delete credentials that still resume")
			}
			if resolvesAsUnpaired(t, session) {
				t.Fatal("the session answers not_paired for an account nothing revoked")
			}
		})
	}
}

// Telling a request that lost its answer from a cleanup that stalled rests on whatsmeow's
// wording of the first, so an upgrade that rewords it has to fail here rather than turn
// every logout that ran out of time back into a revocation nobody confirmed.
func TestWhatsmeowStillWordsAFailedLogoutRequestTheSameWay(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")

	// The real client, not connected: the request step fails before anything is written,
	// through the same wrapping every other failure of that step goes through.
	err := session.current().Logout(t.Context())
	if !errors.Is(err, wm.ErrNotConnected) {
		t.Fatalf("Logout on a client with no socket failed with %v, want ErrNotConnected", err)
	}
	if !strings.HasPrefix(err.Error(), logoutRequestFailed) {
		t.Fatalf("whatsmeow words a failed logout request as %q, which no longer starts with %q", err, logoutRequestFailed)
	}
}
