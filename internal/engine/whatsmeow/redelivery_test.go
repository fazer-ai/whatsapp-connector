package whatsmeow

import (
	"errors"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// redeliveringSession is a session on a socket, asking about the stream every few
// milliseconds instead of every few seconds.
func redeliveringSession(t *testing.T) (*Session, *syncBuffer) {
	t.Helper()

	session, written := newLoggedTestSession(t, "5511999990001")
	session.redeliveryProbe = 5 * time.Millisecond
	dialedAndConnected(session)
	return session, written
}

// withheld hands the session one inbound message and fails its publish, which is the
// acknowledgement WhatsApp only sends again on a new connection (#354).
func withheld(t *testing.T, session *Session, id string) {
	t.Helper()

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(textMessage(id, "bom dia")) }()
	emission := next(t, session)
	if emission.Type != protocol.EventMessageReceived {
		t.Fatalf("the message went out as %s", emission.Type)
	}
	emission.Settle(errors.New("redis is gone"))
	if <-acknowledged {
		t.Fatal("a message that was not published was acknowledged")
	}
}

// state is what a session.state emission says.
func state(t *testing.T, emission *engine.Emission) (took, reason string) {
	t.Helper()

	if emission.Type != protocol.EventSessionState {
		t.Fatalf("expected a session.state, got %s", emission.Type)
	}
	published := decode(t, emission.Payload)
	took, _ = published["state"].(string)
	reason, _ = published["reason"].(string)
	return took, reason
}

// quiet fails when the session publishes anything within a few probe intervals.
func quiet(t *testing.T, session *Session, why string) {
	t.Helper()

	select {
	case emission := <-session.Events():
		t.Fatalf("%s, and the session published %s: %s", why, emission.Type, emission.Payload)
	case <-time.After(100 * time.Millisecond):
	}
}

// The whole of the issue: an acknowledgement withheld because the stream failed is one
// WhatsApp sends again only on a new connection, and with the lease still good nothing
// else brings one. Once the stream takes a write again the session takes its own socket
// down, so the redelivery comes now and not whenever the connection next drops.
func TestAnAcknowledgementWithheldForAFailedPublishTakesTheSocketDownOnceTheStreamIsBack(t *testing.T) {
	t.Parallel()

	session, written := redeliveringSession(t)
	withheld(t, session, "3EB0REDELIVER")

	// The stream is still down: the session asks, hears no, and stays on its socket. A
	// reconnect now would only withhold the redelivered message again.
	probe := next(t, session)
	if got, _ := state(t, probe); got != "open" {
		t.Fatalf("the session asked about the stream with state %q, which is not what it is", got)
	}
	probe.Settle(errors.New("redis is still gone"))
	if got := session.state(); got != "open" {
		t.Fatalf("the session went %q while the stream was still down", got)
	}

	// And now it is back.
	next(t, session).Settle(nil)
	took, reason := state(t, next(t, session))
	if took != "reconnecting" || reason != reasonRedelivery {
		t.Fatalf("the session published %s (%s) once the stream was back, want reconnecting (%s)", took, reason, reasonRedelivery)
	}
	if got := session.state(); got != "reconnecting" {
		t.Fatalf("the session reports %q while taking its own socket down", got)
	}
	if !strings.Contains(written.String(), "redelivers") {
		t.Fatalf("the takedown is not in the log: %s", written.String())
	}
	// The reset itself, as far as a test client lets it be seen: with no socket under the
	// client it stands down saying so, and a session that only published the state would
	// never get this far.
	waitUntil(t, "the takedown to reach the socket", func() bool {
		return strings.Contains(written.String(), "already gone before it could be taken down")
	})
}

// Several messages withheld through one outage are all redelivered by one new
// connection, so one takedown answers all of them.
func TestSeveralWithheldAcknowledgementsCostOneTakedown(t *testing.T) {
	t.Parallel()

	session, _ := redeliveringSession(t)
	withheld(t, session, "3EB0FIRST")

	// The first failure already started the wait, so its question can go out before the
	// second message does; whichever order they come in, it is the only one.
	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(textMessage("3EB0SECOND", "bom dia")) }()
	var probe *engine.Emission
	for probe == nil || len(acknowledged) == 0 {
		emission := next(t, session)
		switch emission.Type {
		case protocol.EventSessionState:
			if probe != nil {
				t.Fatal("a second question about the stream went out while the first was unanswered")
			}
			probe = emission
		case protocol.EventMessageReceived:
			emission.Settle(errors.New("redis is gone"))
			if <-acknowledged {
				t.Fatal("a message that was not published was acknowledged")
			}
			acknowledged <- false
		default:
			t.Fatalf("unexpected %s", emission.Type)
		}
	}

	// One wait, so one question in flight: a second waiter would ask again while the first
	// is still waiting for its answer.
	quiet(t, session, "a second question about the stream went out while the first was unanswered")
	probe.Settle(nil)
	if took, _ := state(t, next(t, session)); took != "reconnecting" {
		t.Fatalf("the session published %q once the stream was back", took)
	}
	quiet(t, session, "a second takedown followed the first")
}

// The wait ends with the takedown, and the next failure on the new connection starts a
// new one: a session that remembered the first wait as still running would leave every
// later outage to the next drop, which is the issue all over again.
func TestAWithholdingAfterATakedownWaitsForTheStreamAgain(t *testing.T) {
	t.Parallel()

	session, _ := redeliveringSession(t)
	withheld(t, session, "3EB0BEFORE")
	next(t, session).Settle(nil)
	if took, _ := state(t, next(t, session)); took != "reconnecting" {
		t.Fatalf("the session published %q once the stream was back", took)
	}

	session.setConnected(true)
	withheld(t, session, "3EB0AFTER")
	next(t, session).Settle(nil)
	if took, reason := state(t, next(t, session)); took != "reconnecting" || reason != reasonRedelivery {
		t.Fatalf("a second outage on the new connection published %s (%s)", took, reason)
	}
}

// A connection that came and went since the acknowledgement was withheld has already had
// the redelivery: WhatsApp sends what is unacknowledged to every new connection. Taking
// that one down too is a reconnect for nothing.
func TestAWithheldAcknowledgementANewConnectionAlreadyAnsweredIsLeftAlone(t *testing.T) {
	t.Parallel()

	session, _ := redeliveringSession(t)
	session.redeliveryProbe = 50 * time.Millisecond
	withheld(t, session, "3EB0OVERTAKEN")

	session.setConnected(false)
	session.setConnected(true)

	quiet(t, session, "the session acted on a withheld acknowledgement a new connection already redelivered")
	if got := session.state(); got != "open" {
		t.Fatalf("the session went %q over a redelivery that already happened", got)
	}
}

// Withheld for a reason that is not the stream -- something this build cannot publish at
// all -- is a message a reconnect would only bring back to be withheld again.
func TestAnAcknowledgementWithheldForAnythingButThePublishTakesNothingDown(t *testing.T) {
	t.Parallel()

	session, _ := redeliveringSession(t)
	unpublishable := textMessage("", "sem id")
	session.deliverWait = 50 * time.Millisecond
	if session.receive(unpublishable) {
		t.Fatal("the test needs a message this build cannot publish, and this one was acknowledged")
	}
	quiet(t, session, "a message withheld for a reason the stream has nothing to do with started a takedown")
}

// A publish that ran past deliverWait has no outcome yet when the acknowledgement is
// withheld. If it lands after all, the message is on the stream and a redelivery would
// only publish it a second time; if it fails, it is the case above.
func TestAPublishThatOutlivedTheWaitDecidesAfterwards(t *testing.T) {
	t.Parallel()

	for _, outcome := range []struct {
		name     string
		err      error
		takeDown bool
	}{
		{name: "landed", err: nil, takeDown: false},
		{name: "failed", err: errors.New("redis is gone"), takeDown: true},
	} {
		t.Run(outcome.name, func(t *testing.T) {
			t.Parallel()

			session, _ := redeliveringSession(t)
			session.deliverWait = 20 * time.Millisecond
			acknowledged := make(chan bool, 1)
			go func() { acknowledged <- session.receive(textMessage("3EB0SLOW", "bom dia")) }()
			late := next(t, session)
			if <-acknowledged {
				t.Fatal("a message whose publish had not finished was acknowledged")
			}
			late.Settle(outcome.err)

			if !outcome.takeDown {
				quiet(t, session, "a message that did reach the stream started a takedown")
				return
			}
			next(t, session).Settle(nil)
			if took, _ := state(t, next(t, session)); took != "reconnecting" {
				t.Fatalf("the session published %q once the stream was back", took)
			}
		})
	}
}

// An inbox that stayed full for the whole wait never got the message into the queue, so
// nothing will ever publish it and only a redelivery brings it back.
func TestAnAcknowledgementWithheldForAFullInboxIsRedelivered(t *testing.T) {
	t.Parallel()

	session, written := redeliveringSession(t)
	session.deliverWait = 20 * time.Millisecond
	// The forwarder takes one emission out and then blocks handing it on, since nobody is
	// reading: waited for, so the inbox filled after it stays full.
	filler := pending{event: engine.Emission{Type: protocol.EventMessageReceived}}
	session.inbox <- filler
	waitUntil(t, "the forwarder to take the first emission", func() bool { return len(session.inbox) == 0 })
	for len(session.inbox) < cap(session.inbox) {
		session.inbox <- filler
	}
	if session.receive(textMessage("3EB0NOROOM", "bom dia")) {
		t.Fatal("a message that never got into the queue was acknowledged")
	}
	if !strings.Contains(written.String(), "could not be queued") {
		t.Fatalf("the message was not withheld for want of room, so this test is not about that: %s", written.String())
	}

	// Drain what filled it, and the probe follows.
	for range cap(session.inbox) + 1 {
		if emission := next(t, session); emission.Type != protocol.EventMessageReceived {
			t.Fatalf("expected the fillers first, got %s", emission.Type)
		}
	}
	next(t, session).Settle(nil)
	if took, _ := state(t, next(t, session)); took != "reconnecting" {
		t.Fatalf("the session published %q once the stream was back", took)
	}
}

// A session already closing is not asked about: restating `close` would be a probe that
// tells the client the account is finished in the middle of a takedown that is not.
func TestAClosingSessionIsNotProbed(t *testing.T) {
	t.Parallel()

	session, _ := redeliveringSession(t)
	withheld(t, session, "3EB0CLOSED")
	session.mu.Lock()
	session.closed = true
	session.mu.Unlock()
	// Put back before the cleanup that closes the session, which runs after this one: its
	// Close returns early on a session marked closed and would leave the forwarder running.
	t.Cleanup(func() {
		session.mu.Lock()
		session.closed = false
		session.mu.Unlock()
	})
	quiet(t, session, "a session already closing restated its state to ask about the stream")
}

// A failure that settles after the socket was replaced is about the connection the message
// arrived on, and the replacement already was its redelivery: taking the replacement down
// too is a reconnect for nothing.
func TestALateFailureAfterTheSocketWasReplacedTakesNothingDown(t *testing.T) {
	t.Parallel()

	session, _ := redeliveringSession(t)
	session.deliverWait = 20 * time.Millisecond
	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(textMessage("3EB0LATE", "bom dia")) }()
	late := next(t, session)
	if <-acknowledged {
		t.Fatal("a message whose publish had not finished was acknowledged")
	}

	session.setConnected(false)
	session.setConnected(true)
	late.Settle(errors.New("redis is gone"))

	quiet(t, session, "a failure about a connection already replaced started a takedown of its replacement")
	if got := session.state(); got != "open" {
		t.Fatalf("the replacement went %q over a redelivery it already was", got)
	}
}

// A placeholder has nothing at WhatsApp to send again: whatsmeow acknowledged the stanza
// before it said the message could not be read. A failed publish of one is retried by the
// session itself, and a reconnect would bring back nothing.
func TestAFailedPlaceholderTakesNothingDown(t *testing.T) {
	t.Parallel()

	session, _ := redeliveringSession(t)
	published := make(chan bool, 1)
	go func() {
		published <- session.deliverUnless(protocol.EventMessageReceived, map[string]any{"message": "x"}, session.learned(), "3EB0HOLE")
	}()
	next(t, session).Settle(errors.New("redis is gone"))
	if <-published {
		t.Fatal("a placeholder that was not published was reported as published")
	}
	quiet(t, session, "a placeholder that failed to publish started a takedown")
}

// A history slice is the same: the notification that carried it is acknowledged once the
// dump is written down, and the dump is tried again from there.
func TestAFailedHistorySliceTakesNothingDown(t *testing.T) {
	t.Parallel()

	session, _ := redeliveringSession(t)
	session.setHistory(true)
	bench := &historyBench{dump: dumpOf(waHistorySync.HistorySync_INITIAL_BOOTSTRAP,
		&waHistorySync.Conversation{ID: proto.String("5511999990002@s.whatsapp.net"), Messages: []*waHistorySync.HistorySyncMsg{
			pastText("5511999990002@s.whatsapp.net", "3EB0D1", 1754000000, "oi"),
		}})}
	bench.install(session)

	acknowledged := make(chan bool, 1)
	go func() {
		acknowledged <- session.receive(historyNotification("NOTIF9", waE2E.HistorySyncType_INITIAL_BOOTSTRAP))
	}()
	slice := next(t, session)
	if slice.Type != protocol.EventHistorySync {
		t.Fatalf("expected the slice, got %s", slice.Type)
	}
	slice.Settle(errors.New("redis is gone"))
	if !<-acknowledged {
		t.Fatal("the dump was withheld, which is not the path this test is about")
	}
	deadline := time.After(100 * time.Millisecond)
	for {
		select {
		case emission := <-session.Events():
			if emission.Type == protocol.EventSessionState {
				t.Fatalf("a history slice that failed to publish started a takedown: %s", emission.Payload)
			}
			if emission.Settle != nil {
				emission.Settle(errors.New("redis is gone"))
			}
		case <-deadline:
			return
		}
	}
}

// A ping coming back says the socket answers, and that is not what this takedown is about:
// WhatsApp still holds what was left unacknowledged. One left waiting for a command goes
// ahead once the command is answered, whatever the keepalive did in between.
func TestARecoveredKeepAliveDoesNotCallOffARedeliveryTakedown(t *testing.T) {
	t.Parallel()

	session, written := redeliveringSession(t)
	withheld(t, session, "3EB0WAITING")
	session.countCommand()
	next(t, session).Settle(nil)
	if took, _ := state(t, next(t, session)); took != "reconnecting" {
		t.Fatalf("the session published %q once the stream was back", took)
	}

	session.handle(&waEvents.KeepAliveRestored{})
	if got := session.state(); got != "reconnecting" {
		t.Fatalf("a ping coming back put the session %q and called off the redelivery", got)
	}

	session.endCommand()
	waitUntil(t, "the takedown to reach the socket once the command was answered", func() bool {
		return strings.Contains(written.String(), "already gone before it could be taken down")
	})
}

// The session going away ends the wait for the stream: nothing is left to take down.
func TestTheWaitForTheStreamEndsWithTheSession(t *testing.T) {
	t.Parallel()

	session, _ := redeliveringSession(t)
	withheld(t, session, "3EB0CLOSING")
	next(t, session)
	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// What would fail here is the race detector or a goroutine leak check, and a takedown
	// running against a closed session; the state is the observable half.
	if got := session.state(); got == "reconnecting" {
		t.Fatal("a session that closed went on to take its socket down")
	}
}
