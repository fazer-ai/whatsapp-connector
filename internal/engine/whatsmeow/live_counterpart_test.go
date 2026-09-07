//go:build live

// The second side of the conversation.
//
// Every phase before this had a person there: sending the message to react to, reading
// the typing indicator, deleting the message whose tombstone we wanted to see. That was
// affordable while a phase asked for one or two actions. Groups are not, and neither is
// anything that has to be measured rather than watched -- a window, an ordering, a key
// namespace -- because measuring means running it many times.
//
// Run the pairing once, then the phases:
//
//	WAC_LIVE_COUNTERPART_PHONE=<number> go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLivePairCounterpart
//	go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveTwoAccountsTalk
//
// The counterpart is paired as a linked device on the same store as the account under
// test, under its own sid. It spends a linked-device slot on that phone and has to stay
// online: WhatsApp logs out a device that has not connected in about two weeks.
package whatsmeow

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// TestLivePairCounterpart pairs the second account by code, which is the practical one
// here: the number is known and typing eight characters beats holding two phones.
func TestLivePairCounterpart(t *testing.T) {
	phone := os.Getenv("WAC_LIVE_COUNTERPART_PHONE")
	if phone == "" {
		t.Skip("set WAC_LIVE_COUNTERPART_PHONE to the second number, country code and no plus")
	}

	waEngine, container := liveEngine(t, MediaOptions{})
	counterpart := liveSessionOn(t, waEngine, container, liveCounterpartSID)
	events := watch(t, counterpart)

	if err := counterpart.Connect(t.Context(), engine.ConnectRequest{Pairing: "code", Phone: phone}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	events.await(t, protocol.EventPairingCode, 2*time.Minute)
	events.await(t, protocol.EventPairingSuccess, liveDeadline(t, 5*time.Minute))
	events.awaitState(t, "open", 2*time.Minute)

	jid, bound, err := container.For(liveCounterpartSID).JID(t.Context())
	if err != nil || !bound {
		t.Fatalf("the counterpart paired but nothing was written down (bound=%v, err=%v)", bound, err)
	}
	// The two must not be the same account. Pairing the number already under test would
	// leave every phase after this checking a conversation with itself, which passes and
	// proves nothing.
	subject, subjectBound, err := container.For(liveSID).JID(t.Context())
	if err == nil && subjectBound && subject.User == jid.User {
		t.Fatalf("the counterpart paired as %s, which is the account under test", jid)
	}
	t.Logf("counterpart paired as %s", jid)
}

// TestLiveTwoAccountsTalk is the phase that says the harness works: the counterpart sends
// and the account under test receives it, both driven from here with no phone in hand.
//
// It is deliberately the smallest thing that proves it. What it establishes is what the
// measured phases need and cannot check for themselves: that both sessions resume, that
// they are different accounts, and that a message from one arrives at the other.
func TestLiveTwoAccountsTalk(t *testing.T) {
	subject, counterpart, container := liveBoth(t, MediaOptions{})

	subjectJID := liveMustBePaired(t, container, liveSID)
	counterpartJID := liveMustBePaired(t, container, liveCounterpartSID)

	liveResume(t, subject)
	liveResume(t, counterpart)
	// Watched after the resumes, not before. A watcher's buffer holds 256 emissions and
	// drops what does not fit, and a resume delivers whatever came in while the session
	// was down: on an account with a real backlog the probe below would be dropped on
	// arrival, and the phase would wait out its deadline reporting a message that had in
	// fact been delivered. Nothing here needs the resume's own events.
	inbox := watch(t, subject)

	said := "conector nativo, contraparte falando " + time.Now().Format(time.TimeOnly)
	sent := liveSay(t, counterpart, subjectJID.User, said)

	arrived := inbox.awaitMessage(t, sent, 2*time.Minute)
	var message struct {
		Sender *struct {
			Phone string `json:"phone"`
			LID   string `json:"lid"`
		} `json:"sender"`
		Content struct {
			Body string `json:"body"`
		} `json:"content"`
	}
	if err := json.Unmarshal(arrived, &message); err != nil {
		t.Fatalf("unmarshal what arrived: %v", err)
	}
	if message.Content.Body != said {
		t.Fatalf("the body is %q, want %q", message.Content.Body, said)
	}
	if message.Sender == nil {
		t.Fatal("the message arrived attributed to nobody")
	}
	// Both halves, which is what the addressing work put there: the counterpart is known
	// by number and by LID, and a client keying on either finds the same person.
	if message.Sender.Phone == "" || message.Sender.LID == "" {
		t.Errorf("the sender is %+v, want both namespaces filled in", message.Sender)
	}
	if message.Sender.Phone != counterpartJID.User {
		t.Errorf("the sender is %s and the counterpart is %s", message.Sender.Phone, counterpartJID.User)
	}
}

// liveMustBePaired reads back which account a sid is, and stops the phase when it is
// none: a resume against nothing paired fails much later and less clearly.
func liveMustBePaired(t *testing.T, container *store.Container, sid string) waTypes.JID {
	t.Helper()

	jid, bound, err := container.For(sid).JID(t.Context())
	if err != nil || !bound {
		t.Fatalf("%s is not paired yet (bound=%v, err=%v); pair it before this phase", sid, bound, err)
	}
	return jid
}

// liveSay sends a text from one account to a number and returns the id it went out under.
func liveSay(t *testing.T, from *Session, to, body string) string {
	t.Helper()

	messageID := from.current().GenerateMessageID()
	liveSayUnder(t, from, to, body, messageID)
	return messageID
}

// liveSayUnder is liveSay with the id decided by the caller, for a phase that has to
// arrange something around this message before it goes out.
func liveSayUnder(t *testing.T, from *Session, to, body, messageID string) {
	t.Helper()

	payload, err := json.Marshal(map[string]any{
		"message_id": messageID,
		"to":         map[string]any{"kind": "phone", "id": to},
		"content":    map[string]any{"type": "text", "body": body},
	})
	if err != nil {
		t.Fatalf("build the send: %v", err)
	}
	if _, err := from.Execute(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend, Payload: payload,
	}); err != nil {
		t.Fatalf("message.send: %v", err)
	}
}

// liveResume brings a paired session back up, the way a connector restart does.
func liveResume(t *testing.T, session *Session) {
	t.Helper()

	liveResumeAsking(t, session, engine.ConnectRequest{Pairing: "resume"})
}

// liveResumeAsking is liveResume with the subscriptions the phase needs.
//
// Groups are the one that matters, and getting it wrong looks like the feature being
// broken rather than unasked for: a session that did not subscribe has its group messages
// acknowledged and published nowhere, so a group phase resuming the ordinary way sits and
// watches a silence it arranged itself.
func liveResumeAsking(t *testing.T, session *Session, req engine.ConnectRequest) {
	t.Helper()

	events := watch(t, session)
	if err := session.Connect(t.Context(), req); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	events.awaitState(t, "open", 2*time.Minute)
}
