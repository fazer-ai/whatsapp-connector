package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// WhatsApp does not hand a view-once photo to a companion device: what arrives is a
// stanza with no ciphertext in it at all. Published as nothing, an agent sees silence
// where a customer sent something, and nothing later corrects it -- whatsmeow asks the
// primary phone to forward the message, and that phone may never do it.
func TestAMessageThisDeviceWasNeverGivenStillReachesTheInbox(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.rerequestWait = 10 * time.Millisecond
	emission, acknowledged := unreadable(t, session, unavailableMessage("3EB0VIEWONCE", "view_once"))
	if !acknowledged {
		t.Fatal("a stanza carrying nothing was left for WhatsApp to send again, and it carries nothing again")
	}
	if emission.Type != protocol.EventMessageReceived {
		t.Fatalf("what came out is %s", emission.Type)
	}

	message := messageOf(t, emission)
	if message.ID != "3EB0VIEWONCE" {
		t.Errorf("the bubble names %q, and the stanza named 3EB0VIEWONCE", message.ID)
	}
	if message.Sender == nil || message.Sender.Phone != "5511999990002" {
		t.Errorf("the bubble has nobody to attribute it to: %+v", message.Sender)
	}
	if message.Timestamp == 0 {
		t.Error("the bubble has no time, so it lands wherever the client puts one with none")
	}
	var content protocol.UnsupportedContent
	if err := json.Unmarshal(mustMarshal(t, message.Content), &content); err != nil {
		t.Fatalf("unmarshal the content: %v", err)
	}
	if content.Reason != protocol.UnsupportedUnavailable {
		t.Errorf("the content says %q, and nothing arrived to be decrypted in the first place", content.Reason)
	}
	validateInboundAgainstContract(t, &message)
}

// A ciphertext that arrived and would not open is a different thing to a person reading
// the thread, and the name WhatsApp put on the stanza is what says which it was. Whether
// whatsmeow called it unavailable does not: a group message with no sender key is
// reported that way too, and it is an ordinary decryption failure.
func TestWhatWasWrongWithAMessageIsWhatWhatsAppNamed(t *testing.T) {
	t.Parallel()

	for _, unread := range []struct {
		name  string
		build func() *waEvents.UndecryptableMessage
		want  protocol.UnsupportedReason
	}{
		{
			name:  "withheld on purpose",
			build: func() *waEvents.UndecryptableMessage { return unavailableMessage("3EB0VO", "view_once") },
			want:  protocol.UnsupportedUnavailable,
		},
		{
			name: "a ciphertext that would not open",
			build: func() *waEvents.UndecryptableMessage {
				event := unavailableMessage("3EB0BADSESSION", "")
				event.IsUnavailable = false
				return event
			},
			want: protocol.UnsupportedUndecryptable,
		},
		{
			// The `type` on an `<unavailable/>` node is optional, and whatsmeow passes
			// the empty string straight through. Read off that attribute alone, a stanza
			// that carried no ciphertext at all would be reported as one that did.
			name: "withheld with no name put on it",
			build: func() *waEvents.UndecryptableMessage {
				return unavailableMessage("3EB0UNTYPED", "")
			},
			want: protocol.UnsupportedUnavailable,
		},
		{
			// whatsmeow reports a group message with no sender key as unavailable, and
			// sends a retry receipt for it like any other failure to decrypt. Read off
			// that flag, this one would say the server withheld a message it did not.
			name: "a group message with no sender key",
			build: func() *waEvents.UndecryptableMessage {
				event := unavailableMessage("3EB0NOSENDERKEY", "")
				event.Info.Chat = waTypes.NewJID("120363041234567890", waTypes.GroupServer)
				event.Info.IsGroup = true
				return event
			},
			want: protocol.UnsupportedUndecryptable,
		},
	} {
		t.Run(unread.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.groups = true
			session.rerequestWait = 10 * time.Millisecond

			emission, acknowledged := unreadable(t, session, unread.build())
			if !acknowledged {
				t.Fatal("a stanza carrying nothing was left for WhatsApp to send again")
			}
			var content protocol.UnsupportedContent
			if err := json.Unmarshal(mustMarshal(t, messageOf(t, emission).Content), &content); err != nil {
				t.Fatalf("unmarshal the content: %v", err)
			}
			if content.Reason != unread.want {
				t.Errorf("the bubble says %q, want %q", content.Reason, unread.want)
			}
		})
	}
}

// Nothing is published while the message could still arrive. Both recoveries deliver it
// under the id the placeholder would take, and a client deduplicates on that id: the
// placeholder would be the only thing that chat ever shows, over a message that arrived.
func TestNoPlaceholderGoesOutWhileTheMessageCouldStillArrive(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.rerequestWait = 5 * time.Second

	if !session.handle(unavailableMessage("3EB0WAITING", "view_once")) {
		t.Fatal("an unreadable message was left for WhatsApp to send again")
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("a placeholder went out over a message that may still arrive: %s", emission.Payload)
	case <-time.After(300 * time.Millisecond):
	}
}

// And the message arriving is what calls the placeholder off, which is the whole reason
// for waiting at all.
func TestAMessageThatArrivesAfterAllCallsOffItsPlaceholder(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	// Long enough that it never fires here. What is under test is the cancellation, and
	// a window short enough to race the recovery would be testing the scheduler.
	session.rerequestWait = time.Minute

	if !session.handle(unavailableMessage("3EB0RECOVERED", "view_once")) {
		t.Fatal("an unreadable message was left for WhatsApp to send again")
	}
	if !waitingOn(session, "3EB0RECOVERED") {
		t.Fatal("no placeholder was ever waiting, so nothing here could have called one off")
	}
	emission := publishedBy(t, session, textMessage("3EB0RECOVERED", "bom dia"))

	message := messageOf(t, emission)
	var content struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(mustMarshal(t, message.Content), &content); err != nil {
		t.Fatalf("unmarshal the content: %v", err)
	}
	if content.Type != "text" {
		t.Fatalf("what reached the chat is %s, and the message itself arrived", content.Type)
	}
	// And nothing left waiting to follow it: the placeholder carries the same id, and a
	// client that deduplicates on the id would keep it over this one.
	if waitingOn(session, "3EB0RECOVERED") {
		t.Fatal("the placeholder is still waiting behind the message it was standing in for")
	}
}

// waitingOn reports whether a placeholder for this message is still scheduled, which is
// what a recovery has to leave behind it and what a wall clock cannot be asked about.
func waitingOn(session *Session, id string) bool {
	session.awaitedMu.Lock()
	defer session.awaitedMu.Unlock()
	_, waiting := session.awaited[id]
	return waiting
}

// The same boundary the message path draws. A client that asked for direct chats only
// never got the group's messages and could not put this one anywhere either.
func TestAnUnreadableGroupMessageIsNotPublishedToADirectOnlyClient(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.rerequestWait = 10 * time.Millisecond
	event := unavailableMessage("3EB0GROUPGONE", "view_once")
	event.Info.Chat = waTypes.NewJID("120363041234567890", waTypes.GroupServer)
	event.Info.IsGroup = true

	publishedNothingUnreadable(t, session, event)
}

// And it is published when the client did ask for groups, keyed to the participant who
// sent it rather than to the group.
func TestAnUnreadableGroupMessageNamesWhoSentIt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.groups = true
	session.rerequestWait = 10 * time.Millisecond
	event := unavailableMessage("3EB0GROUPSEEN", "view_once")
	event.Info.Chat = waTypes.NewJID("120363041234567890", waTypes.GroupServer)
	event.Info.IsGroup = true

	emission, acknowledged := unreadable(t, session, event)
	if !acknowledged {
		t.Fatal("a group's unreadable message was left for WhatsApp to send again")
	}
	message := messageOf(t, emission)
	if message.Chat.Kind != protocol.AddressGroup {
		t.Errorf("the bubble is in a %s chat", message.Chat.Kind)
	}
	if message.Sender == nil || message.Sender.Phone != "5511999990002" {
		t.Errorf("a group's bubble does not say who sent it: %+v", message.Sender)
	}
}

// unavailableMessage is the event whatsmeow dispatches for a stanza that arrived with no
// ciphertext in it.
func unavailableMessage(id, kind string) *waEvents.UndecryptableMessage {
	return &waEvents.UndecryptableMessage{
		Info: waTypes.MessageInfo{
			ID:        id,
			Timestamp: time.UnixMilli(1755000000000),
			PushName:  "Alice",
			MessageSource: waTypes.MessageSource{
				Chat:   waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
				Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
			},
		},
		IsUnavailable:   true,
		UnavailableType: waEvents.UnavailableType(kind),
	}
}

// publishedNothingUnreadable runs the handler and fails if anything was published or if
// the handler did not answer WhatsApp.
//
// The bound is shortened first and the handler run off the test's own goroutine, because
// a build that publishes when it should not waits the whole publisher bound on a settle
// nobody is going to give: a minute of nothing, rather than a failure.
func publishedNothingUnreadable(t *testing.T, session *Session, event *waEvents.UndecryptableMessage) {
	t.Helper()

	session.deliverWait = 100 * time.Millisecond
	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.handle(event) }()
	select {
	case emission := <-session.Events():
		t.Fatalf("something was published for a message that should have gone nowhere: %s", emission.Payload)
	case got := <-acknowledged:
		if !got {
			t.Fatal("the message was left for WhatsApp to send again, and it carries nothing again")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never came back")
	}
}

func unreadable(t *testing.T, session *Session, event *waEvents.UndecryptableMessage) (engine.Emission, bool) {
	t.Helper()

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.handle(event) }()
	emission := next(t, session)
	emission.Settle(nil)
	select {
	case got := <-acknowledged:
		return emission, got
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never came back")
		return engine.Emission{}, false
	}
}

func messageOf(t *testing.T, emission engine.Emission) protocol.InboundMessage {
	t.Helper()

	var body struct {
		Message protocol.InboundMessage `json:"message"`
	}
	if err := json.Unmarshal(emission.Payload, &body); err != nil {
		t.Fatalf("unmarshal the published message: %v", err)
	}
	return body.Message
}

func mustMarshal(t *testing.T, value any) json.RawMessage {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// A reaction, an edit, a revoke and a poll vote are things done to a message, and WhatsApp
// says so on the stanza: the sender sets `decrypt-fail: hide` on all four. One that will
// not decrypt has nothing a bubble could hold -- the message it acts on is already in the
// chat, unchanged -- and an unsupported bubble of its own would be a second entry in the
// thread for something that was never an entry at all.
func TestAnActionTheSenderAskedToHideIsNotGivenABubble(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.rerequestWait = time.Minute

	event := unavailableMessage("3EB0HIDDEN", "")
	event.IsUnavailable = false
	event.DecryptFailMode = waEvents.DecryptFailHide

	if !session.handle(event) {
		t.Fatal("an unreadable action was left for WhatsApp to send again, and it fails to decrypt again")
	}
	// Neither scheduled nor published, which are the two shapes this could take. Both are
	// settled by the time the handler returns, so neither needs waiting out.
	if waitingOn(session, "3EB0HIDDEN") {
		t.Error("a placeholder is waiting for an action the sender asked to hide")
	}
	select {
	case published := <-session.Events():
		t.Fatalf("something was published for an action the sender asked to hide: %s", published.Payload)
	default:
	}
}

// The same message can come back as a change rather than a message: an edit that failed to
// decrypt and was resent leaves as `message.edited`, under the id its placeholder is
// waiting on. The recovery has to call the placeholder off from there too, or the chat gets
// the correction and then an unsupported bubble claiming the same message never arrived.
//
// Reachable because `decrypt-fail` is the sender's to set: the clients that mark an edit
// hidden are the ones this build knows about, and one that does not mark it lands here.
func TestACorrectionThatArrivesAfterAllCallsOffItsPlaceholder(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.rerequestWait = time.Minute
	session.unseal = func(context.Context, *waEvents.Message) (*waE2E.Message, error) {
		return &waE2E.Message{Conversation: proto.String("bom dia, corrigido")}, nil
	}

	unread := unavailableMessage(carrier, "")
	unread.IsUnavailable = false
	if !session.handle(unread) {
		t.Fatal("an unreadable message was left for WhatsApp to send again")
	}
	if !waitingOn(session, carrier) {
		t.Fatal("no placeholder was ever waiting, so nothing here could have called one off")
	}

	emission := publishedBy(t, session, sealedEditEvent(carrier, subject))
	if emission.Type != protocol.EventMessageEdited {
		t.Fatalf("the resent correction was published as %s, want %s", emission.Type, protocol.EventMessageEdited)
	}
	if waitingOn(session, carrier) {
		t.Fatal("the placeholder is still waiting behind the correction it was standing in for")
	}
}

// Every other event answers a publisher that would not take it by withholding the
// acknowledgement, and WhatsApp sending it again is the retry. This one cannot: whatsmeow
// acknowledged the stanza before it dispatched the failure. So the retry has to be here,
// or a publisher that was down for a second costs the message its only bubble.
func TestAPlaceholderThePublisherWouldNotTakeIsOfferedAgain(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.rerequestWait = 10 * time.Millisecond
	session.rerequestRetry = 10 * time.Millisecond

	if !session.handle(unavailableMessage("3EB0REFUSED", "view_once")) {
		t.Fatal("an unreadable message was left for WhatsApp to send again")
	}

	refused := next(t, session)
	refused.Settle(errors.New("the publisher is not answering"))

	again := next(t, session)
	again.Settle(nil)
	if got := messageOf(t, again).ID; got != "3EB0REFUSED" {
		t.Fatalf("what was offered the second time is %q, and the first was 3EB0REFUSED", got)
	}
}

// The phone's answer to a message this device was never given comes back through
// ParseWebMessage, and that door rewrites the id of an edit to the message the edit
// corrects. The placeholder is waiting under the id of the stanza that failed, which the
// parser leaves on the web message and nowhere else. Read off the event, the correction is
// published and the placeholder follows it out 45 seconds later, claiming a message that
// arrived never did.
func TestACorrectionForwardedByThePhoneCallsOffItsPlaceholder(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.rerequestWait = time.Minute

	unread := unavailableMessage(carrier, "view_once")
	if !session.handle(unread) {
		t.Fatal("an unreadable message was left for WhatsApp to send again")
	}
	if !waitingOn(session, carrier) {
		t.Fatal("no placeholder was ever waiting, so nothing here could have called one off")
	}

	// Built by the library rather than by hand: the rewrite under test is whatsmeow's,
	// and an imitation of it would go on passing after the library changed it.
	chat := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	forwarded, err := session.current().ParseWebMessage(chat, &waWeb.WebMessageInfo{
		Key: &waCommon.MessageKey{
			RemoteJID: proto.String(chat.String()),
			FromMe:    proto.Bool(false),
			ID:        proto.String(carrier),
		},
		MessageTimestamp: proto.Uint64(1755000009),
		PushName:         proto.String("Alice"),
		Message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
			Key:           messageKey(subject),
			Type:          waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
			EditedMessage: &waE2E.Message{Conversation: proto.String("bom dia, corrigido")},
			TimestampMS:   proto.Int64(1755000009750),
		}},
	})
	if err != nil {
		t.Fatalf("ParseWebMessage: %v", err)
	}
	if forwarded.Info.ID != subject {
		t.Fatalf("the parser left %q on the event, and this test is about it leaving the target", forwarded.Info.ID)
	}

	if publishedBy(t, session, forwarded).Type != protocol.EventMessageEdited {
		t.Fatal("the forwarded correction was not published as a correction")
	}
	if waitingOn(session, carrier) {
		t.Fatal("the placeholder is still waiting behind the correction the phone forwarded")
	}
}

// The placeholder is queued in one place and published in another, and the message can
// arrive between the two. Whoever decides has to be the second of those: taken off the
// waiting list where it was queued, a message landing a moment later finds nothing to call
// off, and both go out -- the placeholder ahead of the message it stands for, which the
// client keeps over the real one for good.
func TestAMessageThatArrivesWhileItsPlaceholderIsQueuedStillWins(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.rerequestWait = 10 * time.Millisecond

	// The forwarder is parked on something nobody is reading, so what follows sits in the
	// inbox instead of being decided the moment it is queued.
	session.inbox <- pending{event: engine.Emission{Type: protocol.EventSessionState, Payload: []byte(`{}`)}}
	waitUntil(t, "the forwarder to be holding an emission", func() bool { return len(session.inbox) == 0 })

	if !session.handle(unavailableMessage("3EB0QUEUED", "view_once")) {
		t.Fatal("an unreadable message was left for WhatsApp to send again")
	}
	waitUntil(t, "the placeholder to be queued", func() bool { return len(session.inbox) == 1 })

	// And now it arrives, with its placeholder already in the queue ahead of it.
	recovered := make(chan bool, 1)
	go func() { recovered <- session.receive(textMessage("3EB0QUEUED", "bom dia")) }()
	// Noticed before the forwarder is let go, so what this reads is which of the two the
	// forwarder chooses and not which goroutine got there first.
	waitUntil(t, "the recovered message to be taken off the waiting list", func() bool {
		return !waitingOn(session, "3EB0QUEUED")
	})

	if parked := next(t, session); parked.Type != protocol.EventSessionState {
		t.Fatalf("what the forwarder was parked on is %s", parked.Type)
	}
	emission := next(t, session)
	emission.Settle(nil)
	select {
	case <-recovered:
	case <-time.After(2 * time.Second):
		t.Fatal("the handler for the recovered message never came back")
	}

	message := messageOf(t, emission)
	var content struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(mustMarshal(t, message.Content), &content); err != nil {
		t.Fatalf("unmarshal the content: %v", err)
	}
	if content.Type != "text" {
		t.Fatalf("what reached the chat is %s, and the message itself had arrived", content.Type)
	}
}
