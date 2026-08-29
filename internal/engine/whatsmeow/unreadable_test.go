package whatsmeow

import (
	"encoding/json"
	"testing"
	"time"

	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

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

// The other half of the same event, and the opposite answer. A ciphertext that arrived
// and would not open has a recovery that works: whatsmeow asks the sender to send it
// again, and it arrives under the same id. The client deduplicates on that id, so a
// placeholder now is a placeholder for good over a message that did arrive.
func TestAMessageThatWouldNotDecryptIsLeftToItsRetry(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := unavailableMessage("3EB0BADSESSION", "")
	event.IsUnavailable = false

	publishedNothingUnreadable(t, session, event)
}

// The same boundary the message path draws. A client that asked for direct chats only
// never got the group's messages and could not put this one anywhere either.
func TestAnUnreadableGroupMessageIsNotPublishedToADirectOnlyClient(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
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
