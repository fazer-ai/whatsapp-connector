package whatsmeow

import (
	"testing"
	"time"

	waCommon "go.mau.fi/whatsmeow/proto/waCommon"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

const (
	// The stanza that carries a change, and the message it is about. They are never the
	// same id, and telling them apart is what most of this file is checking.
	carrier = "3EB0CARRIER"
	subject = "3EB0SUBJECT"
)

func TestAReactionIsPublishedAsItsOwnEvent(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	emission := publishedBy(t, session, reactionEvent(carrier, subject, "👍", 1755000009000))

	if emission.Type != protocol.EventMessageReaction {
		t.Fatalf("a reaction was published as %s, want %s", emission.Type, protocol.EventMessageReaction)
	}
	validateAgainstContract(t, "event_message_reaction", emission.Payload)

	payload := decode(t, emission.Payload)
	if payload["id"] != carrier {
		t.Errorf("the reaction is filed under %v, and the client deduplicates on its own id %q", payload["id"], carrier)
	}
	if payload["target_id"] != subject {
		t.Errorf("the reaction annotates %v, want the message it was put on, %q", payload["target_id"], subject)
	}
	if payload["emoji"] != "👍" {
		t.Errorf("the reaction reads %v", payload["emoji"])
	}
	if payload["from_me"] != false {
		t.Errorf("a reaction somebody else made was published as the account's own")
	}
	// The sender's own clock, not arrival: two reactions from one person are one row on
	// the client and the later one wins, and arrival is the thing that is out of order.
	if payload["timestamp"] != float64(1755000009000) {
		t.Errorf("the reaction is stamped %v, want the sender's own clock", payload["timestamp"])
	}
}

// An empty emoji is how the contract says a reaction was taken back. Left out of the
// frame it reads as a reaction with nothing in it, and the client would put an empty
// bubble on the message instead of clearing the one that is there.
func TestAReactionTakenBackKeepsTheEmojiFieldEmptyRatherThanDroppingIt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	emission := publishedBy(t, session, reactionEvent(carrier, subject, "", 1755000009000))

	validateAgainstContract(t, "event_message_reaction", emission.Payload)
	payload := decode(t, emission.Payload)
	emoji, present := payload["emoji"]
	if !present {
		t.Fatal("a reaction that was taken back was published without an emoji field at all")
	}
	if emoji != "" {
		t.Fatalf("a removal carries %q", emoji)
	}
}

// REVOKE is the zero value of the protobuf enum, so GetType() on the nil protocol
// message an ordinary text carries answers REVOKE. Read without the guard, every message
// in the account classifies as its own deletion and nothing is ever delivered.
func TestAnOrdinaryMessageIsNotReadAsADeletionOfItself(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	emission := publishedBy(t, session, textMessage("3EB0PLAIN", "bom dia"))

	if emission.Type != protocol.EventMessageReceived {
		t.Fatalf("an ordinary message was published as %s", emission.Type)
	}
}

func TestAnEditNamesTheMessageItCorrectsRatherThanTheStanzaCarryingIt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	corrected := &waE2E.Message{Conversation: proto.String("bom dia, corrigido")}
	emission := publishedBy(t, session, editEvent(carrier, subject, corrected, 1755000009000))

	if emission.Type != protocol.EventMessageEdited {
		t.Fatalf("an edit was published as %s, want %s", emission.Type, protocol.EventMessageEdited)
	}
	validateAgainstContract(t, "event_message_edited", emission.Payload)

	payload := decode(t, emission.Payload)
	if payload["message_id"] != subject {
		t.Fatalf("the edit corrects %v, and the client has to find %q to rewrite it", payload["message_id"], subject)
	}
	content, _ := payload["content"].(map[string]any)
	if content["type"] != "text" || content["body"] != "bom dia, corrigido" {
		t.Fatalf("the edit reads %v", content)
	}
	if payload["timestamp"] != float64(1755000009000) {
		t.Errorf("the edit is stamped %v, want the editor's own clock", payload["timestamp"])
	}
}

// WhatsApp lets a caption be rewritten and never the file underneath it. Fetching it
// again would mint a second blob for bytes the client already holds a reference to, and
// charge the account's bandwidth for a caption.
func TestACaptionEditCarriesTheNewCaptionAndFetchesNoFile(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	corrected := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Mimetype:      proto.String("image/jpeg"),
		Caption:       proto.String("legenda corrigida"),
		FileLength:    proto.Uint64(2048),
		DirectPath:    proto.String("/v/t62.7118-24/whatever"),
		MediaKey:      make([]byte, 32),
		FileEncSHA256: make([]byte, 32),
		FileSHA256:    make([]byte, 32),
	}}
	emission := publishedBy(t, session, editEvent(carrier, subject, corrected, 1755000009000))

	validateAgainstContract(t, "event_message_edited", emission.Payload)
	content, _ := decode(t, emission.Payload)["content"].(map[string]any)
	if content["type"] != "media" || content["kind"] != "image" {
		t.Fatalf("a caption edit was published as %v", content)
	}
	if content["caption"] != "legenda corrigida" {
		t.Fatalf("the caption reads %v", content["caption"])
	}
	if ref, present := content["ref"]; present {
		t.Fatalf("a caption edit handed out a second reference to the same file: %v", ref)
	}
}

// A channel's edit arrives unwrapped, under the original post's id, with the edit's own
// clock off to the side. Read the ordinary way there is no key to name a target in.
func TestAChannelEditNamesThePostItself(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage(subject, "o post, corrigido")
	channel := waTypes.NewJID("120363111111111111", waTypes.NewsletterServer)
	event.Info.Chat = channel
	event.Info.Sender = channel
	event.NewsletterMeta = &waEvents.NewsletterMessageMeta{
		EditTS:     time.UnixMilli(1755000009000),
		OriginalTS: time.UnixMilli(1755000000000),
	}

	emission := publishedBy(t, session, event)
	if emission.Type != protocol.EventMessageEdited {
		t.Fatalf("a channel edit was published as %s", emission.Type)
	}
	validateAgainstContract(t, "event_message_edited", emission.Payload)

	payload := decode(t, emission.Payload)
	if payload["message_id"] != subject {
		t.Fatalf("a channel edit corrects %v, want the post's own id %q", payload["message_id"], subject)
	}
	if payload["timestamp"] != float64(1755000009000) {
		t.Errorf("a channel edit is stamped %v, want the edit's own clock", payload["timestamp"])
	}
}

// The two revokers are not the same event to a client: its own deletion takes the file
// with it, and somebody else's only flags the bubble so an agent can still read what was
// said. Getting this backwards either destroys an attachment nobody deleted or leaves a
// message on screen that WhatsApp has taken off every other phone.
func TestADeletionSaysWhoPerformedIt(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		prepare func(*waEvents.Message)
		want    string
	}{
		{"the contact deleted their own message", func(*waEvents.Message) {}, "contact"},
		{"the account deleted one from another device", func(event *waEvents.Message) {
			event.Info.IsFromMe = true
		}, "self"},
		{"an admin deleted somebody else's in a group", func(event *waEvents.Message) {
			event.Info.Edit = waTypes.EditAttributeAdminRevoke
			event.Info.Chat = waTypes.NewJID("120363000000000000", waTypes.GroupServer)
			event.Info.IsGroup = true
		}, "contact"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.setGroups(true)
			event := revokeEvent(carrier, subject)
			tc.prepare(event)

			emission := publishedBy(t, session, event)
			if emission.Type != protocol.EventMessageRevoked {
				t.Fatalf("a deletion was published as %s, want %s", emission.Type, protocol.EventMessageRevoked)
			}
			validateAgainstContract(t, "event_message_revoked", emission.Payload)

			payload := decode(t, emission.Payload)
			if payload["by"] != tc.want {
				t.Fatalf("the deletion says %v performed it, want %q", payload["by"], tc.want)
			}
			if payload["message_id"] != subject {
				t.Fatalf("the deletion names %v, want the message it deletes, %q", payload["message_id"], subject)
			}
		})
	}
}

// A channel deletes a post by sending the deletion under the post's own id, with no body
// at all to name a key in.
func TestAChannelDeletionNamesThePostItself(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	channel := waTypes.NewJID("120363111111111111", waTypes.NewsletterServer)
	event := textMessage(subject, "")
	event.Info.Chat = channel
	event.Info.Sender = channel
	event.Info.Edit = waTypes.EditAttributeAdminRevoke
	event.Message = &waE2E.Message{}

	emission := publishedBy(t, session, event)
	if emission.Type != protocol.EventMessageRevoked {
		t.Fatalf("a channel deletion was published as %s", emission.Type)
	}
	payload := decode(t, emission.Payload)
	if payload["message_id"] != subject {
		t.Fatalf("a channel deletion names %v, want the post's own id %q", payload["message_id"], subject)
	}
}

// Acknowledged and published nowhere is the answer for what no build will ever carry.
// Withholding instead leaves WhatsApp redelivering it for as long as the session is up.
func TestWhatNothingWillEverPublishIsAcknowledgedRatherThanKeptOnThePhone(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		build func() *waEvents.Message
	}{
		{"a pin, which the contract has no event for", func() *waEvents.Message {
			event := textMessage(carrier, "")
			event.Info.Edit = waTypes.EditAttributePinInChat
			event.Message = &waE2E.Message{}
			return event
		}},
		{"a reaction that names no message", func() *waEvents.Message {
			return reactionEvent(carrier, "", "👍", 1755000009000)
		}},
		{"a reaction with no id of its own", func() *waEvents.Message {
			return reactionEvent("", subject, "👍", 1755000009000)
		}},
		{"an edit that names no message", func() *waEvents.Message {
			return editEvent(carrier, "", &waE2E.Message{Conversation: proto.String("x")}, 1755000009000)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.deliverWait = 50 * time.Millisecond
			if !publishedNothing(t, session, tc.build()) {
				t.Fatal("WhatsApp was left redelivering something no build is ever going to publish")
			}
		})
	}
}

// The opposite half: what a later build could publish stays on the phone, which is the
// same trade the message path makes.
func TestWhatALaterBuildCouldPublishIsKeptOnThePhone(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		build func() *waEvents.Message
	}{
		{"a way of changing a message WhatsApp has yet to explain", func() *waEvents.Message {
			event := textMessage(carrier, "")
			event.Info.Edit = waTypes.EditAttribute("42")
			event.Message = &waE2E.Message{}
			return event
		}},
		{"an edit whose new body has no arm in this build", func() *waEvents.Message {
			corrected := &waE2E.Message{LocationMessage: &waE2E.LocationMessage{
				DegreesLatitude:  proto.Float64(-23.5),
				DegreesLongitude: proto.Float64(-46.6),
			}}
			return editEvent(carrier, subject, corrected, 1755000009000)
		}},
		{"a reaction encrypted under a message secret", func() *waEvents.Message {
			event := textMessage(carrier, "")
			event.Message = &waE2E.Message{EncReactionMessage: &waE2E.EncReactionMessage{
				TargetMessageKey: messageKey(subject),
				EncPayload:       []byte("cifrado"),
				EncIV:            make([]byte, 12),
			}}
			return event
		}},
		{"a reaction from somebody the contract cannot name", func() *waEvents.Message {
			event := reactionEvent(carrier, subject, "👍", 1755000009000)
			event.Info.Sender = waTypes.NewJID("someone", "unknown.server")
			event.Info.SenderAlt = waTypes.EmptyJID
			return event
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.deliverWait = 50 * time.Millisecond
			if publishedNothing(t, session, tc.build()) {
				t.Fatal("something nothing published was acknowledged, so WhatsApp will not send it again")
			}
		})
	}
}

// The subscription check sits in front of all of this, so a client that asked for direct
// chats only does not get a group's reactions either.
func TestAReactionInAGroupTheClientDidNotAskForIsDroppedAndAcknowledged(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.deliverWait = 50 * time.Millisecond
	event := reactionEvent(carrier, subject, "👍", 1755000009000)
	event.Info.Chat = waTypes.NewJID("120363000000000000", waTypes.GroupServer)
	event.Info.IsGroup = true

	if !publishedNothing(t, session, event) {
		t.Fatal("a group reaction the client never asked for was left for WhatsApp to redeliver for good")
	}
}

// An echo carries no sender, the same as a message the account sent from another device:
// `from_me` is the whole answer to who did it, and naming the account there files the
// operator's own number as a party in somebody else's conversation.
func TestTheEchoOfTheAccountsOwnReactionCarriesNoSender(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := reactionEvent(carrier, subject, "👍", 1755000009000)
	event.Info.IsFromMe = true

	emission := publishedBy(t, session, event)
	payload := decode(t, emission.Payload)
	if _, named := payload["sender"]; named {
		t.Fatalf("the account's own reaction named a sender: %v", payload["sender"])
	}
	if payload["from_me"] != true {
		t.Fatal("the account's own reaction was published as somebody else's")
	}
}

// publishedBy runs the handler the way whatsmeow does and returns the one event it put
// out, settled the way a working publisher settles it.
func publishedBy(t *testing.T, session *Session, event *waEvents.Message) engine.Emission {
	t.Helper()

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(event) }()
	emission := next(t, session)
	emission.Settle(nil)
	select {
	case got := <-acknowledged:
		if !got {
			t.Fatal("a published event was left unacknowledged, so WhatsApp will send it again")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never came back")
	}
	return emission
}

// publishedNothing runs the handler and reports what it answered WhatsApp with, failing
// if anything was published at all.
func publishedNothing(t *testing.T, session *Session, event *waEvents.Message) bool {
	t.Helper()

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(event) }()
	select {
	case emission := <-session.Events():
		t.Fatalf("nothing should have been published, and %s was: %s", emission.Type, string(emission.Payload))
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case got := <-acknowledged:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never came back")
		return false
	}
}

func reactionEvent(stanzaID, targetID, emoji string, at int64) *waEvents.Message {
	event := textMessage(stanzaID, "")
	event.Message = &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{
		Key:               messageKey(targetID),
		Text:              proto.String(emoji),
		SenderTimestampMS: proto.Int64(at),
	}}
	return event
}

func editEvent(stanzaID, targetID string, corrected *waE2E.Message, at int64) *waEvents.Message {
	event := textMessage(stanzaID, "")
	// Both halves, the way one arrives: whatsmeow raises IsEdit when it unwraps the
	// envelope, and WhatsApp puts the attribute on the stanza.
	event.IsEdit = true
	event.Info.Edit = waTypes.EditAttributeMessageEdit
	event.Message = &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
		Key:           messageKey(targetID),
		Type:          waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
		EditedMessage: corrected,
		TimestampMS:   proto.Int64(at),
	}}
	return event
}

func revokeEvent(stanzaID, targetID string) *waEvents.Message {
	event := textMessage(stanzaID, "")
	event.Info.Edit = waTypes.EditAttributeSenderRevoke
	event.Message = &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
		Type: waE2E.ProtocolMessage_REVOKE.Enum(),
		Key:  messageKey(targetID),
	}}
	return event
}

func messageKey(id string) *waCommon.MessageKey {
	if id == "" {
		return nil
	}
	return &waCommon.MessageKey{
		ID:        proto.String(id),
		FromMe:    proto.Bool(false),
		RemoteJID: proto.String("5511999990001@" + waTypes.DefaultUserServer),
	}
}
