package whatsmeow

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"

	waCommon "go.mau.fi/whatsmeow/proto/waCommon"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waWeb "go.mau.fi/whatsmeow/proto/waWeb"
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

// A message that did not reach this device the first time comes back through another
// door, and whatsmeow's parser for that door takes an edit apart on the way in: the
// corrected body becomes the message, the target becomes the id on the event, and the
// protocol message the ordinary shape keeps its key in is gone. Read the ordinary way
// that correction names nothing and is silently acknowledged away.
//
// Parsed by the library rather than assembled here on purpose. The shape is whatsmeow's
// and not this repository's, and a hand-built imitation of it would go on passing after
// the library changed it.
func TestAnEditThatCameBackThroughAResendIsStillPublished(t *testing.T) {
	t.Parallel()

	correction := &waE2E.ProtocolMessage{
		Key:           messageKey(subject),
		Type:          waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
		EditedMessage: &waE2E.Message{Conversation: proto.String("bom dia, corrigido")},
		// Off the second the stanza carries, which is the whole point of reading it: the
		// web message's own clock has no room for the milliseconds that order two
		// corrections made inside one second.
		TimestampMS: proto.Int64(1755000009750),
	}

	for _, tc := range []struct {
		name string
		raw  *waE2E.Message
	}{
		{"inside the envelope it was sent in", &waE2E.Message{
			EditedMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{ProtocolMessage: correction}},
		}},
		// The parser takes this one apart too, and raises no flag doing it. Read as an
		// ordinary message it lands under the id of the message it was correcting.
		{"with the envelope already off it", &waE2E.Message{ProtocolMessage: correction}},
		// One the account made from another device. The parser goes through this
		// envelope before it looks for the edit, and so must anything reading the raw
		// message back for the clock the parser left behind.
		{"inside the envelope another of the account's devices sent it in", &waE2E.Message{
			DeviceSentMessage: &waE2E.DeviceSentMessage{Message: &waE2E.Message{
				EditedMessage: &waE2E.FutureProofMessage{
					Message: &waE2E.Message{ProtocolMessage: correction},
				},
			}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			chat := waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
			resent := &waWeb.WebMessageInfo{
				Key: &waCommon.MessageKey{
					RemoteJID: proto.String(chat.String()),
					FromMe:    proto.Bool(false),
					ID:        proto.String(carrier),
				},
				MessageTimestamp: proto.Uint64(1755000009),
				PushName:         proto.String("Alice"),
				Message:          tc.raw,
			}
			event, err := session.current().ParseWebMessage(chat, resent)
			if err != nil {
				t.Fatalf("ParseWebMessage: %v", err)
			}

			emission := publishedBy(t, session, event)
			if emission.Type != protocol.EventMessageEdited {
				t.Fatalf("a resent edit was published as %s, want %s", emission.Type, protocol.EventMessageEdited)
			}
			validateAgainstContract(t, "event_message_edited", emission.Payload)

			payload := decode(t, emission.Payload)
			if payload["message_id"] != subject {
				t.Fatalf("the resent edit corrects %v, want %q", payload["message_id"], subject)
			}
			content, _ := payload["content"].(map[string]any)
			if content["body"] != "bom dia, corrigido" {
				t.Fatalf("the resent edit reads %v", content)
			}
			if payload["timestamp"] != float64(1755000009750) {
				t.Fatalf("the resent edit is stamped %v, and the correction's own clock reads 1755000009750",
					payload["timestamp"])
			}
		})
	}
}

// This is the shape a correction really arrives in. A phone editing a message in a plain
// one-to-one chat seals the new body under the secret of the message it is correcting and
// puts the target on the envelope; the wrapped protocol message the library also builds
// is what comes through other doors. Measured against a real phone -- the first live run
// of this phase published two of the three events and refused the edit, which is what
// sent this arm back for the socket it needs.
func TestASealedCorrectionIsOpenedAndPublished(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		plain *waE2E.Message
		want  float64
	}{
		// The plaintext is a whole message, and WhatsApp is free to put the correction
		// in either place.
		{"the new body on its own", &waE2E.Message{
			Conversation: proto.String("bom dia, corrigido"),
		}, 1755000000000},
		{"the new body inside the protocol message an unsealed one carries", &waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{
				Key:           messageKey(subject),
				Type:          waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
				EditedMessage: &waE2E.Message{Conversation: proto.String("bom dia, corrigido")},
				TimestampMS:   proto.Int64(1755000009750),
			},
		}, 1755000009750},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.unseal = func(context.Context, *waEvents.Message) (*waE2E.Message, error) {
				return tc.plain, nil
			}

			emission := publishedBy(t, session, sealedEditEvent(carrier, subject))
			if emission.Type != protocol.EventMessageEdited {
				t.Fatalf("a sealed correction was published as %s, want %s", emission.Type, protocol.EventMessageEdited)
			}
			validateAgainstContract(t, "event_message_edited", emission.Payload)

			payload := decode(t, emission.Payload)
			if payload["message_id"] != subject {
				// The envelope is the only place a sealed correction names its target.
				t.Fatalf("the sealed correction corrects %v, want %q", payload["message_id"], subject)
			}
			content, _ := payload["content"].(map[string]any)
			if content["type"] != "text" || content["body"] != "bom dia, corrigido" {
				t.Fatalf("the sealed correction reads %v", content)
			}
			if payload["timestamp"] != tc.want {
				t.Fatalf("the sealed correction is stamped %v, want %v", payload["timestamp"], tc.want)
			}
		})
	}
}

// The two ways opening one fails are not the same answer, and this milestone has got
// that backwards in both directions already. A key that was never stored is not going to
// be handed over by a redelivery; a socket that is not up yet will be.
func TestASealedCorrectionIsKeptOnlyWhileTheKeysMayStillArrive(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		failure error
		keep    bool
	}{
		{"a secret this session never had", wm.ErrOriginalMessageSecretNotFound, false},
		{"a payload that will not decrypt", errors.New("message authentication failed"), false},
		{"a socket that is not up yet", wm.ErrNotLoggedIn, true},
		{"a store that ran out of time", context.DeadlineExceeded, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.deliverWait = 50 * time.Millisecond
			session.unseal = func(context.Context, *waEvents.Message) (*waE2E.Message, error) {
				return nil, tc.failure
			}

			acknowledged := publishedNothing(t, session, sealedEditEvent(carrier, subject))
			switch {
			case tc.keep && acknowledged:
				t.Fatal("a correction whose keys may still arrive was acknowledged, so WhatsApp will not send it again")
			case !tc.keep && !acknowledged:
				t.Fatal("a correction nothing is ever going to open was left for WhatsApp to redeliver for good")
			}
		})
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
			// The participant a real one carries: in a group it is half of what names
			// the message, and a key without it names none.
			event.Message.GetProtocolMessage().GetKey().Participant =
				proto.String("5541988887777@" + waTypes.DefaultUserServer)
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

// A deletion's key names the message it deletes by (id, participant), and any member of
// a group can put anybody in that second field. WhatsApp applies nothing when the name
// is wrong, and the client is the only side that can tell -- it has the message and
// knows who wrote it. Publishing the id alone is what left an agent looking at a bubble
// marked deleted while every phone in the group still showed the message.
func TestADeletionCarriesTheAuthorItsKeyClaims(t *testing.T) {
	t.Parallel()

	const author = "5541988887777"

	for _, tc := range []struct {
		name        string
		group       bool
		participant string
		fromMe      bool
		want        string
	}{
		{name: "the key names who wrote it", group: true, participant: author + "@" + waTypes.DefaultUserServer, want: author},
		// The legacy spelling, agent and device and all. It is the same person, and
		// nothing about a formatting difference makes the claim less true -- which is why
		// the check below counts what ParseJID discards instead of demanding that a JID
		// come back out spelled the way it went in.
		{name: "the key names who wrote it, with a device on the address", group: true,
			participant: author + ".0:12@" + waTypes.DefaultUserServer, want: author},
		// `from_me` on the key says the message is the sender's own, and WhatsApp
		// resolves it that way whatever the participant says. Reading the participant
		// here would publish a claim WhatsApp does not make, and it is the claim that
		// makes the client's comparison pass for a deletion no phone applied.
		{name: "the key claims the sender's own message and names somebody else too", group: true,
			participant: author + "@" + waTypes.DefaultUserServer, fromMe: true, want: "5511999990001"},
		{name: "the key claims the sender's own message and names nobody", group: true, fromMe: true, want: "5511999990001"},
		// A direct chat's key names the chat, and there are two parties to be: `sender`
		// and `by` are the whole answer, and no claim is the honest shape for it.
		{name: "a direct chat, where the key names nobody", want: ""},
		// And a participant there is a field WhatsApp does not read: `BuildMessageKey`
		// writes one only outside a chat, and `getOrigSenderFromKey` takes the key's own
		// remote JID instead. Publishing it would hand a client a claim nothing else
		// honours, and the claim is what makes its comparison pass.
		{name: "a direct chat, where the key names a participant anyway",
			participant: "5511999990001@" + waTypes.DefaultUserServer, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.setGroups(true)
			event := revokeEvent(carrier, subject)
			if tc.group {
				event.Info.Chat = waTypes.NewJID("120363000000000000", waTypes.GroupServer)
				event.Info.IsGroup = true
			}
			if tc.participant != "" {
				event.Message.GetProtocolMessage().GetKey().Participant = proto.String(tc.participant)
			}
			if tc.fromMe {
				event.Message.GetProtocolMessage().GetKey().FromMe = proto.Bool(true)
			}

			emission := publishedBy(t, session, event)
			if emission.Type != protocol.EventMessageRevoked {
				t.Fatalf("a deletion was published as %s, want %s", emission.Type, protocol.EventMessageRevoked)
			}
			validateAgainstContract(t, "event_message_revoked", emission.Payload)

			payload := decode(t, emission.Payload)
			claimed, named := payload["message_author"].(map[string]any)
			switch {
			case tc.want == "" && named:
				t.Fatalf("the deletion says %v wrote the message, want no claim at all", claimed)
			case tc.want == "":
			case !named:
				t.Fatalf("the deletion claims nobody wrote the message, want the author its key names, %q", tc.want)
			case claimed["phone"] != tc.want:
				t.Fatalf("the deletion says %v wrote the message, want the author its key names, %q", claimed["phone"], tc.want)
			}
		})
	}
}

// In a group a key identifies a message by its participant, or by `from_me` where it is
// the sender's own. One that carries neither names no message: WhatsApp applies nothing
// and every phone goes on showing it. Publishing without the claim would not do -- absent
// is what a direct chat sends, and a client cannot tell the two apart -- so an agent would
// lose a bubble on the strength of a key nobody else honoured.
func TestAGroupDeletionWhoseKeyNamesNoAuthorIsDropped(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		participant string
	}{
		{"the key names nobody", ""},
		{"the key names something that is not an address", "quem escreveu"},
		{"the key names an address whose number is not one", "not-a-number@" + waTypes.DefaultUserServer},
		{"the key names a group rather than a person", "120363000000000009@" + waTypes.GroupServer},
		// ParseJID splits on `@` and keeps the first two pieces, so this reads as the real
		// participant with the rest thrown away. The key is not one any WhatsApp client
		// writes, and the claim it would produce names somebody it does not name.
		{"the key names an address with something after it", "5541988887777@" + waTypes.DefaultUserServer + "@junk"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.setGroups(true)
			event := revokeEvent(carrier, subject)
			event.Info.Chat = waTypes.NewJID("120363000000000000", waTypes.GroupServer)
			event.Info.IsGroup = true
			if tc.participant != "" {
				event.Message.GetProtocolMessage().GetKey().Participant = proto.String(tc.participant)
			}

			// Acknowledged all the same: WhatsApp resending it would only produce the
			// same key, and holding it back keeps the phone trying forever.
			if !publishedNothing(t, session, event) {
				t.Fatal("a deletion that names no message was left unacknowledged, so WhatsApp will send it again")
			}
		})
	}
}

// Which of WhatsApp's two identifiers arrives as the sender and which as the alternative
// depends on the chat's addressing mode. A claim carrying only one of them is one the
// client may not be able to match -- its copy of the message is keyed by whichever half it
// learned first -- and a claim it cannot match reads as somebody else having written the
// message, which drops a deletion that was real.
//
// Addressed by the LID, which is the direction that carries both: the alternative there is
// the number off `participant_pn`, read from the stanza and nowhere else. The other way
// round the alternative may have been filled in from the mapping the deployment shares,
// and `TestADeletionsAuthorIsNamedByWhatTheStanzaCarried` is that case.
func TestADeletionOfTheSendersOwnClaimsBothNamespaces(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setGroups(true)
	event := revokeEvent(carrier, subject)
	event.Info.Chat = waTypes.NewJID("120363000000000000", waTypes.GroupServer)
	event.Info.IsGroup = true
	event.Info.Sender = waTypes.NewJID("998877665544332", waTypes.HiddenUserServer)
	event.Info.SenderAlt = waTypes.NewJID("5541988887777", waTypes.DefaultUserServer)
	event.Message.GetProtocolMessage().GetKey().FromMe = proto.Bool(true)

	emission := publishedBy(t, session, event)
	validateAgainstContract(t, "event_message_revoked", emission.Payload)

	claimed, named := decode(t, emission.Payload)["message_author"].(map[string]any)
	if !named {
		t.Fatal("the deletion claims nobody wrote the message")
	}
	if claimed["phone"] != "5541988887777" || claimed["lid"] != "998877665544332" {
		t.Errorf("the claim names %v, want both namespaces the event carried", claimed)
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

// sealedEditEvent is a correction as a phone really sends one: the new body sealed under
// the secret of the message being corrected, the target on the envelope, and the same
// edit attribute an unsealed correction carries -- so nothing but the sealed body
// separates the two on the way in.
func sealedEditEvent(stanzaID, targetID string) *waEvents.Message {
	event := textMessage(stanzaID, "")
	event.Info.Edit = waTypes.EditAttributeMessageEdit
	event.Message = &waE2E.Message{SecretEncryptedMessage: &waE2E.SecretEncryptedMessage{
		TargetMessageKey: messageKey(targetID),
		SecretEncType:    waE2E.SecretEncryptedMessage_MESSAGE_EDIT.Enum(),
		EncPayload:       []byte("cifrado"),
		EncIV:            make([]byte, 12),
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

// sealedReactionEvent is the stanza a community announcement group sends: the emoji
// sealed under the secret of the message it is on, and the message it names carried on
// the envelope rather than inside the ciphertext.
func sealedReactionEvent(stanzaID, targetID string) *waEvents.Message {
	event := textMessage(stanzaID, "")
	event.Message = &waE2E.Message{EncReactionMessage: &waE2E.EncReactionMessage{
		TargetMessageKey: messageKey(targetID),
		EncPayload:       []byte("cifrado"),
		EncIV:            make([]byte, 12),
	}}
	return event
}

// The reaction opens, and the message it belongs to comes off the envelope.
//
// This is the arm that fails silently when it is wrong. EncryptReaction takes the key
// off the reaction before sealing it, so what comes back out of the decryption names no
// message at all: handed on as it is, it is dropped for having no target, with the emoji
// decrypted and in hand and nothing in the log saying it was ever readable. The stand-in
// returns exactly that shape, which is what makes this a test of the reattachment rather
// than of the decryption.
func TestASealedReactionIsPublishedWithTheTargetOffItsEnvelope(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.unsealReaction = func(context.Context, *waEvents.Message) (*waE2E.ReactionMessage, error) {
		return &waE2E.ReactionMessage{
			Text:              proto.String("🎉"),
			SenderTimestampMS: proto.Int64(1755000009000),
		}, nil
	}

	emission := publishedBy(t, session, sealedReactionEvent(carrier, subject))
	if emission.Type != protocol.EventMessageReaction {
		t.Fatalf("a sealed reaction was published as %s, want %s", emission.Type, protocol.EventMessageReaction)
	}
	validateAgainstContract(t, "event_message_reaction", emission.Payload)

	payload := decode(t, emission.Payload)
	if payload["target_id"] != subject {
		t.Errorf("the reaction annotates %v, and the envelope named %q", payload["target_id"], subject)
	}
	if payload["emoji"] != "🎉" {
		t.Errorf("the reaction reads %v, and what was sealed was 🎉", payload["emoji"])
	}
	if payload["id"] != carrier {
		t.Errorf("the reaction is filed under %v, and the client deduplicates on %q", payload["id"], carrier)
	}
}

// The two ways opening one fails are opposite answers, and this milestone has already
// got that backwards twice. A socket that is not up yet is not final: the same stanza
// redelivered to a ready session opens. A secret this session never had is: it was
// stored when the message being reacted to arrived, and a redelivery brings the same
// ciphertext and no key, so keeping it buys a loop that can never end.
func TestASealedReactionKeepsOnlyWhatARedeliveryCouldFix(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		err     error
		keeping bool
	}{
		{"a socket that is not up yet", wm.ErrNotLoggedIn, true},
		{"a client that is gone", wm.ErrClientIsNil, true},
		{"a store that ran out of time", context.DeadlineExceeded, true},
		{"a session on its way out", context.Canceled, true},
		{"a secret this session never had", errors.New("original message secret not found"), false},
		{"a payload that will not open", errors.New("failed to decrypt reaction: cipher: message authentication failed"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.deliverWait = 50 * time.Millisecond
			session.unsealReaction = func(context.Context, *waEvents.Message) (*waE2E.ReactionMessage, error) {
				return nil, tc.err
			}

			acknowledged := publishedNothing(t, session, sealedReactionEvent(carrier, subject))
			if tc.keeping && acknowledged {
				t.Fatal("a reaction a ready session could have opened was acknowledged, so WhatsApp will not send it again")
			}
			if !tc.keeping && !acknowledged {
				t.Fatal("a reaction nothing can ever open was kept, and WhatsApp will send it again for as long as this session is up")
			}
		})
	}
}

// An envelope with no message on it is nothing a chat can show, and no redelivery
// supplies one. Acknowledged, or it comes back for as long as the session is up.
func TestASealedReactionThatNamesNoMessageIsDroppedAndAcknowledged(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.deliverWait = 50 * time.Millisecond
	session.unsealReaction = func(context.Context, *waEvents.Message) (*waE2E.ReactionMessage, error) {
		return &waE2E.ReactionMessage{Text: proto.String("🎉")}, nil
	}

	event := sealedReactionEvent(carrier, subject)
	event.Message.EncReactionMessage.TargetMessageKey = nil
	if !publishedNothing(t, session, event) {
		t.Fatal("a reaction with no message to put it on was kept, and it names none on the way back either")
	}
}

// The wiring, against whatsmeow's own crypto rather than a stand-in.
//
// The stand-in above pins what this build does with what comes back; this pins that
// what comes back is what whatsmeow produces. It is the half a stand-in cannot cover:
// that DecryptReaction is the right call for this envelope, and that the key really is
// missing from the plaintext, which is the whole reason the reattachment exists. A
// stand-in that returned a key would have agreed with a build that never reattached one.
func TestASealedReactionOpensAgainstWhatsmeowsOwnCrypto(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	client := session.current()

	// The account's hidden identifier. Encryption derives the key from it, and
	// decryption from the sender of the stanza, so the reaction has to arrive as the
	// account's own for the two to meet -- which is what a reaction of one's own in an
	// announcement group is.
	own, err := waTypes.ParseJID("167392323834034@lid")
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	client.Store.LID = own

	chat := waTypes.NewJID("120363000000000000", waTypes.GroupServer)
	author, err := waTypes.ParseJID("167392323834035@lid")
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	// The secret whatsmeow filed when the announcement itself arrived. Without it there
	// is nothing to derive either half from.
	secret := bytes.Repeat([]byte{7}, 32)
	if err := client.Store.MsgSecrets.PutMessageSecret(t.Context(), chat, author, subject, secret); err != nil {
		t.Fatalf("PutMessageSecret: %v", err)
	}

	plain := &waE2E.ReactionMessage{
		Key: &waCommon.MessageKey{
			RemoteJID:   proto.String(chat.String()),
			FromMe:      proto.Bool(false),
			ID:          proto.String(subject),
			Participant: proto.String(author.String()),
		},
		Text:              proto.String("🎉"),
		SenderTimestampMS: proto.Int64(1755000009000),
	}
	sealed, err := client.EncryptReaction(t.Context(), &waTypes.MessageInfo{
		ID: subject,
		MessageSource: waTypes.MessageSource{
			Chat:   chat,
			Sender: author,
		},
	}, plain)
	if err != nil {
		t.Fatalf("EncryptReaction: %v", err)
	}
	// What #38 warned about, asserted rather than assumed: the library takes the key off
	// the reaction on its way in, so nothing downstream of the decryption knows what the
	// reaction was put on.
	if sealed.GetTargetMessageKey().GetID() != subject {
		t.Fatalf("the envelope names %q, and the reaction was put on %q",
			sealed.GetTargetMessageKey().GetID(), subject)
	}
	if plain.GetKey() != nil {
		t.Fatal("the library left the key on the reaction, and the reattachment this tests is for its absence")
	}

	event := textMessage(carrier, "")
	event.Info.Chat = chat
	event.Info.Sender = own
	event.Info.SenderAlt = waTypes.EmptyJID
	event.Info.IsGroup = true
	event.Message = &waE2E.Message{EncReactionMessage: sealed}

	session.setGroups(true)
	emission := publishedBy(t, session, event)
	if emission.Type != protocol.EventMessageReaction {
		t.Fatalf("what came out is %s", emission.Type)
	}
	payload := decode(t, emission.Payload)
	if payload["target_id"] != subject {
		t.Errorf("the reaction annotates %v, and it was put on %q", payload["target_id"], subject)
	}
	if payload["emoji"] != "🎉" {
		t.Errorf("the reaction reads %v, and what was sealed was 🎉", payload["emoji"])
	}
}

// The author a deletion claims is named off the same event a message is, so it is held to
// the same rule: a phone-addressed stanza that arrived without `sender_lid` has one filled
// in from the mapping every account on the deployment shares, and naming the author by it
// would put another operator's pairing on this event and into what the session learns.
func TestADeletionsAuthorIsNamedByWhatTheStanzaCarried(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	author := session.claimedAuthor(&waEvents.Message{
		Info: waTypes.MessageInfo{
			MessageSource: waTypes.MessageSource{
				IsGroup:   true,
				Chat:      waTypes.NewJID("120363000000000001", waTypes.GroupServer),
				Sender:    waTypes.NewJID("5511999990012", waTypes.DefaultUserServer),
				SenderAlt: waTypes.NewJID("167392323834045", waTypes.HiddenUserServer),
			},
		},
		Message: &waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{
				Type: waE2E.ProtocolMessage_REVOKE.Enum(),
				Key:  &waCommon.MessageKey{FromMe: proto.Bool(true), ID: proto.String("3EB0")},
			},
		},
	}, protocol.Address{Kind: protocol.AddressGroup, ID: "120363000000000001"})
	if author == nil {
		t.Fatal("the deletion named no author at all")
	}
	if author.LID != "" {
		t.Errorf("the author is %+v, want the address the stanza carried and nothing filled in", author)
	}
	if author.Phone != "5511999990012" {
		t.Errorf("the author is %+v, want the number the stanza was addressed by", author)
	}
}
