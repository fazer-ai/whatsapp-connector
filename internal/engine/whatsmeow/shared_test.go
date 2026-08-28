package whatsmeow

import (
	"testing"
	"time"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

func TestAPinOnAMapIsPublishedWithItsCoordinates(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0PIN", "")
	event.Message = &waE2E.Message{LocationMessage: &waE2E.LocationMessage{
		DegreesLatitude:  proto.Float64(-25.4284),
		DegreesLongitude: proto.Float64(-49.2733),
		Name:             proto.String("Praça Tiradentes"),
		Address:          proto.String("Curitiba, PR"),
	}}

	content := inboundContentOf(t, publishedBy(t, session, event))
	switch {
	case content["type"] != "location":
		t.Fatalf("a pin was published as %v", content)
	case content["latitude"] != -25.4284 || content["longitude"] != -49.2733:
		t.Fatalf("the pin is at %v, %v", content["latitude"], content["longitude"])
	case content["name"] != "Praça Tiradentes" || content["address"] != "Curitiba, PR":
		t.Fatalf("the pin reads %v", content)
	case content["live"] != false:
		t.Fatal("a static pin was published as one the sender is still moving")
	}
}

// A live pin is where the sender was when they started, and nothing here follows the
// updates that come after. Published with `live` set rather than as an ordinary pin,
// because a stale position shown as current is the one reading a client must not make.
func TestALivePinSaysTheSenderIsStillMoving(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0LIVE", "")
	event.Message = &waE2E.Message{LiveLocationMessage: &waE2E.LiveLocationMessage{
		DegreesLatitude:  proto.Float64(-23.5505),
		DegreesLongitude: proto.Float64(-46.6333),
		Caption:          proto.String("chegando"),
	}}

	content := inboundContentOf(t, publishedBy(t, session, event))
	switch {
	case content["type"] != "location":
		t.Fatalf("a live pin was published as %v", content)
	case content["live"] != true:
		t.Fatal("a live pin was published as a static one")
	case content["name"] != "chegando":
		t.Fatalf("the live pin's caption reads %v", content["name"])
	}
}

func TestASharedCardCarriesItsNameItsNumberAndTheCardItself(t *testing.T) {
	t.Parallel()

	const vcard = "BEGIN:VCARD\nVERSION:3.0\nN:Dias;Carlos;;;\nFN:Carlos Dias\n" +
		"TEL;type=CELL;waid=5541988881111:+55 41 98888-1111\nEND:VCARD\n"

	for _, tc := range []struct {
		name  string
		card  *waE2E.ContactMessage
		want  string
		phone string
	}{
		{"named beside the card", &waE2E.ContactMessage{
			DisplayName: proto.String("Carlos Dias"), Vcard: proto.String(vcard),
		}, "Carlos Dias", "+55 41 98888-1111"},
		// WhatsApp shows the display name and not the card, so a share arriving without
		// one is a row with no label unless the card is read.
		{"named only on the card", &waE2E.ContactMessage{
			Vcard: proto.String(vcard),
		}, "Carlos Dias", "+55 41 98888-1111"},
		// A TEL whose value says nothing this connector can use. The account behind the
		// card is in the parameter, which is what makes it worth falling back to.
		{"reachable only through the waid parameter", &waE2E.ContactMessage{
			DisplayName: proto.String("Carlos Dias"),
			Vcard: proto.String("BEGIN:VCARD\nVERSION:3.0\nFN:Carlos Dias\n" +
				"TEL;type=CELL;waid=5541988881111:\nEND:VCARD\n"),
		}, "Carlos Dias", "+5541988881111"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			event := textMessage("3EB0CARD", "")
			event.Message = &waE2E.Message{ContactMessage: tc.card}

			content := inboundContentOf(t, publishedBy(t, session, event))
			if content["type"] != "contacts" {
				t.Fatalf("a shared card was published as %v", content)
			}
			cards, _ := content["contacts"].([]any)
			if len(cards) != 1 {
				t.Fatalf("one card was published as %d", len(cards))
			}
			card, _ := cards[0].(map[string]any)
			if card["display_name"] != tc.want {
				t.Errorf("the card is labelled %v, want %q", card["display_name"], tc.want)
			}
			if card["phone"] != tc.phone {
				t.Errorf("the card's number reads %v, want %q", card["phone"], tc.phone)
			}
			if card["vcard"] == nil || card["vcard"] == "" {
				// Carried verbatim: a card may hold several numbers, an email and a
				// company, none of which the other two fields can express.
				t.Error("the card itself was not carried")
			}
		})
	}
}

func TestAStackOfCardsIsPublishedAsAllOfThem(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0STACK", "")
	event.Message = &waE2E.Message{ContactsArrayMessage: &waE2E.ContactsArrayMessage{
		DisplayName: proto.String("Ana, Bruno"),
		Contacts: []*waE2E.ContactMessage{
			{DisplayName: proto.String("Ana"), Vcard: proto.String("BEGIN:VCARD\nFN:Ana\nEND:VCARD\n")},
			{DisplayName: proto.String("Bruno"), Vcard: proto.String("BEGIN:VCARD\nFN:Bruno\nEND:VCARD\n")},
		},
	}}

	content := inboundContentOf(t, publishedBy(t, session, event))
	cards, _ := content["contacts"].([]any)
	if len(cards) != 2 {
		t.Fatalf("a stack of two was published as %d cards: %v", len(cards), content)
	}
}

// The policy this slice changes. A body with no arm used to leave the acknowledgement
// withheld, which buys nothing: a poll is a poll on every redelivery, and the agent sees
// neither the message nor a reason nothing arrived. A bubble they cannot read at least
// says somebody sent something.
func TestABodyWithNoArmIsPublishedAsAPlaceholderRatherThanRedeliveredForGood(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		body   *waE2E.Message
		reason string
	}{
		{"a poll", &waE2E.Message{PollCreationMessage: &waE2E.PollCreationMessage{
			Name: proto.String("almoço?"),
		}}, "unknown_type"},
		{"a stanza carrying nothing", &waE2E.Message{}, "empty"},
		// Context info rides along on a message rather than being one, and WhatsApp
		// attaches it to stanzas whose body is genuinely absent. Counted as content, all
		// of those would be published as an unknown type instead of as the empty thing
		// they are.
		{"a stanza carrying only what rides along", &waE2E.Message{
			MessageContextInfo: &waE2E.MessageContextInfo{MessageSecret: []byte("segredo")},
		}, "empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			event := textMessage("3EB0NOARM", "")
			event.Message = tc.body

			content := inboundContentOf(t, publishedBy(t, session, event))
			if content["type"] != "unsupported" {
				t.Fatalf("a body with no arm was published as %v", content)
			}
			if content["reason"] != tc.reason {
				t.Fatalf("the placeholder says %v, want %q", content["reason"], tc.reason)
			}
		})
	}
}

// Machinery rather than something somebody sent. The placeholder above would put the
// account's own housekeeping in an agent's thread, once per sync.
func TestProtocolMachineryIsAcknowledgedRatherThanShownToAnAgent(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.deliverWait = 50 * time.Millisecond
	event := textMessage("3EB0SYNC", "")
	event.Info.IsFromMe = true
	event.Message = &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
		Type: waE2E.ProtocolMessage_HISTORY_SYNC_NOTIFICATION.Enum(),
		HistorySyncNotification: &waE2E.HistorySyncNotification{
			FileLength: proto.Uint64(1024),
		},
	}}

	if !publishedNothing(t, session, event) {
		t.Fatal("the account's own housekeeping was left for WhatsApp to redeliver for good")
	}
}

// inboundContentOf is the content of the message an emission published.
func inboundContentOf(t *testing.T, emission engine.Emission) map[string]any {
	t.Helper()

	if emission.Type != protocol.EventMessageReceived {
		t.Fatalf("the session published %s, want %s", emission.Type, protocol.EventMessageReceived)
	}
	validateAgainstContract(t, "event_message_received", emission.Payload)

	message, _ := decode(t, emission.Payload)["message"].(map[string]any)
	content, ok := message["content"].(map[string]any)
	if !ok {
		t.Fatalf("the message was published with no content: %s", emission.Payload)
	}
	return content
}
