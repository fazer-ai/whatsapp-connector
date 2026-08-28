package whatsmeow

import (
	"math"
	"strings"
	"testing"
	"time"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waEvents "go.mau.fi/whatsmeow/types/events"
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

// A live pin arrives in either shape, and the sender is what says which it is. Read off
// the arm alone, the one below is published as a static pin at the position the sender
// has already left.
func TestALivePinInTheStaticShapeStillSaysSo(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0PINLIVE", "")
	event.Message = &waE2E.Message{LocationMessage: &waE2E.LocationMessage{
		DegreesLatitude:  proto.Float64(-23.5505),
		DegreesLongitude: proto.Float64(-46.6333),
		IsLive:           proto.Bool(true),
	}}

	content := inboundContentOf(t, publishedBy(t, session, event))
	if content["live"] != true {
		t.Fatalf("a moving pin was published as a static one: %v", content)
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

// Both getters answer zero for a coordinate that is not there, so a pin missing one is
// published somewhere the sender has never been, rendered as where they are.
func TestAPinMissingACoordinateIsNotPublishedAsAPlaceInTheGulfOfGuinea(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		body *waE2E.Message
	}{
		{"a pin with no latitude", &waE2E.Message{LocationMessage: &waE2E.LocationMessage{
			DegreesLongitude: proto.Float64(-46.6333),
		}}},
		{"a live pin with no longitude", &waE2E.Message{LiveLocationMessage: &waE2E.LiveLocationMessage{
			DegreesLatitude: proto.Float64(-23.5505),
		}}},
		{"a pin nobody can draw", &waE2E.Message{LocationMessage: &waE2E.LocationMessage{
			DegreesLatitude:  proto.Float64(500),
			DegreesLongitude: proto.Float64(-46.6333),
		}}},
		// The worst of the three, and the reason this is a range check rather than a nil
		// check: JSON has no way to write one, so the event fails to render, the
		// acknowledgement is withheld, and the message redelivers for as long as the
		// session is up -- a loop inside the change that exists to end loops.
		{"a pin at a coordinate that is not a number", &waE2E.Message{
			LocationMessage: &waE2E.LocationMessage{
				DegreesLatitude:  proto.Float64(math.NaN()),
				DegreesLongitude: proto.Float64(math.Inf(1)),
			},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			event := textMessage("3EB0NOWHERE", "")
			event.Message = tc.body

			content := inboundContentOf(t, publishedBy(t, session, event))
			if content["type"] == "location" {
				t.Fatalf("a pin missing a coordinate was published as a place: %v", content)
			}
			if content["type"] != "unsupported" {
				t.Fatalf("a pin missing a coordinate was published as %v", content)
			}
		})
	}
}

// A body this build cannot read is still a reply, still tags people, and is still on the
// chat's timer. The placeholder is the one renderer that must not lose that: it is the
// one that runs for every arm nobody has written a renderer for.
func TestAPlaceholderKeepsWhatTheMessageWasAnnotatedWith(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0POLLREPLY", "")
	event.Message = &waE2E.Message{PollCreationMessage: &waE2E.PollCreationMessage{
		Name: proto.String("almoço?"), ContextInfo: annotatedContext(),
	}}

	assertPlaceholderKeptItsAnnotation(t, session, event)
}

// Twenty-seven of the message's arms are a wrapper carrying another message, and
// whatsmeow unwraps six of them before a handler sees the event. What is left keeps the
// annotation on what is inside, so a placeholder that only looks at the top publishes a
// reply that answers nothing.
func TestAPlaceholderKeepsTheAnnotationOfABodyThatArrivedWrapped(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0WRAPPED", "")
	event.Message = &waE2E.Message{SpoilerMessage: &waE2E.FutureProofMessage{
		Message: &waE2E.Message{PollCreationMessage: &waE2E.PollCreationMessage{
			Name:        proto.String("almoço?"),
			ContextInfo: annotatedContext(),
		}},
	}}

	assertPlaceholderKeptItsAnnotation(t, session, event)
}

// The same thing through the wrappers that are not a FutureProofMessage. Five arms nest a
// message under a type of their own, whatsmeow unwraps none of them, and a descent that
// matched on the wrapper's type instead of on the nesting followed the twenty-seven that
// share a name and lost the annotation of exactly these.
func TestAPlaceholderKeepsTheAnnotationOfABodyWrappedInSomethingOtherThanAFutureProof(t *testing.T) {
	t.Parallel()

	unreadable := func() *waE2E.Message {
		return &waE2E.Message{PollCreationMessage: &waE2E.PollCreationMessage{
			Name:        proto.String("almoço?"),
			ContextInfo: annotatedContext(),
		}}
	}

	for _, tc := range []struct {
		name string
		body *waE2E.Message
	}{
		{"a comment on a channel post", &waE2E.Message{
			CommentMessage: &waE2E.CommentMessage{Message: unreadable()},
		}},
		{"the note on a payment", &waE2E.Message{
			SendPaymentMessage: &waE2E.SendPaymentMessage{NoteMessage: unreadable()},
		}},
		{"the note on a request for one", &waE2E.Message{
			RequestPaymentMessage: &waE2E.RequestPaymentMessage{NoteMessage: unreadable()},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			event := textMessage("3EB0OTHERWRAP", "")
			event.Message = tc.body

			assertPlaceholderKeptItsAnnotation(t, session, event)
		})
	}
}

func annotatedContext() *waE2E.ContextInfo {
	return &waE2E.ContextInfo{
		StanzaID:      proto.String("3EB0QUOTED"),
		MentionedJID:  []string{"5511999990002@s.whatsapp.net"},
		Expiration:    proto.Uint32(604800),
		Participant:   proto.String("5511999990001@s.whatsapp.net"),
		QuotedMessage: &waE2E.Message{Conversation: proto.String("onde vamos?")},
	}
}

func assertPlaceholderKeptItsAnnotation(t *testing.T, session *Session, event *waEvents.Message) {
	t.Helper()

	emission := publishedBy(t, session, event)
	message, _ := decode(t, emission.Payload)["message"].(map[string]any)
	if message["quoted_id"] != "3EB0QUOTED" {
		t.Errorf("the placeholder answers %v, and the poll was a reply to %q", message["quoted_id"], "3EB0QUOTED")
	}
	if message["ephemeral"] != float64(604800) {
		t.Errorf("the placeholder is on timer %v, want the chat's own", message["ephemeral"])
	}
	mentions, _ := message["mentions"].([]any)
	if len(mentions) != 1 {
		t.Errorf("the placeholder tags %v", message["mentions"])
	}
}

// protobuf keeps an arm newer than this descriptor in the unknown bytes, and Range never
// visits it. Read as empty, a message made of nothing else loses its reason -- and one
// that also carried a group's key is dropped as key material and never seen at all,
// which is the opposite of what the placeholder is for.
func TestAnArmNewerThanThisBuildIsStillPublished(t *testing.T) {
	t.Parallel()

	// Field 9999, length-delimited: a body this descriptor has no name for.
	newer := &waE2E.Message{}
	newer.ProtoReflect().SetUnknown([]byte{0xfa, 0xe1, 0x03, 0x02, 0x68, 0x69})

	for _, tc := range []struct {
		name  string
		build func() *waE2E.Message
	}{
		{"on its own", func() *waE2E.Message { return newer }},
		{"alongside a group's key", func() *waE2E.Message {
			withKey := &waE2E.Message{SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{
				GroupID:                             proto.String("120363000000000000@g.us"),
				AxolotlSenderKeyDistributionMessage: []byte("chave"),
			}}
			withKey.ProtoReflect().SetUnknown(newer.ProtoReflect().GetUnknown())
			return withKey
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.setGroups(true)
			event := textMessage("3EB0NEWER", "")
			event.Message = tc.build()

			content := inboundContentOf(t, publishedBy(t, session, event))
			if content["type"] != "unsupported" {
				t.Fatalf("an arm newer than this build was published as %v", content)
			}
			if content["reason"] != "unknown_type" {
				t.Fatalf("an arm newer than this build was called %v, and it is not empty", content["reason"])
			}
		})
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
		// How several address books export a card: the property carries a group, and the
		// group is not part of its name.
		{"written with property groups", &waE2E.ContactMessage{
			Vcard: proto.String("BEGIN:VCARD\nVERSION:3.0\nitem1.FN:Carlos Dias\n" +
				"item2.TEL;type=CELL;waid=5541988881111:+55 41 98888-1111\nEND:VCARD\n"),
		}, "Carlos Dias", "+55 41 98888-1111"},
		// RFC 2426 breaks a property past 75 octets and starts the next line with one
		// space that is not part of the value, which is what an address book exports. A
		// TEL with a few parameters is past 75 before the number begins.
		{"folded across physical lines", &waE2E.ContactMessage{
			Vcard: proto.String("BEGIN:VCARD\nVERSION:3.0\nFN:Carlos D\n ias\n" +
				"item2.TEL;type=CELL;type=VOICE;type=pref;waid=5541988881111\n" +
				" :+55 41 98888-1111\nEND:VCARD\n"),
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

// Media never travels inside a frame, and a card is the one place it could arrive
// already inside one: a contact picture is base64 in the card's own text, so a card
// published verbatim puts a file on the wire in a field nothing can fetch and nothing
// bounds. What has to survive is everything the contract reads off the card.
//
// Both wrappings are here because they are what a card in the wild is written in, and
// they look nothing alike: RFC 2426 folds a long value under a leading space, while a
// vCard 2.1 base64 block is left flush against the margin and ends on a blank line. A
// strip that only understood folding would keep the second one's file and, worse, read
// the lines after it as part of the card.
func TestACardArrivesWithoutTheFilesItCarried(t *testing.T) {
	t.Parallel()

	const picture = "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkSEw8U"

	for _, tc := range []struct {
		name string
		card string
	}{
		{"folded under a space, the way RFC 2426 says", "BEGIN:VCARD\nVERSION:3.0\n" +
			"FN:Carlos Dias\nPHOTO;ENCODING=b;TYPE=JPEG:" + picture + "\n " + picture +
			"\n " + picture + "\nTEL;type=CELL;waid=5541988881111:+55 41 98888-1111\nEND:VCARD\n"},
		{"left flush, the way a vCard 2.1 block is", "BEGIN:VCARD\nVERSION:2.1\n" +
			"FN:Carlos Dias\nPHOTO;ENCODING=BASE64;JPEG:\n" + picture + "\n" + picture +
			"\n\nTEL;type=CELL;waid=5541988881111:+55 41 98888-1111\nEND:VCARD\n"},
		// The group is not part of the property's name, so a stripper matching on the
		// whole token keeps the file on every card an address book exported.
		{"written with a property group", "BEGIN:VCARD\nVERSION:3.0\n" +
			"item1.FN:Carlos Dias\nitem2.PHOTO;ENCODING=b:" + picture +
			"\nitem3.TEL;type=CELL;waid=5541988881111:+55 41 98888-1111\nEND:VCARD\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			event := textMessage("3EB0PHOTO", "")
			event.Message = &waE2E.Message{ContactMessage: &waE2E.ContactMessage{
				Vcard: proto.String(tc.card),
			}}

			content := inboundContentOf(t, publishedBy(t, session, event))
			cards, _ := content["contacts"].([]any)
			if len(cards) != 1 {
				t.Fatalf("one card was published as %d", len(cards))
			}
			card, _ := cards[0].(map[string]any)

			published, _ := card["vcard"].(string)
			if strings.Contains(published, picture) {
				t.Errorf("the picture went out on the wire inside the card: %q", published)
			}
			if strings.Contains(strings.ToUpper(published), "PHOTO") {
				t.Errorf("the property the picture was on is still on the card: %q", published)
			}
			// The point of stripping rather than dropping the card: what the contract
			// reads off it has to come through untouched.
			if card["display_name"] != "Carlos Dias" {
				t.Errorf("the card is labelled %v, want %q", card["display_name"], "Carlos Dias")
			}
			if card["phone"] != "+55 41 98888-1111" {
				t.Errorf("the card's number reads %v, want %q", card["phone"], "+55 41 98888-1111")
			}
			if !strings.Contains(published, "END:VCARD") {
				t.Errorf("the card lost its end: %q", published)
			}
		})
	}
}

// The other half, and the one that says the strip is a strip and not a rewrite: a card
// carrying no file is published byte for byte as it arrived.
func TestACardCarryingNoFileIsPublishedAsItArrived(t *testing.T) {
	t.Parallel()

	const card = "BEGIN:VCARD\nVERSION:3.0\nN:Dias;Carlos;;;\nFN:Carlos Dias\n" +
		"ORG:Acme\nEMAIL;type=INTERNET:carlos@acme.example\n" +
		"TEL;type=CELL;waid=5541988881111:+55 41 98888-1111\nEND:VCARD\n"

	session, _ := newTestSession(t, "5511999990001")
	event := textMessage("3EB0PLAINCARD", "")
	event.Message = &waE2E.Message{ContactMessage: &waE2E.ContactMessage{
		Vcard: proto.String(card),
	}}

	content := inboundContentOf(t, publishedBy(t, session, event))
	cards, _ := content["contacts"].([]any)
	if len(cards) != 1 {
		t.Fatalf("one card was published as %d", len(cards))
	}
	if published, _ := cards[0].(map[string]any)["vcard"].(string); published != card {
		t.Errorf("the card was rewritten on the way out:\n got %q\nwant %q", published, card)
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

// What is not something somebody sent gets no bubble at all. The placeholder is for a
// message this build cannot read, not for everything that travels as one: each of these
// would otherwise be an unreadable bubble in an agent's thread, at the rate the account
// syncs, a group changes members, or a poll is voted in.
func TestWhatIsNotAMessageIsAcknowledgedRatherThanShownToAnAgent(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		body *waE2E.Message
	}{
		{"the account's own plumbing", &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
			Type: waE2E.ProtocolMessage_HISTORY_SYNC_NOTIFICATION.Enum(),
			HistorySyncNotification: &waE2E.HistorySyncNotification{
				FileLength: proto.Uint64(1024),
			},
		}}},
		// whatsmeow files this and dispatches it anyway, so it reaches the same handler
		// a message does.
		{"a group handing out the key its messages are readable with", &waE2E.Message{
			SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{
				GroupID:                             proto.String("120363000000000000@g.us"),
				AxolotlSenderKeyDistributionMessage: []byte("chave"),
			},
		}},
		{"the other three shapes key material travels in", &waE2E.Message{
			GroupRootKeyShare: &waE2E.GroupRootKeyShare{},
		}},
		{"a vote, which marks a poll rather than adding a message", &waE2E.Message{
			PollUpdateMessage: &waE2E.PollUpdateMessage{
				PollCreationMessageKey: messageKey(subject),
			},
		}},
		{"an option somebody added to a poll", &waE2E.Message{
			PollAddOptionMessage: &waE2E.PollAddOptionMessage{
				PollCreationMessageKey: messageKey(subject),
			},
		}},
		{"a disappearing message being kept", &waE2E.Message{
			KeepInChatMessage: &waE2E.KeepInChatMessage{Key: messageKey(subject)},
		}},
		{"a message being pinned", &waE2E.Message{
			PinInChatMessage: &waE2E.PinInChatMessage{Key: messageKey(subject)},
		}},
		{"an RSVP, which names the event it answers", &waE2E.Message{
			EncEventResponseMessage: &waE2E.EncEventResponseMessage{
				EventCreationMessageKey: messageKey(subject),
				EncPayload:              []byte("cifrado"),
				EncIV:                   make([]byte, 12),
			},
		}},
		// Every sealed type but the correction, which has an event of its own and is
		// opened before this runs.
		{"a poll edited under the seal of the message it edits", &waE2E.Message{
			SecretEncryptedMessage: &waE2E.SecretEncryptedMessage{
				TargetMessageKey: messageKey(subject),
				SecretEncType:    waE2E.SecretEncryptedMessage_POLL_EDIT.Enum(),
				EncPayload:       []byte("cifrado"),
				EncIV:            make([]byte, 12),
			},
		}},
		{"a scheduled call being called off", &waE2E.Message{
			ScheduledCallEditMessage: &waE2E.ScheduledCallEditMessage{
				Key:      messageKey(subject),
				EditType: waE2E.ScheduledCallEditMessage_CANCEL.Enum(),
			},
		}},
		{"a payment request being declined", &waE2E.Message{
			DeclinePaymentRequestMessage: &waE2E.DeclinePaymentRequestMessage{Key: messageKey(subject)},
		}},
		{"a sticker pack being asked for again", &waE2E.Message{
			StickerSyncRmrMessage: &waE2E.StickerSyncRMRMessage{
				Filehash:         []string{"abc"},
				RequestTimestamp: proto.Int64(1755000000),
			},
		}},
		// Every few seconds for as long as somebody is walking, if it were a message.
		{"a further position in a live share", &waE2E.Message{
			LiveLocationMessage: &waE2E.LiveLocationMessage{
				DegreesLatitude:  proto.Float64(-23.5505),
				DegreesLongitude: proto.Float64(-46.6333),
				SequenceNumber:   proto.Int64(7),
			},
		}},
		// The one that names what it marks without a key: a split by id and a person by
		// JID, and those two fields are the whole arm. Every status change on a bill
		// being divided, as a bubble, if this were a message.
		{"somebody's share of a split payment changing", &waE2E.Message{
			SplitPaymentUpdateMessage: &waE2E.SplitPaymentUpdateMessage{
				SplitID:        proto.String("split-1"),
				ParticipantJID: proto.String("5541988881111@s.whatsapp.net"),
			},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.deliverWait = 50 * time.Millisecond
			event := textMessage("3EB0NOTAMESSAGE", "")
			event.Info.IsFromMe = true
			event.Message = tc.body

			if !publishedNothing(t, session, event) {
				t.Fatal("something that is not a message was left for WhatsApp to redeliver for good")
			}
		})
	}
}

// The other side of the same list, and the side that matters. A protocol message is not
// automatically plumbing: some of them are somebody acting in the conversation, and
// acknowledging one is losing it. The contract has a reason that says exactly this.
func TestAProtocolMessageSomebodySentIsShownRatherThanDropped(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		kind waE2E.ProtocolMessage_Type
	}{
		// The answer to WhatsApp's "share your phone number" prompt: a contact deciding
		// to be reachable, dropped by a blanket rule and gone.
		{"a contact sharing their phone number", waE2E.ProtocolMessage_SHARE_PHONE_NUMBER},
		{"a disappearing timer being set", waE2E.ProtocolMessage_EPHEMERAL_SETTING},
		// Nobody has looked at this one, which is the point of the list running the
		// other way: an unread type is visible rather than silently gone.
		{"a type nobody has looked at yet", waE2E.ProtocolMessage_REMINDER_MESSAGE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			event := textMessage("3EB0SOMEBODY", "")
			event.Message = &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{Type: tc.kind.Enum()}}

			content := inboundContentOf(t, publishedBy(t, session, event))
			if content["type"] != "unsupported" || content["reason"] != "protocol" {
				t.Fatalf("something somebody did was published as %v, want the protocol placeholder", content)
			}
		})
	}
}

// The key usually rides along with the message it was sent to make readable, and then
// that message is what the agent sees. Dropped on the pair, a group's first message from
// a new member disappears.
func TestAMessageCarryingAGroupsKeyIsStillTheMessage(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setGroups(true)
	event := textMessage("3EB0WITHKEY", "")
	event.Message = &waE2E.Message{
		Conversation: proto.String("bom dia"),
		SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{
			GroupID:                             proto.String("120363000000000000@g.us"),
			AxolotlSenderKeyDistributionMessage: []byte("chave"),
		},
	}

	content := inboundContentOf(t, publishedBy(t, session, event))
	if content["type"] != "text" || content["body"] != "bom dia" {
		t.Fatalf("a message that carried a group key was published as %v", content)
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
