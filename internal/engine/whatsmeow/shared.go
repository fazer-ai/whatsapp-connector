package whatsmeow

import (
	"strings"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// What somebody shares rather than says: a pin on a map, or a card out of their address
// book.
//
// Both are the outbound half of #27 read backwards, and neither had an arm on the way in
// -- so a contact sending a location or a card reached nothing and WhatsApp redelivered
// it for as long as the session was up. Neither has bytes to fetch: a location is its
// coordinates and a card is its text, which is why they render here and not in media.go.

// sharedBody renders a location or a stack of contact cards, and says so when the
// message is neither.
func sharedBody(event *waEvents.Message) (body, bool) {
	message := event.Message
	switch {
	case message.GetLocationMessage() != nil:
		pin := message.GetLocationMessage()
		if pin.DegreesLatitude == nil || pin.DegreesLongitude == nil {
			// Both getters answer zero for a field that is not there, so a pin missing
			// one is published at the equator or in the Gulf of Guinea -- somewhere the
			// sender has never been, rendered as where they are. Said to be unrenderable
			// instead, which files it with everything else this build cannot read rather
			// than inventing a position for it. The outbound path refuses the same thing
			// for the same reason.
			return body{}, false
		}
		content := protocol.Location(pin.GetDegreesLatitude(), pin.GetDegreesLongitude())
		content.Name = pin.GetName()
		content.Address = pin.GetAddress()
		// This shape carries a live one too, and not only the shape below it. Read off
		// the sender rather than off which arm arrived: a moving pin published as static
		// is the one reading a client must not make, and it makes it either way.
		content.Live = pin.GetIsLive()
		return body{content: content, context: pin.GetContextInfo()}, true

	case message.GetLiveLocationMessage() != nil:
		// The pin the sender is still moving. WhatsApp updates it with further messages
		// and nothing here follows those yet, so what the client gets is where they were
		// when they started -- which is why `live` is carried rather than dropped: a
		// stale pin shown as current is the one reading a client must not make.
		//
		// A live location has a caption where a static one has a place name. It goes in
		// `name` because that is the only text the contract has for a pin, and because a
		// client renders the two together.
		moving := message.GetLiveLocationMessage()
		if moving.DegreesLatitude == nil || moving.DegreesLongitude == nil {
			return body{}, false
		}
		content := protocol.Location(moving.GetDegreesLatitude(), moving.GetDegreesLongitude())
		content.Name = moving.GetCaption()
		content.Live = true
		return body{content: content, context: moving.GetContextInfo()}, true

	case message.GetContactMessage() != nil:
		// One card. WhatsApp has two shapes for a share and they are not interchangeable
		// -- see contactsToSend, which builds the same two on the way out.
		card := message.GetContactMessage()
		return body{
			content: protocol.Contacts([]protocol.Contact{cardShared(card)}),
			context: card.GetContextInfo(),
		}, true

	case message.GetContactsArrayMessage() != nil:
		stack := message.GetContactsArrayMessage()
		cards := make([]protocol.Contact, 0, len(stack.GetContacts()))
		for _, card := range stack.GetContacts() {
			cards = append(cards, cardShared(card))
		}
		if len(cards) == 0 {
			// A stack with nothing in it is not a share. Said here rather than published
			// as an empty list, so it falls through to the placeholder with everything
			// else that arrived carrying nothing.
			return body{}, false
		}
		return body{content: protocol.Contacts(cards), context: stack.GetContextInfo()}, true

	default:
		return body{}, false
	}
}

// cardShared renders one card.
//
// The vCard travels verbatim and the other two fields are read out of it. That is not
// redundancy: a card may carry several numbers, an email and a company, none of which
// the contract's three fields can express, so a client storing the contact wants the
// whole card while one rendering a row wants a name and a number. Reading them here
// rather than leaving it to each client is what keeps two clients from disagreeing about
// what a card is called.
func cardShared(card *waE2E.ContactMessage) protocol.Contact {
	vcard := card.GetVcard()
	shared := protocol.Contact{DisplayName: card.GetDisplayName(), Vcard: vcard}
	if shared.DisplayName == "" {
		// WhatsApp shows the display name and not the card, so a share that arrives
		// without one is a row with no label unless the card is read.
		shared.DisplayName = vcardName(vcard)
	}
	shared.Phone = vcardPhone(vcard)
	return shared
}

// vcardPhone is the number on a card, as the sender's client wrote it.
//
// WhatsApp's own TEL line is `TEL;type=CELL;type=VOICE;waid=<digits>:+<formatted>`, and
// the two halves are not the same string: the value is the number a human reads and the
// waid parameter is the account it belongs to. The value is preferred because that is
// what a client renders, and the parameter is the fallback because a card written by
// something else may have a TEL with no value this connector can make sense of.
func vcardPhone(card string) string {
	value, parameters := vcardLine(card, "TEL")
	if value != "" {
		return value
	}
	for parameter := range strings.SplitSeq(parameters, ";") {
		if name, digits, found := strings.Cut(parameter, "="); found && strings.EqualFold(name, "waid") {
			if digits = digitsOf(digits); digits != "" {
				return "+" + digits
			}
		}
	}
	return ""
}
