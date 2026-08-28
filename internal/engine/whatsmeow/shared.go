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
		if !aPlace(pin.DegreesLatitude, pin.DegreesLongitude) {
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
		// The pin the sender is still moving, at the moment they started. What comes
		// after is a stream of positions, and a stream published a message at a time is
		// a bubble every few seconds for as long as the share lasts -- so only the start
		// of it is a message here, and the rest is dropped by changeOf.
		//
		// `live` is carried rather than the pin being dropped, because a stale position
		// shown as current is the one reading a client must not make.
		//
		// A live location has a caption where a static one has a place name. It goes in
		// `name` because that is the only text the contract has for a pin, and because a
		// client renders the two together.
		moving := message.GetLiveLocationMessage()
		if !aPlace(moving.DegreesLatitude, moving.DegreesLongitude) {
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

// aPlace reports whether a pair of coordinates names somewhere on Earth.
//
// Three ways they do not, and the range check answers all three because every comparison
// against a value that is not a number is false. A missing coordinate reads as zero and
// puts the pin in the Gulf of Guinea. One out of range is a pin nobody can draw. And one
// that is not a number at all is the worst of them: JSON has no way to write it, so the
// event would fail to render, the acknowledgement would be withheld, and the message
// would redeliver for as long as the session is up -- a loop inside the change that
// exists to end loops.
func aPlace(latitude, longitude *float64) bool {
	if latitude == nil || longitude == nil {
		return false
	}
	return *latitude >= -90 && *latitude <= 90 && *longitude >= -180 && *longitude <= 180
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
	vcard := vcardWithoutFiles(card.GetVcard())
	shared := protocol.Contact{DisplayName: card.GetDisplayName(), Vcard: vcard}
	if shared.DisplayName == "" {
		// WhatsApp shows the display name and not the card, so a share that arrives
		// without one is a row with no label unless the card is read.
		shared.DisplayName = vcardName(vcard)
	}
	shared.Phone = vcardPhone(vcard)
	return shared
}

// vcardFiles are the properties whose value can be a file rather than a string. A card
// out of an address book carries a contact's picture in PHOTO, and a card built by hand
// can carry a company mark, a pronunciation clip or a certificate the same way.
var vcardFiles = map[string]struct{}{"PHOTO": {}, "LOGO": {}, "SOUND": {}, "KEY": {}}

// vcardWithoutFiles is the card with those properties taken out.
//
// Media never travels inside a frame -- it goes by reference, fetched from this
// connector against a token -- and a card is the one place it could arrive already
// inside one: a PHOTO is base64 in the card's own text, so publishing the card verbatim
// would put a file on the wire in a field nothing can fetch and nothing bounds. It is
// also the only field on any event whose size is the sender's to choose.
//
// The whole property goes, not only the inline form. A PHOTO written as a URI is small
// enough to carry, and it is an address on somebody else's server that a client rendering
// the card would dial for a picture, which is a fetch this connector would be handing out
// rather than making. One rule covers both, and what the contract exists to carry -- the
// name and the number -- is not in any of these properties.
func vcardWithoutFiles(card string) string {
	if card == "" {
		return card
	}
	kept := strings.Builder{}
	kept.Grow(len(card))
	dropping := false
	for line := range strings.SplitSeq(card, "\n") {
		if property, names := vcardProperty(line); names {
			_, dropping = vcardFiles[strings.ToUpper(property)]
		}
		// A line that names no property continues the one above it, and a file's base64
		// is written across many of them. Which way the sender's client wrapped them --
		// folded under a space the way RFC 2426 says, or left flush the way vCard 2.1
		// blocks are -- does not matter here: neither can be read as a property, so both
		// belong to whatever was dropped or kept last.
		if dropping {
			continue
		}
		kept.WriteString(line)
		kept.WriteString("\n")
	}
	return strings.TrimSuffix(kept.String(), "\n")
}

// vcardProperty is the name a line declares, and false for a line that declares none.
//
// Base64 is what this has to tell a property from, and it cannot be one: the alphabet has
// no colon in it, so a line of it never parses as `NAME:value`.
func vcardProperty(line string) (string, bool) {
	trimmed := strings.TrimRight(line, "\r")
	if trimmed == "" || trimmed[0] == ' ' || trimmed[0] == '\t' {
		return "", false
	}
	name, _, found := strings.Cut(trimmed, ":")
	if !found {
		return "", false
	}
	property, _, _ := strings.Cut(name, ";")
	if _, ungrouped, grouped := strings.Cut(property, "."); grouped {
		property = ungrouped
	}
	if property == "" || strings.ContainsAny(property, " \t") {
		return "", false
	}
	return property, true
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
