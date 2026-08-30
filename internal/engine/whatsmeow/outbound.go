package whatsmeow

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// uploadTimeout bounds the time one outbound message spends fetching its file and
// handing it to WhatsApp.
//
// A send answers a command, so what this competes with is the caller's own deadline
// rather than the ordering of a session's inbound messages. The contract's commands carry
// one and the session applies it, so this is only ever the ceiling for a send that
// arrived without one: it stops a file coming from an address that accepts the connection
// and then says nothing from holding a session's command queue open indefinitely.
//
// In practice the caller's deadline is much shorter than this and it is what decides
// whether a large file can be sent at all. See #29: the supported client allows 18
// seconds, which no file near WAC_MEDIA_SEND_MAX will fetch and upload inside.
const uploadTimeout = 2 * time.Minute

// fetchRedirects is how many hops a fetch of the caller's URL will follow. Redirects are
// ordinary on a storage service that signs its URLs; a chain longer than this is a loop
// or a misconfiguration, and following it costs a request per hop.
const fetchRedirects = 5

// errTooManyRedirects is a chain that did not end. Deterministic, so it is the caller's
// reference to fix rather than something to try again.
var errTooManyRedirects = errors.New("whatsmeow: the address of the file to send redirects without ending")

// vcardVersion is the vCard the connector writes when the caller gave it a number and a
// name rather than a card. 3.0 because that is what WhatsApp's own clients send and what
// every one of them parses back.
const vcardVersion = "3.0"

// The four bodies `message.send` can carry, one struct each.
//
// One struct each rather than one wide one, and the difference is not tidiness. Every arm
// of the contract's `content` says `additionalProperties: true`, and AGENTS.md promises
// that a client can add an optional field without anything else having to know. Decoded
// into a single struct covering all four, a field one arm adds under a name another arm
// already uses at a different type fails the whole command -- and it fails it for
// messages of the type that never carried the field at all.
type (
	textContent struct {
		Body string `json:"body"`
	}

	mediaContent struct {
		Kind      protocol.MediaKind `json:"kind"`
		Mime      string             `json:"mime"`
		Filename  string             `json:"filename"`
		Caption   string             `json:"caption"`
		VoiceNote bool               `json:"voice_note"`
		Size      int64              `json:"size"`
		Duration  uint32             `json:"duration"`
		Thumbnail string             `json:"thumbnail"`
		Ref       *protocol.MediaRef `json:"ref"`
	}

	locationContent struct {
		// Pointers because the contract requires both and a missing one decodes to zero,
		// which is a real place: a body that names neither would go out as a pin in the
		// Gulf of Guinea and report success.
		Latitude  *float64 `json:"latitude"`
		Longitude *float64 `json:"longitude"`
		Name      string   `json:"name"`
		Address   string   `json:"address"`
		Live      bool     `json:"live"`
	}

	contactsContent struct {
		Contacts []outboundContact `json:"contacts"`
	}
)

// outboundContact is one card in a `contacts` body.
type outboundContact struct {
	DisplayName string `json:"display_name"`
	Phone       string `json:"phone"`
	Vcard       string `json:"vcard"`
}

// mediaPlan is a media body that has passed everything the payload alone can be judged
// on, and still has to be fetched and uploaded.
type mediaPlan struct {
	content mediaContent
	// address is where the bytes are, already checked as one this connector fetches
	// over.
	address string
	// thumbnail is the caller's inline preview, decoded.
	thumbnail []byte
}

// bodyType is the discriminator, read on its own before anything else is decoded.
type bodyType struct {
	Type string `json:"type"`
}

// planBody works out what the caller asked to send, without touching the network.
//
// It answers a built message for every body that needs nothing fetched, and a plan for
// the one that does. The split is not tidiness: it is the order the two halves have to
// run in. A payload this connector can never send is the caller's bug whatever the
// socket is doing, and answering `not_connected` to one sends it away to wait for a
// connection that would not have helped. Fetching a file and uploading it, on the other
// hand, is work worth spending only on a session that can actually send, so it waits
// until the session is known to be up.
//
// The type is read first and only its own arm is decoded after it, so a field one arm
// adds cannot fail a message of another type. The context that rides along is passed in
// rather than built here because it is the same for every body: a quote, a mention and
// the chat's disappearing-message timer belong to the message, not to what is in it.
func planBody(req *sendRequest, alongside *waE2E.ContextInfo, limit int64) (*waE2E.Message, *mediaPlan, error) {
	var body bodyType
	if err := json.Unmarshal(req.Content, &body); err != nil {
		return nil, nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a send has to say what kind of body it is sending")
	}

	switch body.Type {
	case "text":
		content, err := decodeBody[textContent](req.Content, body.Type)
		if err != nil {
			return nil, nil, err
		}
		message, err := textToSend(&content, alongside)
		return message, nil, err
	case "location":
		content, err := decodeBody[locationContent](req.Content, body.Type)
		if err != nil {
			return nil, nil, err
		}
		message, err := locationToSend(&content, alongside)
		return message, nil, err
	case "contacts":
		content, err := decodeBody[contactsContent](req.Content, body.Type)
		if err != nil {
			return nil, nil, err
		}
		message, err := contactsToSend(&content, alongside)
		return message, nil, err
	case "media":
		if req.To.Kind == protocol.AddressBroadcast {
			// whatsmeow serves a broadcast list from getBroadcastListParticipants, which
			// answers ErrBroadcastListUnsupported for everything but status@broadcast.
			// The send is therefore refused whatever it carries, and sendFailure already
			// says so -- but for a file it says so after the fetch and the upload, which
			// is up to WAC_MEDIA_SEND_MAX moved for a message that could never go out,
			// and an upload on WhatsApp nothing will ever refer to. The same answer,
			// before the transfer.
			return nil, nil, protocol.NewError(protocol.ErrorUnsupported,
				"this connector cannot send to a broadcast list yet")
		}
		if req.To.Kind == protocol.AddressNewsletter {
			// A newsletter takes its media unencrypted, through a different upload call,
			// and the message has to carry the handle that upload answers with. Sent the
			// ordinary way it goes out with coordinates nobody can resolve, and the send
			// reports success: the caller has no reason to try again and the followers
			// see a broken attachment. Refused until #28 does it properly, because
			// nothing here can exercise the newsletter path.
			return nil, nil, protocol.NewError(protocol.ErrorUnsupported,
				"this connector cannot send a file to a newsletter yet")
		}
		content, err := decodeBody[mediaContent](req.Content, body.Type)
		if err != nil {
			return nil, nil, err
		}
		plan, err := planMedia(&content, limit)
		return nil, plan, err
	default:
		// Refused rather than sent as something else. A caller told its message went out
		// has no reason to send it again, so a body quietly delivered as its caption is
		// one nobody finds out about.
		return nil, nil, protocol.NewError(protocol.ErrorUnsupported,
			fmt.Sprintf("this connector cannot send %q yet", body.Type))
	}
}

// decodeBody reads one arm of the content, named so the refusal says which one.
func decodeBody[T any](raw json.RawMessage, kind string) (T, error) {
	var body T
	if err := json.Unmarshal(raw, &body); err != nil {
		return body, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("that is not a %s body: %v", kind, err))
	}
	return body, nil
}

// locationToSend renders a pin on a map.
func locationToSend(content *locationContent, alongside *waE2E.ContextInfo) (*waE2E.Message, error) {
	if content.Latitude == nil || content.Longitude == nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a location has to name both of its coordinates")
	}
	latitude, longitude := *content.Latitude, *content.Longitude
	if latitude < -90 || latitude > 90 {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%v is not a latitude", latitude))
	}
	if longitude < -180 || longitude > 180 {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%v is not a longitude", longitude))
	}
	if content.Live {
		// A live location is a message WhatsApp expects to be updated for as long as it
		// is live, and nothing in this connector updates one: sent as a static pin it
		// would say it is sharing until it silently expires, which is worse than a
		// refusal the caller can act on. It becomes possible when there is a command to
		// update one.
		return nil, protocol.NewError(protocol.ErrorUnsupported,
			"this connector cannot send a live location: nothing here would keep it moving")
	}

	location := &waE2E.LocationMessage{
		DegreesLatitude:  proto.Float64(latitude),
		DegreesLongitude: proto.Float64(longitude),
		ContextInfo:      alongside,
	}
	if content.Name != "" {
		location.Name = proto.String(content.Name)
	}
	if content.Address != "" {
		location.Address = proto.String(content.Address)
	}
	return &waE2E.Message{LocationMessage: location}, nil
}

// contactsToSend renders one card or a stack of them.
//
// WhatsApp has two shapes and they are not interchangeable: one card goes as a
// ContactMessage and several go as a ContactsArrayMessage. A single card sent as an
// array renders as a list of one on some clients and as nothing on others.
func contactsToSend(content *contactsContent, alongside *waE2E.ContextInfo) (*waE2E.Message, error) {
	if len(content.Contacts) == 0 {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "a contacts message with no contacts is not a message")
	}

	cards := make([]*waE2E.ContactMessage, 0, len(content.Contacts))
	for i := range content.Contacts {
		card, err := contactCard(&content.Contacts[i])
		if err != nil {
			return nil, err
		}
		cards = append(cards, card)
	}
	if len(cards) == 1 {
		cards[0].ContextInfo = alongside
		return &waE2E.Message{ContactMessage: cards[0]}, nil
	}
	return &waE2E.Message{ContactsArrayMessage: &waE2E.ContactsArrayMessage{
		DisplayName: proto.String(stackLabel(cards)),
		Contacts:    cards,
		ContextInfo: alongside,
	}}, nil
}

// namesInLabel is how many of a stack's cards are spelled out in its label. Enough to
// recognise the stack, short enough that fifty cards do not produce a label of fifty
// names.
const namesInLabel = 3

// stackLabel is what a stack of cards is called in the recipient's chat.
//
// WhatsApp reads this as the label for the whole stack, and a stack sent without one
// arrives blank on the clients that show it. The contract has no aggregate label -- it
// carries the cards and nothing about them together -- so it is built from the names.
//
// Built from the names rather than from a phrase, and that is deliberate: this string is
// read by the recipient, in whatever language they use, and this connector has no idea
// what that is. "Ana, Bruno" reads the same everywhere; "Ana and 2 others" would be
// English in somebody's Portuguese chat. The overflow is a count for the same reason.
func stackLabel(cards []*waE2E.ContactMessage) string {
	names := make([]string, 0, namesInLabel)
	for _, card := range cards[:min(len(cards), namesInLabel)] {
		names = append(names, card.GetDisplayName())
	}
	label := strings.Join(names, ", ")
	if rest := len(cards) - len(names); rest > 0 {
		label += fmt.Sprintf(" +%d", rest)
	}
	return label
}

// contactCard turns one entry into the card WhatsApp carries.
//
// A caller that has a vCard sends it as it is: it may hold several numbers, an email, a
// company, none of which the contract's three fields can express, and rewriting it here
// would lose whatever is not in them. A caller that has a number and a name gets a card
// written for it, because that is the whole of what it has and refusing would make the
// simple case the hard one.
func contactCard(entry *outboundContact) (*waE2E.ContactMessage, error) {
	name := strings.TrimSpace(entry.DisplayName)
	if entry.Vcard != "" {
		if name == "" {
			// WhatsApp shows this string, not the card: a card with no display name
			// renders as an empty row in the recipient's chat.
			name = vcardName(entry.Vcard)
		}
		if name == "" {
			return nil, protocol.NewError(protocol.ErrorInvalidPayload,
				"a contact card has to have a name, on the card or beside it")
		}
		return &waE2E.ContactMessage{
			DisplayName: proto.String(name),
			Vcard:       proto.String(entry.Vcard),
		}, nil
	}

	// Reduced to digits rather than trimmed: a number reaches this connector however a
	// human typed it into a CRM, and the contract's own fixture carries
	// `+55 41 98888-1111`. The waid parameter is what makes the card open a chat, and
	// WhatsApp matches it against an account by its digits: one carrying spaces and
	// hyphens matches nobody, so the card renders and does nothing when it is tapped.
	phone := digitsOf(entry.Phone)
	if phone == "" || name == "" {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a contact has to carry a vcard, or a name and a phone number to write one from")
	}
	return &waE2E.ContactMessage{
		DisplayName: proto.String(name),
		Vcard:       proto.String(vcardOf(name, phone)),
	}, nil
}

// vcardOf writes the card for a name and a number.
//
// The WAID parameter is what makes the card actionable: without it a recipient sees a
// number to copy, and with it the card opens the chat. Written the way WhatsApp's own
// clients write it, because the recipient's client is what parses this back.
func vcardOf(name, digits string) string {
	return strings.Join([]string{
		"BEGIN:VCARD",
		"VERSION:" + vcardVersion,
		// N is positional -- family;given;middle;prefix;suffix -- so a semicolon inside
		// the name would add a field rather than be part of one. Dropped rather than
		// escaped, for the reason on vcardText: nothing reading this undoes an escape,
		// so escaping it puts a backslash in the name instead. FN below carries the name
		// as it was written, and FN is what is displayed.
		"N:;" + strings.ReplaceAll(vcardText(name), ";", " ") + ";;;",
		"FN:" + vcardText(name),
		"TEL;type=CELL;type=VOICE;waid=" + digits + ":+" + digits,
		"END:VCARD",
	}, "\n") + "\n"
}

// vcardText is a value written the way WhatsApp writes one, which is the only way worth
// writing it: verbatim, with only what would break a line taken out.
//
// RFC 2426 says a semicolon in a text value must be escaped with a backslash, and doing
// that is wrong here. The RFC is not what reads this back -- the recipient's WhatsApp is,
// and it does not undo the escape: a card written `FN:Souza\; Ana` is shown as
// `Souza\; Ana`, backslash and all. The contract's own fixture of a card WhatsApp sent
// escapes nothing either (`N:Dias;Carlos;;;`, `FN:Carlos Dias`).
//
// A newline is the one thing that cannot go through: a value carrying one ends the
// property early and everything after it is read as another line of the card.
func vcardText(value string) string {
	return strings.NewReplacer("\r", "", "\n", " ").Replace(value)
}

// vcardUnescape is the inverse of vcardEscape, for reading a value back off a card
// somebody else wrote.
//
// Written as a scan rather than as a chain of replacements: unescaping `\;` and then
// `\\` turns a literal backslash followed by a semicolon into a semicolon, which is a
// different name from the one on the card.
func vcardUnescape(value string) string {
	var out strings.Builder
	out.Grow(len(value))
	for i := 0; i < len(value); i++ {
		if value[i] != '\\' || i+1 >= len(value) {
			out.WriteByte(value[i])
			continue
		}
		i++
		switch value[i] {
		case 'n', 'N':
			out.WriteByte('\n')
		default:
			// `\;`, `\,`, `\\` and anything else escaped: the character itself.
			out.WriteByte(value[i])
		}
	}
	return out.String()
}

// vcardName reads FN off a card, for a caller that sent one without saying what to
// label it. Empty when the card has no FN, which is a card this connector will not send.
func vcardName(card string) string { return vcardValue(card, "FN") }

// vcardValue reads one property off a card, with its parameters and its escapes taken
// off. Empty when the card does not carry it.
//
// The property may carry parameters (`FN;CHARSET=UTF-8:...`), so the match is on the
// name up to the first delimiter rather than on a prefix with a colon. And the value is
// unescaped, for a card this connector did not write: a client that follows RFC 2426
// escapes a semicolon, and copying that across verbatim would show the backslash. Cards
// written here carry no escapes at all -- see vcardText -- so this does nothing to them.
func vcardValue(card, want string) string {
	value, _ := vcardLine(card, want)
	return value
}

// vcardLine is vcardValue plus the parameters the property carried, for the one caller
// that needs them: WhatsApp puts the account behind a card in a parameter on TEL rather
// than in any value.
func vcardLine(card, want string) (value, parameters string) {
	for line := range strings.SplitSeq(vcardUnfold(card), "\n") {
		trimmed := line
		name, raw, found := strings.Cut(trimmed, ":")
		if !found {
			continue
		}
		property, params, _ := strings.Cut(name, ";")
		// A property may be written in a group -- `item1.TEL;waid=...:+...` -- which is
		// how several address books export a card, and the group is not part of the
		// property's name. A name never carries a dot otherwise, so this takes nothing
		// off a card that was not grouped.
		if _, ungrouped, grouped := strings.Cut(property, "."); grouped {
			property = ungrouped
		}
		if strings.EqualFold(property, want) {
			return vcardUnescape(strings.TrimSpace(raw)), params
		}
	}
	return "", ""
}

// vcardUnfold joins the physical lines a card was wrapped across back into the logical
// ones it was written as, for reading only: what is published stays as it arrived.
//
// RFC 2426 breaks a property longer than 75 octets and starts the next line with a single
// space or tab that is not part of the value, and an address book exporting a card does
// this routinely -- a TEL with a few parameters is past 75 before the number begins.
// Reading the physical lines gives a name cut in half, and when the break lands before
// the colon it gives no property at all, so the card publishes an empty number.
func vcardUnfold(card string) string {
	lines := strings.Split(card, "\n")
	unfolded := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimRight(line, "\r")
		if len(unfolded) > 0 && trimmed != "" && (trimmed[0] == ' ' || trimmed[0] == '\t') {
			// The one whitespace that did the folding, and only that one: a value may
			// well begin with a space of its own on the line after it.
			unfolded[len(unfolded)-1] += trimmed[1:]
			continue
		}
		unfolded = append(unfolded, trimmed)
	}
	return strings.Join(unfolded, "\n")
}

// source is the bytes of an outbound file, as the address the caller named serves them.
type source struct {
	body io.ReadCloser
	// size is what the server said, and -1 when it said nothing. It is a claim like the
	// sender's own is on the inbound side: acted on when it is already past the cap, and
	// never trusted in place of counting.
	size int64
	mime string
}

// mediaTypes maps the contract's five kinds onto the key WhatsApp derives its upload
// encryption from. Getting this wrong does not fail the upload: it produces a file the
// recipient cannot decrypt.
var mediaTypes = map[protocol.MediaKind]wm.MediaType{
	protocol.MediaImage:    wm.MediaImage,
	protocol.MediaVideo:    wm.MediaVideo,
	protocol.MediaAudio:    wm.MediaAudio,
	protocol.MediaDocument: wm.MediaDocument,
	// WhatsApp has no sticker upload key of its own: a sticker is uploaded as an image
	// and carried as a StickerMessage.
	protocol.MediaSticker: wm.MediaImage,
}

// planMedia judges a media body on the payload alone: what kind of file it is, whether
// there is an address to fetch it from, whether the caller already says it is too big,
// and whether the preview it carries is one that travels.
func planMedia(content *mediaContent, limit int64) (*mediaPlan, error) {
	if _, known := mediaTypes[content.Kind]; !known {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is not a kind of file this contract carries", content.Kind))
	}
	if content.Caption != "" && !captions[content.Kind] {
		// Dropped silently, a caption the caller wrote never reaches anybody and the send
		// reports success. WhatsApp has nowhere to put one on either of these: neither
		// StickerMessage nor AudioMessage has the field at all.
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("a %s carries no caption: send the text as its own message", content.Kind))
	}
	address, err := fetchable(content.Ref)
	if err != nil {
		return nil, err
	}
	// Both of the caller's own numbers: the contract carries a size on the content and
	// another on the reference, and a client that fills in only one of them is filling in
	// the one it has. Refused before the transfer, the same way an inbound file is
	// refused on the sender's claim.
	for _, declared := range [...]int64{content.Size, content.Ref.Size} {
		if declared < 0 {
			// Not the same as absent, which is what every check below would read it as:
			// the contract says `minimum: 0` and nothing validates a command against the
			// schema at runtime, so a negative here silently turns off the comparison
			// that catches a file arriving short.
			return nil, protocol.NewError(protocol.ErrorInvalidPayload,
				fmt.Sprintf("%d is not a size a file can have", declared))
		}
		if declared > 0 && declared > limit {
			return nil, protocol.NewError(protocol.ErrorMediaTooLarge,
				fmt.Sprintf("the caller says %d bytes and this instance sends at most %d", declared, limit))
		}
	}
	if content.Size > 0 && content.Ref.Size > 0 && content.Size != content.Ref.Size {
		// No file satisfies both, so this is a payload that can only ever fail. Left to
		// be found out where the two are compared, it fails after the whole transfer,
		// answers something retryable, and every retry repeats an impossible upload.
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("the message says the file is %d bytes and its reference says %d",
				content.Size, content.Ref.Size))
	}
	if err := sendableHeaders(content.Ref.Headers); err != nil {
		return nil, err
	}
	if err := matchable(content.Ref.SHA256); err != nil {
		return nil, err
	}
	thumbnail, err := thumbnailBytes(content.Thumbnail, content.Kind)
	if err != nil {
		return nil, err
	}
	return &mediaPlan{content: *content, address: address, thumbnail: thumbnail}, nil
}

// captions are the kinds WhatsApp gives somewhere to put one. An audio and a sticker have
// no such field, so a caption on either is text that would go nowhere.
var captions = map[protocol.MediaKind]bool{
	protocol.MediaImage:    true,
	protocol.MediaVideo:    true,
	protocol.MediaDocument: true,
}

// mediaToSend fetches the file the plan names and hands it to WhatsApp.
func (s *Session) mediaToSend(
	ctx context.Context, plan *mediaPlan, alongside *waE2E.ContextInfo,
) (*waE2E.Message, error) {
	content := &plan.content
	file, err := s.retrieve(ctx, plan.address, content.Ref.Headers)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.body.Close() }()

	if file.size > s.sendLimit {
		return nil, protocol.NewError(protocol.ErrorMediaTooLarge,
			fmt.Sprintf("that file is %d bytes and this instance sends at most %d", file.size, s.sendLimit))
	}

	// Counted rather than trusted: a server that understates Content-Length, or sends
	// none at all, would otherwise stream past the cap into a temporary file.
	capped := &counting{from: file.body, limit: s.sendLimit}
	// Buffered so the first bytes can be looked at without being taken out of the stream:
	// whether a sticker animates is in its header and nowhere in the contract.
	peeking := bufio.NewReaderSize(capped, webpHeader)
	animated := animatedWebP(peeking)

	uploaded, err := s.upload(ctx, mediaTypes[content.Kind], peeking)
	switch {
	case errors.Is(err, errTooLarge):
		return nil, protocol.NewError(protocol.ErrorMediaTooLarge,
			fmt.Sprintf("that file is larger than the %d bytes this instance sends", s.sendLimit))
	case capped.failed != nil:
		// Asked before the upload's own error, because it is the cause of it when there
		// is one: the file stopped arriving, and WhatsApp never got the chance to refuse
		// anything. Reported the other way round, an operator goes looking at WhatsApp.
		return nil, sourceStopped(capped.failed)
	case err != nil:
		return nil, err
	}

	// And the half whatsmeow hides: it pads and encrypts whatever it managed to read, so
	// a body that ended early is a perfectly valid upload of the wrong file and there is
	// nowhere earlier than here to notice.
	// Both claims, because they are made by different parties about the same bytes: the
	// caller says what it stored and the address says what it is sending. A stale blob
	// served with a Content-Length that agrees with itself passes the second and fails
	// the first.
	if err := allOfIt(file.size, &uploaded); err != nil {
		return nil, err
	}
	if err := theSameSize(content.Size, &uploaded, "the file to send is"); err != nil {
		return nil, err
	}
	if err := theSameSize(content.Ref.Size, &uploaded, "the reference names a file of"); err != nil {
		return nil, err
	}
	if err := theSameFile(content.Ref.SHA256, &uploaded); err != nil {
		return nil, err
	}

	return renderMedia(content, &uploaded, mimeToSend(content, file.mime), plan.thumbnail, animated, alongside), nil
}

// webpHeader is how far into a file the animation flag can be: `RIFF`, the size, `WEBP`,
// the `VP8X` chunk header and its size, and then the flags byte.
const webpHeader = 21

// animatedWebP reports whether the bytes about to be uploaded are an animated sticker.
//
// Read off the file because there is nowhere else to read it from: the contract carries
// one sticker kind on purpose ("an animated sticker is still a sticker"), so a caller
// forwarding one it received has no field to say so in. Left unset, WhatsApp encodes the
// message as a static sticker and the recipient sees one frame of something that was
// meant to move.
//
// A file that is not a WebP at all, or is too short to say, is not animated, and that is
// the same answer as before this existed: nothing here refuses a file over its header.
func animatedWebP(peeking *bufio.Reader) bool {
	header, err := peeking.Peek(webpHeader)
	if err != nil || len(header) < webpHeader {
		return false
	}
	if string(header[0:4]) != "RIFF" || string(header[8:12]) != "WEBP" {
		return false
	}
	// Only the extended format has flags at all; a plain `VP8 ` or `VP8L` is one frame.
	if string(header[12:16]) != "VP8X" {
		return false
	}
	const animationFlag = 0x02
	return header[20]&animationFlag != 0
}

// theSameFile refuses bytes that are not the ones the reference named.
//
// A length says how much arrived and a digest says what did. Compared only by length, a
// proxy serving a stale or misrouted blob of the same size sends somebody else's file
// under this message, and the command reports success.
//
// Final rather than retryable, and for the reason the length check is not: the same
// address answers with the same wrong bytes every time. `media_unavailable` is what the
// caller already reads when the file its reference names is not there, and what is there
// instead being a different file is the same answer.
func theSameFile(want string, uploaded *wm.UploadResponse) error {
	if want == "" {
		// The contract makes it optional, and most references travel without one.
		return nil
	}
	if got := hex.EncodeToString(uploaded.FileSHA256); !strings.EqualFold(got, want) {
		return protocol.NewError(protocol.ErrorMediaUnavailable,
			fmt.Sprintf("the reference names a file that hashes to %s and the address served one that hashes to %s",
				want, got))
	}
	return nil
}

// sourceStopped is a file that stopped arriving partway through.
//
// Retryable on purpose: the same fetch may well arrive whole next time, and the caller
// still holds the only copy of what it wanted to send. Named as the caller's own
// dependency for the same reason as every other failure of its address: what stopped
// serving is not this connector.
func sourceStopped(err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		// The deadline landed while the body was being read rather than before the
		// headers arrived. It is the same budget running out either way, and the caller
		// is about to stop waiting; answered as anything else, the two halves of one
		// deadline report themselves differently.
		return protocol.NewError(protocol.ErrorTimeout,
			"the file to send did not arrive before this command's deadline")
	}
	return protocol.NewError(protocol.ErrorProviderUnavailable,
		fmt.Sprintf("the file to send stopped arriving partway through: %s", whyUnreachable(err)))
}

// allOfIt refuses an upload of less than the address said in its own Content-Length it
// was sending.
//
// It catches what the reader cannot: cbcutil.EncryptStream treats io.ErrUnexpectedEOF
// exactly like io.EOF, so a body that ended early against its own Content-Length reaches
// the reader as a clean end of stream and is uploaded as a whole file. Sent as it stands,
// the recipient gets a file that is half there and the sender is told it worked.
func allOfIt(declared int64, uploaded *wm.UploadResponse) error {
	if declared <= 0 {
		// Nothing was claimed: a chunked response says nothing up front.
		return nil
	}
	if sent := int64(uploaded.FileLength); sent != declared { //nolint:gosec // a length this side counted
		// Retryable, and it is the one of the three that is: what the address said it
		// was sending and what arrived disagree, which is a transfer that stopped, and
		// the next one may well carry the whole file. The address that stopped serving
		// is the caller's own, so it is named as that rather than as this connector.
		return protocol.NewError(protocol.ErrorProviderUnavailable,
			fmt.Sprintf("the file to send was said to be %d bytes and %d arrived", declared, sent))
	}
	return nil
}

// theSameSize refuses bytes that are not the size the caller or its reference named.
//
// The same disagreement as theSameFile and answered the same way, because it is the same
// question asked with a cheaper claim: what somebody said the file is, against what the
// address served. Neither is a transfer that stopped -- the address served all it meant
// to, and the length it declared was honoured -- so neither is worth another fetch and
// another upload. Answered as a truncation it is exactly that, every time the caller
// keeps the message, and the answer never changes.
func theSameSize(declared int64, uploaded *wm.UploadResponse, whose string) error {
	if declared <= 0 {
		// The contract makes both of these optional, and most references travel without
		// either.
		return nil
	}
	if sent := int64(uploaded.FileLength); sent != declared { //nolint:gosec // a length this side counted
		return protocol.NewError(protocol.ErrorMediaUnavailable,
			fmt.Sprintf("%s %d bytes and the address served %d", whose, declared, sent))
	}
	return nil
}

// safeAddress is where a fetch was pointed, with nothing that opens it.
//
// A reference from the client is very often a signed URL, and which part of it is the
// credential depends on who signed it: S3 and its imitators put the signature in the
// query, and Rails Active Storage puts a signed blob id in the path, where it is a
// bearer token for that file. The reply this goes into is stored by the caller and read
// back out of its logs, so anything kept here outlives the send it was minted for.
//
// So only the scheme and the host survive. That is enough for the thing an operator
// reads this to learn -- which storage was not answering -- and the caller already knows
// which file it asked for, because it wrote the reference.
func safeAddress(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		// Unparseable, or an address with no host to name. Either way there is no way to
		// be sure which part of it opens anything, so none of it is repeated.
		if scheme, _, found := strings.Cut(raw, ":"); found && scheme != "" {
			return scheme + ":[redacted]"
		}
		return "[redacted]"
	}
	return parsed.Scheme + "://" + parsed.Host
}

// whyUnreachable says what went wrong with a fetch, in words this connector chose.
//
// None of the library's own text is repeated, and that is the point rather than caution.
// net/http writes addresses into its errors in more places than are worth chasing one at
// a time: url.Error carries the request line, a Location that will not parse is quoted
// whole, and a redirect's own failure carries the hop. Every one of those can be a signed
// URL, the reply is stored by the caller and read back out of its logs, and the fourth
// time a class of bug turns up the answer is to close the door rather than to patch the
// gap. What is left is a closed vocabulary: enough to tell DNS from a refused connection
// from a certificate, and nothing that came from outside.
func whyUnreachable(err error) string {
	var dns *net.DNSError
	var cert *tls.CertificateVerificationError
	var record tls.RecordHeaderError
	var timeout net.Error

	switch {
	case errors.As(err, &dns):
		return "its host does not resolve"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "nothing is listening there"
	case errors.Is(err, syscall.ECONNRESET):
		return "it closed the connection"
	case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		return "it cannot be reached from this instance"
	case errors.As(err, &cert), errors.As(err, &record):
		return "its certificate was not accepted"
	case errors.As(err, &timeout) && timeout.Timeout():
		return "it did not answer in time"
	default:
		return "it could not be reached"
	}
}

// fetchable answers the address to fetch from, or says why there is not one.
func fetchable(ref *protocol.MediaRef) (string, error) {
	if ref == nil {
		return "", protocol.NewError(protocol.ErrorInvalidPayload,
			"a media message with no reference names no file to send")
	}
	if ref.URL == "" {
		// `uazapi_message` is the shape that gets here: a message id only the instance
		// that saw it can resolve, which this connector cannot ask anybody about. The
		// kind itself is not what is checked, because a `connector_blob` reference
		// carries a URL and a header, and forwarding one back is an ordinary thing for a
		// caller to do.
		return "", protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("a %q reference carries no address this connector can fetch from", ref.Kind))
	}
	parsed, err := url.Parse(ref.URL)
	if err != nil {
		return "", protocol.NewError(protocol.ErrorInvalidPayload,
			"that reference is not an address")
	}
	if err := overHTTP(parsed); err != nil {
		return "", protocol.NewError(protocol.ErrorInvalidPayload, err.Error())
	}
	return ref.URL, nil
}

// errNotOverHTTP is an address this connector does not fetch over. Deterministic, so it
// is the caller's reference to fix rather than something to try again -- which matters on
// a redirect, where the alternative is net/http refusing it as an error this side reports
// as retryable and the same reference is retried for as long as the caller keeps the
// message.
var errNotOverHTTP = errors.New("whatsmeow: that is not an address this connector fetches over")

// overHTTP is what makes an address one a file can be fetched from, checked on the
// caller's own URL and again on every hop it redirects to.
//
// A `file:` URL would have this instance read its own disk and send whatever is at that
// path, which is not something a message body should be able to ask for, and a redirect
// is a message body asking for it one step later.
func overHTTP(address *url.URL) error {
	if address.Scheme != "http" && address.Scheme != "https" {
		return fmt.Errorf("%w: %q", errNotOverHTTP, address.Scheme)
	}
	if address.Host == "" {
		return fmt.Errorf("%w: it names no host", errNotOverHTTP)
	}
	return nil
}

// matchable refuses a digest no file could ever hash to.
//
// Checked here rather than where it is compared, which is after the file has been fetched
// and uploaded: a digest of the wrong length can only ever fail, so leaving it until then
// spends a transfer of up to the whole send limit and leaves an upload on WhatsApp that
// nothing will ever refer to. And the answer is different -- a mismatch is the address
// serving the wrong file, while this is the caller's own payload.
func matchable(digest string) error {
	if digest == "" {
		// The contract makes it optional, and most references travel without one.
		return nil
	}
	if _, err := hex.DecodeString(digest); err != nil || len(digest) != sha256.Size*2 {
		return protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is not a SHA-256 any file could hash to", digest))
	}
	return nil
}

// sendableHeaders refuses a header the HTTP client would refuse anyway.
//
// It refuses it here instead, and that is the whole point: rejected by the client, the
// failure is indistinguishable from a server that would not answer, so it is reported as
// worth trying again and the same reference is retried for as long as the caller keeps
// the message. A header with a newline in it will never be sendable, whoever is asked.
func sendableHeaders(headers map[string]string) error {
	// Two names that differ only in case are one header on the wire: http.Header.Set
	// canonicalises both onto the same field, and which of them survives is decided by
	// the order Go happens to walk the map in, which is deliberately not the same twice.
	// A reference carrying `Authorization` and `authorization` would then send one
	// credential on one attempt and the other on the next.
	seen := make(map[string]string, len(headers))
	for name := range headers {
		folded := strings.ToLower(name)
		if folded == "host" {
			// Set on a request's header map, this one does nothing: net/http takes the
			// authority from Request.Host or from the URL and ignores the field. Accepted
			// silently, a reference that needs virtual-host addressing fetches from
			// somewhere else entirely and says it worked. Honouring it would mean
			// carrying it through the redirects as something that is not a header, which
			// is a feature nothing here has asked for; the address is where a host
			// belongs.
			return protocol.NewError(protocol.ErrorInvalidPayload,
				"Host is not a header this connector can send: put the host in the address")
		}
		if first, repeated := seen[folded]; repeated {
			return protocol.NewError(protocol.ErrorInvalidPayload,
				fmt.Sprintf("%q and %q are the same header, and which one would be sent is not decided anywhere",
					first, name))
		}
		seen[folded] = name
	}

	for name, value := range headers {
		if !isToken(name) {
			return protocol.NewError(protocol.ErrorInvalidPayload,
				fmt.Sprintf("%q is not a header name", name))
		}
		for _, char := range []byte(value) {
			// What the HTTP client itself allows in a value: printable bytes and a tab.
			// A carriage return or a newline here is what a header injection looks like.
			if char != '\t' && (char < ' ' || char == 0x7f) {
				return protocol.NewError(protocol.ErrorInvalidPayload,
					fmt.Sprintf("the value of %q carries a character a header cannot", name))
			}
		}
	}
	return nil
}

// isToken reports whether a string is a header name.
//
// The whole grammar rather than a handful of characters that are obviously wrong: what
// this exists to catch is a name the transport will reject, and the transport checks
// against this list. A check that only looked for spaces and colons would let `X(API)`
// through, and the refusal would then arrive from the transport as something to retry.
func isToken(name string) bool {
	if name == "" {
		return false
	}
	for _, char := range []byte(name) {
		switch {
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char >= '0' && char <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", char) >= 0:
		default:
			return false
		}
	}
	return true
}

// mimeToSend decides what to tell WhatsApp the file is.
//
// The caller's own word comes first: it knows what it stored, and the address it named
// may be a proxy that labels everything a stream of bytes. What the server said is the
// fallback, and the filename's extension is the last resort. Whoever it comes from, a
// type that is the kind's own said with less detail is corrected to the kind's own --
// see canonical.
func mimeToSend(content *mediaContent, served string) string {
	only := onlyTypeFor(content)
	if !unknownType(content.Mime) {
		return canonical(content.Mime, only)
	}
	// The reference's own, which is what this connector puts on one it issues: a caller
	// forwarding a file it received hands back the type the blob was stored under, and
	// ignoring it loses exactly the cases that depend on it -- a sticker is a webp and a
	// voice note an opus, and neither renders as anything else.
	if content.Ref != nil && !unknownType(content.Ref.Mime) {
		return canonical(content.Ref.Mime, only)
	}

	if !unknownType(served) {
		return canonical(served, only)
	}
	if only != "" {
		return only
	}
	if ext := filenameExt(content.Filename); ext != "" {
		if guessed := mime.TypeByExtension(ext); guessed != "" {
			return guessed
		}
	}
	// Nothing said anything a recipient could use: everything above was empty, generic,
	// or unreadable, and the last of those is not repeated -- what the server said is
	// only worth passing on while it can be read.
	return "application/octet-stream"
}

// canonical is the type to send for one somebody stated.
//
// It is what they said, except when the kind's own type is the same format with more
// detail, and then it is the kind's own. A voice note stated as `audio/ogg` is an
// `audio/ogg; codecs=opus` that lost its parameter somewhere -- in a store that keeps
// base types, in a proxy, in a header -- and WhatsApp does not render the one without
// it: it drops the message and says nothing, which is how this was found. Where the two
// name different formats the stated one is saying something, and it keeps saying it: a
// caller that calls a sticker a PNG is not to be corrected here.
func canonical(stated, only string) string {
	if base, _, err := mime.ParseMediaType(stated); err == nil && sameFormat(base, only) {
		return only
	}
	return stated
}

// unknownType reports whether a stated type says nothing about what the file is.
//
// `application/octet-stream` is what one says when it does not know, and it is that
// whoever said it: a proxy labelling every body a stream of bytes, a store that lost the
// type, a caller repeating either. Treating it as a claim is how a voice note goes out
// described as a stream of bytes, which WhatsApp drops without a word -- the same failure
// the codec parameter causes, by the other end of the same ladder.
//
// One that will not parse says nothing either, and is worse: `audio/ogg;` reaches
// WhatsApp as a type it cannot read, and passed on ahead of the kind's own it is a voice
// note that goes out without its codec by another route. Skipped rather than refused,
// because the ladder below has a real answer for it -- the kind, then the extension --
// and refusing the send would drop an attachment over a trailing semicolon.
func unknownType(stated string) bool {
	base, _, err := mime.ParseMediaType(stated)
	return stated == "" || err != nil || base == "application/octet-stream"
}

// sameFormat reports whether a type the kind guarantees describes the same format as one
// somebody else named, ignoring the parameters that are the point of preferring it.
func sameFormat(base, guaranteed string) bool {
	if guaranteed == "" {
		return false
	}
	only, _, err := mime.ParseMediaType(guaranteed)
	return err == nil && strings.EqualFold(only, base)
}

// onlyTypeFor is the type a body can be when the caller has already said what it is
// sending, and empty for the kinds that can be anything.
//
// A sticker is a webp on WhatsApp and a voice note is opus in ogg, and neither renders as
// anything else: labelled `application/octet-stream` because nothing named the file, one
// arrives as a broken sticker and the other as a note that will not play, while the send
// reports success. It does not override a type anybody stated -- a caller that says a
// sticker is a PNG is saying something, and it is not this function's to correct.
func onlyTypeFor(content *mediaContent) string {
	switch {
	case content.Kind == protocol.MediaSticker:
		return "image/webp"
	case content.Kind == protocol.MediaAudio && content.VoiceNote:
		return "audio/ogg; codecs=opus"
	default:
		return ""
	}
}

// filenameExt is the extension of a name, lowercased, or empty when there is none.
func filenameExt(filename string) string {
	dot := strings.LastIndex(filename, ".")
	if dot < 0 || dot == len(filename)-1 {
		return ""
	}
	return strings.ToLower(filename[dot:])
}

// previewFormat is the image a kind's preview field holds.
//
// WhatsApp names the field after the format, and each leaf type has exactly one: a
// sticker carries a PNG and everything else a JPEG. Which is also what this connector
// publishes on the way in, so a caller forwarding a file it received hands back the
// format the same kind expects.
func previewFormat(kind protocol.MediaKind) string {
	if kind == protocol.MediaSticker {
		return "image/png"
	}
	return "image/jpeg"
}

// thumbnailBytes decodes the inline preview the caller supplied, and answers nil when
// there is none.
//
// The format is checked rather than trusted: the bytes go into a field named for one, and
// the wrong image in it renders as a broken preview on every client that takes the field
// at its word.
func thumbnailBytes(uri string, kind protocol.MediaKind) ([]byte, error) {
	if uri == "" {
		return nil, nil
	}
	if len(uri) > thumbnailLimit {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("that preview is %d bytes and at most %d travel in a frame", len(uri), thumbnailLimit))
	}
	format := previewFormat(kind)
	prefix := "data:" + format + ";base64,"
	if !strings.HasPrefix(uri, prefix) {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("the preview of a %s has to be a base64 data: URI of a %s", kind, format))
	}
	raw, err := base64.StdEncoding.DecodeString(uri[len(prefix):])
	if err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("that preview is not base64: %v", err))
	}
	// The label is what the caller says the bytes are, and the field they go into is
	// named for a format. Where the two disagree the bytes are the ones that can be
	// checked, and a recipient's client reads the field rather than the label: PNG bytes
	// in the JPEG field render as a broken preview on every client that takes the field
	// at its word, with the send reporting success. Refused rather than filed by what
	// they turn out to be, because a caller whose preview does not match its own label is
	// a caller with a bug, and answering it is how it gets found.
	if found, _, _ := strings.Cut(http.DetectContentType(raw), ";"); found != format {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("that preview says it is a %s and its bytes are a %s", format, found))
	}
	return raw, nil
}

// renderMedia builds the leaf message for what was just uploaded.
//
// The five shapes are separate types on WhatsApp's side with separate fields, and the
// fields they do not share are the ones that matter to a recipient: a document has its
// name, a voice note its flag and its length, a sticker neither.
func renderMedia(
	content *mediaContent, uploaded *wm.UploadResponse,
	mimetype string, thumbnail []byte, animated bool, alongside *waE2E.ContextInfo,
) *waE2E.Message {
	switch content.Kind {
	case protocol.MediaImage:
		return &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
			URL: &uploaded.URL, DirectPath: &uploaded.DirectPath, MediaKey: uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256, FileSHA256: uploaded.FileSHA256,
			FileLength: proto.Uint64(uploaded.FileLength),
			Mimetype:   proto.String(mimetype),
			Caption:    optional(content.Caption), JPEGThumbnail: thumbnail,
			ContextInfo: alongside,
		}}
	case protocol.MediaVideo:
		return &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
			URL: &uploaded.URL, DirectPath: &uploaded.DirectPath, MediaKey: uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256, FileSHA256: uploaded.FileSHA256,
			FileLength: proto.Uint64(uploaded.FileLength),
			Mimetype:   proto.String(mimetype),
			Caption:    optional(content.Caption), Seconds: optionalSeconds(content.Duration),
			JPEGThumbnail: thumbnail,
			ContextInfo:   alongside,
		}}
	case protocol.MediaAudio:
		return &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
			URL: &uploaded.URL, DirectPath: &uploaded.DirectPath, MediaKey: uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256, FileSHA256: uploaded.FileSHA256,
			FileLength: proto.Uint64(uploaded.FileLength),
			Mimetype:   proto.String(mimetype),
			Seconds:    optionalSeconds(content.Duration),
			// Only ever set when true. WhatsApp reads a false the same as an absent one,
			// and every voice note this connector publishes on the way in was recognised
			// by this field being there at all.
			PTT:         optionalTrue(content.VoiceNote),
			ContextInfo: alongside,
		}}
	case protocol.MediaDocument:
		document := &waE2E.DocumentMessage{
			URL: &uploaded.URL, DirectPath: &uploaded.DirectPath, MediaKey: uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256, FileSHA256: uploaded.FileSHA256,
			FileLength: proto.Uint64(uploaded.FileLength),
			Mimetype:   proto.String(mimetype),
			FileName:   optional(content.Filename), Caption: optional(content.Caption),
			JPEGThumbnail: thumbnail,
			ContextInfo:   alongside,
		}
		if content.Caption == "" {
			return &waE2E.Message{DocumentMessage: document}
		}
		// A document sent with a caption travels inside an envelope of its own, and it is
		// the only leaf type that does. whatsmeow unwraps one on the way in -- which is
		// how this connector sees a plain DocumentMessage when a caption was sent -- and
		// never builds one on the way out, so a caption put on the bare leaf is one a
		// current client has no reason to look for.
		return &waE2E.Message{DocumentWithCaptionMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{DocumentMessage: document},
		}}
	default:
		return &waE2E.Message{StickerMessage: &waE2E.StickerMessage{
			URL: &uploaded.URL, DirectPath: &uploaded.DirectPath, MediaKey: uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256, FileSHA256: uploaded.FileSHA256,
			FileLength: proto.Uint64(uploaded.FileLength),
			Mimetype:   proto.String(mimetype),
			// The one leaf type whose preview field is a PNG, which is also the format
			// this connector publishes a sticker's preview in.
			PngThumbnail: thumbnail,
			IsAnimated:   optionalTrue(animated),
			ContextInfo:  alongside,
		}}
	}
}

// optional is a pointer to a string, and nil for the empty one. Set to an empty string
// the field is present and blank on the wire, which some clients render as an empty
// caption line under the file.
func optional(value string) *string {
	if value == "" {
		return nil
	}
	return proto.String(value)
}

// optionalSeconds is the same for a duration the caller may not know.
func optionalSeconds(seconds uint32) *uint32 {
	if seconds == 0 {
		return nil
	}
	return proto.Uint32(seconds)
}

// optionalTrue is the same for a flag whose false is not a value.
func optionalTrue(set bool) *bool {
	if !set {
		return nil
	}
	return proto.Bool(true)
}

// errTooLarge is a file that outran the cap while it was being read.
var errTooLarge = errors.New("whatsmeow: the file is past the cap for a send")

// counting reads a source and refuses to hand over more than it should.
//
// Needed because the cap has to hold against a server that lies: Content-Length is a
// claim, a chunked response carries none at all, and whatsmeow streams what it is given
// into a temporary file before it knows how big it is.
type counting struct {
	from  io.Reader
	limit int64
	read  int64
	// failed is the last error the source gave that was not a clean end of stream.
	//
	// Kept because whatsmeow throws it away: cbcutil.EncryptStream treats
	// io.ErrUnexpectedEOF exactly like io.EOF, pads what it has and encrypts it, so a
	// body that stopped halfway is uploaded as a whole file and the send reports success.
	// The recipient gets half an image and the sender is never told.
	failed error
}

func (c *counting) Read(into []byte) (int, error) {
	if c.read > c.limit {
		return 0, errTooLarge
	}
	// One byte past the cap may be read, and only one. It is what separates a file that
	// is exactly the cap from one that is larger: a reader is under no obligation to
	// report EOF alongside the last bytes rather than on the read after them, so a
	// counter that refused at the cap would refuse a file the setting says it sends.
	//
	// The probe byte is added only when there is room in an int64 for it. A cap set to
	// the largest one there is would otherwise wrap to a negative length, and slicing to
	// that is a panic that takes the whole connector down on an ordinary send.
	room := c.limit - c.read
	if room < math.MaxInt64 {
		room++
	}
	if int64(len(into)) > room {
		into = into[:room]
	}
	read, err := c.from.Read(into)
	c.read += int64(read)
	if c.read > c.limit {
		return 0, errTooLarge
	}
	if err != nil && !errors.Is(err, io.EOF) {
		c.failed = err
		return read, err //nolint:wrapcheck // the reader's own error, passed through unchanged
	}
	if err != nil {
		return read, err //nolint:wrapcheck // io.EOF, which every reader is entitled to return
	}
	return read, nil
}

// errMetadataAddress is an address no file is fetched from.
var errMetadataAddress = errors.New("whatsmeow: that address answers with credentials")

// metadataAddresses are the metadata services that answer somewhere other than the
// link-local range, each named on its own.
//
// Each sits in a range something else legitimately uses, so the range cannot go with the
// address: `fd00:ec2::/32` and `fd20:ce::/32` are unique-local, which is what a private
// IPv6 network serves from, and `100.64.0.0/10` is shared address space, which is where
// a Tailscale network lives. Refusing one address out of each costs nothing.
//
// A list of known endpoints is not a boundary, and this one is not complete by
// construction: a cloud that answers somewhere new is reachable until it is named here.
// What would be complete is an allowlist of the hosts this connector may fetch from,
// which is #31 and needs an operator to configure it.
var metadataAddresses = map[netip.Addr]struct{}{
	// AWS, on an IPv6-enabled instance.
	netip.MustParseAddr("fd00:ec2::254"): {},
	// GCP, on an IPv6-only instance.
	netip.MustParseAddr("fd20:ce::254"): {},
	// Alibaba Cloud.
	netip.MustParseAddr("100.100.100.200"): {},
}

// fetchTransport is what a caller's URL is fetched over.
//
// This is not a refusal to fetch from the private network. Fetching from the private
// network is the design -- the client sits next to this connector and hands over an
// address on their own network, which is what INTERNAL_HOST_URL exists to be -- and a
// connector that refused one would not fetch a single attachment in the deployment the
// contract describes. What it refuses is where the answer is credentials rather than a
// file: 169.254.169.254 is the instance metadata endpoint on every major cloud, and a
// fetch of it hands the host's own keys to whatever WhatsApp number the command named.
// No deployment serves media from there, so nothing is lost by never dialling it. See
// #31 for the general question, which an allowlist is the only real answer to.
//
// Checked on the address actually dialled rather than on the host in the URL, which is
// what makes it cover a name that resolves to one, a name that resolves to one only on
// the second lookup, and every redirect hop -- they all dial through this.
//
// The settings are net/http's own defaults, spelled out rather than cloned off
// DefaultTransport: the dial has to be replaced anyway, and a clone would leave what the
// rest of them are somewhere else. With one deliberate difference -- this transport does
// not proxy, where the default reads HTTP_PROXY from the environment. A proxied fetch
// dials the proxy, so the check below would be about the proxy's address while the proxy
// fetches whatever it was asked for: an environment variable that happened to be set,
// inherited from an image or exported for something else in the same container, would
// turn this off and say nothing. What is given up is a deployment that reaches its own
// client, or a presigned storage URL, only through a proxy. None exists, and adding one
// deliberately is a smaller thing than losing the guarantee by accident.
var fetchTransport = &http.Transport{
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		Control:   refuseMetadataAddress,
	}).DialContext,
	ForceAttemptHTTP2:     true,
	MaxIdleConns:          100,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: time.Second,
}

// refuseMetadataAddress is the dial fetchTransport is built on.
func refuseMetadataAddress(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}
	// A dial control is handed an address that has already been resolved, so anything
	// that will not parse as one is not something this can judge. The schemes that could
	// put a path here are refused before any dial is attempted.
	parsed, err := netip.ParseAddr(host)
	if err == nil && refusedAddress(parsed) {
		return fmt.Errorf("%w: %s", errMetadataAddress, parsed)
	}
	return nil
}

// refusedAddress reports whether an address is one a metadata service answers on.
//
// Link-local as a whole range rather than 169.254.169.254 alone: all of it is
// unroutable, nothing serves media from it, and naming the single address would leave
// the ones a cloud answers alongside it (169.254.170.2 is where ECS answers for a task
// role) reachable for nothing gained. Everywhere else, one address at a time.
func refusedAddress(address netip.Addr) bool {
	unmapped := address.Unmap()
	if unmapped.IsLinkLocalUnicast() || unmapped.IsLinkLocalMulticast() {
		return true
	}
	_, refused := metadataAddresses[unmapped]
	return refused
}

// retrieveOverHTTP fetches the caller's URL.
//
// The address comes from the client, which is the only thing this connector takes
// commands from, so this is not a stranger's URL. It is still bounded: a deadline, a
// redirect ceiling, and a refusal to read past the cap, because the failure mode of
// getting that wrong is a session's command queue held open by a server that never
// answers.
func retrieveOverHTTP(ctx context.Context, address string, headers map[string]string) (source, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, http.NoBody)
	if err != nil {
		return source{}, protocol.NewError(protocol.ErrorInvalidPayload,
			"that is not an address a file can be fetched from")
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}

	client := &http.Client{Transport: fetchTransport, CheckRedirect: followingRedirects(headers)}
	answer, err := client.Do(request)
	switch {
	case errors.Is(err, errNotOverHTTP):
		// The caller's address redirected somewhere this connector does not fetch from.
		// Left to net/http, the refusal arrives as an error this side reads as worth
		// retrying, and the same reference redirects the same way every time.
		return source{}, protocol.NewError(protocol.ErrorInvalidPayload,
			"the address of the file to send redirects somewhere this connector cannot fetch from")
	case errors.Is(err, errTooManyRedirects):
		// Deterministic: the same reference redirects the same way every time, so this is
		// the caller's address to fix and not a minute to wait out. Reported as retryable
		// it is retried for as long as the caller keeps the message.
		return source{}, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("the address of the file to send redirects more than %d times", fetchRedirects))
	case errors.Is(err, errMetadataAddress):
		// The caller's address resolved to the metadata endpoint's range. Deterministic
		// as far as this side is concerned -- and where it is not, where a name answers
		// differently on the next lookup, that is the rebinding this refuses and not a
		// minute to wait out. Reported as retryable it would be dialled again for as
		// long as the caller keeps the message.
		return source{}, protocol.NewError(protocol.ErrorInvalidPayload,
			"the address of the file to send resolves to a link-local address")
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		// Asked about first, because this one is already accounted for: the deadline is
		// the caller's own and it has run out, so the caller is about to stop waiting
		// whatever this answers. Folded into the failure below it would be reported as
		// this connector breaking, and the send would look like a bug rather than a
		// budget. See #29 for how short that budget actually is.
		return source{}, protocol.NewError(protocol.ErrorTimeout,
			"the file to send did not arrive before this command's deadline")
	case err != nil:
		// Worth trying again: the address may be a service that is restarting, and the
		// caller holds the only copy of what it wanted to send. Named as the caller's own
		// dependency rather than as this connector, because an operator reading
		// `internal` goes to connector logs that are clean while what is actually down is
		// the storage the reference points at.
		return source{}, protocol.NewError(protocol.ErrorProviderUnavailable,
			fmt.Sprintf("could not fetch the file to send from %s: %s", safeAddress(address), whyUnreachable(err)))
	}
	if answer.StatusCode != http.StatusOK {
		_ = answer.Body.Close()
		return source{}, statusFailure(answer.StatusCode)
	}

	return source{
		body: answer.Body,
		size: answer.ContentLength,
		// Kept whole, parameters and all. `audio/ogg; codecs=opus` is what a voice note
		// is, and trimmed to `audio/ogg` it reaches the recipient described as something
		// else. What the base type is needed for is the comparison in mimeToSend, which
		// parses it there rather than throwing the rest away here.
		mime: strings.TrimSpace(answer.Header.Get("Content-Type")),
	}, nil
}

// followingRedirects is the policy for a fetch of the caller's URL: how far it follows,
// and what it stops carrying on the way.
//
// A function of its own rather than a closure written inline, because what it decides is
// worth testing without a TLS server and a swapped-out process-wide transport standing
// between the test and the decision.
func followingRedirects(headers map[string]string) func(*http.Request, []*http.Request) error {
	return func(hop *http.Request, via []*http.Request) error {
		// Strictly greater: via holds the requests already made, so on the first redirect
		// it has one entry. Comparing with >= would follow one hop fewer than the
		// constant says.
		if len(via) > fetchRedirects {
			return errTooManyRedirects
		}
		if err := overHTTP(hop.URL); err != nil {
			return err
		}
		// net/http drops Authorization, Cookie and WWW-Authenticate of its own accord
		// when a redirect leaves the host, and nothing else: a reference authenticated
		// with X-API-Key or a vendor's own header would arrive at whoever the first host
		// chose to point at, carrying a working credential. The caller's headers are what
		// open its storage, so they go no further than the origin it named.
		//
		// The scheme is half of that origin. A redirect from https to http on the same
		// name is the classic downgrade: compared by host alone the destination looks
		// like the same trusted place, and the credential goes out in plaintext.
		if !sameOrigin(hop.URL, via[0].URL) {
			for name := range headers {
				hop.Header.Del(name)
			}
			// And the one net/http adds by itself. It fills Referer with the whole
			// previous URL, query and all, and the previous URL is the signed one: the
			// credential this was careful not to put in a message would arrive at the
			// redirect's destination and in its access log. Deleted here rather than
			// prevented, because the client sets it just before calling this
			// (net/http/client.go:696, then 699).
			hop.Header.Del("Referer")
		}
		return nil
	}
}

// sameOrigin reports whether a redirect stayed where the caller pointed it.
//
// Compared as origins rather than as strings, because the same origin is spelled several
// ways: a host name is case-insensitive, and a port left off means the scheme's own. A
// redirect from `storage.example` to `STORAGE.example:443` reaches the same server, and
// read as a different one it costs the caller its credentials and the fetch answers 401.
func sameOrigin(hop, first *url.URL) bool {
	return strings.EqualFold(hop.Scheme, first.Scheme) &&
		strings.EqualFold(hop.Hostname(), first.Hostname()) &&
		effectivePort(hop) == effectivePort(first)
}

// effectivePort is the port an address reaches, named or implied by its scheme.
func effectivePort(address *url.URL) string {
	if port := address.Port(); port != "" {
		return port
	}
	if strings.EqualFold(address.Scheme, "https") {
		return "443"
	}
	return "80"
}

// statusFailure splits what the caller's own server said into the answers a client acts
// on differently: a file that is not there is the caller's to fix, and a server that is
// having a bad minute is worth another go.
func statusFailure(status int) error {
	switch {
	case status == http.StatusNotFound || status == http.StatusGone:
		return protocol.NewError(protocol.ErrorMediaUnavailable,
			"the file to send is not at the address the caller gave")
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		// Named apart from the rest because the fix is different and it is a
		// configuration mistake rather than a transient one: the reference travelled
		// without the header that opens it, or with one that has since been rotated.
		return protocol.NewError(protocol.ErrorMediaUnavailable,
			"the address of the file to send refused this connector: "+strconv.Itoa(status))
	case status == http.StatusTooManyRequests:
		// The caller's own server asking to be left alone for a moment. Read as a
		// permanent mistake, a client abandons a file that is there and would have been
		// served on the next attempt.
		return protocol.NewError(protocol.ErrorRateLimited,
			"the address of the file to send is rate limiting this connector")
	case status == http.StatusRequestTimeout:
		return protocol.NewError(protocol.ErrorTimeout,
			"the address of the file to send timed out on its own request")
	case status >= 500:
		// The caller's own server having a bad minute, which is the same answer as being
		// unreachable above and for the same reason: retryable, and not this connector.
		return protocol.NewError(protocol.ErrorProviderUnavailable,
			"the address of the file to send answered "+strconv.Itoa(status))
	default:
		return protocol.NewError(protocol.ErrorInvalidPayload,
			"the address of the file to send answered "+strconv.Itoa(status))
	}
}

// uploadOverClient hands the bytes to WhatsApp.
//
// UploadReader rather than Upload: it streams through a temporary file instead of
// holding the whole thing in memory, so a fleet sending several large files at once
// costs disk rather than the heap. The temporary file is whatsmeow's to make and to
// remove.
func uploadOverClient(
	ctx context.Context, client *wm.Client, kind wm.MediaType, from io.Reader,
) (wm.UploadResponse, error) {
	uploaded, err := client.UploadReader(ctx, from, nil, kind)
	if err != nil {
		return wm.UploadResponse{}, err //nolint:wrapcheck // classified by the caller, which needs the sentinels
	}
	return uploaded, nil
}

// upload is the session's own way in, so a test can make an upload fail without a
// socket.
func (s *Session) upload(ctx context.Context, kind wm.MediaType, from io.Reader) (wm.UploadResponse, error) {
	uploaded, err := s.uploadFile(ctx, s.current(), kind, from)
	switch {
	case err == nil:
		return uploaded, nil
	case errors.Is(err, errTooLarge):
		return wm.UploadResponse{}, err
	default:
		return wm.UploadResponse{}, uploadFailure(err)
	}
}

// isPathError reports a failure that happened against this instance's own filesystem.
//
// It is what separates the two halves of an upload: whatsmeow encrypts into a temporary
// file first and only then talks to WhatsApp, and every way the first half fails --
// creating, writing to, or seeking that file -- arrives wrapped in an *fs.PathError,
// which nothing on the network side produces.
func isPathError(err error) bool {
	var path *fs.PathError
	return errors.As(err, &path)
}

// uploadFailure turns whatsmeow's answer into a code the caller can act on. An upload
// that failed sent nothing, so unlike a send there is no message in somebody's chat to
// be careful about: what matters is only whether trying again could work.
func uploadFailure(err error) error {
	switch {
	case errors.Is(err, wm.ErrNotLoggedIn):
		return protocol.NewError(protocol.ErrorNotPaired, "the session has no WhatsApp account to send from")
	case errors.Is(err, wm.ErrNotConnected):
		return protocol.NewError(protocol.ErrorNotConnected, "the session is not connected to WhatsApp")
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled),
		errors.Is(err, wm.ErrIQTimedOut):
		// Nothing was sent, so this is not the send's own timeout: the caller retries
		// under the same message id and the upload happens again, which costs a transfer
		// and duplicates nothing.
		return protocol.NewError(protocol.ErrorTimeout, "WhatsApp did not answer the upload")
	case isPathError(err):
		// The upload stages the encrypted file on disk before it sends anything, so a
		// temporary directory that is read-only or full fails here without WhatsApp
		// having been contacted at all. Reported as WhatsApp refusing the file, an
		// operator goes looking at WhatsApp for a disk of their own.
		return protocol.NewError(protocol.ErrorInternal,
			"this instance could not stage the file to send")
	default:
		return protocol.NewError(protocol.ErrorWaError, "WhatsApp would not take the file")
	}
}
