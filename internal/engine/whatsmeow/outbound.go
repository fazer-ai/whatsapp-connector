package whatsmeow

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
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
// rather than the ordering of a session's inbound messages. The contract's commands
// carry one and it is usually shorter than this; what this is for is the send that
// arrived without one, so a file coming from an address that accepts the connection and
// then says nothing cannot hold a session's command queue open indefinitely.
const uploadTimeout = 2 * time.Minute

// fetchRedirects is how many hops a fetch of the caller's URL will follow. Redirects are
// ordinary on a storage service that signs its URLs; a chain longer than this is a loop
// or a misconfiguration, and following it costs a request per hop.
const fetchRedirects = 5

// vcardVersion is the vCard the connector writes when the caller gave it a number and a
// name rather than a card. 3.0 because that is what WhatsApp's own clients send and what
// every one of them parses back.
const vcardVersion = "3.0"

// outboundContent is `message.send`'s content, wide enough for every body this connector
// can send. It is one struct rather than a union because JSON gives no discriminated
// union and the type field is the discriminator: what each body actually requires is
// checked by the function that renders it, where the error can say what is missing.
type outboundContent struct {
	Type string `json:"type"`

	// Text.
	Body string `json:"body"`

	// Media.
	Kind      protocol.MediaKind `json:"kind"`
	Mime      string             `json:"mime"`
	Filename  string             `json:"filename"`
	Caption   string             `json:"caption"`
	VoiceNote bool               `json:"voice_note"`
	Size      int64              `json:"size"`
	Duration  uint32             `json:"duration"`
	Thumbnail string             `json:"thumbnail"`
	Ref       *protocol.MediaRef `json:"ref"`

	// Location. Pointers because the contract requires both and a missing one decodes to
	// zero, which is a real place: a body that names neither would go out as a pin in the
	// Gulf of Guinea and report success.
	Latitude  *float64 `json:"latitude"`
	Longitude *float64 `json:"longitude"`
	Name      string   `json:"name"`
	Address   string   `json:"address"`
	Live      bool     `json:"live"`

	// Contacts.
	Contacts []outboundContact `json:"contacts"`
}

// outboundContact is one card in a `contacts` body.
type outboundContact struct {
	DisplayName string `json:"display_name"`
	Phone       string `json:"phone"`
	Vcard       string `json:"vcard"`
}

// mediaPlan is a media body that has passed everything the payload alone can be judged
// on, and still has to be fetched and uploaded.
type mediaPlan struct {
	// address is where the bytes are, already checked as one this connector fetches
	// over.
	address string
	// thumbnail is the caller's inline preview, decoded.
	thumbnail []byte
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
// The context that rides along is passed in rather than built here because it is the
// same for every body: a quote, a mention and the chat's disappearing-message timer
// belong to the message, not to what is in it.
func planBody(req *sendRequest, alongside *waE2E.ContextInfo, limit int64) (*waE2E.Message, *mediaPlan, error) {
	switch req.Content.Type {
	case "text":
		message, err := textToSend(req, alongside)
		return message, nil, err
	case "location":
		message, err := locationToSend(req, alongside)
		return message, nil, err
	case "contacts":
		message, err := contactsToSend(req, alongside)
		return message, nil, err
	case "media":
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
		plan, err := planMedia(&req.Content, limit)
		return nil, plan, err
	default:
		// Refused rather than sent as something else. A caller told its message went out
		// has no reason to send it again, so a body quietly delivered as its caption is
		// one nobody finds out about.
		return nil, nil, protocol.NewError(protocol.ErrorUnsupported,
			fmt.Sprintf("this connector cannot send %q yet", req.Content.Type))
	}
}

// locationToSend renders a pin on a map.
func locationToSend(req *sendRequest, alongside *waE2E.ContextInfo) (*waE2E.Message, error) {
	content := &req.Content
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
func contactsToSend(req *sendRequest, alongside *waE2E.ContextInfo) (*waE2E.Message, error) {
	if len(req.Content.Contacts) == 0 {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "a contacts message with no contacts is not a message")
	}

	cards := make([]*waE2E.ContactMessage, 0, len(req.Content.Contacts))
	for i := range req.Content.Contacts {
		card, err := contactCard(&req.Content.Contacts[i])
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
		Contacts:    cards,
		ContextInfo: alongside,
	}}, nil
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

	phone := strings.TrimSpace(entry.Phone)
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
func vcardOf(name, phone string) string {
	digits := strings.TrimPrefix(phone, "+")
	return strings.Join([]string{
		"BEGIN:VCARD",
		"VERSION:" + vcardVersion,
		"N:;" + vcardEscape(name) + ";;;",
		"FN:" + vcardEscape(name),
		"TEL;type=CELL;type=VOICE;waid=" + digits + ":+" + digits,
		"END:VCARD",
	}, "\n") + "\n"
}

// vcardEscape quotes what a vCard reads as structure. Left in, a name with a comma or a
// semicolon in it splits into fields the recipient's client renders as separate parts of
// a name.
func vcardEscape(value string) string {
	replacer := strings.NewReplacer("\\", "\\\\", ";", "\\;", ",", "\\,", "\n", "\\n", "\r", "")
	return replacer.Replace(value)
}

// vcardName reads FN off a card, for a caller that sent one without saying what to
// label it. Empty when the card has no FN, which is a card this connector will not send.
func vcardName(card string) string {
	for line := range strings.SplitSeq(card, "\n") {
		trimmed := strings.TrimRight(line, "\r")
		// The property may carry parameters (`FN;CHARSET=UTF-8:...`), so the match is on
		// the name up to the first delimiter rather than on a prefix with a colon.
		name, value, found := strings.Cut(trimmed, ":")
		if !found {
			continue
		}
		if property, _, _ := strings.Cut(name, ";"); strings.EqualFold(property, "FN") {
			return strings.TrimSpace(value)
		}
	}
	return ""
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
func planMedia(content *outboundContent, limit int64) (*mediaPlan, error) {
	if _, known := mediaTypes[content.Kind]; !known {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is not a kind of file this contract carries", content.Kind))
	}
	if content.Kind == protocol.MediaSticker && content.Caption != "" {
		// Dropped silently, a caption the caller wrote never reaches anybody and the
		// send reports success. WhatsApp has nowhere to put one on a sticker.
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a sticker carries no caption: send the text as its own message")
	}
	address, err := fetchable(content.Ref)
	if err != nil {
		return nil, err
	}
	if declared := content.Size; declared > 0 && declared > limit {
		// Refused before the transfer rather than after it, on the caller's own claim,
		// the same way an inbound file is refused on the sender's.
		return nil, protocol.NewError(protocol.ErrorMediaTooLarge,
			fmt.Sprintf("the caller says %d bytes and this instance sends at most %d", declared, limit))
	}
	thumbnail, err := thumbnailBytes(content.Thumbnail)
	if err != nil {
		return nil, err
	}
	return &mediaPlan{address: address, thumbnail: thumbnail}, nil
}

// mediaToSend fetches the file the plan names and hands it to WhatsApp.
func (s *Session) mediaToSend(
	ctx context.Context, req *sendRequest, plan *mediaPlan, alongside *waE2E.ContextInfo,
) (*waE2E.Message, error) {
	content := &req.Content
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
	uploaded, err := s.upload(ctx, mediaTypes[content.Kind], capped)
	switch {
	case errors.Is(err, errTooLarge):
		return nil, protocol.NewError(protocol.ErrorMediaTooLarge,
			fmt.Sprintf("that file is larger than the %d bytes this instance sends", s.sendLimit))
	case err != nil:
		return nil, err
	}

	return renderMedia(content, &uploaded, mimeToSend(content, file.mime), plan.thumbnail, alongside), nil
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
			fmt.Sprintf("%q is not an address: %v", ref.URL, err))
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		// A `file:` URL would have this instance read its own disk and send whatever is
		// at that path, which is not something a message body should be able to ask for.
		return "", protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is not an address this connector fetches over", parsed.Scheme))
	}
	if parsed.Host == "" {
		return "", protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q names no host to fetch from", ref.URL))
	}
	return ref.URL, nil
}

// mimeToSend decides what to tell WhatsApp the file is.
//
// The caller's own word comes first: it knows what it stored, and the address it named
// may be a proxy that labels everything a stream of bytes. What the server said is the
// fallback, and the filename's extension is the last resort.
func mimeToSend(content *outboundContent, served string) string {
	if content.Mime != "" {
		return content.Mime
	}
	if served != "" && served != "application/octet-stream" {
		return served
	}
	if ext := filenameExt(content.Filename); ext != "" {
		if guessed := mime.TypeByExtension(ext); guessed != "" {
			return guessed
		}
	}
	if served != "" {
		return served
	}
	return "application/octet-stream"
}

// filenameExt is the extension of a name, lowercased, or empty when there is none.
func filenameExt(filename string) string {
	dot := strings.LastIndex(filename, ".")
	if dot < 0 || dot == len(filename)-1 {
		return ""
	}
	return strings.ToLower(filename[dot:])
}

// thumbnailBytes decodes the inline preview the caller supplied, and answers nil when
// there is none.
//
// Only a JPEG is carried: the field WhatsApp reads is named for the format, and a PNG
// put in it renders as a broken preview on clients that take it at its word.
func thumbnailBytes(uri string) ([]byte, error) {
	if uri == "" {
		return nil, nil
	}
	if len(uri) > thumbnailLimit {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("that preview is %d bytes and at most %d travel in a frame", len(uri), thumbnailLimit))
	}
	const prefix = "data:image/jpeg;base64,"
	if !strings.HasPrefix(uri, prefix) {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a preview has to be a base64 data: URI of a JPEG")
	}
	raw, err := base64.StdEncoding.DecodeString(uri[len(prefix):])
	if err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("that preview is not base64: %v", err))
	}
	return raw, nil
}

// renderMedia builds the leaf message for what was just uploaded.
//
// The five shapes are separate types on WhatsApp's side with separate fields, and the
// fields they do not share are the ones that matter to a recipient: a document has its
// name, a voice note its flag and its length, a sticker neither.
func renderMedia(
	content *outboundContent, uploaded *wm.UploadResponse,
	mimetype string, thumbnail []byte, alongside *waE2E.ContextInfo,
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
		return &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
			URL: &uploaded.URL, DirectPath: &uploaded.DirectPath, MediaKey: uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256, FileSHA256: uploaded.FileSHA256,
			FileLength: proto.Uint64(uploaded.FileLength),
			Mimetype:   proto.String(mimetype),
			FileName:   optional(content.Filename), Caption: optional(content.Caption),
			JPEGThumbnail: thumbnail,
			ContextInfo:   alongside,
		}}
	default:
		return &waE2E.Message{StickerMessage: &waE2E.StickerMessage{
			URL: &uploaded.URL, DirectPath: &uploaded.DirectPath, MediaKey: uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256, FileSHA256: uploaded.FileSHA256,
			FileLength:  proto.Uint64(uploaded.FileLength),
			Mimetype:    proto.String(mimetype),
			ContextInfo: alongside,
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
}

func (c *counting) Read(into []byte) (int, error) {
	if c.read > c.limit {
		return 0, errTooLarge
	}
	// One byte past the cap may be read, and only one. It is what separates a file that
	// is exactly the cap from one that is larger: a reader is under no obligation to
	// report EOF alongside the last bytes rather than on the read after them, so a
	// counter that refused at the cap would refuse a file the setting says it sends.
	if room := c.limit - c.read + 1; int64(len(into)) > room {
		into = into[:room]
	}
	read, err := c.from.Read(into)
	c.read += int64(read)
	if c.read > c.limit {
		return 0, errTooLarge
	}
	if err != nil {
		return read, err //nolint:wrapcheck // the reader's own error, passed through unchanged
	}
	return read, nil
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
			fmt.Sprintf("that is not an address a file can be fetched from: %v", err))
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}

	client := &http.Client{
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) >= fetchRedirects {
				return fmt.Errorf("stopped after %d redirects", fetchRedirects)
			}
			return nil
		},
	}
	answer, err := client.Do(request)
	if err != nil {
		// Worth trying again: the address may be a service that is restarting, and the
		// caller holds the only copy of what it wanted to send. Reported as `internal`
		// because the contract has no code for a dependency the caller itself named
		// being down, and of the codes it does have this is the only retryable one that
		// is not a lie about who timed out. The message says whose fault it actually is,
		// and the missing code is issue #26.
		return source{}, protocol.NewError(protocol.ErrorInternal,
			fmt.Sprintf("could not fetch the file to send: %v", err))
	}
	if answer.StatusCode != http.StatusOK {
		_ = answer.Body.Close()
		return source{}, statusFailure(answer.StatusCode)
	}

	return source{
		body: answer.Body,
		size: answer.ContentLength,
		mime: strings.TrimSpace(strings.Split(answer.Header.Get("Content-Type"), ";")[0]),
	}, nil
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
		// `internal` for the same reason as the unreachable case above: retryable, and
		// the contract has nothing that says the caller's own server is having a bad
		// minute (#26).
		return protocol.NewError(protocol.ErrorInternal,
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
	default:
		return protocol.NewError(protocol.ErrorWaError, "WhatsApp would not take the file")
	}
}
