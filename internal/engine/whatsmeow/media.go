package whatsmeow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"strings"
	"time"

	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// downloadTimeout bounds the time one inbound message spends fetching its file.
//
// whatsmeow gives a node handler five minutes before it starts the next node's handler
// alongside it, and the moment two run at once the order they publish in is the
// scheduler's to decide. A media message spends this and then, at worst, two
// deliverWaits: the message itself, and the failure that follows it. Ninety seconds
// puts that ceiling at three and a half minutes, which is the margin that keeps a
// session's messages in the order WhatsApp sent them.
const downloadTimeout = 90 * time.Second

// thumbnailLimit is the largest inline preview the contract carries. Measured on the
// data: URI rather than on the bytes behind it, because the URI is what travels and
// base64 is a third longer than what it encodes.
const thumbnailLimit = 32 << 10

// Why the file of a message is not coming. These reach an agent as the whole
// explanation, by way of `media.download_failed`, so each one names something that
// happened rather than the call that returned it.
const (
	// reasonMediaExpired is WhatsApp's own servers refusing the file. Media is kept for
	// a bounded time and the key that decrypts it lapses with it, which is what an old
	// message downloaded late looks like from here.
	reasonMediaExpired = "media_key_expired"
	// reasonCorrupt is bytes that arrived and are not the ones the message describes.
	reasonCorrupt = "integrity_check_failed"
	// reasonUnreferenced is a media message that names no file to fetch.
	reasonUnreferenced = "no_media_reference"
	// reasonTooLarge is a file past what this instance keeps, whether the sender said
	// so up front or the bytes said so on the way in.
	reasonTooLarge = "too_large"
	// reasonViewOnce is a file the sender meant to be seen once and then be gone. It is
	// not a failure to download and it is published as one on purpose: the client's
	// answer to both is the same bubble, and there is no field on the contract yet that
	// says why this one is unavailable.
	reasonViewOnce = "view_once"
	// reasonNoStore is an instance with nowhere to put a file. The message is still
	// worth publishing: its caption, its name and its size are on the event, so the
	// client renders a bubble whose file is unavailable rather than an empty one.
	reasonNoStore = "no_media_store"
)

// refused is a download that is not worth trying again: the bytes are gone, they do not
// check out, or this instance will not keep them. It is the half of a failure a client
// is told about; everything else leaves the message on the phone for WhatsApp to send
// again.
type refused struct {
	reason string
	err    error
}

func (r refused) Error() string {
	if r.err == nil {
		return r.reason
	}
	return r.reason + ": " + r.err.Error()
}

func (r refused) Unwrap() error { return r.err }

// Blobs is the half of the media store a session writes to. An interface so a test can
// make a store fail without needing a filesystem that will.
type Blobs interface {
	Put(ctx context.Context, source io.Reader, about *media.Blob) (media.Blob, error)
	MaxBlob() int64
	TTL() time.Duration
}

// MediaOptions is where a session puts the file of an inbound message, and the address
// it tells a client to fetch it from.
type MediaOptions struct {
	Blobs Blobs
	// BaseURL is this instance's own address as the rest of the deployment reaches it.
	// A blob lives on the disk of whoever downloaded it, so the reference has to name
	// that instance: a URL pointing at the service in front of the fleet reaches a
	// different one on every request and answers 404 from all but one.
	BaseURL string
	// SendMax is the largest file a session will send. It belongs here for the subject
	// rather than for the store: the two are independent, and an instance given no blob
	// root at all still sends. The zero value asks for DefaultSendMax, so an engine
	// built without one is not an engine that refuses every file.
	SendMax int64
}

// attachment is the media part of a message: what the contract says about the file, the
// context that came with it, and the handle whatsmeow downloads it by.
type attachment struct {
	content  protocol.MediaContent
	context  *waE2E.ContextInfo
	download wm.DownloadableMessage
}

// attachmentOf pulls the media out of a message and reports whether there is any.
//
// Only the five leaf types are reachable: whatsmeow has already unwrapped the envelopes
// WhatsApp puts around a message — view once, ephemeral, a document sent with a caption
// — before a handler sees it.
func attachmentOf(message *waE2E.Message) (attachment, bool) {
	switch {
	case message.GetImageMessage() != nil:
		part := message.GetImageMessage()
		content := protocol.Media(protocol.MediaImage)
		content.Mime = part.GetMimetype()
		content.Caption = part.GetCaption()
		content.Size = declaredSize(part.GetFileLength())
		content.Thumbnail = thumbnailOf("image/jpeg", part.GetJPEGThumbnail())
		return attachment{content: content, context: part.GetContextInfo(), download: part}, true

	case message.GetVideoMessage() != nil:
		// An animated GIF arrives as a video with `gifPlayback` set. The contract has no
		// kind for it and a client renders it as the video it is.
		part := message.GetVideoMessage()
		content := protocol.Media(protocol.MediaVideo)
		content.Mime = part.GetMimetype()
		content.Caption = part.GetCaption()
		content.Size = declaredSize(part.GetFileLength())
		content.Duration = part.GetSeconds()
		content.Thumbnail = thumbnailOf("image/jpeg", part.GetJPEGThumbnail())
		return attachment{content: content, context: part.GetContextInfo(), download: part}, true

	case message.GetAudioMessage() != nil:
		// No name and no caption: WhatsApp's audio message carries neither. Inventing a
		// filename here would beat the one the client builds from the mime type and the
		// message id, which is the name an agent downloading it should see.
		part := message.GetAudioMessage()
		content := protocol.Media(protocol.MediaAudio)
		content.Mime = part.GetMimetype()
		content.Size = declaredSize(part.GetFileLength())
		content.Duration = part.GetSeconds()
		content.VoiceNote = part.GetPTT()
		return attachment{content: content, context: part.GetContextInfo(), download: part}, true

	case message.GetDocumentMessage() != nil:
		part := message.GetDocumentMessage()
		content := protocol.Media(protocol.MediaDocument)
		content.Mime = part.GetMimetype()
		content.Filename = part.GetFileName()
		content.Caption = part.GetCaption()
		content.Size = declaredSize(part.GetFileLength())
		content.Thumbnail = thumbnailOf("image/jpeg", part.GetJPEGThumbnail())
		return attachment{content: content, context: part.GetContextInfo(), download: part}, true

	case message.GetStickerMessage() != nil:
		// A sticker's preview is a PNG rather than a JPEG, and a sticker carries no
		// caption of its own.
		part := message.GetStickerMessage()
		content := protocol.Media(protocol.MediaSticker)
		content.Mime = part.GetMimetype()
		content.Size = declaredSize(part.GetFileLength())
		content.Thumbnail = thumbnailOf("image/png", part.GetPngThumbnail())
		return attachment{content: content, context: part.GetContextInfo(), download: part}, true

	default:
		return attachment{}, false
	}
}

// declaredSize is the length the sender put on the file.
//
// It is a claim and not a measurement — what is stored is measured on the way in — and
// the one thing it decides is whether to start a download at all. A value that does not
// fit is reported as unknown rather than as the negative number the conversion would
// make of it: a size that reads as less than nothing passes every cap there is.
func declaredSize(length uint64) int64 {
	if length > math.MaxInt64 {
		return 0
	}
	return int64(length)
}

// thumbnailOf renders an inline preview as the data: URI the contract carries, and
// nothing at all for one that is missing or too big to travel. Losing a preview costs a
// placeholder until the file arrives; carrying an oversized one costs every reader of
// the stream, for a picture of something they are about to fetch anyway.
func thumbnailOf(mime string, raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	uri := "data:" + mime + ";base64," + base64.StdEncoding.EncodeToString(raw)
	if len(uri) > thumbnailLimit {
		return ""
	}
	return uri
}

// mediaBody renders a media message, fetching its file on the way.
//
// A download that fails permanently is not a refusal: the message is published with no
// reference and the reason travels behind it, because a bubble that says the file is
// unavailable is worth more to an agent than a message that never arrives. A download
// that may work next time is a refusal, and the message stays on the phone.
func (s *Session) mediaBody(ctx context.Context, event *waEvents.Message) (body, bool) {
	part, ok := attachmentOf(event.Message)
	if !ok {
		return body{}, false
	}
	if viewOnce(event) {
		// Never fetched, so there is nothing to keep. A blob is served for as long as
		// anybody keeps asking for it — every hand-out puts its clock back — so storing
		// one of these turns a file the sender expected to disappear into one the
		// account holds indefinitely, replicated wherever its attachments go.
		//
		// The message still goes out: an agent seeing an unavailable attachment knows
		// somebody sent something, which is worth more than a message WhatsApp
		// redelivers for good and nobody ever reads. Delivering the file is a decision
		// with a contract change behind it (a flag on the content, and a blob handed out
		// once), and this is the side of it that can still be changed.
		// The preview goes with it. A thumbnail is the same picture at a lower
		// resolution and it travels inside the event, so leaving it on would put in
		// Redis and in front of every client exactly what not storing the file was for.
		part.content.Thumbnail = ""
		s.log.Info().Str("message_id", event.Info.ID).Str("kind", string(part.content.Kind)).
			Msg("publishing a view-once message without keeping the file it carried")
		return body{content: part.content, context: part.context, failure: reasonViewOnce}, true
	}

	download, cancel := context.WithTimeout(ctx, s.downloadWait)
	defer cancel()

	ref, err := s.fetch(download, &part)
	var giveUp refused
	switch {
	case errors.As(err, &giveUp):
		s.log.Warn().Err(err).Str("kind", string(part.content.Kind)).
			Msg("publishing a media message whose file is not coming")
		return body{content: part.content, context: part.context, failure: giveUp.reason}, true
	case err != nil:
		s.log.Warn().Err(err).Str("kind", string(part.content.Kind)).
			Msg("withholding an acknowledgement for a media message whose file may arrive next time")
		return body{}, false
	}

	part.content.Ref = &ref
	// The measured length replaces the sender's claim now that there is one. They agree
	// for every honest sender, and where they do not the client should size the file it
	// can actually fetch.
	part.content.Size = ref.Size

	// Recorded after that, so a refetch checks the length that was actually written
	// against the cap rather than the one the sender announced. The two only differ for
	// a sender who understated, and that is exactly the file a lowered cap should stop
	// before it is downloaded rather than after.
	s.remember(event, &part)
	return body{content: part.content, context: part.context}, true
}

// viewOnce reports whether the sender meant this file to be seen once and then be gone.
//
// Both halves are read because they are set by different paths: whatsmeow raises the
// flag on the event when it unwraps one of the three view-once envelopes, and the
// sender's own field rides on the media itself. A document and a sticker have no such
// field, which is WhatsApp's answer to whether either can be sent this way.
func viewOnce(event *waEvents.Message) bool {
	return event.IsViewOnce ||
		event.Message.GetImageMessage().GetViewOnce() ||
		event.Message.GetVideoMessage().GetViewOnce() ||
		event.Message.GetAudioMessage().GetViewOnce()
}

// fetch downloads the file of a media message, stores it, and returns the reference a
// client fetches it by.
//
// The error it returns says what should happen to the message: a refused is permanent,
// anything else is this instance's problem right now.
func (s *Session) fetch(ctx context.Context, part *attachment) (protocol.MediaRef, error) {
	if s.blobs == nil {
		return protocol.MediaRef{}, refused{reason: reasonNoStore}
	}
	if declared, limit := part.content.Size, s.blobs.MaxBlob(); declared > limit {
		// Refused before the transfer rather than after it. The sender's claim is the
		// only measure available this early, and it is worth acting on: a file that
		// says it is a gigabyte is one nothing here would keep.
		return protocol.MediaRef{}, refused{
			reason: reasonTooLarge,
			err:    fmt.Errorf("the sender says %d bytes and this instance keeps %d", declared, limit),
		}
	}

	if err := unfetchable(part.download); err != nil {
		return protocol.MediaRef{}, err
	}

	data, err := s.download(ctx, s.current(), part.download)
	if err != nil {
		return protocol.MediaRef{}, downloadFailure(err)
	}

	stored, err := s.blobs.Put(ctx, bytes.NewReader(data), &media.Blob{
		Mime: part.content.Mime, Filename: part.content.Filename,
	})
	switch {
	case errors.Is(err, media.ErrTooLarge):
		// The sender understated the length. Permanent for this file: the same bytes
		// arrive on every attempt and are refused by the same cap.
		return protocol.MediaRef{}, refused{reason: reasonTooLarge, err: err}
	case err != nil:
		// A disk that could not take this file may take the next one, and the message is
		// worth another go rather than a bubble that says the file is gone.
		return protocol.MediaRef{}, fmt.Errorf("store the file of a media message: %w", err)
	}

	return protocol.MediaRef{
		Kind: protocol.MediaRefConnectorBlob, ID: stored.ID, URL: s.blobURL(stored.ID),
		Size: stored.Size, Mime: stored.Mime, SHA256: stored.SHA256,
		ExpiresAt: stored.StoredAt + s.blobs.TTL().Milliseconds(),
	}, nil
}

// unfetchable reports metadata that can never describe a file, with the reason to
// publish, and nil for metadata worth trying.
//
// whatsmeow answers both of these with a plain error rather than one of its sentinels,
// so without the check they fall to the transient arm of downloadFailure: the message is
// withheld, WhatsApp redelivers it carrying exactly the same metadata, and it fails
// exactly the same way, for good. Checked here rather than matched on the text whatsmeow
// writes, which is not part of its contract and changes when it likes.
func unfetchable(part wm.DownloadableMessage) error {
	path := part.GetDirectPath()
	if !strings.HasPrefix(path, "/") {
		// The path is pasted into a URL under whichever media host answers, so one that
		// is not a path names nothing there. Reported without the value: it is a
		// fragment of a URL to somebody's file, and nothing in a log needs it.
		return refused{reason: reasonUnreferenced, err: errors.New("the direct path is not a path")}
	}
	if _, err := url.Parse(path); err != nil {
		// A leading slash is not enough: whatsmeow pastes the whole thing into a URL and
		// builds a request out of it, so a bad escape or a control character fails there
		// instead, with a plain error, on every attempt forever.
		return refused{reason: reasonUnreferenced, err: errors.New("the direct path cannot be parsed as one")}
	}
	// nil rather than empty, because that is the line whatsmeow draws: a nil digest is
	// unencrypted media and it drops the key to match, while a digest that is present
	// and empty keeps the key, takes the encrypted path, and fails the length check
	// there with a plain error. A field encoded on the wire as empty unmarshals to a
	// non-nil slice of length zero, so the two are told apart by nil alone.
	if digest := part.GetFileEncSHA256(); digest != nil && len(digest) != sha256.Size {
		return refused{
			reason: reasonCorrupt,
			err:    fmt.Errorf("the ciphertext digest is %d bytes and a SHA-256 is %d", len(digest), sha256.Size),
		}
	}
	return nil
}

// downloadFailure classifies what whatsmeow said about a download.
//
// Only the answers that mean the same thing every time are permanent. A network that
// was down, a media connection that could not be refreshed and a deadline that ran out
// are all this instance's problem right now, and the message is worth another go.
func downloadFailure(err error) error {
	switch {
	case errors.Is(err, wm.ErrMediaDownloadFailedWith403),
		errors.Is(err, wm.ErrMediaDownloadFailedWith404),
		errors.Is(err, wm.ErrMediaDownloadFailedWith410):
		// WhatsApp keeps a file for a bounded time and the key that decrypts it lapses
		// with it. Nothing about asking again brings either back.
		return refused{reason: reasonMediaExpired, err: err}

	case errors.Is(err, wm.ErrInvalidMediaHMAC),
		errors.Is(err, wm.ErrInvalidMediaSHA256),
		errors.Is(err, wm.ErrInvalidMediaEncSHA256),
		errors.Is(err, wm.ErrInvalidUnencryptedMediaSHA256),
		errors.Is(err, wm.ErrTooShortFile):
		// The bytes arrived and are not the ones the message describes. A redelivery
		// carries the same description and downloads the same file.
		return refused{reason: reasonCorrupt, err: err}

	case errors.Is(err, wm.ErrNoURLPresent), errors.Is(err, wm.ErrUnknownMediaType):
		// The message names no file to fetch, so there is nothing a second attempt would
		// find.
		return refused{reason: reasonUnreferenced, err: err}

	default:
		// Masked rather than wrapped whole. whatsmeow builds the media request URL out of
		// the direct path and the hash, and a transport failure comes back carrying that
		// URL: it is how the ciphertext is fetched, so it belongs in a log no more than
		// the file does. The chain is dropped with it, which costs nothing — this is the
		// arm that means "unclassified", and nothing branches on what is in it.
		return fmt.Errorf("download the file of a media message: %s", redact(err.Error()))
	}
}

// blobURL is where this instance serves one blob.
func (s *Session) blobURL(id string) string {
	return strings.TrimSuffix(s.blobBase, "/") + "/media/" + id
}

// --- fetching the same file a second time ------------------------------------------

// remember records how to fetch this message's file again.
//
// The reference published with an event stops working, and on a schedule the client
// cannot see: the blob is dropped on its TTL or evicted by the quota, and the instance
// serving it is replaced on every deploy. Coming back for the file then needs the
// coordinates WhatsApp wants, which are on the original message and nowhere else once
// the event has gone out.
//
// A failure here is logged rather than returned. The message has its file now and the
// client is about to be handed a reference that works; withholding it because a later
// refetch might not be possible trades a message that arrives for one that might.
func (s *Session) remember(event *waEvents.Message, part *attachment) {
	ctx, cancel := context.WithTimeout(s.ctx, s.storeLimit)
	defer cancel()

	messageID := event.Info.ID
	// Through chatOf, which is what the event was published under. Recomputing it here
	// would be a second copy of the broadcast rule, and a copy that drifted would file a
	// message's file under a chat the message is not in.
	chat, _ := chatOf(&event.Info)
	kept := store.MediaPart{
		MessageID: messageID,
		ChatKind:  string(chat.Kind), ChatID: chat.ID, Kind: string(part.content.Kind),
		DirectPath: part.download.GetDirectPath(), MediaKey: part.download.GetMediaKey(),
		FileEncSHA256: part.download.GetFileEncSHA256(), FileSHA256: part.download.GetFileSHA256(),
		FileLength: part.content.Size, Mime: part.content.Mime, Filename: part.content.Filename,
	}
	if err := s.store.PutMediaPart(ctx, &kept, time.Now()); err != nil {
		s.log.Warn().Err(err).Str("message_id", messageID).
			Msg("published a file this session will not be able to fetch a second time")
	}
}

// downloadMedia is `message.download_media`: the file of a message this session already
// published, fetched again and put in this instance's store.
//
// It is what stands between a blob that is gone and a message the client marks
// unsupported for good. Which of the two happens is decided by the error code alone, so
// each one below is chosen for what it makes the client do rather than for what
// happened here.
func (s *Session) downloadMedia(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var body struct {
		MessageID string            `json:"message_id"`
		Chat      *protocol.Address `json:"chat"`
	}
	if err := json.Unmarshal(command.Payload, &body); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "a download has to say whose file it wants")
	}
	if body.MessageID == "" {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "a download has to name the message it is fetching")
	}
	if s.blobs == nil {
		// Nowhere to put the answer. Deliberately not `unsupported`: that is final on
		// the client and marks the message unsupported for good, and an instance with
		// no media root is a deployment somebody has to fix, not a file that is gone.
		return nil, protocol.NewError(protocol.ErrorInternal, "this instance has nowhere to keep a file")
	}

	kept, found, err := s.store.MediaPart(ctx, body.MessageID)
	switch {
	case err != nil:
		s.log.Warn().Err(err).Str("message_id", body.MessageID).Msg("could not look up how to fetch a file again")
		return nil, protocol.NewError(protocol.ErrorInternal, "how to fetch that file could not be looked up")
	case !found:
		// The message never had a file, or what was kept for it has aged past what this
		// deployment retains. Both are final: nothing about asking again brings the
		// coordinates back, and the client is better off saying so than retrying.
		return nil, protocol.NewError(protocol.ErrorMediaUnavailable,
			"nothing is kept for that message to fetch its file with")
	}

	if body.Chat != nil && (string(body.Chat.Kind) != kept.ChatKind || body.Chat.ID != kept.ChatID) {
		// A message id is the sender's to choose, so two chats under one account can
		// carry the same one and the second row replaces the first. Vanishingly rare and
		// the client keys by message id too, so it is not a case this can resolve -- but
		// it is one this must not resolve wrongly: handing back the file would put one
		// conversation's attachment in another's thread. The chat is optional on the
		// command, so this only fires when the caller brought one.
		s.log.Warn().Str("message_id", body.MessageID).
			Msg("refusing a download whose message is filed under a different chat")
		return nil, protocol.NewError(protocol.ErrorMediaUnavailable,
			"what is kept for that message belongs to a different chat")
	}

	if s.state() != "open" {
		// The download goes out over this session's own connection: whatsmeow refreshes
		// the media credentials on the socket, so a session that is not up fetches
		// nothing however good the coordinates are. Retried by the client rather than
		// given up on, which is what a connection coming back deserves.
		return nil, protocol.NewError(protocol.ErrorNotConnected, "the session is not connected to WhatsApp")
	}

	part, err := attachmentFrom(&kept)
	if err != nil {
		return nil, err
	}
	download, cancel := context.WithTimeout(ctx, s.downloadWait)
	defer cancel()

	ref, err := s.fetch(download, &part)
	if err != nil {
		s.log.Warn().Err(err).Str("message_id", body.MessageID).Msg("could not fetch a file a second time")
		return nil, refetchFailure(err)
	}
	return json.Marshal(ref)
}

// refetchFailure turns what fetch decided into what the client should do about it.
//
// The split is the same one the inbound path makes, read the other way round: what is
// permanent there is what the client should stop asking for here, and everything else is
// worth another go.
func refetchFailure(err error) error {
	var giveUp refused
	if !errors.As(err, &giveUp) {
		// A network, a deadline or a disk. None of them says anything about the file, so
		// the client retries and then surfaces a job somebody looks at, rather than
		// telling an agent the attachment is gone.
		return protocol.NewError(protocol.ErrorInternal, "the file could not be fetched")
	}
	if giveUp.reason == reasonTooLarge {
		return protocol.NewError(protocol.ErrorMediaTooLarge, giveUp.reason)
	}
	return protocol.NewError(protocol.ErrorMediaUnavailable, giveUp.reason)
}

// attachmentFrom rebuilds enough of a media message to download it again.
//
// Only what fetch reads: the coordinates, the size it checks against the cap, and the
// mime and name the blob is stored under. What is missing -- the caption, the preview,
// how long the audio runs -- was never kept, because the client already has all of it
// from the event and the answer to this command is a reference and nothing else.
func attachmentFrom(kept *store.MediaPart) (attachment, error) {
	download, err := downloadableOf(kept)
	if err != nil {
		return attachment{}, err
	}
	content := protocol.Media(protocol.MediaKind(kept.Kind))
	content.Mime = kept.Mime
	content.Filename = kept.Filename
	content.Size = kept.FileLength
	return attachment{content: content, download: download}, nil
}

// downloadableOf rebuilds the message whatsmeow downloads from.
//
// The concrete type carries half the address: whatsmeow reads the media type off it, and
// asks a different endpoint for each. So the kind is not a label on the row, it is part
// of what makes the fetch work.
func downloadableOf(kept *store.MediaPart) (wm.DownloadableMessage, error) {
	path, key := proto.String(kept.DirectPath), kept.MediaKey
	enc, plain, mime := kept.FileEncSHA256, kept.FileSHA256, proto.String(kept.Mime)

	switch protocol.MediaKind(kept.Kind) {
	case protocol.MediaImage:
		return &waE2E.ImageMessage{
			DirectPath: path, MediaKey: key, FileEncSHA256: enc, FileSHA256: plain, Mimetype: mime,
		}, nil
	case protocol.MediaVideo:
		return &waE2E.VideoMessage{
			DirectPath: path, MediaKey: key, FileEncSHA256: enc, FileSHA256: plain, Mimetype: mime,
		}, nil
	case protocol.MediaAudio:
		return &waE2E.AudioMessage{
			DirectPath: path, MediaKey: key, FileEncSHA256: enc, FileSHA256: plain, Mimetype: mime,
		}, nil
	case protocol.MediaDocument:
		return &waE2E.DocumentMessage{
			DirectPath: path, MediaKey: key, FileEncSHA256: enc, FileSHA256: plain, Mimetype: mime,
			FileName: proto.String(kept.Filename),
		}, nil
	case protocol.MediaSticker:
		return &waE2E.StickerMessage{
			DirectPath: path, MediaKey: key, FileEncSHA256: enc, FileSHA256: plain, Mimetype: mime,
		}, nil
	}
	// A row this build cannot read is a row an older or a newer one wrote, and guessing a
	// type would ask WhatsApp for the file under the wrong endpoint.
	return nil, protocol.NewError(protocol.ErrorMediaUnavailable,
		fmt.Sprintf("what is kept for that message is a %q, which this connector cannot fetch", kept.Kind))
}
