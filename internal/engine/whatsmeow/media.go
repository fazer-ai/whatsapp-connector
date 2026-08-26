package whatsmeow

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
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
		return fmt.Errorf("download the file of a media message: %w", err)
	}
}

// blobURL is where this instance serves one blob.
func (s *Session) blobURL(id string) string {
	return strings.TrimSuffix(s.blobBase, "/") + "/media/" + id
}
