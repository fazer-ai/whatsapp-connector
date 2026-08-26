package whatsmeow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// storedAt is the clock the blob store keeps for these tests, so an expiry can be
// asserted against a number rather than against whatever the wall clock said.
var storedAt = time.UnixMilli(1755000000000)

// The metadata whatsmeow insists on before it will go and look: a direct path that is a
// path, and a ciphertext digest of the right length. A test about what happens to the
// file carries it, so what it exercises is the download and not the check in front of it.
const directPath = "/v/t62.7118-24/file"

func encSHA256() []byte { return make([]byte, sha256.Size) }

func TestEachMediaKindIsRenderedTheWayTheContractCarriesIt(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		message *waE2E.Message
		want    protocol.MediaContent
	}{
		{
			name: "an image with a caption",
			message: &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
				Mimetype: proto.String("image/jpeg"), Caption: proto.String("olha isso"),
				FileLength: proto.Uint64(184320),
			}},
			want: protocol.MediaContent{
				Type: "media", Kind: protocol.MediaImage, Mime: "image/jpeg",
				Caption: "olha isso", Size: 184320,
			},
		},
		{
			// An animated GIF arrives as a video with gifPlayback set, and the contract
			// has no kind of its own for it.
			name: "a video, which is also what a GIF is",
			message: &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
				Mimetype: proto.String("video/mp4"), Caption: proto.String("veja"),
				FileLength: proto.Uint64(1048576), Seconds: proto.Uint32(12),
				GifPlayback: proto.Bool(true),
			}},
			want: protocol.MediaContent{
				Type: "media", Kind: protocol.MediaVideo, Mime: "video/mp4",
				Caption: "veja", Size: 1048576, Duration: 12,
			},
		},
		{
			name: "a voice note",
			message: &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
				Mimetype:   proto.String("audio/ogg; codecs=opus"),
				FileLength: proto.Uint64(20480), Seconds: proto.Uint32(7), PTT: proto.Bool(true),
			}},
			want: protocol.MediaContent{
				Type: "media", Kind: protocol.MediaAudio, Mime: "audio/ogg; codecs=opus",
				Size: 20480, Duration: 7, VoiceNote: true,
			},
		},
		{
			// The same shape without the flag. A client files a voice note beside the
			// conversation and an audio file as an attachment, so the two must not read
			// alike.
			name: "an audio file somebody attached",
			message: &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
				Mimetype: proto.String("audio/mpeg"), FileLength: proto.Uint64(3145728),
				Seconds: proto.Uint32(180),
			}},
			want: protocol.MediaContent{
				Type: "media", Kind: protocol.MediaAudio, Mime: "audio/mpeg",
				Size: 3145728, Duration: 180,
			},
		},
		{
			name: "a document",
			message: &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
				Mimetype: proto.String("application/pdf"), FileName: proto.String("contrato.pdf"),
				Caption: proto.String("segue o contrato"), FileLength: proto.Uint64(512000),
			}},
			want: protocol.MediaContent{
				Type: "media", Kind: protocol.MediaDocument, Mime: "application/pdf",
				Filename: "contrato.pdf", Caption: "segue o contrato", Size: 512000,
			},
		},
		{
			name: "a sticker",
			message: &waE2E.Message{StickerMessage: &waE2E.StickerMessage{
				Mimetype: proto.String("image/webp"), FileLength: proto.Uint64(30720),
			}},
			want: protocol.MediaContent{
				Type: "media", Kind: protocol.MediaSticker, Mime: "image/webp", Size: 30720,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			part, ok := attachmentOf(tc.message)
			if !ok {
				t.Fatal("a media message this build carries was not recognised as one")
			}
			if part.content != tc.want {
				t.Fatalf("the content is %+v, want %+v", part.content, tc.want)
			}
			if part.download == nil {
				t.Fatal("the media has nothing to download it by")
			}

			payload, err := json.Marshal(part.content)
			if err != nil {
				t.Fatalf("marshal the content: %v", err)
			}
			validateAgainstContract(t, "content_media", payload)
		})
	}
}

// A message with no file is not this package's to render, and saying so is what lets
// the caller try the renderer that does carry it.
func TestAMessageWithNoFileIsNotMediaAtAll(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		message *waE2E.Message
	}{
		{"plain text", &waE2E.Message{Conversation: proto.String("bom dia")}},
		{"text with a quote", &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text: proto.String("em resposta"),
		}}},
		{"a type this build has yet to reach", &waE2E.Message{LocationMessage: &waE2E.LocationMessage{
			DegreesLatitude: proto.Float64(-25.4), DegreesLongitude: proto.Float64(-49.2),
		}}},
		{"nothing at all", &waE2E.Message{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if part, ok := attachmentOf(tc.message); ok {
				t.Fatalf("a message with no file was rendered as %+v", part.content)
			}
		})
	}
}

func TestAThumbnailTravelsAsADataURIAndOneTooBigToTravelIsDropped(t *testing.T) {
	t.Parallel()

	small := []byte{0xFF, 0xD8, 0xFF, 0xE0}
	image, ok := attachmentOf(&waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		JPEGThumbnail: small,
	}})
	if !ok {
		t.Fatal("an image was not recognised as media")
	}
	want := "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(small)
	if image.content.Thumbnail != want {
		t.Fatalf("the preview is %q, want %q", image.content.Thumbnail, want)
	}

	// A sticker's preview is a PNG, and saying JPEG would have every client render a
	// broken image where the placeholder should be.
	sticker, ok := attachmentOf(&waE2E.Message{StickerMessage: &waE2E.StickerMessage{
		PngThumbnail: small,
	}})
	if !ok {
		t.Fatal("a sticker was not recognised as media")
	}
	if !strings.HasPrefix(sticker.content.Thumbnail, "data:image/png;base64,") {
		t.Fatalf("a sticker's preview is %q, want a PNG data URI", sticker.content.Thumbnail)
	}

	// The whole point of the bound is the frame: a preview past it costs every reader
	// of the stream, for a picture of a file they are about to fetch anyway.
	oversized, ok := attachmentOf(&waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		JPEGThumbnail: make([]byte, thumbnailLimit),
	}})
	if !ok {
		t.Fatal("an image was not recognised as media")
	}
	if oversized.content.Thumbnail != "" {
		t.Fatalf("a preview of %d bytes travelled inside the frame", len(oversized.content.Thumbnail))
	}
}

// A size that does not fit an int64 is a claim nobody could have meant. Converted
// straight it reads as a negative number, which passes every cap there is and starts a
// download of whatever the sender feels like sending.
func TestASizeTheSenderCannotHaveMeantIsTreatedAsUnknown(t *testing.T) {
	t.Parallel()

	part, ok := attachmentOf(&waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
		FileLength: proto.Uint64(1 << 63),
	}})
	if !ok {
		t.Fatal("a document was not recognised as media")
	}
	if part.content.Size != 0 {
		t.Fatalf("the size is %d, want it reported as unknown", part.content.Size)
	}
}

func TestAMediaMessageIsPublishedWithTheBlobItWasStoredIn(t *testing.T) {
	t.Parallel()

	file := []byte("os bytes da imagem")
	session, downloads := mediaSession(t, media.Options{})
	downloads.answer(file, nil)

	// Deliberately not the length of the file: what a client is handed has to be what
	// it will actually fetch, not what the sender said it was sending.
	event := mediaEvent("3EB0IMAGE", &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Mimetype: proto.String("image/jpeg"), Caption: proto.String("olha isso"),
		FileLength: proto.Uint64(999999),
		DirectPath: proto.String(directPath), MediaKey: []byte("key"), FileEncSHA256: encSHA256(),
	}})

	emissions, acknowledged := deliver(t, session, event, 1)
	if !acknowledged {
		t.Fatal("a media message whose file was stored was left unacknowledged")
	}
	if emissions[0].Type != protocol.EventMessageReceived {
		t.Fatalf("the session published %s, want the message", emissions[0].Type)
	}

	content := mediaContentOf(t, emissions[0])
	if content.Size != int64(len(file)) {
		t.Fatalf("the size is %d, want the %d bytes actually stored", content.Size, len(file))
	}
	ref := content.Ref
	if ref == nil {
		t.Fatal("the message carries no reference to the file that was stored")
	}
	if ref.Kind != protocol.MediaRefConnectorBlob {
		t.Fatalf("the reference is a %q, want a blob on this instance", ref.Kind)
	}
	if want := "http://connector-a1b2c3:8080/media/" + ref.ID; ref.URL != want {
		t.Fatalf("the reference points at %q, want %q", ref.URL, want)
	}
	if ref.Size != int64(len(file)) || ref.Mime != "image/jpeg" || ref.SHA256 == "" {
		t.Fatalf("the reference does not describe what was stored: %+v", ref)
	}
	if want := storedAt.Add(media.DefaultTTL).UnixMilli(); ref.ExpiresAt != want {
		t.Fatalf("the reference lapses at %d, want %d", ref.ExpiresAt, want)
	}

	// And the bytes are really there, under the id the client was handed.
	body, about, err := session.blobs.(*media.Store).Open(ref.ID)
	if err != nil {
		t.Fatalf("the reference names a blob the store does not have: %v", err)
	}
	defer func() { _ = body.Close() }()
	if kept, _ := io.ReadAll(body); !bytes.Equal(kept, file) {
		t.Fatalf("the blob holds %q, want %q", kept, file)
	}
	if about.SHA256 != ref.SHA256 {
		t.Fatalf("the stored digest is %q and the published one %q", about.SHA256, ref.SHA256)
	}

	validateAgainstContract(t, "event_message_received", emissions[0].Payload)
}

// The context a media message carries is the media part's own, not the envelope's.
// Reading it off the wrong place loses every quote, mention and timer a media message
// was sent with.
func TestAMediaMessageCarriesTheQuoteTheMentionsAndTheTimerItCameWith(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer([]byte("bytes"), nil)

	event := mediaEvent("3EB0DOC", &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
		Mimetype: proto.String("application/pdf"), FileName: proto.String("contrato.pdf"),
		DirectPath: proto.String(directPath), MediaKey: []byte("key"), FileEncSHA256: encSHA256(),
		ContextInfo: &waE2E.ContextInfo{
			StanzaID: proto.String("3EB0ABCDEF"), Expiration: proto.Uint32(604800),
			MentionedJID: []string{"5511999990001@" + waTypes.DefaultUserServer},
		},
	}})

	emissions, _ := deliver(t, session, event, 1)
	var payload struct {
		Message protocol.InboundMessage `json:"message"`
	}
	if err := json.Unmarshal(emissions[0].Payload, &payload); err != nil {
		t.Fatalf("decode the event: %v", err)
	}
	if payload.Message.QuotedID != "3EB0ABCDEF" {
		t.Fatalf("the quote is %q, want the message it answers", payload.Message.QuotedID)
	}
	if payload.Message.Ephemeral != 604800 {
		t.Fatalf("the disappearing timer is %d, want the chat's own", payload.Message.Ephemeral)
	}
	want := protocol.Address{Kind: protocol.AddressPhone, ID: "5511999990001"}
	if len(payload.Message.Mentions) != 1 || payload.Message.Mentions[0] != want {
		t.Fatalf("the mentions are %+v, want %+v", payload.Message.Mentions, want)
	}
}

// The two events are a pair and the order is the whole of it: the client looks the
// message up to flag its bubble, and a failure that arrives first names a message
// nobody has stored.
func TestAFileThatIsNotComingIsAnnouncedAfterTheMessageItBelongsTo(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer(nil, wm.ErrMediaDownloadFailedWith404)

	event := mediaEvent("3EB0GONE", &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Mimetype: proto.String("image/jpeg"), Caption: proto.String("olha isso"),
		DirectPath: proto.String(directPath), MediaKey: []byte("key"), FileEncSHA256: encSHA256(),
	}})

	emissions, acknowledged := deliver(t, session, event, 2)
	if !acknowledged {
		t.Fatal("a message whose file will never arrive was left for WhatsApp to redeliver forever")
	}
	if emissions[0].Type != protocol.EventMessageReceived {
		t.Fatalf("the first event is %s, want the message itself", emissions[0].Type)
	}
	if emissions[1].Type != protocol.EventMediaDownloadFailed {
		t.Fatalf("the second event is %s, want the failure", emissions[1].Type)
	}

	// The bubble still renders: the caption reads and the file is marked unavailable,
	// which is worth more to an agent than a message that never arrives.
	content := mediaContentOf(t, emissions[0])
	if content.Ref != nil {
		t.Fatalf("a message whose download failed carries a reference anyway: %+v", content.Ref)
	}
	if content.Caption != "olha isso" {
		t.Fatalf("the caption is %q, want the one WhatsApp sent", content.Caption)
	}

	var failure protocol.MediaDownloadFailure
	if err := json.Unmarshal(emissions[1].Payload, &failure); err != nil {
		t.Fatalf("decode the failure: %v", err)
	}
	if failure.MessageID != "3EB0GONE" || failure.Reason != reasonMediaExpired {
		t.Fatalf("the failure is %+v, want the message it belongs to and why", failure)
	}
	if failure.Chat.ID != "5511999990001" {
		t.Fatalf("the failure names chat %+v, want the one the message is in", failure.Chat)
	}

	validateAgainstContract(t, "event_media_download_failed", emissions[1].Payload)
}

// The split this whole path turns on. What WhatsApp answers the same way every time is
// announced and acknowledged; what may work on the next attempt is left on the phone.
func TestADownloadIsOnlyGivenUpOnWhenAnotherAttemptWouldFailTheSameWay(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"the file is past its life on WhatsApp's servers", wm.ErrMediaDownloadFailedWith410, reasonMediaExpired},
		{"the key that decrypts it has lapsed", wm.ErrMediaDownloadFailedWith403, reasonMediaExpired},
		{"the bytes are not the ones the message describes", wm.ErrInvalidMediaSHA256, reasonCorrupt},
		{"the ciphertext does not authenticate", wm.ErrInvalidMediaHMAC, reasonCorrupt},
		{"the message names no file to fetch", wm.ErrNoURLPresent, reasonUnreferenced},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, downloads := mediaSession(t, media.Options{})
			downloads.answer(nil, tc.err)

			emissions, acknowledged := deliver(t, session, imageEvent("3EB0PERM"), 2)
			if !acknowledged {
				t.Fatal("a download that will fail the same way forever was left to be redelivered")
			}
			var failure protocol.MediaDownloadFailure
			if err := json.Unmarshal(emissions[1].Payload, &failure); err != nil {
				t.Fatalf("decode the failure: %v", err)
			}
			if failure.Reason != tc.want {
				t.Fatalf("the reason is %q, want %q", failure.Reason, tc.want)
			}
		})
	}
}

func TestADownloadThatMayWorkNextTimeLeavesTheMessageOnThePhone(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"the network was down", errors.New("dial tcp: connection refused")},
		{"the deadline ran out", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, downloads := mediaSession(t, media.Options{})
			downloads.answer(nil, tc.err)

			acknowledged := make(chan bool, 1)
			go func() { acknowledged <- session.receive(imageEvent("3EB0AGAIN")) }()

			select {
			case emission := <-session.Events():
				t.Fatalf("a message whose file may arrive next time was published as %s", emission.Type)
			case got := <-acknowledged:
				if got {
					t.Fatal("WhatsApp was told the account has a message that reached no client")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("the handler never came back")
			}
		})
	}
}

// The cap is checked against the sender's claim before a byte moves, because the claim
// is the only measure there is that early and a file that says it is a gigabyte is one
// nothing here would keep.
func TestAFileTheSenderSaysIsTooBigIsRefusedBeforeItIsDownloaded(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{MaxBlob: 1024, Quota: 1 << 20})
	downloads.answer([]byte("nunca pedido"), nil)

	// Well formed, so what refuses it is the size and not the check in front of it.
	event := mediaEvent("3EB0HUGE", &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
		Mimetype: proto.String("video/mp4"), FileLength: proto.Uint64(1 << 30),
		DirectPath: proto.String(directPath), MediaKey: []byte("key"), FileEncSHA256: encSHA256(),
	}})

	emissions, acknowledged := deliver(t, session, event, 2)
	if !acknowledged {
		t.Fatal("a file this instance will never keep was left to be redelivered forever")
	}
	if downloads.count() != 0 {
		t.Fatalf("the file was downloaded %d times before being refused for its size", downloads.count())
	}
	assertFailure(t, emissions[1], reasonTooLarge)
}

// And the other half: a sender that understated the length gets as far as the store,
// which measures the bytes rather than believing them.
func TestAFileThatRunsPastTheCapOnTheWayInIsRefusedByTheStore(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{MaxBlob: 8, Quota: 1 << 20})
	downloads.answer([]byte("muito mais do que oito bytes"), nil)

	event := mediaEvent("3EB0LIED", &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Mimetype: proto.String("image/jpeg"), FileLength: proto.Uint64(4),
		DirectPath: proto.String(directPath), MediaKey: []byte("key"), FileEncSHA256: encSHA256(),
	}})

	emissions, acknowledged := deliver(t, session, event, 2)
	if !acknowledged {
		t.Fatal("a file past the cap was left to be redelivered forever")
	}
	if downloads.count() != 1 {
		t.Fatalf("the download ran %d times, want the one attempt the claim allowed", downloads.count())
	}
	assertFailure(t, emissions[1], reasonTooLarge)
}

// A store that could not take this file may take the next one, so the message is worth
// another go rather than a bubble that says the file is gone for good.
func TestAStoreThatFailedThisTimeLeavesTheMessageOnThePhone(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer([]byte("bytes"), nil)
	session.blobs = failingBlobs{errors.New("no space left on device")}

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(imageEvent("3EB0DISK")) }()

	select {
	case emission := <-session.Events():
		t.Fatalf("a message whose file the store may take next time was published as %s", emission.Type)
	case got := <-acknowledged:
		if got {
			t.Fatal("WhatsApp was told the account has a message that reached no client")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never came back")
	}
}

// An instance given no media root publishes the message and says the file is not
// coming. The alternative is an empty bubble an agent cannot tell from a message that
// really had nothing in it.
func TestAnInstanceWithNowhereToPutAFileSaysSoRatherThanPublishingAnEmptyBubble(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")

	emissions, acknowledged := deliver(t, session, imageEvent("3EB0NOROOT"), 2)
	if !acknowledged {
		t.Fatal("an instance with no media root left the message to be redelivered forever")
	}
	if content := mediaContentOf(t, emissions[0]); content.Ref != nil {
		t.Fatalf("an instance with nowhere to write handed out a reference: %+v", content.Ref)
	}
	assertFailure(t, emissions[1], reasonNoStore)
}

// Metadata that can never describe a file is the one shape where "try again" is a loop:
// whatsmeow answers both of these with a plain error rather than a sentinel, so without
// the check the message is withheld and WhatsApp redelivers exactly the same metadata,
// for good.
func TestMetadataThatCanNeverDescribeAFileIsGivenUpOnRatherThanRetriedForever(t *testing.T) {
	t.Parallel()

	// The path is pasted into a URL under whichever media host answers, so one that is
	// not a path names nothing there. whatsmeow refuses it before it dials.
	digest := make([]byte, 32)
	for _, tc := range []struct {
		name  string
		image *waE2E.ImageMessage
		want  string
	}{
		{
			name: "a direct path that is not a path",
			image: &waE2E.ImageMessage{
				Mimetype: proto.String("image/jpeg"), DirectPath: proto.String("v/t62.7118-24/nope"),
				MediaKey: []byte("key"), FileEncSHA256: digest,
			},
			want: reasonUnreferenced,
		},
		{
			name: "a ciphertext digest that is not a SHA-256",
			image: &waE2E.ImageMessage{
				Mimetype: proto.String("image/jpeg"), DirectPath: proto.String(directPath),
				MediaKey: []byte("key"), FileEncSHA256: []byte("too short"),
			},
			want: reasonCorrupt,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, downloads := mediaSession(t, media.Options{})
			downloads.answer([]byte("nunca pedido"), nil)

			emissions, acknowledged := deliver(t, session,
				mediaEvent("3EB0BADMETA", &waE2E.Message{ImageMessage: tc.image}), 2)
			if !acknowledged {
				t.Fatal("a message that can only ever fail the same way was left to be redelivered forever")
			}
			if downloads.count() != 0 {
				t.Fatalf("metadata that describes no file still cost %d downloads", downloads.count())
			}
			assertFailure(t, emissions[1], tc.want)
		})
	}
}

// And the control: metadata that is well formed is still handed to whatsmeow, so the
// check refuses what cannot work rather than standing between every message and its file.
func TestWellFormedMetadataIsStillDownloaded(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer([]byte("guardado"), nil)

	event := mediaEvent("3EB0OK", &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Mimetype: proto.String("image/jpeg"), DirectPath: proto.String(directPath),
		MediaKey: []byte("key"), FileEncSHA256: encSHA256(),
	}})

	emissions, acknowledged := deliver(t, session, event, 1)
	if !acknowledged || downloads.count() != 1 {
		t.Fatalf("a well formed message was downloaded %d times and acknowledged %v",
			downloads.count(), acknowledged)
	}
	if content := mediaContentOf(t, emissions[0]); content.Ref == nil {
		t.Fatal("a well formed message was published with no file to fetch")
	}
}

// A file sent to be seen once is not kept. A blob is served for as long as anybody
// keeps asking for it, so storing one turns something the sender expected to disappear
// into something the account holds indefinitely.
func TestAFileSentToBeSeenOnceIsNeverKept(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		build func() *waEvents.Message
	}{
		{
			// whatsmeow unwraps the three view-once envelopes before a handler sees the
			// message, and the flag on the event is all that is left of one.
			name: "unwrapped from a view-once envelope",
			build: func() *waEvents.Message {
				event := mediaEvent("3EB0ONCE", &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
					Mimetype: proto.String("image/jpeg"), JPEGThumbnail: []byte{0xFF, 0xD8, 0xFF, 0xE0},
				}})
				event.IsViewOnce = true
				return event
			},
		},
		{
			name: "an image carrying the sender's own flag",
			build: func() *waEvents.Message {
				return mediaEvent("3EB0ONCE", &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
					Mimetype: proto.String("image/jpeg"), ViewOnce: proto.Bool(true),
					JPEGThumbnail: []byte{0xFF, 0xD8, 0xFF, 0xE0},
				}})
			},
		},
		{
			name: "a video carrying the sender's own flag",
			build: func() *waEvents.Message {
				return mediaEvent("3EB0ONCE", &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
					Mimetype: proto.String("video/mp4"), ViewOnce: proto.Bool(true),
					JPEGThumbnail: []byte{0xFF, 0xD8, 0xFF, 0xE0},
				}})
			},
		},
		{
			name: "a voice note carrying the sender's own flag",
			build: func() *waEvents.Message {
				return mediaEvent("3EB0ONCE", &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
					Mimetype: proto.String("audio/ogg"), PTT: proto.Bool(true),
					ViewOnce: proto.Bool(true),
				}})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, downloads := mediaSession(t, media.Options{})
			downloads.answer([]byte("nunca guardado"), nil)

			emissions, acknowledged := deliver(t, session, tc.build(), 2)
			if !acknowledged {
				t.Fatal("a view-once message was left for WhatsApp to redeliver for good")
			}
			if downloads.count() != 0 {
				t.Fatalf("a file the sender meant to be seen once was downloaded %d times", downloads.count())
			}
			content := mediaContentOf(t, emissions[0])
			if content.Ref != nil {
				t.Fatalf("a view-once file was handed out as %+v", content.Ref)
			}
			// The preview is the same picture at a lower resolution, and it travels
			// inside the event: leaving it on keeps in Redis and in front of every
			// client exactly what not storing the file was for.
			if content.Thumbnail != "" {
				t.Fatalf("a view-once message carried a preview of what it was not supposed to keep: %q",
					content.Thumbnail)
			}
			assertFailure(t, emissions[1], reasonViewOnce)
		})
	}
}

// And the other side of it: an ordinary file with the flag absent is kept as before, so
// the check is on the flag rather than on the media type carrying it.
func TestAnOrdinaryFileIsStillKeptWhenTheViewOnceFlagIsAbsent(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer([]byte("guardado"), nil)

	event := mediaEvent("3EB0KEPT", &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Mimetype: proto.String("image/jpeg"), ViewOnce: proto.Bool(false),
		DirectPath: proto.String(directPath), MediaKey: []byte("key"), FileEncSHA256: encSHA256(),
	}})

	emissions, acknowledged := deliver(t, session, event, 1)
	if !acknowledged {
		t.Fatal("an ordinary image was left unacknowledged")
	}
	if content := mediaContentOf(t, emissions[0]); content.Ref == nil {
		t.Fatal("an ordinary image was published with no file to fetch")
	}
}

// Rendering the body is the last thing that happens, and it has to stay that way: a
// message the envelope around it refuses must not cost a download first, both for the
// transfer and for the ninety seconds it would spend not answering whatsmeow.
func TestAMessageThisBuildRefusesIsNeverDownloadedFirst(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer([]byte("bytes"), nil)

	event := imageEvent("3EB0EDIT")
	event.IsEdit = true

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(event) }()

	select {
	case got := <-acknowledged:
		if got {
			t.Fatal("an edit was acknowledged as a message received")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never came back")
	}
	if downloads.count() != 0 {
		t.Fatalf("a message that was going to be refused cost %d downloads", downloads.count())
	}
}

// A reference is published after the message it belongs to has been acknowledged, so an
// address a client cannot fetch from costs the file rather than an error somebody sees.
// It is checked once, where an operator finds out from a container that will not start.
func TestAnEngineIsRefusedWhenBlobsCouldNotBeFetchedFromWhereItWouldPublishThem(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	blobs, err := media.New(media.Options{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = blobs.Close() })

	for _, tc := range []struct {
		name string
		base string
	}{
		{"nothing at all", ""},
		// The ordinary way to get this wrong. url.Parse reads it as the scheme
		// `connector` with an opaque body rather than refusing it, so nothing downstream
		// would notice.
		{"the scheme left off", "connector:8080"},
		{"a scheme nothing fetches over", "redis://connector:8080"},
		{"no host to reach", "http:///media"},
		// The id is appended as a path segment, so anything after it would end up in
		// front of the query rather than behind it.
		{"a query the id would be appended in front of", "http://connector:8080/?token=x"},
		{"a fragment", "http://connector:8080/#here"},
		// What a listener reads as any free port, which is not one anybody can be told
		// to come back to.
		{"the port a listener picks for itself", "http://connector:0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opts := Options{Media: MediaOptions{Blobs: blobs, BaseURL: tc.base}}
			if _, err := New(container, opts, zerolog.Nop()); err == nil {
				t.Fatalf("an engine publishing blobs under %q was built anyway", tc.base)
			}
		})
	}
}

// And the addresses a deployment really uses are taken, including one advertised under a
// path of its own, which is how an instance sits behind somebody else's host.
func TestAnEngineTakesTheAddressesBlobsCanBeFetchedFrom(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	blobs, err := media.New(media.Options{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = blobs.Close() })

	for _, base := range []string{
		"http://connector-a1b2c3:8080",
		"https://connector.internal",
		"http://gateway.internal/connector-a1b2c3",
		"http://connector-a1b2c3:8080/",
	} {
		t.Run(base, func(t *testing.T) {
			t.Parallel()

			opts := Options{Media: MediaOptions{Blobs: blobs, BaseURL: base}}
			waEngine, err := New(container, opts, zerolog.Nop())
			if err != nil {
				t.Fatalf("an address a client can fetch from was refused: %v", err)
			}
			if err := waEngine.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

// mediaSession is a session that can keep a file: a real store on a directory of its
// own, and a download the test answers for. The store is the real one because the
// reference a client is handed is the store's own accounting, and a stub would prove
// nothing about it.
func mediaSession(t *testing.T, opts media.Options) (*Session, *downloads) {
	t.Helper()

	session, _ := newTestSession(t, "5511999990001")
	opts.Root = t.TempDir()
	opts.Now = func() time.Time { return storedAt }
	blobs, err := media.New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = blobs.Close() })

	handing := &downloads{}
	session.blobs = blobs
	session.blobBase = "http://connector-a1b2c3:8080"
	session.download = handing.hand
	return session, handing
}

// downloads stands in for the socket: it answers with what the test set and counts what
// it was asked for, which is how the tests that turn on a download not happening say so.
type downloads struct {
	mu    sync.Mutex
	calls int
	bytes []byte
	err   error
}

func (d *downloads) answer(file []byte, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bytes, d.err = file, err
}

func (d *downloads) hand(_ context.Context, _ *wm.Client, _ wm.DownloadableMessage) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	return d.bytes, d.err
}

func (d *downloads) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

// failingBlobs is a store that will not take anything, which no real filesystem can be
// asked to be on demand.
type failingBlobs struct{ err error }

func (f failingBlobs) Put(context.Context, io.Reader, *media.Blob) (media.Blob, error) {
	return media.Blob{}, f.err
}
func (f failingBlobs) MaxBlob() int64     { return media.DefaultMaxBlob }
func (f failingBlobs) TTL() time.Duration { return media.DefaultTTL }

// imageEvent is the plainest media message there is, for the tests that are about what
// happens to the file rather than about how the message is rendered.
func imageEvent(id string) *waEvents.Message {
	return mediaEvent(id, &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Mimetype:   proto.String("image/jpeg"),
		DirectPath: proto.String(directPath), MediaKey: []byte("key"), FileEncSHA256: encSHA256(),
	}})
}

// mediaEvent puts a media body in the envelope textMessage builds, so a test varies the
// file and nothing else.
func mediaEvent(id string, content *waE2E.Message) *waEvents.Message {
	event := textMessage(id, "")
	event.Message = content
	return event
}

// deliver runs one message through the session, settles every event it produces as
// published, and returns them with the acknowledgement the session decided on.
//
// Asking for a count is what makes it discriminating in both directions: an event that
// does not come times out, and one that comes and was not expected leaves the handler
// waiting on a settle nobody gives it.
func deliver(t *testing.T, session *Session, event *waEvents.Message, count int) ([]engine.Emission, bool) {
	t.Helper()

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- session.receive(event) }()

	emissions := make([]engine.Emission, 0, count)
	for range count {
		emission := next(t, session)
		emission.Settle(nil)
		emissions = append(emissions, emission)
	}
	select {
	case got := <-acknowledged:
		return emissions, got
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never came back from the message it published")
		return nil, false
	}
}

// mediaContentOf reads the media content back off a published message, the way a client
// decoding the event does.
func mediaContentOf(t *testing.T, emission engine.Emission) protocol.MediaContent {
	t.Helper()

	var payload struct {
		Message struct {
			Content protocol.MediaContent `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(emission.Payload, &payload); err != nil {
		t.Fatalf("decode the message: %v", err)
	}
	if payload.Message.Content.Type != "media" {
		t.Fatalf("the content is a %q, want media", payload.Message.Content.Type)
	}
	return payload.Message.Content
}

func assertFailure(t *testing.T, emission engine.Emission, reason string) {
	t.Helper()

	if emission.Type != protocol.EventMediaDownloadFailed {
		t.Fatalf("the event is %s, want the failure", emission.Type)
	}
	var failure protocol.MediaDownloadFailure
	if err := json.Unmarshal(emission.Payload, &failure); err != nil {
		t.Fatalf("decode the failure: %v", err)
	}
	if failure.Reason != reason {
		t.Fatalf("the reason is %q, want %q", failure.Reason, reason)
	}
}
