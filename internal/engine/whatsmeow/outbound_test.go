package whatsmeow

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// A pin on a map is the whole body, and the fields beside the coordinates are what a
// recipient actually reads: a name and a street rather than two numbers.
func TestALocationGoesOutWithWhatNamesThePlace(t *testing.T) {
	t.Parallel()

	req := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"location","latitude":-25.4284,"longitude":-49.2733,
		"name":"Escritório","address":"Rua XV de Novembro, 1"}}`)

	message, plan, err := planBody(req, nil, 1<<20)
	if err != nil {
		t.Fatalf("planBody: %v", err)
	}
	if plan != nil {
		t.Fatal("a location asked for a fetch, and there is no file in one")
	}
	pin := message.GetLocationMessage()
	if pin.GetDegreesLatitude() != -25.4284 || pin.GetDegreesLongitude() != -49.2733 {
		t.Fatalf("the pin is at %v,%v", pin.GetDegreesLatitude(), pin.GetDegreesLongitude())
	}
	if pin.GetName() != "Escritório" {
		t.Fatalf("the pin is named %q", pin.GetName())
	}
	if pin.GetAddress() != "Rua XV de Novembro, 1" {
		t.Fatalf("the pin's address is %q", pin.GetAddress())
	}
}

// A live location is a message WhatsApp expects to keep moving, and nothing here moves
// one. Sent as a static pin it would say it is sharing until it silently expires, which
// is a lie the caller cannot see; refused, the caller can send an ordinary pin instead.
func TestALiveLocationIsRefusedRatherThanSentAsAStaticOne(t *testing.T) {
	t.Parallel()

	req := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"location","latitude":-25.4284,"longitude":-49.2733,"live":true}}`)

	_, _, err := planBody(req, nil, 1<<20)
	assertCode(t, err, protocol.ErrorUnsupported)
}

// Coordinates off the globe are the caller's bug. WhatsApp takes them and the recipient
// gets a pin in the sea, which nobody reports as a bug against this connector.
func TestCoordinatesOffTheGlobeAreRefused(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, payload string }{
		{"a latitude past the pole", `{"type":"location","latitude":91,"longitude":0}`},
		{"a latitude past the other pole", `{"type":"location","latitude":-91,"longitude":0}`},
		{"a longitude past the date line", `{"type":"location","latitude":0,"longitude":181}`},
		{"a longitude past the other side", `{"type":"location","latitude":0,"longitude":-181}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
				"content":`+tc.payload+`}`)
			_, _, err := planBody(req, nil, 1<<20)
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// WhatsApp has two shapes for contacts and they are not interchangeable. A single card
// sent as an array renders as a list of one on some clients and as nothing on others.
func TestOneContactGoesAsACardAndSeveralGoAsAStack(t *testing.T) {
	t.Parallel()

	one := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"contacts","contacts":[{"display_name":"Ana","phone":"5511999990002"}]}}`)
	message, _, err := planBody(one, nil, 1<<20)
	if err != nil {
		t.Fatalf("planBody: %v", err)
	}
	if message.GetContactMessage() == nil {
		t.Fatalf("one contact went out as %T", message.GetContactsArrayMessage())
	}
	if message.GetContactsArrayMessage() != nil {
		t.Fatal("one contact went out as a stack as well")
	}

	two := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"contacts","contacts":[
			{"display_name":"Ana","phone":"5511999990002"},
			{"display_name":"Bruno","phone":"5511999990003"}]}}`)
	message, _, err = planBody(two, nil, 1<<20)
	if err != nil {
		t.Fatalf("planBody: %v", err)
	}
	stack := message.GetContactsArrayMessage()
	if stack == nil {
		t.Fatal("two contacts did not go out as a stack")
	}
	if len(stack.GetContacts()) != 2 {
		t.Fatalf("the stack carries %d card(s)", len(stack.GetContacts()))
	}
	if message.GetContactMessage() != nil {
		t.Fatal("two contacts went out as a single card as well")
	}
}

// A caller that already has a card sends it as it is: a vCard may carry several numbers,
// an email and a company, none of which the contract's three fields can express, so
// rewriting it here would quietly drop whatever is not in them.
func TestACallersOwnCardIsSentUntouched(t *testing.T) {
	t.Parallel()

	card := "BEGIN:VCARD\nVERSION:3.0\nFN:Ana Souza\nORG:fazer.ai\n" +
		"TEL;type=CELL;waid=5511999990002:+5511999990002\nEMAIL:ana@example.com\nEND:VCARD\n"
	req := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"contacts","contacts":[{"display_name":"Ana","vcard":`+
		mustJSON(t, card)+`}]}}`)

	message, _, err := planBody(req, nil, 1<<20)
	if err != nil {
		t.Fatalf("planBody: %v", err)
	}
	if got := message.GetContactMessage().GetVcard(); got != card {
		t.Fatalf("the card went out as %q", got)
	}
}

// The simple case is a name and a number, and it is the one a client actually has. The
// card written for it has to be one the recipient can act on: without the waid parameter
// WhatsApp shows a number to copy instead of a chat to open.
func TestACardIsWrittenForANameAndANumber(t *testing.T) {
	t.Parallel()

	req := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"contacts","contacts":[{"display_name":"Ana","phone":"+5511999990002"}]}}`)

	message, _, err := planBody(req, nil, 1<<20)
	if err != nil {
		t.Fatalf("planBody: %v", err)
	}
	card := message.GetContactMessage()
	if card.GetDisplayName() != "Ana" {
		t.Fatalf("the card is labelled %q", card.GetDisplayName())
	}
	for _, want := range []string{
		"BEGIN:VCARD", "VERSION:3.0", "FN:Ana",
		"waid=5511999990002:+5511999990002", "END:VCARD",
	} {
		if !strings.Contains(card.GetVcard(), want) {
			t.Fatalf("the card has no %q in it:\n%s", want, card.GetVcard())
		}
	}
	if strings.Contains(card.GetVcard(), "waid=+") {
		t.Fatalf("the waid kept the plus a caller wrote:\n%s", card.GetVcard())
	}
}

// A vCard reads semicolons and commas as structure. A name carrying one, and people's
// names do, splits into fields the recipient's client renders as parts of a name.
func TestANameIsEscapedIntoTheCardRatherThanSplittingIt(t *testing.T) {
	t.Parallel()

	written := vcardOf("Souza; Ana, Dra.", "5511999990002")
	if !strings.Contains(written, `FN:Souza\; Ana\, Dra.`) {
		t.Fatalf("the name went in unescaped:\n%s", written)
	}
	if strings.Count(written, "\nFN:") != 1 {
		t.Fatalf("the card has more than one FN line:\n%s", written)
	}
}

// WhatsApp shows the display name, not the card, so a card with nothing to label it
// renders as an empty row. The name is read off the card when the caller did not repeat
// it, and a card with neither is refused rather than sent blank.
func TestACardWithoutALabelTakesOneOffTheCardOrIsRefused(t *testing.T) {
	t.Parallel()

	found, err := contactCard(&outboundContact{
		Vcard: "BEGIN:VCARD\r\nVERSION:3.0\r\nFN;CHARSET=UTF-8:Ana Souza\r\nEND:VCARD\r\n",
	})
	if err != nil {
		t.Fatalf("contactCard: %v", err)
	}
	if found.GetDisplayName() != "Ana Souza" {
		t.Fatalf("the card is labelled %q, and FN says otherwise", found.GetDisplayName())
	}

	_, err = contactCard(&outboundContact{Vcard: "BEGIN:VCARD\nVERSION:3.0\nEND:VCARD\n"})
	assertCode(t, err, protocol.ErrorInvalidPayload)
}

// Each of these is a contacts body that must not go out as something emptier.
func TestAContactsBodyWithNothingToSendIsRefused(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, contacts string }{
		{"no contacts at all", `[]`},
		{"a card with neither a vcard nor a number", `[{"display_name":"Ana"}]`},
		{"a number with nobody attached to it", `[{"phone":"5511999990002"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
				"content":{"type":"contacts","contacts":`+tc.contacts+`}}`)
			_, _, err := planBody(req, nil, 1<<20)
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// A quote, a mention and the chat's disappearing-message timer belong to the message
// rather than to what is in it, so every body has to carry them. A body that drops the
// timer is the one message in a chat on one that stays behind after the rest has gone.
func TestEveryBodyCarriesWhatRidesAlongWithTheMessage(t *testing.T) {
	t.Parallel()

	alongside := &waE2E.ContextInfo{StanzaID: proto.String("3EB0ORIGINAL"), Expiration: proto.Uint32(86400)}
	for _, tc := range []struct {
		name    string
		content string
		read    func(*waE2E.Message) *waE2E.ContextInfo
	}{
		{"text", `{"type":"text","body":"oi"}`, func(m *waE2E.Message) *waE2E.ContextInfo {
			return m.GetExtendedTextMessage().GetContextInfo()
		}},
		{"location", `{"type":"location","latitude":-25.4,"longitude":-49.2}`, func(m *waE2E.Message) *waE2E.ContextInfo {
			return m.GetLocationMessage().GetContextInfo()
		}},
		{"one contact", `{"type":"contacts","contacts":[{"display_name":"Ana","phone":"5511999990002"}]}`,
			func(m *waE2E.Message) *waE2E.ContextInfo { return m.GetContactMessage().GetContextInfo() }},
		{"several contacts", `{"type":"contacts","contacts":[{"display_name":"Ana","phone":"5511999990002"},
			{"display_name":"Bruno","phone":"5511999990003"}]}`,
			func(m *waE2E.Message) *waE2E.ContextInfo { return m.GetContactsArrayMessage().GetContextInfo() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
				"content":`+tc.content+`}`)
			message, _, err := planBody(req, alongside, 1<<20)
			if err != nil {
				t.Fatalf("planBody: %v", err)
			}
			carried := tc.read(message)
			if carried.GetStanzaID() != "3EB0ORIGINAL" {
				t.Fatalf("the quote did not travel with a %s body", tc.name)
			}
			if carried.GetExpiration() != 86400 {
				t.Fatalf("the disappearing-message timer did not travel with a %s body", tc.name)
			}
		})
	}
}

// The five kinds are separate types on WhatsApp's side, and the fields they do not share
// are the ones a recipient reads: a document has its name, a voice note its flag and its
// length, a sticker neither.
func TestEachKindOfFileGoesOutAsItsOwnShape(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		content string
		check   func(*testing.T, *waE2E.Message)
	}{
		{"an image with a caption", `{"type":"media","kind":"image","mime":"image/jpeg","caption":"olha isso",
			"ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}`,
			func(t *testing.T, m *waE2E.Message) {
				image := m.GetImageMessage()
				if image == nil {
					t.Fatal("an image did not go out as one")
				}
				if image.GetCaption() != "olha isso" {
					t.Fatalf("the caption is %q", image.GetCaption())
				}
				if image.GetMimetype() != "image/jpeg" {
					t.Fatalf("the mime is %q", image.GetMimetype())
				}
			}},
		{"a video with a duration", `{"type":"media","kind":"video","mime":"video/mp4","duration":42,
			"ref":{"kind":"url","url":"http://rails:3000/blob.mp4"}}`,
			func(t *testing.T, m *waE2E.Message) {
				if m.GetVideoMessage().GetSeconds() != 42 {
					t.Fatalf("the video runs for %ds", m.GetVideoMessage().GetSeconds())
				}
			}},
		{"a voice note", `{"type":"media","kind":"audio","mime":"audio/ogg; codecs=opus","voice_note":true,
			"duration":5,"ref":{"kind":"url","url":"http://rails:3000/blob.ogg"}}`,
			func(t *testing.T, m *waE2E.Message) {
				audio := m.GetAudioMessage()
				if !audio.GetPTT() {
					t.Fatal("a voice note went out as an ordinary audio file")
				}
				if audio.GetSeconds() != 5 {
					t.Fatalf("the voice note runs for %ds", audio.GetSeconds())
				}
			}},
		{"an ordinary audio file", `{"type":"media","kind":"audio","mime":"audio/mpeg",
			"ref":{"kind":"url","url":"http://rails:3000/blob.mp3"}}`,
			func(t *testing.T, m *waE2E.Message) {
				// Absent rather than false: every voice note this connector recognises on
				// the way in is recognised by the field being there at all.
				if m.GetAudioMessage().PTT != nil {
					t.Fatal("an ordinary audio file carries the voice-note flag")
				}
			}},
		{"a document", `{"type":"media","kind":"document","mime":"application/pdf","filename":"proposta.pdf",
			"caption":"segue","ref":{"kind":"url","url":"http://rails:3000/blob.pdf"}}`,
			func(t *testing.T, m *waE2E.Message) {
				document := m.GetDocumentMessage()
				if document.GetFileName() != "proposta.pdf" {
					t.Fatalf("the document is named %q", document.GetFileName())
				}
				if document.GetCaption() != "segue" {
					t.Fatalf("the document's caption is %q", document.GetCaption())
				}
			}},
		{"a sticker", `{"type":"media","kind":"sticker","mime":"image/webp",
			"ref":{"kind":"url","url":"http://rails:3000/blob.webp"}}`,
			func(t *testing.T, m *waE2E.Message) {
				if m.GetStickerMessage() == nil {
					t.Fatal("a sticker did not go out as one")
				}
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, serving, _ := outboundSession(t)
			serving.answer([]byte("os bytes do arquivo"), "")
			message := mustSendBody(t, session, `{"message_id":"3EB0",
				"to":{"kind":"phone","id":"5511999990001"},"content":`+tc.content+`}`)
			tc.check(t, message)
		})
	}
}

// What was uploaded is the only way the recipient can fetch anything: the message carries
// the address and the key WhatsApp answered with, and a field left off is a bubble that
// never loads.
func TestTheUploadsOwnCoordinatesTravelWithTheMessage(t *testing.T) {
	t.Parallel()

	session, serving, uploads := outboundSession(t)
	serving.answer([]byte("os bytes do arquivo"), "")
	uploads.answer(&wm.UploadResponse{
		URL: "https://mmg.whatsapp.net/d/f/upload.enc", DirectPath: "/v/t62.7118-24/upload.enc",
		MediaKey: []byte("0123456789abcdef0123456789abcdef"), FileEncSHA256: []byte("enc"),
		FileSHA256: []byte("plain"), FileLength: 19,
	}, nil)

	image := mustSendBody(t, session, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"media","kind":"image","mime":"image/jpeg",
		"ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}}`).GetImageMessage()

	if image.GetURL() != "https://mmg.whatsapp.net/d/f/upload.enc" {
		t.Fatalf("the message points at %q", image.GetURL())
	}
	if image.GetDirectPath() != "/v/t62.7118-24/upload.enc" {
		t.Fatalf("the message's direct path is %q", image.GetDirectPath())
	}
	if string(image.GetMediaKey()) != "0123456789abcdef0123456789abcdef" {
		t.Fatal("the message went out without the key that decrypts it")
	}
	if image.GetFileLength() != 19 {
		t.Fatalf("the message says %d bytes", image.GetFileLength())
	}
}

// The upload key is what WhatsApp derives the encryption from, and getting it wrong does
// not fail the upload: it produces a file the recipient cannot decrypt, which reads as a
// WhatsApp problem rather than as this one.
//
// Checked against whatsmeow's own answer for each leaf type rather than against the
// constants written out here, so the mapping this file keeps cannot drift from the
// library's without the test noticing. A sticker is the one that surprises: it has no key
// of its own and travels under the image one.
func TestEachKindIsUploadedUnderTheKeyWhatsAppDecryptsItWith(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		kind protocol.MediaKind
		leaf wm.DownloadableMessage
		mime string
	}{
		{protocol.MediaImage, &waE2E.ImageMessage{}, "image/jpeg"},
		{protocol.MediaVideo, &waE2E.VideoMessage{}, "video/mp4"},
		{protocol.MediaAudio, &waE2E.AudioMessage{}, "audio/mpeg"},
		{protocol.MediaDocument, &waE2E.DocumentMessage{}, "application/pdf"},
		{protocol.MediaSticker, &waE2E.StickerMessage{}, "image/webp"},
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			t.Parallel()

			session, serving, uploads := outboundSession(t)
			serving.answer([]byte("bytes"), "")
			mustSendBody(t, session, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
				"content":{"type":"media","kind":"`+string(tc.kind)+`","mime":"`+tc.mime+`",
				"ref":{"kind":"url","url":"http://rails:3000/blob"}}}`)

			if got, want := uploads.kind(), wm.GetMediaType(tc.leaf); got != want {
				t.Fatalf("a %s was uploaded under %q and whatsmeow downloads one under %q", tc.kind, got, want)
			}
		})
	}
}

// The caller knows what it stored. The address it named may be a proxy that labels
// everything a stream of bytes, so what the server said is the fallback and the
// filename's extension the last resort.
func TestWhatTheFileIsCalledFallsBackInThatOrder(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, content, served, want string }{
		{"the caller said so", `"mime":"application/pdf","filename":"a.pdf"`, "text/html", "application/pdf"},
		{"the server said so", `"filename":"a.pdf"`, "image/png", "image/png"},
		// The parameters are part of what the file is: a voice note trimmed to
		// `audio/ogg` reaches the recipient described as something else.
		{"the server said so, with parameters", `"filename":"a.ogg"`,
			"audio/ogg; codecs=opus", "audio/ogg; codecs=opus"},
		{"only the name is left", `"filename":"proposta.pdf"`, "application/octet-stream", "application/pdf"},
		{"the generic type with parameters is still generic", `"filename":"proposta.pdf"`,
			"application/octet-stream; charset=binary", "application/pdf"},
		{"nothing said anything", `"filename":"proposta"`, "", "application/octet-stream"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, serving, _ := outboundSession(t)
			serving.answer([]byte("bytes"), tc.served)
			message := mustSendBody(t, session, `{"message_id":"3EB0",
				"to":{"kind":"phone","id":"5511999990001"},
				"content":{"type":"media","kind":"document",`+tc.content+`,
				"ref":{"kind":"url","url":"http://rails:3000/blob"}}}`)
			if got := message.GetDocumentMessage().GetMimetype(); got != tc.want {
				t.Fatalf("the file went out as %q, want %q", got, tc.want)
			}
		})
	}
}

// The preview stands in until the bytes arrive, and the field WhatsApp reads is named
// for the format. A PNG put in it renders as a broken preview on clients that take the
// field at its word.
func TestOnlyAJPEGPreviewTravelsWithTheFile(t *testing.T) {
	t.Parallel()

	session, serving, _ := outboundSession(t)
	serving.answer([]byte("bytes"), "")
	preview := base64.StdEncoding.EncodeToString([]byte("\xff\xd8\xff\xe0 jpeg-ish"))
	message := mustSendBody(t, session, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"media","kind":"image","mime":"image/jpeg",
		"thumbnail":"data:image/jpeg;base64,`+preview+`",
		"ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}}`)
	if got := string(message.GetImageMessage().GetJPEGThumbnail()); got != "\xff\xd8\xff\xe0 jpeg-ish" {
		t.Fatalf("the preview arrived as %q", got)
	}

	for _, tc := range []struct{ name, kind, thumbnail string }{
		{"a PNG where an image wants a JPEG", "image", "data:image/png;base64," + preview},
		{"a JPEG where a sticker wants a PNG", "sticker", "data:image/jpeg;base64," + preview},
		{"something that is not base64", "image", "data:image/jpeg;base64,não é base64"},
		{"a preview too big to travel in a frame", "image",
			"data:image/jpeg;base64," + strings.Repeat("A", thumbnailLimit)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
				"content":{"type":"media","kind":"`+tc.kind+`","thumbnail":`+mustJSON(t, tc.thumbnail)+`,
				"ref":{"kind":"url","url":"http://rails:3000/blob"}}}`)
			_, _, err := planBody(req, nil, 1<<20)
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// A sticker's preview is a PNG on the way in and a PNG on the way out, and it goes in the
// field named for it. Refused as a JPEG, a caller forwarding a sticker it just received
// cannot send it back; put in JPEGThumbnail, the preview never renders.
func TestAStickersPreviewIsAPNGInTheFieldNamedForIt(t *testing.T) {
	t.Parallel()

	session, serving, _ := outboundSession(t)
	serving.answer([]byte("webp"), "")
	png := base64.StdEncoding.EncodeToString([]byte("\x89PNG\r\n\x1a\n png-ish"))
	message := mustSendBody(t, session, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"media","kind":"sticker","mime":"image/webp",
		"thumbnail":"data:image/png;base64,`+png+`",
		"ref":{"kind":"url","url":"http://rails:3000/blob.webp"}}}`)

	sticker := message.GetStickerMessage()
	if got := string(sticker.GetPngThumbnail()); got != "\x89PNG\r\n\x1a\n png-ish" {
		t.Fatalf("the sticker's preview arrived as %q", got)
	}
}

// whatsmeow pads and encrypts whatever it managed to read: cbcutil.EncryptStream treats
// io.ErrUnexpectedEOF exactly like io.EOF. A body that stopped halfway therefore uploads
// as a whole file, the send reports success, and the recipient gets half an image while
// the sender is never told.
func TestAFileThatStoppedArrivingIsNotSentAsAWholeOne(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		body io.Reader
		size int64
	}{
		{"the connection broke mid-stream", iotest.TimeoutReader(strings.NewReader("os primeiros bytes e mais")), -1},
		{"the body ended early and whatsmeow read it as a clean end",
			io.MultiReader(strings.NewReader("metade do arquivo"), errorReader{io.ErrUnexpectedEOF}), -1},
		{"the source ended early against what it promised", strings.NewReader("curto demais"), 4096},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, serving, _ := outboundSession(t)
			serving.answerStream(tc.body, tc.size, "")

			_, err := session.send(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageSend,
				Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
					"content":{"type":"media","kind":"image",
					"ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}}`),
			})
			// Retryable: the same fetch may arrive whole next time, and the caller holds
			// the only copy of what it wanted to send.
			assertCode(t, err, protocol.ErrorInternal)
		})
	}
}

// Every one of these is a media send that names no file this connector can go and get.
func TestAFileThisConnectorCannotGoAndGetIsRefusedOnThePayload(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, content string }{
		{"no reference at all", `{"type":"media","kind":"image"}`},
		{"a reference with no address", `{"type":"media","kind":"image","ref":{"kind":"uazapi_message","id":"3EB0"}}`},
		{"an address this connector does not fetch over",
			`{"type":"media","kind":"image","ref":{"kind":"url","url":"file:///etc/passwd"}}`},
		{"an address with no host", `{"type":"media","kind":"image","ref":{"kind":"url","url":"http:///blob.jpg"}}`},
		{"a kind the contract does not carry",
			`{"type":"media","kind":"hologram","ref":{"kind":"url","url":"http://rails:3000/blob"}}`},
		{"a sticker with a caption", `{"type":"media","kind":"sticker","caption":"olha",
			"ref":{"kind":"url","url":"http://rails:3000/blob.webp"}}`},
		{"an audio with a caption", `{"type":"media","kind":"audio","caption":"escuta isso",
			"ref":{"kind":"url","url":"http://rails:3000/blob.ogg"}}`},
		{"a voice note with a caption", `{"type":"media","kind":"audio","voice_note":true,"caption":"escuta",
			"ref":{"kind":"url","url":"http://rails:3000/blob.ogg"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
				"content":`+tc.content+`}`)
			_, _, err := planBody(req, nil, 1<<20)
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// A `connector_blob` reference carries a URL and the header that opens it, and handing
// one back is how a caller forwards a file it received. The kind is not what decides:
// the address is.
func TestAReferenceThisConnectorIssuedCanBeSentBack(t *testing.T) {
	t.Parallel()

	session, serving, _ := outboundSession(t)
	serving.answer([]byte("os mesmos bytes"), "image/jpeg")
	mustSendBody(t, session, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"media","kind":"image","ref":{"kind":"connector_blob","id":"blob_a1",
		"url":"http://connector-a1b2c3:8080/media/blob_a1","headers":{"Authorization":"Bearer t"}}}}`)

	if got := serving.header("Authorization"); got != "Bearer t" {
		t.Fatalf("the fetch went out with Authorization %q, and without it the blob answers 401", got)
	}
}

// The caller's own claim is acted on before the transfer, the same way the sender's is
// on the way in: a file that says it is a gigabyte is one nothing here would send.
func TestAFileTheCallerSaysIsTooBigIsRefusedBeforeItIsFetched(t *testing.T) {
	t.Parallel()

	session, serving, uploads := outboundSession(t)
	session.sendLimit = 1 << 10
	serving.answer([]byte("bytes"), "")

	_, err := session.send(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend,
		Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
			"content":{"type":"media","kind":"image","size":1048576,
			"ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}}`),
	})
	assertCode(t, err, protocol.ErrorMediaTooLarge)
	if serving.count() != 0 {
		t.Fatalf("a file refused on the caller's own numbers was fetched %d time(s)", serving.count())
	}
	if uploads.count() != 0 {
		t.Fatalf("a file refused on the caller's own numbers was uploaded %d time(s)", uploads.count())
	}
}

// Content-Length is a claim, and a chunked response carries none at all. Without counting
// what actually arrives, a server that understates it streams past the cap into a
// temporary file and this instance sends whatever fits.
func TestAServerThatUnderstatesTheLengthStillCannotSendPastTheCap(t *testing.T) {
	t.Parallel()

	session, serving, _ := outboundSession(t)
	session.sendLimit = 8
	serving.answerStream(strings.NewReader(strings.Repeat("x", 4096)), -1, "")

	_, err := session.send(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend,
		Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
			"content":{"type":"media","kind":"image","ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}}`),
	})
	assertCode(t, err, protocol.ErrorMediaTooLarge)
}

// The address belongs to the caller, and what its server answers splits into things the
// caller has to fix and things worth another go. Told the wrong one, a client either
// retries a file that will never be there or gives up on one that would have arrived.
func TestWhatTheCallersOwnServerAnsweredDecidesWhetherItIsWorthAnotherGo(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		status int
		want   protocol.ErrorCode
	}{
		{"the file is not there", 404, protocol.ErrorMediaUnavailable},
		{"the file has been deleted", 410, protocol.ErrorMediaUnavailable},
		{"the reference travelled without its header", 401, protocol.ErrorMediaUnavailable},
		{"the header no longer opens it", 403, protocol.ErrorMediaUnavailable},
		{"the caller's own server is asking to be left alone", 429, protocol.ErrorRateLimited},
		{"the caller's own server timed out on its own request", 408, protocol.ErrorTimeout},
		{"the caller's own server is having a bad minute", 503, protocol.ErrorInternal},
		{"the caller's own server broke", 500, protocol.ErrorInternal},
		{"the caller asked for something the server did not understand", 400, protocol.ErrorInvalidPayload},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertCode(t, statusFailure(tc.status), tc.want)
		})
	}
}

// A payload this connector can never send is the caller's bug whatever the socket is
// doing. Answered `not_connected`, the caller waits for a connection that would not have
// helped, and the message sits in a queue forever.
func TestAMediaPayloadNobodyCouldSendIsRefusedOnADisconnectedSessionToo(t *testing.T) {
	t.Parallel()

	session, serving, _ := outboundSession(t)
	disconnect(session)

	_, err := session.send(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend,
		Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
			"content":{"type":"media","kind":"image"}}`),
	})
	assertCode(t, err, protocol.ErrorInvalidPayload)
	if serving.count() != 0 {
		t.Fatalf("a payload that was never going to be sent was fetched %d time(s)", serving.count())
	}
}

// And the other half of that order: a payload with nothing wrong with it costs no
// transfer on a session that cannot send it. The fetch and the upload are the expensive
// half, and spending them on a socket that is down uploads a file nobody receives.
func TestAFileIsNotFetchedForASessionThatCannotSendIt(t *testing.T) {
	t.Parallel()

	session, serving, uploads := outboundSession(t)
	serving.answer([]byte("bytes"), "")
	disconnect(session)

	_, err := session.send(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend,
		Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
			"content":{"type":"media","kind":"image","ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}}`),
	})
	assertCode(t, err, protocol.ErrorNotConnected)
	if serving.count() != 0 {
		t.Fatalf("a session that cannot send still fetched %d file(s)", serving.count())
	}
	if uploads.count() != 0 {
		t.Fatalf("a session that cannot send still uploaded %d file(s)", uploads.count())
	}
}

// An upload that failed sent nothing, so unlike a send there is no message in somebody's
// chat to be careful about: what matters is only whether trying again could work.
func TestAnUploadThatFailedSaysWhetherItIsWorthAnotherGo(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
		want protocol.ErrorCode
	}{
		{"the account is gone", wm.ErrNotLoggedIn, protocol.ErrorNotPaired},
		{"the socket went down mid-upload", wm.ErrNotConnected, protocol.ErrorNotConnected},
		{"WhatsApp never answered", wm.ErrIQTimedOut, protocol.ErrorTimeout},
		{"WhatsApp would not take it", errors.New("upload failed: 500"), protocol.ErrorWaError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, serving, uploads := outboundSession(t)
			serving.answer([]byte("bytes"), "")
			uploads.answer(nil, tc.err)

			_, err := session.send(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageSend,
				Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
					"content":{"type":"media","kind":"image",
					"ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}}`),
			})
			assertCode(t, err, tc.want)
		})
	}
}

// outboundSession is a paired, connected session whose fetch and upload are the test's
// to answer. Nothing here talks to a socket or to an HTTP server.
func outboundSession(t *testing.T) (*Session, *serving, *uploads) {
	t.Helper()

	session, _ := newTestSession(t, "5511999990001")
	connect(session)
	files, sent := &serving{}, &uploads{}
	session.retrieve = files.hand
	session.uploadFile = sent.hand
	return session, files, sent
}

// mustSendBody runs a send far enough to have built the body and hands back what it
// built. The send itself needs a socket, so what is asserted on is the message rather
// than the reply.
func mustSendBody(t *testing.T, session *Session, payload string) *waE2E.Message {
	t.Helper()

	req := requestOf(t, payload)
	alongside, err := contextToSend(req, session.ownJID(peerJID(t)), peerJID(t))
	if err != nil {
		t.Fatalf("contextToSend: %v", err)
	}
	message, plan, err := planBody(req, alongside, session.sendLimit)
	if err != nil {
		t.Fatalf("planBody: %v", err)
	}
	if plan == nil {
		return message
	}
	message, err = session.mediaToSend(t.Context(), req, plan, alongside)
	if err != nil {
		t.Fatalf("mediaToSend: %v", err)
	}
	return message
}

// serving stands in for the address the caller named: it answers with what the test set
// and counts what it was asked for, which is how the tests that turn on a fetch not
// happening say so.
type serving struct {
	mu      sync.Mutex
	calls   int
	headers map[string]string
	body    io.Reader
	size    int64
	mime    string
	err     error
}

func (s *serving) answer(file []byte, contentType string) {
	s.answerStream(strings.NewReader(string(file)), int64(len(file)), contentType)
}

func (s *serving) answerStream(body io.Reader, size int64, contentType string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.body, s.size, s.mime = body, size, contentType
}

func (s *serving) hand(_ context.Context, _ string, headers map[string]string) (source, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.headers = headers
	if s.err != nil {
		return source{}, s.err
	}
	body := s.body
	if body == nil {
		body = strings.NewReader("")
	}
	return source{body: io.NopCloser(body), size: s.size, mime: s.mime}, nil
}

func (s *serving) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *serving) header(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.headers[name]
}

// uploads stands in for the socket's upload, for the same reason serving stands in for
// the fetch.
type uploads struct {
	mu       sync.Mutex
	calls    int
	lastKind wm.MediaType
	answered wm.UploadResponse
	err      error
	set      bool
}

func (u *uploads) answer(response *wm.UploadResponse, err error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.err, u.set = err, true
	if response != nil {
		u.answered = *response
	}
}

func (u *uploads) hand(
	_ context.Context, _ *wm.Client, kind wm.MediaType, from io.Reader,
) (wm.UploadResponse, error) {
	// Read to the end first: the cap is enforced by the reader whatsmeow is handed, so a
	// fake that never reads is a fake where the cap never fires.
	//
	// And it ends the way cbcutil.EncryptStream ends, which is the behaviour the code
	// under test has to survive: io.ErrUnexpectedEOF is treated exactly like io.EOF, so a
	// body that stopped halfway looks here like a file that finished. A fake that
	// returned that error instead would be a fake where the check for it never fires.
	drained, readErr := io.Copy(io.Discard, from)
	if errors.Is(readErr, io.ErrUnexpectedEOF) {
		readErr = nil
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	u.calls++
	u.lastKind = kind
	if readErr != nil {
		return wm.UploadResponse{}, readErr
	}
	if u.err != nil {
		return wm.UploadResponse{}, u.err
	}
	if u.set {
		return u.answered, nil
	}
	return wm.UploadResponse{
		URL: "https://mmg.whatsapp.net/d/f/x.enc", DirectPath: "/v/t62/x.enc",
		MediaKey: []byte("k"), FileEncSHA256: []byte("e"), FileSHA256: []byte("p"),
		FileLength: uint64(drained),
	}, nil
}

func (u *uploads) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.calls
}

func (u *uploads) kind() wm.MediaType {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.lastKind
}

// requestOf decodes a payload the way send does, so the tests read as the frames the
// contract carries rather than as Go structs.
func requestOf(t *testing.T, payload string) *sendRequest {
	t.Helper()

	var req sendRequest
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		t.Fatalf("the payload in this test is not a send: %v", err)
	}
	return &req
}

func mustJSON(t *testing.T, value string) string {
	t.Helper()

	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return string(encoded)
}

// errorReader is a source that has already failed, for composing a body that stops
// partway through.
type errorReader struct{ err error }

func (e errorReader) Read([]byte) (int, error) { return 0, e.err }

func peerJID(t *testing.T) waTypes.JID {
	t.Helper()

	jid, err := waTypes.ParseJID("5511999990001@" + waTypes.DefaultUserServer)
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	return jid
}

// A file exactly the size of the cap is a file the setting says this instance sends. A
// counter that refuses at the cap instead of past it turns the documented "at most" into
// "less than", and the caller has no way to see the difference from the message.
func TestAFileExactlyTheSizeOfTheCapGoesOut(t *testing.T) {
	t.Parallel()

	const limit = 64
	for _, tc := range []struct {
		name  string
		bytes int
		sends bool
	}{
		{"one byte under the cap", limit - 1, true},
		{"exactly the cap", limit, true},
		{"one byte over it", limit + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, serving, _ := outboundSession(t)
			session.sendLimit = limit
			// Handed out a few bytes at a time, because what decides this is whether the
			// reader reports EOF alongside the last bytes or on the read after them, and
			// a single-chunk source only ever exercises one of those.
			serving.answerStream(iotest.OneByteReader(strings.NewReader(strings.Repeat("x", tc.bytes))), -1, "")

			_, err := session.send(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageSend,
				Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
					"content":{"type":"media","kind":"image",
					"ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}}`),
			})
			if tc.sends {
				// The send itself needs a socket, so getting past the upload is the
				// whole assertion: what must not happen is being refused for size.
				var coded *protocol.Error
				if errors.As(err, &coded) && coded.Code == protocol.ErrorMediaTooLarge {
					t.Fatalf("a file of %d bytes was refused against a cap of %d", tc.bytes, limit)
				}
				return
			}
			assertCode(t, err, protocol.ErrorMediaTooLarge)
		})
	}
}

// A location that names neither coordinate decodes to zero, which is a real place off
// the coast of Africa. The contract requires both, and a pin sent there reports success:
// the caller has no reason to send it again and the recipient gets somewhere nobody meant.
func TestALocationMissingACoordinateIsRefusedRatherThanSentToZero(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, content string }{
		{"neither coordinate", `{"type":"location"}`},
		{"no latitude", `{"type":"location","longitude":-49.2733}`},
		{"no longitude", `{"type":"location","latitude":-25.4284}`},
		{"both null", `{"type":"location","latitude":null,"longitude":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
				"content":`+tc.content+`}`)
			_, _, err := planBody(req, nil, 1<<20)
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}

	// And the coordinates that are a real place: zero is refused for being absent, not
	// for being zero, so a caller that means the Gulf of Guinea can still say so.
	req := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"location","latitude":0,"longitude":0}}`)
	message, _, err := planBody(req, nil, 1<<20)
	if err != nil {
		t.Fatalf("a location that names zero twice was refused: %v", err)
	}
	if message.GetLocationMessage() == nil {
		t.Fatal("a location at zero did not go out as one")
	}
}

// A newsletter takes its media unencrypted, through a different upload call, and the
// message has to carry the handle that upload answers with. Sent the ordinary way it goes
// out with coordinates nobody can resolve while the send reports success, which is the
// one outcome this connector refuses everywhere else.
func TestAFileToANewsletterIsRefusedRatherThanSentUnresolvable(t *testing.T) {
	t.Parallel()

	session, serving, uploads := outboundSession(t)
	serving.answer([]byte("bytes"), "")

	_, err := session.send(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend,
		Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"newsletter","id":"120363000000000000"},
			"content":{"type":"media","kind":"image","ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}}`),
	})
	assertCode(t, err, protocol.ErrorUnsupported)
	if serving.count() != 0 || uploads.count() != 0 {
		t.Fatalf("a file that was never going to arrive was fetched %d and uploaded %d time(s)",
			serving.count(), uploads.count())
	}

	// Everything else still reaches a newsletter: it is the file that cannot, not the
	// address.
	text := requestOf(t, `{"message_id":"3EB0","to":{"kind":"newsletter","id":"120363000000000000"},
		"content":{"type":"text","body":"oi"}}`)
	if _, _, err := planBody(text, nil, 1<<20); err != nil {
		t.Fatalf("a text to a newsletter was refused as well: %v", err)
	}
}

// A reference from the client is very often a signed URL, and the credential is the
// query. The reply is stored by the caller and read back out of its logs, so a message
// carrying the whole address writes a working credential somewhere it outlives the send.
//
// Against the real fetch rather than the fake one: what has to redact is
// retrieveOverHTTP, and a fake standing in for it would only ever prove the fake redacts.
func TestASignedAddressDoesNotTravelIntoTheFailure(t *testing.T) {
	t.Parallel()

	// A listener opened and closed, so the port is one nothing answers on and the fetch
	// fails without waiting for anything.
	var listening net.ListenConfig
	listener, err := listening.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	closed := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	signed := "http://" + closed + "/bucket/file.pdf" +
		"?X-Amz-Credential=AKIAEXAMPLE&X-Amz-Signature=deadbeefcafe&X-Amz-Expires=900"
	_, err = retrieveOverHTTP(t.Context(), signed, nil)
	if err == nil {
		t.Fatal("fetching from a port nothing answers on reported success")
	}
	for _, secret := range []string{"X-Amz-Signature", "deadbeefcafe", "AKIAEXAMPLE", "X-Amz-Credential"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("the failure carries %q from the signed address:\n%s", secret, err)
		}
	}
	// And it still says which storage was not answering, or naming it at all is
	// pointless. The path is not repeated: with Active Storage it is the credential.
	if !strings.Contains(err.Error(), "http://"+closed) {
		t.Fatalf("the failure does not say which address failed:\n%s", err)
	}
	if strings.Contains(err.Error(), "/bucket/file.pdf") {
		t.Fatalf("the failure repeated the path:\n%s", err)
	}
}

// safeAddress has to keep saying which address failed, or naming it at all is pointless.
func TestARedactedAddressStillSaysWhichOneItWas(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ raw, want string }{
		{"https://s3.example.com/bucket/file.pdf?X-Amz-Signature=secret", "https://s3.example.com"},
		// The signed blob id is in the path here, and it is a bearer token for that file.
		{"http://rails:3000/rails/active_storage/blobs/proxy/eyJfcmFpbHMiOnsibWVzc2FnZSI6/a.pdf",
			"http://rails:3000"},
		{"https://user:pass@host/file", "https://host"},
		{"https://host/file#frag", "https://host"},
		{"://nonsense", "[redacted]"},
		{"file:///etc/passwd", "file:[redacted]"},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			t.Parallel()

			got := safeAddress(tc.raw)
			if got != tc.want {
				t.Fatalf("safeAddress(%q) = %q, want %q", tc.raw, got, tc.want)
			}
			for _, secret := range []string{"secret", "pass", "eyJfcmFpbHMiOnsibWVzc2FnZSI6", "passwd"} {
				if strings.Contains(got, secret) {
					t.Fatalf("safeAddress(%q) kept %q: %q", tc.raw, secret, got)
				}
			}
		})
	}
}

// The deadline is the caller's own and it has already run out, so the caller is about to
// stop waiting whatever this answers. Reported as this connector breaking, a send that
// simply did not fit in its budget reads as a bug rather than as a budget.
func TestAFetchThatOutlivedTheDeadlineIsATimeoutRatherThanABreakage(t *testing.T) {
	t.Parallel()

	// A server that accepts the connection and then says nothing, which is the shape this
	// exists for: a refused connection fails immediately and never reaches the deadline.
	blocking := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(blocking.Close)

	expiring, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	_, err := retrieveOverHTTP(expiring, blocking.URL+"/blob.pdf", nil)
	assertCode(t, err, protocol.ErrorTimeout)
}

// A number reaches this connector however a human typed it into a CRM, and the contract's
// own fixture carries one with spaces and a hyphen. WhatsApp matches the waid against an
// account by its digits, so a card carrying the formatting renders and does nothing when
// it is tapped, which is a failure only the recipient sees.
func TestANumberIsReducedToDigitsBeforeItBecomesAWAID(t *testing.T) {
	t.Parallel()

	for _, typed := range []string{
		"+55 41 98888-1111", "+554198881111", "55 (41) 98888-1111", "  +55-41-98888-1111  ",
	} {
		t.Run(typed, func(t *testing.T) {
			t.Parallel()

			card, err := contactCard(&outboundContact{DisplayName: "Ana", Phone: typed})
			if err != nil {
				t.Fatalf("contactCard(%q): %v", typed, err)
			}
			vcard := card.GetVcard()
			for _, line := range strings.Split(vcard, "\n") {
				if !strings.HasPrefix(line, "TEL") {
					continue
				}
				if strings.ContainsAny(line, " ()-") && !strings.Contains(line, "TEL;type") {
					t.Fatalf("the TEL line kept formatting: %q", line)
				}
			}
			if !strings.Contains(vcard, "waid=554198881111:") && !strings.Contains(vcard, "waid=5541988881111:") {
				t.Fatalf("the waid is not the digits of %q:\n%s", typed, vcard)
			}
		})
	}

	// And a number that is no number at all, which would otherwise write a card whose
	// waid matches nobody.
	if _, err := contactCard(&outboundContact{DisplayName: "Ana", Phone: "sem número"}); err == nil {
		t.Fatal("a contact whose number has no digits in it was accepted")
	}
}

// net/http drops Authorization, Cookie and WWW-Authenticate of its own accord when a
// redirect leaves the host, and nothing else. A reference authenticated with a vendor's
// own header would otherwise arrive at whoever the first host chose to point at, carrying
// a credential that still works.
func TestACallersHeadersDoNotFollowARedirectOffItsOwnHost(t *testing.T) {
	t.Parallel()

	var landed http.Header
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		landed = r.Header.Clone()
		w.Header().Set("Content-Type", "application/pdf")
		_, _ = w.Write([]byte("bytes"))
	}))
	t.Cleanup(elsewhere.Close)

	var kept http.Header
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kept = r.Header.Clone()
		http.Redirect(w, r, elsewhere.URL+"/file.pdf", http.StatusFound)
	}))
	t.Cleanup(origin.Close)

	file, err := retrieveOverHTTP(t.Context(), origin.URL+"/file.pdf", map[string]string{
		"X-API-Key":     "sk_live_deadbeef",
		"Authorization": "Bearer t",
	})
	if err != nil {
		t.Fatalf("retrieveOverHTTP: %v", err)
	}
	defer func() { _ = file.body.Close() }()

	// The host the caller named gets what the caller sent, or the reference does not open.
	if kept.Get("X-API-Key") != "sk_live_deadbeef" {
		t.Fatalf("the host the caller named did not receive its own header: %v", kept)
	}
	for _, name := range []string{"X-API-Key", "Authorization"} {
		if got := landed.Get(name); got != "" {
			t.Fatalf("%s followed the redirect to another host with %q", name, got)
		}
	}
}

// A chain that does not end is the caller's address to fix, not a minute to wait out.
// Reported as retryable it is retried for as long as the caller keeps the message.
func TestAnEndlessRedirectIsTheCallersToFixRatherThanToRetry(t *testing.T) {
	t.Parallel()

	var loop *httptest.Server
	loop = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, loop.URL+"/again", http.StatusFound)
	}))
	t.Cleanup(loop.Close)

	_, err := retrieveOverHTTP(t.Context(), loop.URL+"/file.pdf", nil)
	assertCode(t, err, protocol.ErrorInvalidPayload)
}

// A header the HTTP client would refuse anyway is refused here instead. Left to the
// client, the failure is indistinguishable from a server that would not answer, so it is
// reported as worth trying again and the same reference is retried forever.
func TestAHeaderNoClientWouldSendIsRefusedOnThePayload(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, headers string }{
		{"a newline in the value", `{"X-API-Key":"sk\nX-Injected: yes"}`},
		{"a carriage return in the value", `{"X-API-Key":"sk\rmore"}`},
		{"a colon in the name", `{"X-API:Key":"sk"}`},
		{"a space in the name", `{"X API Key":"sk"}`},
		{"a name that is not there at all", `{"":"sk"}`},
		// Not obviously wrong to look at, and rejected by the transport all the same,
		// which is where the classification would go wrong.
		{"parentheses in the name", `{"X(API)":"sk"}`},
		{"an at sign in the name", `{"X@Key":"sk"}`},
		{"a comma in the name", `{"X,Key":"sk"}`},
		{"a slash in the name", `{"X/Key":"sk"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
				"content":{"type":"media","kind":"image","ref":{"kind":"url",
				"url":"http://rails:3000/blob.jpg","headers":`+tc.headers+`}}}`)
			_, _, err := planBody(req, nil, 1<<20)
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// A cap set to the largest int64 there is used to wrap the probe byte to a negative
// length, and slicing to that is a panic: an accepted configuration that took the whole
// connector down on an ordinary send.
func TestTheLargestCapThereIsDoesNotTakeTheConnectorDown(t *testing.T) {
	t.Parallel()

	session, serving, _ := outboundSession(t)
	session.sendLimit = math.MaxInt64
	serving.answer([]byte("os bytes do arquivo"), "image/jpeg")

	message := mustSendBody(t, session, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"media","kind":"image","ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}}`)
	if message.GetImageMessage() == nil {
		t.Fatal("a send under the largest cap there is did not produce an image")
	}
}

// The deadline can land before the headers arrive or while the body is being read, and it
// is the same budget running out either way. Answered differently, one half of one
// deadline reports itself as this connector breaking.
func TestADeadlineLandingMidBodyIsTheSameAnswerAsOneLandingBeforeIt(t *testing.T) {
	t.Parallel()

	session, serving, _ := outboundSession(t)
	// Headers already in, and then the body stops because the command's budget ran out.
	serving.answerStream(io.MultiReader(
		strings.NewReader("metade do arquivo"),
		errorReader{fmt.Errorf("read tcp: %w", context.DeadlineExceeded)},
	), -1, "image/jpeg")

	_, err := session.send(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend,
		Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
			"content":{"type":"media","kind":"image",
			"ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}}`),
	})
	assertCode(t, err, protocol.ErrorTimeout)
}

// Where the caller's headers stop. net/http drops Authorization, Cookie and
// WWW-Authenticate of its own accord when a redirect leaves the host, and nothing else,
// so anything a vendor authenticates with is this policy's to stop.
//
// Against the policy rather than through a pair of servers: the downgrade case needs TLS
// on one side and plaintext on the other, and reaching it through a real fetch means
// swapping the process-wide transport out from under every other test.
func TestWhereACallersHeadersStopOnARedirect(t *testing.T) {
	t.Parallel()

	const secret = "sk_live_deadbeef"
	for _, tc := range []struct {
		name    string
		from    string
		to      string
		carries bool
	}{
		{"the same origin", "https://storage.example/a", "https://storage.example/b", true},
		{"another path on the same origin", "https://storage.example/a", "https://storage.example/deep/b", true},
		{"another host", "https://storage.example/a", "https://elsewhere.example/b", false},
		{"a downgrade to plaintext on the same name", "https://storage.example/a", "http://storage.example/a", false},
		{"another port on the same name", "https://storage.example/a", "https://storage.example:8443/a", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			headers := map[string]string{"X-API-Key": secret}
			first := requestTo(t, tc.from, headers)
			hop := requestTo(t, tc.to, headers)

			if err := followingRedirects(headers)(hop, []*http.Request{first}); err != nil {
				t.Fatalf("the hop was refused: %v", err)
			}
			carried := hop.Header.Get("X-API-Key") == secret
			if carried != tc.carries {
				t.Fatalf("the credential carried=%v to %s, want %v", carried, tc.to, tc.carries)
			}
		})
	}
}

// How far the chain is followed. via holds the requests already made, so the count is off
// by one from the obvious reading, and the constant is what the refusal quotes.
func TestARedirectChainIsFollowedAsFarAsTheConstantSays(t *testing.T) {
	t.Parallel()

	policy := followingRedirects(nil)
	hop := requestTo(t, "https://storage.example/b", nil)
	for made := 1; made <= fetchRedirects; made++ {
		via := make([]*http.Request, made)
		for i := range via {
			via[i] = requestTo(t, "https://storage.example/a", nil)
		}
		if err := policy(hop, via); err != nil {
			t.Fatalf("hop %d of %d was refused: %v", made, fetchRedirects, err)
		}
	}

	via := make([]*http.Request, fetchRedirects+1)
	for i := range via {
		via[i] = requestTo(t, "https://storage.example/a", nil)
	}
	if err := policy(hop, via); !errors.Is(err, errTooManyRedirects) {
		t.Fatalf("the chain past %d hops answered %v", fetchRedirects, err)
	}
}

func requestTo(t *testing.T, address string, headers map[string]string) *http.Request {
	t.Helper()

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, address, http.NoBody)
	if err != nil {
		t.Fatalf("NewRequest(%q): %v", address, err)
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	return request
}

// The parameters are part of what the file is: `audio/ogg; codecs=opus` is what a voice
// note is, and trimmed to `audio/ogg` it reaches the recipient described as something
// else.
//
// Against the real fetch, because that is where the header is read. The fake hands back
// whatever a test set, so it would report this working whether or not anything trims.
func TestWhatTheServerCalledTheFileArrivesWithItsParameters(t *testing.T) {
	t.Parallel()

	const served = "audio/ogg; codecs=opus"
	serving := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", served)
		_, _ = w.Write([]byte("bytes"))
	}))
	t.Cleanup(serving.Close)

	file, err := retrieveOverHTTP(t.Context(), serving.URL+"/voice.ogg", nil)
	if err != nil {
		t.Fatalf("retrieveOverHTTP: %v", err)
	}
	defer func() { _ = file.body.Close() }()

	if file.mime != served {
		t.Fatalf("the server said %q and the fetch read %q", served, file.mime)
	}
}
