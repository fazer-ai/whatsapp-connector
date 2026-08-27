package whatsmeow

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"syscall"
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

// The name goes on the card as it was written. RFC 2426 says a semicolon in a text value
// must be escaped, and doing that is wrong here: the recipient's WhatsApp is what reads
// this back and it does not undo the escape, so `FN:Souza\; Ana` is shown with the
// backslash in it. Found on a real phone, not in a unit test.
func TestANameGoesOnTheCardAsItWasWritten(t *testing.T) {
	t.Parallel()

	written := vcardOf("Souza; Ana, Dra.", "5511999990002")
	if !strings.Contains(written, "FN:Souza; Ana, Dra.") {
		t.Fatalf("the name did not go in as it was written:\n%s", written)
	}
	if strings.Contains(written, `\`) {
		t.Fatalf("the card carries an escape nothing reading it will undo:\n%s", written)
	}
	if strings.Count(written, "\nFN:") != 1 {
		t.Fatalf("the card has more than one FN line:\n%s", written)
	}

	// N is positional, so the same semicolon there would add a field rather than be part
	// of one. It is dropped rather than escaped, and FN carries the real name.
	for _, line := range strings.Split(written, "\n") {
		if !strings.HasPrefix(line, "N:") {
			continue
		}
		if fields := strings.Split(line, ";"); len(fields) != 5 {
			t.Fatalf("N has %d fields rather than five: %q", len(fields), line)
		}
	}

	// A newline cannot travel: it would end the property early and everything after it
	// would be read as another line of the card.
	// Asserted on the lines rather than on the text: with the newline turned into a space
	// the name still reads `Ana BEGIN:VCARD`, and that is fine -- it is one value on one
	// line. What must not happen is a second line appearing.
	lines := strings.Split(strings.TrimSuffix(vcardOf("Ana\nBEGIN:VCARD", "5511999990002"), "\n"), "\n")
	if len(lines) != 6 {
		t.Fatalf("a name with a newline in it wrote %d lines rather than six:\n%q", len(lines), lines)
	}
	for i, line := range lines {
		if i > 0 && strings.HasPrefix(line, "BEGIN:VCARD") {
			t.Fatalf("a name with a newline in it started a second card:\n%q", lines)
		}
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
			"ref":{"kind":"url","url":"http://rails:3000/blob.pdf"}}`,
			func(t *testing.T, m *waE2E.Message) {
				document := m.GetDocumentMessage()
				if document.GetFileName() != "proposta.pdf" {
					t.Fatalf("the document is named %q", document.GetFileName())
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

	pngBytes := base64.StdEncoding.EncodeToString([]byte("\x89PNG\r\n\x1a\n png-ish"))
	for _, tc := range []struct{ name, kind, thumbnail string }{
		{"a PNG where an image wants a JPEG", "image", "data:image/png;base64," + preview},
		{"a JPEG where a sticker wants a PNG", "sticker", "data:image/jpeg;base64," + preview},
		// The label agrees with the kind and the bytes disagree with the label. The
		// field is named for a format and a recipient's client reads the field, so this
		// is the same broken preview by a longer route -- and it is the route a label
		// check cannot see.
		{"PNG bytes under a JPEG label", "image", "data:image/jpeg;base64," + pngBytes},
		{"JPEG bytes under a sticker's PNG label", "sticker", "data:image/png;base64," + preview},
		{"bytes that are no image at all", "image",
			"data:image/jpeg;base64," + base64.StdEncoding.EncodeToString([]byte("not an image"))},
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
	message, err = session.mediaToSend(t.Context(), plan, alongside)
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
		// The same origin spelled differently, which a string comparison reads as another
		// one: the caller then loses its credentials and the fetch answers 401.
		{"the same host in another case", "https://storage.example/a", "https://STORAGE.example/b", true},
		{"the scheme's own port written out", "https://storage.example/a", "https://storage.example:443/b", true},
		{"the scheme's own port written out on http", "http://storage.example/a", "http://storage.example:80/b", true},
		{"the port dropped from a plaintext address", "http://storage.example:80/a", "http://storage.example/b", true},
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

// The reference carries what the blob was stored as, and this connector puts it there
// itself. Ignored, a caller forwarding a file it received loses exactly the types that
// depend on being named: a sticker is a webp and a voice note an opus.
func TestTheReferencesOwnTypeIsUsedWhenNothingElseSaysWhatTheFileIs(t *testing.T) {
	t.Parallel()

	session, serving, _ := outboundSession(t)
	// A proxy that labels everything a stream of bytes, which is the case this exists for.
	serving.answer([]byte("webp"), "application/octet-stream")

	message := mustSendBody(t, session, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"media","kind":"sticker","ref":{"kind":"connector_blob","id":"blob_a1",
		"url":"http://connector-a1b2c3:8080/media/blob_a1","mime":"image/webp"}}}`)
	if got := message.GetStickerMessage().GetMimetype(); got != "image/webp" {
		t.Fatalf("the sticker went out as %q, and the reference said image/webp", got)
	}

	// And the caller's own word on the message still wins over it.
	session, serving, _ = outboundSession(t)
	serving.answer([]byte("bytes"), "")
	message = mustSendBody(t, session, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"media","kind":"document","mime":"application/pdf",
		"ref":{"kind":"url","url":"http://rails:3000/blob","mime":"text/plain"}}}`)
	if got := message.GetDocumentMessage().GetMimetype(); got != "application/pdf" {
		t.Fatalf("the document went out as %q, and the content said application/pdf", got)
	}
}

// A length says how much arrived and a digest says what did. Compared only by length, a
// proxy serving a stale or misrouted blob of the same size sends somebody else's file
// under this message and the command reports success.
func TestBytesThatAreNotTheOnesTheReferenceNamedAreNotSent(t *testing.T) {
	t.Parallel()

	const file = "os bytes do arquivo"
	right := sha256.Sum256([]byte(file))
	wrong := sha256.Sum256([]byte("outra coisa inteiramente"))

	send := func(t *testing.T, digest string) error {
		t.Helper()

		session, serving, uploads := outboundSession(t)
		serving.answer([]byte(file), "image/jpeg")
		// The fake answers a fixed response, so the digest it reports has to be the one
		// the bytes actually hash to or this would test the fake's opinion.
		uploads.answer(&wm.UploadResponse{
			URL: "https://mmg.whatsapp.net/d/f/x.enc", DirectPath: "/v/t62/x.enc",
			MediaKey: []byte("k"), FileEncSHA256: []byte("e"), FileSHA256: right[:],
			FileLength: uint64(len(file)),
		}, nil)

		_, err := session.send(t.Context(), &protocol.Command{
			Type: protocol.CommandMessageSend,
			Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
				"content":{"type":"media","kind":"image","ref":{"kind":"url",
				"url":"http://rails:3000/blob.jpg","sha256":"` + digest + `"}}}`),
		})
		return err
	}

	// Final rather than retryable: the same address answers with the same wrong bytes
	// every time, and this is the answer a caller already reads for a reference whose
	// file is not there.
	assertCode(t, send(t, hex.EncodeToString(wrong[:])), protocol.ErrorMediaUnavailable)

	// The matching one gets past the check; the send itself needs a socket.
	var coded *protocol.Error
	if err := send(t, strings.ToUpper(hex.EncodeToString(right[:]))); errors.As(err, &coded) &&
		coded.Code == protocol.ErrorMediaUnavailable {
		t.Fatalf("the right file was refused, and hex is not case-sensitive: %v", err)
	}
}

// vCard escaping is the card's own encoding and WhatsApp's display name is a plain
// string. Copied across as it stands, a name written `Souza\; Ana` is shown with the
// backslash in it, which is what the recipient sees.
func TestANameReadOffACardComesBackUnescaped(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, on, want string }{
		{"a semicolon", `Souza\; Ana`, "Souza; Ana"},
		{"a comma", `Souza\, Ana`, "Souza, Ana"},
		{"a backslash", `Souza \\ Ana`, `Souza \ Ana`},
		{"a backslash before a semicolon", `Souza \\; Ana`, `Souza \; Ana`},
		{"a newline", `Ana\nSouza`, "Ana\nSouza"},
		{"nothing to unescape", "Ana Souza", "Ana Souza"},
		{"a trailing backslash", `Ana \`, `Ana \`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			card, err := contactCard(&outboundContact{
				Vcard: "BEGIN:VCARD\nVERSION:3.0\nFN:" + tc.on + "\nEND:VCARD\n",
			})
			if err != nil {
				t.Fatalf("contactCard: %v", err)
			}
			if got := card.GetDisplayName(); got != tc.want {
				t.Fatalf("the card is labelled %q, want %q", got, tc.want)
			}
		})
	}

	// And the round trip: what this connector writes, it reads back as what went in.
	// Without a backslash in it, because nothing this connector writes carries one and a
	// literal backslash in a name is what the unescape above would eat.
	const name = "Souza; Ana, Dra. III"
	written := vcardOf(name, "5511999990002")
	if got := vcardName(written); got != name {
		t.Fatalf("a name written and read back came out %q, want %q", got, name)
	}
}

// The caller says what it stored and the address says what it is sending, and they are
// two parties making a claim about the same bytes. A stale blob served with a
// Content-Length that agrees with itself satisfies the second and not the first, so
// checking only the transport's number sends a shortened file and reports success.
//
// And the two disagreements are answered differently, because they are different events.
// The transport's own number not adding up is a transfer that stopped, and the next one
// may carry the whole file. The caller's number not matching what the address serves is
// two records of the same file disagreeing, which the next fetch reproduces exactly:
// answered as a truncation it costs the fetch and the upload again for as long as the
// caller keeps the message, and answers the same thing every time.
func TestAFileSmallerThanTheCallerSaidIsNotSentAsWhatWasAskedFor(t *testing.T) {
	t.Parallel()

	const stale = "metade do arquivo"
	session, serving, _ := outboundSession(t)
	// Self-consistent on the wire: the server declares exactly what it sends.
	serving.answer([]byte(stale), "image/jpeg")

	_, err := session.send(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend,
		Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
			"content":{"type":"media","kind":"image","size":4096,
			"ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}}`),
	})
	assertCode(t, err, protocol.ErrorMediaUnavailable)

	// And a caller that said nothing is not held to a number it never gave.
	session, serving, _ = outboundSession(t)
	serving.answer([]byte(stale), "image/jpeg")
	message := mustSendBody(t, session, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"media","kind":"image","ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}}`)
	if message.GetImageMessage() == nil {
		t.Fatal("a send that declared no size was refused for not matching one")
	}
}

// A digest of the wrong length can only ever fail, so leaving it until the comparison
// spends a transfer of up to the whole send limit and leaves an upload on WhatsApp that
// nothing will ever refer to.
func TestADigestNoFileCouldMatchIsRefusedBeforeTheTransfer(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, digest string }{
		{"too short", "abc123"},
		{"too long", strings.Repeat("ab", 33)},
		{"not hexadecimal", strings.Repeat("z", 64)},
		{"a base64 digest, which is somebody else's encoding", "3q2+7wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, serving, uploads := outboundSession(t)
			serving.answer([]byte("bytes"), "")

			_, err := session.send(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageSend,
				Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
					"content":{"type":"media","kind":"image","ref":{"kind":"url",
					"url":"http://rails:3000/blob.jpg","sha256":"` + tc.digest + `"}}}`),
			})
			// The caller's own payload, not the address serving the wrong file, so the
			// answer is not the one a mismatch gets.
			assertCode(t, err, protocol.ErrorInvalidPayload)
			if serving.count() != 0 || uploads.count() != 0 {
				t.Fatalf("a digest nothing could match still cost %d fetch(es) and %d upload(s)",
					serving.count(), uploads.count())
			}
		})
	}
}

// A document sent with a caption travels inside an envelope of its own, and it is the
// only leaf type that does. whatsmeow unwraps one on the way in and never builds one on
// the way out, so a caption put on the bare leaf is one a current client has no reason to
// look for: the file arrives and the text does not, while the send reports success.
func TestADocumentWithACaptionTravelsInTheEnvelopeWhatsAppUses(t *testing.T) {
	t.Parallel()

	session, serving, _ := outboundSession(t)
	serving.answer([]byte("pdf"), "application/pdf")
	message := mustSendBody(t, session, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"media","kind":"document","mime":"application/pdf","filename":"proposta.pdf",
		"caption":"segue a proposta","ref":{"kind":"url","url":"http://rails:3000/blob.pdf"}}}`)

	if message.GetDocumentMessage() != nil {
		t.Fatal("a captioned document went out as a bare leaf, where the caption is not looked for")
	}
	inner := message.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage()
	if inner == nil {
		t.Fatalf("the envelope carries no document: %v", message)
	}
	if inner.GetCaption() != "segue a proposta" {
		t.Fatalf("the caption inside the envelope is %q", inner.GetCaption())
	}
	if inner.GetFileName() != "proposta.pdf" {
		t.Fatalf("the document inside the envelope is named %q", inner.GetFileName())
	}
	// The quote and the timer belong to the leaf, which is where whatsmeow reads them
	// from once it has unwrapped the envelope.
	if inner.GetURL() == "" || inner.GetMediaKey() == nil {
		t.Fatal("the document inside the envelope carries nothing to fetch")
	}

	// And a document without one stays a bare leaf, which is what WhatsApp sends.
	session, serving, _ = outboundSession(t)
	serving.answer([]byte("pdf"), "application/pdf")
	plain := mustSendBody(t, session, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"media","kind":"document","mime":"application/pdf","filename":"proposta.pdf",
		"ref":{"kind":"url","url":"http://rails:3000/blob.pdf"}}}`)
	if plain.GetDocumentMessage() == nil {
		t.Fatal("a document with no caption was wrapped in the caption envelope")
	}
}

// The contract carries a size on the content and another on the reference, and a client
// that fills in only one of them is filling in the one it has. Reading only the first
// leaves the other unchecked in both directions: a file too big to send is fetched, and a
// stale one shorter than the reference says goes out as what was asked for.
func TestTheReferencesOwnSizeIsHeldToAsWell(t *testing.T) {
	t.Parallel()

	session, serving, uploads := outboundSession(t)
	session.sendLimit = 1 << 10
	serving.answer([]byte("bytes"), "")
	_, err := session.send(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend,
		Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
			"content":{"type":"media","kind":"image",
			"ref":{"kind":"url","url":"http://rails:3000/blob.jpg","size":1048576}}}`),
	})
	assertCode(t, err, protocol.ErrorMediaTooLarge)
	if serving.count() != 0 || uploads.count() != 0 {
		t.Fatalf("a file refused on the reference's own number still cost %d fetch(es) and %d upload(s)",
			serving.count(), uploads.count())
	}

	session, serving, _ = outboundSession(t)
	serving.answer([]byte("metade do arquivo"), "image/jpeg")
	_, err = session.send(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend,
		Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
			"content":{"type":"media","kind":"image",
			"ref":{"kind":"url","url":"http://rails:3000/blob.jpg","size":4096}}}`),
	})
	// The reference's number is the same kind of claim as the caller's, and gets the
	// same answer: not a transfer that stopped, and not worth another one.
	assertCode(t, err, protocol.ErrorMediaUnavailable)
}

// net/http fills Referer with the whole previous URL, query and all, and the previous URL
// is the signed one. It skips the header on an https-to-http downgrade and on nothing
// else, so a cross-origin hop carries the credential this connector was careful not to
// put in a message into the destination's access log.
func TestTheSignedAddressDoesNotFollowTheRedirectAsAReferer(t *testing.T) {
	t.Parallel()

	const signed = "https://storage.example/bucket/file.pdf?X-Amz-Signature=deadbeefcafe"
	hop := requestTo(t, "https://elsewhere.example/file.pdf", nil)
	// What the client itself does just before calling the policy.
	hop.Header.Set("Referer", signed)

	if err := followingRedirects(nil)(hop, []*http.Request{requestTo(t, signed, nil)}); err != nil {
		t.Fatalf("the hop was refused: %v", err)
	}
	if got := hop.Header.Get("Referer"); got != "" {
		t.Fatalf("the signed address followed the redirect as a Referer: %q", got)
	}

	// On the same origin it is somebody's own log about their own URL, and removing it
	// would be this connector rewriting an ordinary request for no gain.
	same := requestTo(t, "https://storage.example/bucket/other.pdf", nil)
	same.Header.Set("Referer", signed)
	if err := followingRedirects(nil)(same, []*http.Request{requestTo(t, signed, nil)}); err != nil {
		t.Fatalf("the hop was refused: %v", err)
	}
	if got := same.Header.Get("Referer"); got != signed {
		t.Fatalf("a hop on the same origin lost its Referer: %q", got)
	}
}

// Every arm of the contract's content says `additionalProperties: true`, and AGENTS.md
// promises a client can add an optional field without anything else having to know.
// Decoded into one struct covering all four bodies, a field one arm adds under a name
// another arm uses at a different type fails the whole command -- for messages of the
// type that never carried the field at all.
func TestAFieldOneBodyDoesNotKnowAboutDoesNotFailAnother(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, payload string }{
		// `size` is an integer on a media body, and here it is a string on a text one.
		{"a text carrying a field media types as a number",
			`{"type":"text","body":"oi","size":"unknown"}`},
		// `contacts` is an array on a contacts body.
		{"a text carrying a field contacts types as an array",
			`{"type":"text","body":"oi","contacts":"none"}`},
		// `latitude` is a number on a location body.
		{"a text carrying a field location types as a number",
			`{"type":"text","body":"oi","latitude":"unknown"}`},
		// And a field nothing has ever defined, which is the ordinary additive case.
		{"a text carrying a field nothing has defined",
			`{"type":"text","body":"oi","scheduled_for":"2027-01-01T00:00:00Z"}`},
		{"a location carrying a field media types as a string",
			`{"type":"location","latitude":-25.4,"longitude":-49.2,"mime":42}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
				"content":`+tc.payload+`}`)
			if _, _, err := planBody(req, nil, 1<<20); err != nil {
				t.Fatalf("a body carrying a field of another arm was refused: %v", err)
			}
		})
	}
}

// Two numbers about the same file that cannot both be right. Left to be found out where
// they are compared, the payload fails after the whole transfer, answers something
// retryable, and every retry repeats an impossible upload.
func TestTwoSizesThatCannotBothBeRightAreRefusedBeforeTheTransfer(t *testing.T) {
	t.Parallel()

	session, serving, uploads := outboundSession(t)
	serving.answer([]byte("bytes"), "")

	_, err := session.send(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend,
		Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
			"content":{"type":"media","kind":"image","size":4096,
			"ref":{"kind":"url","url":"http://rails:3000/blob.jpg","size":2048}}}`),
	})
	assertCode(t, err, protocol.ErrorInvalidPayload)
	if serving.count() != 0 || uploads.count() != 0 {
		t.Fatalf("a payload that could only fail still cost %d fetch(es) and %d upload(s)",
			serving.count(), uploads.count())
	}

	// Two that agree are the ordinary case: Chatwoot fills both from one blob.
	session, serving, _ = outboundSession(t)
	serving.answer([]byte("os bytes do arquivo"), "image/jpeg")
	message := mustSendBody(t, session, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
		"content":{"type":"media","kind":"image","size":19,
		"ref":{"kind":"url","url":"http://rails:3000/blob.jpg","size":19}}}`)
	if message.GetImageMessage() == nil {
		t.Fatal("two sizes that agree were refused")
	}
}

// The upload stages the encrypted file on disk before it sends anything, so a temporary
// directory that is read-only or full fails without WhatsApp having been contacted.
// Reported as WhatsApp refusing the file, an operator goes looking at WhatsApp for a disk
// of their own.
func TestADiskThisInstanceCouldNotWriteIsNotWhatsAppRefusingTheFile(t *testing.T) {
	t.Parallel()

	session, serving, uploads := outboundSession(t)
	serving.answer([]byte("bytes"), "")
	// The shape whatsmeow's own staging failures arrive in: os.CreateTemp, Write and Seek
	// all answer with one of these, and nothing on the network side does.
	uploads.answer(nil, fmt.Errorf("failed to create temporary file: %w",
		&fs.PathError{Op: "open", Path: "/tmp/whatsmeow-upload-123", Err: fs.ErrPermission}))

	_, err := session.send(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend,
		Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
			"content":{"type":"media","kind":"image",
			"ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}}`),
	})
	assertCode(t, err, protocol.ErrorInternal)
	if strings.Contains(err.Error(), "WhatsApp") {
		t.Fatalf("a disk of this instance's own was reported as WhatsApp: %v", err)
	}
}

// The contract carries one sticker kind on purpose, so a caller forwarding an animated
// one has no field to say so in. Left unset, WhatsApp encodes the message as a static
// sticker and the recipient sees one frame of something that was meant to move.
func TestAnAnimatedStickerIsSentAsOne(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		file     []byte
		animated bool
	}{
		{"an animated webp", webpFile(t, "VP8X", 0x02|0x10), true},
		{"a still webp in the extended format", webpFile(t, "VP8X", 0x10), false},
		{"a plain webp, which has no flags at all", webpFile(t, "VP8 ", 0), false},
		{"a lossless webp", webpFile(t, "VP8L", 0), false},
		{"something that is not a webp", []byte("\x89PNG\r\n\x1a\n and then some more bytes"), false},
		{"a file too short to say", []byte("RIFF"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, serving, _ := outboundSession(t)
			serving.answer(tc.file, "image/webp")
			message := mustSendBody(t, session, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
				"content":{"type":"media","kind":"sticker","mime":"image/webp",
				"ref":{"kind":"url","url":"http://rails:3000/blob.webp"}}}`)

			sticker := message.GetStickerMessage()
			if sticker.GetIsAnimated() != tc.animated {
				t.Fatalf("the sticker went out with is_animated=%v, want %v",
					sticker.GetIsAnimated(), tc.animated)
			}
			// And whatever was peeked at is still uploaded: reading the header must not
			// take it out of the file.
			if got := int(sticker.GetFileLength()); got != len(tc.file) {
				t.Fatalf("%d bytes were uploaded and the file is %d", got, len(tc.file))
			}
		})
	}
}

// webpFile builds a header the animation flag can be read out of, followed by enough
// bytes that the peek has something to work with.
func webpFile(t *testing.T, chunk string, flags byte) []byte {
	t.Helper()

	if len(chunk) != 4 {
		t.Fatalf("%q is not a four-character chunk name", chunk)
	}
	file := make([]byte, 0, 64)
	file = append(file, "RIFF"...)
	file = append(file, 0, 0, 0, 0)
	file = append(file, "WEBP"...)
	file = append(file, chunk...)
	file = append(file, 0, 0, 0, 0, flags)
	return append(file, "os bytes do sticker"...)
}

// Every failure of the caller's own address is said in words this connector chose.
// net/http writes addresses into its errors in more places than are worth chasing one at
// a time, and a signed URL in any of them ends up in the caller's logs.
func TestWhyAFetchFailedIsSaidWithoutRepeatingTheLibrary(t *testing.T) {
	t.Parallel()

	const signed = "https://storage.example/f.pdf?X-Amz-Signature=deadbeefcafe"
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"a host that does not resolve", &net.DNSError{Err: "no such host", Name: "storage.example"},
			"its host does not resolve"},
		{"nothing listening", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, "nothing is listening there"},
		{"a connection cut", &net.OpError{Op: "read", Err: syscall.ECONNRESET}, "it closed the connection"},
		{"a network with no route", &net.OpError{Op: "dial", Err: syscall.EHOSTUNREACH},
			"it cannot be reached from this instance"},
		// The shape this round was about: net/http quotes an unparseable Location whole.
		{"a redirect nobody can parse", fmt.Errorf(`failed to parse Location header %q: invalid URI`, signed),
			"it could not be reached"},
		{"anything else", errors.New("some new thing net/http says, carrying " + signed),
			"it could not be reached"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Wrapped the way the client wraps it, so the unwrapping is exercised too.
			wrapped := &url.Error{Op: "Get", URL: signed, Err: tc.err}
			if got := whyUnreachable(wrapped); got != tc.want {
				t.Fatalf("whyUnreachable = %q, want %q", got, tc.want)
			}
			if strings.Contains(whyUnreachable(wrapped), "deadbeefcafe") {
				t.Fatal("the reason carries the signature out of the address")
			}
		})
	}
}

// WhatsApp reads DisplayName as the label for the whole stack, and a stack sent without
// one arrives blank on the clients that show it. The contract has no aggregate label, so
// it is built from the names -- which also keeps it out of a language this connector has
// no way to know.
func TestAStackOfCardsIsLabelledWithWhoIsInIt(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		count int
		want  string
	}{
		{"two", 2, "Contato 1, Contato 2"},
		{"exactly as many as are spelled out", 3, "Contato 1, Contato 2, Contato 3"},
		{"more than fit", 6, "Contato 1, Contato 2, Contato 3 +3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			entries := make([]string, 0, tc.count)
			for i := 1; i <= tc.count; i++ {
				entries = append(entries, fmt.Sprintf(
					`{"display_name":"Contato %d","phone":"551199999000%d"}`, i, i))
			}
			req := requestOf(t, `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
				"content":{"type":"contacts","contacts":[`+strings.Join(entries, ",")+`]}}`)

			message, _, err := planBody(req, nil, 1<<20)
			if err != nil {
				t.Fatalf("planBody: %v", err)
			}
			if got := message.GetContactsArrayMessage().GetDisplayName(); got != tc.want {
				t.Fatalf("the stack is labelled %q, want %q", got, tc.want)
			}
		})
	}
}

// The caller's own address is checked before anything is fetched, and a redirect is that
// address asking for somewhere else one step later. Left to net/http, the refusal arrives
// as an error this side reads as worth retrying, and the same reference redirects the
// same way every time.
func TestARedirectSomewhereThisConnectorDoesNotFetchFromIsRefused(t *testing.T) {
	t.Parallel()

	for _, target := range []string{
		"file:///etc/passwd",
		"ftp://storage.example/f.pdf",
		"gopher://storage.example/f.pdf",
	} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()

			sending := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				// Written by hand: http.Redirect would not object either, and what is
				// under test is this connector's own policy.
				w.Header().Set("Location", target)
				w.WriteHeader(http.StatusFound)
			}))
			t.Cleanup(sending.Close)

			_, err := retrieveOverHTTP(t.Context(), sending.URL+"/f.pdf", nil)
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// And the same check on the address the caller wrote, which is where it started.
func TestOnlyAnHTTPAddressIsFetchedFrom(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		address string
		ok      bool
	}{
		{"http", "http://storage.example/f.pdf", true},
		{"https", "https://storage.example/f.pdf", true},
		{"a file on this instance's disk", "file:///etc/passwd", false},
		{"another protocol entirely", "ftp://storage.example/f.pdf", false},
		{"no host", "http:///f.pdf", false},
		{"no scheme", "//storage.example/f.pdf", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := url.Parse(tc.address)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.address, err)
			}
			switch err := overHTTP(parsed); {
			case tc.ok && err != nil:
				t.Fatalf("%q was refused: %v", tc.address, err)
			case !tc.ok && err == nil:
				t.Fatalf("%q was accepted", tc.address)
			case !tc.ok && !errors.Is(err, errNotOverHTTP):
				t.Fatalf("%q was refused with %v, which the fetch cannot classify", tc.address, err)
			}
		})
	}
}

// A sticker is a webp on WhatsApp and a voice note is opus in ogg, and neither renders as
// anything else. Labelled with the generic type because nothing named the file, one
// arrives as a broken sticker and the other as a note that will not play, while the send
// reports success.
func TestAStickerAndAVoiceNoteAreLabelledWithWhatTheyCanOnlyBe(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, content, ref, served, want string }{
		{"a sticker nobody named", `"kind":"sticker"`, "", "application/octet-stream", "image/webp"},
		{"a sticker the server would not name", `"kind":"sticker"`, "", "", "image/webp"},
		{"a voice note nobody named", `"kind":"audio","voice_note":true`, "", "", "audio/ogg; codecs=opus"},
		// The extension would answer `audio/ogg`, which WhatsApp does not play as a note.
		{"a voice note named only by its extension", `"kind":"audio","voice_note":true,"filename":"a.ogg"`,
			"", "application/octet-stream", "audio/ogg; codecs=opus"},
		// An ordinary audio can be anything, so the extension still decides.
		{"an ordinary audio file", `"kind":"audio","filename":"a.mp3"`, "", "", "audio/mpeg"},
		// The case that made every voice note vanish: a server naming the same format
		// with less detail than the kind guarantees. WhatsApp drops an `audio/ogg` that
		// is not told it is opus, and says nothing about it.
		{"a voice note the server named without the codec", `"kind":"audio","voice_note":true`,
			"", "audio/ogg", "audio/ogg; codecs=opus"},
		{"a sticker the server named as a plain webp", `"kind":"sticker"`, "", "image/webp", "image/webp"},
		// The same loss reaches here by two more doors, and the send reports success
		// through all three. A store that keeps base types hands the caller back
		// `audio/ogg` for the note it holds, and the caller repeats it.
		{"a voice note the caller named without the codec",
			`"kind":"audio","voice_note":true,"mime":"audio/ogg"`, "", "", "audio/ogg; codecs=opus"},
		{"a voice note the reference named without the codec",
			`"kind":"audio","voice_note":true`, "audio/ogg", "", "audio/ogg; codecs=opus"},
		// And `application/octet-stream` is not a claim about the format, whoever
		// repeats it: a store that lost the type, a proxy that labels every body a
		// stream of bytes, a caller passing either along.
		{"a voice note the caller calls a stream of bytes",
			`"kind":"audio","voice_note":true,"mime":"application/octet-stream"`,
			"", "", "audio/ogg; codecs=opus"},
		{"a sticker the reference calls a stream of bytes",
			`"kind":"sticker"`, "application/octet-stream", "", "image/webp"},
		{"a document the caller calls a stream of bytes, named by its extension",
			`"kind":"document","filename":"a.pdf","mime":"application/octet-stream"`,
			"", "", "application/pdf"},
		// Nor is one that cannot be read a claim. Passed on it reaches WhatsApp as a type
		// it cannot parse, and ahead of the kind's own it is the voice note losing its
		// codec by a third route. Two shapes, because Go's parser fails at two depths:
		// a parameter it cannot read, where the base type is still there to be had, and
		// a type that is not one at all.
		{"a voice note whose type has a parameter that will not parse",
			`"kind":"audio","voice_note":true,"mime":"audio/ogg; codecs"`, "", "", "audio/ogg; codecs=opus"},
		{"a document with one, falling through to its extension",
			`"kind":"document","filename":"a.pdf","mime":"application/pdf; ="`, "", "", "application/pdf"},
		{"a type that is not one, with nothing left to go on",
			`"kind":"document","mime":"audio ogg"`, "", "", "application/octet-stream"},
		// And a stated type that names a different format is information, not something
		// to correct.
		{"a sticker the caller says is a PNG", `"kind":"sticker","mime":"image/png"`, "", "", "image/png"},
		{"a sticker the reference says is a PNG", `"kind":"sticker"`, "image/png", "", "image/png"},
		{"a sticker the server says is a PNG", `"kind":"sticker"`, "", "image/png", "image/png"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, serving, _ := outboundSession(t)
			serving.answer([]byte("bytes"), tc.served)
			ref := `"ref":{"kind":"url","url":"http://rails:3000/blob"`
			if tc.ref != "" {
				ref += `,"mime":"` + tc.ref + `"`
			}
			message := mustSendBody(t, session, `{"message_id":"3EB0",
				"to":{"kind":"phone","id":"5511999990001"},
				"content":{"type":"media",`+tc.content+`,`+ref+`}}}`)

			var got string
			switch {
			case message.GetStickerMessage() != nil:
				got = message.GetStickerMessage().GetMimetype()
			case message.GetAudioMessage() != nil:
				got = message.GetAudioMessage().GetMimetype()
			case message.GetDocumentMessage() != nil:
				got = message.GetDocumentMessage().GetMimetype()
			default:
				t.Fatalf("that did not go out as a sticker, an audio or a document: %v", message)
			}
			if got != tc.want {
				t.Fatalf("the file went out as %q, want %q", got, tc.want)
			}
		})
	}
}

// Two names that differ only in case are one header on the wire, and which of them
// survives is decided by the order Go happens to walk the map in, which is deliberately
// not the same twice. A reference carrying both would send one credential on one attempt
// and the other on the next.
func TestTwoHeadersThatAreTheSameHeaderAreRefused(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, headers string }{
		{"the same name in two cases", `{"Authorization":"Bearer a","authorization":"Bearer b"}`},
		{"a vendor header in two cases", `{"X-API-Key":"a","x-api-key":"b"}`},
		{"three of them", `{"Accept":"a","accept":"b","ACCEPT":"c"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, serving, _ := outboundSession(t)
			serving.answer([]byte("bytes"), "")

			_, err := session.send(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageSend,
				Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
					"content":{"type":"media","kind":"image","ref":{"kind":"url",
					"url":"http://rails:3000/blob.jpg","headers":` + tc.headers + `}}}`),
			})
			assertCode(t, err, protocol.ErrorInvalidPayload)
			if serving.count() != 0 {
				t.Fatalf("a request nobody could reproduce was still sent %d time(s)", serving.count())
			}
		})
	}

	// Different headers are the ordinary case and stay so.
	if err := sendableHeaders(map[string]string{
		"Authorization": "Bearer a", "X-API-Key": "b", "Accept": "*/*",
	}); err != nil {
		t.Fatalf("three different headers were refused: %v", err)
	}
}

// Set on a request's header map, Host does nothing: net/http takes the authority from
// Request.Host or from the URL and ignores the field. Accepted silently, a reference that
// needs virtual-host addressing fetches from somewhere else entirely and says it worked.
func TestAHostHeaderIsRefusedRatherThanSilentlyIgnored(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"Host", "host", "HOST"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			session, serving, _ := outboundSession(t)
			serving.answer([]byte("bytes"), "")

			_, err := session.send(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageSend,
				Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
					"content":{"type":"media","kind":"image","ref":{"kind":"url",
					"url":"http://10.0.0.5/blob.jpg","headers":{"` + name + `":"storage.example"}}}}`),
			})
			assertCode(t, err, protocol.ErrorInvalidPayload)
			if serving.count() != 0 {
				t.Fatalf("a fetch that would have gone somewhere else was still made %d time(s)", serving.count())
			}
		})
	}
}

// A negative size is not an absent one, which is what every check below it would read it
// as: the contract says `minimum: 0` and nothing validates a command against the schema
// at runtime, so a negative silently turns off the comparison that catches a file
// arriving short.
func TestASizeNoFileCouldHaveIsRefused(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ name, content string }{
		{"on the message", `"size":-1,"ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}`},
		{"on the reference", `"ref":{"kind":"url","url":"http://rails:3000/blob.jpg","size":-1}`},
		{"on both", `"size":-4096,"ref":{"kind":"url","url":"http://rails:3000/blob.jpg","size":-4096}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, serving, _ := outboundSession(t)
			serving.answer([]byte("metade do arquivo"), "")

			_, err := session.send(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageSend,
				Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
					"content":{"type":"media","kind":"image",` + tc.content + `}}`),
			})
			assertCode(t, err, protocol.ErrorInvalidPayload)
			if serving.count() != 0 {
				t.Fatalf("a payload the contract forbids still cost %d fetch(es)", serving.count())
			}
		})
	}
}

// 169.254.169.254 is the instance metadata endpoint on every major cloud, and what it
// answers is the host's own credentials. A fetch of it uploads them to whatever WhatsApp
// number the command named, and the send reports success.
//
// The private network as a whole is not refused, and cannot be: the client sits next to
// this connector and hands over an address on their own network, which is the ordinary
// case and not an attack. So what is asserted here is both halves -- the addresses that
// answer with credentials refused, and everything else this connector actually fetches
// from still dialled. The second half is what decides how wide the first can be.
func TestAMetadataAddressIsNeverDialled(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, address string
		refused       bool
	}{
		{"the metadata endpoint", "169.254.169.254:80", true},
		{"it in the v6 form a resolver can answer with", "[::ffff:169.254.169.254]:80", true},
		{"an IPv6 link-local address", "[fe80::1]:80", true},
		{"the whole link-local range, not one address", "169.254.1.2:8080", true},
		// Where ECS answers for a task role, which is the same credentials by another
		// address and is why the range goes rather than the one address.
		{"the task role endpoint next to it", "169.254.170.2:80", true},
		// Each of these answers outside the link-local range, and each sits in a range
		// something else legitimately uses, so each goes as one address.
		{"AWS's IPv6 metadata endpoint", "[fd00:ec2::254]:80", true},
		{"GCP's IPv6 metadata endpoint", "[fd20:ce::254]:80", true},
		{"Alibaba Cloud's metadata endpoint", "100.100.100.200:80", true},
		// Every one of these is somewhere a deployment does serve media from.
		{"the client next door on a compose network", "10.0.0.4:3000", false},
		{"the client on the same host", "127.0.0.1:3000", false},
		{"it over IPv6", "[::1]:3000", false},
		{"a private IPv6 network, which is the range the address above sits in",
			"[fd00:ec2::1]:3000", false},
		{"another private IPv6 network", "[fd12:3456:789a::1]:3000", false},
		{"the range GCP's endpoint sits in", "[fd20:ce::1]:3000", false},
		// 100.64.0.0/10 is shared address space, which is where a Tailscale network
		// lives, so it is the address that goes and not the range.
		{"a host on a Tailscale network", "100.100.100.201:3000", false},
		{"a storage service on the internet", "93.184.216.34:443", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			switch err := refuseMetadataAddress("tcp", tc.address, nil); {
			case tc.refused && !errors.Is(err, errMetadataAddress):
				t.Fatalf("%s was dialled, answering %v", tc.address, err)
			case !tc.refused && err != nil:
				t.Fatalf("%s was refused: %v", tc.address, err)
			}
		})
	}
}

// The refusal is on the dial rather than on the host in the URL, which is what makes it
// cover a redirect: an address the client vouches for that bounces one hop into the
// metadata range is the shape that gets past a check of the caller's own URL.
//
// And it is the caller's reference to fix, not a minute to wait out. Classified as
// retryable the same address is dialled again for as long as the caller keeps the
// message.
func TestAFetchRedirectedIntoTheMetadataRangeIsRefusedAsThePayload(t *testing.T) {
	t.Parallel()

	serving := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data/", http.StatusFound)
	}))
	t.Cleanup(serving.Close)

	file, err := retrieveOverHTTP(t.Context(), serving.URL+"/blob", nil)
	if err == nil {
		_ = file.body.Close()
		t.Fatal("the fetch followed the redirect into the metadata range")
	}
	assertCode(t, err, protocol.ErrorInvalidPayload)
}

// A proxied fetch dials the proxy, so the dial control would be about the proxy's address
// while the proxy fetches whatever it was asked for. net/http proxies exactly when this
// field is set, and the default transport sets it from HTTP_PROXY -- which means an
// environment variable inherited from an image, or exported for something else in the
// same container, would turn the refusal above off and say nothing about it.
func TestTheFetchTransportDoesNotProxy(t *testing.T) {
	t.Parallel()

	if fetchTransport.Proxy != nil {
		t.Fatal("the fetch proxies, so what it refuses is the proxy's address")
	}
}

// whatsmeow answers ErrBroadcastListUnsupported for every broadcast list but
// status@broadcast, so the send is refused whatever it carries. For a file it is refused
// after the fetch and the upload: up to WAC_MEDIA_SEND_MAX moved for a message that could
// never go out, and an upload left on WhatsApp that nothing will ever refer to.
func TestAFileForABroadcastListIsRefusedBeforeItIsFetched(t *testing.T) {
	t.Parallel()

	session, serving, uploads := outboundSession(t)
	serving.answer([]byte("bytes"), "image/jpeg")

	_, err := session.send(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend,
		Payload: json.RawMessage(`{"message_id":"3EB0","to":{"kind":"broadcast","id":"120363000000000000"},
			"content":{"type":"media","kind":"image",
			"ref":{"kind":"url","url":"http://rails:3000/blob.jpg"}}}`),
	})
	assertCode(t, err, protocol.ErrorUnsupported)
	if serving.count() != 0 || uploads.count() != 0 {
		t.Fatalf("a message that could never go out cost %d fetch(es) and %d upload(s)",
			serving.count(), uploads.count())
	}
}
