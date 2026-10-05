package whatsmeow

import (
	"bytes"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"testing"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

const captionChat = `"to":{"kind":"phone","id":"5511999990002"}`

// sendThenEdit sends a file with the given content through the real send path, then edits
// the message with a text body, and hands back what each one put on the wire and the
// doubles that count the fetches and uploads.
func sendThenEdit(t *testing.T, content, correction string) (sentBody, editBody *waE2E.Message, files *serving, sent *uploads, editErr error) {
	t.Helper()

	session, files, sent := outboundSession(t)
	wired := &wire{}
	session.handOver = wired.hand
	files.answer(tinyPNG(t), "image/png")

	if _, err := session.send(t.Context(), &protocol.Command{Type: protocol.CommandMessageSend, Payload: json.RawMessage(
		`{"message_id":"3EB0SENTFILE",` + captionChat + `,"content":` + content + `}`)}); err != nil {
		t.Fatalf("send: %v", err)
	}
	sentBody = wired.message

	wired.message = nil
	_, editErr = session.edit(t.Context(), &protocol.Command{Type: protocol.CommandMessageEdit, Payload: json.RawMessage(
		`{"message_id":"3EB0THEEDIT",` + captionChat + `,"target_id":"3EB0SENTFILE",` +
			`"content":{"type":"text","body":` + quote(correction) + `}}`)})
	return sentBody, wired.message, files, sent, editErr
}

func quote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func tinyPNG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 8, 8))
	for x := range 8 {
		img.Set(x, x, color.RGBA{R: 200, A: 255})
	}
	var out bytes.Buffer
	if err := png.Encode(&out, img); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return out.Bytes()
}

// The file a caption edit names is the one that was sent, coordinates and all, and it is
// not fetched or uploaded again to get there: an edit replaces the message it names, so a
// correction without the file is a correction that removes it (#32).
func TestACaptionEditSendsTheFileItWasSentWithAndTheNewCaption(t *testing.T) {
	t.Parallel()

	const ref = `"ref":{"kind":"url","url":"http://rails:3000/blob.png"}`
	for _, tc := range []struct {
		name    string
		content string
		leaf    func(*waE2E.Message) interface {
			GetCaption() string
			GetDirectPath() string
			GetMediaKey() []byte
		}
	}{
		{"an image", `{"type":"media","kind":"image","mime":"image/png","caption":"antes",` + ref + `}`,
			func(m *waE2E.Message) interface {
				GetCaption() string
				GetDirectPath() string
				GetMediaKey() []byte
			} {
				return m.GetImageMessage()
			}},
		{"a video", `{"type":"media","kind":"video","mime":"video/mp4","caption":"antes",` + ref + `}`,
			func(m *waE2E.Message) interface {
				GetCaption() string
				GetDirectPath() string
				GetMediaKey() []byte
			} {
				return m.GetVideoMessage()
			}},
		{"a document with a caption", `{"type":"media","kind":"document","mime":"application/pdf","filename":"a.pdf","caption":"antes",` + ref + `}`,
			func(m *waE2E.Message) interface {
				GetCaption() string
				GetDirectPath() string
				GetMediaKey() []byte
			} {
				return m.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage()
			}},
		{"a document sent without one", `{"type":"media","kind":"document","mime":"application/pdf","filename":"a.pdf",` + ref + `}`,
			func(m *waE2E.Message) interface {
				GetCaption() string
				GetDirectPath() string
				GetMediaKey() []byte
			} {
				// Gaining a caption puts it in the envelope a send with one would have used.
				return m.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage()
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, edited, files, sent, err := sendThenEdit(t, tc.content, "depois")
			if err != nil {
				t.Fatalf("edit: %v", err)
			}
			correction := correctionIn(edited)
			leaf := tc.leaf(correction)
			if leaf == nil || leaf.GetDirectPath() == "" {
				t.Fatalf("the correction carries no file:\n%v", correction)
			}
			if leaf.GetCaption() != "depois" {
				t.Fatalf("the caption went out as %q", leaf.GetCaption())
			}
			if leaf.GetDirectPath() != "/v/t62/x.enc" || string(leaf.GetMediaKey()) != "k" {
				t.Fatalf("the correction names another file: %q, %q", leaf.GetDirectPath(), leaf.GetMediaKey())
			}
			if correction.GetConversation() != "" || correction.GetExtendedTextMessage() != nil {
				t.Fatalf("the correction went out as text: %v", correction)
			}
			if files.count() != 1 || sent.count() != 1 {
				t.Fatalf("editing the caption fetched the file %d times and uploaded it %d times, want once each, for the send",
					files.count(), sent.count())
			}
		})
	}
}

// A voice note, an audio file and a sticker carry no caption, so a text correction of one
// has nothing to replace but the file itself, and it is refused before anything goes out.
func TestACaptionEditOfAFileWithNoCaptionIsRefused(t *testing.T) {
	t.Parallel()

	const ref = `"ref":{"kind":"url","url":"http://rails:3000/blob"}`
	for name, content := range map[string]string{
		"a voice note":  `{"type":"media","kind":"audio","mime":"audio/ogg; codecs=opus","voice_note":true,` + ref + `}`,
		"an audio file": `{"type":"media","kind":"audio","mime":"audio/mpeg",` + ref + `}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, edited, _, _, err := sendThenEdit(t, content, "legenda")
			assertCode(t, err, protocol.ErrorUnsupported)
			if edited != nil {
				t.Fatalf("a refused edit reached the wire: %v", edited)
			}
		})
	}
}

// What is not kept goes out as the text it was asked to be: a text message this account
// sent before this build, or one sent from the phone, is still corrected as text.
func TestATextEditOfAMessageNotKeptGoesOutAsText(t *testing.T) {
	t.Parallel()

	session, sent := actingSession(t)
	if _, err := session.edit(t.Context(), &protocol.Command{Type: protocol.CommandMessageEdit, Payload: json.RawMessage(
		`{` + captionChat + `,"target_id":"3EB0NEVERKEPT","content":{"type":"text","body":"corrigido"}}`)}); err != nil {
		t.Fatalf("edit: %v", err)
	}
	if got := correctionIn(sent.message).GetConversation(); got != "corrigido" {
		t.Fatalf("the correction went out as %v", sent.message)
	}
}

// The record is the session's own and names the chat it went to: an edit that names a
// kept message in another chat is refused rather than sent with somebody else's file.
func TestACaptionEditNamingAnotherChatIsRefused(t *testing.T) {
	t.Parallel()

	session, files, _ := outboundSession(t)
	wired := &wire{}
	session.handOver = wired.hand
	files.answer(tinyPNG(t), "image/png")
	if _, err := session.send(t.Context(), &protocol.Command{Type: protocol.CommandMessageSend, Payload: json.RawMessage(
		`{"message_id":"3EB0SENTFILE",` + captionChat + `,"content":{"type":"media","kind":"image","mime":"image/png",` +
			`"caption":"antes","ref":{"kind":"url","url":"http://rails:3000/blob.png"}}}`)}); err != nil {
		t.Fatalf("send: %v", err)
	}
	wired.message = nil
	_, err := session.edit(t.Context(), &protocol.Command{Type: protocol.CommandMessageEdit, Payload: json.RawMessage(
		`{"to":{"kind":"phone","id":"5511999990003"},"target_id":"3EB0SENTFILE","content":{"type":"text","body":"x"}}`)})
	assertCode(t, err, protocol.ErrorInvalidPayload)
	if wired.message != nil {
		t.Fatalf("a refused edit reached the wire: %v", wired.message)
	}
}

// correctionIn is the corrected message inside the envelopes an edit travels in.
func correctionIn(m *waE2E.Message) *waE2E.Message {
	return m.GetEditedMessage().GetMessage().GetProtocolMessage().GetEditedMessage()
}

// A file sent to a number is in the conversation the client has under the LID once the
// pairing is known, and an edit that names that LID is an edit of the same chat.
func TestACaptionEditByTheLIDOfTheNumberItWasSentToIsTheSameChat(t *testing.T) {
	t.Parallel()

	session, files, _ := outboundSession(t)
	wired := &wire{}
	session.handOver = wired.hand
	files.answer(tinyPNG(t), "image/png")
	lid := waTypes.NewJID("167392323834077", waTypes.HiddenUserServer)
	phone := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	if err := session.current().Store.LIDs.PutLIDMapping(t.Context(), lid, phone); err != nil {
		t.Fatalf("PutLIDMapping: %v", err)
	}
	session.aliases.observe(session.aliases.stamp(t.Context()), phone, lid)

	if _, err := session.send(t.Context(), &protocol.Command{Type: protocol.CommandMessageSend, Payload: json.RawMessage(
		`{"message_id":"3EB0SENTFILE",` + captionChat + `,"content":{"type":"media","kind":"image","mime":"image/png",` +
			`"caption":"antes","ref":{"kind":"url","url":"http://rails:3000/blob.png"}}}`)}); err != nil {
		t.Fatalf("send: %v", err)
	}
	wired.message = nil
	if _, err := session.edit(t.Context(), &protocol.Command{Type: protocol.CommandMessageEdit, Payload: json.RawMessage(
		`{"to":{"kind":"lid","id":"167392323834077"},"target_id":"3EB0SENTFILE","content":{"type":"text","body":"depois"}}`)}); err != nil {
		t.Fatalf("edit by the LID: %v", err)
	}
	if got := correctionIn(wired.message).GetImageMessage().GetCaption(); got != "depois" {
		t.Fatalf("the correction went out as %v", wired.message)
	}
}

// A file whose record could not be kept is not sent: a later edit of it would find no
// record, go out as text and replace the file with nothing, and a send refused now is one
// the client can deliver again.
func TestAFileThatCannotBeKeptIsNotSent(t *testing.T) {
	t.Parallel()

	session, container := newTestSession(t, "5511999990001")
	connect(session)
	files, sent := &serving{}, &uploads{}
	session.retrieve = files.hand
	session.uploadFile = sent.hand
	wired := &wire{}
	session.handOver = wired.hand
	files.answer(tinyPNG(t), "image/png")
	if err := container.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := session.send(t.Context(), &protocol.Command{Type: protocol.CommandMessageSend, Payload: json.RawMessage(
		`{"message_id":"3EB0SENTFILE",` + captionChat + `,"content":{"type":"media","kind":"image","mime":"image/png",` +
			`"caption":"antes","ref":{"kind":"url","url":"http://rails:3000/blob.png"}}}`)}); err == nil {
		t.Fatal("a file whose record the store refused was sent")
	}
	if wired.message != nil {
		t.Fatalf("a refused send reached the wire: %v", wired.message)
	}
}
