//go:build live

// What a companion device is actually given when somebody sends a file to be seen once.
//
//	go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveViewOnceReachesACompanion
//
// #21 is a product decision -- whether this connector keeps a view-once file -- and it
// says itself that the decision is not actionable until one thing is measured: does a
// companion device ever receive the bytes at all? `TestLiveViewOnce` asks the question
// with a person holding a phone, and one image on one client version is not an answer.
// Here the sender is the second paired account, so the question can be asked as many
// times and in as many shapes as it takes, and the answer is a measurement rather than
// somebody's recollection of a Tuesday.
//
// What it deliberately does NOT establish: the arm where the sender is the primary phone
// of the account under test (its own echo), and the arm where the sender runs a different
// client version. Both need hardware and stay in #21 as what is still unanswered.
package whatsmeow

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// A one-pixel PNG and the smallest MP4 that WhatsApp accepts as a video. Tiny on purpose:
// what is being measured is whether the message reaches a companion at all, and a file
// large enough to be interesting would only add upload time to every arm.
var onePixelPNG = []byte{
	0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
	0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
	0x89, 0x00, 0x00, 0x00, 0x0a, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00,
	0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae,
	0x42, 0x60, 0x82,
}

func TestLiveViewOnceReachesACompanion(t *testing.T) {
	const token = "live-check"

	// A real blob store, and the control below is why. Without one every media message
	// comes back with no `ref` for want of anywhere to put the file, and "no ref" is
	// exactly the shape a refused view-once has -- so the phase would report every arm as
	// a refusal and read as an answer. The first run of this did precisely that, and the
	// control is what caught it.
	root := filepath.Join(liveDir(t), "blobs")
	blobs, err := media.New(media.Options{Root: root})
	if err != nil {
		t.Fatalf("open the blob store at %s: %v", root, err)
	}
	t.Cleanup(func() { _ = blobs.Close() })

	mux := http.NewServeMux()
	mux.Handle("GET /media/{id}", media.Handler(media.HandlerOptions{Blobs: blobs, Token: token}))
	endpoint := httptest.NewServer(mux)
	t.Cleanup(endpoint.Close)

	subject, counterpart, container := liveBoth(t, MediaOptions{Blobs: blobs, BaseURL: endpoint.URL})
	subjectJID := liveMustBePaired(t, container, liveSID)
	liveMustBePaired(t, container, liveCounterpartSID)

	liveResume(t, subject)
	liveResume(t, counterpart)

	watching := watch(t, subject)

	// Three shapes, because "a view-once image" is three different things on the wire and
	// WhatsApp may well treat them differently. The flag on the media is what the field
	// in `viewOnce()` reads; the V2 envelope is what a current client actually sends; and
	// the plain image beside them is the control, without which a silence here says
	// nothing -- a run where nothing arrives at all proves the harness broken, not
	// WhatsApp withholding.
	for _, probe := range []struct {
		name  string
		build func(*wm.UploadResponse) *waE2E.Message
	}{
		{"a plain image, as the control", func(up *wm.UploadResponse) *waE2E.Message {
			return &waE2E.Message{ImageMessage: viewOnceImage(up, false)}
		}},
		{"the flag on the image itself", func(up *wm.UploadResponse) *waE2E.Message {
			return &waE2E.Message{ImageMessage: viewOnceImage(up, true)}
		}},
		{"wrapped in the V2 envelope, as a current client sends it", func(up *wm.UploadResponse) *waE2E.Message {
			return &waE2E.Message{ViewOnceMessageV2: &waE2E.FutureProofMessage{
				Message: &waE2E.Message{ImageMessage: viewOnceImage(up, true)},
			}}
		}},
	} {
		t.Run(probe.name, func(t *testing.T) {
			sent := liveSendRaw(t, counterpart, subjectJID, probe.build)
			say("sent %s as %s", probe.name, sent)

			// Ninety seconds rather than a spell: the ordinary ending here is whatsmeow
			// asking the primary phone to forward the real message, and a phone that is
			// going to answer answers well inside that.
			body := watching.awaitMessage(t, sent, 90*time.Second)
			reportWhatArrived(t, probe.name, sent, body, watching)
		})
	}
}

// viewOnceImage is the image message WhatsApp expects for an upload, with or without the
// flag the sender sets to mean "seen once".
func viewOnceImage(up *wm.UploadResponse, once bool) *waE2E.ImageMessage {
	image := &waE2E.ImageMessage{
		Mimetype:      proto.String("image/png"),
		URL:           proto.String(up.URL),
		DirectPath:    proto.String(up.DirectPath),
		MediaKey:      up.MediaKey,
		FileEncSHA256: up.FileEncSHA256,
		FileSHA256:    up.FileSHA256,
		FileLength:    proto.Uint64(up.FileLength),
	}
	if once {
		image.ViewOnce = proto.Bool(true)
	}
	return image
}

// liveSendRaw uploads the file once per probe and sends whatever the probe builds out of
// it, through whatsmeow directly.
//
// Directly, and not through `message.send`, because the contract has no way to say "seen
// once" -- which is half of what #21 is about. Going around the connector's own outbound
// path is the point here: this phase measures what WhatsApp delivers, not what this build
// sends.
func liveSendRaw(
	t *testing.T, from *Session, to waTypes.JID,
	build func(*wm.UploadResponse) *waE2E.Message,
) string {
	t.Helper()

	client := from.current()
	uploaded, err := client.Upload(t.Context(), onePixelPNG, wm.MediaImage)
	if err != nil {
		t.Fatalf("upload the file: %v", err)
	}
	messageID := client.GenerateMessageID()
	if _, err := client.SendMessage(t.Context(), to.ToNonAD(), build(&uploaded),
		wm.SendRequestExtra{ID: messageID}); err != nil {
		t.Fatalf("send: %v", err)
	}
	return messageID
}

// reportWhatArrived is the measurement itself: what the companion was given, named in the
// terms #21 has to decide between.
//
// It asserts nothing about which ending is right, and that is deliberate. Both endings
// are correct behaviour on this build, the question is which one WhatsApp produces, and a
// phase that failed on one of them would be asserting the answer it was written to find.
func reportWhatArrived(t *testing.T, probe, sent string, body json.RawMessage, watching *recorder) {
	t.Helper()

	var message struct {
		ID      string          `json:"id"`
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(body, &message); err != nil {
		t.Fatalf("unmarshal the message: %v", err)
	}
	var said struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
		Kind   string `json:"kind"`
		Ref    any    `json:"ref"`
		Thumb  string `json:"thumbnail"`
	}
	if err := json.Unmarshal(message.Content, &said); err != nil {
		t.Fatalf("unmarshal the content: %v", err)
	}

	switch {
	case said.Type == "unsupported":
		say("MEASURED %-52s -> the bytes never reached the companion: %s/%s", probe, said.Type, said.Reason)
	case said.Type == "media" && said.Ref != nil:
		say("MEASURED %-52s -> the bytes reached the companion and the file was kept (kind=%s, thumbnail=%d bytes)",
			probe, said.Kind, len(said.Thumb))
	case said.Type == "media":
		// No ref has more than one cause, which is the trap the control fell into. The
		// reason beside the message is what separates "this build would not keep it" from
		// "this instance had nowhere to put it", and a phase that did not read it would
		// report the second as the first.
		say("MEASURED %-52s -> the bytes reached the companion, no file kept, reason=%s (kind=%s, thumbnail=%d bytes)",
			probe, whyNoFile(t, sent, watching), said.Kind, len(said.Thumb))
	default:
		say("MEASURED %-52s -> something else arrived: %s", probe, message.Content)
	}
}

// whyNoFile reads the reason the connector published beside a message whose file it did
// not keep.
func whyNoFile(t *testing.T, sent string, watching *recorder) string {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		failure := watching.await(t, protocol.EventMediaDownloadFailed, time.Until(deadline))
		var why protocol.MediaDownloadFailure
		if err := json.Unmarshal(failure.Payload, &why); err != nil {
			t.Fatalf("unmarshal the failure: %v", err)
		}
		if why.MessageID == sent {
			return why.Reason
		}
	}
	return "none published"
}
