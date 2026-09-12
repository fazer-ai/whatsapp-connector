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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

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
	// The raw event beside the published frame, because the published frame cannot answer
	// the question. `mediaBody` returns at the `viewOnce` guard BEFORE fetching anything,
	// so a view-once arm that reports `type=media` has proved that the media envelope
	// decrypted -- the URL, the path, the keys -- and nothing at all about the bytes those
	// coordinates point at. Downloading them with the recipient's own client is the only
	// thing that separates "the file is there" from "a description of a file is there".
	arrived := liveRawMedia(t, subject)

	// Three shapes, because "a view-once image" is three different things on the wire and
	// WhatsApp may well treat them differently. The flag on the media is what the field
	// in `viewOnce()` reads; the V2 envelope is what a current client actually sends; and
	// the plain image beside them is the control, without which a silence here says
	// nothing -- a run where nothing arrives at all proves the harness broken, not
	// WhatsApp withholding.
	for _, probe := range []struct {
		name    string
		control bool
		build   func(*wm.UploadResponse) *waE2E.Message
	}{
		{"a plain image, as the control", true, func(up *wm.UploadResponse) *waE2E.Message {
			return &waE2E.Message{ImageMessage: viewOnceImage(up, false)}
		}},
		{"the flag on the image itself", false, func(up *wm.UploadResponse) *waE2E.Message {
			return &waE2E.Message{ImageMessage: viewOnceImage(up, true)}
		}},
		{"wrapped in the V2 envelope, as a current client sends it", false, func(up *wm.UploadResponse) *waE2E.Message {
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
			reportWhatArrived(t, probe.name, sent, body, watching, subject, arrived, probe.control)
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
// The control asserts and the view-once arms report, and the asymmetry is the whole
// design. A control that only logged its outcome would be a control that cannot fail,
// which is what it exists for: if the plain image does not arrive and download and match
// the bytes that were sent, the harness is broken and every conclusion below it is noise.
// The view-once arms are the opposite -- both endings are correct behaviour on this build,
// the question is which one WhatsApp produces, and a phase that failed on one of them
// would be asserting the answer it was written to find.
func reportWhatArrived(
	t *testing.T, probe, sent string, body json.RawMessage, watching *recorder,
	subject *Session, arrived func(*testing.T, string) *waEvents.Message, control bool,
) {
	t.Helper()

	var message struct {
		ID      string          `json:"id"`
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(body, &message); err != nil {
		t.Fatalf("unmarshal the message: %v", err)
	}
	var said struct {
		Type   string             `json:"type"`
		Reason string             `json:"reason"`
		Kind   string             `json:"kind"`
		Ref    *protocol.MediaRef `json:"ref"`
		Thumb  string             `json:"thumbnail"`
	}
	if err := json.Unmarshal(message.Content, &said); err != nil {
		t.Fatalf("unmarshal the content: %v", err)
	}

	if control {
		if said.Type != "media" {
			t.Fatalf("the control arrived as %q/%q, so nothing below it means anything: the harness is broken, not WhatsApp",
				said.Type, said.Reason)
		}
		if said.Ref == nil {
			t.Fatalf("the control kept no file (reason=%s), so 'no ref' stops telling a refused view-once from a broken store",
				whyNoFile(t, sent, watching))
		}
		got := liveDownload(t, subject, arrived(t, sent))
		if !bytes.Equal(got, onePixelPNG) {
			t.Fatalf("the control downloaded %d bytes and %d were sent: the file did not survive the round trip",
				len(got), len(onePixelPNG))
		}
		say("MEASURED %-52s -> arrived, kept, and %d bytes downloaded match what was sent", probe, len(got))
		return
	}

	switch {
	case said.Type == "unsupported":
		say("MEASURED %-52s -> nothing reached the companion but a stub: %s/%s", probe, said.Type, said.Reason)
	case said.Type != "media":
		say("MEASURED %-52s -> something else arrived: %s", probe, message.Content)
	default:
		// The published frame says the envelope decrypted. Only this download says the
		// bytes it describes are really there and really the ones that were sent, which is
		// the question #21 asks and the one a `type=media` alone does not answer.
		why := whyNoFile(t, sent, watching)
		got := liveDownload(t, subject, arrived(t, sent))
		switch {
		case got == nil:
			say("MEASURED %-52s -> the envelope decrypted but the bytes could not be fetched (kind=%s, reason=%s)",
				probe, said.Kind, why)
		case !bytes.Equal(got, onePixelPNG):
			say("MEASURED %-52s -> the bytes fetched are not the ones sent: %d vs %d (kind=%s, reason=%s)",
				probe, len(got), len(onePixelPNG), said.Kind, why)
		default:
			say("MEASURED %-52s -> the BYTES reached the companion (%d downloaded, identical to what was sent); this build kept nothing, reason=%s (kind=%s, thumbnail=%d bytes)",
				probe, len(got), why, said.Kind, len(said.Thumb))
		}
	}
}

// liveRawMedia records the decrypted messages whatsmeow hands the recipient, so a phase
// can reach the media coordinates the session itself never publishes.
//
// A second handler on the same client rather than anything inside the Session: it sees
// every event the production handler sees, changes nothing about what that handler does,
// and the thing being measured stays untouched.
//
// Waited on rather than read once, and the first version of this got it wrong. whatsmeow
// runs handlers in the order they were added, so the production one publishes the frame
// before this one has written the raw event down: a phase that reads the map the moment
// `awaitMessage` returns is racing its own recorder, and loses often enough to fail two
// arms out of three. The deadline is what the wait is bounded by, not a spell of sleeping:
// the event either arrives or the phase says it never did.
func liveRawMedia(t *testing.T, session *Session) func(*testing.T, string) *waEvents.Message {
	t.Helper()

	var mu sync.Mutex
	seen := map[string]*waEvents.Message{}
	landed := make(chan struct{}, 64)
	session.current().AddEventHandler(func(event any) {
		message, ok := event.(*waEvents.Message)
		if !ok {
			return
		}
		mu.Lock()
		seen[message.Info.ID] = message
		mu.Unlock()
		select {
		case landed <- struct{}{}:
		default:
		}
	})
	return func(t *testing.T, id string) *waEvents.Message {
		t.Helper()

		deadline := time.After(30 * time.Second)
		for {
			mu.Lock()
			message, ok := seen[id]
			mu.Unlock()
			if ok {
				return message
			}
			select {
			case <-landed:
			case <-deadline:
				return nil
			}
		}
	}
}

// liveDownload fetches the file a received message describes, with the recipient's own
// client, and hands back nil when there is nothing to fetch or the fetch fails.
//
// Nil rather than a fatal: for a view-once arm, a download that does not work IS one of
// the answers this phase is looking for, and stopping on it would throw the measurement
// away in the case that is most worth reporting.
func liveDownload(t *testing.T, session *Session, message *waEvents.Message) []byte {
	t.Helper()

	if message == nil {
		t.Fatalf("the raw event for that message was never seen, so its bytes cannot be checked")
	}
	body := message.Message
	if wrapped := body.GetViewOnceMessageV2().GetMessage(); wrapped != nil {
		body = wrapped
	}
	if wrapped := body.GetViewOnceMessage().GetMessage(); wrapped != nil {
		body = wrapped
	}
	image := body.GetImageMessage()
	if image == nil {
		t.Fatalf("the message carried no image to download: %T", body)
	}
	fetching, stop := context.WithTimeout(t.Context(), 60*time.Second)
	defer stop()
	got, err := session.current().Download(fetching, image)
	if err != nil {
		say("the download of %s failed: %v", message.Info.ID, err)
		return nil
	}
	return got
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
