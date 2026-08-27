package whatsmeow

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// The whole of M2.2c in one test: a blob is instance-local and time-bounded, so the
// reference published with the event stops working, and the client comes back for the
// file. Without this it is answered `unsupported`, which the client reads as final and
// marks the message unsupported for good.
func TestAFileIsFetchedAgainAfterTheBlobItWasPublishedWithIsGone(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer([]byte("os mesmos bytes"), nil)
	connect(session)

	emissions, acknowledged := deliver(t, session, imageEvent("3EB0AGAIN"), 1)
	if !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}
	first := mediaContentOf(t, emissions[0]).Ref
	if first == nil {
		t.Fatal("a message whose file was downloaded was published with no reference")
	}

	// The instance that wrote it is replaced, and the blob goes with it.
	replaceInstance(t, session)

	second := refetch(t, session, "3EB0AGAIN", nil)
	if second.URL == first.URL {
		t.Fatal("the refetch handed back the address of an instance that is gone")
	}
	if !strings.HasPrefix(second.URL, "http://connector-d4e5f6:8080/") {
		t.Fatalf("the refetch published under %q, want the address of the instance answering it", second.URL)
	}
	if second.Kind != protocol.MediaRefConnectorBlob {
		t.Fatalf("the refetch answered a %q reference, want the blob it just wrote", second.Kind)
	}
	if second.Size != first.Size || second.SHA256 != first.SHA256 {
		t.Fatalf("the refetch served %d bytes (%s), want the same file as %d (%s)",
			second.Size, second.SHA256, first.Size, first.SHA256)
	}
	if downloads.count() != 2 {
		t.Fatalf("the file was downloaded %d times, want one for the message and one for the refetch", downloads.count())
	}
}

// A message this session never kept anything for is final: nothing about asking again
// brings the coordinates back, and `media_unavailable` is what tells the client to stop
// rather than spend its retry ladder.
func TestAMessageNothingWasKeptForIsGivenUpOnRatherThanRetried(t *testing.T) {
	t.Parallel()

	session, _ := mediaSession(t, media.Options{})
	connect(session)

	_, err := refetchErr(session, "3EB0NEVER", nil)
	assertCode(t, err, protocol.ErrorMediaUnavailable)
}

// The download goes out over this session's own socket. A session that is down fetches
// nothing, and that is worth waiting for rather than giving up on.
func TestAFileIsNotFetchedAgainWhileTheSessionIsDown(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer([]byte("os mesmos bytes"), nil)
	connect(session)
	if _, acknowledged := deliver(t, session, imageEvent("3EB0DOWN"), 1); !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}

	disconnect(session)
	_, err := refetchErr(session, "3EB0DOWN", nil)
	assertCode(t, err, protocol.ErrorNotConnected)
	if downloads.count() != 1 {
		t.Fatalf("a session that is down still spent %d downloads", downloads.count()-1)
	}
}

// An instance with no media root has nowhere to put the answer. Deliberately not
// `unsupported`, which the client reads as final: this is a deployment somebody has to
// fix, and marking the message unsupported would hide it.
func TestARefetchByAnInstanceWithNowhereToPutTheFileIsRetriedRatherThanGivenUpOn(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	connect(session)

	_, err := refetchErr(session, "3EB0NOROOT", nil)
	assertCode(t, err, protocol.ErrorInternal)
}

// Nothing is kept for a message whose file never arrived: the client was told the file
// is not coming and will not come back for it, and a row would be the key to a file that
// cannot be fetched.
func TestNothingIsKeptForAMessageWhoseFileWasNeverFetched(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer(nil, wm.ErrMediaDownloadFailedWith404)
	connect(session)

	if _, acknowledged := deliver(t, session, imageEvent("3EB0GONE"), 2); !acknowledged {
		t.Fatal("a message whose file is gone for good was left to be redelivered forever")
	}
	if _, found, err := session.store.MediaPart(t.Context(), session.sid, "3EB0GONE"); err != nil || found {
		t.Fatalf("a message whose file never arrived was kept anyway (found=%v, err=%v)", found, err)
	}
}

// The concrete type is half the address: whatsmeow reads the media type off it and asks
// a different endpoint for each, so a kind rebuilt as the wrong type fetches nothing.
func TestEveryKindIsRebuiltAsTheMessageTypeItWasDownloadedFrom(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		kind protocol.MediaKind
		want wm.DownloadableMessage
	}{
		{protocol.MediaImage, &waE2E.ImageMessage{}},
		{protocol.MediaVideo, &waE2E.VideoMessage{}},
		{protocol.MediaAudio, &waE2E.AudioMessage{}},
		{protocol.MediaDocument, &waE2E.DocumentMessage{}},
		{protocol.MediaSticker, &waE2E.StickerMessage{}},
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			t.Parallel()

			got, err := downloadableOf(&store.MediaPart{Kind: string(tc.kind), DirectPath: directPath})
			if err != nil {
				t.Fatalf("downloadableOf(%s): %v", tc.kind, err)
			}
			if wm.GetMediaType(got) != wm.GetMediaType(tc.want) {
				t.Fatalf("%s rebuilt as %T, which whatsmeow downloads as %q rather than %q",
					tc.kind, got, wm.GetMediaType(got), wm.GetMediaType(tc.want))
			}
		})
	}

	// A kind an older or a newer build wrote. Guessing a type would ask WhatsApp for the
	// file under the wrong endpoint, which fails in a way nobody can read.
	if _, err := downloadableOf(&store.MediaPart{Kind: "hologram"}); err == nil {
		t.Fatal("a kind this build cannot fetch was rebuilt as something anyway")
	}
}

// The coordinates that came off the message are what the refetch goes out with. One
// wrong byte and WhatsApp hands back nothing, or bytes that fail their digest.
func TestARefetchGoesOutWithTheCoordinatesTheMessageCarried(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer([]byte("os mesmos bytes"), nil)
	connect(session)

	document := mediaEvent("3EB0DOC", &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
		Mimetype: proto.String("application/pdf"), FileName: proto.String("recibo de março.pdf"),
		Caption:    proto.String("segue o recibo"),
		DirectPath: proto.String(directPath), MediaKey: []byte("key"), FileEncSHA256: encSHA256(),
		FileSHA256: []byte("plain"),
	}})
	if _, acknowledged := deliver(t, session, document, 1); !acknowledged {
		t.Fatal("a document with a file was left unacknowledged")
	}

	kept, found, err := session.store.MediaPart(t.Context(), session.sid, "3EB0DOC")
	if err != nil || !found {
		t.Fatalf("a document that was downloaded was not kept (found=%v, err=%v)", found, err)
	}
	if kept.DirectPath != directPath || string(kept.MediaKey) != "key" || string(kept.FileSHA256) != "plain" {
		t.Fatalf("what was kept is %+v, want the coordinates the message carried", kept)
	}
	if kept.Filename != "recibo de março.pdf" || kept.Mime != "application/pdf" {
		t.Fatalf("the file is kept as %q (%s), want the name and type it was published under", kept.Filename, kept.Mime)
	}
}

// The length kept is the one that was written, not the one the sender announced. They
// only differ for a sender who understated, and that is exactly the file a lowered cap
// should refuse before downloading it a second time rather than after.
func TestTheLengthKeptIsTheOneThatWasWrittenAndNotTheOneTheSenderClaimed(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	const file = "quinze bytes ok"
	downloads.answer([]byte(file), nil)
	connect(session)

	overstated := mediaEvent("3EB0LIED", &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Mimetype: proto.String("image/jpeg"), FileLength: proto.Uint64(9_000_000),
		DirectPath: proto.String(directPath), MediaKey: []byte("key"), FileEncSHA256: encSHA256(),
	}})
	if _, acknowledged := deliver(t, session, overstated, 1); !acknowledged {
		t.Fatal("an image with a file was left unacknowledged")
	}

	kept, found, err := session.store.MediaPart(t.Context(), session.sid, "3EB0LIED")
	if err != nil || !found {
		t.Fatalf("an image that was downloaded was not kept (found=%v, err=%v)", found, err)
	}
	if kept.FileLength != int64(len(file)) {
		t.Fatalf("the length kept is %d, want the %d that were written", kept.FileLength, len(file))
	}
}

// A message id is the sender's to choose, so two chats under one account can carry the
// same one and the second row replaces the first. Rare enough that this cannot resolve
// it -- the client keys by message id too -- but it must not resolve it wrongly: handing
// the file over would put one conversation's attachment in another's thread.
func TestADownloadForADifferentChatThanTheOneOnRecordIsRefused(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer([]byte("os mesmos bytes"), nil)
	connect(session)
	if _, acknowledged := deliver(t, session, imageEvent("3EB0MINE"), 1); !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}

	_, err := refetchErr(session, "3EB0MINE", &protocol.Address{Kind: protocol.AddressPhone, ID: "5511000000000"})
	assertCode(t, err, protocol.ErrorMediaUnavailable)
	if downloads.count() != 1 {
		t.Fatalf("a refused download still fetched the file (%d downloads)", downloads.count())
	}

	// And the chat it really is filed under is served, so the check is a check and not a
	// refusal of everything.
	chat, ok := chatOf(imageEvent("3EB0MINE"))
	if !ok {
		t.Fatal("the test event has no chat")
	}
	if ref := refetch(t, session, "3EB0MINE", &chat); ref.ID == "" {
		t.Fatal("a download naming the chat the message is in was not served")
	}
}

// The command makes `chat` optional, and a caller that omits it is no worse off than
// before this column existed.
func TestADownloadThatNamesNoChatIsStillServed(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer([]byte("os mesmos bytes"), nil)
	connect(session)
	if _, acknowledged := deliver(t, session, imageEvent("3EB0NOCHAT"), 1); !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}

	if ref := refetch(t, session, "3EB0NOCHAT", nil); ref.ID == "" {
		t.Fatal("a download that named no chat was not served")
	}
}

// The chat a file is filed under is the one the event was published under, and the two
// come from the same function so they cannot drift apart.
func TestTheChatAFileIsFiledUnderIsTheOneItsMessageWasPublishedUnder(t *testing.T) {
	t.Parallel()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer([]byte("os mesmos bytes"), nil)
	connect(session)

	emissions, acknowledged := deliver(t, session, imageEvent("3EB0FILED"), 1)
	if !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}
	var published struct {
		Message protocol.InboundMessage `json:"message"`
	}
	raw, err := json.Marshal(emissions[0].Payload)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := json.Unmarshal(raw, &published); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	kept, found, err := session.store.MediaPart(t.Context(), session.sid, "3EB0FILED")
	if err != nil || !found {
		t.Fatalf("a message that was downloaded was not kept (found=%v, err=%v)", found, err)
	}
	if kept.ChatKind != string(published.Message.Chat.Kind) || kept.ChatID != published.Message.Chat.ID {
		t.Fatalf("the file is filed under %s:%s and its message was published under %s:%s",
			kept.ChatKind, kept.ChatID, published.Message.Chat.Kind, published.Message.Chat.ID)
	}
}

// --- helpers ------------------------------------------------------------------------

func connect(session *Session) {
	session.mu.Lock()
	session.connected = true
	session.mu.Unlock()
}

func disconnect(session *Session) {
	session.mu.Lock()
	session.connected = false
	session.mu.Unlock()
}

// refetch runs `message.download_media` and insists it worked.
func refetch(t *testing.T, session *Session, messageID string, chat *protocol.Address) protocol.MediaRef {
	t.Helper()

	ref, err := refetchErr(session, messageID, chat)
	if err != nil {
		t.Fatalf("message.download_media(%s): %v", messageID, err)
	}
	return ref
}

func refetchErr(session *Session, messageID string, chat *protocol.Address) (protocol.MediaRef, error) {
	payload, err := json.Marshal(map[string]any{"message_id": messageID, "chat": chat})
	if err != nil {
		return protocol.MediaRef{}, err
	}
	raw, err := session.Execute(context.Background(), &protocol.Command{
		Type: protocol.CommandMessageDownloadMedia, Payload: payload,
	})
	if err != nil {
		return protocol.MediaRef{}, err
	}
	var ref protocol.MediaRef
	err = json.Unmarshal(raw, &ref)
	return ref, err
}

// replaceInstance stands in for a deploy: the same database, a media store that has
// never seen this blob, and a new address to publish under. It is the case issue #19 is
// about, and the one an operator cannot avoid.
func replaceInstance(t *testing.T, session *Session) {
	t.Helper()

	blobs, err := media.New(media.Options{Root: t.TempDir(), Now: func() time.Time { return storedAt }})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = blobs.Close() })
	session.blobs = blobs
	session.blobBase = "http://connector-d4e5f6:8080"
}
