package whatsmeow

import (
	"context"
	"errors"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	"go.mau.fi/whatsmeow/util/gcmutil"
	"go.mau.fi/whatsmeow/util/hkdfutil"
	"google.golang.org/protobuf/proto"

	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// WhatsApp keeps a file on its CDN for a bounded time and then drops it, and the sender's
// phone often still has it. Without the request to upload it again, a message that sat in
// a backlog -- a session that was down, an account resumed after a while -- loses its
// attachment for good on the 404.
func TestAFileWhatsAppDroppedIsAskedForAgainAndFetchedFromTheNewPath(t *testing.T) {
	t.Parallel()

	session, phone := reuploadSession(t, "3EB0GONE")
	phone.answersWith(reuploaded("/v/fresh-path"))

	ref := refetch(t, session, "3EB0GONE", nil)
	if ref.Kind != protocol.MediaRefConnectorBlob {
		t.Fatalf("the refetch answered a %q reference, want the blob it wrote from the new path", ref.Kind)
	}
	if phone.asked() != 1 {
		t.Fatalf("the sender's phone was asked %d times, want once", phone.asked())
	}
}

// A 403 is the key having lapsed, and nothing the phone does brings it back. Asking
// anyway spends the caller's deadline waiting for an answer that cannot help.
func TestAKeyThatLapsedIsNotAskedAboutAtAll(t *testing.T) {
	t.Parallel()

	session, phone := reuploadSession(t, "3EB0403")
	session.download = func(context.Context, *wm.Client, wm.DownloadableMessage, media.File) error {
		return wm.ErrMediaDownloadFailedWith403
	}

	_, err := refetchErr(session, "3EB0403", nil)
	assertCode(t, err, protocol.ErrorMediaUnavailable)
	if phone.asked() != 0 {
		t.Error("a file whose key had lapsed was still asked about on the sender's phone")
	}
}

// The two answers that are not a new path, and both leave the caller exactly where the
// 404 left it. Reported as what the download said rather than as what the phone said:
// the phone declining tells the caller nothing it did not already know.
func TestAPhoneThatWillNotUploadTheFileAgainLeavesTheDownloadsOwnAnswer(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		answer *reupload
	}{
		{"the phone does not have it either", &reupload{failure: &waEvents.MediaRetryError{Code: 2}}},
		{"the phone answers with no path", reuploaded("")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, phone := reuploadSession(t, "3EB0NOPE")
			phone.answersWith(tc.answer)

			_, err := refetchErr(session, "3EB0NOPE", nil)
			assertCode(t, err, protocol.ErrorMediaUnavailable)
			if phone.asked() != 1 {
				t.Errorf("the sender's phone was asked %d times, want once", phone.asked())
			}
		})
	}
}

// A phone that is off answers nothing, and the wait has to end on the caller's deadline
// rather than on one this invents. The command comes back with what the download said,
// and the caller is free to ask again later.
func TestAPhoneThatNeverAnswersEndsOnTheCallersDeadline(t *testing.T) {
	t.Parallel()

	session, phone := reuploadSession(t, "3EB0QUIET")
	phone.answersWith(nil)

	deadline, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err := session.downloadMedia(deadline, &protocol.Command{
		Type:    protocol.CommandMessageDownloadMedia,
		Payload: []byte(`{"message_id":"3EB0QUIET"}`),
	})
	assertCode(t, err, protocol.ErrorMediaUnavailable)
	if phone.asked() != 1 {
		t.Errorf("the sender's phone was asked %d times, want once", phone.asked())
	}
}

// An answer whose command has already given up is dropped rather than kept. Held, it
// would be held for a caller that is not coming back, and the next request for the same
// message would read the stale one.
func TestAnAnswerNobodyIsWaitingForIsDropped(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	if !session.handle(&waEvents.MediaRetry{MessageID: "3EB0NOBODY"}) {
		t.Error("an answer nobody was waiting for was left for WhatsApp to send again")
	}
	session.reuploadsMu.Lock()
	held := len(session.reuploads)
	session.reuploadsMu.Unlock()
	if held != 0 {
		t.Errorf("%d answers are being held for callers that are not coming back", held)
	}
}

// reuploadSession is a session holding the coordinates of one message whose file
// WhatsApp has dropped: the first download answers 404, and what happens next is what
// each test is about.
func reuploadSession(t *testing.T, messageID string) (*Session, *phoneAsked) {
	t.Helper()

	session, downloads := mediaSession(t, media.Options{})
	downloads.answer([]byte("os bytes originais"), nil)
	connect(session)
	if _, acknowledged := deliver(t, session, imageEvent(messageID), 1); !acknowledged {
		t.Fatal("a media message with a file was left unacknowledged")
	}

	kept, found, err := session.store.MediaPart(t.Context(), messageID)
	if err != nil || !found {
		t.Fatalf("MediaPart: %v (found %v)", err, found)
	}
	phone := &phoneAsked{session: session}
	session.askReupload = phone.hand
	// The path the message carried is gone; the one the phone answers with is not. Which
	// of the two the second download is given is the whole question.
	session.download = func(_ context.Context, _ *wm.Client, part wm.DownloadableMessage, file media.File) error {
		if part.GetDirectPath() == kept.DirectPath {
			return wm.ErrMediaDownloadFailedWith404
		}
		_, err := file.Write([]byte("os bytes que o telefone subiu de novo"))
		return err
	}
	return session, phone
}

// phoneAsked stands in for the sender's phone: it counts what it was asked and answers
// on the session's own event path, the way a real one does.
type phoneAsked struct {
	session *Session
	answer  *reupload
	calls   int
}

// reupload is what the phone will answer: either a notification it seals under the media
// key, or the bare error a phone sends when it cannot.
type reupload struct {
	notification *waMmsRetry.MediaRetryNotification
	failure      *waEvents.MediaRetryError
}

func (p *phoneAsked) answersWith(answer *reupload) { p.answer = answer }

func (p *phoneAsked) asked() int { return p.calls }

func (p *phoneAsked) hand(_ context.Context, _ *wm.Client, info *waTypes.MessageInfo, key []byte) error {
	p.calls++
	if info.ID == "" {
		return errors.New("a receipt was sent naming no message")
	}
	if p.answer == nil {
		return nil
	}
	event := &waEvents.MediaRetry{MessageID: info.ID, Error: p.answer.failure}
	if p.answer.notification != nil {
		sealed, iv, err := sealReupload(info.ID, key, p.answer.notification)
		if err != nil {
			return err
		}
		event.Ciphertext, event.IV = sealed, iv
	}
	// Through the handler, because that is the only way a real answer arrives and the
	// routing to the waiting command is what this has to exercise.
	go p.session.handle(event)
	return nil
}

// reuploaded is the phone saying it uploaded the file again, at the path given.
func reuploaded(path string) *reupload {
	return &reupload{notification: &waMmsRetry.MediaRetryNotification{
		Result:     waMmsRetry.MediaRetryNotification_SUCCESS.Enum(),
		DirectPath: proto.String(path),
	}}
}

// sealReupload encrypts a notification the way the phone does, so the decryption this
// exercises is whatsmeow's own and not a stand-in for it.
func sealReupload(
	messageID string, mediaKey []byte, notification *waMmsRetry.MediaRetryNotification,
) (ciphertext, iv []byte, err error) {
	plaintext, err := proto.Marshal(notification)
	if err != nil {
		return nil, nil, err
	}
	iv = make([]byte, 12)
	key := hkdfutil.SHA256(mediaKey, nil, []byte("WhatsApp Media Retry Notification"), 32)
	ciphertext, err = gcmutil.Encrypt(key, iv, plaintext, []byte(messageID))
	return ciphertext, iv, err
}

// A group receipt names the participant who sent the message, and a row written before
// that was kept has nobody to name. Sent anyway it would ask about a message WhatsApp
// cannot identify; the caller gets what the download said instead.
func TestAGroupRowWithNobodyNamedIsNotAskedAbout(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		chat  string
		asked int
	}{
		// A direct chat has nobody else to name: the chat is the person, so the receipt
		// can still be addressed from what the row has.
		{"a direct chat", "phone", 1},
		{"a group", "group", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, phone := reuploadSession(t, "3EB0NONAME")
			phone.answersWith(nil)
			forgetWhoSent(t, session, "3EB0NONAME", tc.chat)

			deadline, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			_, err := session.downloadMedia(deadline, &protocol.Command{
				Type:    protocol.CommandMessageDownloadMedia,
				Payload: []byte(`{"message_id":"3EB0NONAME"}`),
			})
			assertCode(t, err, protocol.ErrorMediaUnavailable)
			if phone.asked() != tc.asked {
				t.Errorf("the sender's phone was asked %d times, want %d", phone.asked(), tc.asked)
			}
		})
	}
}

// forgetWhoSent rewrites the kept row the way one written before the sender was kept
// looks, in the chat named.
func forgetWhoSent(t *testing.T, session *Session, messageID, chatKind string) {
	t.Helper()

	kept, found, err := session.store.MediaPart(t.Context(), messageID)
	if err != nil || !found {
		t.Fatalf("MediaPart: %v (found %v)", err, found)
	}
	kept.Sender = ""
	kept.ChatKind = chatKind
	if chatKind == "group" {
		kept.ChatID = "120363041234567890"
	}
	if err := session.store.PutMediaPart(t.Context(), &kept, time.Now()); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
}
