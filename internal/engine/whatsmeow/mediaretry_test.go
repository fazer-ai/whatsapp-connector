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
	"github.com/fazer-ai/whatsapp-connector/internal/store"
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
	session     *Session
	answer      *reupload
	calls       int
	chat        string
	participant string
}

// reupload is what the phone will answer: either a notification it seals under the media
// key, or the bare error a phone sends when it cannot.
type reupload struct {
	notification *waMmsRetry.MediaRetryNotification
	failure      *waEvents.MediaRetryError
}

func (p *phoneAsked) answersWith(answer *reupload) { p.answer = answer }

func (p *phoneAsked) asked() int { return p.calls }

// addressed is the chat the last receipt named, which is the whole of what a broadcast
// gets wrong when the published chat is used instead of the one it was sent to.
func (p *phoneAsked) addressed() (chat, participant string) { return p.chat, p.participant }

func (p *phoneAsked) hand(_ context.Context, _ *wm.Client, info *waTypes.MessageInfo, key []byte) error {
	p.calls++
	p.chat = info.Chat.String()
	// What SendMediaRetryReceipt would put on the `rmr` node, and the flag that decides
	// whether it does.
	p.participant = ""
	if info.IsGroup {
		p.participant = info.Sender.String()
	}
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

// A receipt is addressed by where the message was sent, by whether this account sent it
// and, in a group, by which participant did. A row that has none of those -- one written
// before they were kept -- cannot be asked about, and a group row that has the chat but
// nobody to name cannot either. Sent anyway, either would name a message WhatsApp cannot
// find, answered by nothing and paid for with the caller's whole deadline.
func TestARowThatCannotAddressAReceiptIsNotAskedAbout(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		forget func(*store.MediaPart)
	}{
		{"a row written before any of it was kept", func(kept *store.MediaPart) {
			kept.ReceiptChat, kept.Sender = "", ""
		}},
		{"a group whose participant was not kept", func(kept *store.MediaPart) {
			kept.ReceiptChat = "120363041234567890@g.us"
			kept.Sender = ""
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, phone := reuploadSession(t, "3EB0NONAME")
			phone.answersWith(nil)
			forgetWhoSent(t, session, "3EB0NONAME", tc.forget)

			deadline, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			_, err := session.downloadMedia(deadline, &protocol.Command{
				Type:    protocol.CommandMessageDownloadMedia,
				Payload: []byte(`{"message_id":"3EB0NONAME"}`),
			})
			assertCode(t, err, protocol.ErrorMediaUnavailable)
			if phone.asked() != 0 {
				t.Errorf("the sender's phone was asked %d times about a row that cannot address a receipt", phone.asked())
			}
		})
	}
}

// forgetWhoSent rewrites the kept row the way the test wants it.
func forgetWhoSent(t *testing.T, session *Session, messageID string, forget func(*store.MediaPart)) {
	t.Helper()

	kept, found, err := session.store.MediaPart(t.Context(), messageID)
	if err != nil || !found {
		t.Fatalf("MediaPart: %v (found %v)", err, found)
	}
	forget(&kept)
	if err := session.store.PutMediaPart(t.Context(), &kept, time.Now()); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}
}

// The contract makes a command's `deadline` optional, and one that omits it runs on the
// session's own lifetime. Without a ceiling of this connector's own, a phone that is
// switched off holds the session's executor -- and every command queued behind it --
// until the session closes.
func TestAPhoneIsWaitedOnEvenWhenTheCommandNamedNoDeadline(t *testing.T) {
	t.Parallel()

	session, phone := reuploadSession(t, "3EB0FOREVER")
	phone.answersWith(nil)
	session.reuploadWait = 50 * time.Millisecond

	answered := make(chan error, 1)
	go func() {
		// t.Context() outlives this call, which is the shape a command with no deadline
		// arrives in: the session's own lifetime and nothing narrower.
		_, err := session.downloadMedia(t.Context(), &protocol.Command{
			Type:    protocol.CommandMessageDownloadMedia,
			Payload: []byte(`{"message_id":"3EB0FOREVER"}`),
		})
		answered <- err
	}()

	select {
	case err := <-answered:
		assertCode(t, err, protocol.ErrorMediaUnavailable)
	case <-time.After(5 * time.Second):
		t.Fatal("a command with no deadline waited on a phone that was never going to answer")
	}
}

// A broadcast is published in the direct chat with whoever sent it, because that is where
// WhatsApp shows it -- so the chat the message was published under is not the chat it was
// sent to. A receipt addressed from the published one names a message WhatsApp cannot
// find, and the caller pays for it with the whole wait.
func TestABroadcastIsAskedAboutUnderTheChatItWasSentTo(t *testing.T) {
	t.Parallel()

	session, phone := reuploadSession(t, "3EB0CAST")
	phone.answersWith(reuploaded("/v/fresh-path"))
	kept, _, err := session.store.MediaPart(t.Context(), "3EB0CAST")
	if err != nil {
		t.Fatalf("MediaPart: %v", err)
	}
	// What remember writes for one: published under the sender, sent to the list.
	kept.ReceiptChat = "5511999990002@broadcast"
	if err := session.store.PutMediaPart(t.Context(), &kept, time.Now()); err != nil {
		t.Fatalf("PutMediaPart: %v", err)
	}

	if _, err := refetchErr(session, "3EB0CAST", nil); err != nil {
		t.Fatalf("refetch: %v", err)
	}
	chat, participant := phone.addressed()
	if chat != "5511999990002@broadcast" {
		t.Errorf("the receipt was addressed to %q, want the chat the message was sent to", chat)
	}
	// whatsmeow puts the participant on the node only for a chat it calls a group, and a
	// broadcast list is one of those. Left off, the phone has no message to match.
	if participant == "" {
		t.Error("the receipt named no participant, so the phone has nothing to match it to")
	}
}

// The blob this writes is dropped on its TTL or evicted by the quota, and the row is what
// the request after that reads. Left pointing at the path that answered 404, that request
// spends the failure and the whole wait on the phone again -- for a file WhatsApp is by
// then serving perfectly well.
func TestAFreshPathIsRememberedSoTheNextCallerDoesNotAskTwice(t *testing.T) {
	t.Parallel()

	session, phone := reuploadSession(t, "3EB0KEEP")
	phone.answersWith(reuploaded("/v/fresh-path"))
	if _, err := refetchErr(session, "3EB0KEEP", nil); err != nil {
		t.Fatalf("refetch: %v", err)
	}

	kept, found, err := session.store.MediaPart(t.Context(), "3EB0KEEP")
	if err != nil || !found {
		t.Fatalf("MediaPart: %v (found %v)", err, found)
	}
	if kept.DirectPath != "/v/fresh-path" {
		t.Errorf("the row still points at %q, want the path the phone answered with", kept.DirectPath)
	}

	// And the proof it is worth something: the next caller gets the file without the
	// phone being asked again.
	if _, err := refetchErr(session, "3EB0KEEP", nil); err != nil {
		t.Fatalf("the second refetch failed: %v", err)
	}
	if phone.asked() != 1 {
		t.Errorf("the sender's phone was asked %d times, want once for the two refetches", phone.asked())
	}
}
