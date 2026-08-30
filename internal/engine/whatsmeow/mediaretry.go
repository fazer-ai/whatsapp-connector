package whatsmeow

import (
	"context"
	"errors"
	"fmt"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// WhatsApp keeps a file on its CDN for a bounded time and then drops it, and the sender's
// phone often still has it. The protocol has a recovery for exactly that: a receipt
// asking the phone to upload the file again, answered later with a fresh path to fetch it
// from.
//
// It lives here, on `message.download_media`, and not on the inbound path. Two things put
// it here. The inbound handler runs under whatsmeow's node loop with the ack withheld
// until the event is published, and whatsmeow starts the next node's handler alongside it
// after five minutes -- at which point the session's ordering is gone. A wait on somebody
// else's phone answering has no bound the connector can pick, because the phone may be
// off. And the announcement the inbound path would otherwise make is final on the client:
// a message flagged as having no attachment is never asked about again.
//
// Here the caller asked, the caller is waiting, and the command carries a deadline of its
// own that this can honour without inventing one.
func (s *Session) askForReupload(
	ctx context.Context, kept *store.MediaPart,
) (wm.DownloadableMessage, error) {
	info, err := sentAs(kept)
	if err != nil {
		return nil, err
	}

	// Registered before the receipt goes out. The phone can answer faster than this
	// goroutine gets back to the select, and an answer that arrives with nobody
	// listening is one that is dropped and waited out in full.
	answer := s.awaitReupload(kept.MessageID)
	defer s.stopAwaitingReupload(kept.MessageID)

	if err := s.askReupload(ctx, s.current(), info, kept.MediaKey); err != nil {
		return nil, fmt.Errorf("ask the sender's phone to upload the file again: %w", err)
	}

	select {
	case event := <-answer:
		return freshPath(event, kept)
	case <-ctx.Done():
		// The phone did not answer inside what the caller allowed. Not a file that is
		// gone: the caller may ask again, and the phone may be somewhere with signal by
		// then.
		return nil, fmt.Errorf("wait for the sender's phone to upload the file again: %w", ctx.Err())
	}
}

// freshPath reads the phone's answer, which is a new direct path or a reason there is
// none.
func freshPath(event *waEvents.MediaRetry, kept *store.MediaPart) (wm.DownloadableMessage, error) {
	notification, err := wm.DecryptMediaRetryNotification(event, kept.MediaKey)
	if err != nil {
		// ErrMediaNotAvailableOnPhone among them, which is the one genuinely permanent
		// answer on this path: the file is off the CDN and off the phone too.
		return nil, fmt.Errorf("read what the sender's phone answered: %w", err)
	}
	if notification.GetResult() != waMmsRetry.MediaRetryNotification_SUCCESS {
		return nil, fmt.Errorf("%w: the sender's phone answered %s",
			errNoReupload, notification.GetResult())
	}
	if notification.GetDirectPath() == "" {
		// A success with nowhere to fetch from. Refused rather than downloaded from the
		// path that already answered 404, which is what reusing the old one would do.
		return nil, fmt.Errorf("%w: the sender's phone answered with no path", errNoReupload)
	}

	// Everything but the path is unchanged: the file is the same file, so the key that
	// decrypts it and the digests that prove it are the ones the message carried.
	fresh := *kept
	fresh.DirectPath = notification.GetDirectPath()
	return downloadableOf(&fresh)
}

// errNoReupload is the phone having been asked and having declined, which is different
// from not having been asked.
var errNoReupload = errors.New("the file was not uploaded again")

// sentAs rebuilds what the receipt has to be addressed with.
//
// The chat comes from the two columns that always had it. Whether this account sent the
// message, and which participant did, are on the message and were kept for this alone --
// so a row written before they were is one this cannot ask about, and says so rather than
// sending a receipt naming nobody.
func sentAs(kept *store.MediaPart) (*waTypes.MessageInfo, error) {
	chat, err := jidOf(protocol.Address{Kind: protocol.AddressKind(kept.ChatKind), ID: kept.ChatID})
	if err != nil {
		return nil, err
	}
	info := &waTypes.MessageInfo{
		ID: kept.MessageID,
		MessageSource: waTypes.MessageSource{
			Chat:     chat,
			IsFromMe: kept.FromMe,
			IsGroup:  chat.Server == waTypes.GroupServer,
		},
	}
	if kept.Sender == "" {
		if info.IsGroup {
			// A group receipt names the participant, and there is nobody to name.
			return nil, fmt.Errorf("%w: nothing was kept about who sent it", errNoReupload)
		}
		// A direct chat is the person, so the chat is the sender.
		info.Sender = chat
		return info, nil
	}
	sender, err := waTypes.ParseJID(kept.Sender)
	if err != nil {
		return nil, fmt.Errorf("%w: what was kept about who sent it is unreadable", errNoReupload)
	}
	info.Sender = sender
	return info, nil
}

// awaitReupload takes a place in the queue for the answer to one message's request, and
// stopAwaitingReupload gives it up.
//
// Buffered by one and never blocked on: the answer arrives on whatsmeow's node handler,
// and a handler waiting for this command to come back for it would hold the node loop and
// everything behind it.
func (s *Session) awaitReupload(messageID string) chan *waEvents.MediaRetry {
	answer := make(chan *waEvents.MediaRetry, 1)
	s.reuploadsMu.Lock()
	s.reuploads[messageID] = answer
	s.reuploadsMu.Unlock()
	return answer
}

func (s *Session) stopAwaitingReupload(messageID string) {
	s.reuploadsMu.Lock()
	delete(s.reuploads, messageID)
	s.reuploadsMu.Unlock()
}

// reupload hands a phone's answer to whoever asked for it, and drops it when nobody did.
//
// Dropped rather than kept, because this only ever arrives for a request this connector
// made: an answer with no waiter is one whose command has already given up, and holding
// it would be holding it for a caller that is not coming back.
func (s *Session) reupload(event *waEvents.MediaRetry) bool {
	s.reuploadsMu.Lock()
	answer, waiting := s.reuploads[event.MessageID]
	s.reuploadsMu.Unlock()
	if !waiting {
		s.log.Debug().Str("message_id", event.MessageID).
			Msg("a phone answered about a file nobody is waiting for any more")
		return true
	}
	select {
	case answer <- event:
	default:
		// One request, one answer. A second is the same request answered twice, and the
		// waiter has the first.
	}
	return true
}
