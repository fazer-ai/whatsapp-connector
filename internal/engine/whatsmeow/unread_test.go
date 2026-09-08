package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

func unreadCommand(t *testing.T, payload string) *protocol.Command {
	t.Helper()
	return &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandMessageMarkUnread,
		SID: "s1", Payload: json.RawMessage(payload),
	}
}

// The anchor is what tells WhatsApp which message the chat is unread from, and the
// contract makes it optional. Both shapes have to reach the patch as themselves: a caller
// that tracks its last message gets a range naming it, and one that does not gets a range
// with no message in it, which is still a chat marked unread.
func TestAnUnreadMarkCarriesTheMessageItIsUnreadFromWhenGivenOne(t *testing.T) {
	t.Parallel()

	for _, mark := range []struct {
		name     string
		payload  string
		wantKey  bool
		wantID   string
		wantMine bool
		wantChat string
	}{
		{
			name:    "with the last message named",
			payload: `{"chat":{"kind":"phone","id":"5511999990002"},"last_message_id":"3EB0ABC","from_me":true}`,
			wantKey: true, wantID: "3EB0ABC", wantMine: true, wantChat: "5511999990002@s.whatsapp.net",
		},
		{
			name:    "with no last message at all",
			payload: `{"chat":{"kind":"phone","id":"5511999990002"}}`,
			wantKey: false, wantChat: "5511999990002@s.whatsapp.net",
		},
		{
			name:    "with the last message explicitly null",
			payload: `{"chat":{"kind":"phone","id":"5511999990002"},"last_message_id":null}`,
			wantKey: false, wantChat: "5511999990002@s.whatsapp.net",
		},
		{
			// The schema takes any string, so this is a payload a conforming client may
			// send. It names no message, so it anchors on none -- and it must not reach
			// the key, where an id of "" is a message WhatsApp cannot match.
			name:    "with the last message an empty string",
			payload: `{"chat":{"kind":"phone","id":"5511999990002"},"last_message_id":""}`,
			wantKey: false, wantChat: "5511999990002@s.whatsapp.net",
		},
	} {
		t.Run(mark.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)

			var sent appstate.PatchInfo
			session.sendAppState = func(_ context.Context, _ *wm.Client, patch appstate.PatchInfo) error {
				sent = patch
				return nil
			}

			if _, err := session.Execute(t.Context(), unreadCommand(t, mark.payload)); err != nil {
				t.Fatalf("message.mark_unread: %v", err)
			}
			if len(sent.Mutations) != 1 {
				t.Fatalf("the patch carries %d mutations, want 1", len(sent.Mutations))
			}
			action := sent.Mutations[0].Value.GetMarkChatAsReadAction()
			if action == nil {
				t.Fatal("the patch carries no mark-chat-as-read action")
			}
			// The command is `mark_unread`, so the one thing the patch must never say is
			// that the chat was read: that would clear the badge it was sent to raise.
			if action.GetRead() {
				t.Error("an unread mark went out saying the chat was read")
			}
			// A range ending at zero is a range WhatsApp has no anchor for. whatsmeow
			// fills the moment in when it is left unset, and a patch that lost it would
			// still send, still be acknowledged, and do nothing.
			if action.GetMessageRange().GetLastMessageTimestamp() == 0 {
				t.Error("the patch carries no timestamp, so it names no point to be unread from")
			}
			messages := action.GetMessageRange().GetMessages()
			if !mark.wantKey {
				if len(messages) != 0 {
					t.Fatalf("the patch names %d messages, want none when the caller named none", len(messages))
				}
				return
			}
			if len(messages) != 1 {
				t.Fatalf("the patch names %d messages, want the one the caller gave", len(messages))
			}
			key := messages[0].GetKey()
			if key.GetID() != mark.wantID {
				t.Errorf("the patch is anchored on %q, want %q", key.GetID(), mark.wantID)
			}
			if key.GetFromMe() != mark.wantMine {
				t.Errorf("the anchor says from_me=%v, want %v", key.GetFromMe(), mark.wantMine)
			}
			if key.GetRemoteJID() != mark.wantChat {
				t.Errorf("the anchor is in chat %q, want %q", key.GetRemoteJID(), mark.wantChat)
			}
		})
	}
}

func TestAnUnreadMarkRefusesAPayloadItCannotActOn(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name    string
		payload string
	}{
		{name: "no chat at all", payload: `{}`},
		{name: "a chat that is not an address", payload: `{"chat":{"kind":"nonsense","id":"x"}}`},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.sendAppState = func(context.Context, *wm.Client, appstate.PatchInfo) error {
				t.Error("a payload this connector cannot act on was sent to WhatsApp anyway")
				return nil
			}

			_, err := session.Execute(t.Context(), unreadCommand(t, refused.payload))
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// The same classification the contact queries get, for the same reason: `wa_error` says
// WhatsApp refused and `internal` says to come and read this connector's own logs.
// A key for somebody else's message in a group names its author, and this command's
// payload carries no field for one -- its neighbour `message.mark_read` takes a `sender`
// and this does not. A key without it names a message WhatsApp cannot find, so the anchor
// is dropped rather than sent incomplete: the patch still carries the range, which is
// what marks the chat unread and what a caller naming no message gets anyway.
//
// A message this account sent is the exception, in a group as anywhere else: `from_me`
// identifies it without an author.
func TestAnUnreadMarkDropsAnAnchorItCannotNameTheAuthorOf(t *testing.T) {
	t.Parallel()

	for _, mark := range []struct {
		name    string
		payload string
		wantKey bool
	}{
		{
			name:    "somebody else's message in a group",
			payload: `{"chat":{"kind":"group","id":"120363041234567890"},"last_message_id":"3EB0ABC","from_me":false}`,
			wantKey: false,
		},
		{
			name:    "this account's own message in a group",
			payload: `{"chat":{"kind":"group","id":"120363041234567890"},"last_message_id":"3EB0ABC","from_me":true}`,
			wantKey: true,
		},
		{
			name:    "somebody else's message in a direct chat",
			payload: `{"chat":{"kind":"phone","id":"5511999990002"},"last_message_id":"3EB0ABC","from_me":false}`,
			wantKey: true,
		},
		{
			// The same question the read mark asks, answered from the same table: a
			// broadcast list and a status both have an author the chat does not name.
			name:    "somebody else's message in a broadcast",
			payload: `{"chat":{"kind":"broadcast","id":"120363041234567890"},"last_message_id":"3EB0ABC","from_me":false}`,
			wantKey: false,
		},
		{
			name:    "somebody else's status",
			payload: `{"chat":{"kind":"status","id":"status"},"last_message_id":"3EB0ABC","from_me":false}`,
			wantKey: false,
		},
	} {
		t.Run(mark.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)

			var sent appstate.PatchInfo
			session.sendAppState = func(_ context.Context, _ *wm.Client, patch appstate.PatchInfo) error {
				sent = patch
				return nil
			}

			if _, err := session.Execute(t.Context(), unreadCommand(t, mark.payload)); err != nil {
				t.Fatalf("message.mark_unread: %v", err)
			}
			action := sent.Mutations[0].Value.GetMarkChatAsReadAction()
			// Whichever way the anchor went, the chat is still marked unread: that is
			// the range, and dropping the key must not take it with it.
			if action.GetRead() {
				t.Error("an unread mark went out saying the chat was read")
			}
			if action.GetMessageRange().GetLastMessageTimestamp() == 0 {
				t.Error("the patch carries no range, so it marks nothing")
			}
			if got := len(action.GetMessageRange().GetMessages()); (got == 1) != mark.wantKey {
				t.Errorf("the patch names %d messages, want key=%v", got, mark.wantKey)
			}
		})
	}
}

func TestAnUnreadMarkIsAnsweredWithWhicheverSideFailed(t *testing.T) {
	t.Parallel()

	for _, failure := range []struct {
		name string
		err  error
		want protocol.ErrorCode
	}{
		{name: "whatsapp refused the patch", err: wm.ErrIQNotAcceptable, want: protocol.ErrorWaError},
		{name: "the session is not connected", err: wm.ErrNotConnected, want: protocol.ErrorNotConnected},
		{name: "something local failed", err: errors.New("the app state store is unhappy"), want: protocol.ErrorInternal},
		// The IQ succeeded and the rejection is inside its response, so this is not an
		// *IQError and the default would call WhatsApp's decision this connector's fault.
		{name: "whatsapp rejected the patch itself", err: wm.ErrAppStateUpdate, want: protocol.ErrorWaError},
		// A 409 has whatsmeow apply the patches it got back before retrying, and a
		// deadline running out in there wraps both. Read in the wrong order, a command
		// that timed out tells the client WhatsApp refused it.
		{
			name: "the deadline ran out while a conflict was being applied",
			err:  fmt.Errorf("%w (also, applying patches failed: %w)", wm.ErrAppStateUpdate, context.DeadlineExceeded),
			want: protocol.ErrorTimeout,
		},
	} {
		t.Run(failure.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.sendAppState = func(context.Context, *wm.Client, appstate.PatchInfo) error {
				return failure.err
			}

			_, err := session.Execute(t.Context(), unreadCommand(t, `{"chat":{"kind":"phone","id":"5511999990002"}}`))
			assertCode(t, err, failure.want)
		})
	}
}
