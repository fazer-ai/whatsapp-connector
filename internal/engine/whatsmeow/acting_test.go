package whatsmeow

import (
	"encoding/json"
	"testing"

	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// A reaction names the message it is on with a key, and a key that resolves to no message
// is accepted by WhatsApp all the same: the send answers with a timestamp and nobody ever
// sees the reaction. So the sender the key is built around is decided here, and what
// cannot be decided is refused rather than guessed.
func TestWhoAReactionIsSaidToBeOn(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		payload string
		chat    string
		want    string
		refused bool
	}{
		{"the account's own message", `"target_from_me":true`, "5511999990002@s.whatsapp.net", "", false},
		// A chat with one other person names its sender on its own: everything in it the
		// account did not send came from whoever it is with.
		{"the contact's message in a direct chat", ``, "5511999990002@s.whatsapp.net",
			"5511999990002@s.whatsapp.net", false},
		{"the contact's, in a chat addressed by LID", ``, "182736451928374@lid",
			"182736451928374@lid", false},
		// A group has many, and the key needs the one.
		{"somebody's message in a group, said whose",
			`"target_participant":{"kind":"phone","id":"5511999990003"}`, "120363000000000000@g.us",
			"5511999990003@s.whatsapp.net", false},
		{"somebody's message in a group, not said whose", ``, "120363000000000000@g.us", "", true},
		// Both, and they cannot both be true.
		{"the account's own and somebody else's at once",
			`"target_from_me":true,"target_participant":{"kind":"phone","id":"5511999990003"}`,
			"5511999990002@s.whatsapp.net", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var req reactRequest
			body := `{"target_id":"3EB0","emoji":"👍"`
			if tc.payload != "" {
				body += "," + tc.payload
			}
			if err := json.Unmarshal([]byte(body+"}"), &req); err != nil {
				t.Fatalf("unmarshal %s: %v", body, err)
			}
			chat, err := waTypes.ParseJID(tc.chat)
			if err != nil {
				t.Fatalf("ParseJID(%q): %v", tc.chat, err)
			}

			got, err := whoSentTheTarget(&req, chat)
			switch {
			case tc.refused && err == nil:
				t.Fatalf("that was built around %s instead of being refused", got)
			case tc.refused:
				assertCode(t, err, protocol.ErrorInvalidPayload)
				return
			case err != nil:
				t.Fatalf("that was refused: %v", err)
			}
			// An empty JID is how whatsmeow's BuildMessageKey is told the target is the
			// account's own, and it renders as the empty string.
			if got.String() != tc.want {
				t.Fatalf("the reaction was built around %q, want %q", got, tc.want)
			}
		})
	}
}

// The sender above is only half of it: what reaches WhatsApp is the key whatsmeow builds
// from it, so the two are checked together. The questions it answers are whose message
// this is and, in a chat with more than one other person, which of them sent it. A key
// that says `from_me` about the contact's message points at a message the account never
// sent, and the reaction lands on nothing.
func TestTheKeyAReactionEndsUpCarrying(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, payload, chat string
		fromMe              bool
		participant         string
	}{
		{"on the account's own message", `"target_from_me":true`,
			"5511999990002@s.whatsapp.net", true, ""},
		// One other person, and the chat already names them: a participant here is a
		// field WhatsApp does not expect in a direct chat.
		{"on the contact's, in a direct chat", ``,
			"5511999990002@s.whatsapp.net", false, ""},
		{"on somebody's, in a group", `"target_participant":{"kind":"phone","id":"5511999990003"}`,
			"120363000000000000@g.us", false, "5511999990003@s.whatsapp.net"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _, _ := outboundSession(t)
			var req reactRequest
			body := `{"target_id":"3EB0","emoji":"👍"`
			if tc.payload != "" {
				body += "," + tc.payload
			}
			if err := json.Unmarshal([]byte(body+"}"), &req); err != nil {
				t.Fatalf("unmarshal %s: %v", body, err)
			}
			chat, err := waTypes.ParseJID(tc.chat)
			if err != nil {
				t.Fatalf("ParseJID(%q): %v", tc.chat, err)
			}
			sender, err := whoSentTheTarget(&req, chat)
			if err != nil {
				t.Fatalf("whoSentTheTarget: %v", err)
			}

			key := session.current().
				BuildReaction(chat, sender, req.TargetID, *req.Emoji).
				GetReactionMessage().GetKey()
			if key.GetFromMe() != tc.fromMe {
				t.Fatalf("the key says from_me=%v, want %v", key.GetFromMe(), tc.fromMe)
			}
			if got := key.GetParticipant(); got != tc.participant {
				t.Fatalf("the key names %q as the sender, want %q", got, tc.participant)
			}
			if got, want := key.GetRemoteJID(), chat.String(); got != want {
				t.Fatalf("the key names %q as the chat, want %q", got, want)
			}
		})
	}
}

// An empty emoji is how the contract says to take a reaction off, and no emoji at all is
// a caller that did not say what to react with. Read into a plain string the two arrive
// the same, and every malformed reaction would quietly remove one instead.
func TestAReactionWithNoEmojiIsNotAReactionThatRemovesOne(t *testing.T) {
	t.Parallel()

	session, _, _ := outboundSession(t)
	_, err := session.react(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageReact,
		Payload: json.RawMessage(`{"to":{"kind":"phone","id":"5511999990002"},
			"target_id":"3EB0A1B2C3D4E5F60718"}`),
	})
	assertCode(t, err, protocol.ErrorInvalidPayload)

	// And the empty one is not refused with it: it is what removing looks like.
	var req reactRequest
	if err := json.Unmarshal([]byte(`{"target_id":"3EB0","emoji":""}`), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if req.Emoji == nil || *req.Emoji != "" {
		t.Fatalf("an empty emoji decoded as %v", req.Emoji)
	}
}

// Every one of the three names a message that already exists, and a command that does not
// name one has nothing to act on. Refused on the payload rather than sent: WhatsApp
// accepts a key built around an empty id and answers success.
func TestACommandThatActsOnNothingIsRefused(t *testing.T) {
	t.Parallel()

	const chat = `"to":{"kind":"phone","id":"5511999990002"}`
	for _, tc := range []struct {
		name    string
		run     func(*Session, string) error
		payload string
	}{
		{"an edit", func(s *Session, p string) error {
			_, err := s.edit(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageEdit, Payload: json.RawMessage(p)})
			return err
		}, `{` + chat + `,"content":{"type":"text","body":"corrigido"}}`},
		{"a revoke", func(s *Session, p string) error {
			_, err := s.revoke(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageRevoke, Payload: json.RawMessage(p)})
			return err
		}, `{` + chat + `}`},
		{"a reaction", func(s *Session, p string) error {
			_, err := s.react(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageReact, Payload: json.RawMessage(p)})
			return err
		}, `{` + chat + `,"emoji":"👍"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _, _ := outboundSession(t)
			assertCode(t, tc.run(session, tc.payload), protocol.ErrorInvalidPayload)
		})
	}
}

// A correction is the whole corrected message, so a caption edit needs the file's upload
// coordinates again and nothing here keeps them once a send is done. Refused with the
// reason rather than sent with coordinates that resolve to nothing, which would replace a
// caption with a broken attachment and report success. See #32.
func TestOnlyATextBodyCanBeCorrected(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, content string
		code          protocol.ErrorCode
	}{
		{"a media caption", `{"type":"media","kind":"image","caption":"outra legenda"}`,
			protocol.ErrorUnsupported},
		{"a location", `{"type":"location","latitude":-25.4,"longitude":-49.2}`,
			protocol.ErrorUnsupported},
		{"a body that does not say what it is", `{"body":"corrigido"}`, protocol.ErrorInvalidPayload},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := editedBody(json.RawMessage(tc.content))
			assertCode(t, err, tc.code)
		})
	}

	// And the one that can.
	corrected, err := editedBody(json.RawMessage(`{"type":"text","body":"corrigido"}`))
	if err != nil {
		t.Fatalf("editedBody: %v", err)
	}
	if got := corrected.GetConversation(); got != "corrigido" {
		t.Fatalf("the correction reads %q", got)
	}
}
