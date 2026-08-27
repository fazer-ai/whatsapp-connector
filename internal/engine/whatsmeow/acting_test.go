package whatsmeow

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	wm "go.mau.fi/whatsmeow"

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

// Invariant 5 says a redelivered command must not duplicate a side effect, and for a
// message the last mile of that is the stanza id: the receiving client is what discards
// the second copy, and it discards on the id. The session layer answers a redelivery from
// its record, but the record is written after the send, so a crash between the two hands
// the same command to this code twice -- and an id made up on the spot is different each
// time, which lands a second edit and a second reaction.
func TestAnIdTheCallerLeftOutIsTheSameOnTheSecondTry(t *testing.T) {
	t.Parallel()

	session, _, _ := outboundSession(t)
	for _, tc := range []struct {
		name    string
		command *protocol.Command
	}{
		{"a command identified by its own id", &protocol.Command{
			Type: protocol.CommandMessageReact, ID: "cmd_000012"}},
		{"one identified by the caller's key", &protocol.Command{
			Type: protocol.CommandMessageReact, ID: "cmd_000012", IdempotencyKey: "react-once"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			first := session.orDerived(tc.command, "")
			if again := session.orDerived(tc.command, ""); again != first {
				t.Fatalf("the same command went out as %s and then as %s", first, again)
			}
			if !strings.HasPrefix(first, wm.WebMessageIDPrefix) || len(first) != len(wm.WebMessageIDPrefix)+18 {
				t.Fatalf("%q is not the shape whatsmeow generates", first)
			}
		})
	}

	// A different command is a different message, and two of them sharing a stanza id
	// would have the recipient discard the second as a duplicate of the first.
	react := session.orDerived(&protocol.Command{Type: protocol.CommandMessageReact, ID: "cmd_a"}, "")
	for _, other := range []*protocol.Command{
		{Type: protocol.CommandMessageReact, ID: "cmd_b"},
		{Type: protocol.CommandMessageEdit, ID: "cmd_a"},
		{Type: protocol.CommandMessageReact, ID: "cmd_a", IdempotencyKey: "somebody's key"},
	} {
		if got := session.orDerived(other, ""); got == react {
			t.Fatalf("%s/%s took the same stanza id as react/cmd_a", other.Type, other.ID)
		}
	}

	// And the caller's own id still wins, which is the ordinary case.
	if got := session.orDerived(&protocol.Command{Type: protocol.CommandMessageReact, ID: "cmd_a"}, "3EB0CAFE"); got != "3EB0CAFE" {
		t.Fatalf("the caller named %q and the message went out as %q", "3EB0CAFE", got)
	}
}

// A channel names the post a reaction is on with a server id, not with a message key, and
// carries it on a node of its own. Sent the ordinary way it goes out naming a key the
// channel cannot resolve, WhatsApp accepts it, and nobody sees a reaction. See #34.
//
// An edit and a revoke are not in the same position: whatsmeow recognises both on the
// newsletter path and rewrites the stanza id to the target's, so they are not refused and
// a test that refused all three would pin the wrong rule.
func TestOnlyAReactionIsRefusedOnAChannel(t *testing.T) {
	t.Parallel()

	session, _, _ := outboundSession(t)
	const channel = `"to":{"kind":"newsletter","id":"120363000000000000"}`

	_, err := session.react(t.Context(), &protocol.Command{
		Type:    protocol.CommandMessageReact,
		Payload: json.RawMessage(`{` + channel + `,"target_id":"3EB0A1B2C3D4E5F60718","emoji":"👍"}`),
	})
	assertCode(t, err, protocol.ErrorUnsupported)

	// The other two reach the wire, where an unconnected session is what stops them --
	// which is a different answer from `unsupported`, and the point.
	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{"an edit of a channel post", func() error {
			_, err := session.edit(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageEdit,
				Payload: json.RawMessage(`{` + channel + `,"target_id":"3EB0A1B2C3D4E5F60718",
					"content":{"type":"text","body":"corrigido"}}`)})
			return err
		}},
		{"a revoke of one", func() error {
			_, err := session.revoke(t.Context(), &protocol.Command{
				Type:    protocol.CommandMessageRevoke,
				Payload: json.RawMessage(`{` + channel + `,"target_id":"3EB0A1B2C3D4E5F60718"}`)})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.run()
			var coded *protocol.Error
			if errors.As(err, &coded) && coded.Code == protocol.ErrorUnsupported {
				t.Fatalf("%s was refused as unsupported, and whatsmeow carries it: %v", tc.name, err)
			}
		})
	}
}
