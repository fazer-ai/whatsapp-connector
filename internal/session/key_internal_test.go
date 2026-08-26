package session

import (
	"encoding/json"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// What a command is remembered under decides two things at once: whether a redelivery
// is answered from a record, and whether two different commands share one. Both of the
// wrong answers are silent, so the derivation is pinned here rather than inferred from
// the one command that happens to be implemented.
func TestWhatACommandIsRememberedUnder(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		command protocol.Command
		want    string
	}{{
		// The caller names the message, so every attempt at this send names the same
		// one, whether the transport redelivered it or the caller sent it again.
		name: "a send is keyed by the message it names",
		command: protocol.Command{
			ID: "c1", Type: protocol.CommandMessageSend,
			Payload: json.RawMessage(`{"message_id":"m1","to":{"kind":"phone","id":"5511"},"content":{}}`),
		},
		want: "msg:m1",
	}, {
		// The edit puts a message of its own on the wire and the caller names it, which
		// is the same reasoning as the send.
		name: "an edit is keyed by the message it puts on the wire",
		command: protocol.Command{
			ID: "c2", Type: protocol.CommandMessageEdit,
			Payload: json.RawMessage(`{"message_id":"m2","target_id":"m1","to":{"kind":"phone","id":"5511"},"content":{}}`),
		},
		want: "msg:m2",
	}, {
		// Here `message_id` names a message that already exists, and the send that
		// created it is keyed by that same id. Sharing the key would answer the
		// download with the send's result, and the other way round.
		name: "a media download does not borrow the id of the send that created the message",
		command: protocol.Command{
			ID: "c3", Type: protocol.CommandMessageDownloadMedia,
			Payload: json.RawMessage(`{"message_id":"m1"}`),
		},
		want: "cmd:c3",
	}, {
		name: "a media download prefers the key its own frame carries",
		command: protocol.Command{
			ID: "c4", Type: protocol.CommandMessageDownloadMedia, IdempotencyKey: "download-once",
			Payload: json.RawMessage(`{"message_id":"m1"}`),
		},
		want: "idem:download-once",
	}, {
		// Reading the invite code changes nothing, and the answer is only worth having
		// if it is current.
		name: "reading an invite code is a question",
		command: protocol.Command{
			ID: "c5", Type: protocol.CommandGroupInviteGet,
			Payload: json.RawMessage(`{"group":{"kind":"group","id":"g1"},"revoke":false}`),
		},
		want: "",
	}, {
		// The same command with `revoke` set rotates the code first. A redelivery would
		// rotate a code nobody has seen yet and answer with a different one than the
		// reply that was lost.
		name: "revoking an invite code is not",
		command: protocol.Command{
			ID: "c6", Type: protocol.CommandGroupInviteGet,
			Payload: json.RawMessage(`{"group":{"kind":"group","id":"g1"},"revoke":true}`),
		},
		want: "cmd:c6",
	}, {
		// The caller picks this string and the schema takes any, so an unprefixed one
		// reading `msg:m1` would be answered from the record of the send of m1: a logout
		// reported successful over an account that is still paired.
		name: "a caller's own key cannot reach into the message namespace",
		command: protocol.Command{
			ID: "c9", Type: protocol.CommandSessionLogout, IdempotencyKey: "msg:m1",
			Payload: json.RawMessage(`{}`),
		},
		want: "idem:msg:m1",
	}, {
		name: "a question is asked again",
		command: protocol.Command{
			ID: "c7", Type: protocol.CommandSessionStatus, Payload: json.RawMessage(`{}`),
		},
		want: "",
	}, {
		name: "a side effect with no key of its own falls back to the frame it arrived as",
		command: protocol.Command{
			ID: "c8", Type: protocol.CommandSessionLogout, Payload: json.RawMessage(`{}`),
		},
		want: "cmd:c8",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := idempotencyKey(&tc.command); got != tc.want {
				t.Fatalf("%s is remembered under %q, want %q", tc.command.Type, got, tc.want)
			}
		})
	}
}
