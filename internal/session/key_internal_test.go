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
		// A download is carried out again rather than answered from a record. Its answer
		// is a reference to a blob on one instance, good until a TTL, so a remembered
		// one is an address that answers nothing -- handed back on the very path that
		// exists to recover an attachment. Spending the download twice is the cheaper of
		// the two, and it is bounded.
		name: "a media download is asked again rather than answered from a record",
		command: protocol.Command{
			ID: "c3", Type: protocol.CommandMessageDownloadMedia,
			Payload: json.RawMessage(`{"message_id":"m1"}`),
		},
		want: "",
	}, {
		// Not even with a key of its own. What makes the old answer unusable is that it
		// went stale, and the caller's key says nothing about that.
		name: "a media download is asked again even when its frame carries a key",
		command: protocol.Command{
			ID: "c4", Type: protocol.CommandMessageDownloadMedia, IdempotencyKey: "download-once",
			Payload: json.RawMessage(`{"message_id":"m1"}`),
		},
		want: "",
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

// Belt and braces behind the rule above, and the reason it is worth keeping: if a
// download were ever remembered again, its `message_id` names a message somebody else's
// command created, and keying by it would answer the download with that send's result.
func TestADownloadDoesNotBorrowTheIDNamespaceOfTheSendThatCreatedItsMessage(t *testing.T) {
	t.Parallel()

	if protocol.CommandMessageDownloadMedia.NamesItsOwnMessage() {
		t.Fatal("a download would be remembered under the id of the send that created its message")
	}
}

// A command whose effect belongs to the socket that carried it out cannot be answered
// from a record of the first time. WhatsApp forgets an account's availability and its
// presence subscriptions when the connection goes and whatsmeow replays neither, so a
// redelivery that lands on a new socket and is answered from the ledger reports a
// success over a connection where nothing was done.
func TestAPresenceIsCarriedOutAgainRatherThanRecalled(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		command    protocol.CommandType
		remembered bool
	}{
		{"setting availability", protocol.CommandPresenceSet, false},
		{"subscribing to somebody", protocol.CommandPresenceSubscribe, false},
		// Skipping this one costs a typing indicator nobody sees; carrying it out again
		// long after shows one that is not true. Repeating is not the safer mistake.
		{"a typing indicator", protocol.CommandChatPresence, true},
		{"a send", protocol.CommandMessageSend, true},
		// And the other reason a command is not remembered, which is a different one: an
		// answer worth having only while it is current.
		{"a question", protocol.CommandSessionStatus, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			key := idempotencyKey(&protocol.Command{
				Type: tc.command, ID: "c1", IdempotencyKey: "k1", Payload: json.RawMessage(`{}`),
			})
			if remembered := key != ""; remembered != tc.remembered {
				t.Errorf("%s is remembered under %q, and it should be %v", tc.command, key, tc.remembered)
			}
		})
	}
}
