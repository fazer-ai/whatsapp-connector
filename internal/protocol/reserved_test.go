package protocol_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// Every command the ledger reserves has to be one the ledger can reach.
//
// A question is never recorded, a repeatable one is deliberately carried out again, and a
// send is keyed by the message it names rather than by the caller's key. Reserving one of
// those writes a record nothing reads, or holds a key the resend was supposed to reuse.
func TestEveryReservedCommandIsOneTheLedgerKeys(t *testing.T) {
	t.Parallel()

	if len(protocol.ReservedCommands) == 0 {
		t.Fatal("nothing is reserved, so #282 closed nothing")
	}
	for command := range protocol.ReservedCommands {
		t.Run(string(command), func(t *testing.T) {
			t.Parallel()

			// The payload that makes the one command whose answer depends on it say yes:
			// `group.invite.get` is a question until `revoke` rotates the link.
			asked := &protocol.Command{Type: command, Payload: json.RawMessage(`{"revoke":true}`)}
			if !asked.ChangesSomething() {
				t.Error("it is a question, so nothing about it is ever recorded and reserving " +
					"it writes a key nobody reads")
			}
			if protocol.RepeatableCommands[command] {
				t.Error("it is carried out again on purpose, because what it set belongs to a " +
					"socket that may be gone; reserving it refuses the repetition that is the point")
			}
			if command.NamesItsOwnMessage() {
				t.Error("it is keyed by the message it names, and a resend under that id is " +
					"what the contract tells a client to do; reserving it refuses the resend")
			}
		})
	}
}

// And the absences the table's own reasoning rests on stay absent.
//
// Each of these has a recovery of its own that this one would stand in front of, and each
// was found in review standing behind it. Written down so that adding one back is a
// decision somebody takes rather than a line somebody appends.
//
// The teardowns carry a second consequence, named where it lives: `Session.run` reports
// whether a command did anything here, and it reads the failure alone because a failure
// answered from a record can only reach it for a reserved command. Reserve a teardown and
// a redelivery answered from an attempt starts counting as work done, which keeps an
// adopted session's lease renewed for an account nobody wants.
func TestTheCommandsWithARecoveryOfTheirOwnAreNotReserved(t *testing.T) {
	t.Parallel()

	for command, why := range map[protocol.CommandType]string{
		protocol.CommandGroupCreate: "it keeps a record of its own attempts in the store, and a " +
			"retry under the same key answers with the group the first one made",
		protocol.CommandSessionDelete: "a teardown whose local cleanup failed answers with an " +
			"error precisely so the retry finishes it",
		protocol.CommandSessionLogout: "the same, and an account already unlinked has nothing " +
			"left to unlink, so repeating costs nothing",
		protocol.CommandMessageSend: "a resend carries the message_id the first attempt used and " +
			"every client downstream discards the repeat",
		protocol.CommandMessageEdit:   "the same cover as the send",
		protocol.CommandMessageReact:  "the same cover as the send",
		protocol.CommandMessageRevoke: "revoking a message that is already revoked changes nothing",
	} {
		if protocol.ReservedCommands[command] {
			t.Errorf("%s is reserved, and it must not be: %s", command, why)
		}
	}
}

// The contract names every command it reserves, and names nothing else.
//
// The client-facing consequence of `ReservedCommands` is that a resend under the same key
// stops forcing the work, and a client cannot work out from "sets a value" which of its
// commands that covers. So the list travels, and a table in the code that the prose does
// not match is a promise to a client that this build does not keep -- in either direction:
// a command reserved and not named leaves a client resending into a refusal it was never
// told about, and one named and not reserved tells it to read the state back for a command
// that would have run.
func TestTheContractNamesEveryCommandItReserves(t *testing.T) {
	t.Parallel()

	// One phrase per command, as `contract/PROTOCOL.md` spells it. Several commands share
	// a phrase where the contract groups them, which is what a client reads.
	named := map[protocol.CommandType]string{
		protocol.CommandGroupParticipantsUpdate: "a participant added or removed",
		protocol.CommandGroupJoinRequestsUpdate: "a join request answered",
		protocol.CommandMessageMarkRead:         "a chat marked read or unread",
		protocol.CommandMessageMarkUnread:       "a chat marked read or unread",
		protocol.CommandGroupNameSet:            "a group's name, description, photo or settings",
		protocol.CommandGroupDescriptionSet:     "a group's name, description, photo or settings",
		protocol.CommandGroupPhotoSet:           "a group's name, description, photo or settings",
		protocol.CommandGroupSettingsSet:        "a group's name, description, photo or settings",
		protocol.CommandGroupInviteGet:          "an invite link rotated",
		protocol.CommandPresenceSet:             "a presence",
		protocol.CommandPairingRequestCode:      "a request for a pairing code",
	}
	prose := readContract(t)

	for command := range protocol.ReservedCommands {
		phrase, listed := named[command]
		if !listed {
			t.Errorf("%s is reserved and this test does not say how the contract names it, so "+
				"nothing checks that a client was told a resend of it stops forcing the work",
				command)
			continue
		}
		if !strings.Contains(prose, phrase) {
			t.Errorf("%s is reserved and contract/PROTOCOL.md does not say %q, so a client "+
				"resending it meets a refusal the contract never mentioned", command, phrase)
		}
	}
	for command := range named {
		if !protocol.ReservedCommands[command] {
			t.Errorf("this test says the contract names %s among the commands whose resend is "+
				"answered, and it is not reserved: the contract is telling a client to read "+
				"the state back for a command that would have run", command)
		}
	}
}

// readContract is the half of the prose that travels to a client.
func readContract(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile("../../contract/PROTOCOL.md")
	if err != nil {
		t.Fatalf("read contract/PROTOCOL.md: %v", err)
	}
	return string(body)
}
