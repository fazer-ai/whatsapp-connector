package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	wm "go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// The way out and the way back have to agree, or a reply goes to a different account
// from the one the message came from. Round-tripping is the only check that keeps the
// two tables from drifting apart as kinds are added.
func TestAnAddressSurvivesTheRoundTripToAJIDAndBack(t *testing.T) {
	t.Parallel()

	for _, want := range []protocol.Address{
		{Kind: protocol.AddressPhone, ID: "5511999990001"},
		{Kind: protocol.AddressLID, ID: "167392323834034"},
		{Kind: protocol.AddressGroup, ID: "120363000000000000"},
		{Kind: protocol.AddressNewsletter, ID: "120363111111111111"},
		{Kind: protocol.AddressBroadcast, ID: "5511999990001-1600000000"},
		{Kind: protocol.AddressStatus, ID: "status"},
	} {
		t.Run(string(want.Kind), func(t *testing.T) {
			t.Parallel()

			jid, err := jidOf(want)
			if err != nil {
				t.Fatalf("jidOf(%+v): %v", want, err)
			}
			got, ok := addressOf(jid)
			if !ok {
				t.Fatalf("%s came back as an address the contract cannot name", jid)
			}
			if got != want {
				t.Fatalf("%+v round-tripped through %s as %+v", want, jid, got)
			}
		})
	}
}

func TestAnAddressThisConnectorCannotBuildIsRefused(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		address protocol.Address
	}{
		{"a kind that is not in the contract", protocol.Address{Kind: "carrier_pigeon", ID: "5511999990001"}},
		{"a kind with no id behind it", protocol.Address{Kind: protocol.AddressPhone, ID: ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			jid, err := jidOf(tc.address)
			if err == nil {
				t.Fatalf("jidOf(%+v) built %s out of an address it cannot name", tc.address, jid)
			}
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// Every one of these is a send that must not go out as something else. A caller told
// its message went out has no reason to send it again.
func TestASendThisConnectorCannotMakeIsRefusedWithItsOwnCode(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		payload string
		want    protocol.ErrorCode
	}{
		{"a payload that is not a send at all", `not json`, protocol.ErrorInvalidPayload},
		{"a send that names no message", `{"to":{"kind":"phone","id":"5511999990001"},
			"content":{"type":"text","body":"oi"}}`, protocol.ErrorInvalidPayload},
		{"a send with no body", `{"message_id":"3EB0","to":{"kind":"phone","id":"5511999990001"},
			"content":{"type":"text","body":""}}`, protocol.ErrorInvalidPayload},
		{"an address with no id", `{"message_id":"3EB0","to":{"kind":"phone","id":""},
			"content":{"type":"text","body":"oi"}}`, protocol.ErrorInvalidPayload},
		{"a reaction, which a later slice brings", `{"message_id":"3EB0",
			"to":{"kind":"phone","id":"5511999990001"},"content":{"type":"reaction","emoji":"\u2764\ufe0f"}}`,
			protocol.ErrorUnsupported},
		{"an image naming no file to send", `{"message_id":"3EB0",
			"to":{"kind":"phone","id":"5511999990001"},"content":{"type":"media","kind":"image"}}`,
			protocol.ErrorInvalidPayload},
		{"a quote that names no message", `{"message_id":"3EB0",
			"to":{"kind":"phone","id":"5511999990001"},"content":{"type":"text","body":"oi"},
			"quoted":{}}`, protocol.ErrorInvalidPayload},
		{"a quote pointing at an address the contract cannot name", `{"message_id":"3EB0",
			"to":{"kind":"group","id":"120363000000000000"},"content":{"type":"text","body":"oi"},
			"quoted":{"id":"3EB0AAA","participant":{"kind":"carrier_pigeon","id":"1"}}}`,
			protocol.ErrorInvalidPayload},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			_, err := session.send(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageSend, Payload: json.RawMessage(tc.payload),
			})
			if err == nil {
				t.Fatal("the send was accepted, and this case exists because it cannot be made")
			}
			assertCode(t, err, tc.want)
		})
	}
}

// The two states a send can be refused for, and they are not the same answer: one has
// the client pair an account, the other has it wait for a connection to come back. An
// unpaired session is never connected either, so asking about the socket first makes
// the pairing answer unreachable and leaves the client waiting on something nothing is
// going to do.
func TestASendSaysWhichOfTheSessionsOwnStatesRefusedIt(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		phone string
		want  protocol.ErrorCode
	}{
		{"a session that never paired", "", protocol.ErrorNotPaired},
		{"a paired session whose socket is down", "5511999990001", protocol.ErrorNotConnected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, tc.phone)
			_, err := session.send(t.Context(), &protocol.Command{
				Type: protocol.CommandMessageSend,
				Payload: json.RawMessage(`{"message_id":"3EB0ABCDEF",
					"to":{"kind":"phone","id":"5511999990002"},"content":{"type":"text","body":"oi"}}`),
			})
			assertCode(t, err, tc.want)
		})
	}
}

// A quote a client cannot attribute is a quote it renders as coming from nobody. The
// caller can only name the participant for somebody else's message in a group, so the
// two cases it cannot fill in are the ones this session has to.
func TestAQuoteIsAttributedToWhoeverWroteTheMessageItAnswers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		quoted func() *sendRequest
		to     waTypes.JID
		want   string
	}{
		{"the caller named the participant itself", func() *sendRequest {
			return quotingRequest("3EB0ORIGINAL", false, &protocol.Address{
				Kind: protocol.AddressPhone, ID: "5511999990003",
			})
		}, groupChat, "5511999990003@" + waTypes.DefaultUserServer},
		{"a quote of this account's own message", func() *sendRequest {
			return quotingRequest("3EB0ORIGINAL", true, nil)
		}, groupChat, ownAccount.String()},
		{"a quote in a direct chat, where the only other party wrote it", func() *sendRequest {
			return quotingRequest("3EB0ORIGINAL", false, nil)
		}, peer, peer.String()},
		{"somebody else's message in a group the caller did not name", func() *sendRequest {
			return quotingRequest("3EB0ORIGINAL", false, nil)
		}, groupChat, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			message, err := textWith(tc.quoted(), ownAccount, tc.to)
			if err != nil {
				t.Fatalf("textWith: %v", err)
			}
			info := message.GetExtendedTextMessage().GetContextInfo()
			if info.GetStanzaID() != "3EB0ORIGINAL" {
				t.Fatalf("the quote points at %q", info.GetStanzaID())
			}
			if got := info.GetParticipant(); got != tc.want {
				t.Fatalf("the quote attributes the original to %q, want %q", got, tc.want)
			}
		})
	}
}

// textWith renders a text message the way send does: the context that rides along is
// built first and the body is built around it. They were one function until every other
// outbound body needed the same context.
func textWith(req *sendRequest, own, to waTypes.JID) (*waE2E.Message, error) {
	alongside, err := contextToSend(req, own, to)
	if err != nil {
		return nil, err
	}
	content, err := decodeBody[textContent](req.Content, "text")
	if err != nil {
		return nil, err
	}
	return textToSend(&content, alongside)
}

func quotingRequest(id string, fromMe bool, participant *protocol.Address) *sendRequest {
	req := &sendRequest{MessageID: "3EB0", Content: json.RawMessage(`{"type":"text","body":"answering that"}`)}
	req.Quoted = &struct {
		ID          string            `json:"id"`
		Participant *protocol.Address `json:"participant"`
		FromMe      bool              `json:"from_me"`
	}{ID: id, Participant: participant, FromMe: fromMe}
	return req
}

// The plain shape is what a phone sends for a bare line of text, and every client
// renders it without thinking about it. The extended one is for a message that carries
// something besides the words.
func TestABareTextGoesOutInTheShapeAPhoneWouldSendIt(t *testing.T) {
	t.Parallel()

	req := &sendRequest{MessageID: "3EB0", Content: json.RawMessage(`{"type":"text","body":"bom dia"}`)}

	message, err := textWith(req, ownAccount, peer)
	if err != nil {
		t.Fatalf("textWith: %v", err)
	}
	if message.GetConversation() != "bom dia" {
		t.Fatalf("a bare text went out as %+v", message)
	}
	if message.GetExtendedTextMessage() != nil {
		t.Fatal("a bare text went out in the shape reserved for one that carries something else")
	}
}

func TestATextThatCarriesSomethingElseGoesOutWithIt(t *testing.T) {
	t.Parallel()

	req := &sendRequest{
		MessageID: "3EB0",
		Mentions:  []protocol.Address{{Kind: protocol.AddressPhone, ID: "5511999990002"}},
		Ephemeral: 604800,
	}
	req.Content = json.RawMessage(`{"type":"text","body":"@5511999990002 bom dia"}`)
	req.Quoted = &struct {
		ID          string            `json:"id"`
		Participant *protocol.Address `json:"participant"`
		FromMe      bool              `json:"from_me"`
	}{ID: "3EB0ORIGINAL", Participant: &protocol.Address{Kind: protocol.AddressLID, ID: "167392323834034"}}

	message, err := textWith(req, ownAccount, peer)
	if err != nil {
		t.Fatalf("textWith: %v", err)
	}
	extended := message.GetExtendedTextMessage()
	if extended == nil {
		t.Fatalf("a text carrying a quote, a mention and a timer went out as %+v", message)
	}
	info := extended.GetContextInfo()
	if info.GetStanzaID() != "3EB0ORIGINAL" {
		t.Fatalf("the quote points at %q", info.GetStanzaID())
	}
	if info.GetParticipant() != "167392323834034@"+waTypes.HiddenUserServer {
		t.Fatalf("the quote attributes the original to %q", info.GetParticipant())
	}
	if got := info.GetMentionedJID(); len(got) != 1 || got[0] != "5511999990002@"+waTypes.DefaultUserServer {
		t.Fatalf("the mentions went out as %v", got)
	}
	// Carried per message, not per chat: a send that leaves it off in a chat on a timer
	// is the one message in that conversation that stays behind after the rest has gone.
	if info.GetExpiration() != 604800 {
		t.Fatalf("the disappearing timer went out as %d", info.GetExpiration())
	}
}

func TestWhatsAppsOwnRefusalsKeepTheirMeaning(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
		want protocol.ErrorCode
	}{
		{"a store with no account in it", wm.ErrNotLoggedIn, protocol.ErrorNotPaired},
		{"a socket that is down", wm.ErrNotConnected, protocol.ErrorNotConnected},
		// The message may well have gone out: what timed out is the answer, not the
		// send. Saying so is what lets the caller retry under the same id.
		{"an answer that never came", wm.ErrMessageTimedOut, protocol.ErrorTimeout},
		// The command's own deadline, not WhatsApp's. Same reasoning and the same answer:
		// what ran out is the answer, not the send, and a refusal would have the caller
		// give up on a message that is already in somebody's chat.
		{"the command's deadline running out", context.DeadlineExceeded, protocol.ErrorTimeout},
		{"the command being abandoned", context.Canceled, protocol.ErrorTimeout},
		// whatsmeow sends every direct message under a LID and looks one up for a number
		// that has none cached. A number nobody registered has none to find, and that is
		// permanent: as a WhatsApp refusal it reads as something to try again, and a
		// client retries a number that can never receive anything.
		{"a number nobody has registered", errors.New("no LID found for 5511999999999@s.whatsapp.net from server"),
			protocol.ErrorRecipientNotOnWhatsapp},
		// This connector's own limit, not WhatsApp's, and the two mean different things
		// to a caller: a refusal is worth trying again and a limit never is.
		{"a broadcast list, which the library does not send to", wm.ErrBroadcastListUnsupported,
			protocol.ErrorUnsupported},
		{"anything this does not name", errors.New("something new in the protocol"), protocol.ErrorWaError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assertCode(t, sendFailure(tc.err), tc.want)
		})
	}
}

// The two addresses every send has: the account it goes out from and the chat it goes
// to. A quote needs both, because the caller can only name the participant for somebody
// else's message in a group.
var (
	ownAccount = waTypes.NewJID("5511999990001", waTypes.DefaultUserServer)
	peer       = waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	groupChat  = waTypes.NewJID("120363000000000000", waTypes.GroupServer)
)

func assertCode(t *testing.T, err error, want protocol.ErrorCode) {
	t.Helper()

	var coded *protocol.Error
	if !errors.As(err, &coded) {
		t.Fatalf("the failure is %v, which carries no code a client can branch on", err)
	}
	if coded.Code != want {
		t.Fatalf("the failure is %q, want %q", coded.Code, want)
	}
}

// Which identity a message goes out under is whatsmeow's rule, not a preference: a
// direct chat is sent under the LID whichever way the caller addressed it, because the
// library looks the LID up and replaces the destination with it. A quote attributed
// under the other form is one a client cannot match to anybody.
func TestAQuoteOfThisAccountsOwnMessageUsesTheIdentityItWasSentUnder(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		to   waTypes.JID
		want string
	}{
		{"a direct chat the caller addressed by phone", peer, "89572297961476@" + waTypes.HiddenUserServer},
		{"a direct chat the caller addressed by LID",
			waTypes.NewJID("167392323834034", waTypes.HiddenUserServer), "89572297961476@" + waTypes.HiddenUserServer},
		// A group is sent under the LID only when the group itself is LID-addressed,
		// which is behind a lookup this connector would pay a round trip for on the send
		// path. Being wrong costs a quote the recipient cannot attribute, which is where
		// it was before it was attributed at all.
		{"a group", groupChat, "5511999990001@" + waTypes.DefaultUserServer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.setIdentity("5511999990001", "89572297961476")

			message, err := textWith(quotingRequest("3EB0ORIGINAL", true, nil), session.ownJID(tc.to), tc.to)
			if err != nil {
				t.Fatalf("textWith: %v", err)
			}
			got := message.GetExtendedTextMessage().GetContextInfo().GetParticipant()
			if got != tc.want {
				t.Fatalf("the quote attributes this account as %q, want %q", got, tc.want)
			}
		})
	}
}
