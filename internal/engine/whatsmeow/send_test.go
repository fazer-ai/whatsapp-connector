package whatsmeow

import (
	"encoding/json"
	"errors"
	"testing"

	wm "go.mau.fi/whatsmeow"
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
		{"an image, which a later slice brings", `{"message_id":"3EB0",
			"to":{"kind":"phone","id":"5511999990001"},"content":{"type":"media","kind":"image"}}`,
			protocol.ErrorUnsupported},
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

// A send on a session whose socket is down. Answering anything but this leaves a caller
// believing a message went out over a connection that is not there.
func TestASendOnASessionThatIsNotConnectedSaysSo(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	_, err := session.send(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend,
		Payload: json.RawMessage(`{"message_id":"3EB0ABCDEF",
			"to":{"kind":"phone","id":"5511999990002"},"content":{"type":"text","body":"oi"}}`),
	})
	assertCode(t, err, protocol.ErrorNotConnected)
}

// The plain shape is what a phone sends for a bare line of text, and every client
// renders it without thinking about it. The extended one is for a message that carries
// something besides the words.
func TestABareTextGoesOutInTheShapeAPhoneWouldSendIt(t *testing.T) {
	t.Parallel()

	req := &sendRequest{MessageID: "3EB0"}
	req.Content.Type, req.Content.Body = "text", "bom dia"

	message, err := textToSend(req)
	if err != nil {
		t.Fatalf("textToSend: %v", err)
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
	req.Content.Type, req.Content.Body = "text", "@5511999990002 bom dia"
	req.Quoted = &struct {
		ID          string            `json:"id"`
		Participant *protocol.Address `json:"participant"`
		FromMe      bool              `json:"from_me"`
	}{ID: "3EB0ORIGINAL", Participant: &protocol.Address{Kind: protocol.AddressLID, ID: "167392323834034"}}

	message, err := textToSend(req)
	if err != nil {
		t.Fatalf("textToSend: %v", err)
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
		{"anything this does not name", errors.New("something new in the protocol"), protocol.ErrorWaError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assertCode(t, sendFailure(tc.err), tc.want)
		})
	}
}

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
