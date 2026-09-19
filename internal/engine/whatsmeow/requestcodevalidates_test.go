package whatsmeow

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// A connect this engine refuses for its shape is refused before it has changed anything.
//
// `Connect` still asks `ConnectRequest.Validate()` after #266 moved the same question up
// to the session layer, and it is not a second answer to the same question for the sake of
// it: this engine reaches its own `Connect` from `requestCode`, with a request it builds
// rather than one a client sent, so that one caller never passes under the layer above.
//
// What the early refusal buys is not the error -- `pairWithCode` refuses a phone number
// with no digits on its own first line, with the same code -- but everything `Connect`
// does between the two. It sets the subscription, waits for a hang-up, tears down a
// pairing conversation that is in flight and rebuilds a device it cannot use. A refusal
// that has already done those is a command that failed and took effect, which is the
// reason the base gave for checking here in the first place.
//
// The subscription is what this test reads, because it is the one of those a bench with no
// socket can see: the others need a live pairing conversation, which is a limit written
// down rather than worked around.
func TestAConnectRefusedForItsShapeChangesNothing(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	if session.wantsGroups() {
		t.Fatal("a fresh session already wants groups, so this test cannot tell a refusal that " +
			"left the subscription alone from one that set it")
	}

	err := session.Connect(t.Context(), engine.ConnectRequest{
		Pairing: "code", Phone: "+ ()-", Groups: true,
	})

	var coded *protocol.Error
	switch {
	case err == nil:
		t.Fatal("a connect asking to pair by code with a phone number of punctuation was accepted")
	case !errors.As(err, &coded) || coded.Code != protocol.ErrorInvalidPayload:
		t.Fatalf("the refusal was %v, want one carrying %q, which is what a client branches on "+
			"and what the same request answers through the session layer.", err, protocol.ErrorInvalidPayload)
	}
	if session.wantsGroups() {
		t.Fatal("the connect was refused and the session came out subscribed to group traffic " +
			"anyway. The request never happened as far as the client is concerned -- it got an " +
			"error back -- and the session it was sent to is not the one it was sent to any more. " +
			"That is the refusal happening after the change instead of before it.")
	}
}

// The command that does not arrive as a connect is held to the same shape, with the same
// code.
//
// `pairing.request_code` carries a phone number and nothing else: the engine turns it into
// a `ConnectRequest` of its own, so nothing above validated it. A client asking for a code
// with a number that is not one gets the answer a `session.connect` carrying the same
// number gets, whichever door it came through.
func TestAPairingCodeRequestIsHeldToTheSameShapeAsAConnect(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")

	_, err := session.Execute(t.Context(), &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandPairingRequestCode, SID: session.sid,
		Payload: json.RawMessage(`{"phone":"+ ()-"}`),
	})
	var coded *protocol.Error
	switch {
	case err == nil:
		t.Fatal("a pairing code was requested for a phone number with no digits in it and the " +
			"connector accepted it, which is a socket opened and a code asked of WhatsApp for " +
			"nobody, with an operator waiting on a screen for something that is not coming")
	case !errors.As(err, &coded) || coded.Code != protocol.ErrorInvalidPayload:
		t.Fatalf("the refusal was %v, want one carrying %q", err, protocol.ErrorInvalidPayload)
	}
}
