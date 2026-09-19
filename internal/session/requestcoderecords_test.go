package session_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// Pairing an inbox by typing a code leaves the same record as pairing it by scanning.
//
// `pairing.request_code` carries a phone number and nothing else, but it is a connect: the
// whatsmeow engine turns it into a `ConnectRequest` of its own and dials, which is the
// only way WhatsApp will hand out a code. Until #266 the record came from inside that
// engine, on the way through the connect it built, so moving the write up to this layer
// took it away from the one command that does not arrive as a connect.
//
// What that costs is the whole of #266 through a door nobody was watching: an operator who
// pairs by code gets an account that works until the instance running it goes away, and
// then stays down with no error and nothing anywhere saying it should be in the air. The
// suite would not have noticed, because the engine that pairs by code is the engine that
// used to write the row.
//
// The subscription is the second half. This command names a phone number, so the only
// place the groups and call policy can come from is the standing request, which is what
// the engine itself carries into the connect it builds. A record that reset them would
// bring the account back deaf to the group traffic its client had asked for.
func TestPairingByCodeIsRememberedLikeAnyOtherConnect(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	h := newHarnessWithStore(t, container)
	ctx := context.Background()

	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	// Paired and then turned off, which is the operator re-pairing an inbox that had
	// stopped working. It is also what gives this assertion something to read: `Wanted`
	// joins the desired row with the pairing, so on a session that never paired it answers
	// empty however the row reads.
	send := func(id string, kind protocol.CommandType, payload string) {
		t.Helper()
		h.manager.Dispatch(delivery(&protocol.Command{
			V: protocol.Version, ID: id, Type: kind, SID: "s1", ReplyTo: id,
			Payload: json.RawMessage(payload),
		}, &atomic.Bool{}))
		waitFor(t, "a reply to "+id, func() bool { _, ok := h.recorder.reply(id); return ok })
	}
	send("c1", protocol.CommandSessionConnect, `{"pairing":"qr","groups":true,"calls":{"auto_reject":true}}`)
	send("d1", protocol.CommandSessionDisconnect, `{}`)
	if seeded, err := container.Wanted(ctx); err != nil || len(seeded) != 0 {
		t.Fatalf("the given is not what this test needs: after pairing and disconnecting the sweep "+
			"would bring back %v (err=%v), want nothing, or the assertion below cannot fail.", seeded, err)
	}

	send("p1", protocol.CommandPairingRequestCode, `{"phone":"5511999990001"}`)

	wanted, err := container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(wanted) != 1 || wanted[0].SID != "s1" {
		t.Fatalf("after the client asked for a pairing code, the sweep would bring back %v, want just s1.\n"+
			"Nothing says this account should be in the air, so the instance after this one leaves it "+
			"down: an inbox paired by code works until its instance goes away and then never comes "+
			"back, which is #266 through the one command that is a connect without saying so.", wanted)
	}
	if !wanted[0].Groups || !wanted[0].CallAutoReject {
		t.Fatalf("the row left by a pairing code carries groups=%v auto_reject=%v, want both.\n"+
			"This command names a phone number and nothing else, so what it records is the standing "+
			"request -- the same one the engine carries into the connect it builds. Reset here, the "+
			"account comes back acknowledging group traffic it publishes nowhere, and ringing on a "+
			"phone whose operator had asked for the opposite.",
			wanted[0].Groups, wanted[0].CallAutoReject)
	}
}
