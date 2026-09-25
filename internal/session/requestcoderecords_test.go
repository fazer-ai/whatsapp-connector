package session_test

import (
	"context"
	"encoding/json"
	"errors"
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
	send := func(id string, kind protocol.CommandType, payload string) protocol.Reply {
		t.Helper()
		h.manager.Dispatch(delivery(&protocol.Command{
			V: protocol.Version, ID: id, Type: kind, SID: "s1", ReplyTo: id,
			Payload: json.RawMessage(payload),
		}, &atomic.Bool{}))
		waitFor(t, "a reply to "+id, func() bool { _, ok := h.recorder.reply(id); return ok })
		reply, _ := h.recorder.reply(id)
		return reply
	}
	send("c1", protocol.CommandSessionConnect, `{"pairing":"qr","groups":true,"calls":{"auto_reject":true},`+
		`"proxy":{"url":"http://user:secret@10.0.0.1:3128"}}`)
	send("d1", protocol.CommandSessionDisconnect, `{}`)
	if seeded, err := container.Wanted(ctx); err != nil || len(seeded) != 0 {
		t.Fatalf("the given is not what this test needs: after pairing and disconnecting the sweep "+
			"would bring back %v (err=%v), want nothing, or the assertion below cannot fail.", seeded, err)
	}

	// The reply is asserted and not just waited for, because a command that failed is
	// answered too: without this the test passed against an engine that refused the
	// command outright, and what it was measuring was a row written before the refusal.
	if reply := send("p1", protocol.CommandPairingRequestCode, `{"phone":"5511999990001"}`); !reply.OK {
		t.Fatalf("the pairing code request failed (%+v), so the row read below would say nothing "+
			"about what a successful one leaves behind", reply.Error)
	}

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
	if wanted[0].Proxy != "http://user:secret@10.0.0.1:3128" {
		t.Fatalf("the row left by a pairing code carries the proxy %q.\n"+
			"The command has no field for one, so what it records is the proxy already standing. "+
			"Cleared here, the next resume dials WhatsApp from this instance's own address.",
			wanted[0].Proxy)
	}
}

// A pairing code request the connector refuses leaves the account as the operator left it.
//
// The mirror of the test above, and the reason the write waits for the engine there. This
// command carries a phone number and nothing else, so a number that is not one -- a field
// cleared down to the brackets the interface put in it -- is refused, and a refusal that
// had already recorded `connected` would have the next sweep dial an account whose client
// asked for nothing of the sort and whose operator had turned it off.
func TestAPairingCodeRequestThatFailedBringsNothingBack(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	h := newHarnessWithStore(t, container)
	ctx := context.Background()

	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	send := func(id string, kind protocol.CommandType, payload string) protocol.Reply {
		t.Helper()
		h.manager.Dispatch(delivery(&protocol.Command{
			V: protocol.Version, ID: id, Type: kind, SID: "s1", ReplyTo: id,
			Payload: json.RawMessage(payload),
		}, &atomic.Bool{}))
		waitFor(t, "a reply to "+id, func() bool { _, ok := h.recorder.reply(id); return ok })
		reply, _ := h.recorder.reply(id)
		return reply
	}
	send("c1", protocol.CommandSessionConnect, `{"pairing":"qr","groups":true}`)
	send("d1", protocol.CommandSessionDisconnect, `{}`)

	if reply := send("p1", protocol.CommandPairingRequestCode, `{"phone":"+ ()-"}`); reply.OK {
		t.Fatal("a pairing code was requested for a phone number with no digits in it and the " +
			"connector accepted it")
	}

	var desired string
	if err := container.DB().QueryRowContext(ctx,
		`SELECT desired FROM wac_session_desired WHERE sid = 's1'`).Scan(&desired); err != nil {
		t.Fatalf("read the desired state back: %v", err)
	}
	if desired != "disconnected" {
		t.Fatalf("after a pairing code request was refused, s1 reads %q, want %q.\n"+
			"The operator turned this account off and the connector refused to turn it back on, "+
			"so the only thing that may be written here is what the operator asked for.",
			desired, "disconnected")
	}
	if wanted, err := container.Wanted(ctx); err != nil || len(wanted) != 0 {
		t.Fatalf("the sweep would bring back %v (err=%v) after a request that failed", wanted, err)
	}
}

// A pairing code request the engine refused is remembered all the same.
//
// The other side of the test above, and the pair of them is what pins the ordering. This
// one is the case that sent the write back in front of the engine after a round where it
// waited for the answer: on an account that is already paired, `pairWithCode` resumes
// rather than pairing, and a resume whose deadline expires answers an error while the
// socket carries on opening underneath it. Recorded only on success, that account is up
// with `desired = 'disconnected'` on it, and the next instance leaves it down -- #266
// again, in the window a command's ceiling opens.
//
// So the rule is the connect's: what the connector refuses by construction is refused
// before the record, and what an engine refuses for its own reasons is recorded anyway,
// because a later attempt may well get through and `cluster.Quarantine` is what stops one
// that never will from being retried for ever.
func TestAPairingCodeRequestTheEngineRefusedIsStillRemembered(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	h := newHarnessWithStore(t, container)
	ctx := context.Background()

	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	send := func(id string, kind protocol.CommandType, payload string) protocol.Reply {
		t.Helper()
		h.manager.Dispatch(delivery(&protocol.Command{
			V: protocol.Version, ID: id, Type: kind, SID: "s1", ReplyTo: id,
			Payload: json.RawMessage(payload),
		}, &atomic.Bool{}))
		waitFor(t, "a reply to "+id, func() bool { _, ok := h.recorder.reply(id); return ok })
		reply, _ := h.recorder.reply(id)
		return reply
	}
	send("c1", protocol.CommandSessionConnect, `{"pairing":"qr","groups":true}`)
	send("d1", protocol.CommandSessionDisconnect, `{}`)
	if seeded, err := container.Wanted(ctx); err != nil || len(seeded) != 0 {
		t.Fatalf("the given is not what this test needs: the sweep would already bring back %v "+
			"(err=%v) before the command under test ran", seeded, err)
	}

	// The engine answers an error and the socket opens anyway, which is what a resume
	// whose ceiling ran out does: `dial` returns `ctx.Err()` and says so in as many words,
	// and the connect it gave up waiting for carries on.
	session, ok := h.engine.Session("s1")
	if !ok {
		t.Fatal("the fake never handed out a session for s1")
	}
	session.FailConnect(errors.New("the ceiling ran out while the socket was coming up"))

	if reply := send("p1", protocol.CommandPairingRequestCode, `{"phone":"5511999990001"}`); reply.OK {
		t.Fatal("the engine refused the pairing code request and the client was told it worked")
	}

	wanted, err := container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(wanted) != 1 || wanted[0].SID != "s1" {
		t.Fatalf("after a pairing code request the engine refused, the sweep would bring back %v, "+
			"want just s1.\n"+
			"An engine that answers an error has not necessarily done nothing: a resume that ran "+
			"out of ceiling answers one and keeps connecting. Recorded only on success, that "+
			"account is in the air with nothing anywhere saying it should be, and the instance "+
			"after this one leaves it down.", wanted)
	}
	if !wanted[0].Groups {
		t.Fatalf("the row carries groups=%v, want the standing subscription", wanted[0].Groups)
	}
}
