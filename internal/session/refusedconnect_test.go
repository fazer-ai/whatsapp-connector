package session_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// A connect this build refuses by construction leaves nothing for the sweep to repeat.
//
// The record of what a client asked for moved out of the whatsmeow engine in #266 and now
// sits here, written before the engine is called so that an instance dying inside a
// connect still leaves a resumable account. That ordering put the write in front of five
// refusals that used to sit above it, and each of them is a request no later attempt can
// get through: a proxy and a history sync this build does not do, and two payloads it
// cannot read. Recorded, they would have the sweep dial the account again every pass, for
// ever.
//
// The proxy is the one with teeth, and it is why this is a correctness test and not a
// tidiness one. The connect a sweep synthesises carries no proxy at all -- `store.Wants`
// holds the subscription and the call policy, and nothing else -- so an account recorded
// from a refused proxy request comes back connected *directly*, putting the deployment's
// own address on the wire. That is the exact outcome the refusal exists to prevent, and
// the client is never told, because from its side the command it sent failed.
//
// Both halves are asserted for each request, because either alone passes for the wrong
// reason: a refusal with the row written is the defect above, and an empty table with the
// wrong error code is a connector that refuses for a reason the client cannot act on.
func TestAConnectThisBuildRefusesIsNotRecorded(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		payload string
		code    protocol.ErrorCode
	}{
		{"proxy", `{"pairing":"qr","proxy":{"url":"socks5://10.0.0.1:1080"}}`, protocol.ErrorUnsupported},
		{"history sync", `{"pairing":"qr","history_sync":true}`, protocol.ErrorUnsupported},
		{"unknown pairing mode", `{"pairing":"telepatia"}`, protocol.ErrorInvalidPayload},
		{"code pairing without a phone", `{"pairing":"code","phone":"+ ()-"}`, protocol.ErrorInvalidPayload},
	}

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			container := openStore(t)
			h := newHarnessWithStore(t, container)
			ctx := context.Background()
			sid := fmt.Sprintf("s%d", i+1)

			if _, err := h.manager.Adopt(ctx, sid); err != nil {
				t.Fatalf("Adopt: %v", err)
			}

			// The account is paired and then turned off, and that given is load-bearing
			// twice over. It is the sequence with teeth -- an operator whose inbox is
			// disconnected switching on egress routing and hitting connect -- and it is
			// what makes the assertion below able to fail: `Wanted` joins the desired row
			// with the pairing, so on a session that never paired it reads empty whether
			// or not the row was written, and a version of this test without these two
			// commands passed with the fix removed.
			pair := "p" + sid
			h.manager.Dispatch(delivery(&protocol.Command{
				V: protocol.Version, ID: pair, Type: protocol.CommandSessionConnect, SID: sid, ReplyTo: pair,
				Payload: json.RawMessage(`{"pairing":"qr"}`),
			}, &atomic.Bool{}))
			waitFor(t, "the account to pair", func() bool { _, ok := h.recorder.reply(pair); return ok })
			down := "d" + sid
			h.manager.Dispatch(delivery(&protocol.Command{
				V: protocol.Version, ID: down, Type: protocol.CommandSessionDisconnect, SID: sid, ReplyTo: down,
			}, &atomic.Bool{}))
			waitFor(t, "the session to be turned off", func() bool { _, ok := h.recorder.reply(down); return ok })
			if seeded, err := container.Wanted(ctx); err != nil || len(seeded) != 0 {
				t.Fatalf("the given is not what this test needs: after pairing and disconnecting, "+
					"the sweep would bring back %v (err=%v), want nothing. Without that the "+
					"assertion below cannot tell a written row from an unwritten one.", seeded, err)
			}

			id := "c" + sid
			h.manager.Dispatch(delivery(&protocol.Command{
				V: protocol.Version, ID: id, Type: protocol.CommandSessionConnect, SID: sid, ReplyTo: id,
				Payload: json.RawMessage(c.payload),
			}, &atomic.Bool{}))
			waitFor(t, "the connect to be answered", func() bool { _, ok := h.recorder.reply(id); return ok })

			reply, _ := h.recorder.reply(id)
			switch {
			case reply.OK:
				t.Fatalf("a connect carrying %s was answered ok.\n"+
					"This build does not serve that request, so answering it leaves the client "+
					"waiting for something that is never going to happen.", c.name)
			case reply.Error == nil:
				t.Fatalf("a connect carrying %s failed with no error object at all", c.name)
			case reply.Error.Code != c.code:
				t.Fatalf("a connect carrying %s was refused with %q, want %q.\n"+
					"The code is what the client branches on, and it is the same code whichever "+
					"engine the deployment runs, because both ask `ConnectRequest.Validate`.",
					c.name, reply.Error.Code, c.code)
			}

			// Read through `Wanted`, which is what the sweep reads and what would act on a
			// row written here. A table with no row and a row the sweep cannot act on are
			// the same answer to the question this test asks.
			var desired string
			if err := container.DB().QueryRowContext(ctx,
				`SELECT desired FROM wac_session_desired WHERE sid = '`+sid+`'`).Scan(&desired); err != nil {
				t.Fatalf("read the desired state of %s back: %v", sid, err)
			}
			if desired != "disconnected" {
				t.Fatalf("after a connect carrying %s was refused, %s reads %q, want %q.\n"+
					"The operator turned this account off and the connector refused to turn it back "+
					"on, so the only thing that may be written here is what the operator asked for.",
					c.name, sid, desired, "disconnected")
			}
			wanted, err := container.Wanted(ctx)
			if err != nil {
				t.Fatalf("Wanted: %v", err)
			}
			if len(wanted) != 0 {
				t.Fatalf("after a connect carrying %s was refused, the sweep would bring back %v.\n"+
					"Nothing can make that request succeed -- it is this build refusing a capability, "+
					"or a payload it cannot read -- so the sweep retries it every pass for ever. For "+
					"the proxy it is worse than a wasted pass: the synthesised connect carries no "+
					"proxy, so the account comes back dialling WhatsApp directly and the deployment's "+
					"address goes on the wire, which is what the refusal existed to prevent.",
					c.name, wanted)
			}
		})
	}
}

// The same four requests, asked of the shape rather than of a running session.
//
// This is the half that holds for every engine at once. `Validate` is a method on the
// request, both production engines call it, and the session layer calls it before it
// records: an engine that answered one of these differently would have a client's error
// code depend on which engine its deployment happens to run, which is the divergence the
// fake engine had before #266 (a bare error, reaching the client as `internal`, where
// whatsmeow answered `invalid_payload`).
func TestTheRefusalsOfAConnectDoNotDependOnTheEngine(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		request engine.ConnectRequest
		code    protocol.ErrorCode
	}{
		{"proxy", engine.ConnectRequest{Pairing: "qr", Proxy: &engine.ProxyRequest{URL: "socks5://10.0.0.1:1080"}}, protocol.ErrorUnsupported},
		{"history sync", engine.ConnectRequest{Pairing: "qr", HistorySync: true}, protocol.ErrorUnsupported},
		{"unknown pairing mode", engine.ConnectRequest{Pairing: "telepatia"}, protocol.ErrorInvalidPayload},
		{"code pairing without a phone", engine.ConnectRequest{Pairing: "code", Phone: "+ ()-"}, protocol.ErrorInvalidPayload},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			err := c.request.Validate()
			if err == nil {
				t.Fatalf("a connect carrying %s passed validation", c.name)
			}
			var coded *protocol.Error
			if !errors.As(err, &coded) {
				t.Fatalf("a connect carrying %s was refused with %v, which carries no error code.\n"+
					"Everything that crosses the wire degrades to a `protocol.ErrorCode`, and an "+
					"uncoded refusal reaches the client as `internal`.", c.name, err)
			}
			if coded.Code != c.code {
				t.Fatalf("a connect carrying %s was refused with %q, want %q", c.name, coded.Code, c.code)
			}
		})
	}

	// The negative control: the three modes this build does serve pass, so the assertions
	// above are about these four requests and not about validation refusing everything.
	for _, request := range []engine.ConnectRequest{
		{Pairing: "resume"},
		{Pairing: "qr", Groups: true},
		{Pairing: "code", Phone: "+55 (11) 99999-0001"},
	} {
		if err := request.Validate(); err != nil {
			t.Fatalf("a %s connect this build serves was refused: %v", request.Pairing, err)
		}
	}
}
