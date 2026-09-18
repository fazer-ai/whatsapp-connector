package session_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// A teardown the connector never sent is answered with a word of its own, and not with
// the one every other command uses for "we do not know how far this got".
//
// The engine already tells the two apart and already answers a failure rather than
// throwing the credentials away (#208). What a client could not tell apart was the
// answer: the failure arrived as `timeout`, because the error wraps the caller's own
// expired context and that is the case `asProtocolError` matched first. `timeout` is
// literally true and says the wrong thing -- it is the word for a send that may or may
// not be on somebody's phone by now, and here nothing left this process at all, so the
// account is exactly as it was and a retry does the whole thing.
//
// Asserted against the string on the wire rather than against the constant, because a
// test that compares the constant to itself passes on any spelling, and this value is the
// client's rather than ours. The stimulus is built the same way: the code goes in as a
// string, so this file says nothing about whether the catalogue has it -- which is the
// point, since a code outside the catalogue is degraded to `internal` before it is sent.
func TestATeardownThatWasNeverSentIsAnsweredWithItsOwnWord(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.manager.Adopt(ctx, "s-unsent"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	opened, ok := h.engine.Session("s-unsent")
	if !ok {
		t.Fatal("given: the engine has no session for an account just adopted")
	}
	// The shape the engine produces when the caller's time ran out while a dial held the
	// socket: a coded failure, carrying the caller's expired context underneath it.
	opened.FailDelete(fmt.Errorf("whatsmeow: delete s-unsent: %w: %w",
		protocol.NewError(protocol.ErrorCode("not_attempted"), "the unlink was never sent"),
		context.DeadlineExceeded))

	var acked atomic.Bool
	h.manager.Dispatch(delivery(&protocol.Command{
		V: protocol.Version, ID: "c-unsent", Type: protocol.CommandSessionDelete, SID: "s-unsent",
		ReplyTo: "c-unsent", Payload: json.RawMessage(`{"idempotency_key":"k-unsent"}`),
	}, &acked))
	waitFor(t, "the teardown to be answered", func() bool { _, ok := h.recorder.reply("c-unsent"); return ok })

	reply, _ := h.recorder.reply("c-unsent")
	if reply.OK {
		t.Fatal("a teardown that was never sent answered success; the credentials that would sign the unlink are gone and the device stays listed on the phone for good")
	}
	if reply.Error == nil {
		t.Fatal("the failure carried no error at all")
	}
	if got := string(reply.Error.Code); got != "not_attempted" {
		t.Fatalf("a teardown that never left answered %q, want \"not_attempted\". A word outside the catalogue is degraded to \"internal\" before it is sent, so a client reading this has to assume the teardown went half way; here nothing was sent and a retry does the whole thing",
			got)
	}
}

// And the word does not spread to the failure it is easy to confuse it with: a context
// that expired with no word of its own is still `timeout`, because there the connector
// does not know what happened. This one is green on the base too, on purpose: a change
// whose only red was the new word would have fenced nothing about the old one.
func TestAnOrdinaryExpiredContextIsStillTimeout(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	ctx := context.Background()
	if _, err := h.manager.Adopt(ctx, "s-timeout"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	opened, ok := h.engine.Session("s-timeout")
	if !ok {
		t.Fatal("given: the engine has no session for an account just adopted")
	}
	opened.FailDelete(fmt.Errorf("whatsmeow: delete s-timeout: %w", context.DeadlineExceeded))

	var acked atomic.Bool
	h.manager.Dispatch(delivery(&protocol.Command{
		V: protocol.Version, ID: "c-timeout", Type: protocol.CommandSessionDelete, SID: "s-timeout",
		ReplyTo: "c-timeout", Payload: json.RawMessage(`{"idempotency_key":"k-timeout"}`),
	}, &acked))
	waitFor(t, "the teardown to be answered", func() bool { _, ok := h.recorder.reply("c-timeout"); return ok })

	reply, _ := h.recorder.reply("c-timeout")
	if got := string(reply.Error.Code); got != "timeout" {
		t.Fatalf("a command that ran out of time answered %q, want \"timeout\": the new word must mean the connector knows nothing was sent, not merely that a deadline passed", got)
	}
}
