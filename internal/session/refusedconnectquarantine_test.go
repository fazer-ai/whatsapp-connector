package session_test

import (
	"context"
	"errors"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// An account whose connect always fails stops being asked for.
//
// This is the other half of a decision, and without it the decision is a hope. A connect
// the engine refuses for its own reasons is recorded all the same, on the grounds that a
// device which would not open now is the kind of thing a later attempt fixes. The sweep is
// what makes the later attempt, so on an account that will never open, that same rule is a
// connector dialling WhatsApp for ever. What stops it is `cluster.Quarantine`, and what
// this test asserts is that the composed path -- record, engine refuses, the sweep's own
// callback -- actually reaches it.
//
// The mechanism itself is older than this branch (#241) and has fences of its own. What is
// new here is the path into it: before #266 a connect under this engine recorded nothing,
// so there was no sweep to quarantine. This asserts the join, and nothing about the
// backoff curve, which is the other tests' subject.
func TestAnAccountThatNeverOpensStopsBeingAskedFor(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	h := newHarnessWithStore(t, container)
	ctx := context.Background()

	// Seeded through the engine and the store rather than through a client's connect, and
	// the reason is the sweep's own precondition: `Resume` declines an account this
	// instance is already running, so the session cannot be one the manager holds. What is
	// under test is what a failed resume leaves behind, not who wrote the row -- the tests
	// beside this one cover that.
	engineSession, err := h.engine.Open(ctx, "s1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := engineSession.Connect(ctx, engine.ConnectRequest{Pairing: "qr"}); err != nil {
		t.Fatalf("pair the account: %v", err)
	}
	if err := container.For("s1").PutDesiredConnected(ctx, store.Wants{}); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
	}
	wanted, err := container.Wanted(ctx)
	if err != nil || len(wanted) != 1 {
		t.Fatalf("the given is not what this test needs: the sweep sees %v (err=%v), want s1", wanted, err)
	}

	// From here the account is one WhatsApp will not have: the device is gone, the
	// credentials no longer resume, whatever it is, every attempt ends the same way.
	session, ok := h.engine.Session("s1")
	if !ok {
		t.Fatal("the fake never handed out a session for s1")
	}
	session.FailConnect(errors.New("this account is never coming back"))

	if !h.manager.Resume("s1", store.Wants{}) {
		t.Fatal("the manager would not take the account the sweep offered it, so the resume " +
			"this test is about never happened")
	}

	waitFor(t, "the quarantine to hold s1", func() bool {
		waiting, err := h.quarantine.Waiting(ctx, []string{"s1"})
		return err == nil && len(waiting) == 1
	})

	// And the row survives it, because quarantine is a pause and not a decision about what
	// the client wants: clearing the record here would be the connector forgetting the
	// request on the account least likely to be asked for again by hand.
	after, err := container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(after) != 1 || after[0].SID != "s1" {
		t.Fatalf("after a resume that failed the sweep sees %v, want s1 still.\n"+
			"The quarantine holds the account back for a while; it does not answer the "+
			"question of whether a client asked for it, and an account dropped here is one "+
			"nobody brings back when the reason it failed goes away.", after)
	}
}
