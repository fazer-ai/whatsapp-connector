package app_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// A deployment pairs an account and leaves behind what brings it back, with nothing
// seeded by hand.
//
// This goes through the door a deployment goes through -- a connector built by `Run` from
// its configuration, a `session.wake` and a `session.connect` on the streams the contract
// names -- because the two halves of #266 are wiring, and wiring is exactly what a unit
// test with its collaborators passed in by hand cannot fail on. Both halves were written
// and both had their own test, and the fleet still recorded nothing, because the engine
// this build hands to the manager was still being constructed without a store. Nothing in
// the suite noticed: every test that asserted a row had put the store there itself.
//
// So the subject here is the construction, and the assertion is the one an operator
// cares about: after this, is there anything in the database that would bring the account
// back? `store.Wanted` is the reading the sweep does, and it joins the request with the
// pairing, so whichever half the wiring dropped is enough to fail it.
func TestADeploymentLeavesBehindWhatBringsAnAccountBack(t *testing.T) {
	server := miniredis.RunT(t)
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "wa.db")
	ctx := context.Background()

	const sid = "2f1c6f0e-0000-4000-8000-0000000000d1"
	connector := start(t, server.Addr(), "inst-a", map[string]string{"WAC_DATABASE_URL": dsn})
	client := newClient(t, server.Addr())

	// Woken first, because `wa:cmd:<sid>` is read only for a session this instance is
	// already running: a connect for a session nobody adopted reaches no connector at
	// all. That is the contract's own order, and skipping it is a test that waits for an
	// answer nobody was asked for.
	client.send(ctx, client.key.Control(), &protocol.Command{
		V: protocol.Version, ID: "wake-1", Type: protocol.CommandSessionWake, SID: sid,
		Payload: json.RawMessage(`{}`),
	})
	waitFor(t, "the woken session to be adopted", func() bool { return connector.Sessions() == 1 })

	client.send(ctx, client.key.Commands(sid), &protocol.Command{
		V: protocol.Version, ID: "connect-1", Type: protocol.CommandSessionConnect, SID: sid,
		ReplyTo: client.key.Reply("connect-1"), Payload: json.RawMessage(`{"pairing":"qr","groups":true}`),
	})
	if reply := client.await(ctx, "connect-1", 10*time.Second); !reply.OK {
		t.Fatalf("the connect was refused: %+v", reply)
	}

	container, err := store.Open(ctx, dsn, store.AlwaysOwned, zerolog.Nop())
	if err != nil {
		t.Fatalf("open the store this deployment was pointed at: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := container.Close(); closeErr != nil {
			t.Errorf("Close: %v", closeErr)
		}
	})

	wanted, err := container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(wanted) != 1 || wanted[0].SID != sid {
		t.Fatalf("after a deployment paired this account the sweep reads %v, want %s.\n"+
			"The account is up and answering and nothing anywhere says it should be: kill "+
			"this process and it stays down until somebody opens the inbox and asks again, "+
			"which is what fazer-ai/chatwoot#577 was. `Wanted` joins the request with the "+
			"pairing, so the half that is missing is the half the wiring dropped.",
			wanted, sid)
	}
	if !wanted[0].Groups {
		t.Fatal("the account would come back without the group traffic its client asked " +
			"for, acknowledging it and publishing it nowhere")
	}
}
