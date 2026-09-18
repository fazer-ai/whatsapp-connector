package app_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// What a rolling deploy costs, measured on the deployment at chat.fazer.ai and reproduced
// here.
//
//	+4.317s  new container started
//	+5.025s  old container got SIGTERM
//	+6.286s  old container died
//
//	13:15:50  new:  connector is up
//	13:15:51  old:  connector is down
//	13:16:20  new:  bringing back a session ... / adopted a session   epoch=9
//
// Thirty seconds of an account being nobody's, on every release, and this connector is
// redeployed on every fix that lands.
//
// The mechanism is ordering and it is deterministic. The incoming instance makes its first
// resume pass on the way in, and at that instant the outgoing one still holds the leases:
// it exits about a second later. On the steady interval alone the next pass is a whole
// `resumeInterval` away. So the one early pass happens early by about a second, and the
// price is thirty.
//
// The test is shaped like that and not like a restart: the successor is started while the
// account is still owned, so its first pass finds the lease taken and comes back with
// nothing, exactly as the new container does a second before the old one exits.
//
// What is asserted is that the account is back **in the air**, by looking for an event
// published under the successor's own epoch, and that is the whole point rather than a
// flourish. Owning a session and running one are different things: the lease moving is not
// the account working, and an assertion on `Sessions()` alone passes on a successor that
// adopted the session and never dialled -- which is a deploy turning a thirty-second
// interruption into a permanent one, since `resumeOnce` skips what is already owned.
func TestTheSuccessorPutsBackWhatItsPredecessorGaveUpWithoutWaitingOutAWholeInterval(t *testing.T) {
	server := miniredis.RunT(t)
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "wa.db")

	const sid = "2f1c6f0e-0000-4000-8000-000000000268"
	seedWantedConnected(t, dsn, sid, "5511999990268")

	// Eight shards, said rather than left to the default, because the client below reads
	// the shard for a session with eight and the connector publishes with sixteen: for
	// half the session ids the two disagree and the read comes back empty with no error.
	// That is #269, and pinning it here is what stops this assertion from being vacuous.
	env := map[string]string{"WAC_DATABASE_URL": dsn, "WAC_EVENT_SHARDS": "8"}

	outgoing, stopOutgoing := startStoppable(t, server.Addr(), "inst-out", env)
	waitFor(t, "the outgoing instance to be running the account", func() bool {
		return outgoing.Sessions() == 1
	})

	incoming := start(t, server.Addr(), "inst-in", env)
	// Waited on the pass counter and not on `Sessions() == 0`, which is true the instant
	// the value exists and would be satisfied by a successor whose goroutines have never
	// run. That difference decides whether this test reproduces a deploy at all: if the
	// successor's first pass happened only after the predecessor let go, it would find the
	// account free and bring it back off that very pass, and the ramp this exists to prove
	// would not be needed to pass.
	//
	// So the barrier is a finished pass while the predecessor is still the owner, which is
	// exactly the position a new container is in a second before the old one exits.
	waitFor(t, "the successor to finish a pass while its predecessor still owns the account", func() bool {
		return incoming.ResumePasses() >= 1 && incoming.Sessions() == 0 && outgoing.Sessions() == 1
	})

	stopOutgoing()

	client := newClient(t, server.Addr())
	inTheAir := func() bool {
		for _, event := range client.events(context.Background(), sid) {
			if event.Type == protocol.EventSessionState && event.Epoch >= 2 {
				return true
			}
		}
		return false
	}
	waitFor(t, "the successor to put the account back in the air under its own epoch", inTheAir)
}

// The epoch is what tells the two owners apart on one shard, so a reader that ignored it
// would find the predecessor's `session.state` and call the hand-over done. This fences
// the assertion above against exactly that: before the successor has published anything,
// the shard already holds an event, and it is not evidence.
func TestAPredecessorsEventIsNotEvidenceThatTheSuccessorPutTheAccountBack(t *testing.T) {
	server := miniredis.RunT(t)
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "wa.db")

	const sid = "2f1c6f0e-0000-4000-8000-000000000269"
	seedWantedConnected(t, dsn, sid, "5511999990269")
	env := map[string]string{"WAC_DATABASE_URL": dsn, "WAC_EVENT_SHARDS": "8"}

	outgoing, _ := startStoppable(t, server.Addr(), "inst-out", env)
	waitFor(t, "the outgoing instance to be running the account", func() bool {
		return outgoing.Sessions() == 1
	})

	client := newClient(t, server.Addr())
	var epochs []uint64
	waitFor(t, "the predecessor to have published its state", func() bool {
		epochs = epochs[:0]
		for _, event := range client.events(context.Background(), sid) {
			if event.Type == protocol.EventSessionState {
				epochs = append(epochs, event.Epoch)
			}
		}
		return len(epochs) > 0
	})

	for _, epoch := range epochs {
		if epoch >= 2 {
			t.Fatalf("the only owner so far published under epoch %d, so `epoch >= 2` does not "+
				"separate a successor's work from its predecessor's and the hand-over assertion "+
				"next to this one proves nothing.\nEpochs seen: %v", epoch, epochs)
		}
	}
	if fmt.Sprint(epochs[0]) != "1" {
		t.Errorf("the first owner published under epoch %v, want 1", epochs[0])
	}
}

// The cool-off carries the name of whoever took it, and a shutdown may only drop its own.
//
// The case is not hypothetical and it is the one an unconditional delete gets wrong: the
// mark this instance took when it first brought the account back lasts a minute, and by
// the time it is asked to stop that minute may be over. A peer that swept in between holds
// its own mark, with an adoption still in flight behind it. Deleting that one hands the
// account to the next pass while the peer is still bringing it up, which is two instances
// asked for one account -- the single thing the mark exists to arbitrate.
//
// Written by hand rather than raced into place: what is under test is the delete, not the
// timing that produces the state, and a test that had to lose a race to be meaningful
// would be a test that usually proves nothing.
func TestAShutdownDropsItsOwnResumeCoolOffAndNobodyElses(t *testing.T) {
	server := miniredis.RunT(t)
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "wa.db")

	const sid = "2f1c6f0e-0000-4000-8000-00000000026a"
	seedWantedConnected(t, dsn, sid, "5511999990270")
	env := map[string]string{"WAC_DATABASE_URL": dsn, "WAC_EVENT_SHARDS": "8"}

	outgoing, stopOutgoing := startStoppable(t, server.Addr(), "inst-out", env)
	waitFor(t, "the outgoing instance to be running the account", func() bool {
		return outgoing.Sessions() == 1
	})

	// Stand in for the peer that swept after this instance's own mark had expired.
	mark := redisx.NewKeys("wa:", 8).Resume(sid)
	if err := server.Set(mark, "some-other-instance"); err != nil {
		t.Fatalf("plant a peer's turn: %v", err)
	}

	stopOutgoing()

	held, err := server.Get(mark)
	if err != nil {
		t.Fatalf("a shutdown deleted a turn belonging to another instance: the peer that "+
			"holds it is bringing the account up, and the next pass will now be offered "+
			"the same account: %v", err)
	}
	if held != "some-other-instance" {
		t.Errorf("the resume turn reads %q, want it untouched at %q", held, "some-other-instance")
	}
}
