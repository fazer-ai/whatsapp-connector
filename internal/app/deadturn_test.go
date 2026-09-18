package app_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// The resume turn names whoever is trying, and an instance that dies takes the name with
// it and leaves the mark standing.
//
// `wa:resume:<sid>` paces retries: a pass that cannot take it skips the account entirely,
// and it lives a minute. The instance that took it is the one that had brought the account
// back, so a crash inside that minute leaves every survivor stepping over an account that
// nobody is coming for. #271 covers the instance that is asked to stop, because it clears
// its own marks on the way out; nothing clears the marks of one that is killed.
//
// What settles it is already in Redis and already right. `wa:instance:<name>` is refreshed
// by the heartbeat and lives `3 * WAC_HEARTBEAT`, fifteen seconds by default, so fifteen
// seconds after the last beat the registry knows what the mark does not. The gap between
// the two is the defect, and at the defaults it is forty-five seconds wide.
//
// Nothing here waits for a key to expire, and that is deliberate: under miniredis nothing
// expires on its own, so any recovery this test sees is the mark being taken rather than
// timing out. The instrument cannot pass by waiting.
func TestASurvivorTakesTheTurnOfAnInstanceThatIsGone(t *testing.T) {
	server := miniredis.RunT(t)
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "wa.db")

	const sid = "2f1c6f0e-0000-4000-8000-000000000272"
	seedWantedConnected(t, dsn, sid, "5511999990272")

	// What a killed instance leaves behind: no lease, and a turn in a name nobody answers
	// to. No `wa:instance:inst-dead`, which is what makes it gone rather than quiet.
	keys := redisx.NewKeys("wa:", 0)
	if err := server.Set(keys.Resume(sid), "inst-dead"); err != nil {
		t.Fatalf("plant the dead instance's turn: %v", err)
	}
	server.SetTTL(keys.Resume(sid), time.Minute)
	// And still in the directory, which is the half of the state that makes this test
	// discriminating rather than merely green. `wa:instances` is a set with no TTL of its
	// own: an instance that dies leaves its name there until something prunes it, while
	// its entry expires three heartbeats later. So a fix that asked the set instead of the
	// entry would read `inst-dead` as alive and leave the account down, and without this
	// line the set is empty and such a fix passes.
	if _, err := server.SetAdd(keys.Instances(), "inst-dead"); err != nil {
		t.Fatalf("leave the dead instance in the directory: %v", err)
	}

	survivor := start(t, server.Addr(), "inst-alive",
		map[string]string{"WAC_DATABASE_URL": dsn})

	client := newClient(t, server.Addr())
	waitFor(t, "the survivor to put the account back in the air over a dead instance's turn", func() bool {
		for _, event := range client.events(context.Background(), sid) {
			if event.Type == protocol.EventSessionState {
				return true
			}
		}
		return false
	})
	if survivor.Sessions() != 1 {
		t.Errorf("the survivor runs %d sessions, want 1", survivor.Sessions())
	}
}

// The control that decides whether the fix is a repair or a removal: a turn held by an
// instance that is still in the fleet keeps working.
//
// Without this, the cheapest way to pass the test above is to stop reading the mark at
// all, and then two instances dial one account on every pass — which is worse than the
// defect, and is the single thing the mark was written to prevent.
//
// The two tests differ in exactly one variable, and it is the one the fix is allowed to
// read: whether `wa:instance:<holder>` is there. Everything else is identical.
func TestATurnHeldByAnInstanceStillInTheFleetIsLeftAlone(t *testing.T) {
	server := miniredis.RunT(t)
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "wa.db")

	const sid = "2f1c6f0e-0000-4000-8000-000000000273"
	seedWantedConnected(t, dsn, sid, "5511999990273")

	keys := redisx.NewKeys("wa:", 0)
	if err := server.Set(keys.Resume(sid), "inst-peer"); err != nil {
		t.Fatalf("plant the peer's turn: %v", err)
	}
	server.SetTTL(keys.Resume(sid), time.Minute)
	// The one difference from the test above: this holder is announced, so it is a peer
	// that is working on the account rather than a name left behind by a crash.
	server.HSet(keys.Instance("inst-peer"), "version", "test")
	server.SetTTL(keys.Instance("inst-peer"), 15*time.Second)

	survivor := start(t, server.Addr(), "inst-alive",
		map[string]string{"WAC_DATABASE_URL": dsn})

	waitFor(t, "the instance to have swept a few times", func() bool {
		return survivor.ResumePasses() >= 3
	})
	if survivor.Sessions() != 0 {
		t.Fatalf("an account whose turn belongs to a live peer was taken anyway: the mark "+
			"stops meaning anything and two instances dial one account on every pass, which "+
			"is what it was written to prevent. Sessions = %d", survivor.Sessions())
	}
	held, err := server.Get(keys.Resume(sid))
	if err != nil {
		t.Fatalf("the live peer's turn was deleted: %v", err)
	}
	if held != "inst-peer" {
		t.Errorf("the turn reads %q, want it untouched at %q", held, "inst-peer")
	}

	// And the account is not stuck for a reason unrelated to the mark: with the peer gone
	// from the fleet, the same instance brings it back.
	server.Del(keys.Instance("inst-peer"))
	waitFor(t, "the account to come back once its holder leaves the fleet", func() bool {
		return survivor.Sessions() == 1
	})
}

// The edge the fix does not close, measured rather than assumed, and fenced so that it
// cannot stop being a deliberate choice.
//
// Deciding by identity means the question is "whose name is on this turn", and a name is
// only as good as its uniqueness. `WAC_INSTANCE` defaults to the hostname, and README.md
// says what that is worth: in a container the hostname is the container id, unique per
// replica, so a process that dies and comes back is a different name and the turn it left
// reads as gone. That is the deployment shape this repository ships and the one #272 was
// opened about.
//
// Pin `WAC_INSTANCE` to a fixed string and the property is gone: the replacement announces
// the same name, its predecessor's turn reads as its own, and the account waits out the
// whole minute exactly as it did before the fix. That is what this test measures.
//
// Treating a turn in one's own name as free is NOT the repair it looks like, and this is
// where the two cases stop being distinguishable. A turn in this instance's own name has
// two causes: a predecessor under a pinned name, and this instance's own previous pass,
// whose attempt failed. Nothing in the mark tells them apart, and the second is the retry
// floor itself — the account that could not connect is meant to be left alone for a minute.
// Taking one's own turn would dial it again every pass, which is the cool-off removed and
// the account hammered. So the choice here is to leave it, and the cost is that a pinned
// `WAC_INSTANCE` keeps the defect on that one path.
func TestATurnInThisInstancesOwnNameIsLeftAloneEvenWhenItsPredecessorLeftIt(t *testing.T) {
	server := miniredis.RunT(t)
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "wa.db")

	const sid = "2f1c6f0e-0000-4000-8000-000000000279"
	seedWantedConnected(t, dsn, sid, "5511999990279")

	// A deployment that pins the name: the turn the dead process left carries the name its
	// replacement comes up under.
	keys := redisx.NewKeys("wa:", 0)
	if err := server.Set(keys.Resume(sid), "inst-pinned"); err != nil {
		t.Fatalf("plant the predecessor's turn under the pinned name: %v", err)
	}
	server.SetTTL(keys.Resume(sid), time.Minute)

	replacement := start(t, server.Addr(), "inst-pinned",
		map[string]string{"WAC_DATABASE_URL": dsn})

	waitFor(t, "the replacement to have swept a few times", func() bool {
		return replacement.ResumePasses() >= 3
	})
	if replacement.Sessions() != 0 {
		t.Fatalf("a turn in this instance's own name was taken: that is the retry floor "+
			"gone, and an account that failed to connect gets dialled on every pass. "+
			"Sessions = %d", replacement.Sessions())
	}

	// And the account is not stuck for some reason of its own: the same instance, same
	// sweep, same everything, brings it back the moment the name on the turn is one it
	// does not answer to. The difference between waiting out the minute and coming back
	// is the name, which is what makes the limitation a naming one.
	if err := server.Set(keys.Resume(sid), "inst-gone"); err != nil {
		t.Fatalf("rename the turn to a stranger nobody announces: %v", err)
	}
	server.SetTTL(keys.Resume(sid), time.Minute)
	waitFor(t, "the account to come back once the turn carries a name this instance does not answer to", func() bool {
		return replacement.Sessions() == 1
	})
}
