package app

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// The takeover is a compare-and-set, and this is the table that makes it one.
//
// Between reading who holds a turn and replacing it, the mark can expire and be taken by a
// third instance that is alive and working. Stamping over that one is the collision the
// mark exists to prevent, arrived at by the code written to repair it -- so the write only
// lands while the turn still reads as the name that was found holding it.
//
// Driven against the script rather than through a sweep, because what separates the two
// cells is an interleaving no test can schedule: the second cell is the state that race
// leaves behind, written down directly.
func TestATurnIsOnlyTakenWhileItStillReadsAsTheOneThatWasFound(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		holdsNow string
		want     int
		wantMark string
	}{
		{
			name:     "still the instance that was found gone",
			holdsNow: "inst-dead",
			want:     1,
			wantMark: "inst-taking",
		},
		{
			name: "taken by somebody else in between",
			// The state the race leaves: the mark expired after the read, and a live
			// instance took the turn and is bringing the account up behind it.
			holdsNow: "inst-third",
			want:     0,
			wantMark: "inst-third",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := miniredis.RunT(t)
			client, err := redisx.New(redisx.Config{
				URL: "redis://" + srv.Addr(), Prefix: "wa:", Shards: 8,
			})
			if err != nil {
				t.Fatalf("dial the test redis: %v", err)
			}
			t.Cleanup(func() { _ = client.Close() })

			const sid = "sid-1"
			mark := client.Keys().Resume(sid)
			if err := srv.Set(mark, tc.holdsNow); err != nil {
				t.Fatalf("set the turn: %v", err)
			}

			took, err := takeTurnFromGone.Run(
				t.Context(), client, []string{mark}, "inst-dead", "inst-taking", int64(60000),
			).Int()
			if err != nil {
				t.Fatalf("run the takeover: %v", err)
			}
			if took != tc.want {
				t.Errorf("takeover returned %d, want %d", took, tc.want)
			}
			held, err := srv.Get(mark)
			if err != nil {
				t.Fatalf("the turn was deleted outright, and %q was supposed to hold it: "+
					"an account whose turn has gone is an account every survivor tries at "+
					"once on the next pass, which is the pile-up the mark exists to stop",
					tc.wantMark)
			}
			if held != tc.wantMark {
				t.Errorf("the turn now reads %q and should still read %q.\n"+
					"Writing over a turn that changed hands between the read and the write "+
					"reopens the race between two survivors: both hold a turn naming "+
					"themselves, both adopt, and the loser's adoption is an account handed "+
					"straight back. Arbitrating that is the only thing this mark does.",
					held, tc.wantMark)
			}
		})
	}
}

// The whole table the predicate decides, including the cells that answer "no", because
// three quite different states come out as "leave it alone" and only one of them is the
// one a sweep meets most often.
//
// The cell that nothing else reaches is "mine, and my own registry entry is not there".
// The early return on one's own name looks like a shortcut for the liveness read that
// follows it, and on a healthy instance it is: the entry is there, the instance reads as
// alive, and both paths answer no. It stops being a shortcut the moment the entry is not
// there, and there are two ways to get that and they are not the same one: an instance
// whose beat is late by more than the entry's TTL, and an instance that has just come up
// and has not beaten once. The second is the sharper case, because a sweep runs from the
// same goroutine loop the heartbeat does, and the first pass of the ramp can reach an
// account before the first beat has landed. Without the early return, such an instance
// reads ITSELF as gone,
// takes its own turn, and dials an account it decided a moment ago to leave alone: the
// retry floor removed by the code written to repair a different hole in it.
func TestWhoseTurnItIsDecidesWhetherItCanBeTaken(t *testing.T) {
	t.Parallel()

	const me = "inst-mine"
	for _, tc := range []struct {
		name string
		// holder is the name on the turn; empty means it expired between the write that
		// was refused and the read, which is a state a pass really does meet.
		holder string
		// announced are the instances with a registry entry.
		announced []string
		want      bool
		wantMark  string
	}{
		{
			name: "nobody holds it any more",
			// Expired between the refused write and the read. Nothing to take, and the
			// next pass finds it free, which is the outcome this function is for.
			holder:    "",
			announced: []string{me},
			want:      false,
		},
		{
			name:      "mine, and I am announced",
			holder:    me,
			announced: []string{me},
			want:      false,
			wantMark:  me,
		},
		{
			name: "mine, and my own beat has not landed",
			// Not a licence to take it: a turn in this instance's own name is its own
			// previous attempt far more often than it is a predecessor under a pinned
			// WAC_INSTANCE, and taking it is the cool-off gone.
			holder:    me,
			announced: nil,
			want:      false,
			wantMark:  me,
		},
		{
			name:      "a peer's, and the peer is announced",
			holder:    "inst-peer",
			announced: []string{me, "inst-peer"},
			want:      false,
			wantMark:  "inst-peer",
		},
		{
			name:      "a peer's, and the peer is gone from the fleet",
			holder:    "inst-peer",
			announced: []string{me},
			want:      true,
			wantMark:  me,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := miniredis.RunT(t)
			client, err := redisx.New(redisx.Config{
				URL: "redis://" + srv.Addr(), Prefix: "wa:", Shards: 8,
			})
			if err != nil {
				t.Fatalf("dial the test redis: %v", err)
			}
			t.Cleanup(func() { _ = client.Close() })

			const sid = "sid-1"
			mark := client.Keys().Resume(sid)
			if tc.holder != "" {
				if err := srv.Set(mark, tc.holder); err != nil {
					t.Fatalf("set the turn: %v", err)
				}
			}
			for _, instance := range tc.announced {
				srv.HSet(client.Keys().Instance(instance), "version", "test")
			}

			connector := &Connector{
				cfg: Config{Instance: me}, log: zerolog.Nop(), client: client,
			}
			got := connector.tookTurnFromAnInstanceThatIsGone(t.Context(), sid)
			if got != tc.want {
				t.Errorf("took the turn = %v, want %v", got, tc.want)
			}

			held, err := srv.Get(mark)
			if tc.wantMark == "" {
				if err == nil {
					t.Errorf("a turn was written where there was none, reading %q", held)
				}
				return
			}
			if err != nil {
				t.Fatalf("read the turn back: %v", err)
			}
			if held != tc.wantMark {
				t.Errorf("the turn now reads %q, want %q", held, tc.wantMark)
			}
		})
	}
}

// The compare-and-set as the sweep actually reaches it, which the table above does not
// cover and a mutant proved it does not: that table drives `takeTurnFromGone` directly, so
// replacing the *call* to it with a plain `SET` leaves every cell green while the sweep
// stamps over whatever it finds.
//
// The window is real and small: between learning who holds a turn and writing over it, the
// turn can expire and be taken by an instance that is alive and already bringing the
// account up. Here it is made to happen on purpose, by a hook on the one command that sits
// inside the window -- the liveness read of the holder -- which rewrites the turn into a
// third instance's name before the write lands.
//
// What is asserted is the outcome that costs something: the third instance's turn survives
// and this instance does not adopt. A plain `SET` gives the opposite, and then two
// instances hold a turn naming themselves, both adopt, and the loser's adoption is an
// account handed straight back.
//
// The hook is tied to the command the sweep issues today, and that is worth saying out
// loud: a change that decides liveness some other way -- `SISMEMBER wa:instances`, a cached
// registry read -- never fires it, so the turn is never handed over and this test goes red
// for a reason that is not the one it is named after. Red here means read the diff, not
// "the window is broken". The two tests above cover that particular change on its merits.
func TestATurnTakenByALiveThirdInstanceInsideTheWindowIsNotStampedOver(t *testing.T) {
	connector, container, _, srv := newResumeConnector(t)
	keys := redisx.NewKeys("wa:", 8)

	const sid = "sid-window"
	wantConnected(t, container, sid, "5511999990274", false)
	if err := srv.Set(keys.Resume(sid), "inst-dead"); err != nil {
		t.Fatalf("plant the dead instance's turn: %v", err)
	}
	srv.SetTTL(keys.Resume(sid), time.Minute)

	// The liveness read of the holder is the last thing that happens before the write, so a
	// hook on it lands inside the window and nowhere else. Scoped to that exact key: the
	// sweep issues other EXISTS calls, and firing on those would be rewriting the turn
	// before the decision rather than during it.
	var once sync.Once
	var mu sync.Mutex
	fired := false
	srv.Server().SetPreHook(func(_ *server.Peer, cmd string, args ...string) bool {
		if strings.EqualFold(cmd, "EXISTS") && len(args) == 1 && args[0] == keys.Instance("inst-dead") {
			once.Do(func() {
				mu.Lock()
				fired = true
				mu.Unlock()
				if err := srv.Set(keys.Resume(sid), "inst-third"); err != nil {
					t.Errorf("hand the turn to a live third instance: %v", err)
				}
				srv.SetTTL(keys.Resume(sid), time.Minute)
				srv.HSet(keys.Instance("inst-third"), "version", "test")
			})
		}
		return false
	})

	connector.resumeOnce(t.Context())

	// Asserted before anything else, because everything else is about what happened inside
	// a window this test is responsible for opening. A pass that never issued the command
	// the hook waits on leaves the turn in `inst-dead` and the account taken, and the
	// assertions below would then report a stamped-over turn that was never handed over:
	// a true-sounding failure about a race that did not happen.
	mu.Lock()
	opened := fired
	mu.Unlock()
	if !opened {
		t.Fatalf("the sweep never asked whether inst-dead is still in the fleet, so the "+
			"window this test exists to force was never opened. Nothing below is about the "+
			"compare-and-set: read the diff, and see %s.",
			"TestATurnHeldByAnInstanceStillInTheFleetIsLeftAlone")
	}

	held, err := srv.Get(keys.Resume(sid))
	if err != nil {
		t.Fatalf("the turn is gone: it was handed to inst-third inside the window and this "+
			"instance was supposed to leave it alone: %v", err)
	}
	if held != "inst-third" {
		t.Errorf("the turn now reads %q and should still read \"inst-third\".\n"+
			"It changed hands between the read and the write, and writing over it anyway "+
			"reopens the race the mark exists to arbitrate: two instances then hold a turn "+
			"naming themselves and both dial one account.", held)
	}
	if n := len(connector.manager.SIDs()); n != 0 {
		t.Errorf("this instance adopted %d session(s) over a turn a live instance had just "+
			"taken, want 0", n)
	}
}
