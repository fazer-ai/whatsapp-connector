package session

import (
	"testing"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// claimIdle decides whether an account adopted for a teardown that will not run may be
// taken back, and it is asked at the one moment nothing else can speak for: the heartbeat
// has already decided, and the session has had every chance to change since.
//
// Tested here, on the predicate itself, because two of the states it decides cannot be
// reached through a running session. A command sits in the channel between `Offer` putting
// it there and the executor taking it out, and it sits nowhere at all between the executor
// taking it out and `admit` counting it: nothing in this package runs between either pair
// of statements, and every seam that holds an executor -- `Hold`, `HoldUntilCanceled`,
// `OnDelete` -- blocks inside `Execute`, which is past both. A session built by hand has no
// executor to race, so the states can be asked about directly rather than argued about in a
// comment. The second of them is what the hand-back would otherwise read as idle while a
// client waits on a command this session had already accepted.
func TestClaimIdleRefusesAnythingButAnIdleSession(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		set     func(s *Session)
		carried int64
		want    bool
	}{
		{name: "idle, and the count is the one decided on", want: true},
		{
			name: "already stopping",
			set:  func(s *Session) { s.stopping = true },
		},
		{
			name: "the door is shut for a session on its way out",
			set:  func(s *Session) { s.shutFor = 1 },
		},
		{
			name: "a command is running",
			set:  func(s *Session) { s.running, s.accepted = 1, 1 },
		},
		{
			name: "a command is waiting and has not started",
			set:  func(s *Session) { s.commands <- queued{}; s.accepted = 1 },
		},
		{
			// The executor has taken it off the channel and has not reached `admit`, so
			// it is in neither of the two places the other clauses look.
			name: "a command was taken off the queue and not yet admitted",
			set:  func(s *Session) { s.accepted = 1 },
		},
		{
			name:    "a command was answered since the count was taken",
			set:     func(s *Session) { s.carried.Store(4) },
			carried: 3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			session := &Session{commands: make(chan queued, 4)}
			if tc.set != nil {
				tc.set(session)
			}
			was := session.stopping
			if got := session.claimIdle(tc.carried); got != tc.want {
				t.Fatalf("claimIdle(%d) = %v, want %v", tc.carried, got, tc.want)
			}
			// A refusal must leave the door as it found it: the session goes on serving
			// and the next tick asks again. Shutting it on the way out would strand the
			// account with nothing running it and no hand-back coming.
			if !tc.want && session.stopping != was {
				t.Fatal("a refused claim shut the session's door anyway; the account is left with no executor and no hand-back")
			}
		})
	}
}

// Every delivery this session takes is counted out again, by every ending it has.
//
// `accepted` is what the hand-back asks about, and it is only worth asking if it comes
// back down. The endings are four and only one of them is the ordinary one: a command that
// was answered, one turned away at the door because the session is stopping, one turned
// away because the engine has said its last word, and one still on the queue when the
// executor gave up. Three of those leave the session on its way out, so a count stuck above
// zero there changes no decision today -- `claimIdle` asks about `stopping` and `shutFor`
// first -- and that is exactly why it needs asserting here instead of being left to a test
// that would pass either way.
func TestEveryDeliveryThisSessionTakesIsCountedOutAgain(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		end  func(s *Session)
	}{
		{
			name: "answered",
			end: func(s *Session) {
				if !s.admit() {
					t.Fatal("given: an idle session turned a command away at the door")
				}
				s.doneWith()
			},
		},
		{
			name: "turned away at a door that is already shutting",
			end:  func(s *Session) { s.stopping = true; s.admit() },
		},
		{
			name: "turned away because the engine has said its last word",
			end:  func(s *Session) { s.shutFor = 1; s.admit() },
		},
		{
			name: "still on the queue when the executor gave up",
			end:  func(s *Session) { s.abandonQueue() },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			session := &Session{commands: make(chan queued, 4), now: time.Now}
			if offer := session.Offer(&transport.Delivery{Release: func() {}}); offer != OfferAccepted {
				t.Fatalf("given: the offer was not accepted: %v", offer)
			}
			if session.accepted != 1 {
				t.Fatalf("given: an accepted delivery was not counted: accepted=%d", session.accepted)
			}
			tc.end(session)
			if session.accepted != 0 {
				t.Fatalf("a delivery this session finished with is still counted as outstanding: accepted=%d. The hand-back reads this, and a count that only ever goes up is an account nothing will take back", session.accepted)
			}
		})
	}
}
