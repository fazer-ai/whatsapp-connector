package session

import "testing"

// claimIdle decides whether an account adopted for a teardown that will not run may be
// taken back, and it is asked at the one moment nothing else can speak for: the heartbeat
// has already decided, and the session has had every chance to change since.
//
// Tested here, on the predicate itself, because one of its clauses cannot be reached
// through a running session. A command sits in the channel between `Offer` putting it
// there and the executor taking it out, and nothing in this package runs between those two
// statements: every seam that holds an executor -- `Hold`, `HoldUntilCanceled`, `OnDelete`
// -- blocks inside `Execute`, which is past the point where the command has been counted
// and `running` is above zero. A session built by hand has no executor to race, so the
// clause can be asked about directly rather than argued about in a comment.
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
			set:  func(s *Session) { s.running = 1 },
		},
		{
			name: "a command is waiting and has not started",
			set:  func(s *Session) { s.commands <- queued{} },
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
