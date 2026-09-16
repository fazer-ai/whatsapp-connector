package session

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

type doneCommand struct {
	kind    protocol.CommandType
	outcome string
	took    time.Duration
}

type spyWatch struct {
	mu     sync.Mutex
	done   []doneCommand
	leases int
}

func (s *spyWatch) CommandDone(kind protocol.CommandType, outcome string, took time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.done = append(s.done, doneCommand{kind: kind, outcome: outcome, took: took})
}

func (s *spyWatch) LeaseLost() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leases++
}

func (s *spyWatch) all() []doneCommand {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]doneCommand(nil), s.done...)
}

// The outcome label is the contract's error code and not a bare "error", because the
// question an operator asks is which kind of failure got slower. A histogram that only
// knows ok/error cannot answer it.
func TestAFinishedCommandIsReportedWithItsOutcome(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{name: "carried out", err: nil, want: "ok"},
		{
			name: "refused with a contract code",
			err:  protocol.NewError(protocol.ErrorNotConnected, "no socket"),
			want: string(protocol.ErrorNotConnected),
		},
		{
			name: "failed with something the contract has no word for",
			err:  errors.New("a bare error from somewhere"),
			want: string(protocol.ErrorInternal),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			watch := &spyWatch{}
			ticks := []time.Time{time.Unix(0, 0), time.Unix(0, 0).Add(1500 * time.Millisecond)}
			var read int
			s := &Session{
				watch: watch,
				now: func() time.Time {
					if read < len(ticks) {
						read++
					}
					return ticks[read-1]
				},
			}
			command := &protocol.Command{Type: protocol.CommandMessageSend}
			s.reportCommand(command, s.now(), tc.err)

			seen := watch.all()
			if len(seen) != 1 {
				t.Fatalf("reported %d commands, want 1", len(seen))
			}
			if seen[0].outcome != tc.want {
				t.Errorf("outcome = %q, want %q", seen[0].outcome, tc.want)
			}
			if seen[0].kind != protocol.CommandMessageSend {
				t.Errorf("kind = %q, want %q", seen[0].kind, protocol.CommandMessageSend)
			}
			if seen[0].took != 1500*time.Millisecond {
				t.Errorf("took = %s, want 1.5s", seen[0].took)
			}
		})
	}
}

// Nobody watching must not be a crash: it is what every test that is not about the
// numbers passes, and what a build without metrics would pass.
func TestAFinishedCommandWithNobodyWatchingIsNotACrash(t *testing.T) {
	t.Parallel()

	s := &Session{now: time.Now}
	s.reportCommand(&protocol.Command{Type: protocol.CommandSessionStatus}, s.now(), nil)
}

// The manager answers two commands itself, and neither reaches a session: admin.ping and
// the rate_limited refusal a full queue produces. Leaving them out of the histogram would
// omit exactly the overload refusals an operator goes looking for, and omit them when
// there are most of them, because that is when the queue is full.
func TestTheCommandsTheManagerAnswersItselfAreReported(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		kind    protocol.CommandType
		err     error
		outcome string
	}{
		{name: "admin.ping", kind: protocol.CommandAdminPing, err: nil, outcome: "ok"},
		{
			name:    "a full queue refusing",
			kind:    protocol.CommandMessageSend,
			err:     protocol.NewError(protocol.ErrorRateLimited, "too many waiting"),
			outcome: string(protocol.ErrorRateLimited),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			watch := &spyWatch{}
			ticks := []time.Time{time.Unix(0, 0), time.Unix(0, 0).Add(700 * time.Millisecond)}
			var read int
			m := &Manager{watch: watch, now: func() time.Time {
				if read < len(ticks) {
					read++
				}
				return ticks[read-1]
			}}
			m.reportCommand(&protocol.Command{Type: tc.kind}, m.now(), tc.err)

			seen := watch.all()
			if len(seen) != 1 {
				t.Fatalf("reported %d commands, want 1", len(seen))
			}
			if seen[0].kind != tc.kind || seen[0].outcome != tc.outcome {
				t.Errorf("reported %q/%q, want %q/%q", seen[0].kind, seen[0].outcome, tc.kind, tc.outcome)
			}
			if seen[0].took != 700*time.Millisecond {
				t.Errorf("took = %s, want 700ms: timed from the dispatch that picked it up", seen[0].took)
			}
		})
	}
}

// A renewal that came back unlucky while the session was already gone, or already
// replaced, stopped nothing. Counting it would report an ordinary concurrent release as a
// lease this instance lost, which is the opposite of the shape the counter exists to show.
func TestOnlyALeaseLossThatStoppedASessionIsCounted(t *testing.T) {
	t.Parallel()

	watch := &spyWatch{}
	m := &Manager{watch: watch}

	m.lostLease(true)
	m.lostLease(false) // the drop found nothing: this renewal stopped no session
	m.lostLease(false)
	m.lostLease(true)

	watch.mu.Lock()
	got := watch.leases
	watch.mu.Unlock()
	if got != 2 {
		t.Errorf("counted %d lease losses, want 2: only the drops that stopped a session count", got)
	}
}

// Nobody watching must not be a crash on this path either.
func TestALeaseLossWithNobodyWatchingIsNotACrash(t *testing.T) {
	t.Parallel()
	(&Manager{}).lostLease(true)
}

// The wait in the session's own queue is the caller's wait. Timed from the far side of it,
// a command that sat a minute behind a backlog reports as having taken a millisecond --
// and the manager's own paths already time from the dispatch that picked the command up,
// so the two halves of one histogram would be measuring different spans.
func TestTheWaitInTheQueueIsPartOfTheLatency(t *testing.T) {
	t.Parallel()

	var now time.Time
	s := &Session{
		commands: make(chan queued, 4),
		now:      func() time.Time { return now },
	}

	now = time.Unix(100, 0)
	if got := s.Offer(&transport.Delivery{Command: protocol.Command{Type: protocol.CommandMessageSend}}); got != OfferAccepted {
		t.Fatalf("offer = %v, want accepted", got)
	}

	// Two minutes behind a backlog before its turn comes.
	now = time.Unix(220, 0)
	waiting := <-s.commands
	if stamped := waiting.at; !stamped.Equal(time.Unix(100, 0)) {
		t.Fatalf("stamped at %s, want the instant it was accepted (100)", stamped)
	}

	watch := &spyWatch{}
	s.watch = watch
	s.reportCommand(&waiting.delivery.Command, waiting.at, nil)

	seen := watch.all()
	if len(seen) != 1 {
		t.Fatalf("reported %d commands, want 1", len(seen))
	}
	if seen[0].took != 120*time.Second {
		t.Errorf("took = %s, want 2m: the queue wait is the caller's wait", seen[0].took)
	}
}
