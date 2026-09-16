package session

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
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
