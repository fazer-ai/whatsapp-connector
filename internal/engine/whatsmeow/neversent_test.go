package whatsmeow

import (
	"errors"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// The pre-flight refusal says two things at once, and only one of them is the client's.
//
// The client gets `not_connected` or `not_paired`, which send it down different roads. The
// layer that keeps the idempotency ledger gets `engine.ErrNeverSent`, which is what lets it
// take the attempt back off so the retry does the whole thing. Without the mark a session
// that is merely down would hold every command's key against a retry that should run, and
// a send refused this way would never go out under that id again (#282).
func TestARefusalMadeBeforeTheSocketSaysNothingWasSent(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		what  string
		setUp func(t *testing.T) *Session
		want  protocol.ErrorCode
	}{
		{
			what: "a session with no account",
			setUp: func(t *testing.T) *Session {
				session, _ := newTestSession(t, "")
				return session
			},
			want: protocol.ErrorNotPaired,
		},
		{
			what: "a session whose socket is not open",
			setUp: func(t *testing.T) *Session {
				session, _ := newTestSession(t, "5511999990001")
				session.setConnected(false)
				return session
			},
			want: protocol.ErrorNotConnected,
		},
	} {
		t.Run(tc.what, func(t *testing.T) {
			t.Parallel()

			err := tc.setUp(t).readyToSend()
			if err == nil {
				t.Fatal("the pre-flight let the command through")
			}
			if got := codeOf(err); got != tc.want {
				t.Errorf("the client is told %q, want %q", got, tc.want)
			}
			if !errors.Is(err, engine.ErrNeverSent) {
				t.Errorf("the refusal does not carry engine.ErrNeverSent, so the ledger cannot "+
					"tell it from a socket that died with the frame already written, and the "+
					"retry of %s would be refused for work that never happened", tc.what)
			}
		})
	}
}
