package whatsmeow

import (
	"context"
	"errors"

	wm "go.mau.fi/whatsmeow"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
)

// errUnanswered is a write that went out on a connection that dropped before WhatsApp
// answered it. Whether WhatsApp applied it is what nobody here can say, which is the
// contract's `timeout`, and it was not written again (#180).
var errUnanswered = errors.New("the connection went before WhatsApp answered the write")

// onThisConnection runs a group write so that it reaches WhatsApp on one connection at
// most.
//
// whatsmeow answers an IQ the socket dropped under by waiting up to five seconds for a new
// connection and sending the identical frame again (`retryFrame`), and WhatsApp does not
// deduplicate an IQ across connections: measured on the real service, a `group.create` cut
// that way made two groups, and every other group write goes through the same resend. The
// library has a way to say no (`NoRetry`), but none of its group helpers takes it, and
// rebuilding each of them by hand against `DangerousInternals` is a copy of the library to
// keep in step forever. What the resend does honour is the context: the frame goes out
// through `NoiseSocket.SendFrame`, which refuses a context that is done. So the write runs
// under a context this session cancels the moment it learns the connection is gone, which
// is `dropped`, the first thing the `Disconnected` handler does -- well before a reconnect,
// which is a dial and two round trips to WhatsApp away.
//
// A write the drop caught answers errUnanswered, marked as one that may have landed when
// the seam marked it, so that a redelivery under the same key is answered rather than
// carried out. The caller's own deadline is left to answer for itself.
func (s *Session) onThisConnection(ctx context.Context, write func(context.Context) error) error {
	s.lineMu.Lock()
	line := s.line
	s.lineMu.Unlock()

	bound, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-line:
			cancel()
		case <-bound.Done():
		}
	}()

	err := write(bound)
	if err == nil || ctx.Err() != nil {
		return err
	}
	var disconnected *wm.DisconnectedError
	cut := bound.Err() != nil && errors.Is(err, context.Canceled)
	if !cut && !errors.As(err, &disconnected) {
		return err
	}
	s.log.Debug().Err(err).Msg("a group write lost its connection before WhatsApp answered it")
	if errors.Is(err, engine.ErrMayHaveLanded) {
		return engine.MayHaveLanded(errUnanswered)
	}
	return errUnanswered
}

// cutLine ends the connection the writes in flight went out on.
func (s *Session) cutLine() {
	s.lineMu.Lock()
	defer s.lineMu.Unlock()
	close(s.line)
	s.line = make(chan struct{})
}
