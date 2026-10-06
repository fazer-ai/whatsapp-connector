package whatsmeow

import (
	"context"
	"errors"
	"time"

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
// under a context this session cancels the moment it learns the connection is gone, at the
// top of the `Disconnected` handler -- well before a reconnect, which is a dial and two
// round trips to WhatsApp away.
//
// A write the drop caught answers errUnanswered, marked as one that may have landed when
// the seam marked it, so that a redelivery under the same key is answered rather than
// carried out. The caller's own deadline is left to answer for itself.
//
// One case is held that did not land, and nothing here can tell it apart. A frame still
// waiting for the socket's write lock when the drop is handled -- behind a write stuck on a
// peer that stopped draining, which is #74 -- is refused by the same context check, before
// it is written, and comes back as the same `context.Canceled` a refused resend does. It is
// answered `timeout` and held like the rest, so a redelivery under its key is answered
// rather than run. The other way round, not holding a cancelled write, would have every
// redelivery of the ordinary case -- the frame out, the answer lost -- write it again.
func (s *Session) onThisConnection(ctx context.Context, write func(context.Context) error) error {
	bound, cancel := context.WithCancel(ctx)
	defer cancel()
	running := &groupWrite{cancel: cancel}
	s.mu.Lock()
	s.inFlight[running] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.inFlight, running)
		s.mu.Unlock()
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

// groupWrite is one write `onThisConnection` is running, by what ends it.
type groupWrite struct {
	cancel context.CancelFunc
}

// cutLine ends the group writes in flight, for a drop dispatched at `at`, unless the
// replacement has already overtaken it.
//
// A drop the replacement has overtaken is late news about a socket whose writes were resent
// before it could have stopped them, and the writes in flight now went out on the
// replacement. The question and the cut are one step under mu, the lock `Connected` stamps
// connectedAt under: asked and acted on apart, a `Connected` handled between them would
// have the replacement's writes cancelled on a healthy socket.
//
// The cancels run here, inside the step, rather than on a goroutine of each write's that
// hears of the cut: when this returns, every write it ended has a done context, and the
// resend whatsmeow holds for the reconnect is refused however soon that comes.
//
// What remains is whatsmeow's own window: the replacement is up and taking writes before
// it dispatches `Connected` (#181), and a write in that window is in flight here. A drop
// handled then ends it with the rest, which answers it `timeout` on a socket that was fine.
func (s *Session) cutLine(at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.connectedAt.After(at) {
		return
	}
	for running := range s.inFlight {
		running.cancel()
		delete(s.inFlight, running)
	}
}
