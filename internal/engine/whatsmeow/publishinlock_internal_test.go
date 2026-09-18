package whatsmeow

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/rs/zerolog"
	waEvents "go.mau.fi/whatsmeow/types/events"
)

// The invariant this PR measures the cost of, asserted as behaviour rather than as the
// shape of the source.
//
// Publishing from inside the transition lock is deliberate: it is what keeps
// `session.status` and the last frame on the stream from agreeing to disagree. #182 put a
// histogram on how long that publish takes, and the tempting way to make that number
// smaller is to stop waiting for it -- which would improve the number and break the thing
// it measures.
//
// The order fence in keepalive_test.go reads the source, which is cheap and catches an
// inversion of intent. What it cannot catch is a publish that is still in the right place
// and no longer in the lock: `go s.emitDecided(...)` leaves both of its readings true, and
// so does a `go func() { ... }()`, a helper that starts the goroutine one level down, or
// the publish handed to the session executor. Enumerating those is guessing at the next
// one. This asserts the property instead: with the inbox full, the arm is still inside
// `emit` and the lock is not available.
//
// Built by hand rather than through newTestSession, which opens a database/sql pool and
// aborts the synctest bubble under PostgreSQL. `running > 0` makes takeDownSoon note the
// debt and return instead of starting a goroutine that calls ResetConnection on a nil
// client. The technique came from the holdout bench for this issue.
func TestTheKeepAliveArmHoldsTheTransitionLockAcrossThePublish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const inbox = 4
		s := &Session{
			inbox:   make(chan pending, inbox),
			done:    make(chan struct{}),
			log:     zerolog.Nop(),
			running: 1,
		}
		s.wallClock = time.Now
		s.setDialing(true)
		s.setConnected(true)

		// Full, so the publish inside the arm has nowhere to put its emission and blocks
		// there. This is the condition the metric exists to observe, and the only one in
		// which where the publish runs is observable at all.
		for range inbox {
			s.inbox <- pending{}
		}

		returned := make(chan struct{})
		go func() {
			defer close(returned)
			s.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: time.Now()})
		}()
		// Every other goroutine in the bubble is durably blocked once this returns, so the
		// arm has gone as far as it can go.
		synctest.Wait()

		select {
		case <-returned:
			t.Fatal("the keepalive arm returned instead of blocking on the publish, so the inbox was not full " +
				"and this test proved nothing: the condition it needs did not happen")
		default:
		}
		if s.transition.TryLock() {
			s.transition.Unlock()
			t.Fatal("the transition lock was free while the arm sat inside the publish, so the publish is not " +
				"happening under it: `session.status` and the last frame on the stream can disagree for as long " +
				"as the publish takes, and the arm that decides a socket is gone no longer waits behind it")
		}

		// Drain one, let the arm finish, and confirm the lock comes back. This is not
		// symmetry with the assertion above: it is the case in which that assertion would
		// go green over a session that is wedged. An arm that takes the lock and never
		// releases it satisfies "the lock is held across the publish" forever, and
		// `session.status` and the stream then agree for the rest of the session's life
		// because nothing else can move -- which is a worse way for this invariant to be
		// broken than the one the first assertion catches, and the only one it cannot see.
		<-s.inbox
		synctest.Wait()
		<-returned
		if !s.transition.TryLock() {
			t.Fatal("the transition lock was still held after the arm returned")
		}
		s.transition.Unlock()
	})
}
