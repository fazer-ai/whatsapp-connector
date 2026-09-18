package whatsmeow

import (
	"testing"
	"time"

	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// The instant that travels to the publish side is the dispatch, and the whole value of the
// measurement rides on which of the two it is.
//
// The arm takes the transition lock before it judges anything, and a lock already held
// across a publish waiting on a full inbox holds this arm behind it for as long as that
// takes. #182's first paragraph is about exactly that wait -- "every arm waiting on that
// lock waits behind it, including the one that decides a socket is gone" -- so an instant
// read after the lock came free reports the publish and omits the queueing that made it
// late, which is the reading that does not answer the question.
//
// Driven rather than measured, the way the dating test beside it is: two `time.Now()` calls
// in a row cannot produce a gap the real thing takes minutes over. The first reading is the
// dispatch and everything after it is the handling, so `Decided` coming back as the second
// reading is the delivery having read the clock too late.
//
// It also fences reusing `Emission.At`, which is measured to be read inside `emitting`,
// after the lock: `At` would come back as the later reading here and the test would say so.
func TestTheKeepAliveStateCarriesTheDispatchInstantToThePublishSide(t *testing.T) {
	t.Parallel()

	session, _ := newLoggedTestSession(t, "5511999990002")
	dialedAndConnected(session)
	drain(t, session)

	dispatched := time.Now()
	var readings int
	session.wallClock = func() time.Time {
		readings++
		if readings == 1 {
			return dispatched
		}
		return dispatched.Add(time.Minute)
	}

	session.handle(&waEvents.KeepAliveTimeout{ErrorCount: 2, LastSuccess: dispatched})

	var state *emissionSeen
	for {
		select {
		case emission, open := <-session.Events():
			if !open {
				t.Fatal("the session closed without publishing a state")
			}
			if emission.Type != protocol.EventSessionState {
				continue
			}
			state = &emissionSeen{decided: emission.Decided, at: emission.At}
		case <-time.After(time.Second):
			if state == nil {
				t.Fatal("no session.state came out of the keepalive arm")
			}
		}
		if state != nil {
			break
		}
	}

	if state.decided.IsZero() {
		t.Fatal("the state left the keepalive arm with no decision instant, so nothing downstream can measure how long the client waited to be told")
	}
	if !state.decided.Equal(dispatched) {
		t.Errorf("the decision is stamped %s, when the handler got round to it, rather than %s, when the event was dispatched: "+
			"the wait for the transition lock falls outside the measurement, and that wait is what #182 is about",
			state.decided.Format(time.TimeOnly), dispatched.Format(time.TimeOnly))
	}
	// The monotonic reading has to survive the trip, and this is the only place that can
	// say so. `Sub` uses the monotonic readings when both ends have one and falls back to
	// the wall clock when either does not -- silently, with no error and no second return
	// value. So a `.UTC()` added to a log line, a `.Truncate()` added to round a number for
	// a message, or this field being serialised one day would leave the measurement working
	// and wrong, and wrong by exactly whatever the clock was adjusted by during the window.
	// A metric whose reason to exist is the long tail is a metric whose windows are long
	// enough for an NTP step to fit inside one.
	//
	// `Round(0)` is the documented way to strip the reading, and `==` on time.Time compares
	// it, so a value that still has one cannot equal its own stripped copy. `Equal` would
	// not do: it compares instants and ignores exactly what is under test here.
	//nolint:staticcheck // QF1009 suggests Equal, which is the one comparison that cannot answer this: it compares instants and ignores the monotonic reading under test
	if state.decided == state.decided.Round(0) {
		t.Error("the decision instant reached the publish side with no monotonic reading, so the distance is measured against the wall clock: " +
			"a clock adjustment inside the window it exists to show would be added to the measurement, and nothing would report that it had been")
	}
	if state.at == dispatched.UnixMilli() {
		t.Error("`At` and the decision instant are now the same reading, which means one of them stopped meaning what it says: " +
			"`At` is the engine's reading of when the fact happened and crosses the wire as `ts`, and it is read inside `emitting`, after the lock")
	}
}

type emissionSeen struct {
	decided time.Time
	at      int64
}
