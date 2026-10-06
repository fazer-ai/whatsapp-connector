package whatsmeow

import (
	"fmt"
	"testing"
	"time"

	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/testwait"
)

// stall stops the publisher with the inbox full: the forwarder holds one emission it
// cannot hand on, and every slot behind it is taken. That is the state #221 is about --
// the publisher has stopped answering -- and nothing reads Events() for the rest of the
// test unless the test starts a reader itself.
func stall(t *testing.T, session *Session) {
	t.Helper()
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)
	for len(session.inbox) < cap(session.inbox) {
		session.inbox <- pending{event: engine.Emission{Type: protocol.EventSessionState, Payload: []byte(`{}`)}}
	}
}

// returnsWithin runs one handler on its own goroutine, the way whatsmeow's dispatch does,
// and reports what it answered, or fails when it is still holding the dispatch after
// within.
func returnsWithin(t *testing.T, within time.Duration, handle func() bool) bool {
	t.Helper()
	done := make(chan bool, 1)
	go func() { done <- handle() }()
	select {
	case answered := <-done:
		return answered
	case <-time.After(within):
		t.Fatalf("a handler still holds whatsmeow's dispatch after %v with the publisher stalled", within)
		return false
	}
}

// Three calls ringing while the publisher is stalled are all refused. The refusal is
// written off the dispatch, but the call behind a handler parked on the inbox is never
// handled at all, so before this only the first was.
func TestEveryCallRingingDuringAStallIsRefused(t *testing.T) {
	t.Parallel()
	session, watched := callSession(t, true)
	stall(t, session)

	returnsWithin(t, 10*time.Second, func() bool {
		for _, id := range []string{"call-1", "call-2", "call-3"} {
			session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta(id)})
		}
		return true
	})
	deadline := time.Now().Add(testwait.Budget)
	for len(watched.seen()) < 3 && time.Now().Before(deadline) {
		time.Sleep(testwait.Poll)
	}
	if seen := watched.seen(); len(seen) != 3 {
		t.Fatalf("refused %v during the stall, want all three", seen)
	}
}

// An offer that does not fit is dropped, not waited for, and the instrument hears of it: a
// call that rang is worth nothing once the publisher is back, and the dispatch it would
// have held is every other event the account has. Its end is not dropped with it -- the
// contract publishes an end the client may have learned of some other way -- but waits for
// room off the dispatch.
func TestACallOfferThatDoesNotFitIsDroppedAndCounted(t *testing.T) {
	t.Parallel()
	session, _ := callSession(t, false)
	watch := &emitWatch{}
	session.queueing = watch
	stall(t, session)

	returnsWithin(t, testwait.Budget, func() bool {
		session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta("call-1")})
		session.handle(&waEvents.CallTerminate{BasicCallMeta: callMeta("call-1"), Reason: "timeout"})
		return true
	})
	if dropped := watch.dropped(); len(dropped) != 1 || dropped[0] != protocol.EventCallOffer {
		t.Fatalf("the instrument heard of %v, want the offer alone", dropped)
	}
}

// publishedAfterTheStall reads what the session publishes once the publisher comes back,
// until it has seen want of eventType, and returns the call events in the order they came.
func publishedAfterTheStall(t *testing.T, session *Session, eventType protocol.EventType, want int) []protocol.EventType {
	t.Helper()
	var calls []protocol.EventType
	seen := 0
	deadline := time.After(testwait.Budget)
	for seen < want {
		select {
		case emission := <-session.Events():
			if emission.Settle != nil {
				emission.Settle(nil)
			}
			if emission.Type == protocol.EventCallOffer || emission.Type == protocol.EventCallTerminate {
				calls = append(calls, emission.Type)
			}
			if emission.Type == eventType {
				seen++
			}
		case <-deadline:
			t.Fatalf("saw %d %s once the publisher came back, want %d (calls: %v)", seen, eventType, want, calls)
		}
	}
	return calls
}

// An offer that took the last slot before the stall is not left without its end. The end
// does not fit, does not hold the dispatch, and is published behind its offer once the
// publisher is back, so the client does not keep showing a call that is over.
func TestTheEndOfAQueuedCallIsPublishedAfterTheStall(t *testing.T) {
	t.Parallel()
	session, _ := callSession(t, false)
	watch := &emitWatch{}
	session.queueing = watch
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)
	for len(session.inbox) < cap(session.inbox)-1 {
		session.inbox <- pending{event: engine.Emission{Type: protocol.EventSessionState, Payload: []byte(`{}`)}}
	}

	returnsWithin(t, testwait.Budget, func() bool {
		session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta("call-1")})
		session.handle(&waEvents.CallTerminate{BasicCallMeta: callMeta("call-1"), Reason: "timeout"})
		return true
	})
	if dropped := watch.dropped(); len(dropped) != 0 {
		t.Fatalf("the instrument heard of %v dropped, want nothing: the offer fit and its end waited", dropped)
	}
	calls := publishedAfterTheStall(t, session, protocol.EventCallTerminate, 1)
	if len(calls) != 2 || calls[0] != protocol.EventCallOffer || calls[1] != protocol.EventCallTerminate {
		t.Fatalf("published %v once the publisher came back, want the offer and then its end", calls)
	}
}

// The ends waiting for room are bounded: a flood of them into a stalled publisher returns
// at once, keeps owedEnds of them for when it comes back, and counts the rest as dropped.
func TestAFloodOfCallEndsDuringAStallKeepsABoundedFew(t *testing.T) {
	t.Parallel()
	session, _ := callSession(t, false)
	watch := &emitWatch{}
	session.queueing = watch
	stall(t, session)

	const ends = 2000
	returnsWithin(t, testwait.Budget, func() bool {
		for i := range ends {
			session.handle(&waEvents.CallTerminate{BasicCallMeta: callMeta(fmt.Sprintf("call-%d", i)), Reason: "timeout"})
		}
		return true
	})
	if dropped := len(watch.dropped()); dropped != ends-owedEnds {
		t.Errorf("the instrument heard of %d drops, want %d", dropped, ends-owedEnds)
	}
	publishedAfterTheStall(t, session, protocol.EventCallTerminate, owedEnds)
}

// A group notification that cannot be queued in time is not acknowledged, so WhatsApp sends
// it again, and the dispatch is held only as long as a message would hold it. Waiting for
// room without a bound held it for as long as the publisher was stalled.
func TestAGroupFactThatCannotBeQueuedIsNotAcknowledged(t *testing.T) {
	t.Parallel()
	session := groupSession(t)
	session.deliverWait = 50 * time.Millisecond
	stall(t, session)

	changed := returnsWithin(t, testwait.Budget, func() bool {
		return session.handle(&waEvents.GroupInfo{
			JID: groupJID(), Name: &waTypes.GroupName{Name: "renamed"},
		})
	})
	if changed {
		t.Error("a group change that was never queued was acknowledged, and nothing will send it again")
	}
	joined := returnsWithin(t, testwait.Budget, func() bool {
		return session.handle(&waEvents.JoinedGroup{GroupInfo: waTypes.GroupInfo{JID: groupJID()}})
	})
	if joined {
		t.Error("a joined group that was never queued was acknowledged, and nothing will send it again")
	}
}

// The bound is a bound and not a refusal: a group change that finds room before it runs out
// is queued, acknowledged, and published.
func TestAGroupFactThatFindsRoomInTimeIsAcknowledged(t *testing.T) {
	t.Parallel()
	session := groupSession(t)
	session.deliverWait = testwait.Budget
	stall(t, session)

	changed := make(chan bool, 1)
	go func() {
		changed <- session.handle(&waEvents.GroupInfo{JID: groupJID(), Name: &waTypes.GroupName{Name: "renamed"}})
	}()
	// The publisher comes back while the change is waiting for room.
	seen := map[protocol.EventType]int{}
	deadline := time.After(testwait.Budget)
	for seen[protocol.EventGroupUpdated] == 0 {
		select {
		case emission := <-session.Events():
			seen[emission.Type]++
			if emission.Settle != nil {
				emission.Settle(nil)
			}
		case <-deadline:
			t.Fatalf("the change was never published once the publisher came back: %v", seen)
		}
	}
	if !<-changed {
		t.Error("a change that was queued and published was not acknowledged")
	}
}

// A stall of any length costs a constant: the moments that do not fit are dropped as they
// come, so twenty thousand calls ringing into a stalled publisher leave the inbox exactly
// as full as it was, and every one of them is counted.
func TestAFloodOfMomentsDuringAStallKeepsNothing(t *testing.T) {
	t.Parallel()
	session, _ := callSession(t, false)
	watch := &emitWatch{}
	session.queueing = watch
	stall(t, session)

	const calls = 20000
	returnsWithin(t, testwait.Budget, func() bool {
		for i := range calls {
			session.handle(&waEvents.CallOffer{BasicCallMeta: callMeta(fmt.Sprintf("call-%d", i))})
		}
		return true
	})
	if queued := len(session.inbox); queued != cap(session.inbox) {
		t.Errorf("the inbox holds %d after the flood, want it as full as the stall left it", queued)
	}
	if dropped := len(watch.dropped()); dropped != calls {
		t.Errorf("the instrument heard of %d drops, want %d", dropped, calls)
	}
}
