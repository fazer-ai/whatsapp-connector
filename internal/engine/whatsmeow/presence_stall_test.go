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

func typingIn(phone string, state waTypes.ChatPresence) *waEvents.ChatPresence {
	jid := waTypes.NewJID(phone, waTypes.DefaultUserServer)
	return &waEvents.ChatPresence{MessageSource: waTypes.MessageSource{Chat: jid, Sender: jid}, State: state}
}

// presencesAfterTheStall reads what the session publishes once the publisher comes back,
// settling each one, until the inbox the stall filled has drained and want presences have
// come, and then for a moment more, so that one too many shows up as one too many. Waiting
// for the count, not for a silence: the markers waiting off the dispatch are enqueued by
// goroutines of their own, and a silence can end before they run.
func presencesAfterTheStall(t *testing.T, session *Session, want int) []*engine.Emission {
	t.Helper()
	var presences []*engine.Emission
	take := func(emission *engine.Emission) {
		if emission.Settle != nil {
			emission.Settle(nil)
		}
		if emission.Type == protocol.EventChatPresence || emission.Type == protocol.EventPresenceUpdate {
			presences = append(presences, emission)
		}
	}
	fillers := cap(session.inbox) + 1 // the stall's slots and the one the forwarder holds
	for fillers > 0 || len(presences) < want {
		emission := next(t, session)
		if emission.Type == protocol.EventSessionState {
			fillers--
		}
		take(emission)
	}
	for {
		select {
		case emission := <-session.Events():
			take(&emission)
			continue
		case <-time.After(100 * time.Millisecond):
		}
		return presences
	}
}

// A stop that arrives while the publisher is stalled and the inbox is full is published
// once there is room, after everything that was queued before it, and the dispatch that
// brought it is not held for it. Dropped, it left the client showing somebody typing
// with nothing coming to correct it (#47).
func TestAStopWithNoRoomIsPublishedOnceThereIsRoom(t *testing.T) {
	t.Parallel()
	session := newPresenceSession(t, "5511999990001")
	stall(t, session)

	returnsWithin(t, testwait.Budget, func() bool { return session.chatPresence(typingIn("5511999990002", waTypes.ChatPresencePaused)) })

	presences := presencesAfterTheStall(t, session, 1)
	if len(presences) != 1 || stateOf(t, presences[0]) != "paused" {
		t.Fatalf("published %d presences once the publisher came back, want the one stop", len(presences))
	}
}

// The same for somebody going away: an availability has nothing after it either.
func TestAnUnavailableWithNoRoomIsPublishedOnceThereIsRoom(t *testing.T) {
	t.Parallel()
	session := newPresenceSession(t, "5511999990001")
	stall(t, session)

	from := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	returnsWithin(t, testwait.Budget, func() bool { return session.presence(&waEvents.Presence{From: from, Unavailable: true}) })

	presences := presencesAfterTheStall(t, session, 1)
	if len(presences) != 1 || stateOf(t, presences[0]) != "unavailable" {
		t.Fatalf("published %d presences once the publisher came back, want the one going away", len(presences))
	}
}

// A burst of typing through a stall costs the client one event, the last state, and the
// dispatch nothing: the typing that does not fit is a moment and is dropped, and every
// stop after the first takes the place the first is already waiting for.
func TestABurstOfTypingThroughAStallPublishesOnlyTheLastStop(t *testing.T) {
	t.Parallel()
	session := newPresenceSession(t, "5511999990001")
	stall(t, session)

	returnsWithin(t, testwait.Budget, func() bool {
		for i := range 200 {
			state := waTypes.ChatPresenceComposing
			if i%2 == 1 {
				state = waTypes.ChatPresencePaused
			}
			session.chatPresence(typingIn("5511999990002", state))
		}
		return true
	})
	if waiting := len(session.ends); waiting > 1 {
		t.Errorf("%d stops are waiting off the dispatch for one chat, want one place", waiting)
	}

	presences := presencesAfterTheStall(t, session, 1)
	if held := boardSize(session); held != 0 {
		t.Errorf("%d presences are still on the board after the stop was published and settled", held)
	}
	if len(presences) != 1 || stateOf(t, presences[0]) != "paused" {
		states := make([]string, len(presences))
		for i, p := range presences {
			states[i] = stateOf(t, p)
		}
		t.Fatalf("published %v once the publisher came back, want the one last stop", states)
	}
}

// Typing that does not fit is still dropped and leaves nothing behind: a moment published
// late is a lie about now.
func TestTypingWithNoRoomLeavesNothingBehind(t *testing.T) {
	t.Parallel()
	session := newPresenceSession(t, "5511999990001")
	stall(t, session)

	returnsWithin(t, testwait.Budget, func() bool { return session.chatPresence(typingIn("5511999990002", waTypes.ChatPresenceComposing)) })

	if held := len(onBoard(session)); held != 0 {
		t.Errorf("%d presences are on the board for a typing indicator with no room", held)
	}
	if presences := presencesAfterTheStall(t, session, 0); len(presences) != 0 {
		t.Fatalf("published %d presences for a typing indicator that never had room", len(presences))
	}
}

func boardSize(session *Session) int {
	session.boardMu.Lock()
	defer session.boardMu.Unlock()
	return len(session.board)
}

// The place a chat holds through one stall is given up once its marker is resolved: a
// stop in the next stall waits for a place of its own and is published, rather than
// taking the place of a marker that has already gone out.
func TestAStopInTheNextStallIsNotLostToThePreviousOnesPlace(t *testing.T) {
	t.Parallel()
	session := newPresenceSession(t, "5511999990001")
	for round := range 2 {
		stall(t, session)
		returnsWithin(t, testwait.Budget, func() bool { return session.chatPresence(typingIn("5511999990002", waTypes.ChatPresencePaused)) })
		if presences := presencesAfterTheStall(t, session, 1); len(presences) != 1 {
			t.Fatalf("stall %d published %d presences once the publisher came back, want its stop", round+1, len(presences))
		}
	}
}

// Stops from more chats than owedEnds through one stall keep owedEnds of them and count
// the rest as dropped: what is bounded is the goroutines waiting on a publisher that may
// not come back.
func TestStopsFromMoreChatsThanTheBoundKeepTheBound(t *testing.T) {
	t.Parallel()
	session := newPresenceSession(t, "5511999990001")
	watch := &emitWatch{}
	session.queueing = watch
	stall(t, session)

	const chats = owedEnds + 20
	returnsWithin(t, testwait.Budget, func() bool {
		for i := range chats {
			session.chatPresence(typingIn(fmt.Sprintf("55119%08d", i), waTypes.ChatPresencePaused))
		}
		return true
	})
	if dropped := len(watch.dropped()); dropped != chats-owedEnds {
		t.Errorf("the instrument heard of %d drops, want %d", dropped, chats-owedEnds)
	}
	if presences := presencesAfterTheStall(t, session, owedEnds); len(presences) != owedEnds {
		t.Fatalf("published %d stops once the publisher came back, want %d", len(presences), owedEnds)
	}
}

// A state from the connection after a drop does not take the place of a marker from the
// connection before it. That marker may already be in the inbox with the drop behind it,
// and a client clears presence when it sees the drop: put in the old place, the new state
// is published first and then cleared, with nothing after it to put it back.
func TestAStateAfterADropDoesNotTakeAPlaceFromBeforeIt(t *testing.T) {
	t.Parallel()
	session := newPresenceSession(t, "5511999990001")
	stall(t, session)
	from := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)

	// The first availability waits off the dispatch, and goes in as soon as one slot frees.
	session.presence(&waEvents.Presence{From: from, Unavailable: true})
	next(t, session) // the forwarder hands on the filler it held and takes the next
	waitUntil(t, "the waiting marker to be in the inbox", func() bool { return len(session.ends) == 0 })

	// The drop, queued behind that marker, and the connection that replaces it.
	next(t, session)
	session.inbox <- pending{event: engine.Emission{Type: protocol.EventSessionStreamReplaced, Payload: []byte(`{}`)}}
	session.transitions.Add(1)

	// The same party, reported on the new connection, with the inbox full again.
	session.presence(&waEvents.Presence{From: from})

	sawDrop := false
	for {
		emission := next(t, session)
		if emission.Settle != nil {
			emission.Settle(nil)
		}
		switch emission.Type {
		case protocol.EventSessionStreamReplaced:
			sawDrop = true
		case protocol.EventPresenceUpdate:
			if !sawDrop {
				t.Fatalf("%s was published ahead of the drop it came after", stateOf(t, emission))
			}
			if state := stateOf(t, emission); state != "available" {
				t.Fatalf("published %s after the drop, want the new connection's available", state)
			}
			return
		}
	}
}
