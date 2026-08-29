package whatsmeow

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// WhatsApp has no `recording`. It has `composing` with a media attribute beside it, and
// audio is what makes the other phone say recording instead of typing. Read as two
// states rather than two-times-two, every voice note anybody starts is published as
// typing, and the client shows the wrong verb for the whole of it.
func TestTypingAndRecordingAreTheSameStateWithDifferentMedia(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		state waTypes.ChatPresence
		media waTypes.ChatPresenceMedia
		want  string
	}{
		{"typing", waTypes.ChatPresenceComposing, waTypes.ChatPresenceMediaText, "composing"},
		{"recording", waTypes.ChatPresenceComposing, waTypes.ChatPresenceMediaAudio, "recording"},
		{"stopped", waTypes.ChatPresencePaused, waTypes.ChatPresenceMediaText, "paused"},
		// A pause carries no media, and reading one where there is none must not turn
		// the stop into a recording.
		{"stopped recording", waTypes.ChatPresencePaused, waTypes.ChatPresenceMediaAudio, "paused"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session := newPresenceSession(t, "5511999990001")
			event := &waEvents.ChatPresence{
				MessageSource: waTypes.MessageSource{
					Chat:   waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
					Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
				},
				State: tc.state, Media: tc.media,
			}

			payload := decode(t, presencePublishedBy(t, session, protocol.EventChatPresence,
				func() bool { return session.chatPresence(event) }).Payload)
			if payload["state"] != tc.want {
				t.Errorf("%s/%q was published as %v, want %q", tc.state, tc.media, payload["state"], tc.want)
			}
		})
	}
}

// The round trip, which is the half a client drives. The same three names have to survive
// going out, and the recording is again the one that cannot be read as a state alone.
func TestAChatPresenceGoesOutAsTheStateAndTheMediaTogether(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		state protocol.TypingState
		want  waTypes.ChatPresence
		media waTypes.ChatPresenceMedia
	}{
		{"typing", protocol.TypingComposing, waTypes.ChatPresenceComposing, waTypes.ChatPresenceMediaText},
		{"recording", protocol.TypingRecording, waTypes.ChatPresenceComposing, waTypes.ChatPresenceMediaAudio},
		{"stopped", protocol.TypingPaused, waTypes.ChatPresencePaused, waTypes.ChatPresenceMediaText},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			state, media := composingOf(tc.state)
			if state != tc.want || media != tc.media {
				t.Errorf("%s goes out as %q/%q, want %q/%q", tc.state, state, media, tc.want, tc.media)
			}
			// And back, because the two directions have to agree on the same three names.
			if back, named := typingOf(state, media); !named || back != tc.state {
				t.Errorf("%s came back as %s (named=%v)", tc.state, back, named)
			}
		})
	}
}

// Hiding the last seen and having been last seen at the epoch are different facts, and a
// zero would publish the second. The contract makes the field nullable for that.
func TestAPresenceSaysNothingAboutALastSeenThatIsHidden(t *testing.T) {
	t.Parallel()

	seen := time.UnixMilli(1755440000123)
	for _, tc := range []struct {
		name      string
		event     *waEvents.Presence
		wantState string
		wantSeen  any
	}{
		{"online", &waEvents.Presence{
			From: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
		}, "available", nil},
		{"away, last seen at a time", &waEvents.Presence{
			From:        waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
			Unavailable: true, LastSeen: seen,
		}, "unavailable", float64(1755440000123)},
		{"away, last seen hidden", &waEvents.Presence{
			From:        waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
			Unavailable: true,
		}, "unavailable", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session := newPresenceSession(t, "5511999990001")
			payload := decode(t, presencePublishedBy(t, session, protocol.EventPresenceUpdate,
				func() bool { return session.presence(tc.event) }).Payload)

			if payload["state"] != tc.wantState {
				t.Errorf("the presence reads %v, want %q", payload["state"], tc.wantState)
			}
			if payload["last_seen"] != tc.wantSeen {
				t.Errorf("the last seen reads %v, want %v", payload["last_seen"], tc.wantSeen)
			}
			party, _ := payload["party"].(map[string]any)
			if party["phone"] != "5511999990002" {
				t.Errorf("the presence is about %v", payload["party"])
			}
		})
	}
}

// The decision the whole file turns on, and the one place on this path that does not
// withhold. A typing indicator describes a moment, and a moment redelivered is a lie:
// somebody who stopped a minute ago would be shown typing, and the pause that would have
// corrected it was published while the stale one was being retried.
func TestAPresenceNobodyPublishedIsStillAcknowledged(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.deliverWait = 20 * time.Millisecond

	// Nothing is reading the events, so nothing is published. The answer to WhatsApp is
	// the same either way.
	if !session.presence(&waEvents.Presence{
		From: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
	}) {
		t.Error("a presence was left for WhatsApp to send again")
	}
	// Both of them, so the inbox is holding what the pump could not hand on, which is
	// exactly the state a publisher having a bad second puts this in.
	if !session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{
			Chat:   waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
			Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
		},
		State: waTypes.ChatPresenceComposing,
	}) {
		t.Error("a chat presence was left for WhatsApp to send again")
	}
}

// Presence is a person being at their phone. A group has none, and WhatsApp answers a
// subscription to one with nothing at all -- so a client that asked would wait forever
// for an event that was never coming, with a success in hand.
func TestOnlyAPersonHasAPresenceToSubscribeTo(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		party   string
		refused bool
	}{
		{"a group", `{"kind":"group","id":"120363041234567890"}`, true},
		{"a channel", `{"kind":"newsletter","id":"120363041234567890@newsletter"}`, true},
		{"the status feed", `{"kind":"status","id":"status"}`, true},
		{"a number", `{"kind":"phone","id":"5511999990002"}`, false},
		{"a lid", `{"kind":"lid","id":"167392323834034"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _, _ := outboundSession(t)
			_, err := session.subscribePresence(t.Context(), &protocol.Command{
				Type:    protocol.CommandPresenceSubscribe,
				Payload: json.RawMessage(`{"party":` + tc.party + `}`),
			})
			if tc.refused {
				assertCode(t, err, protocol.ErrorInvalidPayload)
				return
			}
			// Not refused over who it names: it gets as far as the socket it does not
			// have, which is a different answer from being told its payload is wrong.
			assertCode(t, err, protocol.ErrorNotConnected)
		})
	}
}

// The states a client may ask for, and nothing else. A third name accepted here would go
// out as a node WhatsApp does not understand, and the send would report success.
func TestAPresenceCommandTakesOnlyTheStatesTheContractHas(t *testing.T) {
	t.Parallel()

	session, _, _ := outboundSession(t)
	for _, payload := range []string{`{}`, `{"state":"online"}`, `{"state":""}`, `[]`} {
		_, err := session.setPresence(t.Context(), &protocol.Command{
			Type: protocol.CommandPresenceSet, Payload: json.RawMessage(payload),
		})
		assertCode(t, err, protocol.ErrorInvalidPayload)
	}
	for _, payload := range []string{`{}`, `{"chat":{"kind":"phone","id":"5511999990002"},"state":"typing"}`} {
		_, err := session.chatPresenceCommand(t.Context(), &protocol.Command{
			Type: protocol.CommandChatPresence, Payload: json.RawMessage(payload),
		})
		assertCode(t, err, protocol.ErrorInvalidPayload)
	}
}

// The same boundary the message and receipt paths draw: a client that asked for direct
// chats only never got the group's messages, so somebody typing in one is an event it
// has nowhere to put.
func TestAGroupsTypingIsNotPublishedToADirectOnlyClient(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.deliverWait = 20 * time.Millisecond
	event := &waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{
			Chat:    waTypes.NewJID("120363041234567890", waTypes.GroupServer),
			Sender:  waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
			IsGroup: true,
		},
		State: waTypes.ChatPresenceComposing,
	}

	if !session.chatPresence(event) {
		t.Fatal("a group's typing was left for WhatsApp to send again")
	}
	// A window rather than a poll: what is published goes to the inbox and is carried to
	// the events channel by the pump, so reading straight after the handler returns
	// would find nothing whether or not anything was published.
	select {
	case emission := <-session.Events():
		t.Fatalf("a group's typing reached a direct-only client: %s", emission.Payload)
	case <-time.After(500 * time.Millisecond):
	}
}

func presencePublishedBy(t *testing.T, session *Session, want protocol.EventType, handle func() bool) engine.Emission {
	t.Helper()

	acknowledged := make(chan bool, 1)
	go func() { acknowledged <- handle() }()
	emission := next(t, session)
	if emission.Type != want {
		t.Fatalf("a presence was published as %s", emission.Type)
	}
	select {
	case got := <-acknowledged:
		if !got {
			t.Fatal("a presence was left for WhatsApp to send again")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never came back")
	}
	return emission
}

// The decision one layer down, and the one the rest of this file would be undone by.
// The inbox waits for room, and waiting is the thing presence must not do: a full one
// holds WhatsApp's node handler for as long as the publisher is down and then delivers a
// `composing` about a minute that has passed.
//
// The inbox is filled to refusal first, so what is read is the full state rather than a
// race for it.
func TestTypingDoesNotWaitOnAnInboxThatIsFull(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	// Filled until the channel itself refuses, rather than by counting to inboxDepth:
	// the forwarder takes one out and blocks handing it on, so a count would leave a slot
	// free and a path that waits for room would not have to wait.
	filled := 0
	for {
		select {
		case session.inbox <- pending{event: engine.Emission{Type: protocol.EventSessionState, Payload: []byte(`{}`)}}:
			filled++
			continue
		default:
		}
		break
	}
	if filled == 0 {
		t.Fatal("the inbox took nothing at all")
	}

	typing := &waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{
			Chat:   waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
			Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
		},
		State: waTypes.ChatPresenceComposing,
	}

	handled := make(chan bool, 1)
	go func() { handled <- session.chatPresence(typing) }()
	select {
	case got := <-handled:
		if !got {
			t.Error("a typing indicator was left for WhatsApp to send again")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a typing indicator waited on a full inbox, holding WhatsApp's node handler")
	}
}

// whatsmeow logs a chat state it does not recognise and dispatches the event anyway, so
// anything WhatsApp adds arrives here as itself. Read as "not paused, so typing" it
// becomes activity nobody reported -- and the contract has no placeholder for a typing
// indicator the way it has one for a body, so there is nothing honest to publish.
func TestATypingStateThisBuildHasNoNameForIsNotRoundedToTheNearestOne(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	event := &waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{
			Chat:   waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
			Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
		},
		State: waTypes.ChatPresence("sketching"),
	}

	if !session.chatPresence(event) {
		t.Fatal("a state nobody can render was left for WhatsApp to send again")
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("a state WhatsApp invented was published as one of ours: %s", emission.Payload)
	case <-time.After(500 * time.Millisecond):
	}
}

// A stop is what clears a typing indicator, and there is no event after a stop. Queued
// behind the typing it stops it is what a full inbox drops, and the client is left
// showing somebody typing with nothing coming to correct it. On the board it replaces
// that typing instead, keyed by the chat, so what is waiting is always the last thing
// the chat did.
func TestAStopReplacesTheTypingItStops(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)

	source := waTypes.MessageSource{
		Chat:   waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
		Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
	}
	session.chatPresence(&waEvents.ChatPresence{MessageSource: source, State: waTypes.ChatPresenceComposing})
	session.chatPresence(&waEvents.ChatPresence{MessageSource: source, State: waTypes.ChatPresencePaused})

	waiting := onBoard(session)
	if len(waiting) != 1 {
		t.Fatalf("%d presences are waiting for one chat, and only its last state should be", len(waiting))
	}
	if state := stateOf(t, waiting[0]); state != "paused" {
		t.Errorf("what is waiting is a %s, and the chat's last state was the stop", state)
	}
	if waiting[0].Expires != nil {
		t.Error("a stop was posted as something that expires")
	}
}

// Two chats are two states, not one: the board is keyed by chat, so a busy conversation
// does not evict a quiet one's stop.
func TestOneChatsTypingDoesNotDisplaceAnothers(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)

	for _, phone := range []string{"5511999990002", "5511999990003"} {
		jid := waTypes.NewJID(phone, waTypes.DefaultUserServer)
		session.chatPresence(&waEvents.ChatPresence{
			MessageSource: waTypes.MessageSource{Chat: jid, Sender: jid},
			State:         waTypes.ChatPresenceComposing,
		})
	}
	if waiting := onBoard(session); len(waiting) != 2 {
		t.Errorf("%d presences are waiting, and two chats were typing", len(waiting))
	}
}

// The other half of the same rule: a moment that waited too long is thrown away instead
// of published, because the session already knows more than the event does.
func TestAMomentThatWentStaleIsNotPublishedLate(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	now := time.Duration(0)
	session.elapsed = func() time.Duration { return now }
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)

	source := waTypes.MessageSource{
		Chat:   waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
		Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
	}
	session.chatPresence(&waEvents.ChatPresence{MessageSource: source, State: waTypes.ChatPresenceComposing})

	waiting := onBoard(session)
	if len(waiting) != 1 {
		t.Fatalf("%d presences are waiting, want the typing", len(waiting))
	}
	typing := waiting[0]
	if typing.Expires == nil {
		t.Fatal("a typing indicator was posted with no sense of when it stops being true")
	}
	if typing.Expires() <= 0 {
		t.Error("a typing indicator was stale the moment it was posted")
	}
	now = presenceLife
	if typing.Expires() > 0 {
		t.Error("a typing indicator is still worth publishing after its whole life")
	}
}

// newPresenceSession is a session on a live connection, which is the only state presence
// arrives in. A node reported by a socket that is already down is refused before it
// reaches the board, so a test that does not say the session is up would be exercising
// that refusal rather than whatever it means to.
func newPresenceSession(t *testing.T, phone string) *Session {
	t.Helper()

	session, _ := newTestSession(t, phone)
	session.setConnected(true)
	return session
}

// blockTheForwarder parks the forwarding goroutine on an emission nobody is reading, so
// what is posted after this stays where the test can look at it rather than being
// carried off mid-assertion.
//
// It waits on the forwarder saying it took one rather than on a clock saying it probably
// has: a poll that gives up after a fixed spell fails on a loaded machine over a session
// that is behaving perfectly.
func blockTheForwarder(t *testing.T, session *Session) {
	t.Helper()

	select {
	case session.inbox <- pending{event: engine.Emission{Type: protocol.EventSessionState, Payload: []byte(`{}`)}}:
	case <-time.After(2 * time.Second):
		t.Fatal("the inbox would not take a filler")
	}
	select {
	case <-session.picked:
	case <-time.After(5 * time.Second):
		t.Fatal("the forwarder never took anything to hand on")
	}
}

// onBoard is what the board is still holding for a turn that has not come, which is what
// the tests below mean by waiting. An entry that has been handed to the publisher stays
// on the board until it settles, and that one is not waiting for anything.
func onBoard(session *Session) []engine.Emission {
	session.boardMu.Lock()
	defer session.boardMu.Unlock()
	waiting := make([]engine.Emission, 0, len(session.board))
	for _, entry := range session.board {
		if entry.sent {
			continue
		}
		waiting = append(waiting, entry.emission)
	}
	return waiting
}

func stateOf(t *testing.T, emission engine.Emission) string {
	t.Helper()

	var body struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(emission.Payload, &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return body.State
}

// The freshness check is only as good as the wait after it. An unbounded handoff passes
// the check and then sits on a reader that is busy, and what comes out the other side is
// exactly the stale event the check exists to stop.
func TestAMomentIsNotHeldWaitingForAReaderThatIsBusy(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.handoffWait = 50 * time.Millisecond
	session.picked = make(chan struct{}, 1)

	source := waTypes.MessageSource{
		Chat:   waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
		Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
	}
	session.chatPresence(&waEvents.ChatPresence{MessageSource: source, State: waTypes.ChatPresenceComposing})
	select {
	case <-session.picked:
	case <-time.After(5 * time.Second):
		t.Fatal("the forwarder never took the typing off the inbox")
	}

	// Nobody is reading, and the moment is given up on rather than held. A stop behind
	// it then reaches the reader that finally shows up, which is the whole point: a
	// perishable event must not be what a durable one is queued behind.
	session.chatPresence(&waEvents.ChatPresence{MessageSource: source, State: waTypes.ChatPresencePaused})

	// Waiting on the pump taking the stop rather than on a clock: it can only reach the
	// stop after it has given up on the typing, so this is the give-up itself saying so.
	select {
	case <-session.picked:
	case <-time.After(5 * time.Second):
		t.Fatal("the forwarder never gave up on the typing")
	}

	emission := next(t, session)
	if emission.Type != protocol.EventChatPresence {
		t.Fatalf("what came through is %s", emission.Type)
	}
	var body struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(emission.Payload, &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body.State != "paused" {
		t.Errorf("a %s was held on a reader that was not there, and the stop waited behind it", body.State)
	}
}

// Somebody being online is a fact that holds until they go away, not a moment. Neither
// state expires, and the newer one replaces the older on the board rather than queueing
// behind it: an `unavailable` that outlived the `available` after it leaves a client
// showing somebody offline who is not, with nothing coming to correct it.
func TestAnAvailabilityIsAFactAndTheLastOneWins(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)

	from := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	session.presence(&waEvents.Presence{From: from, Unavailable: true})
	session.presence(&waEvents.Presence{From: from})

	waiting := onBoard(session)
	if len(waiting) != 1 {
		t.Fatalf("%d presences are waiting for one party, and only its last state should be", len(waiting))
	}
	if waiting[0].Expires != nil {
		t.Error("an availability was posted as something that expires")
	}
	if state := stateOf(t, waiting[0]); state != "available" {
		t.Errorf("what is waiting says %s, and the party's last state was the coming back", state)
	}
}

// A typing indicator has nowhere to show in a channel, a broadcast list or the status
// feed, and SendChatPresence reports only that the node was written -- so the command
// comes back successful and nothing anywhere is typing.
func TestATypingIndicatorIsRefusedWhereItHasNowhereToShow(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		chat    string
		refused bool
	}{
		{"a channel", `{"kind":"newsletter","id":"120363041234567890@newsletter"}`, true},
		{"a broadcast list", `{"kind":"broadcast","id":"5511999990001"}`, true},
		{"the status feed", `{"kind":"status","id":"status"}`, true},
		{"a direct chat", `{"kind":"phone","id":"5511999990002"}`, false},
		{"a group", `{"kind":"group","id":"120363041234567890"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			session, _, _ := outboundSession(t)
			_, err := session.chatPresenceCommand(t.Context(), &protocol.Command{
				Type:    protocol.CommandChatPresence,
				Payload: json.RawMessage(`{"chat":` + tc.chat + `,"state":"composing"}`),
			})
			if tc.refused {
				assertCode(t, err, protocol.ErrorInvalidPayload)
				return
			}
			assertCode(t, err, protocol.ErrorNotConnected)
		})
	}
}

// In a group the state belongs to a participant, not to the chat. Keyed by the chat
// alone, Bob starting to type replaces Alice's stop -- and if Alice's typing had already
// been published, the client is left showing her typing for good.
func TestAGroupsTypingIsKeptPerPersonRatherThanPerChat(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.groups = true
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)

	group := waTypes.NewJID("120363041234567890", waTypes.GroupServer)
	alice := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	bob := waTypes.NewJID("5511999990003", waTypes.DefaultUserServer)

	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: group, Sender: alice, IsGroup: true},
		State:         waTypes.ChatPresencePaused,
	})
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: group, Sender: bob, IsGroup: true},
		State:         waTypes.ChatPresenceComposing,
	})

	waiting := onBoard(session)
	if len(waiting) != 2 {
		t.Fatalf("%d presences are waiting, and two people in the group did two different things", len(waiting))
	}
	states := map[string]bool{}
	for _, emission := range waiting {
		states[stateOf(t, emission)] = true
	}
	if !states["paused"] || !states["composing"] {
		t.Errorf("what is waiting is %v, want one person's stop and the other's typing", states)
	}
}

// A queue with no room left is a publisher that has already stopped answering, and there
// is nothing presence can do about that which is worth holding WhatsApp's node handler
// for. What it must not do is leave a mark behind: an entry with no marker to resolve it
// would sit on the board unpublished, and the next state for that chat would take it for
// one already on its way and quietly replace it instead of queueing one of its own.
func TestPresenceLeavesNothingBehindWhenTheInboxIsFull(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	filler := pending{event: engine.Emission{Type: protocol.EventSessionState, Payload: []byte(`{}`)}}
	// The forwarder is parked on the first one before the rest go in. Filling to refusal
	// instead would race it for the slot it frees on its way to parking, and a queue with
	// one slot left is a queue the presence below fits into.
	session.inbox <- filler
	waitUntil(t, "the forwarder to be holding an emission", func() bool { return len(session.inbox) == 0 })
	for range cap(session.inbox) {
		session.inbox <- filler
	}

	jid := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: jid, Sender: jid}, State: waTypes.ChatPresencePaused,
	})

	session.boardMu.Lock()
	held := len(session.board)
	session.boardMu.Unlock()
	if held != 0 {
		t.Errorf("%d presences are on a board with nothing coming to publish them", held)
	}
}

// One person is one key, whichever of WhatsApp's two namespaces addressed the event. A
// group switching addressing mode mid-burst is the ordinary way this happens: the
// `composing` arrives by LID and the `paused` after it by number. Keyed by whatever
// turned up they are two entries for one person -- nothing coalesces, and which state the
// client is left showing is decided by which of the two was published last.
func TestOnePersonIsOneKeyWhicheverAddressArrives(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.groups = true
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)

	chat := waTypes.NewJID("120363041234567890", waTypes.GroupServer)
	phone := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	lid := waTypes.NewJID("167392323834034", waTypes.HiddenUserServer)
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: chat, Sender: lid, SenderAlt: phone, IsGroup: true},
		State:         waTypes.ChatPresenceComposing,
	})
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: chat, Sender: phone, SenderAlt: lid, IsGroup: true},
		State:         waTypes.ChatPresencePaused,
	})

	waiting := onBoard(session)
	if len(waiting) != 1 {
		t.Fatalf("%d presences are waiting for one person, and only their last state should be", len(waiting))
	}
	if state := stateOf(t, waiting[0]); state != "paused" {
		t.Errorf("what is waiting is a %s, and the person's last state was the stop", state)
	}
}

// A group's typing belongs to a participant. With nobody to attribute it to there is no
// event worth publishing -- no client can render a group that is typing -- and every such
// event would share one key, so one unnameable participant's stop would clear another's
// typing.
func TestAGroupsTypingWithNobodyToAttributeItToIsNotPublished(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.groups = true

	if !session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{
			Chat: waTypes.NewJID("120363041234567890", waTypes.GroupServer), IsGroup: true,
		},
		State: waTypes.ChatPresenceComposing,
	}) {
		t.Fatal("a typing nobody can be shown as sending was left for WhatsApp to send again")
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("a group was published as typing, with nobody typing: %s", emission.Payload)
	case <-time.After(500 * time.Millisecond):
	}
}

// Presence keeps its place among the messages, which is the reason its order lives in the
// same queue as theirs rather than beside it. Published out of turn, a `composing` lands
// after the message that ended it and the client is left showing somebody typing with the
// message already on screen -- the same stuck indicator the board exists to prevent,
// arrived at from the other side.
func TestPresenceKeepsItsPlaceAmongTheMessages(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.picked = make(chan struct{}, 1)
	// Parked, so the typing and the message are both waiting when the forwarder starts
	// choosing. Posting them at a forwarder that is free would publish each as it
	// arrived and prove nothing about the choice.
	blockTheForwarder(t, session)

	jid := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: jid, Sender: jid}, State: waTypes.ChatPresenceComposing,
	})
	session.emit(protocol.EventSessionState, map[string]any{"state": "open"})

	// The filler the forwarder is parked on, and then the two in the order they happened.
	next(t, session)
	if first := next(t, session); first.Type != protocol.EventChatPresence {
		t.Fatalf("what came out first is %s, and the typing was posted before the message", first.Type)
	}
	if second := next(t, session); second.Type != protocol.EventSessionState {
		t.Fatalf("what came out second is %s", second.Type)
	}
}

// A chat that changes its mind while its states are queued publishes the last of them and
// none of the others. Each takes a place of its own, and every place but the newest
// resolves to nothing when its turn comes -- so what reaches the client is one event,
// where the state that survived actually happened, and not a replay of the burst.
func TestAChatThatChangesItsMindPublishesOnlyWhereItLanded(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)

	jid := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	for _, state := range []waTypes.ChatPresence{
		waTypes.ChatPresenceComposing, waTypes.ChatPresenceComposing, waTypes.ChatPresencePaused,
	} {
		session.chatPresence(&waEvents.ChatPresence{
			MessageSource: waTypes.MessageSource{Chat: jid, Sender: jid}, State: state,
		})
	}

	// The filler the forwarder is parked on, and then the one state the chat ended on.
	next(t, session)
	published := next(t, session)
	if state := stateOf(t, published); state != "paused" {
		t.Errorf("what came out is a %s, and the chat had already stopped by then", state)
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("a state the chat had moved on from was published as well: %s", emission.Payload)
	case <-time.After(500 * time.Millisecond):
	}
}

// A stop is the end of a typing burst and there is nothing after it. Lost to a publisher
// having a bad second it is not sent again by WhatsApp and not superseded by anything, so
// the client keeps showing somebody typing until that person does something else -- which
// may be never.
func TestAStopGoesBackOnTheBoardWhenItsPublishFails(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.picked = make(chan struct{}, 1)

	source := waTypes.MessageSource{
		Chat:   waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
		Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
	}
	session.chatPresence(&waEvents.ChatPresence{MessageSource: source, State: waTypes.ChatPresencePaused})

	emission := next(t, session)
	if emission.Settle == nil {
		t.Fatal("a stop was published with no way to hear that it never landed")
	}
	emission.Settle(errors.New("redis is unreachable"))

	waiting := onBoard(session)
	if len(waiting) != 1 {
		t.Fatalf("%d presences are on the board, and the stop that never landed should be", len(waiting))
	}
	if state := stateOf(t, waiting[0]); state != "paused" {
		t.Errorf("what went back is a %s", state)
	}
}

// And it does not go back over something newer. The state that is waiting was written
// after the one that failed, so putting the older one back would publish it last and
// leave the client with the state before the one it already has.
func TestAFailedStopDoesNotDisplaceTheStateAfterIt(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.picked = make(chan struct{}, 1)

	source := waTypes.MessageSource{
		Chat:   waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
		Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
	}
	session.chatPresence(&waEvents.ChatPresence{MessageSource: source, State: waTypes.ChatPresencePaused})
	emission := next(t, session)

	// They started typing again while the stop was still in flight.
	blockTheForwarder(t, session)
	session.chatPresence(&waEvents.ChatPresence{MessageSource: source, State: waTypes.ChatPresenceComposing})
	queued := len(session.inbox)
	emission.Settle(errors.New("redis is unreachable"))

	if len(session.inbox) != queued {
		t.Error("the failed stop took a second place in the queue for a chat that already had one")
	}
	waiting := onBoard(session)
	if len(waiting) != 1 {
		t.Fatalf("%d presences are on the board, want only the newer one", len(waiting))
	}
	if state := stateOf(t, waiting[0]); state != "composing" {
		t.Errorf("what is waiting is a %s, and the newer state was the typing", state)
	}
}

// Board membership cannot answer whether something newer exists: a newer state can be
// taken off the board and be waiting on the publisher while the older one's failure comes
// back. Put back then, the older state is published last and the client is left on it.
func TestAFailedStopDoesNotOvertakeAStateAlreadyOnItsWay(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.picked = make(chan struct{}, 1)

	source := waTypes.MessageSource{
		Chat:   waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
		Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
	}
	session.presence(&waEvents.Presence{From: source.Chat, Unavailable: true})
	away := next(t, session)

	// The newer state goes out while the older one is still publishing, so by the time
	// the failure comes back the board is empty and only the generation says otherwise.
	session.presence(&waEvents.Presence{From: source.Chat})
	back := next(t, session)
	if state := stateOf(t, back); state != "available" {
		t.Fatalf("what went out second is %s", state)
	}
	away.Settle(errors.New("redis is unreachable"))

	if waiting := onBoard(session); len(waiting) != 0 {
		t.Errorf("the going away was put back over the coming back after it: %v", stateOf(t, waiting[0]))
	}
}

// One more go, and not a loop. A publisher that is down stays down for longer than any
// number of immediate retries, and a state that put itself back every time would go round
// with the queue for as long as the outage lasted, taking a place in it from the messages
// each time round.
func TestAStopThatFailsTwiceIsNotTriedAThirdTime(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.picked = make(chan struct{}, 1)

	source := waTypes.MessageSource{
		Chat:   waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
		Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
	}
	session.chatPresence(&waEvents.ChatPresence{MessageSource: source, State: waTypes.ChatPresencePaused})
	first := next(t, session)

	// The pick that carried the stop, cleared so the park below waits on its own rather
	// than reading this one and returning before the forwarder is anywhere.
	select {
	case <-session.picked:
	default:
	}
	// Parked first, so the second go is still waiting when this looks for it.
	blockTheForwarder(t, session)
	first.Settle(errors.New("redis is unreachable"))

	// The filler the forwarder is parked on, and then the stop having its second go.
	next(t, session)
	second := next(t, session)
	if state := stateOf(t, second); state != "paused" {
		t.Fatalf("what was tried again is a %s", state)
	}
	if second.Settle == nil {
		t.Fatal("the second go was published with no way to hear that it never landed either")
	}
	second.Settle(errors.New("redis is still unreachable"))

	if waiting := onBoard(session); len(waiting) != 0 {
		t.Errorf("%d presences went round for a third go at a publisher that is down", len(waiting))
	}
}

// The place a marker holds belongs to the moment it was posted, and a later state may
// take it only while nothing has queued behind it. Once something has, resolving there
// would publish a state ahead of an event that happened before it -- and if that event is
// a session going down, a client that clears presence on it clears this too, with nothing
// after to put it back.
func TestAStateDoesNotTakeThePlaceOfAMarkerSomethingHasQueuedBehind(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)

	jid := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: jid, Sender: jid}, State: waTypes.ChatPresenceComposing,
	})
	session.emit(protocol.EventSessionState, map[string]any{"state": "close"})
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: jid, Sender: jid}, State: waTypes.ChatPresencePaused,
	})

	// The filler the forwarder is parked on. The typing is not published at all: it was
	// replaced on the board before its turn came, which is the board doing its job.
	next(t, session)
	if first := next(t, session); first.Type != protocol.EventSessionState {
		t.Fatalf("what came out first is %s, and the session went down before the stop happened", first.Type)
	}
	second := next(t, session)
	if second.Type != protocol.EventChatPresence {
		t.Fatalf("what came out second is %s", second.Type)
	}
	if state := stateOf(t, second); state != "paused" {
		t.Errorf("what came out after the session state is a %s", state)
	}
}

// A direct chat is a person, and WhatsApp addresses them by either of its two namespaces
// from one event to the next. Keyed by the address that arrived, the `composing` in the
// LID chat and the `paused` in the number chat are two entries: nothing coalesces, and
// the typing under whichever address the client saw first is left showing.
func TestADirectChatIsKeyedByThePersonAndNotTheAddressThatArrived(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)

	phone := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	lid := waTypes.NewJID("167392323834034", waTypes.HiddenUserServer)
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: lid, Sender: lid, SenderAlt: phone},
		State:         waTypes.ChatPresenceComposing,
	})
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: phone, Sender: phone, SenderAlt: lid},
		State:         waTypes.ChatPresencePaused,
	})

	waiting := onBoard(session)
	if len(waiting) != 1 {
		t.Fatalf("%d presences are waiting for one person, and only their last state should be", len(waiting))
	}
	if state := stateOf(t, waiting[0]); state != "paused" {
		t.Errorf("what is waiting is a %s, and the person's last state was the stop", state)
	}
}

// WhatsApp reports this account's own typing when it is done on another linked device,
// and in a direct chat it puts the account in the sender and the conversation in the
// chat. The contract has no way to say that: `sender` is nullable in a direct chat
// because the chat is the person, so a client is entitled to ignore it and would render
// this as the contact typing while the contact does nothing.
func TestThisAccountsOwnTypingIsNotPublishedInADirectChat(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	if !session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{
			Chat:     waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
			Sender:   waTypes.NewJID("5511999990001", waTypes.DefaultUserServer),
			IsFromMe: true,
		},
		State: waTypes.ChatPresenceComposing,
	}) {
		t.Fatal("this account's own typing was left for WhatsApp to send again")
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("a contact was published as typing, and it was this account: %s", emission.Payload)
	case <-time.After(500 * time.Millisecond):
	}
}

// And in a group it is published, because there `sender` is the whole information and
// this account is a participant like any other -- keyed by that participant, so it does
// not sit on anybody else's state.
func TestThisAccountsOwnTypingInAGroupIsKeptApartFromEveryoneElses(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.groups = true
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)

	chat := waTypes.NewJID("120363041234567890", waTypes.GroupServer)
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{
			Chat: chat, Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer), IsGroup: true,
		},
		State: waTypes.ChatPresenceComposing,
	})
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{
			Chat:    chat,
			Sender:  waTypes.NewJID("5511999990001", waTypes.DefaultUserServer),
			IsGroup: true, IsFromMe: true,
		},
		State: waTypes.ChatPresencePaused,
	})

	if waiting := onBoard(session); len(waiting) != 2 {
		t.Errorf("%d presences are waiting, and two participants were typing in that group", len(waiting))
	}
}

// A presence belongs to the socket that reported it. Once the connection has gone, the
// subscription that produced it is gone with it -- WhatsApp forgets those and whatsmeow
// replays none of them -- and the fact is warranted by nothing. Put back then, it lands
// on top of a client that cleared presence when it saw the session go, with nothing
// coming to correct it a second time.
func TestAPresenceIsNotTriedAgainOnceTheConnectionHasGone(t *testing.T) {
	t.Parallel()

	for _, dropped := range []struct {
		name  string
		event any
	}{
		// An ordinary drop, which publishes a session state, and a stream replaced,
		// which publishes an event of its own and no state at all. Counting the states
		// would cover the first and miss the second, and the second is the one where
		// nothing is coming back.
		{name: "disconnected", event: &waEvents.Disconnected{}},
		{name: "stream replaced", event: &waEvents.StreamReplaced{}},
	} {
		t.Run(dropped.name, func(t *testing.T) {
			t.Parallel()

			session := newPresenceSession(t, "5511999990001")
			session.picked = make(chan struct{}, 1)

			from := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
			session.presence(&waEvents.Presence{From: from})
			available := next(t, session)

			session.handle(dropped.event)
			next(t, session)
			available.Settle(errors.New("redis is unreachable"))

			if waiting := onBoard(session); len(waiting) != 0 {
				t.Errorf("an availability from before the connection went was put back: %s", stateOf(t, waiting[0]))
			}
		})
	}
}

// A number is a number on any of the servers that mean one, and `addressOf` is the only
// place that knows which those are. A second copy of that rule inside the key knew the
// ordinary server alone, so a hosted or legacy contact whose two addresses arrived in
// either order was two entries.
func TestAContactOffTheOrdinaryServerIsStillOneKey(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.groups = true
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)

	chat := waTypes.NewJID("120363041234567890", waTypes.GroupServer)
	legacy := waTypes.NewJID("5511999990002", waTypes.LegacyUserServer)
	lid := waTypes.NewJID("167392323834034", waTypes.HiddenUserServer)
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: chat, Sender: lid, SenderAlt: legacy, IsGroup: true},
		State:         waTypes.ChatPresenceComposing,
	})
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: chat, Sender: legacy, SenderAlt: lid, IsGroup: true},
		State:         waTypes.ChatPresencePaused,
	})

	waiting := onBoard(session)
	if len(waiting) != 1 {
		t.Fatalf("%d presences are waiting for one person, and only their last state should be", len(waiting))
	}
	if state := stateOf(t, waiting[0]); state != "paused" {
		t.Errorf("what is waiting is a %s, and the person's last state was the stop", state)
	}
}

// A presence is not published on the far side of the connection that produced it. The
// event saying the connection went is in the same queue, and a client that clears
// presence when it sees one would be left with this on top of the clearing, with nothing
// coming to correct it a second time. Dropped before that event instead, it costs
// nothing: that event would have cleared it anyway.
//
// It is also the answer to the two handlers racing. A presence node and a disconnect
// reach the session from different goroutines with no order between them, so nothing at
// the posting end decides which of the two queues first -- and this holds either way.
func TestAPresenceIsNotPublishedOnceItsConnectionHasGone(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)

	from := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	session.presence(&waEvents.Presence{From: from})
	// Down while the availability is still waiting its turn.
	session.handle(&waEvents.StreamReplaced{})

	// The filler the forwarder is parked on, and then the drop -- with nothing from
	// before it in between.
	next(t, session)
	if published := next(t, session); published.Type != protocol.EventSessionStreamReplaced {
		t.Fatalf("what came out is %s, and the connection it describes was already gone", published.Type)
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("a presence from a connection that had gone was published: %s", emission.Payload)
	case <-time.After(500 * time.Millisecond):
	}
	if waiting := onBoard(session); len(waiting) != 0 {
		t.Errorf("%d presences were left on the board with no marker coming for them", len(waiting))
	}
}

// whatsmeow runs the node handlers and the connection ones on separate goroutines, so a
// presence from the old socket can arrive after the disconnect has already been dealt
// with. No ordering the posting end could arrange helps there: what it describes has been
// over since before it got here, and published it leaves a contact typing or online with
// nothing coming to say otherwise.
func TestAPresenceReportedAfterTheConnectionWentIsRefused(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	session.handle(&waEvents.StreamReplaced{})
	next(t, session)

	from := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	if !session.presence(&waEvents.Presence{From: from}) {
		t.Fatal("a presence from a dead socket was left for WhatsApp to send again")
	}
	select {
	case emission := <-session.Events():
		t.Fatalf("a presence reported by a connection that was already down was published: %s", emission.Payload)
	case <-time.After(500 * time.Millisecond):
	}
}

// The board coalesces two addressings of one person only while both are still waiting.
// Once the first has gone out, what joins them is the address on the wire -- so a
// `composing` published in the LID chat and a `paused` published in the number chat are
// two chats to a client, and the stop clears nothing. Which of the two a client sees at
// all would otherwise depend on whether the events happened to coalesce, which is not
// something a client should be able to tell.
func TestADirectChatGoesOutUnderOneAddressWhicheverOneArrives(t *testing.T) {
	t.Parallel()

	session := newPresenceSession(t, "5511999990001")
	phone := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	lid := waTypes.NewJID("167392323834034", waTypes.HiddenUserServer)

	// Read as they are published, so the two never meet on the board.
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: lid, Sender: lid, SenderAlt: phone},
		State:         waTypes.ChatPresenceComposing,
	})
	typing := chatOfPresence(t, next(t, session))
	session.chatPresence(&waEvents.ChatPresence{
		MessageSource: waTypes.MessageSource{Chat: phone, Sender: phone, SenderAlt: lid},
		State:         waTypes.ChatPresencePaused,
	})
	stop := chatOfPresence(t, next(t, session))

	if typing != stop {
		t.Errorf("the typing went out for %v and the stop for %v, and one of them will never be cleared", typing, stop)
	}
	if want := (protocol.Address{Kind: protocol.AddressPhone, ID: "5511999990002"}); stop != want {
		t.Errorf("the chat went out as %v, and the number is the address WhatsApp offered for it", stop)
	}
}

func chatOfPresence(t *testing.T, emission engine.Emission) protocol.Address {
	t.Helper()

	var body struct {
		Chat protocol.Address `json:"chat"`
	}
	if err := json.Unmarshal(emission.Payload, &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return body.Chat
}
