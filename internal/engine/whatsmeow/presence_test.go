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

			session, _ := newTestSession(t, "5511999990001")
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

			session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

// The value a marker stands for is read when its turn comes and not when it was posted,
// which is what lets one queue slot carry a whole burst of typing. A marker that carried
// the value would publish the state the chat was in when the queue was joined, and behind
// a backlog that is a `composing` about a minute that has passed.
func TestAMarkerPublishesTheStateTheChatIsInWhenItsTurnComes(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.picked = make(chan struct{}, 1)
	blockTheForwarder(t, session)

	jid := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	queued := len(session.inbox)
	for _, state := range []waTypes.ChatPresence{
		waTypes.ChatPresenceComposing, waTypes.ChatPresenceComposing, waTypes.ChatPresencePaused,
	} {
		session.chatPresence(&waEvents.ChatPresence{
			MessageSource: waTypes.MessageSource{Chat: jid, Sender: jid}, State: state,
		})
	}
	if took := len(session.inbox) - queued; took != 1 {
		t.Errorf("a burst of typing in one chat took %d places in the queue, want the one", took)
	}

	next(t, session)
	published := next(t, session)
	if state := stateOf(t, published); state != "paused" {
		t.Errorf("what came out is a %s, and the chat's state when its turn came was the stop", state)
	}
}

// A stop is the end of a typing burst and there is nothing after it. Lost to a publisher
// having a bad second it is not sent again by WhatsApp and not superseded by anything, so
// the client keeps showing somebody typing until that person does something else -- which
// may be never.
func TestAStopGoesBackOnTheBoardWhenItsPublishFails(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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

	session, _ := newTestSession(t, "5511999990001")
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
