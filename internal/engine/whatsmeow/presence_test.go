package whatsmeow

import (
	"encoding/json"
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
// emit waits for room in the inbox, and waiting is the thing presence must not do: a
// backlog holds WhatsApp's node handler for as long as the publisher is down and then
// delivers a `composing` about a minute that has passed.
//
// The inbox is filled first, so what is measured is the full state rather than a race
// for it.
func TestTypingIsDroppedRatherThanQueuedBehindABacklog(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	// Filled until the channel itself refuses, rather than by counting to inboxDepth:
	// the pump takes one out and blocks handing it on, so a count leaves a slot free and
	// the blocking path would not block either.
	filled := 0
	for {
		select {
		case session.inbox <- engine.Emission{Type: protocol.EventSessionState, Payload: []byte(`{}`)}:
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
		t.Fatal("a typing indicator waited for room in a full inbox, holding WhatsApp's node handler")
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

// A stop is what clears a typing indicator, and there is no event after a stop. Dropped
// behind the typing it stops, it leaves a client showing somebody typing with nothing
// coming to correct it -- so the two states do not take the same path: the stop is
// offered whenever there is any room at all, and the typing only while the publisher is
// keeping up.
func TestAStopIsOfferedWhereTheTypingItStopsIsNot(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.picked = make(chan struct{}, 1)
	// Two fillers, because the pump takes one out and blocks handing it on. The second
	// is what makes the inbox non-empty -- the publisher not keeping up -- and it also
	// stops the pump competing for what is queued after it.
	blockThePump(t, session, 2)

	source := waTypes.MessageSource{
		Chat:   waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
		Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
	}
	session.chatPresence(&waEvents.ChatPresence{MessageSource: source, State: waTypes.ChatPresenceComposing})
	session.chatPresence(&waEvents.ChatPresence{MessageSource: source, State: waTypes.ChatPresencePaused})

	states := []string{}
	for _, emission := range queuedIn(session) {
		if emission.Type != protocol.EventChatPresence {
			continue
		}
		var body struct {
			State string `json:"state"`
		}
		if err := json.Unmarshal(emission.Payload, &body); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		states = append(states, body.State)
	}
	if len(states) != 1 || states[0] != "paused" {
		t.Errorf("what was queued behind a publisher already behind is %v, and only the stop should have been", states)
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
	// One filler, so the pump is holding it and not competing for what follows. The
	// inbox itself is left empty, which is what the typing needs to be accepted at all.
	blockThePump(t, session, 1)

	source := waTypes.MessageSource{
		Chat:   waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
		Sender: waTypes.NewJID("5511999990002", waTypes.DefaultUserServer),
	}
	session.chatPresence(&waEvents.ChatPresence{MessageSource: source, State: waTypes.ChatPresenceComposing})

	queued := queuedIn(session)
	if len(queued) != 1 {
		t.Fatalf("%d events were queued, want the typing", len(queued))
	}
	typing := queued[0]
	if typing.Expires == nil {
		t.Fatal("a typing indicator was queued with no sense of when it stops being true")
	}
	if typing.Expires() <= 0 {
		t.Error("a typing indicator was stale the moment it was queued")
	}
	now = presenceLife
	if typing.Expires() > 0 {
		t.Error("a typing indicator is still worth publishing after its whole life")
	}

	// And a stop never goes stale: it states something that stays true.
	session.chatPresence(&waEvents.ChatPresence{MessageSource: source, State: waTypes.ChatPresencePaused})
	stops := queuedIn(session)
	if len(stops) != 1 {
		t.Fatalf("%d events were queued, want the stop", len(stops))
	}
	if stops[0].Expires != nil {
		t.Error("a stop was queued as something that expires")
	}
}

// blockThePump parks the forwarding goroutine on an emission nobody is reading, so what
// is queued after this stays in the inbox for the test to look at rather than being
// carried off mid-assertion.
//
// It waits on the pump saying it took one rather than on a clock saying it probably has:
// a poll that gives up after a fixed spell fails on a loaded machine over a session that
// is behaving perfectly.
func blockThePump(t *testing.T, session *Session, fillers int) {
	t.Helper()

	for range fillers {
		select {
		case session.inbox <- engine.Emission{Type: protocol.EventSessionState, Payload: []byte(`{}`)}:
		case <-time.After(2 * time.Second):
			t.Fatal("the inbox would not take a filler")
		}
	}
	select {
	case <-session.picked:
	case <-time.After(5 * time.Second):
		t.Fatal("the forwarder never took anything off the inbox")
	}
}

func queuedIn(session *Session) []engine.Emission {
	queued := []engine.Emission{}
	for {
		select {
		case emission := <-session.inbox:
			queued = append(queued, emission)
			continue
		default:
		}
		return queued
	}
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

// Somebody being online is a fact that holds until they go away, not a moment.
// Classified with the typing it perishes, and then an `unavailable` already queued
// outlives the newer `available` that was dropped for the same backlog: a client left
// showing somebody offline who is not, with nothing coming to correct it.
func TestAnAvailabilityIsAFactRatherThanAMoment(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.picked = make(chan struct{}, 1)
	blockThePump(t, session, 2)

	from := waTypes.NewJID("5511999990002", waTypes.DefaultUserServer)
	session.presence(&waEvents.Presence{From: from, Unavailable: true})
	session.presence(&waEvents.Presence{From: from})

	states := []string{}
	for _, emission := range queuedIn(session) {
		if emission.Type != protocol.EventPresenceUpdate {
			continue
		}
		if emission.Expires != nil {
			t.Error("an availability was queued as something that expires")
		}
		var body struct {
			State string `json:"state"`
		}
		if err := json.Unmarshal(emission.Payload, &body); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		states = append(states, body.State)
	}
	// Both, in order, so the last one is what the client ends up with. Either dropped
	// and the client's last word is the wrong one.
	if len(states) != 2 || states[0] != "unavailable" || states[1] != "available" {
		t.Errorf("what reached the queue is %v, want the going away and the coming back", states)
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
