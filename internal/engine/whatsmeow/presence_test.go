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
			if back := typingOf(state, media); back != tc.state {
				t.Errorf("%s came back as %s", tc.state, back)
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
