package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// Presence is the one thing on this path that is not worth a redelivery, and that is the
// decision the whole file turns on.
//
// Everywhere else here an acknowledgement is withheld when nothing was published, because
// WhatsApp sending it again is how a message or a tick survives a publisher having a bad
// second. A typing indicator is the opposite: it describes a moment, and a moment
// redelivered is a lie. Somebody who stopped typing a minute ago would be shown typing,
// and the state that would have corrected it -- the pause, or the message itself -- was
// already published while the stale one was being retried.
//
// So these publish and acknowledge, whatever happens. What is lost when the publisher is
// down is a second of somebody's typing, and it is replaced by the next event.
func (s *Session) chatPresence(event *waEvents.ChatPresence) bool {
	chat, addressable := addressOf(event.Chat)
	if !addressable {
		return true
	}
	if chat.Kind == protocol.AddressGroup && !s.wantsGroups() {
		return true
	}
	published := protocol.ChatPresence{Chat: chat, State: typingOf(event.State, event.Media)}
	sender := protocol.Party{}
	naming(&sender, event.Sender, event.SenderAlt)
	if sender.Phone != "" || sender.LID != "" {
		published.Sender = &sender
	}
	s.emit(protocol.EventChatPresence, published)
	return true
}

// presence is somebody coming online or going away, which only arrives for a party this
// session subscribed to. Published on the same terms as the typing above, and for the
// same reason.
func (s *Session) presence(event *waEvents.Presence) bool {
	party := &protocol.Party{}
	naming(party, event.From)
	if party.Phone == "" && party.LID == "" {
		return true
	}
	published := protocol.PresenceUpdate{Party: *party, State: protocol.PresenceAvailable}
	if event.Unavailable {
		published.State = protocol.PresenceUnavailable
	}
	if !event.LastSeen.IsZero() {
		// Zero is somebody who hid it, which is not the same as somebody last seen at
		// the epoch. The contract makes the field nullable for exactly that.
		seen := event.LastSeen.UnixMilli()
		published.LastSeen = &seen
	}
	s.emit(protocol.EventPresenceUpdate, published)
	return true
}

// typingOf is the contract's three states out of whatsmeow's two.
//
// WhatsApp does not have a `recording`: it has `composing` with a media attribute beside
// it, and audio is what makes the bubble say recording instead of typing. Read as two
// states the recording is published as typing, and the client shows the wrong verb for
// every voice note anybody starts.
func typingOf(state waTypes.ChatPresence, media waTypes.ChatPresenceMedia) protocol.TypingState {
	if state == waTypes.ChatPresencePaused {
		return protocol.TypingPaused
	}
	if media == waTypes.ChatPresenceMediaAudio {
		return protocol.TypingRecording
	}
	return protocol.TypingComposing
}

// composingOf is the inverse, for a client saying what to show on the other phone.
func composingOf(state protocol.TypingState) (waTypes.ChatPresence, waTypes.ChatPresenceMedia) {
	switch state {
	case protocol.TypingPaused:
		return waTypes.ChatPresencePaused, waTypes.ChatPresenceMediaText
	case protocol.TypingRecording:
		return waTypes.ChatPresenceComposing, waTypes.ChatPresenceMediaAudio
	default:
		return waTypes.ChatPresenceComposing, waTypes.ChatPresenceMediaText
	}
}

type presenceRequest struct {
	State string `json:"state"`
}

// setPresence is `presence.set`: the account saying whether it is at the keyboard.
//
// It does more than show a green dot, and the more is the reason a client would send it
// at all. whatsmeow only sends real delivery receipts while the account is marked
// available: below that it answers every inbound message with `inactive`, which WhatsApp
// carries to the sender and no client renders. So an account that never sets this leaves
// everybody who writes to it looking at one grey tick forever.
func (s *Session) setPresence(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req presenceRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a presence has to say which state it is setting")
	}
	state, named := presenceStates[req.State]
	if !named {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a presence is either available or unavailable, and nothing else")
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}
	if err := s.current().SendPresence(ctx, state); err != nil {
		s.log.Warn().Err(err).Msg("a presence did not go out")
		return nil, presenceFailure(err)
	}
	return nil, nil
}

type subscribeRequest struct {
	Party protocol.Address `json:"party"`
}

// subscribePresence is `presence.subscribe`: WhatsApp sends nothing about anybody until
// it is asked, one party at a time.
//
// Which is why there is no policy here about how many. The contract puts that decision
// on the client, and a connector deciding it for them would either subscribe to an
// address book nobody is looking at or refuse the one contact somebody has open.
func (s *Session) subscribePresence(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req subscribeRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a subscription has to name whose presence it wants")
	}
	switch req.Party.Kind {
	case protocol.AddressPhone, protocol.AddressLID:
	default:
		// Presence is a person being at their phone. A group has none, and asking for
		// one is answered by WhatsApp with nothing at all.
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"presence belongs to a person, and that is not one")
	}
	party, err := jidOf(req.Party)
	if err != nil {
		return nil, err
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}
	if err := s.current().SubscribePresence(ctx, party); err != nil {
		s.log.Warn().Err(err).Msg("a presence subscription did not go out")
		return nil, presenceFailure(err)
	}
	return nil, nil
}

type chatPresenceRequest struct {
	Chat  protocol.Address `json:"chat"`
	State string           `json:"state"`
}

// chatPresenceCommand is `chat.presence`: what the other side sees above the chat.
func (s *Session) chatPresenceCommand(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req chatPresenceRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a chat presence has to say which chat it is in, and what to show")
	}
	state, named := typingStates[req.State]
	if !named {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a chat presence is composing, recording or paused, and nothing else")
	}
	chat, err := jidOf(req.Chat)
	if err != nil {
		return nil, err
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}
	typing, media := composingOf(state)
	if err := s.current().SendChatPresence(ctx, chat, typing, media); err != nil {
		s.log.Warn().Err(err).Str("chat", chat.String()).Msg("a chat presence did not go out")
		return nil, presenceFailure(err)
	}
	return nil, nil
}

var presenceStates = map[string]waTypes.Presence{
	"available":   waTypes.PresenceAvailable,
	"unavailable": waTypes.PresenceUnavailable,
}

var typingStates = map[string]protocol.TypingState{
	"composing": protocol.TypingComposing,
	"recording": protocol.TypingRecording,
	"paused":    protocol.TypingPaused,
}

// presenceFailure names what went wrong in the contract's own words, on the same terms
// as a read mark's: the library's text does not cross into a reply, and the code is what
// a caller branches on.
func presenceFailure(err error) error {
	if named, coded := commandFailure(err, "presence"); named {
		return coded
	}
	if errors.Is(err, wm.ErrNoPushName) {
		// The account is paired and WhatsApp still will not take a presence from it,
		// which is neither the payload's fault nor something a retry fixes soon.
		return protocol.NewError(protocol.ErrorWaError,
			"this account has no push name yet, and WhatsApp will not take a presence without one")
	}
	return protocol.NewError(protocol.ErrorWaError, "WhatsApp did not take the presence")
}
