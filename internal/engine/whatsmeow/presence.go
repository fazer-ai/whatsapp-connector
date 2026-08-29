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
// So these publish and acknowledge, whatever happens -- and never through the inbox,
// which waits for room. Waiting is the thing this must not do: a backlog would hold
// WhatsApp's node handler for as long as the publisher is down and then deliver a
// `composing` about a minute that has passed.
//
// They go on the board instead, keyed by the chat, so the newest state of a chat
// replaces whatever that chat had waiting. A queue is the wrong shape here: what matters
// is the last thing somebody did, and a FIFO keeps the first. Behind a backlog a queued
// `composing` outlives the `paused` there was no room for, and there is no event after a
// stop, so the client is left showing somebody typing with nothing coming.
//
// The `composing` and `recording` also carry how long they are worth publishing for,
// because they describe a moment; the `paused` does not, and neither does an
// availability, because both stay true until something says otherwise.
func (s *Session) chatPresence(event *waEvents.ChatPresence) bool {
	chat, addressable := addressOf(event.Chat)
	if !addressable {
		return true
	}
	if chat.Kind == protocol.AddressGroup && !s.wantsGroups() {
		return true
	}
	direct := chat.Kind == protocol.AddressPhone || chat.Kind == protocol.AddressLID
	if direct && event.IsFromMe {
		// This account typing on another of its own devices. In a direct chat the
		// contract reads a `chat.presence` as being about the other party -- `sender` is
		// documented as nullable there precisely because the chat is the person -- so a
		// client is entitled to ignore that field, and this would reach it as the contact
		// typing while the contact does nothing. There is no field that says otherwise,
		// and rounding it to the nearest thing the contract does have is what this file
		// refuses to do everywhere else. Registered as #48.
		//
		// A group is not the same case: there `sender` is the whole information, and this
		// account is a participant like any other.
		s.log.Debug().Str("chat", chat.ID).
			Msg("dropping this account's own typing, which a direct chat has no way to say")
		return true
	}
	state, named := typingOf(event.State, event.Media)
	if !named {
		// A state this build has no name for. Published as the nearest one it does have,
		// it is a claim WhatsApp never made -- and unlike a message, the contract has no
		// placeholder for a typing indicator, so there is nothing honest to put here.
		s.log.Debug().Str("state", string(event.State)).
			Msg("dropping a chat presence the contract does not name")
		return true
	}
	if canonical, ok := addressedBy(event.Chat, event.SenderAlt); direct && ok {
		// The chat as the person rather than as the address that arrived, so far as the
		// event says who that is. WhatsApp reaches a direct peer under either of its two
		// namespaces from one event to the next, and published as it came, a `composing`
		// in the LID chat and the `paused` in the number chat are two chats to a client:
		// the stop clears nothing, and which of the two the client sees at all would
		// depend on whether the two happened to coalesce on the board, which is not
		// something a client should be able to tell.
		//
		// This reaches as far as the event does and no further. A real `chatstate` for a
		// direct chat carries one namespace and no alternative -- every node in the live
		// run of 29/08/2026 arrived LID-only -- so where there is nothing to prefer, what
		// goes out is what came in. The mapping the session would need is in the device
		// store and nowhere near this function, and reading it here would put presence on
		// a different address from the receipts, which is one more shape for a client to
		// reconcile rather than one fewer. Registered as #50, across all the paths.
		chat = canonical
	}
	published := protocol.ChatPresence{Chat: chat, State: state}
	sender := protocol.Party{}
	naming(&sender, event.Sender, event.SenderAlt)
	if sender.Phone != "" || sender.LID != "" {
		published.Sender = &sender
	}
	if chat.Kind == protocol.AddressGroup && published.Sender == nil {
		// A group's typing belongs to a participant, and there is nobody to attribute
		// this one to. Published anyway it is a group that is typing, which no client can
		// render and no person is doing -- and every such event would share one key, so
		// one unnameable participant's stop would clear another's typing.
		s.log.Debug().Str("chat", chat.ID).
			Msg("dropping a group's typing there is nobody to attribute it to")
		return true
	}
	life := presenceLife
	if state == protocol.TypingPaused {
		life = 0
	}
	// Keyed by who is typing, and by where only where that adds anything. In a group the
	// state belongs to a participant: keyed by the chat alone, Bob starting to type
	// replaces Alice's stop and the client is left showing Alice typing for good.
	//
	// In a direct chat the chat is already the person by now, canonical either way it was
	// addressed, so it is the whole key: the account's own typing is gone from here, and
	// nobody else types in somebody's direct chat.
	key := string(protocol.EventChatPresence) + ":" + keyOf(chat)
	if !direct {
		who, _ := addressedBy(event.Sender, event.SenderAlt)
		key = string(protocol.EventChatPresence) + ":" + keyOf(chat) + ":" + keyOf(who)
	}
	s.post(key, protocol.EventChatPresence, published, life)
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
	// Neither state perishes. Somebody being online is a fact that holds until they go
	// away, not a moment -- classifying it with the typing was the mistake, and it had a
	// cost: an `unavailable` already queued while the newer `available` was dropped for
	// the same backlog leaves a client showing somebody offline who is not. On the board
	// the later one replaces the earlier, which is the same answer without the ordering.
	// WhatsApp sends these only for parties this session subscribed to, and it addresses
	// them the way they were subscribed, so one contact is one key here for as long as a
	// client asks about them the same way. There is no second address on the event to
	// canonicalise against the way the typing above has one.
	from, _ := addressedBy(event.From)
	s.post(string(protocol.EventPresenceUpdate)+":"+keyOf(from),
		protocol.EventPresenceUpdate, published, 0)
	return true
}

// addressedBy is the one address a person is known by here, which is their number
// wherever WhatsApp offered one.
//
// The same person reaches this under either of WhatsApp's two namespaces, and which one
// arrives is not the session's to choose. Taken as it came, a `composing` addressed by
// LID and the `paused` after it addressed by number are two people: two board entries, so
// nothing coalesces, and two chats on the wire, so the stop clears nothing. Where the
// event carries both -- and a chat presence does -- one of them is always the number, so
// preferring it makes the two events one.
//
// Which JIDs are numbers is `addressOf`'s answer and not a second copy of it, because
// there is more than one server that means "a number": the legacy one and the hosted one
// alongside the ordinary one, and a copy that knew only the ordinary one would take a
// hosted contact as whichever address happened to arrive first.
func addressedBy(jids ...waTypes.JID) (protocol.Address, bool) {
	var chosen protocol.Address
	for _, jid := range jids {
		switch address, ok := addressOf(jid); {
		case !ok:
		case chosen.ID == "", chosen.Kind != protocol.AddressPhone && address.Kind == protocol.AddressPhone:
			chosen = address
		}
	}
	return chosen, chosen.ID != ""
}

// keyOf is an address as one string, for a board key.
func keyOf(address protocol.Address) string { return string(address.Kind) + ":" + address.ID }

// typingOf is the contract's three states out of whatsmeow's two.
//
// WhatsApp does not have a `recording`: it has `composing` with a media attribute beside
// it, and audio is what makes the bubble say recording instead of typing. Read as two
// states the recording is published as typing, and the client shows the wrong verb for
// every voice note anybody starts.
// Unknown is refused rather than rounded to the nearest one, and reports so. whatsmeow
// logs a state it does not recognise and dispatches the event anyway, so anything
// WhatsApp adds arrives here as itself -- and read as "not paused, so typing" it becomes
// activity nobody reported.
func typingOf(state waTypes.ChatPresence, media waTypes.ChatPresenceMedia) (protocol.TypingState, bool) {
	switch state {
	case waTypes.ChatPresencePaused:
		return protocol.TypingPaused, true
	case waTypes.ChatPresenceComposing:
		if media == waTypes.ChatPresenceMediaAudio {
			return protocol.TypingRecording, true
		}
		return protocol.TypingComposing, true
	}
	return "", false
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
	switch req.Chat.Kind {
	case protocol.AddressPhone, protocol.AddressLID, protocol.AddressGroup:
	default:
		// A channel, a broadcast list and the status feed have nowhere to show one, and
		// SendChatPresence only reports that the node was written -- so the command comes
		// back successful and nothing anywhere is typing.
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a typing indicator has nowhere to show in that chat")
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
