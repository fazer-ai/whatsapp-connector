package whatsmeow

import (
	"encoding/json"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// redeliveryProbeEvery is the default redeliveryProbe.
//
// What it costs when it is short is a restated state on the stream every so often during
// an outage, each of which fails; what it costs when it is long is that much more delay on
// a message WhatsApp is already holding. Two seconds is well inside the lease, so an outage
// short enough to leave the session up is answered within a few seconds of it ending.
const redeliveryProbeEvery = 2 * time.Second

// reasonRedelivery is the state reason for a socket this session took down so that WhatsApp
// sends again what was left unacknowledged while the stream was failing.
const reasonRedelivery = "redelivery"

// oweRedelivery records that an acknowledgement was withheld because the event never
// reached the stream, and starts waiting for the stream to come back if nothing is yet.
//
// WhatsApp sends an unacknowledged message again only to a new connection (#354), and a
// failure short enough to leave the lease in place leaves the socket up as well: the
// message then sits at WhatsApp until the connection next drops for some other reason,
// which on a healthy one can be hours. So the session makes that drop itself, once the
// stream takes a write again -- earlier, the redelivered message would only be withheld a
// second time.
//
// Recorded against the socket the message arrived on, read before the publish was waited
// on: a failure that settles after the socket was replaced is about one whose redelivery
// the replacement already was. A session that never came up has nothing to take down.
func (s *Session) oweRedelivery(on int64) {
	s.mu.Lock()
	if on == 0 || s.sockets.Load() != on {
		s.mu.Unlock()
		return
	}
	s.redeliveryOn = on
	start := !s.redelivering
	s.redelivering = true
	s.mu.Unlock()
	if start {
		go s.redeliverOnceTheStreamIsBack()
	}
}

// redeliverOnceTheStreamIsBack asks the stream, every redeliveryProbe, whether it takes a
// write, and takes the socket down the first time it does. It ends with the session.
func (s *Session) redeliverOnceTheStreamIsBack() {
	wait := time.NewTimer(s.redeliveryProbe)
	defer wait.Stop()
	for {
		select {
		case <-wait.C:
		case <-s.ctx.Done():
			return
		}
		if s.redeliverIfTheStreamIsBack() {
			return
		}
		wait.Reset(s.redeliveryProbe)
	}
}

// redeliverIfTheStreamIsBack is one attempt, and reports whether there is nothing left to
// wait for.
func (s *Session) redeliverIfTheStreamIsBack() bool {
	settled, owed := s.probe()
	if !owed {
		return true
	}
	if settled == nil {
		return false
	}
	select {
	case err := <-settled:
		if err != nil {
			s.log.Debug().Err(err).Msg("the stream still does not take a write; the withheld acknowledgements wait")
			return false
		}
	case <-s.ctx.Done():
		return true
	}
	return s.redeliver()
}

// probe restates the session's state with a callback on the outcome, and that outcome is
// the question: an event that lands says the stream takes writes again.
//
// The state is what the session is, so a client reading it learns nothing false; and it is
// what a client already handles at any moment. Rendered and queued under the transition
// lock, because a state read here and queued behind a newer one would put an old state
// last on the stream. Never waited into a full inbox, which already answers the question.
//
// A nil channel with owed true is "not now": the inbox is full, or the socket is not
// answering for the moment -- a keepalive that gave up on it may yet hear it again, and it
// is still the one the stanzas are on. Owed false is a debt a new socket has already
// answered, or a session that is finished.
func (s *Session) probe() (settled chan error, owed bool) {
	s.transition.Lock()
	defer s.transition.Unlock()

	if !s.stillOwed() {
		return nil, false
	}
	payload := s.sessionState()
	switch payload["state"] {
	case "open":
	case "close":
		s.forgetRedelivery()
		return nil, false
	default:
		return nil, true
	}
	body, err := json.Marshal(payload)
	if err != nil {
		s.log.Error().Err(err).Msg("failed to render the state to ask the stream with")
		return nil, true
	}
	answer := make(chan error, 1)
	depth := len(s.inbox)
	select {
	case s.inbox <- pending{event: engine.Emission{
		Type: protocol.EventSessionState, Payload: body, At: s.learned(),
		Settle: func(err error) { answer <- err },
	}}:
		s.queued(0, depth)
		return answer, true
	default:
		return nil, true
	}
}

// redeliver takes the socket down so WhatsApp sends again what is unacknowledged, the way
// the keepalive handler takes down a mute one: the state first, the takedown started off
// this goroutine, and the drop it causes marked as already published.
func (s *Session) redeliver() bool {
	s.transition.Lock()
	defer s.transition.Unlock()

	if !s.stillOwed() {
		return true
	}
	if s.state() != "open" {
		// Went quiet between the question and the answer. Asked again on the next round.
		return false
	}
	s.forgetRedelivery()
	// The answer just landed, so the publisher is answering again, and a receipt arriving
	// on the new socket must be tried rather than refused for a stall this already outlived:
	// refused, it would be withheld with no publish behind it, and nothing would owe it the
	// next redelivery.
	s.stalledUntil.Store(0)
	s.log.Warn().Msg("the stream takes writes again; taking the socket down so WhatsApp redelivers what was left unacknowledged")
	client := s.current()
	judged := s.setConnected(false)
	s.mu.Lock()
	s.redeliveryTakedown = judged
	s.mu.Unlock()
	s.setReconnecting(true, s.now())
	s.announceDrop()
	s.takeDownSoon(client, judged)
	s.emit(protocol.EventSessionState, map[string]any{"state": "reconnecting", "reason": reasonRedelivery})
	return true
}

// stillOwed reports whether the socket an acknowledgement was withheld on is the one the
// session is on, and forgets the debt when it is not: a new socket is the redelivery.
func (s *Session) stillOwed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.redeliveryOn == s.sockets.Load() {
		return true
	}
	s.redeliveryOn, s.redelivering = 0, false
	return false
}

// forgetRedelivery ends the wait, so the next withheld acknowledgement starts a new one.
func (s *Session) forgetRedelivery() {
	s.mu.Lock()
	s.redeliveryOn, s.redelivering = 0, false
	s.mu.Unlock()
}
