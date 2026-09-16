package whatsmeow

import (
	"context"
	"encoding/json"
	"time"

	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// callWriteTimeout bounds the rejection nobody is waiting on. Short for the same reason
// as a presence write: nothing downstream depends on it, a call rings for seconds, and a
// refusal that arrives after the caller has given up is a node written into a call that
// no longer exists.
const callWriteTimeout = 10 * time.Second

// answeredCalls is how many call ids a session remembers to tell a second announcement of
// one call from a second call.
//
// WhatsApp announces a call twice at most and both halves arrive within the same ring, so
// the window this has to cover is one call long. The number is generous against that on
// purpose -- an account can be rung by several people at once -- and it is what bounds the
// memory: a session that runs for weeks keeps this many ids and not one per call it has
// ever been offered.
const answeredCalls = 64

// ring is a bounded set that forgets what it saw first.
//
// Its zero value works, which is what lets the session hold it by value: a set that had to
// be built in the constructor is one a test building a Session by hand would leave nil,
// and the handler would then panic on a write to a nil map rather than fail a test.
type ring struct {
	seen map[string]struct{}
	keys []string
	next int
}

// add records a key and reports whether it is new.
func (r *ring) add(key string) bool {
	if _, known := r.seen[key]; known {
		return false
	}
	if r.seen == nil {
		r.seen = make(map[string]struct{}, answeredCalls)
		r.keys = make([]string, answeredCalls)
	}
	// The slot about to be overwritten is the oldest, and its key leaves the set with it.
	// Advancing a slice header instead (`keys = keys[1:]`) would keep the backing array
	// growing for as long as the session ran, which is the leak this exists to avoid.
	if evicted := r.keys[r.next]; evicted != "" {
		delete(r.seen, evicted)
	}
	r.keys[r.next] = key
	r.next = (r.next + 1) % len(r.keys)
	r.seen[key] = struct{}{}
	return true
}

// callMedia is what an announcement said about the kind of call, and whether it said
// anything at all.
//
// The two halves WhatsApp sends do not carry the same thing: `offer_notice` names the
// media, `offer` does not. A plain bool would render "this is a voice call" and "this
// announcement did not say" identically, and the second is what a bare `offer` is.
type callMedia struct {
	known bool
	video bool
}

// callOffer is the contract's `call.offer`.
type callOffer struct {
	CallID    string         `json:"call_id"`
	From      protocol.Party `json:"from"`
	Video     bool           `json:"video"`
	Timestamp int64          `json:"timestamp,omitempty"`
}

// callTerminate is the contract's `call.terminate`.
type callTerminate struct {
	CallID string          `json:"call_id"`
	From   *protocol.Party `json:"from,omitempty"`
	Reason *string         `json:"reason"`
}

// rejectRequest is `call.reject`.
type rejectRequest struct {
	CallID string            `json:"call_id"`
	From   *protocol.Address `json:"from"`
}

// callOffered publishes a call this account is being rung for, and refuses it when the
// client asked for that.
//
// Acknowledged whatever happens, which is the presence rule rather than the message rule:
// WhatsApp does not redeliver a call offer, so withholding the acknowledgement buys no
// second chance and leaves a node unacknowledged for a call that has already ended.
func (s *Session) callOffered(meta *waTypes.BasicCallMeta, media callMedia) bool {
	// One call, one of everything. WhatsApp sends `offer` and `offer_notice` for the same
	// call, in either order, and both reach here.
	//
	// The one that says whether it is video is `offer_notice`, so the first to arrive is
	// the one published and a bare `offer` publishes `video: false`. That is the right
	// way round: a voice call is what the overwhelming majority of them are, and the
	// alternative -- holding the offer back until the notice arrives -- delays every
	// call for one that may never come.
	first := s.firstSightOf(meta.CallID)

	// Before anything that can wait, and before the subscription is consulted, because
	// the two questions are different ones.
	//
	// The subscription decides what a client is told about; the policy decides whether
	// the account rings. A group call refused only when the client also wanted group
	// conversation is an account ringing on the operator's phone in exactly the case
	// they asked it not to.
	//
	// And publishing can block: `emit` waits on the session's inbox, which is full for
	// as long as the publisher is stalled, and `callWait` does not bound that wait. A
	// call rings for seconds, so a rejection queued behind a stalled publisher is a
	// rejection that arrives after the caller has given up. The ordering this gives up
	// in exchange is between `call.offer` and the `call.terminate` the rejection
	// produces, and it was never this handler's to guarantee anyway: the two go through
	// the same queue from different events, and a client tells them apart by `call_id`.
	if first && s.rejectsCalls() {
		s.refuse(meta)
	}
	if !first {
		return true
	}

	// A group call is group traffic, and an inbox that asked for direct chats only has
	// no group conversation to show it in. The contract's `call.offer` carries no group
	// either, so publishing one would put a call from a group the client does not have
	// into the direct chat with whoever started it.
	if !meta.GroupJID.IsEmpty() && !s.wantsGroups() {
		return true
	}

	ctx, cancel := s.looking()
	defer cancel()

	// The creator and not `From`: for a 1:1 call they are the same JID, and for a group
	// call `From` is the group. Whoever rang is what an inbox shows.
	//
	// `CallCreatorAlt` is the other namespace's half when WhatsApp sent it, which is what
	// lets a client that knows this contact by number find them from a call announced by
	// LID. No push name: a call event carries none, and a store read on this path would
	// put a contact lookup in front of a rejection that has seconds to be written.
	from := s.party(ctx, meta.CallCreator, meta.CallCreatorAlt)
	if from.Phone == "" && from.LID == "" {
		// The contract requires the offer to name somebody, and a frame that names
		// nobody is one the client cannot act on or even dedupe against. Logged rather
		// than published empty.
		s.log.Warn().Str("sid", s.sid).Str("call_id", meta.CallID).
			Msg("a call arrived from an address this connector could not name")
		return true
	}

	s.emit(protocol.EventCallOffer, callOffer{
		CallID:    meta.CallID,
		From:      from,
		Video:     media.known && media.video,
		Timestamp: meta.Timestamp.UnixMilli(),
	})
	return true
}

// refuse declines a call on the client's standing instruction.
//
// Its own goroutine because this runs on whatsmeow's dispatch: a node write that waits on
// the socket lock would hold up every event behind it, including the terminate for this
// same call.
func (s *Session) refuse(meta *waTypes.BasicCallMeta) {
	client := s.current()
	go func() {
		ctx, cancel := context.WithTimeout(s.ctx, s.callWait)
		defer cancel()
		if err := s.declineCall(ctx, client, meta.CallCreator, meta.CallID); err != nil {
			// Nobody is waiting for this and there is nothing to answer: the client
			// asked for a policy, not for a command. A rejection that did not land
			// leaves the call ringing on the operator's phone, which is what the log
			// line is for.
			s.log.Warn().Err(err).Str("sid", s.sid).Str("call_id", meta.CallID).
				Msg("could not refuse an incoming call the client asked to have refused")
		}
	}()
}

// callEnded publishes the end of a call, whoever ended it.
//
// Not gated on having published the offer. The offer can be missed -- an instance that
// took the account over mid-ring never saw it -- and a client that learned about the call
// some other way still has a ringing conversation to close.
func (s *Session) callEnded(event *waEvents.CallTerminate) bool {
	if !event.GroupJID.IsEmpty() && !s.wantsGroups() {
		return true
	}

	ctx, cancel := s.looking()
	defer cancel()

	payload := callTerminate{CallID: event.CallID}
	if from := s.party(ctx, event.CallCreator, event.CallCreatorAlt); from.Phone != "" || from.LID != "" {
		// Optional in the contract, unlike the offer's: what identifies a terminate is
		// the call id, and a client that saw the offer already knows who rang.
		payload.From = &from
	}
	if event.Reason != "" {
		// Sent as null when WhatsApp gave none, rather than omitted: the field is in
		// the contract as nullable precisely so a client can tell "ended, cause unknown"
		// from a frame that forgot to carry it.
		reason := event.Reason
		payload.Reason = &reason
	}
	s.emit(protocol.EventCallTerminate, payload)
	return true
}

// rejectCall carries out `call.reject`.
func (s *Session) rejectCall(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req rejectRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a call rejection has to say which call it is refusing")
	}
	if req.CallID == "" {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a call rejection has to name the call it is refusing")
	}
	if req.From == nil {
		// Required by the contract, and not something this could fill in: WhatsApp
		// addresses the rejection to whoever started the call, and a session holds no
		// record of a call it may never have seen.
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a call rejection has to name who is calling")
	}
	caller, err := jidOf(*req.From)
	if err != nil {
		return nil, err
	}
	if caller.Server == waTypes.GroupServer {
		// The rejection goes to the person who started the call, and WhatsApp writes the
		// group into the node nowhere. A group address here is a caller that addressed
		// the chat instead of the creator, and refusing it is better than writing a node
		// WhatsApp answers by doing nothing.
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a call rejection names whoever started the call, not the group it is in")
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	if err := s.declineCall(ctx, s.current(), caller, req.CallID); err != nil {
		return nil, callFailure(err)
	}
	// Nothing to answer with. The call's end arrives as `call.terminate` on its own, from
	// WhatsApp, which is what tells the client the refusal took effect -- so an answer
	// here would either repeat it or promise more than a written node proves.
	return nil, nil
}

// callFailure turns whatever the rejection came back with into something the contract has
// a word for.
//
// The closed vocabulary and not the library's text, for the reason every path here gives:
// what comes back describes this deployment's insides, and it would be read by whoever
// opened the conversation.
func callFailure(err error) error {
	if named, coded := commandFailure(err, "call rejection"); named {
		return coded
	}
	return protocol.NewError(protocol.ErrorWaError, "WhatsApp refused the call rejection")
}
