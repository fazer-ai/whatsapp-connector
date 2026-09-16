package whatsmeow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	wm "go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// callWriteTimeout bounds the rejection nobody is waiting on. Short for the same reason
// as a presence write: nothing downstream depends on it, a call rings for seconds, and a
// refusal that arrives after the caller has given up is a node written into a call that
// no longer exists.
//
// It covers both halves of a refusal, not one: the node that joins the call and the node
// that refuses it go out under the same deadline, so that a deadline reached between them
// is the same case as a socket lost between them. Measured, that case costs nothing -- the
// call rings its full course and ends normally, and the account's phone clears -- so the
// bound is the write, not the pair.
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
	seen map[string]waTypes.JID
	keys []string
	next int
}

// add records a key and reports whether it is new.
func (r *ring) add(key string, value waTypes.JID) bool {
	if _, known := r.seen[key]; known {
		return false
	}
	if r.seen == nil {
		r.seen = make(map[string]waTypes.JID, answeredCalls)
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
	r.seen[key] = value
	return true
}

// of is the value remembered for a key, and whether the key is still in the set.
func (r *ring) of(key string) (waTypes.JID, bool) {
	value, known := r.seen[key]
	return value, known
}

// callMedia is what an announcement said about the kind of call, and whether it said
// anything at all.
//
// A plain bool would render "this is a voice call" and "this announcement did not say"
// identically, and both happen: `offer_notice` names the media in an attribute, `offer`
// carries it as a child of the node instead, and a node this connector was handed without
// either says nothing.
type callMedia struct {
	known bool
	video bool
}

// mediaOfOffer reads the kind of call off the offer node.
//
// `events.CallOffer` has no field for it -- whatsmeow hands the node through as `Data`
// and leaves the reading to whoever wants it -- and the node is where a 1:1 call says so:
// the offer carries an `<audio>` or a `<video>` child holding the keys for that stream.
// Without this every direct video call reaches the client as a voice call, and the notice
// that does name the media cannot correct it, because by then the call is a call this
// session has already published.
func mediaOfOffer(node *waBinary.Node) callMedia {
	if node == nil {
		return callMedia{}
	}
	if len(node.GetChildrenByTag("video")) > 0 {
		return callMedia{known: true, video: true}
	}
	if len(node.GetChildrenByTag("audio")) > 0 {
		return callMedia{known: true}
	}
	return callMedia{}
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
func (s *Session) callOffered(meta *waTypes.BasicCallMeta, media callMedia, group bool) bool {
	// One call, one of everything. WhatsApp sends `offer` and `offer_notice` for the same
	// call, in either order, and both reach here.
	//
	// The one that says whether it is video is `offer_notice`, so the first to arrive is
	// the one published and a bare `offer` publishes `video: false`. That is the right
	// way round: a voice call is what the overwhelming majority of them are, and the
	// alternative -- holding the offer back until the notice arrives -- delays every
	// call for one that may never come.
	first := s.firstSightOf(meta.CallID, meta.CallCreator)

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
	//
	// Whether it is one is the caller's to say, and not `GroupJID` alone: a notice says
	// so in its `type` attribute and the `group-jid` beside it is optional, so a group
	// call announced without one would read as a direct call from whoever started it.
	if group && !s.wantsGroups() {
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

	// The address a client names carries no device, and joining the call needs one. This
	// session kept it from the offer; a call it never saw leaves the client's address to
	// stand, which is a refusal WhatsApp will take and not act on. Said plainly in the log
	// rather than answered as a failure: the node did go out.
	if rang, known := s.deviceThatRang(req.CallID); known {
		caller = rang
	} else {
		s.log.Warn().Str("sid", s.sid).Str("call_id", req.CallID).
			Msg("refusing a call this session never saw begin, so the refusal names an account and not a device")
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

// refusalSocket is the little of a client a refusal needs: who this account is, a fresh
// stanza id for each node, and somewhere to write one.
//
// Named as an interface rather than taken as a `*wm.Client`, because everything that went
// wrong here is a property of the sequence and not of the nodes: which node goes first,
// whether the second one goes at all when the first failed, whether the two carry
// different ids, and whether either is written by an account that is not logged in.
// `refusalNodes` returns shapes and a test can hold those; none of the four shows up in a
// shape. A mutation battery is what settled it -- swallowing the error from the join, and
// giving both nodes the same id, both survived a green suite before this existed.
type refusalSocket interface {
	// ownID is the account this connector is logged in as, empty when it is not.
	ownID() waTypes.JID
	// stanzaID is a fresh id, and a different one on every call.
	stanzaID() string
	// send writes one node and does not wait for an answer.
	send(ctx context.Context, node waBinary.Node) error
}

// clientSocket is the real one.
//
// `DangerousInternals` is deprecated as dangerous and taken deliberately, on the same
// terms as the keyed group create: these are the library's own calls, reached this way
// because `RejectCall` writes the node that does not work and `sendNode` is not exported.
type clientSocket struct {
	client *wm.Client
	inside *wm.DangerousInternalClient
}

func socketOf(client *wm.Client) clientSocket {
	return clientSocket{client: client, inside: client.DangerousInternals()} //nolint:staticcheck // the only route a working refusal has
}

func (c clientSocket) ownID() waTypes.JID { return c.inside.GetOwnID() }

func (c clientSocket) stanzaID() string { return c.client.GenerateMessageID() }

func (c clientSocket) send(ctx context.Context, node waBinary.Node) error {
	return c.inside.SendNode(ctx, node) //nolint:wrapcheck // wrapped by decline, which names the call
}

// declineOverClient refuses a call the way WhatsApp actually honours it: by joining the
// call's signalling first, and only then refusing.
//
// whatsmeow's own RejectCall writes the `<reject>` alone, and WhatsApp takes that node,
// acks it `<ack class="call" type="reject"/>`, hands it to the caller's client, which
// answers with a `<receipt>` of its own -- and the call keeps ringing. Worse than a no-op:
// the account's own phone is left with a call notification it cannot dismiss, because the
// account was never told the call was over.
//
// What is missing is that this device never entered the call. WhatsApp ignores a refusal
// from a participant that is not in the call, so the `<preaccept>` is what makes the
// `<reject>` mean anything.
func declineOverClient(ctx context.Context, client *wm.Client, caller waTypes.JID, callID string) error {
	return decline(ctx, socketOf(client), caller, callID)
}

// decline writes the two nodes a refusal is made of, in the order WhatsApp honours.
//
// The two go out back to back, with nothing between them, and that is deliberate. Between
// them the account has entered a call it has not left, so the window is worth having small
// -- and with no wait it is one socket write wide. Exercising it on purpose (a `<preaccept>`
// and no `<reject>` at all) costs nothing: the call rings its full course and ends normally,
// the notification clears, and the account behaves as though this connector were not
// attached. The worst this window can do is the behaviour we already had.
func decline(ctx context.Context, socket refusalSocket, caller waTypes.JID, callID string) error {
	own := socket.ownID()
	if own.IsEmpty() {
		return wm.ErrNotLoggedIn
	}
	join, refusal := refusalNodes(own, caller, callID)
	// The refusal is not sent when joining failed. A `<reject>` on its own is the node
	// this function exists to stop writing: it would be acked, it would not end the call,
	// and it would leave the phone exactly as stuck as before.
	join.Attrs["id"] = socket.stanzaID()
	if err := socket.send(ctx, join); err != nil {
		return fmt.Errorf("join call %s to refuse it: %w", callID, err)
	}
	// Its own id, and not the join's. Two nodes of one call sharing a stanza id is a
	// reply addressed to both of them, and whichever waiter answers first cancels the
	// other -- which is how this would go quiet again without anybody noticing.
	refusal.Attrs["id"] = socket.stanzaID()
	if err := socket.send(ctx, refusal); err != nil {
		return fmt.Errorf("refuse call %s: %w", callID, err)
	}
	return nil
}

// refusalNodes is the pair a refusal is made of, in the order they go out. Built apart
// from the writing so that a test can hold the shape of both without a socket, which is
// the whole of what separates a refusal WhatsApp honours from the one that was being
// written before. Only the stanza id is left for the caller to fill: it is the one
// attribute that has to differ between two nodes of the same call.
func refusalNodes(own, caller waTypes.JID, callID string) (join, refusal waBinary.Node) {
	wrap := func(child waBinary.Node) waBinary.Node {
		return waBinary.Node{
			Tag:     "call",
			Attrs:   waBinary.Attrs{"from": own.ToNonAD(), "to": caller},
			Content: []waBinary.Node{child},
		}
	}
	// `caller` is whatever the offer named, unchanged. Not flattened and not elaborated:
	// every refusal measured to work came from a caller whose `call-creator` carried no
	// device at all, so there is no evidence for treating the two halves differently, and
	// inventing an address WhatsApp did not name would be a guard for a state nothing has
	// been measured in. The one caller class that does carry a device is the web client,
	// and that one ignores the refusal whatever it is addressed to (#234).
	return wrap(waBinary.Node{
			Tag:   "preaccept",
			Attrs: waBinary.Attrs{"call-id": callID, "call-creator": caller},
		}), wrap(waBinary.Node{
			Tag:   "reject",
			Attrs: waBinary.Attrs{"call-id": callID, "call-creator": caller, "count": "0"},
		})
}
