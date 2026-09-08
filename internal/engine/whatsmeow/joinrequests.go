package whatsmeow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// joinRequestsRequest is `group.join_requests.list`: who is waiting to be let into a
// group that admits people by approval.
type joinRequestsRequest struct {
	Group protocol.Address `json:"group"`
}

// joinRequest is one row of the answer, `{party, requested_at}`.
//
// `requested_at` is null when WhatsApp did not say when. A request with no date is still a
// request somebody is waiting on, and a zero timestamp reaches a dashboard as January 1970
// -- a date that reads as real and orders wrong against every other row. Null rather than
// absent because that is how the rest of this contract writes "there is none": `address`
// on a contact check, `url` on a profile picture, `code` on a participant row.
type joinRequest struct {
	Party       protocol.Party `json:"party"`
	RequestedAt *int64         `json:"requested_at"`
}

// listJoinRequests answers who is waiting to join a group.
func (s *Session) listJoinRequests(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req joinRequestsRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a join request listing has to name the group it is asking about")
	}
	if req.Group.Kind != protocol.AddressGroup {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is not a group: only a group has people waiting to join it", req.Group.Kind))
	}
	group, err := jidOf(req.Group)
	if err != nil {
		return nil, err
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	waiting, err := s.joinRequests(ctx, s.current(), group)
	if err != nil {
		return nil, contactFailure(err, "join request listing")
	}

	// Never nil: an empty list is the answer "nobody is waiting", and a client reading
	// `null` where it expected an array has to decide which of the two that is.
	rows := make([]joinRequest, 0, len(waiting))
	for i := range waiting {
		one := &waiting[i]
		party := s.party(ctx, one.JID)
		if party.Phone == "" && party.LID == "" {
			// Nobody this connector can name, and a row with no party is one the contract
			// does not allow. Nothing is lost that a client could have used: approving a
			// request means naming who is approved, and there is no name to send.
			s.log.Warn().Msg("left a join request out of the listing: nobody it names can be addressed")
			continue
		}
		rows = append(rows, joinRequest{Party: party, RequestedAt: askedAt(one.RequestedAt)})
	}
	return json.Marshal(rows)
}

// askedAt is a request's date in epoch milliseconds, and nil when WhatsApp gave none.
// `time.Time`'s zero value is year 1, whose UnixMilli is a large negative number that a
// client would read as a date rather than as an absence.
func askedAt(when time.Time) *int64 {
	if when.IsZero() {
		return nil
	}
	millis := when.UnixMilli()
	return &millis
}

// joinRequestsUpdate is `group.join_requests.update`: let somebody in, or turn them away.
type joinRequestsUpdate struct {
	Group        protocol.Address   `json:"group"`
	Participants []protocol.Address `json:"participants"`
	Action       string             `json:"action"`
}

// joinRequestActions is the contract's enum, mapped onto whatsmeow's. Listing it rather
// than casting the string keeps a payload from naming an action this build has not seen.
var joinRequestActions = map[string]wm.ParticipantRequestChange{
	"approve": wm.ParticipantChangeApprove,
	"reject":  wm.ParticipantChangeReject,
}

// updateJoinRequests answers approve or reject for the people waiting on a group.
//
// The same rows as `group.participants.update`, built by the same code: WhatsApp answers
// one IQ with a verdict per person here too, so the command succeeds whenever the request
// was answered and a caller reads its own participant's fate off the row. A request
// nothing was carried out of fails as a whole, which is what keeps a client from marking
// a request handled that WhatsApp refused -- `group_join_requests_controller#handle`
// drops the request from its own list right after the call, and only a raised error stops
// it.
func (s *Session) updateJoinRequests(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req joinRequestsUpdate
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a join request update has to name a group, who is waiting on it and what to do")
	}
	if req.Group.Kind != protocol.AddressGroup {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is not a group: only a group has people waiting to join it", req.Group.Kind))
	}
	group, err := jidOf(req.Group)
	if err != nil {
		return nil, err
	}
	action, known := joinRequestActions[req.Action]
	if !known {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is not something this connector does to a join request", req.Action))
	}
	if len(req.Participants) == 0 {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a join request update with nobody in it decides nothing")
	}
	asked := make([]waTypes.JID, len(req.Participants))
	for i, party := range req.Participants {
		switch party.Kind {
		case protocol.AddressPhone, protocol.AddressLID:
		default:
			return nil, protocol.NewError(protocol.ErrorInvalidPayload,
				fmt.Sprintf("%q is not somebody who can ask to join a group", party.Kind))
		}
		if asked[i], err = jidOf(party); err != nil {
			return nil, err
		}
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	answered, err := s.decideJoinRequests(ctx, s.current(), group, asked, action)
	if err != nil {
		return nil, contactFailure(err, "join request update")
	}

	// Every refusal stays `wa_error`. WhatsApp answers a refused approval with a number
	// and none of them has been confirmed to mean anything in particular here, unlike the
	// two pairs `group.participants.update` translates. A client branches on the code, so
	// a guess spelled as a contract code sends it somewhere nobody checked; the number is
	// logged, which is where it can be confirmed first.
	rows, refused := s.verdicts(req.Participants, answered, req.Action, func(int) protocol.ErrorCode {
		return protocol.ErrorWaError
	})
	if refused == len(rows) {
		return nil, protocol.NewError(refusalAcross(rows),
			fmt.Sprintf("WhatsApp decided none of the %d join requests", len(rows)))
	}
	return json.Marshal(rows)
}
