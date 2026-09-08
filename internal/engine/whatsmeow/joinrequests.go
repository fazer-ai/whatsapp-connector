package whatsmeow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
