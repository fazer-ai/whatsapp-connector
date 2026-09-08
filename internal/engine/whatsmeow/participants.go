package whatsmeow

import (
	"context"
	"encoding/json"
	"fmt"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// participantsRequest is `group.participants.update`: add, remove, promote or demote
// somebody in a group this account administers.
type participantsRequest struct {
	Group        protocol.Address   `json:"group"`
	Participants []protocol.Address `json:"participants"`
	Action       string             `json:"action"`
}

// participantOutcome is one row of the answer, `{address, status, code}`.
//
// The address is the one that was asked about, not the one WhatsApp answered under. A
// caller that named somebody by phone and read back a LID would have to resolve the pair
// before it could tell which of its own rows had succeeded, and the point of the row is
// to answer for what the caller asked.
type participantOutcome struct {
	Address protocol.Address    `json:"address"`
	Status  string              `json:"status"`
	Code    *protocol.ErrorCode `json:"code"`
}

// participantActions is the contract's enum, mapped onto whatsmeow's. Listing it rather
// than casting the string keeps a payload from naming an action this build has not seen.
var participantActions = map[string]wm.ParticipantChange{
	"add":     wm.ParticipantChangeAdd,
	"remove":  wm.ParticipantChangeRemove,
	"promote": wm.ParticipantChangePromote,
	"demote":  wm.ParticipantChangeDemote,
}

// updateGroupParticipants carries out `group.participants.update`.
//
// One row per participant asked about, in the order they were asked. WhatsApp answers a
// single IQ with a verdict per person and refuses them one at a time -- somebody whose
// privacy setting forbids being added fails while the rest of the same request go
// through -- so the command succeeds whenever the IQ was answered at all, and the row is
// where a caller reads whether its own request was carried out. Failing the whole command
// on the first refused participant would report the three that were added as not added.
func (s *Session) updateGroupParticipants(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req participantsRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a participants update has to name a group, who to change in it and what to do")
	}
	if req.Group.Kind != protocol.AddressGroup {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is not a group: only a group has participants to update", req.Group.Kind))
	}
	group, err := jidOf(req.Group)
	if err != nil {
		return nil, err
	}
	action, known := participantActions[req.Action]
	if !known {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is not something this connector does to a participant", req.Action))
	}
	if len(req.Participants) == 0 {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a participants update with nobody in it changes nothing")
	}
	asked := make([]waTypes.JID, len(req.Participants))
	for i, party := range req.Participants {
		switch party.Kind {
		case protocol.AddressPhone, protocol.AddressLID:
		default:
			// A group cannot be a member of a group, and neither can a newsletter or a
			// broadcast list. Sending one anyway would have WhatsApp refuse the whole
			// IQ, which loses the participants that were named correctly alongside it.
			return nil, protocol.NewError(protocol.ErrorInvalidPayload,
				fmt.Sprintf("%q is not somebody who can be in a group", party.Kind))
		}
		if asked[i], err = jidOf(party); err != nil {
			return nil, err
		}
	}

	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	answered, err := s.updateParticipants(ctx, s.current(), group, asked, action)
	if err != nil {
		return nil, contactFailure(err, "participants update")
	}

	// Indexed by every name WhatsApp answered under, because which one comes back is not
	// the caller's choice: a participant asked for by phone is answered under a LID on an
	// account that has one, and matching by position instead would line the rows up
	// wrong the moment WhatsApp leaves somebody out of its answer.
	verdicts := make(map[string]*waTypes.GroupParticipant, len(answered)*2)
	for i := range answered {
		one := &answered[i]
		for _, named := range []waTypes.JID{one.JID, one.PhoneNumber, one.LID} {
			if named.User != "" {
				verdicts[named.User] = one
			}
		}
	}

	rows := make([]participantOutcome, len(req.Participants))
	for i, party := range req.Participants {
		rows[i] = participantOutcome{Address: party, Status: "failed"}
		verdict, mentioned := verdicts[party.ID]
		switch {
		case !mentioned:
			// WhatsApp answered the request and said nothing about this person. Silence
			// is not consent: reporting success here would tell a caller somebody was
			// removed from a group they are still in.
			s.log.Warn().Str("action", req.Action).
				Msg("WhatsApp left a participant out of its answer to a participants update")
			rows[i].Code = refusalOf(protocol.ErrorWaError)
		case verdict.Error != 0:
			// The number, because the vocabulary the row answers in is deliberately
			// narrower than WhatsApp's and this is where the rest of it is kept.
			s.log.Info().Str("action", req.Action).Int("wa_code", verdict.Error).
				Msg("WhatsApp refused one participant of a participants update")
			rows[i].Code = refusalOf(participantRefusal(verdict.Error))
		default:
			rows[i].Status = "success"
		}
	}
	return json.Marshal(rows)
}

// refusalOf is the address of a code, which is what the row carries so that a successful
// row can say `null` rather than an empty string that reads as a code nobody knows.
func refusalOf(code protocol.ErrorCode) *protocol.ErrorCode { return &code }

// participantRefusal names why WhatsApp refused one participant.
//
// Only 403 is translated, and only because it is the one this connector can account for:
// it is `not-authorized`, and WhatsApp attaches an invite to it -- an add it refuses on
// the other person's privacy setting, answered with a code to send them instead. That is
// exactly what `group_participant_not_allowed` is for.
//
// Everything else stays `wa_error` on purpose. The other codes WhatsApp uses here (409,
// 408 and the rest) have no meaning this connector has confirmed per action, and a guess
// spelled as a contract code is worse than the honest `wa_error`: a client branches on
// the code, so a wrong one sends it down a road nobody checked. The numeric code is
// logged, which is where it can be confirmed without a client having been told a story.
func participantRefusal(code int) protocol.ErrorCode {
	if code == 403 {
		return protocol.ErrorGroupParticipantNotAllowed
	}
	return protocol.ErrorWaError
}
