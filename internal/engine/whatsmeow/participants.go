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
	//
	// Keyed by the canonical address and not by its digits alone. A LID and a phone
	// number are separate namespaces that happen to be written the same way, so a request
	// naming both `{phone, 5511999990002}` and `{lid, 5511999990002}` -- two different
	// people -- would collapse onto one key and report one's verdict for both.
	verdicts := make(map[protocol.Address]*waTypes.GroupParticipant, len(answered)*2)
	for i := range answered {
		one := &answered[i]
		for _, named := range []waTypes.JID{one.JID, one.PhoneNumber, one.LID} {
			if canonical, addressable := addressOf(named); addressable {
				verdicts[canonical] = one
			}
		}
	}

	rows := make([]participantOutcome, len(req.Participants))
	refused := 0
	for i, party := range req.Participants {
		rows[i] = participantOutcome{Address: party, Status: "failed"}
		verdict, mentioned := verdicts[party]
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
			rows[i].Code = refusalOf(participantRefusal(action, verdict.Error))
		default:
			rows[i].Status = "success"
		}
		if rows[i].Status == "failed" {
			refused++
		}
	}

	if refused == len(rows) {
		// Nothing was carried out, and that is the command failing rather than a command
		// reporting failures. Three of the four actions reach this connector one
		// participant at a time -- the dashboard promotes, demotes and removes a single
		// member -- and a client updates its own roster on the reply: told `ok` with a
		// row it does not read, it shows somebody as demoted whom WhatsApp refused to
		// demote. The generation before this one raised on exactly this, so answering
		// `ok` here is a regression against what is running today, not a new strictness.
		//
		// A request where some went through still answers `ok` with the rows. Failing it
		// whole would report the participants that were added as not added, and there is
		// no second source for a caller to check that against.
		return nil, protocol.NewError(refusalAcross(rows),
			fmt.Sprintf("WhatsApp carried out none of the %d participant changes", len(rows)))
	}
	return json.Marshal(rows)
}

// refusalAcross is the one code that answers for a request nothing was carried out of.
// The named refusal wins over the opaque one: a caller that hears `wa_error` because one
// of its rows carried it learns less than the row that said why.
func refusalAcross(rows []participantOutcome) protocol.ErrorCode {
	for _, row := range rows {
		if row.Code != nil && *row.Code != protocol.ErrorWaError {
			return *row.Code
		}
	}
	return protocol.ErrorWaError
}

// refusalOf is the address of a code, which is what the row carries so that a successful
// row can say `null` rather than an empty string that reads as a code nobody knows.
func refusalOf(code protocol.ErrorCode) *protocol.ErrorCode { return &code }

// participantRefusal names why WhatsApp refused one participant.
//
// Two pairs are translated, and both because they were confirmed for that action and no
// other. An add refused with 403 is `not-authorized`, and WhatsApp attaches an invite to
// it -- an add it refuses on the other person's privacy setting, answered with a code to
// send them instead. A remove or a demote refused with 406 is the group's creator, who
// cannot be taken out of their own group. Both are what `group_participant_not_allowed`
// is for, and both are what a client acts on rather than just reports.
//
// The same numbers on the other actions are different sentences, and this connector has
// confirmed neither what they mean nor what would help. Sending the privacy code for a
// 403 on a demote would have a client offer to invite somebody it was trying to demote.
//
// Everything else stays `wa_error` on purpose. The other codes WhatsApp uses here (409,
// 408 and the rest) have no meaning this connector has confirmed per action, and a guess
// spelled as a contract code is worse than the honest `wa_error`: a client branches on
// the code, so a wrong one sends it down a road nobody checked. The numeric code is
// logged, which is where it can be confirmed without a client having been told a story.
func participantRefusal(action wm.ParticipantChange, code int) protocol.ErrorCode {
	switch {
	case action == wm.ParticipantChangeAdd && code == 403:
		return protocol.ErrorGroupParticipantNotAllowed
	case code == 406 && (action == wm.ParticipantChangeRemove || action == wm.ParticipantChangeDemote):
		// The group's creator, who cannot be removed from their own group or stripped of
		// it. Confirmed by the generation this connector replaces, which has been mapping
		// this exact pair in production and whose dashboard has a message for it
		// (`group_creator_not_modifiable`). Leaving it opaque here would take that
		// message away from an operator who has been reading it for as long as the
		// feature has existed.
		return protocol.ErrorGroupParticipantNotAllowed
	}
	return protocol.ErrorWaError
}
