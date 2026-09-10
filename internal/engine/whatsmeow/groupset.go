package whatsmeow

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// nameRequest is `group.name.set`.
type nameRequest struct {
	Group   protocol.Address `json:"group"`
	Subject string           `json:"subject"`
}

// descriptionRequest is `group.description.set`. The description is nullable, and null is
// how a group's description is removed: the contract carries the removal as an absence
// rather than as an empty string, so a payload that says nothing at all is a payload that
// asks for nothing.
type descriptionRequest struct {
	Group       protocol.Address `json:"group"`
	Description *string          `json:"description"`
}

// settingRequest is `group.settings.set`.
type settingRequest struct {
	Group   protocol.Address `json:"group"`
	Setting string           `json:"setting"`
	Value   json.RawMessage  `json:"value"`
}

// setGroupName carries out `group.name.set`.
func (s *Session) setGroupName(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req nameRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a name change has to name a group and what to call it")
	}
	group, err := groupToChange(req.Group, "a name")
	if err != nil {
		return nil, err
	}
	if req.Subject == "" {
		// WhatsApp has no nameless group, and it answers the empty string by refusing the
		// IQ. Refusing here says which field was wrong.
		return nil, protocol.NewError(protocol.ErrorInvalidPayload, "a group cannot be called nothing")
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}
	if err := s.setName(ctx, s.current(), group, req.Subject); err != nil {
		return nil, contactFailure(err, "name change")
	}
	return nil, nil
}

// setGroupDescription carries out `group.description.set`.
func (s *Session) setGroupDescription(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req descriptionRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a description change has to name a group")
	}
	group, err := groupToChange(req.Group, "a description")
	if err != nil {
		return nil, err
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}
	// Null and "" reach WhatsApp the same way, because there is one way to say a group has
	// no description and it is an empty body. The contract keeps them apart for the
	// caller's sake -- null reads as "remove it" and "" as "set it to nothing" -- and they
	// mean the same thing here.
	description := ""
	if req.Description != nil {
		description = *req.Description
	}
	// The revision the write goes out under, derived the way a message id is: whatsmeow
	// generates a fresh one when handed an empty id, and a description written twice under
	// two ids is two revisions of the group's description for one command -- the text ends
	// up the same either way, but the second publishes a `group.updated` nobody asked for.
	// Seeded with the session as well as the key, because two instances editing one group
	// under the same caller-supplied key are two commands, and handing WhatsApp the same
	// revision for both would have it read the second as a replay of the first.
	if err := s.setDescription(ctx, s.current(), group, description, s.orDerived(command, "")); err != nil {
		return nil, contactFailure(err, "description change")
	}
	return nil, nil
}

// setGroupSetting carries out `group.settings.set`.
//
// One command for four switches, which is what the contract names. Each is a different IQ
// on whatsmeow's side, so what this does is pick the one the setting names and refuse a
// value that switch cannot take -- a boolean where WhatsApp wants a mode, or a mode this
// build does not know.
func (s *Session) setGroupSetting(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req settingRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a settings change has to name a group, a setting and a value")
	}
	group, err := groupToChange(req.Group, "settings")
	if err != nil {
		return nil, err
	}
	if err := valueGiven(req.Value); err != nil {
		return nil, err
	}

	// The whole payload before the socket, which is the order every other handler here
	// uses and the reason is the caller's: a payload that can never be carried out is
	// wrong whether or not this session happens to be connected, and answering
	// `not_connected` to it invites a client to wait and try the same broken command
	// again. `readyToSend` comes after everything that could refuse it outright.
	if req.Setting == "member_add_mode" {
		mode, err := addMode(req.Value)
		if err != nil {
			return nil, err
		}
		if err := s.readyToSend(); err != nil {
			return nil, err
		}
		if err := s.setAddMode(ctx, s.current(), group, mode); err != nil {
			return nil, contactFailure(err, "settings change")
		}
		return nil, nil
	}
	flip := s.switchFor(req.Setting)
	if flip == nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is not a group setting this connector knows", req.Setting))
	}
	on, err := switchedOn(req.Value, req.Setting)
	if err != nil {
		return nil, err
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}
	if err := flip(ctx, s.current(), group, on); err != nil {
		return nil, contactFailure(err, "settings change")
	}
	return nil, nil
}

// switchFor is the setting's own seam, or nil when this build has no such setting.
//
// One seam per switch rather than one that takes the setting's name, because each is a
// different IQ and picking the wrong one leaves the group with a setting nobody asked for
// while reporting success for the one they did. Routed here, a test can watch which seam a
// payload reaches; behind a single seam it could only watch the name being passed along.
func (s *Session) switchFor(setting string) func(context.Context, *wm.Client, waTypes.JID, bool) error {
	switch setting {
	case "announce":
		return s.setAnnounce
	case "locked":
		return s.setLocked
	case "join_approval":
		return s.setJoinApproval
	}
	return nil
}

// groupToChange is the JID of the group a change is addressed to, and the refusal when the
// address names something that has no such thing.
func groupToChange(address protocol.Address, what string) (waTypes.JID, error) {
	if address.Kind != protocol.AddressGroup {
		return waTypes.EmptyJID, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is not a group: only a group has %s to change", address.Kind, what))
	}
	return jidOf(address)
}

// valueGiven refuses a settings change that carries no value.
//
// Go's json leaves a `null` alone rather than failing on it: unmarshalling null into a
// bool writes nothing and answers no error, so `"value": null` would read as `false` and
// turn a setting off that nobody asked to turn off -- and as `admin_add` on the one
// setting that is not a switch. The contract allows a boolean or a string and nothing
// else, and an absent value is the same nothing spelled differently.
func valueGiven(raw json.RawMessage) error {
	if len(raw) == 0 || string(raw) == "null" {
		return protocol.NewError(protocol.ErrorInvalidPayload,
			"a settings change has to say what to set the setting to")
	}
	return nil
}

// switchedOn reads a switch's value. Only a boolean, because that is what a switch is: a
// string here would be a caller sending a value meant for `member_add_mode`, and guessing
// which way "false" or "admin_add" leans would set the group to something nobody asked for.
func switchedOn(raw json.RawMessage, setting string) (bool, error) {
	var on bool
	if err := json.Unmarshal(raw, &on); err != nil {
		return false, protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%q is on or off, and %s is neither", setting, string(raw)))
	}
	return on, nil
}

// addMode reads who may add members, in either of the two spellings the contract allows: a
// mode by name, or the boolean the dashboard sends for "may every member add people".
func addMode(raw json.RawMessage) (waTypes.GroupMemberAddMode, error) {
	var everyone bool
	if err := json.Unmarshal(raw, &everyone); err == nil {
		if everyone {
			return waTypes.GroupMemberAddModeAllMember, nil
		}
		return waTypes.GroupMemberAddModeAdmin, nil
	}
	var named string
	if err := json.Unmarshal(raw, &named); err != nil {
		return "", protocol.NewError(protocol.ErrorInvalidPayload,
			fmt.Sprintf("%s is neither a mode nor a yes or no", string(raw)))
	}
	for mode, spelled := range memberAddModes {
		if spelled == named {
			return mode, nil
		}
	}
	return "", protocol.NewError(protocol.ErrorInvalidPayload,
		fmt.Sprintf("%q is not a way this connector knows of deciding who adds members", named))
}

// leaveGroup carries out `group.leave`.
//
// The one command in this file that cannot be undone from this side: leaving a group needs
// an invite to reverse, and an invite needs somebody still in it. There is no confirmation
// step here because the confirmation belongs where a person is -- the dashboard asks, and
// this is what it asks for.
func (s *Session) leaveGroup(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req groupTarget
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"leaving has to name the group to leave")
	}
	group, err := groupToChange(req.Group, "members to leave")
	if err != nil {
		return nil, err
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}
	if err := s.leave(ctx, s.current(), group); err != nil {
		return nil, contactFailure(err, "group departure")
	}
	return nil, nil
}

// photoRequest is `group.photo.set`. A nil image removes the picture, and an absent field
// is a nil.
//
// Telling absent from null looks like the careful reading -- a caller who forgot the field
// would then not lose a group's photo to it -- and the contract closes that door on
// purpose: an absent field and an explicit null mean the same thing to a reader, and a
// field that has to distinguish them carries its own flag (`group_info.has_picture` is the
// one that does). The client that speaks this contract is built on the same rule and drops
// nils on the way out, so `image: null` is not a payload it can send at all: a connector
// that insisted on it would answer the only removal a client can express with
// `invalid_payload`.
type photoRequest struct {
	Group protocol.Address `json:"group"`
	Image *string          `json:"image"`
}

// setGroupPhoto carries out `group.photo.set`.
//
// The bytes travel inside the frame, base64, which is the one place this contract puts
// media on the wire: `contract/README.md` says media never does, and the picture of a
// group is the exception the schema spells out. It is a profile picture -- WhatsApp keeps
// these small -- rather than a message attachment, and there is no `media_ref` for
// something that was never a message.
func (s *Session) setGroupPhoto(ctx context.Context, command *protocol.Command) (json.RawMessage, error) {
	var req photoRequest
	if err := json.Unmarshal(command.Payload, &req); err != nil {
		return nil, protocol.NewError(protocol.ErrorInvalidPayload,
			"a photo change has to name a group and say what to put on it")
	}
	group, err := groupToChange(req.Group, "a photo")
	if err != nil {
		return nil, err
	}

	var picture []byte
	if req.Image != nil {
		if picture, err = base64.StdEncoding.DecodeString(*req.Image); err != nil {
			return nil, protocol.NewError(protocol.ErrorInvalidPayload,
				"the image is not base64 this connector can read")
		}
		if len(picture) == 0 {
			// An empty string, or base64 that decodes to nothing. Sent on, WhatsApp would
			// be handed a picture element with no picture in it, which is neither setting
			// a photo nor removing one. Refused rather than read as a removal, because a
			// caller that meant to remove had a way to say so and did not use it.
			return nil, protocol.NewError(protocol.ErrorInvalidPayload,
				"the image decodes to nothing: leave the field out to remove the picture")
		}
	}
	if err := s.readyToSend(); err != nil {
		return nil, err
	}
	// nil is what removes it: whatsmeow reads a nil avatar as the removal, and that is
	// the one way to say it to WhatsApp.
	if err := s.setPhoto(ctx, s.current(), group, picture); err != nil {
		if errors.Is(err, wm.ErrInvalidImageFormat) {
			// WhatsApp refusing the bytes themselves. It answers `not-acceptable`, which
			// every other command here reports as `wa_error` -- and `wa_error` is
			// documented as worth retrying, while these bytes are refused every time.
			// The payload is what is wrong, and the caller has to send a different image
			// rather than the same one again.
			return nil, protocol.NewError(protocol.ErrorInvalidPayload,
				"WhatsApp will not take this image: a group photo has to be a JPEG")
		}
		return nil, contactFailure(err, "photo change")
	}
	return nil, nil
}

// writeTheDescription writes a group's description through the one whatsmeow call that
// gives it an id.
//
// #163 was a removal that could not be carried out. `SetGroupDescription` sends a
// `description` node with no attributes at all -- no `id`, no `prev` -- and a description
// written that way is one nothing can address afterwards: WhatsApp reports its id as the
// literal string `"undefined"`, and every later change to it is refused.
//
// Measured on real groups between the two paired test accounts, 10/09/2026, on a group
// made for the purpose and driven in this order:
//
//	as found                              -> topic_id ""
//	SetGroupDescription("travada")           494 ms, applied -> topic_id "undefined"
//	SetGroupDescription("segunda")           326 ms, 409 conflict
//	SetGroupTopic(rewrite)                   704 ms, 409 conflict
//	SetGroupTopic("")                        716 ms, 409 conflict
//	<description id=...>          by hand    324 ms, 409 conflict
//	<description id=... delete=true>         327 ms, 409 conflict
//
// The last two go through `DangerousInternals().SendGroupIQ`, which is the only way to
// send the stanza this connector would want: an id, naming no predecessor. It is refused
// too. So a description written the old way is frozen -- no call and no stanza shape
// changes it, and there is nothing left here to try. What this can do is stop making new
// ones, and answer the frozen ones in a third of a second rather than in the seventy-five
// #163 measured four times over.
//
// The id is read here rather than left to `SetGroupTopic`, which fetches it itself when
// handed an empty one and flattens a failure of that fetch with `%v`. The reads are under
// a ceiling and the writes are not, which is the connector's rule rather than this
// command's: giving up on a read costs its answer, and giving up on a write turns
// "WhatsApp applied it" into a `timeout` the ledger never records. Two consequences, and
// the second is a limit worth stating rather than implying:
//
// Naming an id makes the write a compare-and-set, and `group.description.set` is not one:
// it is last write wins, and it was before this change too, because the call it used sent
// no `prev` and could not be refused for naming the wrong one. So the one 409 that means
// "somebody else changed it since you looked" is answered by looking again and writing
// over what is there now, under the same revision, once.
//
// And a group with no description has no id to name, so `SetGroupTopic` reads it again
// anyway. That read is whatsmeow's, its failure arrives with the sentinel flattened out of
// it, and there is nothing here that can put it back: a second request answers about
// itself, not about the one that failed. Such an answer reaches the caller as `internal`,
// which is what this repository does with a cause it cannot name. Fixing it properly is a
// `%w` upstream.
func (s *Session) writeTheDescription(
	ctx context.Context, client *wm.Client, group waTypes.JID, description, revision string,
) error {
	// One budget for the two reads, not one each. The ceiling exists to bound how long a
	// single command may hold the session's serial executor, and two lookups given fifteen
	// seconds each hold it for thirty -- which is the thing being prevented, arrived at by
	// applying the prevention twice. The writes are outside it, for the reason spelled out
	// at `writeUnder`.
	budget, giveUp := context.WithTimeout(ctx, groupIQWait)
	defer giveUp()
	info, err := s.groupInfo(budget, client, group)
	if err != nil {
		return err //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
	}
	if info == nil {
		// whatsmeow answers an error for a group it cannot read, so nothing reaches here
		// with neither. Reading a field off it would take the session's executor down and
		// every command queued behind it with it.
		return protocol.NewError(protocol.ErrorInternal,
			"the group came back empty while writing its description")
	}
	if err := budget.Err(); err != nil {
		return err //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
	}
	if alreadyApplied(info.TopicID, revision) {
		return nil
	}

	err = s.writeUnder(ctx, client, group, info.TopicID, revision, description)
	if err == nil {
		return nil
	}
	if !refusedAsAConflict(err) {
		return err //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
	}

	// A 409 says the description this write claimed to replace is not the one that is
	// there. Either somebody changed it in the moment between the read and the write, or
	// the group is frozen -- and the two are told apart by looking, not by guessing: a
	// changed id is a concurrent edit, and the same id back is a group where nothing this
	// connector sends will ever be accepted.
	fresh, again := s.groupInfo(budget, client, group)
	if again != nil {
		return again //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
	}
	if fresh == nil {
		return err //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
	}
	if alreadyApplied(fresh.TopicID, revision) {
		return nil
	}
	if fresh.TopicID == info.TopicID {
		return err //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
	}
	// Once, and under the same revision. Once because a caller waiting on a description is
	// better served by an answer than by this session's only goroutine racing whoever else
	// is editing; the same revision because a redelivery of this command has to write the
	// revision it wrote the first time rather than a second one.
	return s.writeUnder(ctx, client, group, fresh.TopicID, revision, description)
}

// writeUnder writes the description under the caller's own context, and reports a deadline
// as itself when a deadline is what ended the write.
//
// No ceiling of this connector's, and that is the whole point of the function's existence
// being separate from the reads above it: the ledger records only successes, so a ceiling
// that turns "WhatsApp applied it" into `timeout` leaves nothing recorded and the
// redelivery writes again. Every other group write is unbounded for that reason, and this
// one is no exception -- the derived revision recognises a redelivery only while the group
// still carries it, which a third party's edit ends.
//
// The check that remains is about naming what happened: `SetGroupTopic` reads the current
// id for itself when handed an empty one and flattens a failure of that read with `%v`, so
// a caller's own deadline ending the write would otherwise reach it as `internal` -- this
// connector broke, rather than you did not wait long enough.
func (s *Session) writeUnder(
	ctx context.Context, client *wm.Client, group waTypes.JID, previous, revision, description string,
) error {
	err := s.setTopic(ctx, client, group, previous, revision, description)
	if err != nil {
		if ended := ctx.Err(); ended != nil {
			return ended //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
		}
	}
	return err //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
}

// alreadyApplied reports whether the description the group carries is the one this command
// wrote, which is a question that can be asked at all only because the revision is derived
// rather than generated: `orDerived` hashes the session and the command's idempotency key,
// `SetGroupTopic` sends it as the description's `id`, and WhatsApp stores it as the topic
// id. So a redelivery of a command whose answer was lost -- WhatsApp committed it and the
// ledger never learned -- recognises its own work and reports the success it already had.
//
// Writing again instead is not harmless, which is why this is a check and not an
// optimisation: replaying a revision over itself is refused with 409, the second look sees
// the same id, and the caller is told `wa_error` for a command that worked. Invariant 5
// says a redelivered command must not duplicate a side effect; being told it failed
// because it already happened is the same promise broken from the other end.
//
// What it does not do, and the limit is the point: it recognises the revision only while
// the revision is still what the group carries. WhatsApp committing a write whose answer
// this side gave up on, and another admin writing over it before the caller redelivers,
// leaves nothing here to recognise -- the redelivery reads somebody else's id and writes
// its own text over it.
//
// That is the command's own meaning rather than a hole in it. `group.description.set` is
// last write wins: a caller that sends it again is asking for its text to be what the
// group says, and the alternative -- refusing because somebody else got there first -- is
// the compare-and-set this command has never been and that the review rejected when an
// earlier revision of it did exactly that. What would close the gap properly is the ledger
// recording an attempt rather than only a success, which is #165 and is a change to how
// invariant 5 is kept rather than to what it promises.
func alreadyApplied(topicID, revision string) bool {
	return topicID == revision
}

// refusedAsAConflict reports whether WhatsApp answered 409, which for a `description`
// stanza means the `prev` it carried is not what the group has.
func refusedAsAConflict(err error) bool {
	var refused *wm.IQError
	return errors.As(err, &refused) && refused.Code == 409
}
