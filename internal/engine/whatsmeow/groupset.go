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

// unaddressableTopicID is what WhatsApp reports as a description's id when the description
// was written without one. Not a sentinel this connector invented: it is the literal
// string that comes back on the wire, and it reads like a JavaScript `undefined` that got
// as far as the server.
const unaddressableTopicID = "undefined"

// writeTheDescription writes a group's description through the one whatsmeow call that
// gives it an id, and uses the one that does not only where WhatsApp leaves no choice.
//
// The two calls do not send the same stanza. `SetGroupDescription` sends
// `<description><body>…</body></description>` with no attributes at all; `SetGroupTopic`
// always puts an `id` on it, names the description it replaces with `prev`, and for an
// empty text drops the body and sets `delete="true"` instead. A description written
// without an id is one nothing can address afterwards: WhatsApp reports its id as
// `"undefined"`, and a removal, which has to name what it replaces, is refused with a 409.
//
// Measured live on 10/09/2026, on groups between the two paired test accounts:
//
//	write with SetGroupDescription -> topic_id "undefined"
//	  then remove with SetGroupDescription("") -> 1m15.001s, info query timed out
//	  then remove with SetGroupTopic("")       ->    409 ms, 409 conflict
//	  then rewrite with SetGroupTopic          ->    370 ms, 409 conflict
//	write with SetGroupTopic       -> topic_id "3EB0C2A14F4FBC421B2E8C"
//	  then remove with SetGroupTopic("")       ->    959 ms, removed
//	a group that never had one     -> topic_id ""
//	  remove with SetGroupTopic("")            ->    536 ms, removed
//	  remove with SetGroupDescription("")      -> 1m15.001s, info query timed out
//
// The id is read here rather than left to `SetGroupTopic`, which fetches it itself when
// handed an empty one, and that is not duplicated work: it is the same round trip made
// where its outcome can be told apart. Two things depend on seeing it. A 409 means "you
// may not replace the description named by this prev", and there are two ways to get one
// -- a description with no id, and a description somebody else changed between the read
// and the write -- so deciding on the error alone would answer a concurrent edit by
// writing over it, and by leaving another description nothing can ever remove. And
// `SetGroupTopic` flattens a failure of its own lookup with `%v`, so a disconnection or a
// rate limit during it would reach the caller as `internal` instead of as itself.
//
// What that does not buy, and the limit is worth stating rather than implying: a group
// with no description when it was read has no id to name, so `SetGroupTopic` reads it
// again and takes whatever is there. A description added by another admin in that window
// is written over instead of answering 409. That is last write wins, which is what
// `group.description.set` means and what this connector has always done -- the id is here
// so a refusal is not mistaken for a legacy description, not to turn the command into a
// compare-and-set it never promised. Naming the absence would take a stanza with an id and
// no `prev`, which whatsmeow does not expose.
func (s *Session) writeTheDescription(
	ctx context.Context, client *wm.Client, group waTypes.JID, description, revision string,
) error {
	info, err := s.groupInfo(ctx, client, group)
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
	if legacyDescription(info.TopicID, description) {
		// A description this connector wrote before it knew to give one an id. The call
		// below is the only one that still changes it, and it leaves the replacement
		// unaddressable in the same way -- taken anyway, because the alternative is
		// answering 409 for a write that works today.
		return client.SetGroupDescription(ctx, group, description) //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
	}
	err = client.SetGroupTopic(ctx, group, info.TopicID, revision, description)
	if err != nil && ctx.Err() != nil {
		// A group with no description at all sends `SetGroupTopic` to read one for itself,
		// and it flattens a failure of that read with `%v`. The ceiling above is the most
		// likely thing to end it, and a deadline reported as `internal` tells the caller
		// this connector broke rather than that it stopped waiting.
		return ctx.Err() //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
	}
	return err //nolint:wrapcheck // classified by contactFailure, which needs the sentinels
}

// legacyDescription reports whether a write has to go through the call that leaves a
// description without an id.
//
// Narrow on purpose, and each condition rules out a different way of being wrong. Only the
// id WhatsApp reports for a description written without one qualifies: a real id, or none
// at all on a group that never had a description, both address fine. And only a write --
// a removal on such a group is answered as the refusal it is, in a third of a second,
// because sending it the other way is the seventy-five second wait this exists to remove.
func legacyDescription(topicID, description string) bool {
	return topicID == unaddressableTopicID && description != ""
}
