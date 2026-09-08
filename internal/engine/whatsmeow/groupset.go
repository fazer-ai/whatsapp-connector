package whatsmeow

import (
	"context"
	"encoding/json"
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
	if err := s.setDescription(ctx, s.current(), group, description); err != nil {
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
	if err := s.readyToSend(); err != nil {
		return nil, err
	}

	if req.Setting == "member_add_mode" {
		mode, err := addMode(req.Value)
		if err != nil {
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
