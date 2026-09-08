package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

func setCommand(t *testing.T, kind protocol.CommandType, payload string) *protocol.Command {
	t.Helper()
	return &protocol.Command{
		V: protocol.Version, ID: "c1", Type: kind, SID: "s1", Payload: json.RawMessage(payload),
	}
}

// A session that refuses every change: what each test wires up is the one seam it is
// about, so anything reaching another one is a command dispatched to the wrong place.
func settableSession(t *testing.T) *Session {
	t.Helper()
	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.setName = func(context.Context, *wm.Client, waTypes.JID, string) error {
		t.Error("a command that is not a name change renamed the group")
		return nil
	}
	session.setDescription = func(context.Context, *wm.Client, waTypes.JID, string) error {
		t.Error("a command that is not a description change rewrote the description")
		return nil
	}
	refuse := func(named string) func(context.Context, *wm.Client, waTypes.JID, bool) error {
		return func(context.Context, *wm.Client, waTypes.JID, bool) error {
			t.Errorf("a command reached %q, which it did not name", named)
			return nil
		}
	}
	session.setAnnounce = refuse("announce")
	session.setLocked = refuse("locked")
	session.setJoinApproval = refuse("join_approval")
	session.setAddMode = func(context.Context, *wm.Client, waTypes.JID, waTypes.GroupMemberAddMode) error {
		t.Error("a command that is not a settings change changed who may add members")
		return nil
	}
	return session
}

func TestAGroupChangeRefusesAPayloadItCannotCarryOut(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name    string
		kind    protocol.CommandType
		payload string
	}{
		{name: "a name change with no group", kind: protocol.CommandGroupNameSet, payload: `{"subject":"x"}`},
		// WhatsApp has no nameless group and answers the empty string by refusing the IQ.
		// Refusing here says which field was wrong.
		{
			name: "a name change to nothing", kind: protocol.CommandGroupNameSet,
			payload: `{"group":{"kind":"group","id":"120363000000000001"},"subject":""}`,
		},
		{
			name: "a name change to a person", kind: protocol.CommandGroupNameSet,
			payload: `{"group":{"kind":"phone","id":"5511999990002"},"subject":"x"}`,
		},
		{name: "a description change with no group", kind: protocol.CommandGroupDescriptionSet, payload: `{"description":"x"}`},
		{name: "a settings change with no group", kind: protocol.CommandGroupSettingsSet, payload: `{"setting":"announce","value":true}`},
		{
			name: "a setting this build does not know", kind: protocol.CommandGroupSettingsSet,
			payload: `{"group":{"kind":"group","id":"120363000000000001"},"setting":"ephemeral","value":true}`,
		},
		// A switch is a yes or a no. A string here is a caller sending the value meant for
		// member_add_mode, and guessing which way it leans sets the group to something
		// nobody asked for.
		{
			name: "a switch given a mode", kind: protocol.CommandGroupSettingsSet,
			payload: `{"group":{"kind":"group","id":"120363000000000001"},"setting":"announce","value":"all_member_add"}`,
		},
		{
			name: "who adds members, given a mode nobody knows", kind: protocol.CommandGroupSettingsSet,
			payload: `{"group":{"kind":"group","id":"120363000000000001"},"setting":"member_add_mode","value":"whoever"}`,
		},
		{
			name: "who adds members, given a number", kind: protocol.CommandGroupSettingsSet,
			payload: `{"group":{"kind":"group","id":"120363000000000001"},"setting":"member_add_mode","value":3}`,
		},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			session := settableSession(t)
			_, err := session.Execute(t.Context(), setCommand(t, refused.kind, refused.payload))
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

func TestAGroupNameChangeSendsTheNameItWasGiven(t *testing.T) {
	t.Parallel()

	session := settableSession(t)
	sent := ""
	session.setName = func(_ context.Context, _ *wm.Client, group waTypes.JID, subject string) error {
		if group.Server != waTypes.GroupServer || group.User != "120363000000000001" {
			t.Errorf("the group was renamed to %s, want the one that was named", group)
		}
		sent = subject
		return nil
	}

	result, err := session.Execute(t.Context(), setCommand(t, protocol.CommandGroupNameSet,
		`{"group":{"kind":"group","id":"120363000000000001"},"subject":"Turma da tarde"}`))
	if err != nil {
		t.Fatalf("group.name.set: %v", err)
	}
	if sent != "Turma da tarde" {
		t.Errorf("WhatsApp was given %q, want the subject that was asked", sent)
	}
	// The contract's table says this command answers null, and a caller is waiting for the
	// confirmation rather than for data.
	if result != nil {
		t.Errorf("the answer carries %s, want nothing", result)
	}
}

// Removing a description and setting it to an empty one are one thing on WhatsApp's side,
// and the contract keeps them apart for the caller. Both have to arrive as the empty body,
// which is the only way WhatsApp says a group has no description.
func TestAGroupDescriptionChangeSendsAnEmptyBodyForBothWaysOfClearingIt(t *testing.T) {
	t.Parallel()

	for _, cleared := range []struct {
		name    string
		payload string
		want    string
	}{
		{name: "text", payload: `{"group":{"kind":"group","id":"1"},"description":"Combinados"}`, want: "Combinados"},
		{name: "an empty string", payload: `{"group":{"kind":"group","id":"1"},"description":""}`, want: ""},
		{name: "null", payload: `{"group":{"kind":"group","id":"1"},"description":null}`, want: ""},
		{name: "nothing at all", payload: `{"group":{"kind":"group","id":"1"}}`, want: ""},
	} {
		t.Run(cleared.name, func(t *testing.T) {
			t.Parallel()
			session := settableSession(t)
			sent := "unset"
			session.setDescription = func(_ context.Context, _ *wm.Client, _ waTypes.JID, description string) error {
				sent = description
				return nil
			}

			if _, err := session.Execute(t.Context(),
				setCommand(t, protocol.CommandGroupDescriptionSet, cleared.payload)); err != nil {
				t.Fatalf("group.description.set: %v", err)
			}
			if sent != cleared.want {
				t.Errorf("WhatsApp was given %q, want %q", sent, cleared.want)
			}
		})
	}
}

// Each switch is a different IQ, and sending the wrong one leaves the group with a setting
// nobody asked for and reports success for the one they did. What this watches is which
// seam the payload reaches, so a `locked` wired under the name `announce` fails here rather
// than in a group somebody is running.
func TestAGroupSettingsChangeFlipsTheSwitchItNames(t *testing.T) {
	t.Parallel()

	for _, setting := range []string{"announce", "locked", "join_approval"} {
		for _, on := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s=%v", setting, on), func(t *testing.T) {
				t.Parallel()
				session := settableSession(t)
				flipped, value := "", !on
				watch := func(named string) func(context.Context, *wm.Client, waTypes.JID, bool) error {
					return func(_ context.Context, _ *wm.Client, group waTypes.JID, to bool) error {
						if group.Server != waTypes.GroupServer {
							t.Errorf("the setting was changed on %s, want a group", group)
						}
						flipped, value = named, to
						return nil
					}
				}
				session.setAnnounce = watch("announce")
				session.setLocked = watch("locked")
				session.setJoinApproval = watch("join_approval")

				payload := fmt.Sprintf(
					`{"group":{"kind":"group","id":"120363000000000001"},"setting":%q,"value":%v}`, setting, on)
				if _, err := session.Execute(t.Context(),
					setCommand(t, protocol.CommandGroupSettingsSet, payload)); err != nil {
					t.Fatalf("group.settings.set: %v", err)
				}
				if flipped != setting {
					t.Errorf("WhatsApp was asked to change %q, want %q", flipped, setting)
				}
				if value != on {
					t.Errorf("%q was set to %v, want %v", setting, value, on)
				}
			})
		}
	}
}

// Every switch the command accepts is wired to a seam. A name accepted by switchFor and
// left nil there would be answered `ok` and reach nothing at all.
func TestEverySwitchThisBuildAcceptsIsWiredUp(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	for _, setting := range []string{"announce", "locked", "join_approval"} {
		if session.switchFor(setting) == nil {
			t.Errorf("%q is accepted from a caller and reaches nothing", setting)
		}
	}
	if session.switchFor("ephemeral") != nil {
		t.Error("a setting this build does not know was routed somewhere anyway")
	}
}

// Who may add members arrives in either of the two spellings the contract allows: the mode
// by name, or the boolean the dashboard sends for "may every member add people".
func TestAMemberAddModeChangeTakesBothSpellings(t *testing.T) {
	t.Parallel()

	for _, asked := range []struct {
		name  string
		value string
		want  waTypes.GroupMemberAddMode
	}{
		{name: "the mode by name", value: `"all_member_add"`, want: waTypes.GroupMemberAddModeAllMember},
		{name: "the other mode by name", value: `"admin_add"`, want: waTypes.GroupMemberAddModeAdmin},
		{name: "everybody may", value: `true`, want: waTypes.GroupMemberAddModeAllMember},
		{name: "only admins may", value: `false`, want: waTypes.GroupMemberAddModeAdmin},
	} {
		t.Run(asked.name, func(t *testing.T) {
			t.Parallel()
			session := settableSession(t)
			var sent waTypes.GroupMemberAddMode
			session.setAddMode = func(
				_ context.Context, _ *wm.Client, _ waTypes.JID, mode waTypes.GroupMemberAddMode,
			) error {
				sent = mode
				return nil
			}

			if _, err := session.Execute(t.Context(), setCommand(t, protocol.CommandGroupSettingsSet,
				`{"group":{"kind":"group","id":"1"},"setting":"member_add_mode","value":`+asked.value+`}`)); err != nil {
				t.Fatalf("group.settings.set: %v", err)
			}
			if sent != asked.want {
				t.Errorf("WhatsApp was given %q, want %q", sent, asked.want)
			}
		})
	}
}

func TestAGroupChangeNeedsAConnection(t *testing.T) {
	t.Parallel()

	for _, command := range []struct {
		kind    protocol.CommandType
		payload string
	}{
		{protocol.CommandGroupNameSet, `{"group":{"kind":"group","id":"1"},"subject":"x"}`},
		{protocol.CommandGroupDescriptionSet, `{"group":{"kind":"group","id":"1"},"description":"x"}`},
		{protocol.CommandGroupSettingsSet, `{"group":{"kind":"group","id":"1"},"setting":"announce","value":true}`},
	} {
		t.Run(string(command.kind), func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setName = func(context.Context, *wm.Client, waTypes.JID, string) error {
				t.Error("a disconnected session changed the group anyway")
				return nil
			}
			session.setDescription = session.setName
			session.setAnnounce = func(context.Context, *wm.Client, waTypes.JID, bool) error {
				t.Error("a disconnected session changed the group anyway")
				return nil
			}

			_, err := session.Execute(t.Context(), setCommand(t, command.kind, command.payload))
			assertCode(t, err, protocol.ErrorNotConnected)
		})
	}
}

// WhatsApp's own refusal, which is the ordinary case: only an admin may change any of
// this, and the contract has no code for "you are not allowed to".
func TestAGroupChangeAnswersWhatsAppsRefusal(t *testing.T) {
	t.Parallel()

	session := settableSession(t)
	session.setName = func(context.Context, *wm.Client, waTypes.JID, string) error {
		return &wm.IQError{Code: 403, Text: "forbidden"}
	}

	_, err := session.Execute(t.Context(), setCommand(t, protocol.CommandGroupNameSet,
		`{"group":{"kind":"group","id":"1"},"subject":"x"}`))
	assertCode(t, err, protocol.ErrorWaError)
}

// Go's json leaves a `null` alone rather than failing on it -- unmarshalling null into a
// bool writes nothing and answers no error -- so a payload carrying no value would read as
// `false` and turn a setting off that nobody asked to turn off. On `member_add_mode`, the
// setting that is not a switch, it would read as `admin_add` and take away every member's
// ability to add people. The contract allows a boolean or a string and nothing else.
func TestAGroupSettingsChangeRefusesAValueThatSaysNothing(t *testing.T) {
	t.Parallel()

	for _, setting := range []string{"announce", "locked", "join_approval", "member_add_mode"} {
		for _, given := range []struct {
			name    string
			payload string
		}{
			{name: "null", payload: `{"group":{"kind":"group","id":"1"},"setting":%q,"value":null}`},
			{name: "no value at all", payload: `{"group":{"kind":"group","id":"1"},"setting":%q}`},
		} {
			t.Run(setting+"/"+given.name, func(t *testing.T) {
				t.Parallel()
				session := settableSession(t)
				session.setAddMode = func(
					context.Context, *wm.Client, waTypes.JID, waTypes.GroupMemberAddMode,
				) error {
					t.Error("a payload that set nothing changed who may add members")
					return nil
				}

				_, err := session.Execute(t.Context(), setCommand(t, protocol.CommandGroupSettingsSet,
					fmt.Sprintf(given.payload, setting)))
				assertCode(t, err, protocol.ErrorInvalidPayload)
			})
		}
	}
}

// A payload that can never be carried out is wrong whether or not the session happens to
// be connected. Answering `not_connected` to it invites a client to wait for a connection
// and then send the same broken command again, and the connection was never the problem.
func TestAGroupChangeSaysWhatIsWrongWithAPayloadEvenWhileDisconnected(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name    string
		kind    protocol.CommandType
		payload string
	}{
		{
			name: "a setting this build does not know", kind: protocol.CommandGroupSettingsSet,
			payload: `{"group":{"kind":"group","id":"1"},"setting":"ephemeral","value":true}`,
		},
		{
			name: "a switch given a mode", kind: protocol.CommandGroupSettingsSet,
			payload: `{"group":{"kind":"group","id":"1"},"setting":"announce","value":"all_member_add"}`,
		},
		{
			name: "no value at all", kind: protocol.CommandGroupSettingsSet,
			payload: `{"group":{"kind":"group","id":"1"},"setting":"announce"}`,
		},
		{
			name: "a mode nobody knows", kind: protocol.CommandGroupSettingsSet,
			payload: `{"group":{"kind":"group","id":"1"},"setting":"member_add_mode","value":"whoever"}`,
		},
		{
			name: "a group with no name to give it", kind: protocol.CommandGroupNameSet,
			payload: `{"group":{"kind":"group","id":"1"},"subject":""}`,
		},
		{
			name: "a chat that is not a group", kind: protocol.CommandGroupDescriptionSet,
			payload: `{"group":{"kind":"phone","id":"5511999990002"},"description":"x"}`,
		},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			// Never connected: the session has an account and no socket, which is the
			// state a client retries out of.
			session, _ := newTestSession(t, "5511999990001")

			_, err := session.Execute(t.Context(), setCommand(t, refused.kind, refused.payload))
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

func leaveCommand(t *testing.T, payload string) *protocol.Command {
	t.Helper()
	return setCommand(t, protocol.CommandGroupLeave, payload)
}

func TestLeavingRefusesAPayloadThatNamesNoGroup(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name    string
		payload string
	}{
		{name: "no payload at all", payload: `{}`},
		{name: "a group with no id", payload: `{"group":{"kind":"group","id":""}}`},
		// Leaving a direct chat is not a thing, and the address is how a caller says
		// which group it means. Sending this on would have WhatsApp refuse a group IQ
		// addressed to a person, which reaches the client as an opaque `wa_error`.
		{name: "a chat that is not a group", payload: `{"group":{"kind":"phone","id":"5511999990002"}}`},
		{name: "a newsletter", payload: `{"group":{"kind":"newsletter","id":"120363000000000001"}}`},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.leave = func(context.Context, *wm.Client, waTypes.JID) error {
				t.Error("a payload that names no group left one anyway")
				return nil
			}

			_, err := session.Execute(t.Context(), leaveCommand(t, refused.payload))
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

func TestLeavingLeavesTheGroupItWasGiven(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	left := waTypes.EmptyJID
	session.leave = func(_ context.Context, _ *wm.Client, group waTypes.JID) error {
		left = group
		return nil
	}

	result, err := session.Execute(t.Context(), leaveCommand(t,
		`{"group":{"kind":"group","id":"120363000000000001"}}`))
	if err != nil {
		t.Fatalf("group.leave: %v", err)
	}
	if left.Server != waTypes.GroupServer || left.User != "120363000000000001" {
		t.Errorf("the session left %s, want the group that was named", left)
	}
	// The contract's table says this command answers null: the caller is waiting for the
	// confirmation, not for data.
	if result != nil {
		t.Errorf("the answer carries %s, want nothing", result)
	}
}

// Leaving cannot be undone from this side, so a session with no socket must not report
// that it left. A client told `ok` takes the group out of its own list, and the account is
// still in it.
func TestLeavingNeedsAConnection(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.leave = func(context.Context, *wm.Client, waTypes.JID) error {
		t.Error("a disconnected session left a group anyway")
		return nil
	}

	_, err := session.Execute(t.Context(), leaveCommand(t, `{"group":{"kind":"group","id":"1"}}`))
	assertCode(t, err, protocol.ErrorNotConnected)
}

// WhatsApp's own refusal reaches the client as a code from the contract, never as
// whatsmeow's own error.
//
// The refusals are IQ errors and not whatsmeow's named sentinels, which is what `LeaveGroup`
// actually answers with: it hands back what `sendGroupIQ` returned, and the two sentinels
// that mean "not in that group" are attached by two getters this command is not one of.
// Writing the test against those would be testing a case the API cannot produce.
func TestLeavingAnswersWhatsAppsRefusal(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name string
		err  error
		want protocol.ErrorCode
	}{
		{name: "not in the group", err: &wm.IQError{Code: 403, Text: "forbidden"}, want: protocol.ErrorWaError},
		{name: "no such group", err: &wm.IQError{Code: 404, Text: "item-not-found"}, want: protocol.ErrorWaError},
		{name: "the connection went", err: wm.ErrIQDisconnected, want: protocol.ErrorNotConnected},
		{name: "rate limited", err: &wm.IQError{Code: 429}, want: protocol.ErrorRateLimited},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.leave = func(context.Context, *wm.Client, waTypes.JID) error {
				return refused.err
			}

			_, err := session.Execute(t.Context(), leaveCommand(t, `{"group":{"kind":"group","id":"1"}}`))
			assertCode(t, err, refused.want)
			if errors.Is(err, refused.err) {
				t.Error("whatsmeow's own error reached the client instead of a code from the contract")
			}
		})
	}
}

func photoCommand(t *testing.T, payload string) *protocol.Command {
	t.Helper()
	return setCommand(t, protocol.CommandGroupPhotoSet, payload)
}

// A one-pixel JPEG, which is what a real payload carries: base64 of actual bytes.
const aTinyJPEG = "/9j/4AAQSkZJRgABAQEAYABgAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkSEw8UHRofHh0a" +
	"HBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/wAALCAABAAEBAREA/8QAFAABAAAAAAAA" +
	"AAAAAAAAAAAACf/EABQQAQAAAAAAAAAAAAAAAAAAAAD/2gAIAQEAAD8AKp//2Q=="

func TestAPhotoChangeRefusesAPayloadItCannotCarryOut(t *testing.T) {
	t.Parallel()

	for _, refused := range []struct {
		name    string
		payload string
	}{
		{name: "no payload at all", payload: `{}`},
		{name: "a chat that is not a group", payload: `{"group":{"kind":"phone","id":"5511999990002"},"image":null}`},
		// Absent is not null. Null is "take the picture off", which somebody asked for;
		// absent is a payload that never said, and removing on it deletes a group's photo
		// because a caller forgot a field.

		{name: "an image that is not base64", payload: `{"group":{"kind":"group","id":"1"},"image":"not base64!!"}`},
		{name: "base64 that decodes to nothing", payload: `{"group":{"kind":"group","id":"1"},"image":""}`},
		{name: "an image that is not text at all", payload: `{"group":{"kind":"group","id":"1"},"image":42}`},
	} {
		t.Run(refused.name, func(t *testing.T) {
			t.Parallel()
			session := settableSession(t)
			session.setPhoto = func(context.Context, *wm.Client, waTypes.JID, []byte) error {
				t.Error("a payload that says nothing about a picture changed one anyway")
				return nil
			}

			_, err := session.Execute(t.Context(), photoCommand(t, refused.payload))
			assertCode(t, err, protocol.ErrorInvalidPayload)
		})
	}
}

// Absent is not null, and the two are one keystroke apart in a client. Null is "take the
// picture off"; absent is a payload that never said, and carrying that out as a removal
// deletes a group's photo because a caller forgot a field. The refusal has to say which of
// the two it is, or the client cannot tell a bug in its payload from a rejected image.
func TestAPhotoChangeSaysAMissingImageIsNotARemoval(t *testing.T) {
	t.Parallel()

	session := settableSession(t)
	session.setPhoto = func(context.Context, *wm.Client, waTypes.JID, []byte) error {
		t.Error("a payload with no image field removed the group's photo")
		return nil
	}

	_, err := session.Execute(t.Context(), photoCommand(t, `{"group":{"kind":"group","id":"1"}}`))
	assertCode(t, err, protocol.ErrorInvalidPayload)
	if !strings.Contains(err.Error(), "or null to remove") {
		t.Errorf("the refusal reads %q, want it to point at null as the way to remove", err)
	}
}

func TestAPhotoChangeSendsTheBytesItWasGiven(t *testing.T) {
	t.Parallel()

	session := settableSession(t)
	var sent []byte
	session.setPhoto = func(_ context.Context, _ *wm.Client, group waTypes.JID, picture []byte) error {
		if group.Server != waTypes.GroupServer || group.User != "120363000000000001" {
			t.Errorf("the photo was set on %s, want the group that was named", group)
		}
		sent = picture
		return nil
	}

	result, err := session.Execute(t.Context(), photoCommand(t,
		`{"group":{"kind":"group","id":"120363000000000001"},"image":"`+aTinyJPEG+`"}`))
	if err != nil {
		t.Fatalf("group.photo.set: %v", err)
	}
	// Decoded, not passed through as text: WhatsApp is handed the picture, not its
	// spelling.
	if len(sent) < 100 || sent[0] != 0xFF || sent[1] != 0xD8 {
		t.Errorf("WhatsApp was given %d bytes starting %x, want the decoded JPEG", len(sent), sent[:min(2, len(sent))])
	}
	if result != nil {
		t.Errorf("the answer carries %s, want nothing", result)
	}
}

// Null is how a group's picture comes off, and whatsmeow reads a nil avatar as the
// removal. An empty string says the same thing and must not reach WhatsApp as a picture
// element with no picture in it.
func TestAPhotoChangeRemovesTheePictureWithNothingInIt(t *testing.T) {
	t.Parallel()

	session := settableSession(t)
	removed := false
	session.setPhoto = func(_ context.Context, _ *wm.Client, _ waTypes.JID, picture []byte) error {
		removed = true
		if picture != nil {
			t.Errorf("WhatsApp was given %d bytes, want nothing at all", len(picture))
		}
		return nil
	}

	if _, err := session.Execute(t.Context(),
		photoCommand(t, `{"group":{"kind":"group","id":"1"},"image":null}`)); err != nil {
		t.Fatalf("group.photo.set: %v", err)
	}
	if !removed {
		t.Error("a null image did not reach WhatsApp at all")
	}
}

func TestAPhotoChangeNeedsAConnection(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setPhoto = func(context.Context, *wm.Client, waTypes.JID, []byte) error {
		t.Error("a disconnected session changed a group's photo anyway")
		return nil
	}

	_, err := session.Execute(t.Context(), photoCommand(t,
		`{"group":{"kind":"group","id":"1"},"image":null}`))
	assertCode(t, err, protocol.ErrorNotConnected)
}

func TestAPhotoChangeAnswersWhatsAppsRefusal(t *testing.T) {
	t.Parallel()

	session := settableSession(t)
	session.setPhoto = func(context.Context, *wm.Client, waTypes.JID, []byte) error {
		return &wm.IQError{Code: 403, Text: "forbidden"}
	}

	_, err := session.Execute(t.Context(), photoCommand(t,
		`{"group":{"kind":"group","id":"1"},"image":null}`))
	assertCode(t, err, protocol.ErrorWaError)
}
