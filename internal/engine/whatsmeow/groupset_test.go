package whatsmeow

import (
	"context"
	"encoding/json"
	"fmt"
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
