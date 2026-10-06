package whatsmeow

import (
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
	waTypes "go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
)

func groupModeOf(session *Session, group waTypes.JID) (waTypes.AddressingMode, bool) {
	session.mu.Lock()
	defer session.mu.Unlock()
	mode, known := session.groupModes[group]
	return mode, known
}

// A change to a group is the moment its addressing is read again: the next action in it
// asks WhatsApp instead of answering out of what this connection read first. No event says
// a group moved from phone numbers to LIDs, but every notification about the group is one
// whatsmeow hands over, a change it cannot parse included, and before this a reading lasted
// until the next reconnection however many came (#125). Only that group: the others keep
// what was read about them.
func TestAGroupChangeForgetsThatGroupsAddressing(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		change *waEvents.GroupInfo
		wanted bool
	}{
		{"a rename", &waEvents.GroupInfo{Name: &waTypes.GroupName{Name: "renamed"}}, true},
		{"a change whatsmeow could not parse", &waEvents.GroupInfo{UnknownChanges: []*waBinary.Node{{Tag: "addressing_mode"}}}, true},
		{"a change in a group the client did not ask about", &waEvents.GroupInfo{Name: &waTypes.GroupName{Name: "renamed"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			session := groupSession(t)
			session.setGroups(tc.wanted)
			other := waTypes.NewJID("120363000000000099", waTypes.GroupServer)
			session.mu.Lock()
			session.groupModes = map[waTypes.JID]waTypes.AddressingMode{
				groupJID(): waTypes.AddressingModePN,
				other:      waTypes.AddressingModePN,
			}
			session.mu.Unlock()

			change := *tc.change
			change.JID = groupJID()
			session.handle(&change)

			if mode, known := groupModeOf(session, groupJID()); known {
				t.Errorf("the changed group is still answered from memory as %q", mode)
			}
			if _, known := groupModeOf(session, other); !known {
				t.Error("a change to one group forgot what was read about another")
			}
		})
	}
}
