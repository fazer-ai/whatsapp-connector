package whatsmeow

import (
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// Every library call that writes on behalf of a command the ledger reserves marks what it
// returns, and the fence reads the source rather than trusting that it was remembered.
//
// This is what makes #282 work at all. The layer above keeps an attempt standing only for a
// marked failure, so a write whose failure comes back bare is a write a redelivery carries
// out a second time -- the defect that issue is about, restored quietly and in one command
// at a time. A behavioural test per command would cover the ones somebody thought of; this
// covers the ones they did not, and fails when a pin renames a method or a change adds a
// write beside an existing one.
//
// The table is keyed by command rather than by call for one reason: a list of calls is a
// list somebody wrote down, and the writes it does not name are invisible to it. Keyed by
// command, the key set is compared against protocol.ReservedCommands below, so a command
// that reserves an attempt and writes through a call nobody listed fails here instead of
// passing for the want of a row. That is not hypothetical: the first version of this fence
// listed eleven calls and missed both SendAppState, which is how `message.mark_unread`
// writes, and SetGroupMemberAddMode, one of the four writes `group.settings_set` chooses
// between.
func TestEveryWriteAReservedCommandMakesIsMarked(t *testing.T) {
	t.Parallel()

	type write struct {
		call string // the whatsmeow method the command ends in
		file string // the file this package makes it from
	}
	writes := map[protocol.CommandType][]write{
		protocol.CommandPresenceSet:             {{"SendPresence", "session.go"}},
		protocol.CommandMessageMarkRead:         {{"MarkRead", "receipt.go"}},
		protocol.CommandMessageMarkUnread:       {{"SendAppState", "unread.go"}},
		protocol.CommandGroupParticipantsUpdate: {{"UpdateGroupParticipants", "session.go"}},
		protocol.CommandGroupJoinRequestsUpdate: {{"UpdateGroupRequestParticipants", "session.go"}},
		protocol.CommandGroupNameSet:            {{"SetGroupName", "session.go"}},
		protocol.CommandGroupDescriptionSet:     {{"SetGroupTopic", "session.go"}},
		protocol.CommandGroupPhotoSet:           {{"SetGroupPhoto", "session.go"}},
		protocol.CommandGroupInviteGet:          {{"GetGroupInviteLink", "session.go"}},
		protocol.CommandGroupSettingsSet: {
			{"SetGroupAnnounce", "session.go"},
			{"SetGroupLocked", "session.go"},
			{"SetGroupJoinApprovalMode", "session.go"},
			{"SetGroupMemberAddMode", "session.go"},
		},
	}

	sources := map[string]string{}
	read := func(t *testing.T, name string) string {
		t.Helper()
		if body, ok := sources[name]; ok {
			return body
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		sources[name] = string(body)
		return string(body)
	}

	t.Run("the table covers exactly the commands the ledger reserves", func(t *testing.T) {
		for command := range protocol.ReservedCommands {
			if _, ok := writes[command]; !ok {
				t.Errorf("%s reserves an attempt and this fence names no write for it, so "+
					"nothing here checks that its failure is marked. Add the library call it "+
					"ends in, or take it out of protocol.ReservedCommands with the reason",
					command)
			}
		}
		for command := range writes {
			if !protocol.ReservedCommands[command] {
				t.Errorf("%s is listed here but reserves no attempt, so the mark on its write "+
					"buys nothing: either it should be reserved, or this row is stale", command)
			}
		}
	})

	for command, made := range writes {
		t.Run(string(command), func(t *testing.T) {
			t.Parallel()

			for _, w := range made {
				source := read(t, w.file)
				at := regexp.MustCompile(`\.`+w.call+`\(`).FindAllStringIndex(source, -1)
				if len(at) == 0 {
					t.Errorf("%s makes no call to %s any more: either %s stopped writing "+
						"through it, or the pin renamed it. Re-read which library call the "+
						"command ends in before editing this list", w.file, w.call, command)
					continue
				}
				for _, made := range at {
					// The mark belongs on what is returned, so the rule is the first `return`
					// at or after the call: a path that hands the failure back bare fails here
					// even when a mark sits somewhere else in the same function. Scoping to the
					// function instead would pass a second, unmarked write beside a marked one.
					returned, found := firstReturnAt(source, made[0])
					switch {
					case !found:
						t.Errorf("the call to %s in %s is not followed by a return, so this "+
							"fence cannot see what it hands back", w.call, w.file)
					case !regexp.MustCompile(`engine\.MayHaveLanded\(`).MatchString(returned):
						t.Errorf("the failure of %s in %s is returned unmarked:\n\t%s\n"+
							"A write whose failure comes back bare releases the attempt %s "+
							"reserved, so a redelivery carries it out a second time (#282)",
							w.call, w.file, strings.TrimSpace(returned), command)
					}
				}
			}
		})
	}
}

// firstReturnAt is the first line from the call onwards that returns something. It is what
// the caller of the write hands up, and therefore the only place a mark counts.
func firstReturnAt(source string, at int) (string, bool) {
	if line := lineAround(source, at); strings.HasPrefix(strings.TrimSpace(line), "return") {
		return line, true
	}
	for i := at; i < len(source); i++ {
		if source[i] != '\n' {
			continue
		}
		line := lineAround(source, min(i+1, len(source)-1))
		if strings.HasPrefix(strings.TrimSpace(line), "return") {
			return line, true
		}
	}
	line := lineAround(source, at)
	if strings.Contains(line, "return") {
		return line, true
	}
	return "", false
}

// lineAround returns the source line the offset falls on.
func lineAround(source string, at int) string {
	start := 0
	for i := at; i > 0; i-- {
		if source[i-1] == '\n' {
			start = i
			break
		}
	}
	end := len(source)
	for i := at; i < len(source); i++ {
		if source[i] == '\n' {
			end = i
			break
		}
	}
	return source[start:end]
}

// And a refusal decided before the socket carries no mark, which is how the retry of a
// command that touched nothing gets to run.
func TestARefusalMadeBeforeTheSocketIsNotMarked(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		what  string
		setUp func(t *testing.T) *Session
		want  protocol.ErrorCode
	}{
		{
			what: "a session with no account",
			setUp: func(t *testing.T) *Session {
				session, _ := newTestSession(t, "")
				return session
			},
			want: protocol.ErrorNotPaired,
		},
		{
			what: "a session whose socket is not open",
			setUp: func(t *testing.T) *Session {
				session, _ := newTestSession(t, "5511999990001")
				session.setConnected(false)
				return session
			},
			want: protocol.ErrorNotConnected,
		},
	} {
		t.Run(tc.what, func(t *testing.T) {
			t.Parallel()

			err := tc.setUp(t).readyToSend()
			if err == nil {
				t.Fatal("the pre-flight let the command through")
			}
			if got := codeOf(err); got != tc.want {
				t.Errorf("the client is told %q, want %q", got, tc.want)
			}
			if errors.Is(err, engine.ErrMayHaveLanded) {
				t.Errorf("a refusal decided before the socket is marked as a write that may "+
					"have landed, so the command's key is held against a retry that has the "+
					"whole thing to do: %s", tc.what)
			}
		})
	}
}

// The mark is for the ledger and the message is for the client.
func TestTheMarkDoesNotReachWhatTheClientIsTold(t *testing.T) {
	t.Parallel()

	went := errors.New("WhatsApp did not take the read mark")
	marked := engine.MayHaveLanded(went)

	if !errors.Is(marked, engine.ErrMayHaveLanded) {
		t.Error("the marked error does not carry engine.ErrMayHaveLanded")
	}
	if !errors.Is(marked, went) {
		t.Error("the marked error no longer unwraps to what went wrong")
	}
	if got := marked.Error(); got != went.Error() {
		t.Errorf("the marked error reads %q and the failure reads %q", got, went.Error())
	}
	if engine.MayHaveLanded(nil) != nil {
		t.Error("marking nothing produced an error")
	}
}
