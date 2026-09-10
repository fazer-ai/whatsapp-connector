package whatsmeow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	wm "go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

const theGroup = "120363041234567890"

// Every group command is an info query, and whatsmeow gives an info query 75 seconds. The
// session executor is serial, so one WhatsApp decides not to answer costs the account
// every message and receipt queued behind it for that whole time, which is what #163
// measured four times over a description that could not be removed.
func TestAGroupCommandDoesNotWaitOutAnInfoQuery(t *testing.T) {
	t.Parallel()

	for name, command := range map[string]*protocol.Command{
		"a name change":      {Type: protocol.CommandGroupNameSet, Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"subject":"x"}`)},
		"a description":      {Type: protocol.CommandGroupDescriptionSet, Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"description":"x"}`)},
		"a settings change":  {Type: protocol.CommandGroupSettingsSet, Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"setting":"announce","value":true}`)},
		"asking about one":   {Type: protocol.CommandGroupInfo, Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"}}`)},
		"leaving one":        {Type: protocol.CommandGroupLeave, Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"}}`)},
		"changing who is in": {Type: protocol.CommandGroupParticipantsUpdate, Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"action":"add","participants":[{"kind":"phone","id":"5511999990002"}]}`)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			deadline := seamDeadline(t, session)

			_, _ = session.Execute(t.Context(), command)

			left, set := deadline()
			if !set {
				t.Fatal("the command reached WhatsApp with no deadline on it at all")
			}
			// Written as a number rather than as `groupIQWait` on purpose: a test that
			// asserts the code's own constant passes whatever the constant is, including
			// the seventy-five seconds this exists to rule out. Twenty seconds is the
			// ceiling the acceptance scenario names.
			if left > 20*time.Second {
				t.Errorf("the command was given %s before WhatsApp had to answer", left)
			}
		})
	}
}

// A caller that wants to wait less than this connector does still waits less: the ceiling
// bounds a wait, it does not extend one.
func TestACallerWhoWantsLessTimeGetsIt(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	deadline := seamDeadline(t, session)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, _ = session.Execute(ctx, &protocol.Command{
		Type:    protocol.CommandGroupNameSet,
		Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"subject":"x"}`),
	})

	left, set := deadline()
	if !set {
		t.Fatal("the command reached WhatsApp with no deadline on it at all")
	}
	if left > 2*time.Second {
		t.Errorf("a caller who asked for a second was given %s", left)
	}
}

// seamDeadline replaces every group seam with one that records how long the command was
// given, so what is asserted is the deadline the seam was handed rather than a test that
// waits for one to pass.
func seamDeadline(t *testing.T, session *Session) func() (time.Duration, bool) {
	t.Helper()

	var left time.Duration
	var set bool
	record := func(ctx context.Context) error {
		if until, ok := ctx.Deadline(); ok {
			left, set = time.Until(until), true
		}
		return errors.New("recorded")
	}
	session.setName = func(ctx context.Context, _ *wm.Client, _ waTypes.JID, _ string) error {
		return record(ctx)
	}
	session.setDescription = func(ctx context.Context, _ *wm.Client, _ waTypes.JID, _ string) error {
		return record(ctx)
	}
	session.setAnnounce = func(ctx context.Context, _ *wm.Client, _ waTypes.JID, _ bool) error {
		return record(ctx)
	}
	session.leave = func(ctx context.Context, _ *wm.Client, _ waTypes.JID) error {
		return record(ctx)
	}
	session.groupInfo = func(ctx context.Context, _ *wm.Client, _ waTypes.JID) (*waTypes.GroupInfo, error) {
		return nil, record(ctx)
	}
	session.updateParticipants = func(
		ctx context.Context, _ *wm.Client, _ waTypes.JID, _ []waTypes.JID, _ wm.ParticipantChange,
	) ([]waTypes.GroupParticipant, error) {
		return nil, record(ctx)
	}
	return func() (time.Duration, bool) { return left, set }
}

// Which description has to be written the old way. Split out from the call itself because
// the call takes a live client: what is decidable without a socket is the decision, and it
// is the decision that has to be narrow.
func TestOnlyADescriptionWithNoIDIsWrittenTheOldWay(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		topicID     string
		description string
		want        bool
	}{
		"a write over a description with no id": {unaddressableTopicID, "novo texto", true},
		// The one that matters most: going the other way on a removal is the 75-second
		// wait. A group stuck like this is told 409 in a third of a second instead.
		"a removal over a description with no id":     {unaddressableTopicID, "", false},
		"a write over a description with a real id":   {"3EB0C2A14F4FBC421B2E8C", "novo texto", false},
		"a removal over a description with a real id": {"3EB0C2A14F4FBC421B2E8C", "", false},
		// A group that never had a description: nothing to name, and nothing refuses it.
		"a write over no description":   {"", "novo texto", false},
		"a removal over no description": {"", "", false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := legacyDescription(test.topicID, test.description); got != test.want {
				t.Errorf("writing the old way = %v, want %v", got, test.want)
			}
		})
	}
}

// The ceiling protects the account's queue by giving up on a command, and that trade is
// only available where giving up costs an answer. `group.create` makes another group every
// time it runs, and a revoking `group.invite.get` rotates the link again; WhatsApp does not
// undo either because this side stopped waiting, and the ledger records only successes. So
// one of those answered `timeout` here and retried under the same idempotency key is
// invariant 5 broken by the fix for #163.
func TestACommandThatCannotBeRepeatedKeepsItsFullWait(t *testing.T) {
	t.Parallel()

	cannotRepeat := map[protocol.CommandType]bool{
		protocol.CommandGroupCreate:    true,
		protocol.CommandGroupInviteGet: true,
	}
	for command := range cannotRepeat {
		if repeatableGroupCommands[command] {
			t.Errorf("%s is under the ceiling, and a retry of it duplicates what it did", command)
		}
	}
	// The fence, in the shape internal/protocol already uses for the event catalogue: a
	// group command added later is under the ceiling or is named as one that cannot be,
	// and never neither because nobody came back here.
	for _, command := range protocol.AllCommandTypes {
		if !strings.HasPrefix(string(command), "group.") {
			continue
		}
		if !repeatableGroupCommands[command] && !cannotRepeat[command] {
			t.Errorf("%s is neither under the ceiling nor named as one that cannot be", command)
		}
	}
}
