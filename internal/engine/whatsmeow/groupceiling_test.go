package whatsmeow

import (
	"context"
	"errors"
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
		"a description":    {Type: protocol.CommandGroupDescriptionSet, Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"description":"x"}`)},
		"asking about one": {Type: protocol.CommandGroupInfo, Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"}}`)},
		"listing them":     {Type: protocol.CommandGroupList, Payload: []byte(`{}`)},
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
	session.setDescription = func(ctx context.Context, _ *wm.Client, _ waTypes.JID, _, _ string) error {
		return record(ctx)
	}
	session.setAnnounce = func(ctx context.Context, _ *wm.Client, _ waTypes.JID, _ bool) error {
		return record(ctx)
	}
	session.joinedGroups = func(ctx context.Context, _ *wm.Client) ([]*waTypes.GroupInfo, error) {
		return nil, record(ctx)
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

	for name, test := range map[string]struct {
		command *protocol.Command
		bounded bool
	}{
		"creating a group": {&protocol.Command{Type: protocol.CommandGroupCreate}, false},
		// Answered a second time with "not in the group" or "no such request", for work
		// the first attempt did.
		"leaving one":              {&protocol.Command{Type: protocol.CommandGroupLeave}, false},
		"changing who is in":       {&protocol.Command{Type: protocol.CommandGroupParticipantsUpdate}, false},
		"answering a join request": {&protocol.Command{Type: protocol.CommandGroupJoinRequestsUpdate}, false},
		// No id to write twice under, so a retry publishes a second group.updated.
		"a name change":     {&protocol.Command{Type: protocol.CommandGroupNameSet}, false},
		"a settings change": {&protocol.Command{Type: protocol.CommandGroupSettingsSet}, false},
		// Reads: asked again they answer again.
		"listing groups":        {&protocol.Command{Type: protocol.CommandGroupList}, true},
		"listing join requests": {&protocol.Command{Type: protocol.CommandGroupJoinRequestsList}, true},
		// WhatsApp assigns a new picture id and announces another change, and there is no
		// id this side can hand it to make a second write the first one over again.
		"setting a photo": {&protocol.Command{Type: protocol.CommandGroupPhotoSet}, false},
		"rotating an invite": {&protocol.Command{
			Type: protocol.CommandGroupInviteGet, Payload: []byte(`{"revoke":true}`),
		}, false},
		// Reading the link changes nothing, so there is nothing a retry could repeat and
		// no reason for it to hold the account's queue for seventy-five seconds.
		"reading an invite": {&protocol.Command{
			Type: protocol.CommandGroupInviteGet, Payload: []byte(`{"revoke":false}`),
		}, true},
		"reading an invite without saying so": {&protocol.Command{
			Type: protocol.CommandGroupInviteGet, Payload: []byte(`{}`),
		}, true},
		// An unreadable payload is not a question, and the safe reading is the one that
		// costs nothing when it is wrong.
		"an invite whose payload will not parse": {&protocol.Command{
			Type: protocol.CommandGroupInviteGet, Payload: []byte(`{`),
		}, false},
		"changing a description": {&protocol.Command{Type: protocol.CommandGroupDescriptionSet}, true},
		"asking about a group":   {&protocol.Command{Type: protocol.CommandGroupInfo}, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := boundedGroupCommand(test.command); got != test.bounded {
				t.Errorf("under the ceiling = %v, want %v", got, test.bounded)
			}
		})
	}
}

// The read that comes before the write, and what happens when it fails. Swallowing it
// answers the caller that the description was written over a group nobody could even read,
// and the operator then sees the old text on the next refresh with nothing having said no.
func TestADescriptionIsNotReportedWrittenWhenTheGroupCouldNotBeRead(t *testing.T) {
	t.Parallel()

	for name, refusal := range map[string]error{
		"the connection went":     wm.ErrIQDisconnected,
		"whatsapp never answered": wm.ErrIQTimedOut,
		"whatsapp refused":        &wm.IQError{Code: 403},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
				return nil, refusal
			}

			_, err := session.Execute(t.Context(), &protocol.Command{
				Type:    protocol.CommandGroupDescriptionSet,
				Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"description":"x"}`),
			})
			if err == nil {
				t.Fatal("a description was reported written over a group that could not be read")
			}
		})
	}
}

// The ceiling is applied by command and not to the block, so the two that cannot be
// repeated have to be watched arriving without one rather than only listed as absent from
// a map. A map is a claim about a map.
func TestTheTwoThatCannotBeRepeatedArriveWithNoCeiling(t *testing.T) {
	t.Parallel()

	for name, command := range map[string]*protocol.Command{
		"creating a group":   {Type: protocol.CommandGroupCreate, Payload: []byte(`{"subject":"x","participants":[{"kind":"phone","id":"5511999990002"}]}`)},
		"rotating an invite": {Type: protocol.CommandGroupInviteGet, Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"revoke":true}`)},
		"setting a photo":    {Type: protocol.CommandGroupPhotoSet, Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"image":"` + aTinyJPEG + `"}`)},
		"leaving one":        {Type: protocol.CommandGroupLeave, Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"}}`)},
		"a name change":      {Type: protocol.CommandGroupNameSet, Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"subject":"x"}`)},
		"a settings change":  {Type: protocol.CommandGroupSettingsSet, Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"setting":"announce","value":true}`)},
		"changing who is in": {Type: protocol.CommandGroupParticipantsUpdate, Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"action":"add","participants":[{"kind":"phone","id":"5511999990002"}]}`)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			var left time.Duration
			var set bool
			session.createTheGroup = func(ctx context.Context, _ *wm.Client, _ wm.ReqCreateGroup) (*waTypes.GroupInfo, error) {
				if until, ok := ctx.Deadline(); ok {
					left, set = time.Until(until), true
				}
				return nil, errors.New("recorded")
			}
			session.inviteLink = func(ctx context.Context, _ *wm.Client, _ waTypes.JID, _ bool) (string, error) {
				if until, ok := ctx.Deadline(); ok {
					left, set = time.Until(until), true
				}
				return "", errors.New("recorded")
			}
			session.setPhoto = func(ctx context.Context, _ *wm.Client, _ waTypes.JID, _ []byte) error {
				if until, ok := ctx.Deadline(); ok {
					left, set = time.Until(until), true
				}
				return errors.New("recorded")
			}
			session.leave = func(ctx context.Context, _ *wm.Client, _ waTypes.JID) error {
				if until, ok := ctx.Deadline(); ok {
					left, set = time.Until(until), true
				}
				return errors.New("recorded")
			}
			session.setName = func(ctx context.Context, _ *wm.Client, _ waTypes.JID, _ string) error {
				if until, ok := ctx.Deadline(); ok {
					left, set = time.Until(until), true
				}
				return errors.New("recorded")
			}
			session.setAnnounce = func(ctx context.Context, _ *wm.Client, _ waTypes.JID, _ bool) error {
				if until, ok := ctx.Deadline(); ok {
					left, set = time.Until(until), true
				}
				return errors.New("recorded")
			}
			session.updateParticipants = func(
				ctx context.Context, _ *wm.Client, _ waTypes.JID, _ []waTypes.JID, _ wm.ParticipantChange,
			) ([]waTypes.GroupParticipant, error) {
				if until, ok := ctx.Deadline(); ok {
					left, set = time.Until(until), true
				}
				return nil, errors.New("recorded")
			}

			_, _ = session.Execute(t.Context(), command)

			if set && left <= 20*time.Second {
				t.Errorf("it was given %s, and WhatsApp applying it after that is a side effect a retry repeats", left)
			}
		})
	}
}

// The revision a description is written under. A redelivery has to write the same one:
// whatsmeow generates a fresh id when handed an empty one, so two attempts at one command
// would be two revisions of the group's description, and the second publishes a
// `group.updated` nobody asked for. Two instances editing the same group under the same
// caller-supplied key are two commands, though, and must not collide.
func TestARedeliveredDescriptionIsWrittenUnderTheSameRevision(t *testing.T) {
	t.Parallel()

	here, _ := newTestSession(t, "5511999990001")
	command := &protocol.Command{Type: protocol.CommandGroupDescriptionSet, ID: "c1", IdempotencyKey: "desc-42"}

	first := here.orDerived(command, "")
	again := here.orDerived(&protocol.Command{
		Type: protocol.CommandGroupDescriptionSet, ID: "c2", IdempotencyKey: "desc-42",
	}, "")
	if first != again {
		t.Errorf("the same command written twice got %q and then %q", first, again)
	}
	if other := here.orDerived(&protocol.Command{
		Type: protocol.CommandGroupDescriptionSet, ID: "c3", IdempotencyKey: "desc-43",
	}, ""); other == first {
		t.Errorf("two different commands share the revision %q", other)
	}

	// The ledger is keyed by session, so the same key on another instance is another
	// command. Handing WhatsApp one revision for both would have it read the second
	// instance's edit as a replay of the first's.
	// And the command actually reaches WhatsApp under it. Asserting `orDerived` alone is
	// an assertion about `orDerived`: the mutation that stops passing it survives that.
	written := make([]string, 0, 2)
	here.setConnected(true)
	here.setDescription = func(_ context.Context, _ *wm.Client, _ waTypes.JID, _, revision string) error {
		written = append(written, revision)
		return nil
	}
	payload := []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"description":"x"}`)
	for _, id := range []string{"c1", "c2"} {
		if _, err := here.Execute(t.Context(), &protocol.Command{
			Type: protocol.CommandGroupDescriptionSet, ID: id,
			IdempotencyKey: "desc-42", Payload: payload,
		}); err != nil {
			t.Fatalf("group.description.set: %v", err)
		}
	}
	if written[0] == "" {
		t.Error("the description went out under no revision at all")
	}
	if written[0] != written[1] {
		t.Errorf("a redelivery went out under %q where the first went out under %q", written[1], written[0])
	}

	elsewhere, _ := newTestSession(t, "5511999990002")
	// Two test sessions built in one test share a sid, which is what they are keyed by:
	// named apart here so what is compared is two instances rather than one twice.
	elsewhere.sid = "sid-another-instance"
	if theirs := elsewhere.orDerived(command, ""); theirs == first {
		t.Errorf("two sessions editing one group share the revision %q", theirs)
	}
}
