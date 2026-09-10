package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"runtime"
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
		// Not decided here: which call it needs is known only after the group is read, so
		// `writeTheDescription` bounds the half that carries a revision and leaves the
		// legacy half alone.
		"changing a description": {&protocol.Command{Type: protocol.CommandGroupDescriptionSet}, false},
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

// Both halves of a description change are under the ceiling, and under one budget between
// them. The read is bounded because asked again it answers again; the write is bounded
// because it goes out under a revision this side chose, so WhatsApp committing it after
// the wait was given up on and the caller redelivering writes that same revision again
// rather than a second one.
func TestBothHalvesOfADescriptionAreBounded(t *testing.T) {
	t.Parallel()

	for name, topicID := range map[string]string{
		"a description that can carry a revision": "3EB0C2A14F4FBC421B2E8C",
		"a group that never had one":              "",
		// Frozen: written before this connector gave a description an id. WhatsApp
		// refuses every change to it, and the ceiling is what makes that refusal cost a
		// third of a second instead of seventy-five.
		"a description with no id": "undefined",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			var lookedUnder time.Duration
			var lookBounded bool
			session.groupInfo = func(ctx context.Context, _ *wm.Client, _ waTypes.JID) (*waTypes.GroupInfo, error) {
				if until, ok := ctx.Deadline(); ok {
					lookedUnder, lookBounded = time.Until(until), true
				}
				return &waTypes.GroupInfo{GroupTopic: waTypes.GroupTopic{TopicID: topicID}}, nil
			}
			var left time.Duration
			var set bool
			session.setTopic = func(ctx context.Context, _ *wm.Client, _ waTypes.JID, _, _, _ string) error {
				if until, ok := ctx.Deadline(); ok {
					left, set = time.Until(until), true
				}
				return &wm.IQError{Code: 409}
			}

			_, _ = session.Execute(t.Context(), &protocol.Command{
				Type:    protocol.CommandGroupDescriptionSet,
				Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"description":"x"}`),
			})

			if !lookBounded || lookedUnder > 20*time.Second {
				t.Errorf("the group was read with no ceiling (set=%v, left=%s)", lookBounded, lookedUnder)
			}
			if !set || left > 20*time.Second {
				t.Errorf("the description was written with no ceiling (set=%v, left=%s)", set, left)
			}
		})
	}
}

// One ceiling for the command, not one per query. `group.description.set` is a read and
// then a write, and giving each of them fifteen seconds holds the session's serial
// executor for thirty -- the thing the ceiling exists to prevent, reached by applying the
// prevention twice. What separates one budget from two is the instant they expire at: from
// one budget both queries expire together, and from two the second expires later by
// whatever the first one spent.
func TestADescriptionGetsOneCeilingAndNotOnePerQuery(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)

	var readUntil, writeUntil time.Time
	session.groupInfo = func(ctx context.Context, _ *wm.Client, _ waTypes.JID) (*waTypes.GroupInfo, error) {
		readUntil, _ = ctx.Deadline()
		// The read costs time, and the test has to make that cost real: two budgets
		// created a nanosecond apart are two budgets, but not ones an assertion can tell
		// from one. Spent here rather than slept, because nothing is being waited for --
		// this is the elapsed time the second budget would silently hand back.
		for spent := time.Now(); time.Since(spent) < 5*time.Millisecond; {
			runtime.Gosched()
		}
		return &waTypes.GroupInfo{
			GroupTopic: waTypes.GroupTopic{TopicID: "3EB0C2A14F4FBC421B2E8C"},
		}, nil
	}
	session.setTopic = func(ctx context.Context, _ *wm.Client, _ waTypes.JID, _, _, _ string) error {
		writeUntil, _ = ctx.Deadline()
		return errors.New("recorded")
	}

	_, _ = session.Execute(t.Context(), &protocol.Command{
		Type:    protocol.CommandGroupDescriptionSet,
		Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"description":"x"}`),
	})

	if readUntil.IsZero() || writeUntil.IsZero() {
		t.Fatalf("a query went out with no deadline (read set=%v, write set=%v)",
			!readUntil.IsZero(), !writeUntil.IsZero())
	}
	if writeUntil.After(readUntil) {
		t.Errorf("the write was given until %s and the read until %s: %s more than the command's whole ceiling",
			writeUntil, readUntil, writeUntil.Sub(readUntil))
	}
}

// The budget can run out while the read is in flight and the read still answer. Going on
// to the write then spends a second command's worth of the executor on a command that has
// already given up, and the caller is told `internal` for it rather than that this side
// stopped waiting.
func TestADescriptionIsNotWrittenAfterItsBudgetRanOut(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
		return &waTypes.GroupInfo{
			GroupTopic: waTypes.GroupTopic{TopicID: "3EB0C2A14F4FBC421B2E8C"},
		}, nil
	}
	written := false
	session.setTopic = func(context.Context, *wm.Client, waTypes.JID, string, string, string) error {
		written = true
		return nil
	}

	ran, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := session.Execute(ran, &protocol.Command{
		Type:    protocol.CommandGroupDescriptionSet,
		Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"description":"x"}`),
	})
	if written {
		t.Error("the description was written under a budget that had already run out")
	}
	assertCode(t, err, protocol.ErrorTimeout)
}

// `SetGroupTopic` reads the current description for itself when this side has no id to
// hand it, and it flattens a failure of that read with `%v`. So the ceiling ending the
// write arrives as a string with no sentinel left in it, and reported as it comes it tells
// the caller this connector broke rather than that it stopped waiting.
func TestAWriteEndedByTheCeilingIsReportedAsTheCeiling(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
		return &waTypes.GroupInfo{GroupTopic: waTypes.GroupTopic{TopicID: ""}}, nil
	}

	ran, cancel := context.WithCancel(t.Context())
	session.setTopic = func(ctx context.Context, _ *wm.Client, _ waTypes.JID, _, _, _ string) error {
		// The budget runs out with the write already in flight, which is the case the
		// guard is for: before it, the read's own check would have caught it.
		cancel()
		<-ctx.Done()
		// Exactly what whatsmeow answers: the sentinel flattened into a string.
		return fmt.Errorf("failed to get group info: %v", ctx.Err()) //nolint:errorlint // the point is the lost sentinel
	}
	defer cancel()

	_, err := session.Execute(ran, &protocol.Command{
		Type:    protocol.CommandGroupDescriptionSet,
		Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"description":"x"}`),
	})
	assertCode(t, err, protocol.ErrorTimeout)
}

// whatsmeow answers an error for a group it cannot read, so a group and no error should
// not reach the write at all. Should is why the guard is there: reading a field off it
// takes the session's goroutine down, and with it every command queued behind this one.
func TestAGroupThatComesBackEmptyIsAnErrorAndNotAPanic(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
		return nil, nil
	}

	_, err := session.Execute(t.Context(), &protocol.Command{
		Type:    protocol.CommandGroupDescriptionSet,
		Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"description":"x"}`),
	})
	if err == nil {
		t.Fatal("a description was reported written over a group that came back empty")
	}
	assertCode(t, err, protocol.ErrorInternal)
}

// A description of nothing but spaces is a description of nothing but spaces. Collapsing
// it into a removal decides for the operator that what they typed was a mistake, and it
// stores something other than what the command carried while reporting success. Only an
// absent or empty description removes one, which is what the contract says and what the
// holdout scenario for #163 marks the other way round as a failure.
func TestADescriptionOfSpacesIsWrittenAndNotTakenForARemoval(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	var written string
	session.setDescription = func(_ context.Context, _ *wm.Client, _ waTypes.JID, description, _ string) error {
		written = description
		return nil
	}

	if _, err := session.Execute(t.Context(), &protocol.Command{
		Type:    protocol.CommandGroupDescriptionSet,
		Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"description":"   "}`),
	}); err != nil {
		t.Fatalf("group.description.set: %v", err)
	}
	if written != "   " {
		t.Errorf("three spaces reached WhatsApp as %q", written)
	}
}

// `group.description.set` is last write wins, and naming an id would quietly make it a
// compare-and-set: a description another admin changed between the read and the write
// answers 409 for naming the wrong predecessor. Before this change the call in use sent no
// `prev` at all and could not be refused that way, so refusing now would be a semantics
// change smuggled in as a bugfix.
func TestAConcurrentEditIsWrittenOverAndNotRefused(t *testing.T) {
	t.Parallel()

	session, _ := newTestSession(t, "5511999990001")
	session.setConnected(true)
	reads, ids := 0, []string{}
	// Every query of the second round trip, under the same budget as the first: a retry
	// that leaves the ceiling behind hands the account back the wait the ceiling took away.
	var under []time.Duration
	bounded := func(ctx context.Context) {
		if until, ok := ctx.Deadline(); ok {
			under = append(under, time.Until(until))
			return
		}
		under = append(under, time.Hour)
	}
	session.groupInfo = func(ctx context.Context, _ *wm.Client, _ waTypes.JID) (*waTypes.GroupInfo, error) {
		reads++
		bounded(ctx)
		if reads == 1 {
			return &waTypes.GroupInfo{GroupTopic: waTypes.GroupTopic{TopicID: "OLD"}}, nil
		}
		// Somebody else's write landed in between, so the group names a different one now.
		return &waTypes.GroupInfo{GroupTopic: waTypes.GroupTopic{TopicID: "THEIRS"}}, nil
	}
	revisions := []string{}
	session.setTopic = func(
		ctx context.Context, _ *wm.Client, _ waTypes.JID, previous, revision, _ string,
	) error {
		ids, revisions = append(ids, previous), append(revisions, revision)
		bounded(ctx)
		if previous == "OLD" {
			return &wm.IQError{Code: 409}
		}
		return nil
	}

	if _, err := session.Execute(t.Context(), &protocol.Command{
		Type:    protocol.CommandGroupDescriptionSet,
		Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"description":"x"}`),
	}); err != nil {
		t.Fatalf("a concurrent edit was answered %v instead of written over", err)
	}
	if len(ids) != 2 || ids[1] != "THEIRS" {
		t.Errorf("the description was written naming %v, want the second to name THEIRS", ids)
	}
	// A redelivery of one command has to write one revision, not one per attempt.
	if revisions[0] != revisions[1] {
		t.Errorf("the retry went out under %q where the first went out under %q",
			revisions[1], revisions[0])
	}
	for i, left := range under {
		if left > 20*time.Second {
			t.Errorf("query %d of the command was given %s", i+1, left)
		}
	}
}

// And a 409 that is not a concurrent edit is answered, not retried. A frozen description
// gives the same id back however many times it is read, and racing it would spend the
// session's only goroutine on a write WhatsApp has already refused.
func TestARefusalThatIsNotAConcurrentEditIsAnswered(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		refusal error
		want    protocol.ErrorCode
		reads   int
	}{
		// Frozen: read again, same id, so there was no concurrent edit to write over.
		"a frozen description": {&wm.IQError{Code: 409}, protocol.ErrorWaError, 2},
		// Not a conflict at all, so there is nothing to look at a second time.
		"not an admin":            {&wm.IQError{Code: 403}, protocol.ErrorWaError, 1},
		"WhatsApp is throttling":  {&wm.IQError{Code: 429}, protocol.ErrorRateLimited, 1},
		"the connection went":     {wm.ErrIQDisconnected, protocol.ErrorNotConnected, 1},
		"WhatsApp never answered": {wm.ErrIQTimedOut, protocol.ErrorTimeout, 1},
		// whatsmeow's own `%v` around a failure of the lookup it does for itself. Nothing
		// here can put the sentinel back, and this repository answers a cause it cannot
		// name with `internal` rather than with a guess.
		"a cause whatsmeow flattened": {
			errors.New("failed to get old group info to update topic: some error"),
			protocol.ErrorInternal, 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			reads := 0
			session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
				reads++
				return &waTypes.GroupInfo{GroupTopic: waTypes.GroupTopic{TopicID: "undefined"}}, nil
			}
			writes := 0
			session.setTopic = func(context.Context, *wm.Client, waTypes.JID, string, string, string) error {
				writes++
				return test.refusal
			}

			_, err := session.Execute(t.Context(), &protocol.Command{
				Type:    protocol.CommandGroupDescriptionSet,
				Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"description":"x"}`),
			})
			assertCode(t, err, test.want)
			if reads != test.reads {
				t.Errorf("the group was read %d times, want %d", reads, test.reads)
			}
			if writes != 1 {
				t.Errorf("the description was written %d times for a refusal nobody could act on", writes)
			}
		})
	}
}

// What the second look answers is the command's answer. Swallowing a failure of it reports
// the first refusal, which is the one thing that read exists to find out is out of date;
// and a group that comes back with neither a group nor an error takes the session's
// goroutine down on the field read next.
func TestTheSecondLookAtAGroupIsAnsweredLikeTheFirst(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		info    *waTypes.GroupInfo
		err     error
		want    protocol.ErrorCode
		written int
	}{
		"the connection went while it looked again": {
			nil, wm.ErrIQDisconnected, protocol.ErrorNotConnected, 1,
		},
		"WhatsApp throttled the second look": {
			nil, &wm.IQError{Code: 429}, protocol.ErrorRateLimited, 1,
		},
		// Nothing to compare and nothing to name: the first refusal stands, and reading a
		// field off the nil would take every command queued behind this one down with it.
		"the group came back empty": {nil, nil, protocol.ErrorWaError, 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			reads := 0
			session.groupInfo = func(context.Context, *wm.Client, waTypes.JID) (*waTypes.GroupInfo, error) {
				reads++
				if reads == 1 {
					return &waTypes.GroupInfo{GroupTopic: waTypes.GroupTopic{TopicID: "OLD"}}, nil
				}
				return test.info, test.err
			}
			writes := 0
			session.setTopic = func(context.Context, *wm.Client, waTypes.JID, string, string, string) error {
				writes++
				return &wm.IQError{Code: 409}
			}

			_, err := session.Execute(t.Context(), &protocol.Command{
				Type:    protocol.CommandGroupDescriptionSet,
				Payload: []byte(`{"group":{"kind":"group","id":"` + theGroup + `"},"description":"x"}`),
			})
			assertCode(t, err, test.want)
			if writes != test.written {
				t.Errorf("the description was written %d times, want %d", writes, test.written)
			}
		})
	}
}
