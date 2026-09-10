//go:build live

// The description phase, for #163. A group description could not be removed: the removal
// waited out the whole info query and answered `timeout` after a minute and a quarter,
// with every command for that account queued behind it.
//
//	go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveDescription
//
// It makes a group, so running it over and over runs into WhatsApp's own limit rather than
// into anything here: measured, an account that has made about ten groups within the hour
// answers `429 rate-overlimit` to the next `CreateGroup`. Pass WAC_LIVE_GROUP=<jid>@g.us to
// reuse one instead, which is what repeat runs want anyway.
package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// TestLiveDescriptionIsRemovable walks a description through its whole life on a real
// group: written, rewritten, removed, and removed again when there is nothing left to
// remove. Every step is timed, because the defect was never a wrong answer -- it was the
// wait.
func TestLiveDescriptionIsRemovable(t *testing.T) {
	subject, counterpart, container := liveBoth(t, MediaOptions{})
	counterpartJID := liveMustBePaired(t, container, liveCounterpartSID)
	liveResumeAsking(t, subject, engine.ConnectRequest{Pairing: "resume", Groups: true})
	liveResumeAsking(t, counterpart, engine.ConnectRequest{Pairing: "resume", Groups: true})

	group := liveGroup(t, subject, counterpartJID)
	liveGroupReaches(t, counterpart, group)
	target := map[string]any{"kind": "group", "id": group.User}

	// Ten seconds, which is what the acceptance scenario gives these. Well above the
	// second or so they take, and well below the seventy-five the defect took.
	const promptly = 10 * time.Second

	// Asked again while WhatsApp is throttling, and timed only from the attempt that got
	// through. Five description changes in as many seconds is more than this account is
	// allowed -- measured: the fifth comes back `rate_limited` -- and that is WhatsApp
	// pacing a real account, not the connector being slow. Retried rather than paced with
	// a sleep, so what the phase waits on is the answer and not a number somebody guessed.
	describe := func(t *testing.T, description any) time.Duration {
		t.Helper()

		waiting, give := context.WithTimeout(t.Context(), time.Minute)
		defer give()
		for attempt := 0; ; attempt++ {
			started := time.Now()
			err := liveTry(t, subject, protocol.CommandGroupDescriptionSet, map[string]any{
				"group": target, "description": description,
			})
			took := time.Since(started)
			if err == nil {
				return took
			}
			var coded *protocol.Error
			if !errors.As(err, &coded) || coded.Code != protocol.ErrorRateLimited {
				t.Fatalf("group.description.set: %v", err)
			}
			if attempt == 0 {
				t.Logf("WhatsApp is throttling this account's description changes; asking again")
			}
			select {
			case <-waiting.Done():
				t.Fatalf("WhatsApp throttled every attempt within the minute it was given")
			case <-time.After(2 * time.Second):
			}
		}
	}
	// Read from the far side, so what is checked is what the group is rather than what
	// this session remembers about it.
	//
	// Waited on rather than read once: the write is acknowledged by WhatsApp to the
	// session that made it, and the other account learns over its own connection, so a
	// single read races the propagation and fails on a description that is on its way.
	// A fixed pause would be the same race with a number on it.
	reads := func(t *testing.T, wanted string, agrees func(string) bool) {
		t.Helper()

		waiting, give := context.WithTimeout(t.Context(), 30*time.Second)
		defer give()
		var last string
		for {
			info, err := counterpart.current().GetGroupInfo(waiting, group)
			if err == nil {
				if last = info.Topic; agrees(last) {
					return
				}
			}
			select {
			case <-waiting.Done():
				t.Fatalf("the group reads %q on the other account, want %s", last, wanted)
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
	is := func(t *testing.T, want string) {
		t.Helper()
		reads(t, strconv.Quote(want), func(got string) bool { return got == want })
	}

	t.Run("a description is written and reads back", func(t *testing.T) {
		if took := describe(t, "para a 163"); took > promptly {
			t.Errorf("writing a description took %s", took.Round(time.Millisecond))
		}
		is(t, "para a 163")
	})

	t.Run("a description is rewritten", func(t *testing.T) {
		if took := describe(t, "para a 163, de novo"); took > promptly {
			t.Errorf("rewriting a description took %s", took.Round(time.Millisecond))
		}
		is(t, "para a 163, de novo")
	})

	// The defect. Before this change the same call answered `timeout` after 1m15s, four
	// times out of four.
	t.Run("a description is removed", func(t *testing.T) {
		if took := describe(t, nil); took > promptly {
			t.Errorf("removing a description took %s", took.Round(time.Millisecond))
		}
		is(t, "")
	})

	// The state a removal that leans on the previous description's id would hang in:
	// there is no previous description to name.
	t.Run("removing what is already gone is not a wait", func(t *testing.T) {
		if took := describe(t, nil); took > promptly {
			t.Errorf("removing an absent description took %s", took.Round(time.Millisecond))
		}
		is(t, "")
	})

	// Whitespace is text, not a request for nothing. Storing a space where the operator
	// asked for a removal is storing something else and calling it empty.
	t.Run("a description of spaces is not a removal", func(t *testing.T) {
		if took := describe(t, "   "); took > promptly {
			t.Errorf("writing spaces took %s", took.Round(time.Millisecond))
		}
		// Whatever WhatsApp keeps for three spaces is its business -- it may trim them,
		// and the acceptance scenario says so. What it must not be is nothing, because
		// nothing would mean this connector collapsed whitespace into a removal on its
		// own and stored something other than what the operator asked for.
		reads(t, "anything but empty", func(got string) bool { return got != "" })
	})
}

// TestLiveAFrozenDescriptionIsRefusedQuickly is the half of #163 this change cannot fix,
// measured rather than argued. A description this connector wrote before it gave one an id
// is frozen: WhatsApp answers 409 to every later change, whichever call or stanza carries
// it. What changes here is the cost of being told so -- a third of a second instead of the
// minute and a quarter the old removal spent before answering `timeout`.
//
// Needs a group in that state, which the suite will not make for itself: freezing one is
// permanent, and a test that leaves permanent damage behind on every run is a test that
// runs out of groups. Pass WAC_LIVE_FROZEN_GROUP=<jid>@g.us. 120363410605594371@g.us was
// frozen on 10/09/2026 by the probe that established the mechanism.
func TestLiveAFrozenDescriptionIsRefusedQuickly(t *testing.T) {
	frozen := os.Getenv("WAC_LIVE_FROZEN_GROUP")
	if frozen == "" {
		t.Skip("set WAC_LIVE_FROZEN_GROUP to a group whose description was written the old way")
	}
	jid, err := waTypes.ParseJID(frozen)
	if err != nil {
		t.Fatalf("WAC_LIVE_FROZEN_GROUP is not a JID: %v", err)
	}

	subject, _, _ := liveBoth(t, MediaOptions{})
	liveResumeAsking(t, subject, engine.ConnectRequest{Pairing: "resume", Groups: true})
	target := map[string]any{"kind": "group", "id": jid.User}

	for name, description := range map[string]any{
		"rewriting it": "wac-163 outra coisa",
		"removing it":  nil,
	} {
		t.Run(name, func(t *testing.T) {
			started := time.Now()
			err := liveTry(t, subject, protocol.CommandGroupDescriptionSet, map[string]any{
				"group": target, "description": description,
			})
			took := time.Since(started)
			t.Logf("%s a frozen description answered %v after %s", name, err, took.Round(time.Millisecond))
			if err == nil {
				t.Fatal("a frozen description was changed, which every measurement says cannot happen")
			}
			// The point is the wait, not the refusal. Ten seconds is what the acceptance
			// scenario gives a group command, and the defect took seventy-five.
			if took > 10*time.Second {
				t.Errorf("being refused took %s", took.Round(time.Millisecond))
			}
		})
	}
}

// TestLiveARedeliveredRemovalIsNotRefused pins what a redelivery of a successful removal
// costs, because the fix reads a group's topic id to recognise its own work and a removal
// leaves no topic id to recognise: whatsmeow fills `TopicID` from a description body, and
// a removed description has none.
//
// The question that raises is whether WhatsApp refuses a delete stanza whose `id` it has
// already seen. Measured on 10/09/2026 it does not -- the same command sent twice under
// one idempotency key, so one derived revision and one `id`, is answered twice:
//
//	write one to remove                       744 ms   applied -> topic_id "3EB0597422..."
//	remove it                                 798 ms   applied -> topic_id ""
//	remove it again under the same key        991 ms   applied
//	remove it again under another key         903 ms   applied
//
// So a removal needs no recognising: the redelivery succeeds on its own. This is here so
// that stays measured rather than assumed, since the code depends on it by omission.
func TestLiveARedeliveredRemovalIsNotRefused(t *testing.T) {
	subject, counterpart, container := liveBoth(t, MediaOptions{})
	counterpartJID := liveMustBePaired(t, container, liveCounterpartSID)
	liveResumeAsking(t, subject, engine.ConnectRequest{Pairing: "resume", Groups: true})
	liveResumeAsking(t, counterpart, engine.ConnectRequest{Pairing: "resume", Groups: true})

	group := liveGroup(t, subject, counterpartJID)
	target := map[string]any{"kind": "group", "id": group.User}
	client := subject.current()

	read := func(what, wantTopic string) {
		t.Helper()
		info, err := client.GetGroupInfo(t.Context(), group)
		if err != nil {
			t.Fatalf("%s: read the group: %v", what, err)
		}
		t.Logf("%s: topic=%q topic_id=%q", what, info.Topic, info.TopicID)
		if info.Topic != wantTopic {
			t.Errorf("%s: the group says %q, want %q", what, info.Topic, wantTopic)
		}
	}

	send := func(what string, key string, description any) {
		t.Helper()
		payload := map[string]any{"group": target, "description": description}
		started := time.Now()
		err := liveTryKeyed(t, subject, protocol.CommandGroupDescriptionSet, key, payload)
		took := time.Since(started)
		t.Logf("%s took %s and answered %v", what, took.Round(time.Millisecond), err)
		if err != nil {
			t.Fatalf("%s was refused: %v", what, err)
		}
		// The wait matters as much as the answer: this is #163, and the defect was a
		// removal that answered at all only after a minute and a quarter.
		if took > 10*time.Second {
			t.Errorf("%s took %s", what, took.Round(time.Millisecond))
		}
	}

	send("write one to remove", "probe-write", "para apagar")
	read("after writing", "para apagar")
	send("remove it", "probe-remove", nil)
	read("after removing", "")
	// The same command again: same idempotency key, so the same derived revision, so the
	// same `id` on the delete stanza. This is what a redelivery actually sends.
	send("remove it again under the same key", "probe-remove", nil)
	read("after the redelivery", "")
	// And for the comparison the live phase already makes: a different command, so a
	// different revision.
	send("remove it again under another key", "probe-remove-2", nil)
	read("after the second removal", "")
}

// liveTryKeyed is liveTry with an idempotency key, which is what makes two sends the same
// command rather than two of them.
func liveTryKeyed(
	t *testing.T, from *Session, kind protocol.CommandType, key string, payload map[string]any,
) error {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("build a %s: %v", kind, err)
	}
	_, err = from.Execute(t.Context(), &protocol.Command{
		Type: kind, Payload: body, ID: key, IdempotencyKey: key,
	})
	return err
}

// TestLiveTheQueueMovesBehindAGroupCommand is the half of #163 the title calls the worse
// one: the session executor is serial, so a group command WhatsApp does not answer costs
// the account rather than the command. The probe asks about a group the account is not in,
// with a second command queued behind it, and times the second.
func TestLiveTheQueueMovesBehindAGroupCommand(t *testing.T) {
	subject, _, _ := liveBoth(t, MediaOptions{})
	liveResumeAsking(t, subject, engine.ConnectRequest{Pairing: "resume", Groups: true})

	stranger := map[string]any{"kind": "group", "id": "120363000000000001"}
	started := time.Now()
	err := liveTry(t, subject, protocol.CommandGroupDescriptionSet, map[string]any{
		"group": stranger, "description": "wac-163 sonda",
	})
	first := time.Since(started)
	t.Logf("a group this account is not in answered %v after %s", err, first.Round(time.Millisecond))
	if first >= time.Minute {
		t.Errorf("the account waited %s on one group command", first.Round(time.Millisecond))
	}

	// Whatever the first one did, the next command must not have paid for it.
	started = time.Now()
	if _, err := subject.Execute(t.Context(), &protocol.Command{
		Type: protocol.CommandSessionStatus, Payload: []byte(`{}`),
	}); err != nil {
		t.Fatalf("ask the session how it is: %v", err)
	}
	if behind := time.Since(started); behind > 5*time.Second {
		t.Errorf("the command behind it took %s", behind.Round(time.Millisecond))
	}
}

// liveTry is liveCommand for a command whose failure is the phase's business rather than
// the harness's: it hands the error back instead of ending the run.
func liveTry(t *testing.T, from *Session, kind protocol.CommandType, payload map[string]any) error {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("build a %s: %v", kind, err)
	}
	_, err = from.Execute(t.Context(), &protocol.Command{Type: kind, Payload: body})
	return err
}
