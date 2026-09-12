//go:build live

// The group notifications, against a real account. What a unit test cannot reach here is
// the shape of the stanza itself: every mapping in `groupevents.go` is a claim about what
// WhatsApp puts on the wire and what whatsmeow makes of it, and the two settings this
// build deliberately does not carry -- member add mode and join approval -- are claims
// about what whatsmeow *fails* to make of it, which no fixture can confirm.
//
//	go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveGroupChanges
//
// It creates a group between the two paired accounts and changes it, so it leaves a real
// group on both phones. Pass WAC_LIVE_GROUP to reuse one instead, which costs the
// `group.joined` case: nobody joins anything in a run that reuses a group, and that
// subtest skips naming what it did not cover rather than waiting for a notification
// WhatsApp has no reason to send.
//
// WAC_LIVE_GROUP takes the whole JID, `<id>@g.us`, and the server half is not optional.
// `types.ParseJID` reads a bare id as a user, so `WAC_LIVE_GROUP=1203...` asks WhatsApp
// about a person with that number: every `GetGroupInfo` times out, the run dies after a
// minute on "never learned it is in", and nothing in that message says the JID was the
// problem. Measured here twice before the id was the suspect.
package whatsmeow

import (
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

func TestLiveGroupChangesArePublished(t *testing.T) {
	subject, counterpart, container := liveBoth(t, MediaOptions{})
	counterpartJID := liveMustBePaired(t, container, liveCounterpartSID)
	liveMustBePaired(t, container, liveSID)

	groups := engine.ConnectRequest{Pairing: "resume", Groups: true}
	liveResumeAsking(t, subject, groups)
	liveResumeAsking(t, counterpart, groups)

	// Everything is asserted on the counterpart: it is the side that learns about the
	// group over its own connection, which is the path this change is about. The side
	// that made a change is told by the reply to its own command and would pass every
	// probe below on an event handler that publishes nothing.
	watching := watch(t, counterpart)
	// What whatsmeow made of each notification, recorded beside what the connector
	// published. The two settings left out of `changes` are left out because of what this
	// records, and a claim about a library's parser that nobody printed is a guess.
	raw := recordGroupEvents(t, counterpart)

	group := liveGroup(t, subject, counterpartJID)
	liveGroupReaches(t, counterpart, group)
	target := map[string]any{"kind": "group", "id": group.User}

	t.Run("being added to a group is published with the group in it", func(t *testing.T) {
		// Only a group created in this run produces the notification this asserts on. With
		// WAC_LIVE_GROUP the account was added to that group at some point in the past, and
		// nothing announces it again -- the wait below would spend its ninety seconds and
		// fail on a build where the handler works. Skipped rather than quietly passed,
		// because what is not exercised here is `group.joined` in its entirety, and a run
		// that reuses a group has to say so instead of reporting a green it did not earn.
		if reused := os.Getenv("WAC_LIVE_GROUP"); reused != "" {
			t.Skipf("reusing %s, so nobody joins anything in this run: group.joined is not "+
				"exercised. Unset WAC_LIVE_GROUP to cover it, at the cost of one CreateGroup "+
				"against WhatsApp's rate limit.", reused)
		}
		joined := watching.await(t, protocol.EventGroupJoined, 90*time.Second)
		payload := decode(t, joined.Payload)
		info, ok := payload["info"].(map[string]any)
		if !ok {
			t.Fatalf("group.joined carried %v", payload)
		}
		named, _ := info["group"].(map[string]any)
		if named["id"] != group.User {
			t.Errorf("published %v for a group created as %s", info["group"], group)
		}
		roster, _ := info["participants"].([]any)
		if len(roster) != 2 {
			t.Errorf("published %d participants for a group of two: %v", len(roster), info["participants"])
		}
		for _, member := range roster {
			party, _ := member.(map[string]any)["party"].(map[string]any)
			if party["phone"] == nil && party["lid"] == nil {
				t.Errorf("a participant came out with no address: %v", member)
			}
		}
	})

	t.Run("a rename carries the subject, the actor and nothing else", func(t *testing.T) {
		// Stamped rather than constant, and on a reused group that is the difference
		// between a probe and a no-op: WhatsApp sends no notification for a subject that
		// is already the group's, so a fixed name passes on the first run and then spends
		// the whole ninety seconds waiting for a rename that never happened, on a build
		// where renaming works. Measured here, on the second run over the same group.
		renamed := "wac 161 " + time.Now().Format("0102-150405")
		liveCommand(t, subject, protocol.CommandGroupNameSet, map[string]any{
			"group": target, "subject": renamed,
		})

		changes, payload := liveGroupChange(t, watching)
		if changes["subject"] != renamed {
			t.Errorf("published %v as the new subject, want %q", changes["subject"], renamed)
		}
		// The tri-state, measured rather than reasoned about: the client reads a key that
		// is present as a change that was reported, so a rename carrying `announce: false`
		// turns the group's send lock off on every dashboard that applies it.
		if len(changes) != 1 {
			t.Errorf("a rename reported %d changes: %v", len(changes), changes)
		}
		actor, _ := payload["actor"].(map[string]any)
		if actor["phone"] == nil && actor["lid"] == nil {
			t.Errorf("named %v as who renamed the group", payload["actor"])
		}
		stamp, _ := payload["timestamp"].(float64)
		if drift := time.Since(time.UnixMilli(int64(stamp))); drift < 0 || drift > 5*time.Minute {
			t.Errorf("published %v as the moment of a rename that just happened", payload["timestamp"])
		}
	})

	t.Run("a send lock turned on and then off arrives both ways", func(t *testing.T) {
		for _, want := range []bool{true, false} {
			liveCommand(t, subject, protocol.CommandGroupSettingsSet, map[string]any{
				"group": target, "setting": "announce", "value": want,
			})
			changes, _ := liveGroupChange(t, watching)
			got, reported := changes["announce"]
			if !reported {
				t.Fatalf("setting announce to %v reported %v instead", want, changes)
			}
			if got != want {
				t.Errorf("setting announce to %v was published as %v", want, got)
			}
		}
	})

	t.Run("a description set and then cleared arrives as text and then as empty", func(t *testing.T) {
		liveCommand(t, subject, protocol.CommandGroupDescriptionSet, map[string]any{
			"group": target, "description": "antes do 161",
		})
		changes, _ := liveGroupChange(t, watching)
		if got := changes["description"]; got != "antes do 161" {
			t.Errorf("setting the description published %v", got)
		}

		// The other half is #163: clearing a description waits out the whole IQ deadline
		// and answers `timeout`, so the notification this would read never happens. Run
		// rather than skipped over, and skipped only on the failure that issue describes,
		// so the day the command lands this phase goes red and says to put the assertion
		// back instead of quietly never checking it again.
		if err := liveTry(t, subject, protocol.CommandGroupDescriptionSet, map[string]any{
			"group": target, "description": "",
		}); err != nil {
			t.Skipf("clearing a description still does not land (#163): %v", err)
		}
		changes, _ = liveGroupChange(t, watching)
		cleared, reported := changes["description"]
		if !reported {
			t.Fatalf("clearing the description reported %v instead", changes)
		}
		if cleared != "" {
			t.Errorf("clearing the description was published as %v", cleared)
		}
	})

	t.Run("promoting and demoting name the member on both sides", func(t *testing.T) {
		for _, action := range []string{"promote", "demote"} {
			liveCommand(t, subject, protocol.CommandGroupParticipantsUpdate, map[string]any{
				"group": target, "action": action,
				"participants": []any{map[string]any{"kind": "phone", "id": counterpartJID.User}},
			})
			changes, _ := liveGroupChange(t, watching)
			list, ok := changes[action].([]any)
			if !ok || len(list) != 1 {
				t.Fatalf("%s reported %v", action, changes)
			}
			party, _ := list[0].(map[string]any)
			if party["phone"] == nil && party["lid"] == nil {
				t.Errorf("%s named %v", action, list[0])
			}
			if _, alsoJoined := changes["join"]; alsoJoined {
				t.Errorf("%s also reported a join: %v", action, changes)
			}
		}
	})

	// The two the connector refuses to put in `changes`, and the phase that says whether
	// the refusal was right. Both must come out as `group.activity`, and neither may come
	// out as a `group.updated` -- an empty one, or one asserting approval is on.
	for _, setting := range []struct {
		name, key string
		value     any
	}{
		{name: "member add mode", key: "member_add_mode", value: "all_member_add"},
		{name: "join approval turned on", key: "join_approval", value: true},
		{name: "join approval turned off", key: "join_approval", value: false},
	} {
		t.Run("a change the contract cannot carry becomes activity: "+setting.name, func(t *testing.T) {
			updates := watching.count(protocol.EventGroupUpdated)
			liveCommand(t, subject, protocol.CommandGroupSettingsSet, map[string]any{
				"group": target, "setting": setting.key, "value": setting.value,
			})

			activity := watching.await(t, protocol.EventGroupActivity, 90*time.Second)
			payload := decode(t, activity.Payload)
			named, _ := payload["groups"].([]any)
			if len(named) != 1 {
				t.Fatalf("group.activity named %v", payload["groups"])
			}
			if first, _ := named[0].(map[string]any); first["id"] != group.User {
				t.Errorf("group.activity named %v for a change to %s", named[0], group)
			}
			if after := watching.count(protocol.EventGroupUpdated); after != updates {
				t.Errorf("a change the contract cannot carry also published %d group.updated",
					after-updates)
			}
		})
	}

	// Printed rather than asserted: it is the answer to whether WhatsApp sends
	// `membership_approval_mode` in both directions, which is what decides whether the
	// field could ever have been carried, and it is the thing this phase exists to find
	// out. An assertion here would be this build's guess about the library's parser.
	for _, seen := range raw.taken() {
		t.Logf("whatsmeow parsed a group change as %+v", seen)
	}
}

// liveGroupChange waits for the next group.updated and hands back its changes object and
// the whole payload.
func liveGroupChange(t *testing.T, watching *recorder) (changes, payload map[string]any) {
	t.Helper()

	updated := watching.await(t, protocol.EventGroupUpdated, 90*time.Second)
	payload = decode(t, updated.Payload)
	changes, ok := payload["changes"].(map[string]any)
	if !ok {
		t.Fatalf("group.updated carried no changes object: %s", updated.Payload)
	}
	if len(changes) == 0 {
		t.Fatalf("group.updated carried an empty changes object: %s", updated.Payload)
	}
	return changes, payload
}

// groupEventLog keeps what whatsmeow handed the session, beside what the session made of
// it. Registered as a second handler on the same client, which whatsmeow allows and which
// is what keeps this out of the production path: the connector's own handler neither sees
// this nor is changed by it.
type groupEventLog struct {
	sync.Mutex
	seen []string
}

func (l *groupEventLog) taken() []string {
	l.Lock()
	defer l.Unlock()
	return append([]string(nil), l.seen...)
}

func recordGroupEvents(t *testing.T, session *Session) *groupEventLog {
	t.Helper()

	log := &groupEventLog{}
	session.current().AddEventHandler(func(rawEvent any) {
		change, ok := rawEvent.(*waEvents.GroupInfo)
		if !ok {
			return
		}
		rendered, err := json.Marshal(map[string]any{
			"jid": change.JID.String(), "notify": change.Notify,
			"name": change.Name, "topic": change.Topic, "locked": change.Locked,
			"announce": change.Announce, "approval": change.MembershipApprovalMode,
			"ephemeral": change.Ephemeral, "unknown": len(change.UnknownChanges),
			"join": len(change.Join), "leave": len(change.Leave),
			"promote": len(change.Promote), "demote": len(change.Demote),
		})
		if err != nil {
			return
		}
		log.Lock()
		log.seen = append(log.seen, string(rendered))
		log.Unlock()
	})
	return log
}
