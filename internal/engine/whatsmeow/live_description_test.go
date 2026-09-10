//go:build live

// The description phase, for #163. A group description could not be removed: the removal
// waited out the whole info query and answered `timeout` after a minute and a quarter,
// with every command for that account queued behind it.
//
//	go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveDescription
package whatsmeow

import (
	"encoding/json"
	"testing"
	"time"

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

	describe := func(t *testing.T, description any) time.Duration {
		t.Helper()
		started := time.Now()
		liveCommand(t, subject, protocol.CommandGroupDescriptionSet, map[string]any{
			"group": target, "description": description,
		})
		return time.Since(started)
	}
	// Read from the far side, so what is checked is what the group is rather than what
	// this session remembers about it.
	reads := func(t *testing.T) string {
		t.Helper()
		info, err := counterpart.current().GetGroupInfo(t.Context(), group)
		if err != nil {
			t.Fatalf("read the group back: %v", err)
		}
		return info.Topic
	}

	t.Run("a description is written and reads back", func(t *testing.T) {
		if took := describe(t, "para a 163"); took > promptly {
			t.Errorf("writing a description took %s", took.Round(time.Millisecond))
		}
		if got := reads(t); got != "para a 163" {
			t.Errorf("the group reads back %q", got)
		}
	})

	t.Run("a description is rewritten", func(t *testing.T) {
		if took := describe(t, "para a 163, de novo"); took > promptly {
			t.Errorf("rewriting a description took %s", took.Round(time.Millisecond))
		}
		if got := reads(t); got != "para a 163, de novo" {
			t.Errorf("the group reads back %q", got)
		}
	})

	// The defect. Before this change the same call answered `timeout` after 1m15s, four
	// times out of four.
	t.Run("a description is removed", func(t *testing.T) {
		if took := describe(t, nil); took > promptly {
			t.Errorf("removing a description took %s", took.Round(time.Millisecond))
		}
		if got := reads(t); got != "" {
			t.Errorf("the description is still %q after being removed", got)
		}
	})

	// The state a removal that leans on the previous description's id would hang in:
	// there is no previous description to name.
	t.Run("removing what is already gone is not a wait", func(t *testing.T) {
		if took := describe(t, nil); took > promptly {
			t.Errorf("removing an absent description took %s", took.Round(time.Millisecond))
		}
		if got := reads(t); got != "" {
			t.Errorf("the description came back as %q", got)
		}
	})

	// Whitespace is text, not a request for nothing. Storing a space where the operator
	// asked for a removal is storing something else and calling it empty.
	t.Run("a description of spaces is not a removal", func(t *testing.T) {
		if took := describe(t, "   "); took > promptly {
			t.Errorf("writing spaces took %s", took.Round(time.Millisecond))
		}
		if got := reads(t); got == "" {
			t.Error("three spaces were collapsed into a removal")
		}
	})
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
