//go:build live

// Groups, against two real accounts.
//
// The question this file exists for is #35: a message key in a group names its sender in
// whichever namespace the group addresses its members by, and `asTheGroupAddresses` pays
// a round trip per reaction and per admin revoke to translate one into the other. The
// mechanism is not in doubt; whether it matters is, and the direct-chat analogue was
// measured and went the other way -- the recipient's client resolved the key either way,
// and the translation there would have been paid for nothing.
//
// A group differs in a way that might make it behave otherwise: a direct chat's key names
// the conversation, and a client has one conversation with that contact however it is
// spelled, while a group's `participant` names *which member* sent the message, and the
// two spellings are two identity strings for a person rather than two names for a chat.
//
//	WAC_LIVE_GROUP=<jid> go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveGroupKeyNamespace
//
// The group is created on the first run and its JID printed; pass it back in
// WAC_LIVE_GROUP afterwards. Creating one per run is not free -- WhatsApp rate-limits it,
// and a phone left in twenty test groups is a phone somebody has to clean up.
package whatsmeow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// TestLiveGroupKeyNamespace answers whether a reaction whose key names the participant in
// the namespace the group does not use is applied anyway.
//
// Both halves are run, and the control is what makes the answer mean anything. A reaction
// that does not arrive proves nothing on its own: it could be the namespace, or it could
// be that reactions in groups do not work at all in this build. So the same reaction goes
// out twice, once translated and once deliberately not, and the two results are compared.
//
// The counterpart is what makes this measurable at all. It sends the message being
// reacted to and it watches for the reaction, so nothing here needs a person to look at a
// phone and report what they see -- which is what #54 was about.
func TestLiveGroupKeyNamespace(t *testing.T) {
	subject, counterpart, container := liveBoth(t, MediaOptions{})

	subjectJID := liveMustBePaired(t, container, liveSID)
	counterpartJID := liveMustBePaired(t, container, liveCounterpartSID)

	// Both sides subscribed to groups, and the counterpart as much as the subject: it is
	// where the reaction has to show up, and an unsubscribed session drops it in silence.
	groups := engine.ConnectRequest{Pairing: "resume", Groups: true}
	liveResumeAsking(t, subject, groups)
	liveResumeAsking(t, counterpart, groups)

	group := liveGroup(t, subject, counterpartJID)
	mode := liveGroupMode(t, subject, group)
	t.Logf("group %s addresses its members by %q", group, mode)

	// The namespace the group does not use, which is the whole point of the phase, and
	// the one namespace this account cannot be named in is a reason to stop rather than
	// to report a result: without it there is nothing to send wrongly.
	wrong := liveOtherNamespace(t, subject, counterpartJID, mode)

	// Both sides watched at once, which the fan-out in `watch` is what makes possible:
	// the subject has to see the message before it can react to it, and the counterpart
	// is where the answer shows up.
	mine := watch(t, subject)
	inbox := watch(t, counterpart)
	for _, probe := range []struct {
		name        string
		participant protocol.Address
		emoji       string
		lying       bool
	}{
		// The control first. If a translated reaction does not arrive either, the
		// experiment says nothing about namespaces and the phase says so.
		{name: "translated", participant: liveAddressOf(t, counterpartJID), emoji: "👍"},
		{name: "in the namespace the group does not use", participant: wrong, emoji: "❤️", lying: true},
		// The instrument check, and the phase is worth little without it. Our own client
		// applies this one too -- `reactionOf` publishes by target id and never reads
		// the participant -- so what is asserted here is that blindness and not the
		// outcome. On the phone this reaction does **not** appear: measured 07/09/2026,
		// and it is what says the field is consulted by somebody, which is what makes
		// the probe above mean anything.
		{name: "naming a member who did not send it", participant: liveAddressOf(t, subjectJID), emoji: "😀"},
	} {
		t.Run(probe.name, func(t *testing.T) {
			said := "conector nativo, reagir: " + probe.name
			sent := liveSayTo(t, counterpart, protocol.Address{
				Kind: protocol.AddressGroup, ID: group.User,
			}, said)
			// Sent by the counterpart, so the subject has to see it before it can react
			// to it: reacting to an id the account has not received is a different
			// failure and would be read as this one.
			mine.awaitMessage(t, sent, 2*time.Minute)

			// A lie, and the only way to send the key untranslated without changing
			// production code: `asTheGroupAddresses` rewrites a participant whose
			// namespace does not match what this returns, so telling it the group is on
			// the wrong namespace makes it leave the wrong one alone.
			if probe.lying {
				subject.groupMode = func(context.Context, waTypes.JID) (waTypes.AddressingMode, error) {
					return liveOtherMode(mode), nil
				}
				t.Cleanup(func() { subject.groupMode = nil })
			}
			liveActOne(t, subject, protocol.CommandMessageReact, map[string]any{
				"to":                 map[string]any{"kind": "group", "id": group.User},
				"target_id":          sent,
				"target_participant": map[string]any{"kind": probe.participant.Kind, "id": probe.participant.ID},
				"emoji":              probe.emoji,
			})

			// WhatsApp answers a key naming a participant no message was sent by with a
			// timestamp and shows nothing, so a reaction that is ignored is indis-
			// tinguishable from one still in flight. Only a deadline tells them apart,
			// and it has to be long enough that a slow round trip is not read as the
			// namespace mattering.
			got := liveReactionOn(t, inbox, sent, 90*time.Second)
			switch got {
			case probe.emoji:
				t.Logf("the reaction was applied: %s", got)
			case "":
				t.Errorf("no reaction on %s within the window", sent)
			default:
				t.Errorf("the reaction on %s is %q, want %q", sent, got, probe.emoji)
			}
		})
	}
	t.Logf("subject %s, counterpart %s", subjectJID, counterpartJID)
	// The half no harness reaches. Printed rather than left in a comment: the person who
	// runs this is the person who has to look, and they are looking at this output.
	//
	// Read once, on 07/09/2026: the translated one and the one in the namespace the group
	// does not use both carry their reaction; the one naming a member who did not send
	// the message does not. So the namespace does not matter and the member does.
	fmt.Fprintf(os.Stderr, "\nnow open %s on a phone: each of the three messages says "+
		"which probe it is, and the question is which of them carries its reaction\n", group)
}

// liveGroup is the group both accounts are in: the one named in WAC_LIVE_GROUP, or a new
// one when nothing is.
func liveGroup(t *testing.T, subject *Session, counterpart waTypes.JID) waTypes.JID {
	t.Helper()

	if named := os.Getenv("WAC_LIVE_GROUP"); named != "" {
		jid, err := waTypes.ParseJID(named)
		if err != nil {
			t.Fatalf("WAC_LIVE_GROUP is not a JID: %v", err)
		}
		return jid
	}
	info, err := subject.current().CreateGroup(t.Context(), whatsmeow.ReqCreateGroup{
		// Under WhatsApp's 25-character limit, which answers a longer name with a 406.
		Name:         "wac " + time.Now().Format("01-02 15:04"),
		Participants: []waTypes.JID{counterpart},
	})
	if err != nil {
		t.Fatalf("create the group: %v", err)
	}
	t.Logf("created %s -- pass WAC_LIVE_GROUP=%s to reuse it", info.JID, info.JID)
	// The counterpart learns about the group over its own connection, and reacting
	// before it has is a message sent to a group it does not know it is in.
	time.Sleep(5 * time.Second)
	return info.JID
}

// liveGroupMode reads which namespace the group addresses its members by, through the
// same call the production path uses.
func liveGroupMode(t *testing.T, subject *Session, group waTypes.JID) waTypes.AddressingMode {
	t.Helper()

	mode, err := subject.groupModeOverSocket(t.Context(), group)
	if err != nil {
		t.Fatalf("read the group's addressing: %v", err)
	}
	return mode
}

// liveOtherMode is the addressing a group is not on.
func liveOtherMode(mode waTypes.AddressingMode) waTypes.AddressingMode {
	if mode == waTypes.AddressingModeLID {
		return waTypes.AddressingModePN
	}
	return waTypes.AddressingModeLID
}

// liveOtherNamespace names the counterpart the way the group does not.
func liveOtherNamespace(
	t *testing.T, subject *Session, counterpart waTypes.JID, mode waTypes.AddressingMode,
) protocol.Address {
	t.Helper()

	if mode != waTypes.AddressingModeLID {
		alt, err := subject.current().Store.GetAltJID(t.Context(), counterpart)
		if err != nil || alt.IsEmpty() {
			t.Skipf("the counterpart has no LID on this account (err=%v), so there is no wrong namespace to send", err)
		}
		return protocol.Address{Kind: protocol.AddressLID, ID: alt.User}
	}
	// The group is on LID, so the wrong namespace is the phone number, which is the one
	// thing about the counterpart this harness always knows.
	return protocol.Address{Kind: protocol.AddressPhone, ID: counterpart.User}
}

// liveAddressOf names a JID the way the contract does, through the production mapping
// rather than a second one written here: a phase that spells an address its own way is
// checking its own spelling.
func liveAddressOf(t *testing.T, jid waTypes.JID) protocol.Address {
	t.Helper()

	address, ok := addressOf(jid)
	if !ok {
		t.Fatalf("%s is not an address the contract can carry", jid)
	}
	return address
}

// liveSayTo sends a text to any address, which is what a group needs and liveSay's
// phone-only shape cannot express.
func liveSayTo(t *testing.T, from *Session, to protocol.Address, body string) string {
	t.Helper()

	messageID := from.current().GenerateMessageID()
	payload, err := json.Marshal(map[string]any{
		"message_id": messageID,
		"to":         map[string]any{"kind": to.Kind, "id": to.ID},
		"content":    map[string]any{"type": "text", "body": body},
	})
	if err != nil {
		t.Fatalf("build the send: %v", err)
	}
	if _, err := from.Execute(t.Context(), &protocol.Command{
		Type: protocol.CommandMessageSend, Payload: payload,
	}); err != nil {
		t.Fatalf("message.send: %v", err)
	}
	return messageID
}

// liveReactionOn is the emoji that landed on a message, or "" when none did within the
// window. Not a Fatal: "nothing arrived" is one of the two answers this phase is asking
// for, and the caller is what decides whether it is a failure.
func liveReactionOn(t *testing.T, events *recorder, target string, within time.Duration) string {
	t.Helper()

	deadline := time.After(within)
	for {
		select {
		case emission, ok := <-events.seen:
			if !ok {
				t.Fatalf("the session ended while waiting for a reaction on %s", target)
			}
			if emission.Type != protocol.EventMessageReaction {
				continue
			}
			var body struct {
				TargetID string `json:"target_id"`
				Emoji    string `json:"emoji"`
			}
			if err := json.Unmarshal(emission.Payload, &body); err != nil {
				t.Fatalf("unmarshal a message.reaction: %v", err)
			}
			if body.TargetID == target {
				return body.Emoji
			}
		case <-deadline:
			return ""
		}
	}
}

// TestLiveGroupRevokeKeyNamespace is the same question asked through an admin revoke,
// which was the better instrument until it was run and turned out to be the same one.
//
// The reasoning was that a revoke is server-validated where a reaction is not: name a
// member no message was sent by and WhatsApp has nothing to take down, so the message
// staying would be the server refusing the key. What the run says is that all three
// propagate, the wrong member included, and the reason is the same as the reaction's --
// `revokeOf` publishes by the target id and never reads the participant either. Both
// observables this harness has go through our own connector, and our own connector is
// blind to the field under test by construction.
//
// So what these two phases measure is real but narrower than #35 asks: WhatsApp accepts
// and propagates the action whatever the key's participant says, and a client keying on
// the target id alone -- ours, and therefore Chatwoot -- applies it correctly in every
// case. What is still open is whether the WhatsApp app attaches it to the right bubble,
// and no amount of harness answers that: it is a person looking at a phone. Each probe
// says in its own message body which one it is, so that look is one look.
//
// The subject is the group's creator and therefore its admin, which is what makes the
// revoke of somebody else's message a thing it may do at all.
func TestLiveGroupRevokeKeyNamespace(t *testing.T) {
	subject, counterpart, container := liveBoth(t, MediaOptions{})

	subjectJID := liveMustBePaired(t, container, liveSID)
	counterpartJID := liveMustBePaired(t, container, liveCounterpartSID)

	groups := engine.ConnectRequest{Pairing: "resume", Groups: true}
	liveResumeAsking(t, subject, groups)
	liveResumeAsking(t, counterpart, groups)

	group := liveGroup(t, subject, counterpartJID)
	mode := liveGroupMode(t, subject, group)
	t.Logf("group %s addresses its members by %q", group, mode)
	wrong := liveOtherNamespace(t, subject, counterpartJID, mode)

	mine := watch(t, subject)
	theirs := watch(t, counterpart)
	for _, probe := range []struct {
		name        string
		participant protocol.Address
		lying       bool
		// gone is what was measured, written down so the day it changes is a failing
		// run rather than a paragraph nobody reads again. All three, which is the
		// finding: the key's participant does not decide whether the revoke propagates.
		gone bool
	}{
		{name: "translated", participant: liveAddressOf(t, counterpartJID), gone: true},
		{name: "in the namespace the group does not use", participant: wrong, lying: true, gone: true},
		// Published by us, and refused by WhatsApp: the message is still there on the
		// phone. That divergence is its own defect and is #107; what this line pins is
		// that we publish it, so the day the connector starts refusing it, this fails
		// and points at the issue rather than at the harness.
		{name: "naming a member who did not send it", participant: liveAddressOf(t, subjectJID), gone: true},
	} {
		t.Run(probe.name, func(t *testing.T) {
			// Named after the probe, because the half this cannot answer is answered by
			// somebody reading the group on a phone, and a body that says which probe
			// it is turns three looks into one.
			said := "conector nativo, apagar: " + probe.name
			sent := liveSayTo(t, counterpart, protocol.Address{
				Kind: protocol.AddressGroup, ID: group.User,
			}, said)
			mine.awaitMessage(t, sent, 2*time.Minute)

			if probe.lying {
				subject.groupMode = func(context.Context, waTypes.JID) (waTypes.AddressingMode, error) {
					return liveOtherMode(mode), nil
				}
				t.Cleanup(func() { subject.groupMode = nil })
			}
			liveActOne(t, subject, protocol.CommandMessageRevoke, map[string]any{
				"to":          map[string]any{"kind": "group", "id": group.User},
				"target_id":   sent,
				"participant": map[string]any{"kind": probe.participant.Kind, "id": probe.participant.ID},
			})

			// Short, and deliberately: a revoke the server accepted is propagated at
			// once, and the whole cost of this window is paid three times by the probe
			// that is expected to see nothing.
			took := liveRevokeOf(t, theirs, sent, 30*time.Second)
			if took != probe.gone {
				t.Errorf("the revoke reached the other account: %v, want %v", took, probe.gone)
			}
		})
	}
	// Read once, on 07/09/2026: the first two are gone, the third is still there. Same
	// answer as the reaction phase, and the third is #107.
	fmt.Fprintf(os.Stderr, "\nnow open %s on a phone: three messages were sent and a "+
		"revoke went out for each, and the question is which of them are actually gone\n", group)
}

// liveRevokeOf is whether a message was taken down within the window.
func liveRevokeOf(t *testing.T, events *recorder, target string, within time.Duration) bool {
	t.Helper()

	deadline := time.After(within)
	for {
		select {
		case emission, ok := <-events.seen:
			if !ok {
				t.Fatalf("the session ended while waiting on %s", target)
			}
			if emission.Type != protocol.EventMessageRevoked {
				continue
			}
			var body struct {
				MessageID string `json:"message_id"`
			}
			if err := json.Unmarshal(emission.Payload, &body); err != nil {
				t.Fatalf("unmarshal a message.revoked: %v", err)
			}
			if body.MessageID == target {
				return true
			}
		case <-deadline:
			return false
		}
	}
}
