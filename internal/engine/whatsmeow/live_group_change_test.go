//go:build live

// A message being corrected, reacted to and deleted, in a group, watched from the other
// side. This is the group half of #40.
//
// `TestLiveWatchAMessageChange` walked all four of those against a real phone and every
// one came back naming the right message, but all of it happened in one direct chat. The
// group differs in the two places #35 is about: a group message names *which member*
// sent it, in whichever namespace the group addresses its members by, and a deletion in
// a group can be performed by an admin against somebody else's message, which a direct
// chat has no shape for at all.
//
// What makes this phase cheap where the direct-chat one is not: both sides are sessions
// this suite drives, so nobody has to sit at a phone. The ground truth is never taken
// from the side under test -- every id compared here comes back from the *sending*
// session's own command, so a subject that parses an incoming stanza wrongly cannot also
// define what the right answer was.
//
//	WAC_LIVE_GROUP=<jid> go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveGroupMessageChange
package whatsmeow

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// liveGroupChangeWindow is how long each leg gets. Generous next to the seconds a group
// message actually takes, because the cost of being wrong is asymmetric: too short turns
// a slow second into a failure somebody has to re-run two accounts to disprove, and too
// long only makes a genuine failure arrive later.
const liveGroupChangeWindow = 90 * time.Second

func TestLiveGroupMessageChange(t *testing.T) {
	subject, counterpart, container := liveBoth(t, MediaOptions{})

	counterpartJID := liveMustBePaired(t, container, liveCounterpartSID)

	groups := engine.ConnectRequest{Pairing: "resume", Groups: true}
	liveResumeAsking(t, subject, groups)
	liveResumeAsking(t, counterpart, groups)

	group := liveGroup(t, subject, counterpartJID)
	liveGroupReaches(t, subject, group)
	liveGroupReaches(t, counterpart, group)

	// Both sides are watched, and both watchers are opened before anything is sent.
	// The deletion leg is performed by the subject and has to be read on the
	// counterpart, so a watcher opened later would miss the event it exists for.
	watchingSubject := watch(t, subject)
	watchingCounterpart := watch(t, counterpart)

	// Read after the resumes: a session that has not signed in yet has no LID to read.
	theSubject, theCounterpart := liveWhoIs(t, subject), liveWhoIs(t, counterpart)

	to := protocol.Address{Kind: protocol.AddressGroup, ID: group.User}
	mode := liveGroupMode(t, subject, group)
	t.Logf("the group addresses its members by %s", mode)

	// Leg 1: the counterpart says something, and the subject has to publish it as
	// somebody else's, sent by the counterpart.
	target := liveSayTo(t, counterpart, to, "wac group change: the original")
	received := watchingSubject.awaitMessage(t, target, liveGroupChangeWindow)
	liveCheckAGroupSender(t, "the message", received, to, theCounterpart, mode)

	// Leg 2: the counterpart corrects it. The correction is a stanza with an id of its
	// own, and publishing that one instead of the target leaves a client looking for a
	// message nobody stored -- which is the failure the direct-chat phase found first.
	const corrected = "wac group change: corrected"
	liveEdit(t, counterpart, to, target, corrected)
	edited := liveAwaitAbout(t, watchingSubject, protocol.EventMessageEdited,
		"message_id", target, liveGroupChangeWindow)
	liveCheckTheCorrection(t, edited, target)
	// `liveCheckTheCorrection` only asks that the new body is not empty, which republishing
	// the original text also satisfies. The correction is the thing being corrected to, so
	// it is compared against what the command actually sent.
	liveCheckTheBody(t, "the correction", string(edited.Payload), corrected)
	liveCheckAGroupSender(t, "the correction", edited.Payload, to, theCounterpart, mode)

	// Leg 3: the reaction and taking it back, in that order, each awaited before the
	// next is sent. Waiting for both at once would let one leg's absence be covered by
	// the other's arrival.
	const reacted = "👍"
	putID := liveReact(t, counterpart, to, target, reacted)
	put := liveAwaitAbout(t, watchingSubject, protocol.EventMessageReaction,
		"target_id", target, liveGroupChangeWindow)
	liveCheckReactionID(t, "the reaction", put.Payload, putID)
	// And which emoji, not merely that there was one: `liveCheckTheReactions` tells the
	// two legs apart by empty versus non-empty, so any wrong emoji reads as the reaction.
	liveCheckTheEmoji(t, "the reaction", put.Payload, reacted)
	liveCheckAGroupSender(t, "the reaction", put.Payload, to, theCounterpart, mode)

	takenID := liveReact(t, counterpart, to, target, "")
	taken := liveAwaitAbout(t, watchingSubject, protocol.EventMessageReaction,
		"target_id", target, liveGroupChangeWindow)
	liveCheckReactionID(t, "the reaction being taken back", taken.Payload, takenID)
	// Checked on the removal as much as on the reaction. They are two events on the wire
	// and nothing makes the second inherit the first's sender: a removal that named
	// nobody, or named the wrong member, would take a reaction off somebody else's
	// bubble, and asserting only on `put` would let that through.
	liveCheckAGroupSender(t, "the reaction being taken back", taken.Payload, to, theCounterpart, mode)
	liveCheckTheEmoji(t, "the reaction being taken back", taken.Payload, "")
	liveCheckTheReactions(t, []*engine.Emission{put, taken}, target)

	// Leg 4: the subject is the group's creator and therefore its admin, and deletes the
	// counterpart's message. Read on the counterpart, which is the side that has to be
	// told its own message is gone.
	liveRevokeAsAdmin(t, subject, to, target, liveAddressOf(t, counterpartJID))
	revoked := liveAwaitAbout(t, watchingCounterpart, protocol.EventMessageRevoked,
		"message_id", target, liveGroupChangeWindow)
	liveCheckAnAdminDeletion(t, revoked, target, to, theSubject, mode)

	for name, session := range map[string]*Session{"subject": subject, "counterpart": counterpart} {
		if state := session.state(); state != "open" {
			t.Fatalf("the %s session did not stay up: state=%s", name, state)
		}
	}
}

// liveCheckAnAdminDeletion reads a deletion the account did not perform on a message it
// did send. The two fields it exists for are the ones a direct chat cannot produce.
func liveCheckAnAdminDeletion(
	t *testing.T, emission *engine.Emission, target string,
	in protocol.Address, admin protocol.Party, mode waTypes.AddressingMode,
) {
	t.Helper()

	var body struct {
		MessageID string             `json:"message_id"`
		By        protocol.RevokedBy `json:"by"`
		Sender    *protocol.Party    `json:"sender"`
	}
	if err := json.Unmarshal(emission.Payload, &body); err != nil {
		t.Fatalf("unmarshal the deletion: %v", err)
	}
	if body.MessageID != target {
		t.Fatalf("the deletion names %q, and the message deleted is %q", body.MessageID, target)
	}
	// The account wrote the message and somebody else deleted it. Published as `self`,
	// a client applies its own deletion -- files dropped, bubble gone -- to a deletion
	// it never performed, and the text an agent could still have read goes with it.
	if body.By != protocol.RevokedByContact {
		t.Fatalf("an admin's deletion of this account's message was published as %q, want %q: %s",
			body.By, protocol.RevokedByContact, emission.Payload)
	}
	liveCheckAGroupSender(t, "the deletion", emission.Payload, in, admin, mode)
	if body.Sender == nil {
		t.Fatalf("the deletion says somebody else performed it and does not say who: %s", emission.Payload)
	}
}

// liveCheckAGroupSender is the half of #40 that #35 is about: a group event names which
// member it came from, and the namespace that name is in is the group's, not the
// account's.
//
// The spelling the group addresses by is load-bearing and is required. The other one is
// not: whether the session has a mapping for it is a property of what it has seen, not of
// this event, so an absent one is allowed and a wrong one is not. That asymmetry is the
// whole check -- a group event carrying only the spelling the group does not use would
// resolve a second contact for one person, and one carrying a wrong second spelling would
// merge two people into one.
func liveCheckAGroupSender(
	t *testing.T, what string, payload json.RawMessage,
	in protocol.Address, who protocol.Party, mode waTypes.AddressingMode,
) {
	t.Helper()

	var body struct {
		Chat   protocol.Address `json:"chat"`
		FromMe bool             `json:"from_me"`
		Sender *protocol.Party  `json:"sender"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("unmarshal %s: %v", what, err)
	}
	// Every wait below selects on a message id alone, so an event published under the
	// wrong chat would satisfy all of them. A client routes on this field: the same
	// correction filed under a direct chat lands in a one-to-one conversation that never
	// had the message, and under another group it lands in somebody else's.
	if body.Chat != in {
		t.Fatalf("%s was published under chat %+v, and it happened in %+v: %s",
			what, body.Chat, in, payload)
	}
	if body.FromMe {
		t.Fatalf("%s came from another member and was published as the account's own: %s", what, payload)
	}
	if body.Sender == nil {
		t.Fatalf("%s in a group does not say which member it came from: %s", what, payload)
	}

	required, namespace := body.Sender.Phone, "phone number"
	wanted := who.Phone
	if mode == waTypes.AddressingModeLID {
		required, namespace, wanted = body.Sender.LID, "LID", who.LID
	}
	if required == "" {
		t.Fatalf("the group addresses its members by %s and %s names its sender without one: %s",
			namespace, what, payload)
	}
	if required != wanted {
		t.Fatalf("%s names %s %s, and it was sent by %s", what, namespace, required, wanted)
	}

	for _, spelling := range []struct{ kind, got, want string }{
		{"phone number", body.Sender.Phone, who.Phone},
		{"LID", body.Sender.LID, who.LID},
	} {
		if spelling.got != "" && spelling.got != spelling.want {
			t.Fatalf("%s spells its sender's %s as %s, and the sender's own session says %s: %s",
				what, spelling.kind, spelling.got, spelling.want, payload)
		}
	}
	fmt.Fprintf(os.Stderr, "%s: from %s %s (phone %q, lid %q)\n",
		what, namespace, required, body.Sender.Phone, body.Sender.LID)
}

// liveCheckReactionID compares a reaction's own id against the one the sending side put
// it out under. `liveCheckTheReactions` only asks that the id is not empty and not the
// target's, which two reactions published under one wrong id both satisfy -- and a client
// that deduplicates on it would then drop the removal and leave the emoji on the bubble
// forever.
func liveCheckReactionID(t *testing.T, what string, payload json.RawMessage, sent string) {
	t.Helper()

	var body struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("unmarshal %s: %v", what, err)
	}
	if body.ID != sent {
		t.Fatalf("%s arrived under id %q and went out under %q", what, body.ID, sent)
	}
}

// liveCheckTheBody compares what a message now reads against what was actually sent.
func liveCheckTheBody(t *testing.T, what, payload, want string) {
	t.Helper()

	var body struct {
		Content struct {
			Body string `json:"body"`
		} `json:"content"`
	}
	if err := json.Unmarshal([]byte(payload), &body); err != nil {
		t.Fatalf("unmarshal %s: %v", what, err)
	}
	if body.Content.Body != want {
		t.Fatalf("%s reads %q, and %q was sent", what, body.Content.Body, want)
	}
}

// liveCheckTheEmoji compares the emoji on a reaction against the one that went out. The
// empty string is a real value here and is how a removal is spelled, which is why this
// takes what it wants rather than asking whether there is one.
func liveCheckTheEmoji(t *testing.T, what string, payload json.RawMessage, want string) {
	t.Helper()

	var body struct {
		Emoji string `json:"emoji"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("unmarshal %s: %v", what, err)
	}
	if body.Emoji != want {
		t.Fatalf("%s carries emoji %q, and %q went out", what, body.Emoji, want)
	}
}

// liveWhoIs is both spellings of the account a session is signed in as, read out of that
// session's own device. Ground truth for who sent something has to come from the sending
// side: taking it off the event under test would have a wrong answer agree with itself.
func liveWhoIs(t *testing.T, session *Session) protocol.Party {
	t.Helper()

	device := session.current().Store
	if device.ID == nil {
		t.Fatalf("the session is not signed in, so there is nobody to compare a sender against")
	}
	who := protocol.Party{Phone: device.ID.User}
	if !device.LID.IsEmpty() {
		who.LID = device.LID.User
	}
	return who
}

// liveAwaitAbout drains until an event of the wanted type carries `field` equal to
// `want`. The field is named rather than the payload being decoded into a type, because
// the three events this phase reads name their target in three different fields and a
// helper per event would be three copies of this loop.
//
// Draining rather than taking the next one is the point: a live account receives its own
// receipts, presence and whatever else arrives while a phase runs, and "the next event"
// is only the answer on an account nobody else is using.
func liveAwaitAbout(
	t *testing.T, events *recorder, want protocol.EventType, field, id string, within time.Duration,
) *engine.Emission {
	t.Helper()

	deadline := time.After(within)
	for {
		select {
		case emission, ok := <-events.seen:
			if !ok {
				t.Fatalf("the session ended while waiting for %s about %s", want, id)
			}
			if emission.Type == protocol.EventSessionLoggedOut {
				t.Fatalf("the account was logged out while waiting for %s: %s", want, emission.Payload)
			}
			if emission.Type != want {
				continue
			}
			var named map[string]json.RawMessage
			if err := json.Unmarshal(emission.Payload, &named); err != nil {
				t.Fatalf("unmarshal a %s: %v", want, err)
			}
			var got string
			if raw, ok := named[field]; ok {
				if err := json.Unmarshal(raw, &got); err != nil {
					t.Fatalf("read %s off a %s: %v", field, want, err)
				}
			}
			if got == id {
				return &emission
			}
			fmt.Fprintf(os.Stderr, "ignoring a %s whose %s is %q\n", want, field, got)
		case <-deadline:
			t.Fatalf("no %s naming %s in %s arrived within %s%s",
				want, field, id, within, events.overflowed())
		}
	}
}

// liveEdit corrects a message through the connector's own command, which is what makes
// this phase runnable without a human at a phone.
func liveEdit(t *testing.T, from *Session, to protocol.Address, target, body string) {
	t.Helper()

	liveCommand(t, from, protocol.CommandMessageEdit, map[string]any{
		"message_id": from.current().GenerateMessageID(),
		"to":         map[string]any{"kind": to.Kind, "id": to.ID},
		"target_id":  target,
		"content":    map[string]any{"type": "text", "body": body},
	})
}

// liveReact puts an emoji on a message, or takes it off when the emoji is empty, and says
// which id it went out under. A client deduplicates reactions on that id and matches its
// own sends by it, so the id is as load-bearing as the emoji and has to be checked
// against something the receiving side did not invent.
func liveReact(t *testing.T, from *Session, to protocol.Address, target, emoji string) string {
	t.Helper()

	reactionID := from.current().GenerateMessageID()
	liveCommand(t, from, protocol.CommandMessageReact, map[string]any{
		"message_id":     reactionID,
		"to":             map[string]any{"kind": to.Kind, "id": to.ID},
		"target_id":      target,
		"target_from_me": true,
		"emoji":          emoji,
	})
	return reactionID
}

// liveRevokeAsAdmin deletes somebody else's message, which only a group admin can do and
// which the contract spells as the `participant` naming whose message it is.
func liveRevokeAsAdmin(t *testing.T, from *Session, to protocol.Address, target string, whose protocol.Address) {
	t.Helper()

	liveCommand(t, from, protocol.CommandMessageRevoke, map[string]any{
		"to":          map[string]any{"kind": to.Kind, "id": to.ID},
		"target_id":   target,
		"participant": map[string]any{"kind": whose.Kind, "id": whose.ID},
	})
}

// liveCommand marshals and executes, so the three helpers above are their payload and
// nothing else.
func liveCommand(t *testing.T, from *Session, kind protocol.CommandType, payload map[string]any) {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("build a %s: %v", kind, err)
	}
	if _, err := from.Execute(t.Context(), &protocol.Command{Type: kind, Payload: body}); err != nil {
		t.Fatalf("%s: %v", kind, err)
	}
}
