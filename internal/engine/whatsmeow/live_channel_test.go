//go:build live

// A post in a channel being corrected and deleted, watched from a follower. This is the
// channel half of #40, and the half no group phase can stand in for.
//
// What made it reachable: the account under test creates the channel, so it is the owner
// and can post, correct and delete; the counterpart follows it and is where the events
// have to show up. Nobody has to sit at a phone, which is what #40 said this half would
// need.
//
// The two things being measured are the ones the connector reasons about rather than
// observes. A channel names what a correction changes by the post's own stanza id, not by
// a key in the body, and `theCorrection` reads `NewsletterMeta.EditTS` for the clock. A
// channel's deletion has the same asymmetry, and `revokeOf` falls back to `event.Info.ID`
// when the body carries no key. Both came from reading whatsmeow's *send* path and
// assuming the receive side is symmetric.
//
//	WAC_LIVE_CHANNEL=<jid> go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveChannelMessageChange
//
// The channel is created on the first run and its JID printed; pass it back afterwards.
// Creating one per run leaves channels on the account that somebody has to delete by hand.
package whatsmeow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// liveChannelWindow is how long each leg gets. A channel fans a post out to its
// followers, which is not the same delivery as a direct message, and nothing here has
// measured how long that takes.
const liveChannelWindow = 90 * time.Second

func TestLiveChannelMessageChange(t *testing.T) {
	subject, counterpart, _ := liveBoth(t, MediaOptions{})

	liveResume(t, subject)
	liveResume(t, counterpart)

	channel := liveChannel(t, subject)
	live := liveFollowChannel(t, counterpart, channel)
	liveChannelReaches(t, counterpart, channel)
	t.Logf("the counterpart follows %s, live updates for %s", channel, live)

	// Opened before the post, on the follower: it is where a channel's events land, and
	// the owner's own copy is not what #40 is asking about.
	watching := watch(t, counterpart)

	to := protocol.Address{Kind: protocol.AddressNewsletter, ID: channel.User}

	post := liveSayTo(t, subject, to, "wac channel: the original")
	received := watching.awaitMessage(t, post, liveChannelWindow)
	liveCheckTheChannelChat(t, "the post", received, to)

	const corrected = "wac channel: corrected"
	liveEdit(t, subject, to, post, corrected)
	edited := liveAwaitAbout(t, watching, protocol.EventMessageEdited, "message_id", post, liveChannelWindow)
	liveCheckTheChannelChat(t, "the correction", edited.Payload, to)
	liveCheckTheBody(t, "the correction", string(edited.Payload), corrected)

	// The whole question for a channel: the correction has to name the post, and the only
	// thing naming it is the post's own stanza id. Publishing the correction's own id
	// leaves a client looking for a message nobody stored.
	liveCheckTheCorrection(t, edited, post)

	// The deletion, and it does not work. Pinned to what was measured rather than to what
	// `revokeOf` was written for, so that whoever fixes it is told by a failing test
	// instead of finding this phase asserting something that has not been true.
	//
	// What happens: `message.revoke` is accepted, whatsmeow builds the newsletter stanza
	// the way its own send path says to -- `edit=admin_revoke`, no body, the stanza id
	// rewritten to the post's -- WhatsApp answers without an error, and the post stays in
	// the channel. Nothing is published to the follower or to the owner, so the only side
	// that believes the post is gone is the client that asked. Recorded as #134.
	liveRevokeOwn(t, subject, to, post)

	if arrived := liveNothingAbout(t, watching, protocol.EventMessageRevoked, post, 30*time.Second); arrived {
		t.Fatalf("a deletion of %s reached the follower, which #134 says does not happen: "+
			"if this is fixed, assert the deletion here and close it", post)
	}
	if !liveChannelStillHas(t, subject, channel, post) {
		t.Fatalf("%s is gone from the channel, so the deletion worked after all: "+
			"#134 is fixed on WhatsApp's side and this phase has to assert it now", post)
	}
	t.Logf("the deletion left %s in the channel and published nothing, which is #134", post)

	for name, session := range map[string]*Session{"subject": subject, "counterpart": counterpart} {
		if state := session.state(); state != "open" {
			t.Fatalf("the %s session did not stay up: state=%s", name, state)
		}
	}
}

// liveCheckTheChannelChat is the group phase's chat check without the sender half: a
// channel post's author is the channel, so there is no member to name, and `from_me` is
// not worked out for a newsletter stanza at all.
func liveCheckTheChannelChat(t *testing.T, what string, payload json.RawMessage, in protocol.Address) {
	t.Helper()

	var body struct {
		Chat protocol.Address `json:"chat"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("unmarshal %s: %v", what, err)
	}
	if body.Chat != in {
		t.Fatalf("%s was published under chat %+v, and it happened in %+v: %s",
			what, body.Chat, in, payload)
	}
	fmt.Fprintf(os.Stderr, "%s: in %s %s\n", what, body.Chat.Kind, body.Chat.ID)
}

// liveChannel is the channel under test, created on the first run.
func liveChannel(t *testing.T, owner *Session) waTypes.JID {
	t.Helper()

	if named := os.Getenv("WAC_LIVE_CHANNEL"); named != "" {
		jid, err := waTypes.ParseJID(named)
		if err != nil {
			t.Fatalf("WAC_LIVE_CHANNEL is not a JID: %v", err)
		}
		return jid
	}
	info, err := owner.current().CreateNewsletter(t.Context(), whatsmeow.CreateNewsletterParams{
		Name:        "wac " + time.Now().Format("01-02 15:04"),
		Description: "channel used by the connector's live suite",
	})
	if err != nil {
		t.Fatalf("create the channel: %v", err)
	}
	t.Logf("created %s -- pass WAC_LIVE_CHANNEL=%s to reuse it", info.ID, info.ID)
	return info.ID
}

// liveFollowChannel puts the counterpart on the channel and asks for live updates.
//
// The subscription is the part worth noticing: nothing in this connector calls
// `NewsletterSubscribeLiveUpdates`, so a follower's session gets a channel's posts only
// for as long as something else asked for them. This phase asks, because otherwise it
// would be measuring the absence of a subscription rather than what the connector does
// with a post that arrives.
func liveFollowChannel(t *testing.T, follower *Session, channel waTypes.JID) time.Duration {
	t.Helper()

	if err := follower.current().FollowNewsletter(t.Context(), channel); err != nil {
		t.Fatalf("follow %s: %v", channel, err)
	}
	live, err := follower.current().NewsletterSubscribeLiveUpdates(t.Context(), channel)
	if err != nil {
		t.Fatalf("subscribe to live updates on %s: %v", channel, err)
	}
	return live
}

// liveRevokeOwn deletes the account's own message, which is the only deletion a channel
// has: a post's author is the channel, so there is no participant to name.
func liveRevokeOwn(t *testing.T, from *Session, to protocol.Address, target string) {
	t.Helper()

	liveCommand(t, from, protocol.CommandMessageRevoke, map[string]any{
		"to":        map[string]any{"kind": to.Kind, "id": to.ID},
		"target_id": target,
	})
}

// liveChannelReaches waits until the follower's own session knows it follows the channel.
//
// The follow and the subscription are answered before either has propagated, and a post
// sent in that gap reaches nobody -- which the first run of this phase spent ninety
// seconds proving, on a channel created moments earlier. Waited on rather than slept
// through, for the reason AGENTS.md gives: a fixed delay is right until the day WhatsApp
// is slower than the number somebody guessed.
func liveChannelReaches(t *testing.T, follower *Session, channel waTypes.JID) {
	t.Helper()

	// One deadline over the whole wait, not a count of attempts. Each query is an IQ and
	// whatsmeow gives an IQ 75 seconds, so "N tries, half a second apart" is not the bound
	// it looks like.
	waiting, give := context.WithTimeout(t.Context(), time.Minute)
	defer give()
	for attempt := 0; ; attempt++ {
		asking, stop := context.WithTimeout(waiting, 5*time.Second)
		following, err := follower.current().GetSubscribedNewsletters(asking)
		stop()
		if err == nil && slices.ContainsFunc(following, func(one *waTypes.NewsletterMetadata) bool {
			return one.ID == channel
		}) {
			return
		}

		select {
		case <-waiting.Done():
			t.Fatalf("the follower still did not know it follows %s after %d attempts: %v",
				channel, attempt+1, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// liveNothingAbout is the negative of liveAwaitAbout: whether an event of that type naming
// this message turned up at all. Not a Fatal either way -- the caller is what decides
// which answer is the failure, and here the absence is what is being recorded.
func liveNothingAbout(
	t *testing.T, events *recorder, want protocol.EventType, id string, within time.Duration,
) bool {
	t.Helper()

	deadline := time.After(within)
	for {
		select {
		case emission, ok := <-events.seen:
			if !ok {
				return false
			}
			if emission.Type != want {
				continue
			}
			var named struct {
				MessageID string `json:"message_id"`
			}
			if err := json.Unmarshal(emission.Payload, &named); err != nil {
				t.Fatalf("unmarshal a %s: %v", want, err)
			}
			if named.MessageID == id {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

// liveChannelStillHas asks the channel itself, which is the only side that can settle
// whether a deletion took effect: the connector publishes nothing either way, so watching
// events would only measure the silence again.
func liveChannelStillHas(t *testing.T, owner *Session, channel waTypes.JID, post string) bool {
	t.Helper()

	posts, err := owner.current().GetNewsletterMessages(t.Context(), channel,
		&whatsmeow.GetNewsletterMessagesParams{Count: 20})
	if err != nil {
		t.Fatalf("read the channel back: %v", err)
	}
	return slices.ContainsFunc(posts, func(one *waTypes.NewsletterMessage) bool {
		return one.MessageID == post
	})
}
