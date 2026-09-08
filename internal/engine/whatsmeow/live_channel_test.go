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
	to := protocol.Address{Kind: protocol.AddressNewsletter, ID: channel.User}

	// Opened before anything is sent, on the follower: it is where a channel's events
	// land, and the owner's own copy is not what #40 is asking about.
	watching := watch(t, counterpart)
	liveChannelDelivers(t, subject, counterpart, channel, to, watching)

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

	// Nothing arriving is what is being recorded, and nothing is also what a session that
	// stopped delivering looks like. So a post sent after the deletion is what closes it:
	// arriving, it proves the subscription was live across the moment the deletion
	// crossed, which turns the silence in between into evidence rather than an absence.
	// Same shape `TestLiveWatchAShare` uses for a vote.
	closing := liveSayTo(t, subject, to, "wac channel: after the deletion")
	arrived := liveNothingAbout(t, watching, protocol.EventMessageRevoked, post, closing, liveChannelWindow)
	if dropped := watching.dropped.Load(); dropped > 0 {
		t.Fatalf("%d emissions were dropped, so the absence of a deletion for %s proves "+
			"nothing: this watcher's buffer filled before anything drained it", dropped, post)
	}
	if arrived {
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

// liveRevokeOwn deletes the account's own message, which is the only deletion a channel
// has: a post's author is the channel, so there is no participant to name.
func liveRevokeOwn(t *testing.T, from *Session, to protocol.Address, target string) {
	t.Helper()

	liveCommand(t, from, protocol.CommandMessageRevoke, map[string]any{
		"to":        map[string]any{"kind": to.Kind, "id": to.ID},
		"target_id": target,
	})
}

// liveNothingAbout is the negative of liveAwaitAbout: whether an event of that type naming
// `id` turned up before `closing` did.
//
// The clock is a message and not a timer, which is the whole point. A deadline expiring
// says the event did not arrive in that long; a later message arriving says the session
// was delivering across the window, and only then is the silence about the event rather
// than about the connection. The timeout stays as a bound on the wait itself, and reaching
// it is a failure: without the closing message there is nothing to conclude from.
func liveNothingAbout(
	t *testing.T, events *recorder, want protocol.EventType, id, closing string, within time.Duration,
) bool {
	t.Helper()

	found := false
	deadline := time.After(within)
	for {
		select {
		case emission, ok := <-events.seen:
			if !ok {
				t.Fatalf("the session ended before %s closed the window", closing)
			}
			switch emission.Type {
			case want:
				var named struct {
					MessageID string `json:"message_id"`
				}
				if err := json.Unmarshal(emission.Payload, &named); err != nil {
					t.Fatalf("unmarshal a %s: %v", want, err)
				}
				if named.MessageID == id {
					found = true
				}
			case protocol.EventMessageReceived:
				var body struct {
					Message struct {
						ID string `json:"id"`
					} `json:"message"`
				}
				if err := json.Unmarshal(emission.Payload, &body); err != nil {
					t.Fatalf("unmarshal a message: %v", err)
				}
				if body.Message.ID == closing {
					return found
				}
			}
		case <-deadline:
			t.Fatalf("%s never arrived within %s%s, so nothing can be concluded about %s: "+
				"the session may simply have stopped delivering", closing, within,
				events.overflowed(), id)
		}
	}
}

// liveChannelDelivers gets the follower to a state where a post actually reaches it, and
// proves it rather than assuming it.
//
// Two things have to have happened, and neither is done when its call returns. The follow
// propagates on its own schedule, and the live-updates subscription is temporary -- it is
// asked for per run and it is what a channel's posts are delivered under, so a channel
// reused through WAC_LIVE_CHANNEL is already in `GetSubscribedNewsletters` from a previous
// run while this run's subscription has not taken effect.
//
// So the check is a post that arrives. That is the precondition every leg below needs,
// stated as itself instead of through a proxy, and the first run of this phase spent
// ninety seconds discovering what a proxy is worth here.
func liveChannelDelivers(
	t *testing.T, owner, follower *Session, channel waTypes.JID, to protocol.Address, events *recorder,
) {
	t.Helper()

	if err := follower.current().FollowNewsletter(t.Context(), channel); err != nil {
		t.Fatalf("follow %s: %v", channel, err)
	}

	const attempts = 4
	for attempt := 1; ; attempt++ {
		live, err := follower.current().NewsletterSubscribeLiveUpdates(t.Context(), channel)
		if err != nil {
			t.Fatalf("subscribe to live updates on %s: %v", channel, err)
		}

		priming := liveSayTo(t, owner, to, fmt.Sprintf("wac channel: priming %d", attempt))
		if _, arrived := events.sawMessage(t, priming, 20*time.Second); arrived {
			t.Logf("%s is delivering to the follower, live updates for %s", channel, live)
			return
		}
		if attempt == attempts {
			t.Fatalf("%d posts to %s never reached the follower, so this phase cannot tell "+
				"a connector problem from a subscription that never took effect%s",
				attempts, channel, events.overflowed())
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
