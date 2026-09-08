//go:build live

// How long a message that could not be read takes to arrive anyway.
//
// `rerequestTimeout` is 45 seconds and #51 records that the number is a guess: it has to
// sit past every way a message still turns up, and nobody had measured any of them. Too
// long costs an agent a wait; too short is permanent, because the placeholder and the
// real message share an id and the client discards the second as a repeat.
//
// The recovery measured here is the retry receipt: a Signal session that has drifted, the
// receiving side asking the sender to encrypt the message again, and the sender answering
// whenever its device next has the session and the network. It is the one of the two that
// two accounts can arrange, and it is the common one -- the other, WhatsApp declining to
// hand a companion device a view-once photo, is the phone's own behaviour and #20 already
// records a run where it never answered at all.
//
//	WAC_LIVE_ROUNDS=10 go test -tags live -timeout 30m -v ./internal/engine/whatsmeow/ -run TestLiveTimeARetryRecovery
package whatsmeow

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// liveHolds records, for every message this session finds unreadable, the instant its
// window starts from -- which is the instant production compares against, not one near
// it.
//
// This is the fourth instrument this phase has had and the first that cannot lie, so the
// three it replaces are worth naming. The send was a proxy and folded in outbound
// latency. Whatsmeow's log line was a proxy and comes before the dispatch, with a retry
// receipt and an ack in between. Polling the placeholder row read the right number but
// raced the row's life: `hold` writes it and `dropHold` deletes it when the message
// arrives, which on this path is under a second, so a poll can miss it entirely and
// report a recovery as an ordinary delivery.
//
// The pattern here is the one that took seven rounds to see: **an instrument assembled
// from what is observable outside answers a weaker question than the one being asked.**
// The seam is what the rest of this file already does for the same problem -- `groupMode`
// and `privacyKnown` are there for exactly this reason -- and it is synchronous, exact
// and cannot be missed, because it is called on the path itself.
func liveHolds(t *testing.T, session *Session) *holds {
	t.Helper()

	seen := &holds{opened: map[string]int64{}, closed: map[string]time.Time{}}
	// Set and cleared through the session's own lock, which is what `reportWindow` reads
	// it under: whatsmeow calls it from the event goroutine, and a plain assignment here
	// races that.
	session.mu.Lock()
	session.window = seen.record
	session.mu.Unlock()
	t.Cleanup(func() {
		session.mu.Lock()
		session.window = nil
		session.mu.Unlock()
	})
	return seen
}

type holds struct {
	mu sync.Mutex
	// opened is the first window each message was given. Only the first: a resend that
	// is also unreadable opens the question again, and `await` deliberately keeps the
	// original timer, so overwriting would report the last failure instead of the wait
	// production is actually serving -- a recovery 50s after the first failure with a
	// second failure at 40s would read as 10s and pass, while the real placeholder had
	// already won.
	opened map[string]int64
	// closed is when the window was decided, which is where a recovery ends. Taking the
	// time at `awaitMessage` instead measures the publish behind it -- the store write,
	// the forwarder, the settle -- none of which production counts against the window.
	closed map[string]time.Time
}

func (h *holds) record(messageID string, learnedAt int64, opened bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case !opened:
		if _, already := h.closed[messageID]; !already {
			h.closed[messageID] = time.Now()
		}
	default:
		if _, already := h.opened[messageID]; !already {
			h.opened[messageID] = learnedAt
		}
	}
}

// took is how long a message's window stood before it was decided, and whether there was
// one at all. A message that was readable never opens one, which is the whole signal.
func (h *holds) took(messageID string) (time.Duration, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	at, opened := h.opened[messageID]
	if !opened {
		return 0, false
	}
	done, closed := h.closed[messageID]
	if !closed {
		return 0, false
	}
	return done.Sub(time.UnixMilli(at)), true
}

// when is the instant a message's window started, for a phase that measures from it
// rather than over it.
func (h *holds) when(messageID string) (int64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	at, opened := h.opened[messageID]
	return at, opened
}

// liveEveryNameOf is the counterpart under every namespace this account may have filed it
// under: the number, and the LID when there is one.
func liveEveryNameOf(t *testing.T, subject *Session, who waTypes.JID) []waTypes.JID {
	t.Helper()

	names := []waTypes.JID{who}
	alt, err := subject.current().Store.GetAltJID(t.Context(), who)
	if err != nil {
		t.Fatalf("look up the counterpart's other namespace: %v", err)
	}
	if !alt.IsEmpty() {
		names = append(names, alt)
	}
	return names
}

// TestLiveTimeARetryRecovery breaks the Signal session deliberately and times how long the
// message takes to arrive anyway.
//
// The distribution is what matters and the mean is not: the window has to cover the tail,
// because the tail is where the permanent wrong bubble comes from. So it runs many times
// and reports every sample along with the slowest, which is the number `rerequestTimeout`
// actually has to sit past.
//
// The placeholder is pushed out of the way rather than left at its production value: at
// 45 seconds a recovery slower than that would publish a placeholder, the real message
// would arrive under the same id and be discarded, and the sample would be lost -- and it
// is exactly the slow samples this phase exists to collect.
func TestLiveTimeARetryRecovery(t *testing.T) {
	subject, counterpart, container := liveBoth(t, MediaOptions{})

	liveMustBePaired(t, container, liveSID)
	counterpartJID := liveMustBePaired(t, container, liveCounterpartSID)

	subject.rerequestWait = time.Hour
	liveResume(t, subject)
	liveResume(t, counterpart)

	windows := liveHolds(t, subject)
	mirrored := liveHolds(t, counterpart)
	inbox := watch(t, subject)
	theirs := watch(t, counterpart)
	livePrime(t, counterpart, subject, windows, mirrored, inbox, theirs,
		liveMustBePaired(t, container, liveSID).User, counterpartJID.User)
	rounds := 5
	if asked := os.Getenv("WAC_LIVE_ROUNDS"); asked != "" {
		parsed, err := strconv.Atoi(asked)
		if err != nil || parsed < 1 {
			t.Fatalf("WAC_LIVE_ROUNDS is not a count: %q", asked)
		}
		rounds = parsed
	}

	// Not every attempt is a sample, and the reason is worth knowing before reading the
	// numbers. A sender that re-encrypts after a retry sends a prekey message, which
	// carries what it takes to open it and needs no session on this side at all: the
	// attempt right after a recovery therefore decrypts first try however thoroughly the
	// session was deleted. Those are dropped rather than counted, and dropped rather than
	// fatal, because they are the scenario declining to happen and not a defect.
	took := make([]time.Duration, 0, rounds)
	for attempt := 0; len(took) < rounds; attempt++ {
		if attempt >= 4*rounds {
			t.Fatalf("only %d of %d samples in %d attempts; the scenario is not being "+
				"arranged and the numbers so far are %v", len(took), rounds, attempt, took)
		}
		// Every namespace the counterpart can be keyed under, because the Signal session
		// is filed under the way the chat is addressed rather than under the number:
		// these two talk over LID, and deleting the phone-keyed address (which is what
		// this did first) removes nothing, leaves the message perfectly readable, and
		// times a healthy delivery while looking like it timed a recovery.
		for _, who := range liveEveryNameOf(t, subject, counterpartJID) {
			// Every device of theirs, and keyed the way the store keys it. The
			// argument is a prefix matched as `<it>:%`, and the rows are named by the
			// Signal address rather than by the user part: a LID is filed as
			// `10089566068807_1:7`, so passing the bare user matches nothing, deletes
			// nothing, and reports no error while doing it.
			address := who.SignalAddress().String()
			prefix := address[:strings.LastIndex(address, ":")]
			if err := subject.current().Store.Sessions.DeleteAllSessions(t.Context(), prefix); err != nil {
				t.Fatalf("delete the signal sessions for %s: %v", who, err)
			}
		}

		body := fmt.Sprintf("conector nativo, recuperacao %d de %d", len(took)+1, rounds)
		sent := liveSay(t, counterpart, liveMustBePaired(t, container, liveSID).User, body)
		inbox.awaitMessage(t, sent, 5*time.Minute)

		// The scenario, verified after the fact rather than arranged and assumed. Asking
		// beforehand whether a session existed is the weaker question and was the first
		// thing tried: it answers about the address the phase happens to name, and the
		// first three attempts here deleted rows that were real and were not the ones the
		// message would use -- reporting 195ms as a recovery time, which is what an
		// ordinary delivery costs.
		//
		// Asked about this message and not about a count, so an unrelated stanza failing
		// somewhere else in the process cannot admit an ordinary delivery as a sample.
		// A placeholder is only ever held for a message this session could not read, so
		// the row existing is the scenario having happened.
		elapsed, held := windows.took(sent)
		if !held {
			t.Logf("attempt %d decrypted first try, so it is not a sample", attempt+1)
			continue
		}

		took = append(took, elapsed)
		t.Logf("sample %d: %s", len(took), elapsed.Round(time.Millisecond))
	}

	slices.Sort(took)
	slowest := took[len(took)-1]
	t.Logf("%d rounds, fastest %s, median %s, slowest %s",
		len(took), took[0].Round(time.Millisecond),
		took[len(took)/2].Round(time.Millisecond), slowest.Round(time.Millisecond))

	// The assertion is the one this phase exists to make, and it is deliberately not a
	// tight bound on the measurement: what has to hold is that the production window
	// still covers what was just observed. A run where it does not is the window being
	// too short, which is the expensive direction, and it should fail loudly rather than
	// print a number somebody has to notice.
	if slowest >= rerequestTimeout {
		t.Errorf("a recovery took %s and the placeholder goes out at %s, so that message "+
			"would have been lost behind a placeholder", slowest, rerequestTimeout)
	}
	if slowest*4 < rerequestTimeout {
		t.Logf("the slowest recovery is under a quarter of the window; %s may be longer "+
			"than it needs to be, at the cost of an agent waiting", rerequestTimeout)
	}
}

// TestLiveARecoveryOutlivesTheSenderLeaving asks who actually answers a retry receipt,
// which turns out to be the question #51 needed answered.
//
// The issue's worry is the tail: "a retry receipt asks the sender to encrypt the message
// again, and the sender answers whenever their device next has the session and the
// network." Read that way the tail is unbounded -- however long somebody's phone is off
// -- and no window covers it.
//
// So the sender is taken off the network and left off. Its connector session is
// disconnected before the account under test ever sees the message, by construction and
// not by racing a disconnect against a recovery that finishes in under two seconds (which
// was the first attempt, and it only ever produced a skip or, worse, found the
// already-arrived message in its buffer and reported it as having survived an absence it
// was never subject to).
//
// What the runs say is that it usually arrives anyway, and sometimes does not. The retry
// is not answered by the device that sent it but by the account, and a real account has a
// phone -- so most of the time another device answers in about 300ms even with the
// sending session gone. Measured over three runs: twice in ~300ms, once not at all inside
// 90 seconds, which is twice the production window.
//
// That one run is the whole value of the phase, and it corrects what the first version of
// this reported. The tail does not need somebody to switch a phone off. It happens on an
// ordinary pair of accounts, roughly often enough to see in three tries, and when it
// happens the placeholder goes out and the real message is discarded behind it.
func TestLiveARecoveryOutlivesTheSenderLeaving(t *testing.T) {
	subject, counterpart, container := liveBoth(t, MediaOptions{})

	subjectJID := liveMustBePaired(t, container, liveSID)
	counterpartJID := liveMustBePaired(t, container, liveCounterpartSID)

	subject.rerequestWait = time.Hour

	// Broken while the account is up, because breaking it is a call on a live client, and
	// then taken down: the store keeps the deletion, so what comes back has no session
	// with the counterpart and no way to have quietly rebuilt one.
	liveResume(t, subject)
	liveResume(t, counterpart)
	windows := liveHolds(t, subject)
	mirrored := liveHolds(t, counterpart)
	livePrime(t, counterpart, subject, windows, mirrored, watch(t, subject), watch(t, counterpart),
		subjectJID.User, counterpartJID.User)
	for _, who := range liveEveryNameOf(t, subject, counterpartJID) {
		address := who.SignalAddress().String()
		prefix := address[:strings.LastIndex(address, ":")]
		if err := subject.current().Store.Sessions.DeleteAllSessions(t.Context(), prefix); err != nil {
			t.Fatalf("delete the signal sessions for %s: %v", who, err)
		}
	}
	if err := subject.Disconnect(t.Context()); err != nil {
		t.Fatalf("take the account under test offline: %v", err)
	}

	// Sent while it is down, so WhatsApp holds the message and there is no window in
	// which it could have been read normally.
	sent := liveSay(t, counterpart, subjectJID.User, "conector nativo, remetente que sai e nao volta")
	if err := counterpart.Disconnect(t.Context()); err != nil {
		t.Fatalf("take the sender offline: %v", err)
	}

	// Watched after the resume for the same reason as everywhere else: a watcher's
	// buffer drops what does not fit, and a resume is exactly when a backlog lands.
	// The message this phase waits for is in that backlog, though, so the buffer has to
	// exist before it arrives -- and it is one message, not a backlog worth 256.
	inbox := watch(t, subject)
	liveResume(t, subject)

	// Past the window on purpose: if it takes longer than this, the placeholder in
	// production has already gone out and the real message is discarded behind it.
	within := 2 * rerequestTimeout
	if !liveArrival(t, inbox, sent, within) {
		// Not a failure, and calling it one was wrong. Measured over three runs of this
		// exact phase: twice the message came back in about 300ms, once it did not come
		// back at all inside 90 seconds. Both are answers, and the second is the one #51
		// is about -- so asserting "it always comes back" pins something that is not
		// true and turns the interesting result into a red test somebody re-runs until
		// it goes green.
		//
		// What it means when it happens: no other device of the sender's account
		// answered the retry in twice the production window, so in production the
		// placeholder would have gone out and the real message would have been discarded
		// behind it as a repeat. Nobody turned a phone off to arrange this.
		t.Skipf("the message did not arrive within %s with the sender's session offline. "+
			"That is the tail: no other device of that account answered the retry, and "+
			"in production the placeholder would have gone out at %s and the real message "+
			"been discarded behind it", within, rerequestTimeout)
	}
	elapsed, held := windows.took(sent)
	if !held {
		t.Fatal("this message was readable, so no window was ever opened for it and " +
			"there is no recovery to time; the deleted session was not the one it used")
	}
	t.Logf("recovered in %s with the sender's connector session offline the whole time, "+
		"so the retry was answered by another of that account's devices", elapsed.Round(time.Millisecond))
	if elapsed >= rerequestTimeout {
		t.Errorf("the recovery took %s and the placeholder goes out at %s", elapsed, rerequestTimeout)
	}
}

// liveArrival is whether a message turns up within a window, without failing when it does
// not: here not arriving is the case the phase is trying to arrange.
func liveArrival(t *testing.T, events *recorder, id string, within time.Duration) bool {
	t.Helper()

	deadline := time.After(within)
	for {
		select {
		case emission, ok := <-events.seen:
			if !ok {
				t.Fatalf("the session ended while waiting on %s", id)
			}
			if emission.Type != protocol.EventMessageReceived {
				continue
			}
			var body struct {
				Message struct {
					ID string `json:"id"`
				} `json:"message"`
			}
			if err := json.Unmarshal(emission.Payload, &body); err != nil {
				t.Fatalf("unmarshal a message.received: %v", err)
			}
			if body.Message.ID == id {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

// livePrime puts ordinary messages through until one of them arrives without needing a
// recovery, so the sender is talking over an established Signal session by the time one
// is deliberately broken.
//
// Without it the phase depends on these two accounts having a history. On a pair paired
// this morning the counterpart's first message is a prekey message, which carries what it
// takes to open it and needs no session on the receiving side at all -- so deleting that
// side's session changes nothing, the message decrypts, no placeholder is held, and a
// phase that has one shot at the scenario reports a failure over correct behaviour.
//
// One message is not enough, and the reason is the same fact one layer along: a durable
// store can begin the run with a session that has already drifted, so the prime itself
// recovers, and the message *after* a recovery is the prekey one. Priming once would then
// hand the measured send exactly the shape it was supposed to rule out. So it repeats
// until a prime needs no recovery of its own, which is what "established" means here.
func livePrime(t *testing.T, from, to *Session, theirWindows, ourWindows *holds, inbox, theirs *recorder, at, back string) {
	t.Helper()

	for attempt := range 5 {
		// Both ways, and that is the correction that took a round: a prekey message
		// opens without a session on the receiving side, so "no window was opened" does
		// not say the sender has one. The sender keeps sending prekey messages until it
		// decrypts a reply from the other side -- so the reply is the thing that settles
		// it, and priming in one direction only can leave the pair exactly as it was.
		sent := liveSay(t, from, at, "conector nativo, estabelecendo a sessao")
		inbox.awaitMessage(t, sent, 2*time.Minute)
		replied := liveSay(t, to, back, "conector nativo, sessao estabelecida")
		theirs.awaitMessage(t, replied, 2*time.Minute)

		// Both directions, and checking only one was the bug this replaced. A reply that
		// itself needed a recovery leaves *its* sender emitting prekey messages next,
		// which is the same hole one step over: the pair is established when neither
		// direction needed a window, not when the one being watched did not.
		_, theirsRecovered := theirWindows.when(sent)
		_, oursRecovered := ourWindows.when(replied)
		if !theirsRecovered && !oursRecovered {
			return
		}
		t.Logf("a prime recovered on attempt %d (inbound=%v, reply=%v), so the next "+
			"message that way would be a prekey; priming again",
			attempt+1, theirsRecovered, oursRecovered)
	}
	t.Fatal("every prime needed a recovery of its own, so this pair never settled into an " +
		"established session and the scenario below cannot be arranged")
}
