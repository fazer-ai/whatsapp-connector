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
	"testing"
	"time"

	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// liveCouldNotDecrypt is whatsmeow's own words for a stanza that would not open. Matched
// on the library's phrasing because there is nothing else to match on: this is the one
// fact in the phase that reaches neither an event nor a store row.
const liveCouldNotDecrypt = "Error decrypting message"

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

	inbox := watch(t, subject)
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

		// What whatsmeow says when a stanza will not open. Counted before and after, so
		// the round can tell its own failure from one left over from the round before.
		failedBefore := liveTranscript.saying(liveCouldNotDecrypt)

		body := fmt.Sprintf("conector nativo, recuperacao %d de %d", len(took)+1, rounds)
		started := time.Now()
		sent := liveSay(t, counterpart, liveMustBePaired(t, container, liveSID).User, body)
		inbox.awaitMessage(t, sent, 5*time.Minute)
		elapsed := time.Since(started)

		// The scenario, verified after the fact rather than arranged and assumed. Asking
		// beforehand whether a session existed is the weaker question and was the first
		// thing tried: it answers about the address the phase happens to name, and the
		// first three attempts here deleted rows that were real and were not the ones the
		// message would use -- reporting 195ms as a recovery time, which is what an
		// ordinary delivery costs.
		if liveTranscript.saying(liveCouldNotDecrypt) == failedBefore {
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
// What the run says is that the message arrives anyway. The retry is not answered by the
// device that sent it: it is answered by the account, and a real account has a phone. So
// the unbounded tail needs *every* device of the sender offline, which for anyone who is
// not deliberately arranging it means their phone is off.
func TestLiveARecoveryOutlivesTheSenderLeaving(t *testing.T) {
	subject, counterpart, container := liveBoth(t, MediaOptions{})

	subjectJID := liveMustBePaired(t, container, liveSID)
	counterpartJID := liveMustBePaired(t, container, liveCounterpartSID)

	subject.rerequestWait = time.Hour

	// Broken while the account is up, because breaking it is a call on a live client, and
	// then taken down: the store keeps the deletion, so what comes back has no session
	// with the counterpart and no way to have quietly rebuilt one.
	liveResume(t, subject)
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
	liveResume(t, counterpart)
	sent := liveSay(t, counterpart, subjectJID.User, "conector nativo, remetente que sai e nao volta")
	if err := counterpart.Disconnect(t.Context()); err != nil {
		t.Fatalf("take the sender offline: %v", err)
	}

	inbox := watch(t, subject)
	failedBefore := liveTranscript.saying(liveCouldNotDecrypt)
	started := time.Now()
	liveResume(t, subject)

	// Past the window on purpose: if it takes longer than this, the placeholder in
	// production has already gone out and the real message is discarded behind it.
	within := 2 * rerequestTimeout
	if !liveArrival(t, inbox, sent, within) {
		t.Fatalf("the message did not arrive within %s with the sender's session offline; "+
			"the recovery needs that session back, and the tail is then unbounded", within)
	}
	elapsed := time.Since(started)

	if liveTranscript.saying(liveCouldNotDecrypt) == failedBefore {
		t.Fatal("nothing failed to decrypt, so the deleted session was not the one this " +
			"message used and the run measured an ordinary delivery")
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
