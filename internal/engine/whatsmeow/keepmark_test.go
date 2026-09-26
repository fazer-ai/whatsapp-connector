package whatsmeow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	wm "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// The mark a write site puts on its failure reaches the layer that reads it, through the
// classifier that turns the failure into the contract's words, and only when the write is
// unanswered (#282).
//
// The write fence (TestEveryWriteAReservedCommandMakesIsMarked) sees that the mark is put
// on; it cannot see that it survives. Every classifier here builds a fresh protocol error,
// and one that dropped the mark released the attempt, so a group rename whose answer was
// lost was carried out again on the redelivery. This runs each classifier the reserved
// commands go through, from the seam to what Execute returns, which is what carryOut reads.
func TestTheMarkSurvivesClassificationOnlyForAnUnansweredWrite(t *testing.T) {
	t.Parallel()

	type outcome struct {
		name   string
		raw    error
		marked bool
		code   protocol.ErrorCode
	}
	// The same failures for each classifier: three with the write out and no answer, three
	// where the outcome is known -- refused by WhatsApp, or never sent -- and a deadline,
	// which could be either.
	outcomes := []outcome{
		// No code: what a disconnect reads as is each classifier's own business, and the
		// presence one, which never sends a query, has no word for it.
		{"the connection went before the answer", &wm.DisconnectedError{Action: "info query"}, true, ""},
		{"the retry after a reconnect lost its answer", &wm.DisconnectedError{Action: "info query (retry)"}, true, ""},
		{"the answer did not come in time", wm.ErrIQTimedOut, true, protocol.ErrorTimeout},
		{"WhatsApp refused it", &wm.IQError{Code: 403, Text: "forbidden"}, false, protocol.ErrorWaError},
		{"the socket was gone before the write", wm.ErrNotConnected, false, protocol.ErrorNotConnected},
		{"there was no client to write with", wm.ErrClientIsNil, false, protocol.ErrorNotConnected},
		// Ambiguous, and so not marked: the same error ends a store read before the node is
		// built, where nothing went out.
		{"the command's deadline ran out", context.DeadlineExceeded, false, protocol.ErrorTimeout},
		// How SendAppState words a 409 whose conflicting patches could not be fetched:
		// the write was refused, and the timeout is the download's, not the write's.
		{"a refused patch whose conflicts could not be fetched", fmt.Errorf("%w (also, parsing patches in the response failed: %w)",
			fmt.Errorf("%w: conflict", wm.ErrAppStateUpdate), wm.ErrIQTimedOut), false, ""},
	}

	commands := []struct {
		kind protocol.CommandType
		run  func(t *testing.T, raw error) error
	}{
		{protocol.CommandGroupNameSet, func(t *testing.T, raw error) error {
			session := settableSession(t)
			session.setName = func(context.Context, *wm.Client, waTypes.JID, string) error {
				return engine.MayHaveLanded(raw)
			}
			_, err := session.Execute(t.Context(), setCommand(t, protocol.CommandGroupNameSet,
				`{"group":{"kind":"group","id":"1"},"subject":"x"}`))
			return err
		}},
		{protocol.CommandGroupInviteGet, func(t *testing.T, raw error) error {
			session := settableSession(t)
			session.inviteLink = func(context.Context, *wm.Client, waTypes.JID, bool) (string, error) {
				return "", engine.MayHaveLanded(raw)
			}
			_, err := session.Execute(t.Context(), setCommand(t, protocol.CommandGroupInviteGet,
				`{"group":{"kind":"group","id":"1"},"revoke":true}`))
			return err
		}},
		{protocol.CommandMessageMarkUnread, func(t *testing.T, raw error) error {
			session, _ := newTestSession(t, "5511999990001")
			session.setConnected(true)
			session.sendAppState = func(context.Context, *wm.Client, appstate.PatchInfo) error {
				return engine.MayHaveLanded(raw)
			}
			_, err := session.Execute(t.Context(), unreadCommand(t,
				`{"chat":{"kind":"phone","id":"5511999990002"}}`))
			return err
		}},
		{protocol.CommandPresenceSet, func(t *testing.T, raw error) error {
			session, _ := availableSession(t)
			session.sendPresence = func(context.Context, *wm.Client, waTypes.Presence) error {
				return engine.MayHaveLanded(raw)
			}
			_, err := session.setPresence(t.Context(), presenceCommand("available"))
			return err
		}},
	}

	for _, command := range commands {
		for _, happened := range outcomes {
			t.Run(string(command.kind)+"/"+happened.name, func(t *testing.T) {
				t.Parallel()

				err := command.run(t, happened.raw)
				if err == nil {
					t.Fatal("the command succeeded over a write that failed")
				}
				if got := errors.Is(err, engine.ErrMayHaveLanded); got != happened.marked {
					t.Fatalf("marked=%v, want %v: %v", got, happened.marked, err)
				}
				if happened.code != "" {
					assertCode(t, err, happened.code)
				}
			})
		}
	}
}

// A read mark whose socket went away after readyToSend looked comes back from whatsmeow's
// sendNode as ErrNotConnected, before anything is written. Entering MarkRead is not a
// write, and marked, the attempt would stand for a day over a receipt that never went out.
func TestAReadMarkThatNeverReachedTheSocketIsNotMarked(t *testing.T) {
	t.Parallel()

	session, _, _ := outboundSession(t)
	session.privacyKnown = func(context.Context) error { return nil }
	_, err := session.markRead(t.Context(), &protocol.Command{
		Type:    protocol.CommandMessageMarkRead,
		Payload: json.RawMessage(`{"chat":{"kind":"phone","id":"5511999990002"},"message_ids":["3EB0A1B2C3D4E5F60718"]}`),
	})
	assertCode(t, err, protocol.ErrorNotConnected)
	if errors.Is(err, engine.ErrMayHaveLanded) {
		t.Fatal("a read mark refused before the socket is marked as possibly landed")
	}
}
