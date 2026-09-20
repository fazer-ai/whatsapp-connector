package session

import (
	"cmp"
	"errors"
	"fmt"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// What counts as certainty that nothing reached WhatsApp, one outcome per row.
//
// A table and not a handful of end-to-end cases, because the two ways of being wrong here
// are opposite and both are silent. Too permissive and a command whose effect landed has
// its attempt released and is carried out again by the next delivery, which is the whole of
// what #282 exists to stop. Too strict and a refusal that provably touched nothing is held
// against every retry for as long as the record lives: a device that stays linked, a
// message that never goes out under its id.
//
// Every code the contract has is listed, so a new one cannot be added and quietly fall into
// whichever half its author did not think about.
func TestWhatCountsAsCertaintyThatNothingWasSent(t *testing.T) {
	t.Parallel()

	certain := map[protocol.ErrorCode]string{
		protocol.ErrorNotAttempted: "the contract's own word for it: the connector knows the " +
			"command never left this process, which is what the teardowns answer when the " +
			"socket lock was never free",
		protocol.ErrorUnsupported: "an engine that does not implement the command cannot have " +
			"sent it",
		protocol.ErrorInvalidPayload: "a payload that would not decode never became anything " +
			"to put on the wire",
	}
	// The ones worth spelling out among the rest, because each is a way of being wrong.
	because := map[protocol.ErrorCode]string{
		protocol.ErrorNotConnected: "it says both `never started` and `the socket died with " +
			"the frame already written`, and only the engine knows which -- that is what the " +
			"sentinel is for",
		protocol.ErrorTimeout:    "nobody here knows the outcome, which is the opposite of certainty",
		protocol.ErrorInternal:   "a bug here says nothing about what WhatsApp was told",
		protocol.ErrorNotSettled: "something did happen and which is being decided",
	}

	for _, code := range protocol.AllErrorCodes {
		t.Run(string(code), func(t *testing.T) {
			t.Parallel()

			err := protocol.NewError(code, "measured")
			want, isCertain := certain[code]
			got := neverReachedWhatsApp(err)
			switch {
			case isCertain && !got:
				t.Errorf("%s does not release the attempt, and it should: %s", code, want)
			case !isCertain && got:
				t.Errorf("%s releases the attempt, and it must not: %s", code,
					cmp.Or(because[code], "nothing about this code says the command was never sent, "+
						"and releasing on it carries a landed effect out a second time"))
			}
		})
	}

	t.Run("the engine's mark, whatever the code", func(t *testing.T) {
		t.Parallel()

		marked := engine.NeverSent(protocol.NewError(protocol.ErrorNotConnected, "refused before the socket"))
		if !neverReachedWhatsApp(marked) {
			t.Error("a refusal the engine marked as never sent does not release its attempt")
		}
		if neverReachedWhatsApp(protocol.NewError(protocol.ErrorNotConnected, "died mid-write")) {
			t.Error("an unmarked not_connected releases its attempt, so a send that landed " +
				"would be carried out again")
		}
	})

	t.Run("wrapped, because that is how it arrives", func(t *testing.T) {
		t.Parallel()

		wrapped := fmt.Errorf("carry out c1: %w", engine.NeverSent(errors.New("refused")))
		if !neverReachedWhatsApp(wrapped) {
			t.Error("the mark stops counting once the error has been wrapped on its way up")
		}
	})
}
