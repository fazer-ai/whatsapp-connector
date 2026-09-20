package session

import (
	"errors"
	"fmt"
	"testing"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// What keeps a command's attempt standing, one outcome per row.
//
// The mark and nothing else. A table over every code the contract has, because the whole
// safety of #282 is that this predicate says no unless the engine said yes at the line that
// writes: a code that starts keeping attempts on its own would refuse retries for work that
// never happened, and there is nothing in a code alone that can tell a write that went out
// from one that never started -- `not_connected` is answered by both.
func TestOnlyAMarkedFailureKeepsTheAttempt(t *testing.T) {
	t.Parallel()

	for _, code := range protocol.AllErrorCodes {
		t.Run(string(code), func(t *testing.T) {
			t.Parallel()

			if keepsTheAttempt(protocol.NewError(code, "measured")) {
				t.Errorf("%s keeps the attempt on its own, so a retry after a refusal that "+
					"touched nothing is answered `timeout` until the record expires: a device "+
					"left linked, a message that never goes out", code)
			}
			if !keepsTheAttempt(engine.MayHaveLanded(protocol.NewError(code, "measured"))) {
				t.Errorf("%s stops keeping the attempt once the engine has marked it, so a "+
					"redelivery carries out again a write that may already be on WhatsApp", code)
			}
		})
	}

	t.Run("wrapped, because that is how it arrives", func(t *testing.T) {
		t.Parallel()

		wrapped := fmt.Errorf("carry out c1: %w", engine.MayHaveLanded(errors.New("the socket went")))
		if !keepsTheAttempt(wrapped) {
			t.Error("the mark stops counting once the error has been wrapped on its way up")
		}
	})

	t.Run("a plain error keeps nothing", func(t *testing.T) {
		t.Parallel()

		if keepsTheAttempt(errors.New("something with no code at all")) {
			t.Error("an unmarked error keeps the attempt")
		}
	})
}
