package meowcaller

import (
	"github.com/purpshell/meowcaller/diag"
	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow/types/events"
)

// Option configures optional aspects of the call/media types: the diagnostic logger, and
// the gate on which inbound offers are engaged. The zero configuration logs nothing and
// engages every offer.
type Option func(*config)

type config struct {
	log       zerolog.Logger
	diag      *diag.Recorder
	offerGate func(*events.CallOffer) bool
}

func resolveConfig(opts []Option) config {
	c := config{log: zerolog.Nop()}
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// WithLogger sets the zerolog logger for debug/trace diagnostics. The library never
// configures logging itself; without this option the types are silent at zero cost.
// Pass the logger from a context, e.g. WithLogger(*zerolog.Ctx(ctx)).
func WithLogger(l zerolog.Logger) Option {
	return func(c *config) { c.log = l }
}

// WithDiagnostics attaches a developer-only *diag.Recorder that dumps exact,
// per-category call diagnostics (including raw secrets and media) to JSONL files.
// This is an opt-in maintainer carve-out from the library's sanitized logging and
// must never be enabled in production. Without it the recorder is nil and every
// diag emit is a no-op at zero cost.
func WithDiagnostics(rec *diag.Recorder) Option {
	return func(c *config) { c.diag = rec }
}

// WithOfferGate decides, per inbound offer, whether this client takes part in the call
// at all. An offer the gate refuses is left alone: no preaccept, no per-call state and
// no OnIncomingCall, so the call rings on the account's other devices exactly as it
// would without this client. Without a gate every offer is engaged.
func WithOfferGate(gate func(*events.CallOffer) bool) Option {
	return func(c *config) { c.offerGate = gate }
}
