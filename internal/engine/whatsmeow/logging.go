package whatsmeow

import (
	"github.com/rs/zerolog"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// quietLogger passes whatsmeow's logging through, minus its debug level.
//
// whatsmeow debug-logs the material the connector must never write down: the pairing
// channel logs every raw QR code it emits, and the client logs the nodes it sends and
// receives. The process redactor masks phone-shaped tokens and nothing else, so a
// deployment set to debug for an unrelated reason would ship pairing credentials and
// session internals to wherever its logs go.
//
// Warnings and errors stay: those are what an operator needs when a session will not
// connect, and they are written for a human rather than for a protocol trace.
type quietLogger struct {
	log zerolog.Logger
}

// newLibraryLogger returns the logger handed to whatsmeow.
//
//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func newLibraryLogger(log zerolog.Logger) waLog.Logger {
	return &quietLogger{log: log.With().Str("component", "whatsmeow").Logger()}
}

func (l *quietLogger) Warnf(msg string, args ...any)  { l.log.Warn().Msgf(msg, args...) }
func (l *quietLogger) Errorf(msg string, args ...any) { l.log.Error().Msgf(msg, args...) }
func (l *quietLogger) Infof(msg string, args ...any)  { l.log.Info().Msgf(msg, args...) }

// Debugf is where the credentials are, so it goes nowhere.
func (l *quietLogger) Debugf(string, ...any) {}

func (l *quietLogger) Sub(module string) waLog.Logger {
	return &quietLogger{log: l.log.With().Str("module", module).Logger()}
}
