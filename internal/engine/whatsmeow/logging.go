package whatsmeow

import (
	"fmt"
	"regexp"

	"github.com/rs/zerolog"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// quietLogger is what whatsmeow is allowed to write down.
//
// The library logs the material this connector must never keep. Its pairing channel
// logs every raw QR code at debug, its client logs the nodes it sends and receives, and
// its info lines announce an authentication and a pairing by JID. What survives here is
// the warning and the error, which is what an operator reads when a session will not
// connect, with the payloads inside them masked: the process redactor covers
// phone-shaped tokens and nothing else, so key material and node dumps would go out
// intact.
type quietLogger struct {
	log zerolog.Logger
}

// newLibraryLogger returns the logger handed to whatsmeow.
//
// The session id is a field of its own rather than the first Sub: whatsmeow makes its
// own Sub calls for its modules, each overwriting the last, and a line that loses the
// sid is a line nobody can tie to an account.
//
//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func newLibraryLogger(log zerolog.Logger, sid string) waLog.Logger {
	entry := log.With().Str("component", "whatsmeow")
	if sid != "" {
		entry = entry.Str("sid", sid)
	}
	return &quietLogger{log: entry.Logger()}
}

func (l *quietLogger) Warnf(msg string, args ...any)  { l.log.Warn().Msg(mask(msg, args...)) }
func (l *quietLogger) Errorf(msg string, args ...any) { l.log.Error().Msg(mask(msg, args...)) }

// Infof and Debugf go nowhere. Debug is where the pairing codes and the protocol nodes
// are; info is where the library announces an authentication and a pairing, by JID. The
// connector reports its own session lifecycle as events, which is where a reader should
// be looking for it anyway.
func (l *quietLogger) Infof(string, ...any)  {}
func (l *quietLogger) Debugf(string, ...any) {}

func (l *quietLogger) Sub(module string) waLog.Logger {
	return &quietLogger{log: l.log.With().Str("module", module).Logger()}
}

// secretShaped is a run long enough to be key material, a node dump or a base64 blob
// rather than a word. whatsmeow embeds those in warnings and errors as context, and
// none of it is anything an operator acts on.
var secretShaped = regexp.MustCompile(`[A-Za-z0-9+/=_-]{24,}`)

// mask renders a library line with its long payloads taken out.
func mask(msg string, args ...any) string {
	rendered := msg
	if len(args) > 0 {
		rendered = fmt.Sprintf(msg, args...)
	}
	return secretShaped.ReplaceAllString(rendered, "[redacted]")
}
