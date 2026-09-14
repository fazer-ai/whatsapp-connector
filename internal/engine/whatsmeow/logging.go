package whatsmeow

import (
	"fmt"
	"regexp"
	"sync"
	"time"

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
	// authenticated is shared with the session this logger was handed to, and nil on a Sub:
	// see the note on Sub below.
	authenticated *authStamp
}

// authenticatedLine is what whatsmeow logs at the instant it authenticates a socket.
//
// Watched for because this is the only instant of a socket's own life that reaches this
// process synchronously. `handleConnectSuccess` logs this line, writes the same instant to
// `Client.LastSuccessfulConnect`, and only then starts the goroutine that announces the
// connection -- the goroutine that waits behind the prekey count, the prekey upload and the
// passive IQ before dispatching `events.Connected`. The field would be the obvious source
// and is not usable: it is a plain struct field, written by whichever connection is
// authenticating and read here from the goroutine announcing the previous one, which is a
// data race on the very path this exists for. The line arrives on the library's own
// goroutine and is written down under a lock of ours, which is the same instant without the
// race (#181).
//
// A coupling to a string, and the failure it can have is the safe one: a library that stops
// logging it leaves the stamp unset, and an unset stamp falls back to the announcement,
// which is where this was before. `TestTheLineWhatsmeowLogsWhenItAuthenticatesIsTheOneWatched`
// reads it back out of the dependency so the day it changes is a red test rather than a
// quiet loss.
const authenticatedLine = "Successfully authenticated"

// authStamp is when the library last said it authenticated a socket, written by whatever
// goroutine the library logs from and read by the session.
type authStamp struct {
	mu   sync.Mutex
	now  func() time.Time
	last time.Time
}

// driveWith hands the stamp the clock the session reads, so a test that drives one drives
// both. Called once, while the session is being built and before the client it belongs to
// can log anything.
func (a *authStamp) driveWith(now func() time.Time) {
	a.mu.Lock()
	a.now = now
	a.mu.Unlock()
}

func (a *authStamp) record() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.now != nil {
		a.last = a.now()
		return
	}
	a.last = time.Now()
}

// authenticatedAt is the zero instant until the library has announced an authentication,
// which is what a client that has never connected looks like from here.
func (a *authStamp) authenticatedAt() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.last
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
	return &quietLogger{log: entry.Logger(), authenticated: &authStamp{}}
}

func (l *quietLogger) Warnf(msg string, args ...any)  { l.log.Warn().Msg(mask(msg, args...)) }
func (l *quietLogger) Errorf(msg string, args ...any) { l.log.Error().Msg(mask(msg, args...)) }

// Infof and Debugf write nothing down. Debug is where the pairing codes and the protocol
// nodes are; info is where the library announces an authentication and a pairing, by JID.
// The connector reports its own session lifecycle as events, which is where a reader should
// be looking for it anyway.
//
// One info line is still noticed, and the instant it arrives at is the whole of what is
// kept: nothing of the line itself is recorded or logged.
func (l *quietLogger) Infof(msg string, _ ...any) {
	if l.authenticated != nil && msg == authenticatedLine {
		l.authenticated.record()
	}
}

func (l *quietLogger) Debugf(string, ...any) {}

// Sub does not carry the stamp, and that is deliberate: the line it watches for is logged by
// the client's own logger, which is the one handed over here, and a module logger that also
// watched would be a guard for a path the library does not take. The fence that reads the
// dependency is what says so, and it is where a move would be caught.
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
	return redact(rendered)
}

// redact is the same for a line that is already rendered, which is what an error this
// package writes down itself is: a media request URL is built out of the direct path
// and the hash, and both are long enough to match.
func redact(line string) string {
	return secretShaped.ReplaceAllString(line, "[redacted]")
}
