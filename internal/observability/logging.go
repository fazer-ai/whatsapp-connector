package observability

import (
	"io"
	"regexp"
	"strings"

	"github.com/rs/zerolog"
)

// A phone number is a token that is nothing but 8 to 15 digits. The connector logs
// session ids, event types and command ids; a number reaching a log line is a mistake
// at the call site, and this is the net under it.
//
// Whole tokens rather than a digit run inside one, which is what keeps a session id
// (`2f1c6f0e-0000-4000-8000-000000000001`) intact: its last group is twelve digits and
// a run-based rule masks it, taking with it the one field every line is read for.
var (
	tokenPattern = regexp.MustCompile(`[\w+-]+`)
	phoneShaped  = regexp.MustCompile(`^\+?\d{8,15}$`)
	redacted     = []byte("[redacted]")
)

// NewLogger returns the process logger. JSON always: these logs are shipped and
// queried, not read off a terminal.
func NewLogger(out io.Writer, level, instance, version string) zerolog.Logger {
	parsed, err := zerolog.ParseLevel(strings.ToLower(level))
	if err != nil || parsed == zerolog.NoLevel {
		parsed = zerolog.InfoLevel
	}
	return zerolog.New(redactor{out: out}).
		Level(parsed).
		With().
		Timestamp().
		Str("inst", instance).
		Str("version", version).
		Logger()
}

// redactor masks anything phone-shaped on its way out, which is the only place that
// sees every line however it was built.
type redactor struct{ out io.Writer }

func (r redactor) Write(p []byte) (int, error) {
	masked := tokenPattern.ReplaceAllFunc(p, func(token []byte) []byte {
		if phoneShaped.Match(token) {
			return redacted
		}
		return token
	})
	if _, err := r.out.Write(masked); err != nil {
		return 0, err
	}
	// The caller is told its own line was written. Reporting the redacted length
	// instead makes zerolog see a short write and report an error for a line that
	// went out intact.
	return len(p), nil
}
