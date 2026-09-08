package protocol

// ErrorCode is the closed set of failure reasons a reply or a command.failed event
// can carry. Clients branch on it, so a new code is a protocol change: an unknown
// code must be handled as ErrorInternal rather than surfaced raw.
type ErrorCode string

// Every error code in the contract.
//
// Four of these are published and never sent. They are marked below with what a client
// receives instead, because a code a client can branch on and nothing produces is a
// branch that never runs, and the situation it names is answered by something else or
// by nothing at all. Kept rather than removed: they are in the schema, so dropping one
// narrows an enum a client may already match on, which is a breaking change and a
// version bump for codes nobody sends (#96).
const (
	ErrorInvalidPayload ErrorCode = "invalid_payload"
	ErrorUnsupported    ErrorCode = "unsupported"
	// ErrorSessionNotFound is reserved: nothing sends it.
	//
	// A command for a session this instance does not run is released un-acknowledged so
	// that the instance which does own it can read the same stream, and that is
	// deliberate. What no path covers is a command for a session *no* instance runs:
	// nobody answers and nobody fails it, and the caller waits out its own deadline.
	//
	// Sending it needs a way to tell a session that does not exist from one whose owner
	// has just died, and those look identical from here -- the lease is absent in both.
	// Answering the second would end a command for a session that comes back seconds
	// later, which is worse than the silence, because the caller has already acted on it.
	ErrorSessionNotFound ErrorCode = "session_not_found"
	ErrorNotConnected    ErrorCode = "not_connected"
	ErrorNotPaired       ErrorCode = "not_paired"
	ErrorOwnedElsewhere  ErrorCode = "owned_elsewhere"
	// ErrorQuarantined is reserved: nothing sends it, because nothing quarantines.
	//
	// `redisx.Keys.Quarantine` names a key and no code writes or reads it, so a session
	// that fails to connect over and over goes on being retried. A client sees the
	// connect failures themselves and nothing that says the fleet has given up on the
	// account for a while.
	ErrorQuarantined ErrorCode = "quarantined"
	// ErrorClientOutdated is reserved: the condition is an event, not a reply.
	//
	// WhatsApp refusing this build's version reaches a client as
	// `session.client_outdated`, published from the session that heard it. No command is
	// refused with this code, and a command that arrives while the condition holds fails
	// on the disconnected socket rather than on the version.
	ErrorClientOutdated         ErrorCode = "client_outdated"
	ErrorRateLimited            ErrorCode = "rate_limited"
	ErrorExpired                ErrorCode = "expired"
	ErrorTimeout                ErrorCode = "timeout"
	ErrorMediaTooLarge          ErrorCode = "media_too_large"
	ErrorMediaUnavailable       ErrorCode = "media_unavailable"
	ErrorRecipientNotOnWhatsapp ErrorCode = "recipient_not_on_whatsapp"
	// ErrorGroupParticipantNotAllowed is one participant of a `group.participants.update`
	// that WhatsApp would not carry out because of who they are: an add refused on their
	// privacy setting, answered with an invite to send them instead. It is a row of that
	// command's answer and never the command's own failure, because the same request
	// carries the participants it did go through.
	ErrorGroupParticipantNotAllowed ErrorCode = "group_participant_not_allowed"
	ErrorWaError                    ErrorCode = "wa_error"
	ErrorProviderUnavailable        ErrorCode = "provider_unavailable"
	ErrorInternal                   ErrorCode = "internal"
)

// AllErrorCodes lists every error code in the contract.
var AllErrorCodes = []ErrorCode{
	ErrorInvalidPayload,
	ErrorUnsupported,
	ErrorSessionNotFound,
	ErrorNotConnected,
	ErrorNotPaired,
	ErrorOwnedElsewhere,
	ErrorQuarantined,
	ErrorClientOutdated,
	ErrorRateLimited,
	ErrorExpired,
	ErrorTimeout,
	ErrorMediaTooLarge,
	ErrorMediaUnavailable,
	ErrorRecipientNotOnWhatsapp,
	ErrorGroupParticipantNotAllowed,
	ErrorWaError,
	ErrorProviderUnavailable,
	ErrorInternal,
}

// Error is the payload of a failed reply.
type Error struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message,omitempty"`
}

func (e *Error) Error() string {
	if e.Message == "" {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Message
}

// NewError builds an Error, degrading a code outside the contract to
// ErrorInternal so a client never sees a code it cannot branch on.
func NewError(code ErrorCode, message string) *Error {
	if !code.Valid() {
		return &Error{Code: ErrorInternal, Message: message}
	}
	return &Error{Code: code, Message: message}
}

// Valid reports whether the code is part of the contract.
func (c ErrorCode) Valid() bool {
	for _, known := range AllErrorCodes {
		if known == c {
			return true
		}
	}
	return false
}
