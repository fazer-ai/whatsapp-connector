package protocol

// ErrorCode is the closed set of failure reasons a reply or a command.failed event
// can carry. Clients branch on it, so a new code is a protocol change: an unknown
// code must be handled as ErrorInternal rather than surfaced raw.
type ErrorCode string

// Every error code in the contract.
//
// Three of these are published and never sent. They are marked below with what a client
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
	// ErrorQuarantined is reserved, and now deliberately so rather than for want of a
	// mechanism.
	//
	// There is a quarantine: `wa:quarantine:<sid>` counts the failures of a session the
	// connector could not bring back and says how long the fleet leaves it alone, from a
	// minute up to an hour. What it gates is what the connector does on its own -- the
	// sweep that resumes an account nobody is running -- and nothing else.
	//
	// A client that asks for a connection gets one, during a quarantine like at any other
	// time, which is why no command is answered with this. The operator is the one part
	// of the system that may know why the last attempt failed, and a backoff that refused
	// them would be a wall in front of the person fixing it. What a client sees is what it
	// saw before: the connect failures themselves.
	ErrorQuarantined ErrorCode = "quarantined"
	// ErrorClientOutdated is reserved: the condition is an event, not a reply.
	//
	// WhatsApp refusing this build's version reaches a client as
	// `session.client_outdated`, published from the session that heard it. No command is
	// refused with this code, and a command that arrives while the condition holds fails
	// on the disconnected socket rather than on the version.
	ErrorClientOutdated ErrorCode = "client_outdated"
	ErrorRateLimited    ErrorCode = "rate_limited"
	ErrorExpired        ErrorCode = "expired"
	ErrorTimeout        ErrorCode = "timeout"
	// ErrorNotAttempted is the connector saying it knows the command never left this
	// process, which is the one thing `timeout` cannot say.
	//
	// `timeout` is the answer for a command whose outcome nobody here can tell: a send
	// that ran out of time may already be on somebody's phone. This is the opposite and
	// the connector is certain of it -- nothing was written to WhatsApp, so whatever the
	// command was about is exactly as it was, and the caller's retry does the whole thing
	// rather than the half that is left.
	//
	// The teardowns are where the difference is worth a word. A `session.delete` whose
	// unlink was refused is a teardown that goes through, because the retry would have
	// nothing to do; one whose unlink was never attempted leaves an account that is still
	// linked, and a caller that read `timeout` there had to assume the teardown had gone
	// half way and that the device could no longer be removed.
	ErrorNotAttempted ErrorCode = "not_attempted"
	// ErrorNotSettled is the connector saying the outcome exists and it cannot yet be told
	// which one it is. It is the third answer about time, and the other two are wrong here.
	//
	// `timeout` says nobody here can tell, and leaves it there: a send that ran out of time
	// may be on somebody's phone and nothing afterwards will say. `not_attempted` says
	// nothing happened at all. This one says something did happen, the connector will know
	// which shortly, and asking again in a moment settles it -- so a client retries this
	// one and pages a human for `internal`, which is this connector having a bug.
	//
	// `group.create` is where it is produced. Two requests for a group of the same name,
	// both still open, make a group that is evidence for either and proof for neither;
	// answering one of them with it would hand that request the other's conversation and
	// skip the creation it asked for, so the delivery is refused until WhatsApp's own
	// notification names which request made which group.
	ErrorNotSettled             ErrorCode = "not_settled"
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
	ErrorNotAttempted,
	ErrorNotSettled,
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
