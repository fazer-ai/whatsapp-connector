package protocol

// ErrorCode is the closed set of failure reasons a reply or a command.failed event
// can carry. Clients branch on it, so a new code is a protocol change: an unknown
// code must be handled as ErrorInternal rather than surfaced raw.
type ErrorCode string

// Every error code in the contract.
const (
	ErrorInvalidPayload             ErrorCode = "invalid_payload"
	ErrorUnsupported                ErrorCode = "unsupported"
	ErrorSessionNotFound            ErrorCode = "session_not_found"
	ErrorNotConnected               ErrorCode = "not_connected"
	ErrorNotPaired                  ErrorCode = "not_paired"
	ErrorOwnedElsewhere             ErrorCode = "owned_elsewhere"
	ErrorQuarantined                ErrorCode = "quarantined"
	ErrorClientOutdated             ErrorCode = "client_outdated"
	ErrorRateLimited                ErrorCode = "rate_limited"
	ErrorExpired                    ErrorCode = "expired"
	ErrorTimeout                    ErrorCode = "timeout"
	ErrorMediaTooLarge              ErrorCode = "media_too_large"
	ErrorMediaUnavailable           ErrorCode = "media_unavailable"
	ErrorRecipientNotOnWhatsapp     ErrorCode = "recipient_not_on_whatsapp"
	ErrorGroupParticipantNotAllowed ErrorCode = "group_participant_not_allowed"
	ErrorWaError                    ErrorCode = "wa_error"
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
