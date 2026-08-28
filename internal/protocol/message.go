package protocol

// AddressKind is what an address points at. The contract keeps it separate from the id
// because the same digits mean different accounts under different kinds: a LID and a
// phone number are both digits and are not interchangeable.
type AddressKind string

// The address kinds the contract defines.
const (
	AddressPhone      AddressKind = "phone"
	AddressLID        AddressKind = "lid"
	AddressGroup      AddressKind = "group"
	AddressNewsletter AddressKind = "newsletter"
	AddressBroadcast  AddressKind = "broadcast"
	AddressStatus     AddressKind = "status"
)

// Address is a WhatsApp addressable entity as it travels on the wire. The id never
// carries the @server suffix, the device or the agent part, so a client can compare
// two addresses without knowing how WhatsApp spells them this month.
type Address struct {
	Kind AddressKind `json:"kind"`
	ID   string      `json:"id"`
}

// Party is the human or business behind an address. Both identifiers are carried when
// both are known, because a client that has only ever seen one of them still has to
// recognise the other as the same person: WhatsApp addresses the same account by phone
// number in one chat and by LID in the next.
type Party struct {
	Phone        string `json:"phone,omitempty"`
	LID          string `json:"lid,omitempty"`
	PushName     string `json:"push_name,omitempty"`
	VerifiedName string `json:"verified_name,omitempty"`
}

// TextContent is a message whose whole body is text.
type TextContent struct {
	Type string `json:"type"`
	Body string `json:"body"`
}

// Text returns the content of a plain text message.
func Text(body string) TextContent { return TextContent{Type: "text", Body: body} }

// InboundMessage is one message as the client receives it, whoever sent it: `from_me`
// separates a message this account sent from another device from one that arrived.
//
// Content is left open because the contract's `content` is a union and the engine is
// what knows which arm it is rendering.
type InboundMessage struct {
	ID        string    `json:"id"`
	Chat      Address   `json:"chat"`
	Sender    *Party    `json:"sender,omitempty"`
	FromMe    bool      `json:"from_me"`
	Timestamp int64     `json:"timestamp"`
	Content   any       `json:"content"`
	QuotedID  string    `json:"quoted_id,omitempty"`
	Mentions  []Address `json:"mentions,omitempty"`
	// Ephemeral is the disappearing-message expiration in seconds, zero when the chat
	// is not on a timer.
	Ephemeral uint32 `json:"ephemeral,omitempty"`
}

// MediaKind is what sort of file a media message carries. The contract's enum is
// smaller than WhatsApp's own set of message types on purpose: an animated GIF is a
// video, a voice note is an audio with `voice_note` set, and an animated sticker is
// still a sticker. A client renders five things rather than fifteen.
type MediaKind string

// The media kinds the contract defines.
const (
	MediaImage    MediaKind = "image"
	MediaVideo    MediaKind = "video"
	MediaAudio    MediaKind = "audio"
	MediaDocument MediaKind = "document"
	MediaSticker  MediaKind = "sticker"
)

// MediaRefKind is how the bytes behind a media message are reached. The enum is shared
// across every provider that speaks this contract, so a client written against it can
// resolve a reference without knowing which one issued it.
type MediaRefKind string

// The reference kinds the contract defines.
const (
	// MediaRefURL is a URL whoever holds it can fetch. It is what an outbound
	// attachment carries and what survives being handed on.
	MediaRefURL MediaRefKind = "url"
	// MediaRefConnectorBlob is a file on the instance that downloaded it, served over
	// that instance's own HTTP port against the token the registry publishes. It means
	// nothing to anybody else, and it is what this connector issues.
	MediaRefConnectorBlob MediaRefKind = "connector_blob"
	// MediaRefUazapiMessage is a message id only the Uazapi instance that saw it can
	// resolve. This connector never issues one; the constant is here because the enum
	// is the contract's, not this implementation's.
	MediaRefUazapiMessage MediaRefKind = "uazapi_message"
)

// MediaRef is how a client fetches the bytes of a media message. Media never travels
// inside a frame: an event says where the file is, and the client goes and gets it.
type MediaRef struct {
	Kind    MediaRefKind      `json:"kind"`
	ID      string            `json:"id,omitempty"`
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Size    int64             `json:"size,omitempty"`
	Mime    string            `json:"mime,omitempty"`
	SHA256  string            `json:"sha256,omitempty"`
	// ExpiresAt is when the issuer stops promising to serve this, in epoch
	// milliseconds. A blob cache drops what nobody collects, so a reference is a
	// reference to a file for a while and not for good: a client that finds it lapsed
	// asks the session for the bytes again rather than telling an agent the media is
	// gone.
	ExpiresAt int64 `json:"expires_at,omitempty"`
}

// MediaContent is a message whose body is a file.
//
// Everything but the reference describes the file well enough to render a bubble
// without having fetched it: the caption reads, the name and size show, and the
// thumbnail stands in until the bytes arrive. A nil Ref is the file not coming, which
// is what `media.download_failed` explains.
type MediaContent struct {
	Type      string    `json:"type"`
	Kind      MediaKind `json:"kind"`
	Mime      string    `json:"mime,omitempty"`
	Filename  string    `json:"filename,omitempty"`
	Caption   string    `json:"caption,omitempty"`
	VoiceNote bool      `json:"voice_note"`
	Size      int64     `json:"size,omitempty"`
	// Duration is how long an audio or a video runs, in seconds.
	Duration uint32 `json:"duration,omitempty"`
	// Thumbnail is a data: URI small enough to travel inside the frame, which is the
	// one exception to media never doing so.
	Thumbnail string    `json:"thumbnail,omitempty"`
	Ref       *MediaRef `json:"ref,omitempty"`
}

// Media returns the content of a media message of the given kind, for a caller that
// fills in what it knows about the file.
func Media(kind MediaKind) MediaContent { return MediaContent{Type: "media", Kind: kind} }

// MediaDownloadFailure is `media.download_failed`: the bytes of a message the client
// already has are not coming, and asking again would not help.
//
// It is published after the message it is about, never instead of it. The client looks
// the message up to flag it, and a failure that arrives first names a message nobody
// has stored yet.
type MediaDownloadFailure struct {
	Chat      Address `json:"chat"`
	MessageID string  `json:"message_id"`
	Reason    string  `json:"reason"`
}

// MessageEdited is `message.edited`: somebody corrected a message the client already
// has. MessageID names the message being corrected, never the stanza that carries the
// correction, because the client's whole job here is to find the bubble and rewrite it.
//
// Content is the message as it now reads. A media caption edit carries the media arm
// with the new caption and no Ref: an edit changes what a message says and never the
// file it carries, so the reference the client got with the original message is still
// the right one, and issuing a second one would mint a second blob for the same bytes.
type MessageEdited struct {
	Chat      Address `json:"chat"`
	Sender    *Party  `json:"sender,omitempty"`
	MessageID string  `json:"message_id"`
	FromMe    bool    `json:"from_me"`
	Content   any     `json:"content"`
	Timestamp int64   `json:"timestamp"`
}

// RevokedBy is who deleted a message for everyone, from the account's point of view.
type RevokedBy string

// The revokers the contract names. There is no third arm for a group admin deleting
// somebody else's message: the account either performed the deletion or it did not, and
// `sender` is what says who it was.
const (
	RevokedByContact RevokedBy = "contact"
	RevokedBySelf    RevokedBy = "self"
)

// MessageRevoked is `message.revoked`: a message the client already has was deleted for
// everyone. The two revokers are not the same event to a client — a deletion this
// account performed is applied the way its own deletion is, files included, while one
// somebody else performed only flags the bubble and leaves the text an agent can still
// read.
type MessageRevoked struct {
	Chat      Address   `json:"chat"`
	Sender    *Party    `json:"sender,omitempty"`
	MessageID string    `json:"message_id"`
	By        RevokedBy `json:"by"`
	Timestamp int64     `json:"timestamp"`
}

// MessageReaction is `message.reaction`: somebody put an emoji on a message, or took
// one back.
//
// ID is the reaction's own message id, which is what makes it deduplicable and what
// matches the echo of a reaction this client sent. Emoji is never omitted: an empty one
// is the removal, and leaving the field out would read as a reaction with no emoji
// rather than as the reaction being taken back.
type MessageReaction struct {
	ID        string  `json:"id"`
	Chat      Address `json:"chat"`
	Sender    *Party  `json:"sender,omitempty"`
	FromMe    bool    `json:"from_me"`
	TargetID  string  `json:"target_id"`
	Emoji     string  `json:"emoji"`
	Timestamp int64   `json:"timestamp"`
}

// ReceiptKind is what became of a message, as the contract names it.
type ReceiptKind string

// The four the contract has. WhatsApp reports more than this, and the engine is what
// decides which of its own shapes each of these covers.
const (
	ReceiptDelivered ReceiptKind = "delivered"
	ReceiptRead      ReceiptKind = "read"
	ReceiptPlayed    ReceiptKind = "played"
	ReceiptFailed    ReceiptKind = "failed"
)

// MessageReceipt is `message.receipt`: what happened to messages that already exist.
//
// MessageIDs is a list because WhatsApp reports a chat being opened as one receipt over
// everything unread in it, and splitting that into one event per message would multiply
// a burst by the size of the backlog.
//
// Participant is who the receipt is from, and it is set even in a direct chat where it
// repeats the chat's own address. That repetition is the point: the same four names
// cover a receipt about a message this account sent and one about a message it received
// and read from another of its own devices, and the only thing separating them is whose
// device reported it. A client comparing this against its own number can tell, and one
// that does not care can ignore the field.
type MessageReceipt struct {
	Chat        Address     `json:"chat"`
	MessageIDs  []string    `json:"message_ids"`
	Type        ReceiptKind `json:"type"`
	Participant *Address    `json:"participant,omitempty"`
	Error       *Error      `json:"error,omitempty"`
	Timestamp   int64       `json:"timestamp"`
}

// LocationContent is a pin on a map. The coordinates are the whole message: there is
// nothing to fetch, which is what separates it from every other content that is not text.
type LocationContent struct {
	Type      string  `json:"type"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Name      string  `json:"name,omitempty"`
	Address   string  `json:"address,omitempty"`
	// Live says the sender is still moving and WhatsApp expects the pin to be updated.
	// It is carried so a client can say so rather than showing a stale pin as current.
	Live bool `json:"live"`
}

// Location returns the content of a pin, for a caller that fills in what it knows.
func Location(latitude, longitude float64) LocationContent {
	return LocationContent{Type: "location", Latitude: latitude, Longitude: longitude}
}

// Contact is one card in a share.
//
// The vCard is carried verbatim and the other two are read out of it, because the card
// may hold several numbers, an email and a company that these fields cannot express: a
// client that renders a row wants the name and the number, and one that stores the
// contact wants everything.
type Contact struct {
	DisplayName string `json:"display_name,omitempty"`
	Phone       string `json:"phone,omitempty"`
	Vcard       string `json:"vcard,omitempty"`
}

// ContactsContent is one or more cards somebody shared.
type ContactsContent struct {
	Type     string    `json:"type"`
	Contacts []Contact `json:"contacts"`
}

// Contacts returns the content of a share.
func Contacts(cards []Contact) ContactsContent {
	return ContactsContent{Type: "contacts", Contacts: cards}
}

// UnsupportedReason is why a message arrived without a body a client can render.
type UnsupportedReason string

// The reasons the contract defines.
const (
	// UnsupportedUnknownType is a body this build has no arm for: a poll, an order, a
	// card this milestone has yet to reach.
	UnsupportedUnknownType UnsupportedReason = "unknown_type"
	// UnsupportedUndecryptable is a message whose ciphertext could not be opened.
	UnsupportedUndecryptable UnsupportedReason = "undecryptable"
	// UnsupportedProtocol is machinery rather than a message.
	UnsupportedProtocol UnsupportedReason = "protocol"
	// UnsupportedEmpty is a message that arrived carrying nothing at all.
	UnsupportedEmpty UnsupportedReason = "empty"
)

// UnsupportedContent is a message that arrived and cannot be rendered.
//
// It is a placeholder and it is the point: an agent seeing a bubble they cannot read
// knows somebody sent something, and can ask. The alternative this replaces was
// withholding the acknowledgement, which leaves WhatsApp redelivering the same message
// for as long as the session is up and shows the agent nothing either way.
type UnsupportedContent struct {
	Type   string            `json:"type"`
	Reason UnsupportedReason `json:"reason"`
}

// Unsupported returns the content of a message nothing here can render.
func Unsupported(reason UnsupportedReason) UnsupportedContent {
	return UnsupportedContent{Type: "unsupported", Reason: reason}
}
