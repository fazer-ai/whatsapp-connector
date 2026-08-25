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
