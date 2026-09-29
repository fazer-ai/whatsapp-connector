package protocol

// HistorySync is the reason the phone sent a slice of history, which is what tells a client
// the dump it receives on pairing from the answer to a `history.request`.
type HistorySync string

// The reasons the connector publishes history for. WhatsApp names more (the status feed,
// the push names, the non-blocking settings) and none of them carries conversation.
const (
	// HistoryBootstrap is the first dump a newly linked device receives.
	HistoryBootstrap HistorySync = "bootstrap"
	// HistoryRecent is the recent part of that dump, sent alongside it.
	HistoryRecent HistorySync = "recent"
	// HistoryFull is the rest of the phone's history, sent when the device asked for all of it.
	HistoryFull HistorySync = "full"
	// HistoryOnDemand is the phone's answer to a `history.request`.
	HistoryOnDemand HistorySync = "on_demand"
)

// AllHistorySyncs lists every reason a slice of history can carry.
var AllHistorySyncs = []HistorySync{HistoryBootstrap, HistoryRecent, HistoryFull, HistoryOnDemand}

// HistoryKindMessages is the only kind of history the connector publishes: the messages of
// one chat.
const HistoryKindMessages = "messages"

// HistorySlice is one slice of one chat's history, oldest first. A chat longer than a
// slice arrives in several, so no stream entry carries a whole dump.
type HistorySlice struct {
	Kind     string           `json:"kind"`
	Sync     HistorySync      `json:"sync"`
	Chat     Address          `json:"chat"`
	Name     string           `json:"name,omitempty"`
	Messages []InboundMessage `json:"messages"`
	// Exhausted is the phone having nothing older for this chat, which is what lets a
	// client stop offering to ask for more.
	Exhausted bool `json:"exhausted,omitempty"`
	Progress  *int `json:"progress,omitempty"`
}
