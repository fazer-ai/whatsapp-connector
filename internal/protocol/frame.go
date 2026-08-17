package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// ErrUnknownVersion is returned when a frame carries a protocol version outside
// [MinVersion, Version]. The caller drops the frame instead of guessing.
var ErrUnknownVersion = errors.New("protocol: unsupported version")

// Event is one entry of wa:events:<shard>: something that happened on a session.
//
// Payload stays raw here. Decoding it is the job of whoever handles the type, and
// keeping it raw is what lets a client forward or persist a frame it does not know
// how to interpret.
type Event struct {
	V       int             `json:"v"`
	ID      string          `json:"id"`
	Type    EventType       `json:"type"`
	SID     string          `json:"sid"`
	Epoch   uint64          `json:"epoch"`
	Seq     uint64          `json:"seq"`
	TS      int64           `json:"ts"`
	Inst    string          `json:"inst,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

// Command is one entry of wa:cmd:<sid> (or of wa:control for the session-agnostic
// ones): something a client asks the session owner to do.
type Command struct {
	V              int             `json:"v"`
	ID             string          `json:"id"`
	Type           CommandType     `json:"type"`
	SID            string          `json:"sid"`
	TS             int64           `json:"ts"`
	ReplyTo        string          `json:"reply_to,omitempty"`
	Deadline       int64           `json:"deadline,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Payload        json.RawMessage `json:"payload"`
}

// Reply is the single element pushed to wa:reply:<command id> for an RPC command.
type Reply struct {
	V      int             `json:"v"`
	ID     string          `json:"id"`
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *Error          `json:"error,omitempty"`
}

// Cursor orders events of one session. seq is monotonic per (sid, epoch), and a
// higher epoch always wins: it means the session was re-owned by another instance,
// so its numbering restarts.
type Cursor struct {
	Epoch uint64
	Seq   uint64
}

// CursorOf returns the position of an event in its session's stream.
func (e *Event) CursorOf() Cursor { return Cursor{Epoch: e.Epoch, Seq: e.Seq} }

// After reports whether c is strictly newer than other.
func (c Cursor) After(other Cursor) bool {
	if c.Epoch != other.Epoch {
		return c.Epoch > other.Epoch
	}
	return c.Seq > other.Seq
}

// String renders the cursor as the "epoch:seq" form clients persist.
func (c Cursor) String() string {
	return strconv.FormatUint(c.Epoch, 10) + ":" + strconv.FormatUint(c.Seq, 10)
}

// ParseCursor reads back the "epoch:seq" form. A blank string is the zero cursor,
// which every real event is newer than.
func ParseCursor(s string) (Cursor, error) {
	if s == "" {
		return Cursor{}, nil
	}
	var c Cursor
	if _, err := fmt.Sscanf(s, "%d:%d", &c.Epoch, &c.Seq); err != nil {
		return Cursor{}, fmt.Errorf("protocol: malformed cursor %q: %w", s, err)
	}
	return c, nil
}

// Fields renders the event as the flat string map a Redis stream entry carries.
func (e *Event) Fields() (map[string]string, error) {
	payload, err := payloadField(e.Payload)
	if err != nil {
		return nil, err
	}
	fields := map[string]string{
		"v":       strconv.Itoa(e.V),
		"id":      e.ID,
		"type":    string(e.Type),
		"sid":     e.SID,
		"epoch":   strconv.FormatUint(e.Epoch, 10),
		"seq":     strconv.FormatUint(e.Seq, 10),
		"ts":      strconv.FormatInt(e.TS, 10),
		"payload": payload,
	}
	if e.Inst != "" {
		fields["inst"] = e.Inst
	}
	return fields, nil
}

// ParseEvent reads an event back from a stream entry.
func ParseEvent(fields map[string]string) (Event, error) {
	var e Event
	var err error
	if e.V, err = intField(fields, "v"); err != nil {
		return Event{}, err
	}
	if e.V < MinVersion || e.V > Version {
		return Event{}, fmt.Errorf("%w: %d", ErrUnknownVersion, e.V)
	}
	if e.Epoch, err = uintField(fields, "epoch"); err != nil {
		return Event{}, err
	}
	if e.Seq, err = uintField(fields, "seq"); err != nil {
		return Event{}, err
	}
	if e.TS, err = int64Field(fields, "ts"); err != nil {
		return Event{}, err
	}
	e.ID, e.SID, e.Inst = fields["id"], fields["sid"], fields["inst"]
	e.Type = EventType(fields["type"])
	if e.Payload, err = rawField(fields, "payload"); err != nil {
		return Event{}, err
	}
	return e, nil
}

// Fields renders the command as the flat string map a Redis stream entry carries.
func (c *Command) Fields() (map[string]string, error) {
	payload, err := payloadField(c.Payload)
	if err != nil {
		return nil, err
	}
	fields := map[string]string{
		"v":       strconv.Itoa(c.V),
		"id":      c.ID,
		"type":    string(c.Type),
		"sid":     c.SID,
		"ts":      strconv.FormatInt(c.TS, 10),
		"payload": payload,
	}
	if c.ReplyTo != "" {
		fields["reply_to"] = c.ReplyTo
	}
	if c.Deadline != 0 {
		fields["deadline"] = strconv.FormatInt(c.Deadline, 10)
	}
	if c.IdempotencyKey != "" {
		fields["idempotency_key"] = c.IdempotencyKey
	}
	return fields, nil
}

// ParseCommand reads a command back from a stream entry.
func ParseCommand(fields map[string]string) (Command, error) {
	var c Command
	var err error
	if c.V, err = intField(fields, "v"); err != nil {
		return Command{}, err
	}
	if c.V < MinVersion || c.V > Version {
		return Command{}, fmt.Errorf("%w: %d", ErrUnknownVersion, c.V)
	}
	if c.TS, err = int64Field(fields, "ts"); err != nil {
		return Command{}, err
	}
	if raw, ok := fields["deadline"]; ok && raw != "" {
		if c.Deadline, err = int64Field(fields, "deadline"); err != nil {
			return Command{}, err
		}
	}
	c.ID, c.SID = fields["id"], fields["sid"]
	c.ReplyTo, c.IdempotencyKey = fields["reply_to"], fields["idempotency_key"]
	c.Type = CommandType(fields["type"])
	if c.Payload, err = rawField(fields, "payload"); err != nil {
		return Command{}, err
	}
	return c, nil
}

func payloadField(payload json.RawMessage) (string, error) {
	if len(payload) == 0 {
		return "{}", nil
	}
	if !json.Valid(payload) {
		return "", errors.New("protocol: payload is not valid JSON")
	}
	return string(payload), nil
}

func rawField(fields map[string]string, key string) (json.RawMessage, error) {
	raw, ok := fields[key]
	if !ok || raw == "" {
		return nil, fmt.Errorf("protocol: missing field %q", key)
	}
	if !json.Valid([]byte(raw)) {
		return nil, fmt.Errorf("protocol: field %q is not valid JSON", key)
	}
	return json.RawMessage(raw), nil
}

func intField(fields map[string]string, key string) (int, error) {
	value, err := int64Field(fields, key)
	return int(value), err
}

func uintField(fields map[string]string, key string) (uint64, error) {
	raw, ok := fields[key]
	if !ok {
		return 0, fmt.Errorf("protocol: missing field %q", key)
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("protocol: field %q: %w", key, err)
	}
	return value, nil
}

func int64Field(fields map[string]string, key string) (int64, error) {
	raw, ok := fields[key]
	if !ok {
		return 0, fmt.Errorf("protocol: missing field %q", key)
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("protocol: field %q: %w", key, err)
	}
	return value, nil
}
