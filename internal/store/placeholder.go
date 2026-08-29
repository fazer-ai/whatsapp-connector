package store

import (
	"context"
	"fmt"
)

// Placeholder is a message that arrived in a form nothing could read, and whose bubble
// has not been decided yet.
//
// Neither of the two endings has happened when this is written. The message may still
// turn up -- the phone forwards it, or the sender encrypts it again -- and then there
// is no bubble to show; or the window runs out and the placeholder goes into the chat
// saying a message arrived that could not be opened. What makes the row necessary is
// that the choice lives in a timer, and a timer lives exactly as long as the process
// holding it, while the session it belongs to outlives that process.
//
// The rendered event is kept whole rather than field by field, which is the opposite of
// what MediaPart does and for the opposite reason. A media part is written so a file can
// be fetched again, and naming its fields is what keeps the conversation out of the
// table. This row *is* the bubble: it exists to be published unchanged if nothing
// better arrives, so taking it apart and putting it back together would only add a way
// for the two to differ.
type Placeholder struct {
	SID       string
	MessageID string

	// Message is the rendered inbound message, as the event payload would carry it.
	// TEXT rather than a blob column, for the reason the media keys are: it is the same
	// column in both dialects, and JSON is text.
	Message string

	// LearnedAt is when the unreadable message arrived, in milliseconds, and it is the
	// order the event goes out under. Kept rather than taken fresh at publication: the
	// bubble belongs where the message reached this connector, not where the instance
	// that finally published it happened to start.
	LearnedAt int64

	// DueAt is when the window this message was given runs out, in milliseconds. Kept
	// as an instant and not as a duration, so a session picked up by another instance
	// serves out the remainder rather than starting the wait again -- which would let a
	// handoff every 40 seconds hold a bubble back indefinitely.
	DueAt int64
}

// putPlaceholder writes down a bubble that has not been decided yet, and leaves alone
// whatever was already there for the same message.
//
// The first hold wins, and that is not a tie-break: it is what keeps the row agreeing
// with the timer beside it. whatsmeow raises the same failure again when a resend will
// not decrypt either, and the waiting list in the engine keeps the first arming and
// ignores the second. A row that took the second would carry a deadline later than the
// timer that is actually going to fire, so a handoff in between would serve out a window
// nobody ever granted.
//
// Left alone rather than refused, because there is nothing wrong with the second
// delivery: the bubble it would write is the same bubble, already written.
func (c *Container) putPlaceholder(ctx context.Context, held *Placeholder) error {
	if held.SID == "" || held.MessageID == "" {
		return fmt.Errorf(
			"store: a placeholder needs a session and a message, got %q and %q", held.SID, held.MessageID)
	}

	const upsert = `
		INSERT INTO wac_pending_placeholder (sid, message_id, message, learned_at, due_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (sid, message_id) DO NOTHING`
	_, err := c.db.ExecContext(ctx, c.rebind(upsert),
		held.SID, held.MessageID, held.Message, held.LearnedAt, held.DueAt)
	if err != nil {
		return fmt.Errorf("store: hold the placeholder for %s: %w", held.MessageID, err)
	}
	return nil
}

// dropPlaceholder forgets a bubble that has been decided, whichever way it went.
func (c *Container) dropPlaceholder(ctx context.Context, sid, messageID string) error {
	_, err := c.db.ExecContext(ctx,
		c.rebind(`DELETE FROM wac_pending_placeholder WHERE sid = ? AND message_id = ?`), sid, messageID)
	if err != nil {
		return fmt.Errorf("store: release the placeholder for %s: %w", messageID, err)
	}
	return nil
}

// placeholders lists the bubbles a session left undecided, oldest deadline first so the
// most overdue is armed before the rest.
func (c *Container) placeholders(ctx context.Context, sid string) ([]Placeholder, error) {
	const query = `
		SELECT message_id, message, learned_at, due_at
		FROM wac_pending_placeholder WHERE sid = ? ORDER BY due_at`

	rows, err := c.db.QueryContext(ctx, c.rebind(query), sid)
	if err != nil {
		return nil, fmt.Errorf("store: read the placeholders %s left waiting: %w", sid, err)
	}
	defer func() { _ = rows.Close() }()

	var held []Placeholder
	for rows.Next() {
		waiting := Placeholder{SID: sid}
		if err := rows.Scan(&waiting.MessageID, &waiting.Message, &waiting.LearnedAt, &waiting.DueAt); err != nil {
			return nil, fmt.Errorf("store: read the placeholders %s left waiting: %w", sid, err)
		}
		held = append(held, waiting)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read the placeholders %s left waiting: %w", sid, err)
	}
	return held, nil
}
