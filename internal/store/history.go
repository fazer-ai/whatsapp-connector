package store

import (
	"context"
	"fmt"
)

// PendingHistory is a history dump the phone announced and this connector has not finished
// with: not every slice of it was published, or the phone was not told it arrived.
//
// The row is the only thing that brings such a dump back. A dump is announced once: the
// announcement is a protocol message whatsmeow answers with a peer receipt before any
// handler runs, and a notification whose node was left unacknowledged was measured not to
// come again, neither on the same socket after the outage nor on a new one after a restart.
// Withholding the acknowledgement, which is what keeps an ordinary message on the phone,
// keeps nothing here.
type PendingHistory struct {
	SID       string
	MessageID string

	// Notice is the notification as the phone sent it, marshalled: the blob's path and
	// keys, which is everything the download needs. Base64 in a TEXT column, for the
	// reason the media keys are.
	Notice []byte

	// LearnedAt is when the notification reached this connector, in milliseconds. Kept so a
	// dump finished by another instance, or after a restart, goes out stamped where it
	// arrived rather than where it was finally published.
	LearnedAt int64
}

// putPendingHistory writes down a dump that has been announced, and leaves alone a row that
// is already there for the same notification: it is the same dump.
func (c *Container) putPendingHistory(ctx context.Context, held *PendingHistory) error {
	if held.SID == "" || held.MessageID == "" {
		return fmt.Errorf(
			"store: a pending dump needs a session and a notification, got %q and %q", held.SID, held.MessageID)
	}

	const upsert = `
		INSERT INTO wac_pending_history (sid, message_id, notice, learned_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (sid, message_id) DO NOTHING`
	_, err := c.db.ExecContext(ctx, c.rebind(upsert), held.SID, held.MessageID, encode(held.Notice), held.LearnedAt)
	if err != nil {
		return fmt.Errorf("store: hold the history dump %s: %w", held.MessageID, err)
	}
	return nil
}

// dropPendingHistory forgets a dump that is finished with.
func (c *Container) dropPendingHistory(ctx context.Context, sid, messageID string) error {
	_, err := c.db.ExecContext(ctx,
		c.rebind(`DELETE FROM wac_pending_history WHERE sid = ? AND message_id = ?`), sid, messageID)
	if err != nil {
		return fmt.Errorf("store: release the history dump %s: %w", messageID, err)
	}
	return nil
}

// pendingHistory lists the dumps a session has not finished with, in the order they
// arrived, which is the order the phone sent them in.
func (c *Container) pendingHistory(ctx context.Context, sid string) ([]PendingHistory, error) {
	const query = `
		SELECT message_id, notice, learned_at
		FROM wac_pending_history WHERE sid = ? ORDER BY learned_at, message_id`

	rows, err := c.db.QueryContext(ctx, c.rebind(query), sid)
	if err != nil {
		return nil, fmt.Errorf("store: read the history dumps %s left pending: %w", sid, err)
	}
	defer func() { _ = rows.Close() }()

	var held []PendingHistory
	for rows.Next() {
		pending := PendingHistory{SID: sid}
		var notice string
		if err := rows.Scan(&pending.MessageID, &notice, &pending.LearnedAt); err != nil {
			return nil, fmt.Errorf("store: read the history dumps %s left pending: %w", sid, err)
		}
		if pending.Notice, err = decode(notice); err != nil {
			return nil, fmt.Errorf("store: read the history dump %s: %w", pending.MessageID, err)
		}
		held = append(held, pending)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read the history dumps %s left pending: %w", sid, err)
	}
	return held, nil
}
