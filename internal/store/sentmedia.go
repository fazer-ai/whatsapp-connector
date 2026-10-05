package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"time"
)

// SentMediaRetention is how long a media message this account sent is kept for its
// caption to be corrected. WhatsApp closes the edit window about fifteen minutes after a
// send, so an hour covers it with room for a clock that disagrees, and a row is a few
// hundred bytes of coordinates rather than the file.
const SentMediaRetention = time.Hour

// SentMedia is a media message as this account put it on the wire.
type SentMedia struct {
	// Chat is the address it was sent to, as a JID string.
	Chat string
	// Body is the message, serialised as WhatsApp's protobuf.
	Body []byte
}

// PutSentMedia keeps a media message this session is about to send, under the id it goes
// out with. Written again under the same id it replaces what was kept, which is what a
// redelivered send does.
func (s *Scoped) PutSentMedia(ctx context.Context, messageID string, sent SentMedia) error {
	if err := s.fence.held(); err != nil {
		return err
	}
	return s.container.putSentMedia(ctx, s.sid, messageID, sent, time.Now())
}

// SentMedia reads back a media message this session sent, if it is still kept.
func (s *Scoped) SentMedia(ctx context.Context, messageID string) (SentMedia, bool, error) {
	return s.container.sentMedia(ctx, s.sid, messageID)
}

func (c *Container) putSentMedia(ctx context.Context, sid, messageID string, sent SentMedia, now time.Time) error {
	if sid == "" || messageID == "" || sent.Chat == "" || len(sent.Body) == 0 {
		return fmt.Errorf("store: a sent media message needs a session, an id, a chat and a body, got %q, %q, %q and %d bytes",
			sid, messageID, sent.Chat, len(sent.Body))
	}
	const upsert = `
		INSERT INTO wac_sent_media (sid, message_id, chat, body, sent_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (sid, message_id) DO UPDATE SET
			chat = excluded.chat, body = excluded.body, sent_at = excluded.sent_at`
	// Base64 in a TEXT column rather than a blob, so the one schema reads the same under
	// both dialects.
	body := base64.StdEncoding.EncodeToString(sent.Body)
	if _, err := c.db.ExecContext(ctx, c.rebind(upsert), sid, messageID, sent.Chat, body, now.UnixMilli()); err != nil {
		return fmt.Errorf("store: keep the media message %s of %s: %w", messageID, sid, err)
	}
	return nil
}

func (c *Container) sentMedia(ctx context.Context, sid, messageID string) (SentMedia, bool, error) {
	var kept SentMedia
	var body string
	err := c.db.QueryRowContext(ctx,
		c.rebind(`SELECT chat, body FROM wac_sent_media WHERE sid = ? AND message_id = ?`), sid, messageID,
	).Scan(&kept.Chat, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return SentMedia{}, false, nil
	}
	if err != nil {
		return SentMedia{}, false, fmt.Errorf("store: read the media message %s of %s: %w", messageID, sid, err)
	}
	if kept.Body, err = base64.StdEncoding.DecodeString(body); err != nil {
		return SentMedia{}, false, fmt.Errorf("store: decode the media message %s of %s: %w", messageID, sid, err)
	}
	return kept, true, nil
}

// SweepSentMedia drops the media messages sent before the given moment, across every
// session, and reports how many went.
func (c *Container) SweepSentMedia(ctx context.Context, before time.Time) (int64, error) {
	res, err := c.db.ExecContext(ctx,
		c.rebind(`DELETE FROM wac_sent_media WHERE sent_at < ?`), before.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("store: sweep the sent media messages: %w", err)
	}
	return res.RowsAffected()
}
