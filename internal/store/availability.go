package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// availability is the presence a client asked this account to be shown as, kept so the
// next connection can put it back.
//
// It is the session's own memory that answers on a reconnect, and that memory is per
// instance: an ownership handoff builds a session that has never heard the command, and
// has nothing to reapply. This is the record that survives that, and it is a table of its
// own rather than a column on wac_session_device because that row is the sid-to-jid bond
// and is written by pairing, while this is written by a command and has its own life.
//
// The foreign key does the clearing. A forget unbinds the session, the row goes with it,
// and what pairs next is available only if its own client says so.

// PutAvailability records what this account was last asked to be shown as.
func (c *Container) putAvailability(ctx context.Context, sid, state string, now time.Time) error {
	if sid == "" || state == "" {
		return fmt.Errorf("store: an availability needs a session and a state, got %q and %q", sid, state)
	}
	const upsert = `
		INSERT INTO wac_session_presence (sid, state, set_at) VALUES (?, ?, ?)
		ON CONFLICT (sid) DO UPDATE SET state = excluded.state, set_at = excluded.set_at`
	if _, err := c.db.ExecContext(ctx, c.rebind(upsert), sid, state, now.UnixMilli()); err != nil {
		return fmt.Errorf("store: record the availability of %s: %w", sid, err)
	}
	return nil
}

// availability reads back what putAvailability kept, and whether anything was kept at all.
func (c *Container) availability(ctx context.Context, sid string) (state string, kept bool, err error) {
	err = c.db.QueryRowContext(ctx,
		c.rebind(`SELECT state FROM wac_session_presence WHERE sid = ?`), sid).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("store: read the availability of %s: %w", sid, err)
	}
	return state, true, nil
}
