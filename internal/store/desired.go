package store

import (
	"context"
	"fmt"
	"time"
)

// The two states a client can ask a session to be in. They are the words the contract
// uses, and they are stored rather than derived: what is derivable from the rest of the
// schema is whether a session is paired, which is a different question.
const (
	DesiredConnected    = "connected"
	DesiredDisconnected = "disconnected"
)

// putDesired records what a client last asked for.
func (c *Container) putDesired(ctx context.Context, sid, desired string, now time.Time) error {
	if sid == "" || desired == "" {
		return fmt.Errorf("store: a desired state needs a session and a state, got %q and %q", sid, desired)
	}
	const upsert = `
		INSERT INTO wac_session_desired (sid, desired, asked_at) VALUES (?, ?, ?)
		ON CONFLICT (sid) DO UPDATE SET desired = excluded.desired, asked_at = excluded.asked_at`
	if _, err := c.db.ExecContext(ctx, c.rebind(upsert), sid, desired, now.UnixMilli()); err != nil {
		return fmt.Errorf("store: record the desired state of %s: %w", sid, err)
	}
	return nil
}

// dropDesired forgets what was asked for, which is what a session that no longer exists
// leaves behind.
func (c *Container) dropDesired(ctx context.Context, sid string) error {
	if _, err := c.db.ExecContext(ctx,
		c.rebind(`DELETE FROM wac_session_desired WHERE sid = ?`), sid); err != nil {
		return fmt.Errorf("store: forget the desired state of %s: %w", sid, err)
	}
	return nil
}

// Wanted lists the sessions a client asked to be connected and that have a device to
// connect with, oldest request first.
//
// The join is what keeps a pairing nobody finished out of it. A client asks for a
// connection before there is a device -- that is what pairing is -- so a session that
// showed a QR and was abandoned has a desired state and nothing to resume: brought back,
// it would publish a QR into an empty room on every sweep, forever.
//
// Ordered by when the connection was asked for, so a fleet recovering more sessions than
// one pass may take does not keep starting from the same end of an arbitrary order and
// leave the tail unrecovered. The session id breaks the tie, because two requests can
// land in the same millisecond and an order that is undefined between them is one a
// caller cannot reason about at all.
func (c *Container) Wanted(ctx context.Context) ([]string, error) {
	const query = `
		SELECT d.sid FROM wac_session_desired d
		JOIN wac_session_device v ON v.sid = d.sid
		WHERE d.desired = ?
		ORDER BY d.asked_at, d.sid`
	rows, err := c.db.QueryContext(ctx, c.rebind(query), DesiredConnected)
	if err != nil {
		return nil, fmt.Errorf("store: read the sessions that should be connected: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var wanted []string
	for rows.Next() {
		var sid string
		if err := rows.Scan(&sid); err != nil {
			return nil, fmt.Errorf("store: read the sessions that should be connected: %w", err)
		}
		wanted = append(wanted, sid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read the sessions that should be connected: %w", err)
	}
	return wanted, nil
}
