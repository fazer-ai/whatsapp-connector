package store

import (
	"context"
	"database/sql"
	"errors"
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

// Wants is what a client asked to receive while its session runs, which an instance
// bringing the account back has to put back with it.
//
// A struct and not a pair of bools at each call site: both default to off, so passing
// them positionally swaps them without failing to compile, and the two failures look
// nothing alike from a client's side.
type Wants struct {
	// Groups is the subscription the connect carried. Without it a resumed session
	// acknowledges every group message WhatsApp sends and publishes none of them, and
	// the client sees an account that is open and, for groups, deaf.
	Groups bool
	// CallAutoReject is the call policy the connect carried, kept for the same reason
	// and with a sharper edge: a resumed session that quietly stopped refusing calls
	// lets the account ring on the operator's phone after they asked for the opposite,
	// and nothing about the session says it changed its mind.
	CallAutoReject bool
	// Proxy is the address the session's traffic with WhatsApp left through, empty for a
	// session that went out directly. The sharpest of the three: a resumed session that
	// dropped it would dial WhatsApp from this instance's own address, which is the one
	// thing a client that asked for a proxy asked not to happen, and nothing on the wire
	// would say so. It carries credentials, so it is kept here with the rest of a
	// session's auth state and never read back into a log or a frame.
	Proxy string
}

// Wanted is a session a client asked to have running, and what it asked for.
type Wanted struct {
	// SID names the session.
	SID string
	Wants
}

// putDesiredDisconnected records that a client asked for the session to stay down.
//
// The subscription is left as it stands rather than written, because the command this
// serves does not carry one. A disconnect says nothing about which traffic a client
// wants when it comes back, and a disconnect that wrote the column would be answering
// that question with a default nobody asked for. And the columns are read while the
// session is down: `Standing` hands them to a pairing code asked for on a session turned
// off, which is still asking through the proxy its client named. Cleared here, that code
// would be asked for directly, and the row it writes would carry the default onwards.
func (c *Container) putDesiredDisconnected(ctx context.Context, sid string, now time.Time) error {
	if sid == "" {
		return fmt.Errorf("store: a desired state needs a session, got %q", sid)
	}
	const upsert = `
		INSERT INTO wac_session_desired (sid, desired, asked_at) VALUES (?, ?, ?)
		ON CONFLICT (sid) DO UPDATE SET desired = excluded.desired, asked_at = excluded.asked_at`
	if _, err := c.db.ExecContext(ctx, c.rebind(upsert),
		sid, DesiredDisconnected, now.UnixMilli()); err != nil {
		return fmt.Errorf("store: record the desired state of %s: %w", sid, err)
	}
	return nil
}

// putDesiredConnected records that a client asked for a connection, and what it asked to
// receive over it.
//
// One write and not two, because they are one request: the client says "connect" and
// "send me groups" in the same command, and a session brought back later has to be the
// session that was asked for rather than half of it. Two writes could also disagree --
// an instance that died between them would leave an account that resumes with a
// subscription from a connect that never happened.
//
// The switches are persisted and not the request they arrived in. A connect carries
// things this build refuses outright -- `history_sync` -- and a resume that replayed a
// stored payload would synthesise a command the session rejects, leaving the account
// down and in the sweep's backoff: worse than the silence this exists to fix.
func (c *Container) putDesiredConnected(ctx context.Context, sid string, wants Wants, now time.Time) error {
	if sid == "" {
		return fmt.Errorf("store: a desired state needs a session, got %q", sid)
	}
	const upsert = `
		INSERT INTO wac_session_desired
			(sid, desired, wants_groups, wants_call_auto_reject, wants_proxy, asked_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (sid) DO UPDATE SET
			desired = excluded.desired, wants_groups = excluded.wants_groups,
			wants_call_auto_reject = excluded.wants_call_auto_reject,
			wants_proxy = excluded.wants_proxy,
			asked_at = excluded.asked_at`
	if _, err := c.db.ExecContext(ctx, c.rebind(upsert),
		sid, DesiredConnected, asFlag(wants.Groups), asFlag(wants.CallAutoReject), wants.Proxy,
		now.UnixMilli()); err != nil {
		return fmt.Errorf("store: record the desired state of %s: %w", sid, err)
	}
	return nil
}

// standing reads one session's row, with no join on the pairing: a session asking for a
// pairing code has, by definition, nothing paired yet. And whatever the state, because a
// disconnect leaves the request standing (see putDesiredDisconnected).
func (c *Container) standing(ctx context.Context, sid string) (Wants, bool, error) {
	const query = `
		SELECT d.wants_groups, d.wants_call_auto_reject, d.wants_proxy
		FROM wac_session_desired d WHERE d.sid = ?`
	var proxy string
	var groups, autoReject int64
	err := c.db.QueryRowContext(ctx, c.rebind(query), sid).Scan(&groups, &autoReject, &proxy)
	if errors.Is(err, sql.ErrNoRows) {
		return Wants{}, false, nil
	}
	if err != nil {
		return Wants{}, false, fmt.Errorf("store: read what %s was asked to be: %w", sid, err)
	}
	return Wants{Groups: groups != 0, CallAutoReject: autoReject != 0, Proxy: proxy}, true, nil
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
// connect with, oldest request first, each with the subscription its client asked for.
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
func (c *Container) Wanted(ctx context.Context) ([]Wanted, error) {
	const query = `
		SELECT d.sid, d.wants_groups, d.wants_call_auto_reject, d.wants_proxy
		FROM wac_session_desired d
		JOIN wac_session_device v ON v.sid = d.sid
		WHERE d.desired = ?
		ORDER BY d.asked_at, d.sid`
	rows, err := c.db.QueryContext(ctx, c.rebind(query), DesiredConnected)
	if err != nil {
		return nil, fmt.Errorf("store: read the sessions that should be connected: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var wanted []Wanted
	for rows.Next() {
		var sid, proxy string
		var groups, autoReject int64
		if err := rows.Scan(&sid, &groups, &autoReject, &proxy); err != nil {
			return nil, fmt.Errorf("store: read the sessions that should be connected: %w", err)
		}
		wanted = append(wanted, Wanted{
			SID:   sid,
			Wants: Wants{Groups: groups != 0, CallAutoReject: autoReject != 0, Proxy: proxy},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read the sessions that should be connected: %w", err)
	}
	return wanted, nil
}
