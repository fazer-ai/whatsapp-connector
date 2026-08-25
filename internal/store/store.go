// Package store is where a paired session survives a restart.
//
// It holds two things that belong together and are useless apart: whatsmeow's own
// device store, which is where the Signal keys and the pairing live, and the mapping
// from a client's session id onto the device that session paired. whatsmeow keys a
// device by its JID, and a JID only exists once pairing has succeeded, so without that
// mapping a restart cannot tell which of the stored devices belongs to which session
// and every session would pair again.
//
// Ownership is arbitrated by the Redis lease, not here: one instance runs a session at
// a time, so one process at a time writes that session's device. The store-level fence
// arrives with the message store in M2, where this package owns writes of its own that
// a stale owner could otherwise interleave.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"

	// The two dialects the connector supports. Postgres is what a deployment runs;
	// SQLite is the single-instance and test case, on the pure-Go driver so the image
	// needs no cgo.
	_ "github.com/lib/pq"
	_ "modernc.org/sqlite"
)

// ErrNoDatabase is returned when the connector is asked for a store it was given no
// address for. It is a configuration error rather than a runtime one: an engine that
// cannot persist a pairing would ask every session to scan a QR code on every restart.
var ErrNoDatabase = errors.New("store: no database url configured")

// Container is the connector's whole persistence.
type Container struct {
	db      *sql.DB
	devices *sqlstore.Container
	dialect string
}

// Open dials the database, brings both schemas up to date, and returns the container.
//
// The url decides the dialect: `postgres://…` or `postgresql://…` is Postgres,
// `sqlite:…` or `file:…` is SQLite. Nothing is guessed from the file system, so a
// typo fails here rather than silently opening a local file in a deployment that meant
// to reach a server.
func Open(ctx context.Context, url string) (*Container, error) {
	dialect, address, err := parseURL(url)
	if err != nil {
		return nil, err
	}

	devices, err := sqlstore.New(ctx, dialect, address, nil)
	if err != nil {
		return nil, fmt.Errorf("store: open the device store: %w", err)
	}

	db, err := sql.Open(dialect, address)
	if err != nil {
		_ = devices.Close()
		return nil, fmt.Errorf("store: open %s: %w", dialect, err)
	}

	c := &Container{db: db, devices: devices, dialect: dialect}
	if err := c.migrate(ctx); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

// Devices is whatsmeow's own store, which the engine hands to a client.
func (c *Container) Devices() *sqlstore.Container { return c.devices }

// Close releases both handles.
func (c *Container) Close() error {
	return errors.Join(c.devices.Close(), c.db.Close())
}

// Device returns the device a session should connect with: the stored one when that
// session has paired before, and a fresh unpaired one otherwise.
//
// A mapping that points at a device whatsmeow no longer holds is treated as no mapping
// at all: the credentials are what makes a session resumable, and without them the
// only honest answer is to pair again.
func (c *Container) Device(ctx context.Context, sid string) (*store.Device, error) {
	jid, bound, err := c.lookup(ctx, sid)
	if err != nil {
		return nil, err
	}
	if !bound {
		return c.devices.NewDevice(), nil
	}

	device, err := c.devices.GetDevice(ctx, jid)
	if err != nil {
		return nil, fmt.Errorf("store: read the device of %s: %w", sid, err)
	}
	if device == nil {
		if err := c.unbind(ctx, sid); err != nil {
			return nil, err
		}
		return c.devices.NewDevice(), nil
	}
	return device, nil
}

// Bind records which device a session paired.
//
// It is called from the pairing callback, before whatsmeow writes the device, so a
// process that dies mid-pairing leaves a mapping to a device that does not exist yet,
// which Device treats as unpaired. The other order would leave a stored device no
// session claims, which nothing can clean up because nothing knows whose it was.
func (c *Container) Bind(ctx context.Context, sid string, jid types.JID) error {
	if sid == "" {
		return errors.New("store: bind needs a session id")
	}
	if jid.IsEmpty() {
		return fmt.Errorf("store: bind %s needs a jid", sid)
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: bind %s: %w", sid, err)
	}
	defer func() { _ = tx.Rollback() }()

	// A device belongs to one session. Re-pairing the same number under a different
	// session id is the case this covers, and leaving both rows would give two sessions
	// one set of credentials.
	if _, err := tx.ExecContext(ctx, c.rebind(`DELETE FROM wac_session_device WHERE jid = ? AND sid <> ?`),
		jid.ToNonAD().String(), sid); err != nil {
		return fmt.Errorf("store: bind %s: %w", sid, err)
	}
	if _, err := tx.ExecContext(ctx, c.rebind(`
		INSERT INTO wac_session_device (sid, jid, bound_at) VALUES (?, ?, ?)
		ON CONFLICT (sid) DO UPDATE SET jid = excluded.jid, bound_at = excluded.bound_at`),
		sid, jid.ToNonAD().String(), time.Now().UnixMilli()); err != nil {
		return fmt.Errorf("store: bind %s: %w", sid, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: bind %s: %w", sid, err)
	}
	return nil
}

// Forget deletes a session's device and its mapping, which is what a logout means:
// the credentials are gone and the next connect has to pair.
func (c *Container) Forget(ctx context.Context, sid string) error {
	jid, bound, err := c.lookup(ctx, sid)
	if err != nil {
		return err
	}
	if bound {
		device, err := c.devices.GetDevice(ctx, jid)
		if err != nil {
			return fmt.Errorf("store: read the device of %s: %w", sid, err)
		}
		if device != nil {
			if err := c.devices.DeleteDevice(ctx, device); err != nil {
				return fmt.Errorf("store: delete the device of %s: %w", sid, err)
			}
		}
	}
	return c.unbind(ctx, sid)
}

// JID reports which device a session is bound to, and whether it is bound at all.
func (c *Container) JID(ctx context.Context, sid string) (types.JID, bool, error) {
	return c.lookup(ctx, sid)
}

func (c *Container) lookup(ctx context.Context, sid string) (types.JID, bool, error) {
	var raw string
	err := c.db.QueryRowContext(ctx, c.rebind(`SELECT jid FROM wac_session_device WHERE sid = ?`), sid).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return types.EmptyJID, false, nil
	}
	if err != nil {
		return types.EmptyJID, false, fmt.Errorf("store: read the mapping of %s: %w", sid, err)
	}

	jid, err := types.ParseJID(raw)
	if err != nil {
		// A row we wrote that we cannot read back is corruption, not a missing pairing.
		// Saying so is better than quietly asking the operator to scan a QR code.
		return types.EmptyJID, false, fmt.Errorf("store: the mapping of %s holds an unreadable jid %q: %w", sid, raw, err)
	}
	return jid, true, nil
}

func (c *Container) unbind(ctx context.Context, sid string) error {
	if _, err := c.db.ExecContext(ctx, c.rebind(`DELETE FROM wac_session_device WHERE sid = ?`), sid); err != nil {
		return fmt.Errorf("store: drop the mapping of %s: %w", sid, err)
	}
	return nil
}

// migrate creates what this package owns. whatsmeow's own schema is brought up by
// sqlstore.New, so this covers the mapping table alone.
func (c *Container) migrate(ctx context.Context) error {
	const schema = `
		CREATE TABLE IF NOT EXISTS wac_session_device (
			sid      TEXT   PRIMARY KEY,
			jid      TEXT   NOT NULL UNIQUE,
			bound_at BIGINT NOT NULL
		)`
	if _, err := c.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("store: create wac_session_device: %w", err)
	}
	return nil
}

// rebind turns `?` placeholders into `$1`-style ones for Postgres. Writing every
// statement once and translating here is the smaller of the two evils: the alternative
// is two copies of each query, which drift.
func (c *Container) rebind(query string) string {
	if c.dialect != dialectPostgres {
		return query
	}
	var out strings.Builder
	out.Grow(len(query) + 8)
	n := 0
	for i := range len(query) {
		if query[i] != '?' {
			out.WriteByte(query[i])
			continue
		}
		n++
		out.WriteByte('$')
		out.WriteString(strconv.Itoa(n))
	}
	return out.String()
}

const (
	dialectPostgres = "postgres"
	dialectSQLite   = "sqlite"
)

// parseURL splits a database url into the dialect and the address the driver wants.
func parseURL(url string) (dialect, address string, err error) {
	switch {
	case url == "":
		return "", "", ErrNoDatabase
	case strings.HasPrefix(url, "postgres://"), strings.HasPrefix(url, "postgresql://"):
		return dialectPostgres, url, nil
	case strings.HasPrefix(url, "file:"):
		return dialectSQLite, url, nil
	case strings.HasPrefix(url, "sqlite://"):
		return dialectSQLite, "file:" + strings.TrimPrefix(url, "sqlite://"), nil
	case strings.HasPrefix(url, "sqlite:"):
		return dialectSQLite, "file:" + strings.TrimPrefix(url, "sqlite:"), nil
	default:
		return "", "", fmt.Errorf("store: %q is not a database url this connector understands", url)
	}
}
