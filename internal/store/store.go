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
// a time, and the layer above tears a session down within a heartbeat of losing it.
//
// That leaves a window, and closing it is an architecture change rather than a guard.
// whatsmeow writes through its own SQL layer, where a statement carries no session
// identity, so a fence would have to be per-session all the way down: one container per
// session, or a driver wrapper that knows which session it is serving. Both are M2, and
// M2 is when this package starts owning writes of its own, which is what makes the
// question urgent. Until then the lease and the heartbeat are the whole of it, and this
// is documented in the roadmap as open rather than solved.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

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
	log     zerolog.Logger
}

// Open dials the database, brings both schemas up to date, and returns the container.
//
// The url decides the dialect: `postgres://…` or `postgresql://…` is Postgres,
// `sqlite:…` or `file:…` is SQLite. Nothing is guessed from the file system, so a
// typo fails here rather than silently opening a local file in a deployment that meant
// to reach a server.
//
//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func Open(ctx context.Context, address string, log zerolog.Logger) (*Container, error) {
	dialect, dsn, err := parseURL(address)
	if err != nil {
		return nil, err
	}

	// One pool, not two. whatsmeow's store and this package's own table live in the
	// same database, and opening them separately gave SQLite two independent sets of
	// connections writing one file: the first real pairing filled the log with
	// `database is locked (5)` from whatsmeow trying to save an identity key while the
	// mapping was being written, and a message that cannot have its identity saved
	// cannot be decrypted.
	db, err := sql.Open(dialect, dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", dialect, err)
	}
	if dialect == dialectSQLite {
		// A file holds one writer at a time whatever the pool says, and a pool that
		// hands out more connections than that only converts waiting into
		// `database is locked`. Serialising here is what makes busy_timeout the
		// backstop rather than the mechanism.
		db.SetMaxOpenConns(1)
	}

	devices := sqlstore.NewWithDB(db, dialect, nil)
	if err := devices.Upgrade(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: open the device store: %w", err)
	}

	c := &Container{db: db, devices: devices, dialect: dialect, log: log}
	if err := c.migrate(ctx); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

// Ping reports whether the database is answering. Readiness asks, because a session
// cannot be opened or paired without it.
func (c *Container) Ping(ctx context.Context) error {
	if err := c.db.PingContext(ctx); err != nil {
		return fmt.Errorf("store: ping: %w", err)
	}
	return nil
}

// Devices is whatsmeow's own store, which the engine hands to a client.
func (c *Container) Devices() *sqlstore.Container { return c.devices }

// Close releases the pool. There is only one, and whatsmeow's container wraps it
// rather than owning it, so closing it here is the whole of it.
func (c *Container) Close() error {
	return c.db.Close()
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
//
// The JID is stored exactly as WhatsApp issued it, device part included, because that
// is the key whatsmeow files the device under and the one `GetDevice` answers to.
// Normalising it away would leave every restart looking up a device that is there
// under a name we no longer hold, reading it as unpaired, and pairing again.
func (c *Container) Bind(ctx context.Context, sid string, jid types.JID) error {
	if sid == "" {
		return errors.New("store: bind needs a session id")
	}
	if jid.IsEmpty() {
		return fmt.Errorf("store: bind %s needs a jid", sid)
	}

	// An account belongs to one session, and re-pairing it issues a new device rather
	// than reusing the old one, so the match is on the account and not on the device.
	displaced, err := c.displacedBy(ctx, sid, jid.User)
	if err != nil {
		return err
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: bind %s: %w", sid, err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, c.rebind(`DELETE FROM wac_session_device WHERE account = ? AND sid <> ?`),
		jid.User, sid); err != nil {
		return fmt.Errorf("store: bind %s: %w", sid, err)
	}
	if _, err := tx.ExecContext(ctx, c.rebind(`
		INSERT INTO wac_session_device (sid, jid, account, bound_at) VALUES (?, ?, ?, ?)
		ON CONFLICT (sid) DO UPDATE SET
			jid = excluded.jid, account = excluded.account, bound_at = excluded.bound_at`),
		sid, jid.String(), jid.User, time.Now().UnixMilli()); err != nil {
		return fmt.Errorf("store: bind %s: %w", sid, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: bind %s: %w", sid, err)
	}

	// The devices the displaced mappings pointed at are credentials no session can reach
	// any more, so they are deleted rather than left to accumulate. Failing the pairing
	// over one that will not delete would be the worse outcome, so this is reported and
	// not raised.
	//
	// What this does not do is end the displaced session. WhatsApp allows an account
	// several companion devices, so pairing a second one does not unlink the first: if
	// the displaced session is running somewhere, its socket goes on working against the
	// account with credentials that are no longer written down, and it is only on the
	// next restart that it finds itself unpaired. Ending it needs a session-scoped stop
	// that reaches whichever instance runs it, which is the same fleet-wide machinery
	// M2 brings for the write fence, and is tracked with it.
	for _, old := range displaced {
		c.deleteDevice(ctx, old)
	}
	return nil
}

// displacedBy lists the devices bound to an account under some other session.
func (c *Container) displacedBy(ctx context.Context, sid, account string) ([]types.JID, error) {
	rows, err := c.db.QueryContext(ctx,
		c.rebind(`SELECT jid FROM wac_session_device WHERE account = ? AND sid <> ?`), account, sid)
	if err != nil {
		return nil, fmt.Errorf("store: read the mappings of %s: %w", account, err)
	}
	defer func() { _ = rows.Close() }()

	var displaced []types.JID
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("store: read the mappings of %s: %w", account, err)
		}
		jid, err := types.ParseJID(raw)
		if err != nil {
			c.log.Warn().Str("sid", sid).Msg("a mapping holds an unreadable jid; leaving its device alone")
			continue
		}
		displaced = append(displaced, jid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read the mappings of %s: %w", account, err)
	}
	return displaced, nil
}

func (c *Container) deleteDevice(ctx context.Context, jid types.JID) {
	device, err := c.devices.GetDevice(ctx, jid)
	if err != nil || device == nil {
		if err != nil {
			c.log.Warn().Err(err).Msg("failed to read a displaced device")
		}
		return
	}
	if err := c.devices.DeleteDevice(ctx, device); err != nil {
		c.log.Warn().Err(err).Msg("failed to delete a displaced device")
	}
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
	statements := []string{`
		CREATE TABLE IF NOT EXISTS wac_session_device (
			sid      TEXT   PRIMARY KEY,
			jid      TEXT   NOT NULL UNIQUE,
			account  TEXT   NOT NULL,
			bound_at BIGINT NOT NULL
		)`,
		// Unique, not just indexed. The rule is one session per account, and Bind
		// enforced it by reading the competing mappings and then writing: two instances
		// pairing the same number at once both read nothing, both write, and the account
		// ends up on two sessions with the displacement never happening. A constraint is
		// the only version of that check two transactions cannot both pass.
		`CREATE UNIQUE INDEX IF NOT EXISTS wac_session_device_account ON wac_session_device (account)`,
	}
	for _, statement := range statements {
		if _, err := c.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("store: create wac_session_device: %w", err)
		}
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

// parseURL splits a database url into the dialect and the dsn the driver wants.
func parseURL(address string) (dialect, dsn string, err error) {
	switch {
	case address == "":
		return "", "", ErrNoDatabase
	case strings.HasPrefix(address, "postgres://"), strings.HasPrefix(address, "postgresql://"):
		return dialectPostgres, address, nil
	case strings.HasPrefix(address, "file:"):
		return dialectSQLite, sqliteDefaults(address), nil
	case strings.HasPrefix(address, "sqlite://"):
		return dialectSQLite, sqliteDefaults("file:" + strings.TrimPrefix(address, "sqlite://")), nil
	case strings.HasPrefix(address, "sqlite:"):
		return dialectSQLite, sqliteDefaults("file:" + strings.TrimPrefix(address, "sqlite:")), nil
	default:
		return "", "", fmt.Errorf("store: %q is not a database url this connector understands", address)
	}
}

// sqliteDefaults fills in what the driver leaves off and whatsmeow needs on. An
// operator who spells any of these themselves keeps their value; the defaults are for
// the url the README documents, which is a path and nothing else.
//
// Each one is here because of a failure, not a preference:
//
//   - foreign_keys: whatsmeow refuses to bring its schema up without it, so first boot
//     dies on `foreign keys are not enabled`.
//   - journal_mode(WAL): the default rollback journal blocks every reader for the
//     length of a write, and whatsmeow reads on the decrypt path while it writes on the
//     receipt path.
//   - busy_timeout: without it a contended write returns `database is locked` on the
//     spot instead of waiting. That is what a live pairing produced: identity keys that
//     could not be saved, messages that then could not be decrypted, and an app state
//     sync that failed because the key share it needed was inside one of them.
//   - _txlock=immediate: a transaction that starts out reading and later writes has to
//     upgrade its lock, and two of those upgrading at once is a deadlock SQLite breaks
//     by failing one of them, which no timeout can save. Taking the write lock at BEGIN
//     turns that into ordinary waiting.
func sqliteDefaults(dsn string) string {
	base, query, _ := strings.Cut(dsn, "?")
	values, err := url.ParseQuery(query)
	if err != nil {
		// An unparseable query is the operator's to fix, and rewriting it here would
		// hide which half they got wrong.
		return dsn
	}

	for pragma, setting := range map[string]string{
		"foreign_keys": "foreign_keys(1)",
		"journal_mode": "journal_mode(WAL)",
		"busy_timeout": "busy_timeout(10000)",
	} {
		if !hasPragma(values["_pragma"], pragma) {
			values.Add("_pragma", setting)
		}
	}
	if values.Get("_txlock") == "" {
		values.Set("_txlock", "immediate")
	}
	return base + "?" + values.Encode()
}

// hasPragma reports whether the operator already set one, whatever they set it to.
func hasPragma(pragmas []string, name string) bool {
	for _, pragma := range pragmas {
		if strings.HasPrefix(strings.TrimSpace(pragma), name) {
			return true
		}
	}
	return false
}
