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
// What arbitration does not do on its own is stop a session that has been handed on from
// finishing a write it had started, and those land on top of what the new owner has since
// learned. Cancelling the session's context covers most of that for free -- whatsmeow gives
// that context to every node handler, and a statement on a cancelled one is refused before
// it reaches the database -- but not the work the library deliberately detaches from it.
// Fence is the other half: it stands in front of every write a device can make and refuses
// them all from the moment this instance stops owning the session, whichever context they
// arrive with.
//
// What is left is the window between a lease running out and this instance learning it. In
// there this instance still believes it is the owner and the fence still says yes, because
// nothing in a write says which epoch made it. Closing that needs the database to arbitrate,
// which is issue #55.
package store

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"slices"
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

// DB is the pool, for the tests that have to look at the schema itself. Nothing in the
// connector reaches for it: everything else goes through the methods above.
func (c *Container) DB() *sql.DB { return c.db }

// Devices is whatsmeow's own store. Nothing in the connector reaches for it any more --
// a session gets its device from Device, which fences it -- and what comes out of here
// does not stand behind any fence, so a caller that saves a device through this container
// rather than through the device itself takes the fence off it.
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
//
// The fence is taken rather than applied by the caller so that there is no way to get a
// device from here without one. A session builds a device more than once -- a logout and
// a stale mapping both send it back for another -- and the second of those was where an
// unfenced one first got in.
func (c *Container) device(ctx context.Context, sid string, fence *Fence) (*store.Device, error) {
	jid, bound, err := c.lookup(ctx, sid)
	if err != nil {
		return nil, err
	}
	if !bound {
		return Fenced(c.devices.NewDevice(), fence), nil
	}

	device, err := c.devices.GetDevice(ctx, jid)
	if err != nil {
		return nil, fmt.Errorf("store: read the device of %s: %w", sid, err)
	}
	if device == nil {
		// The mapping points at a device whatsmeow does not hold, and clearing it is a
		// write -- the one write on a path that otherwise reads. Fenced like any other,
		// and for a sharper reason than most: Bind runs before the device is saved, so a
		// session that took this one over has a moment where its mapping is exactly this
		// shape. Cleared by the instance on its way out, the new owner's device is left
		// with nothing pointing at it.
		if err := fence.held(); err != nil {
			return nil, err
		}
		if err := c.unbind(ctx, sid); err != nil {
			return nil, err
		}
		return Fenced(c.devices.NewDevice(), fence), nil
	}
	return Fenced(device, fence), nil
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
func (c *Container) bind(ctx context.Context, sid string, jid types.JID) error {
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
func (c *Container) forget(ctx context.Context, sid string) error {
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
	// The media parts go with the mapping, by the cascade on wac_media_part rather than
	// by a delete here. An explicit one would only cover the case the constraint already
	// covers -- foreign keys are on in both dialects, and whatsmeow refuses to bring its
	// own schema up without them, so a store with them off does not boot at all -- and
	// it would leave the case a delete cannot cover, which is a write already on its way
	// when the mapping goes.
	return c.unbind(ctx, sid)
}

// JID reports which device a session is bound to, and whether it is bound at all.
func (c *Container) jid(ctx context.Context, sid string) (types.JID, bool, error) {
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
// sqlstore.New, so this covers the mapping table and the media parts.
//
// The mapping table is brought up on its own and first, because the duplicates in it
// have to be cleared out before the unique index that forbids them can be built.
func (c *Container) migrate(ctx context.Context) error {
	const mapping = `
		CREATE TABLE IF NOT EXISTS wac_session_device (
			sid      TEXT   PRIMARY KEY,
			jid      TEXT   NOT NULL UNIQUE,
			account  TEXT   NOT NULL,
			bound_at BIGINT NOT NULL
		)`
	if _, err := c.db.ExecContext(ctx, mapping); err != nil {
		return fmt.Errorf("store: create wac_session_device: %w", err)
	}
	if err := c.dropDuplicateMappings(ctx); err != nil {
		return err
	}

	for _, statement := range []string{
		// The account index was not unique to begin with, and `IF NOT EXISTS` on the
		// same name is a no-op against a store that already has it: an upgraded database
		// would keep the old one and never gain the constraint. Dropped by name and
		// replaced under a new one, so the rename is what proves the new index exists.
		`DROP INDEX IF EXISTS wac_session_device_account`,
		// Unique, not just indexed. The rule is one session per account, and Bind
		// enforced it by reading the competing mappings and then writing: two instances
		// pairing the same number at once both read nothing, both write, and the account
		// ends up on two sessions with the displacement never happening. A constraint is
		// the only version of that check two transactions cannot both pass.
		`CREATE UNIQUE INDEX IF NOT EXISTS wac_session_device_one_per_account ON wac_session_device (account)`,
		// How to fetch a message's file a second time, for as long as the deployment is
		// willing to. Keyed by the session as well as the message, because a message id
		// is WhatsApp's to reuse across accounts and this table is read by whichever
		// instance holds the session when the client comes back.
		//
		// Keys and digests are TEXT holding base64 rather than bytes: it is the same
		// column in both dialects, and three fields of 32 bytes is not a size worth a
		// difference in the schema for. `from_me` is a BIGINT for the same reason: a
		// boolean is spelled differently in the two dialects and this table has held to
		// one spelling throughout.
		//
		// `receipt_chat`, `sender` and `from_me` are not for the download -- that needs
		// only the path, the key and the digests. They are the three things a request to
		// upload the file again is addressed with, once WhatsApp has dropped it.
		//
		// `receipt_chat` is a column of its own and not the two beside it, and that is
		// the point rather than duplication. `chat_kind` and `chat_id` are what the
		// message was *published* under, and for a broadcast that is deliberately not
		// where it was sent: WhatsApp shows such a message in the direct chat with
		// whoever sent it, so the published chat is the sender and the list it came
		// through is gone. A receipt addressed from that names a message WhatsApp cannot
		// find.
		//
		// The foreign key is what makes a row impossible for a session that is not
		// bound, and it is doing more than tidiness. Ownership moves between instances,
		// so the old owner's handler can still be inside a write when the session has
		// already been unpaired here: the delete finds nothing to take, the write lands
		// afterwards as an insert, and a session that no longer exists is left holding
		// the key to somebody's file until the retention sweep. No ordering argument
		// closes that -- the constraint does, by refusing the parentless row outright.
		`CREATE TABLE IF NOT EXISTS wac_media_part (
			sid             TEXT   NOT NULL,
			message_id      TEXT   NOT NULL,
			chat_kind       TEXT   NOT NULL,
			chat_id         TEXT   NOT NULL,
			kind            TEXT   NOT NULL,
			direct_path     TEXT   NOT NULL,
			media_key       TEXT   NOT NULL,
			file_enc_sha256 TEXT   NOT NULL,
			file_sha256     TEXT   NOT NULL,
			file_length     BIGINT NOT NULL,
			mime            TEXT   NOT NULL,
			filename        TEXT   NOT NULL,
			receipt_chat    TEXT   NOT NULL DEFAULT '',
			sender          TEXT   NOT NULL DEFAULT '',
			from_me         BIGINT NOT NULL DEFAULT 0,
			stored_at       BIGINT NOT NULL,
			PRIMARY KEY (sid, message_id),
			FOREIGN KEY (sid) REFERENCES wac_session_device (sid) ON DELETE CASCADE
		)`,
		// The sweep reads nothing but the clock, and it runs on a table with a row per
		// media message the deployment ever received.
		`CREATE INDEX IF NOT EXISTS wac_media_part_stored_at ON wac_media_part (stored_at)`,
		// A bubble that has been scheduled and not yet decided. The decision lives in a
		// timer, which lives as long as the process; the session it belongs to does not,
		// so without this a deploy inside the window loses the bubble and the chat is
		// left showing nothing at all.
		//
		// The same foreign key as above, for the same reason and with one more: this row
		// is published to a client on behalf of a session, so a row that outlives the
		// pairing would put a bubble in a chat the account no longer has.
		//
		// No retention sweep. Every row is deleted by whichever of its two endings comes
		// first, and one that is left is one whose session has not been picked up yet --
		// which is precisely the row that still has work to do. The cascade is what
		// clears the rows of a session that is never coming back.
		`CREATE TABLE IF NOT EXISTS wac_pending_placeholder (
			sid        TEXT   NOT NULL,
			message_id TEXT   NOT NULL,
			message    TEXT   NOT NULL,
			learned_at BIGINT NOT NULL,
			due_at     BIGINT NOT NULL,
			PRIMARY KEY (sid, message_id),
			FOREIGN KEY (sid) REFERENCES wac_session_device (sid) ON DELETE CASCADE
		)`,
	} {
		if _, err := c.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("store: bring the connector's own schema up: %w", err)
		}
	}
	// The columns a store that predates them does not have. `CREATE TABLE IF NOT EXISTS`
	// above does nothing to a table that is already there, so a deployment upgrading into
	// this would keep the old shape and every write would fail on the missing column.
	//
	// Added rather than the table recreated, because a deployment has rows in it: they
	// are what a client's `message.download_media` reads, and dropping them would lose
	// the file of every message received before the upgrade.
	for _, column := range []struct{ name, definition string }{
		{"receipt_chat", "TEXT NOT NULL DEFAULT ''"},
		{"sender", "TEXT NOT NULL DEFAULT ''"},
		{"from_me", "BIGINT NOT NULL DEFAULT 0"},
	} {
		if err := c.addColumn(ctx, "wac_media_part", column.name, column.definition); err != nil {
			return err
		}
	}
	return nil
}

// addColumn adds one column to a table that may already have it.
//
// Asked rather than attempted, because the two dialects disagree about the attempt:
// Postgres takes `ADD COLUMN IF NOT EXISTS` and SQLite does not, and SQLite's answer to
// a column that is already there is an error indistinguishable by type from the ones
// that matter. Asking first is one round trip on a path that runs once per process
// start.
//
// And asked again if the attempt fails, because asking first is not a lock. Two
// instances starting together -- which is what a rolling deploy is -- can both find the
// column missing, and the one that loses gets a duplicate-column error for work the
// other has already done. Re-reading is what tells "somebody beat me to it" from "this
// did not happen", and the first of those is not a reason to refuse to start.
func (c *Container) addColumn(ctx context.Context, table, column, definition string) error {
	present, err := c.hasColumn(ctx, table, column)
	if err != nil || present {
		return err
	}
	if _, err := c.db.ExecContext(ctx, `ALTER TABLE `+table+` ADD COLUMN `+column+` `+definition); err != nil {
		switch present, reread := c.hasColumn(ctx, table, column); {
		case reread != nil:
			return reread
		case present:
			return nil
		}
		return fmt.Errorf("store: add %s.%s: %w", table, column, err)
	}
	return nil
}

// hasColumn reports whether a table has a column, asking about the table this
// connection's own statements would reach and no other.
//
// That qualification is the whole of the Postgres branch. `information_schema.columns`
// filtered by table name alone spans every schema the role can see, so a copy of this
// table in another schema -- an older one, a neighbour's -- answers for the one the
// writes actually land in, and the column is reported present on a table that has not
// got it. `to_regclass` resolves the name through `search_path` exactly the way the DML
// does, so what is asked about is what is written to.
func (c *Container) hasColumn(ctx context.Context, table, column string) (bool, error) {
	query := `SELECT 1 FROM pragma_table_info(?) WHERE name = ?`
	if c.dialect == dialectPostgres {
		query = `SELECT 1 FROM pg_attribute
			WHERE attrelid = to_regclass(?) AND attname = ? AND attnum > 0 AND NOT attisdropped`
	}
	var present int
	switch err := c.db.QueryRowContext(ctx, c.rebind(query), table, column).Scan(&present); {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, fmt.Errorf("store: ask whether %s.%s is there: %w", table, column, err)
	}
}

// dropDuplicateMappings clears out whatever the old, permissive account index let
// through, so a unique one can be built over it.
//
// The rule was always one session per account, so a second mapping is a row that should
// never have been written. Which one to keep is decided by the credentials and not by
// the clock: Bind writes the mapping before whatsmeow writes the device, so an instance
// that died mid-pairing leaves the newest mapping of all pointing at a device that never
// existed. Keeping that one deletes a working device to preserve an empty row, Device
// then unbinds the survivor for having no credentials, and an upgrade has made an
// account that was paired pair again. So the winner is the most recent mapping whose
// device is actually on disk, and only when none of them has one does it fall back to
// the most recent outright, which is the best a set of empty rows allows.
//
// The devices the losers named go with them, the way Bind takes a displaced device: no
// mapping can reach them afterwards, and leaving them is leaving authentication material
// on disk that nothing will ever use or clean up.
func (c *Container) dropDuplicateMappings(ctx context.Context) error {
	const contested = `
		SELECT sid, jid, account, bound_at FROM wac_session_device
		WHERE account IN (
			SELECT account FROM wac_session_device GROUP BY account HAVING COUNT(*) > 1
		)`

	rows, err := c.db.QueryContext(ctx, contested)
	if err != nil {
		return fmt.Errorf("store: find the mappings the old index allowed: %w", err)
	}
	type mapping struct {
		sid      string
		jid      types.JID
		readable bool
		boundAt  int64
	}
	byAccount := map[string][]mapping{}
	for rows.Next() {
		var sid, raw, account string
		var boundAt int64
		if err := rows.Scan(&sid, &raw, &account, &boundAt); err != nil {
			_ = rows.Close()
			return fmt.Errorf("store: read the mappings the old index allowed: %w", err)
		}
		held := mapping{sid: sid, boundAt: boundAt}
		if jid, parseErr := types.ParseJID(raw); parseErr != nil {
			c.log.Warn().Str("sid", sid).Msg("a duplicate mapping holds an unreadable jid; leaving its device alone")
		} else {
			held.jid, held.readable = jid, true
		}
		byAccount[account] = append(byAccount[account], held)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("store: read the mappings the old index allowed: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("store: read the mappings the old index allowed: %w", err)
	}

	var displaced []mapping
	for _, candidates := range byAccount {
		slices.SortFunc(candidates, func(a, b mapping) int {
			if a.boundAt != b.boundAt {
				return cmp.Compare(b.boundAt, a.boundAt)
			}
			return cmp.Compare(b.sid, a.sid)
		})
		// Newest first, so the first one holding credentials is both the most recent
		// mapping and a usable one. Zero is the fallback the loop leaves standing.
		winner := 0
		for i, candidate := range candidates {
			if !candidate.readable {
				continue
			}
			held, err := c.hasDevice(ctx, candidate.jid)
			if err != nil {
				// Not "no credentials". A lookup that failed says nothing about what is
				// on disk, and reading it as absence is how the mapping that has the
				// device becomes a loser and gets deleted — the very thing this rule
				// exists to prevent. The duplicates keep for another start; the unique
				// index this precedes will not be built either, so nothing here is
				// half-done.
				return fmt.Errorf("store: read the device of %s: %w", candidate.sid, err)
			}
			if held {
				winner = i
				break
			}
		}
		for i, candidate := range candidates {
			if i != winner {
				displaced = append(displaced, candidate)
			}
		}
	}
	if len(displaced) == 0 {
		return nil
	}

	for _, old := range displaced {
		if _, err := c.db.ExecContext(ctx,
			c.rebind(`DELETE FROM wac_session_device WHERE sid = ?`), old.sid); err != nil {
			return fmt.Errorf("store: drop the mapping of %s: %w", old.sid, err)
		}
		if old.readable {
			c.deleteDevice(ctx, old.jid)
		}
	}
	c.log.Warn().Int("dropped", len(displaced)).
		Msg("more than one session held the same account; the losing mappings and their devices are gone")
	return nil
}

// hasDevice reports whether whatsmeow still holds the credentials a mapping names. The
// error is returned rather than folded into the answer, because "there is no device" and
// "this could not be read" are the same value and opposite facts: one is a row to drop,
// the other is a question nobody has answered.
func (c *Container) hasDevice(ctx context.Context, jid types.JID) (bool, error) {
	device, err := c.devices.GetDevice(ctx, jid)
	if err != nil {
		return false, err
	}
	return device != nil, nil
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
