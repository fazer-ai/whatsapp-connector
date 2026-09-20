package store

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	"modernc.org/sqlite"
)

// SQLite's `busy_timeout` and a caller's context deadline are two ceilings that do not
// know about each other, and the smaller does not win. A write already blocked on another
// writer of the same file sits out the whole `busy_timeout` before it looks at the context
// at all: measured on this build's driver and pragmas, a write under a 300ms deadline came
// back in 10.1s against `busy_timeout(10000)`, with `context deadline exceeded`. The error
// is the context's; the time is not. It is per call and not per command, so a command
// doing two writes pays two windows (#293).
//
// The wait needs a second writer, and inside one process there is not one: a single pool
// with `SetMaxOpenConns(1)` serialises in `database/sql`'s own queue, which honours the
// context exactly. What reaches it is a second process on the same file -- the overlap of
// a deploy, an operator's `sqlite3`, a backup -- and there it holds the whole queue of an
// account for ten seconds per store call.
//
// `busy_timeout` is per connection and settable at any time, so the fix is to derive it
// from what the caller still has, on the very connection the statement is about to run on.
// That is below `*sql.DB`, which is the only place that covers whatsmeow's own writes as
// well as this package's: they share one handle, and the library's call sites are not ours
// to wrap.
type budgetedConnector struct {
	driver.Connector
}

// sqliteBudgeted wraps the driver's connector so every statement runs under a
// `busy_timeout` derived from its caller's deadline.
func sqliteBudgeted(dsn string) (driver.Connector, error) {
	base, err := sqlite.NewConnector(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: sqlite connector: %w", err)
	}
	return budgetedConnector{Connector: base}, nil
}

func (c budgetedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: sqlite connect: %w", err)
	}
	return newBudgetedConn(conn)
}

// budgetSlack is how much longer than the caller's own deadline the busy wait is allowed
// to run.
//
// It exists so the error a caller gets is decided rather than raced. Derive exactly the
// time left and the two clocks expire at the same instant, and which one reports is the
// scheduler's to pick: either `context deadline exceeded`, which every caller above reads
// with `errors.Is`, or SQLite's own `database is locked`, which none of them do. Derive a
// little more and the context is first by construction.
//
// It is not the smallest slack that works. Measured against a second process holding a
// write, twenty runs at each of 0, 10ms, 25ms and 50ms, the context reported all eighty
// times, zero included -- what twenty runs cannot tell apart is "always" from "almost
// always" when the two instants coincide, and that is the tie this removes rather than
// wins.
//
// It is also the amount by which a contended call overruns the ceiling its caller
// declared, once per call, because the busy handler is what ends the wait and the context
// is only what names the error: 363..368ms over six runs against a three hundred
// millisecond ceiling. A caller that sets seconds will not notice; a test that sets three
// hundred milliseconds and asserts on the ceiling has to.
const budgetSlack = 50 * time.Millisecond

// budgetedConn holds what it forwards, rather than asserting per call.
//
// A wrapper decides what `database/sql` may use by what it implements, and it falls back
// in silence when something is missing: a transaction begun without its context or
// isolation level, a prepare without its context, a connection never reset between uses
// and never checked for validity. Nothing reports any of that -- not a log line, not an
// error -- so a wrapper that forwards only `ExecContext` and `QueryContext` passes a
// suite while quietly undoing four things the driver offered.
//
// Asserting here rather than at each call is also what makes a pin that stops offering one
// of these a failure at open, naming every one that is missing, instead of a panic inside
// a write: `TestADriverThatStoppedOfferingOneOfThemFailsTheOpen`. The other direction, a
// method declared here that the driver does not have, is
// `TestTheWrappedConnectionOffersWhatTheDriverOffers`.
type budgetedConn struct {
	driver.Conn

	exec    driver.ExecerContext
	query   driver.QueryerContext
	begin   driver.ConnBeginTx
	prepare driver.ConnPrepareContext
	reset   driver.SessionResetter
	valid   driver.Validator
	ping    driver.Pinger

	// fallback is what this connection was opened with, and what a call with no deadline
	// of its own gets back. Without it the previous caller's budget would stay on the
	// connection, which with one connection means one account's ceiling becoming
	// everybody's.
	//
	// Read off the connection rather than parsed out of the DSN. The driver accepts
	// `busy_timeout(10000)`, `busy_timeout=10000`, either with surrounding whitespace,
	// and `sqliteDefaults` leaves an operator's spelling alone -- measured, all five
	// forms open at 7777. A parser that knows one of them answers zero for the rest, and
	// zero here would turn every deadline-free call into an instant `database is locked`,
	// which is the failure `busy_timeout` was added to stop.
	fallback time.Duration
}

func newBudgetedConn(conn driver.Conn) (driver.Conn, error) {
	wrapped := &budgetedConn{Conn: conn}
	var missing []string
	var ok bool
	if wrapped.exec, ok = conn.(driver.ExecerContext); !ok {
		missing = append(missing, "ExecerContext")
	}
	if wrapped.query, ok = conn.(driver.QueryerContext); !ok {
		missing = append(missing, "QueryerContext")
	}
	if wrapped.begin, ok = conn.(driver.ConnBeginTx); !ok {
		missing = append(missing, "ConnBeginTx")
	}
	if wrapped.prepare, ok = conn.(driver.ConnPrepareContext); !ok {
		missing = append(missing, "ConnPrepareContext")
	}
	if wrapped.reset, ok = conn.(driver.SessionResetter); !ok {
		missing = append(missing, "SessionResetter")
	}
	if wrapped.valid, ok = conn.(driver.Validator); !ok {
		missing = append(missing, "Validator")
	}
	if wrapped.ping, ok = conn.(driver.Pinger); !ok {
		missing = append(missing, "Pinger")
	}
	if len(missing) > 0 {
		// A deployment problem rather than a runtime one: the pinned driver offers all
		// of these, and one going missing means the pin moved under this file. Every
		// name is reported, not the first: a pin bump that drops two is one round of
		// reading, not two.
		_ = conn.Close()
		return nil, fmt.Errorf("store: the sqlite driver no longer implements driver.%s, "+
			"so wrapping its connection would drop what database/sql does with it",
			strings.Join(missing, ", driver."))
	}
	fallback, err := readBusyTimeout(wrapped.query)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	wrapped.fallback = fallback
	return wrapped, nil
}

// readBusyTimeout asks the connection what it was opened with.
func readBusyTimeout(query driver.QueryerContext) (time.Duration, error) {
	rows, err := query.QueryContext(context.Background(), "PRAGMA busy_timeout", nil)
	if err != nil {
		return 0, fmt.Errorf("store: read sqlite busy_timeout: %w", err)
	}
	defer func() { _ = rows.Close() }()

	values := make([]driver.Value, len(rows.Columns()))
	if err := rows.Next(values); err != nil {
		return 0, fmt.Errorf("store: read sqlite busy_timeout: %w", err)
	}
	ms, ok := values[0].(int64)
	if !ok {
		// A build problem: this pragma answers one integer, and a driver that answers
		// something else has changed under this file.
		return 0, fmt.Errorf("store: sqlite reported busy_timeout as %T, wanted an integer", values[0])
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// budget puts the caller's remaining time on the connection, and the DSN's value back
// when the caller named no deadline.
//
// Every statement sets it, rather than setting and restoring around the ones that have a
// deadline: with `SetMaxOpenConns(1)` there is exactly one connection, shared with
// whatsmeow's device store, so a value left behind by one call is the ceiling the next
// call inherits. Setting unconditionally is one statement instead of two and leaves
// nothing to inherit.
func (c *budgetedConn) budget(ctx context.Context) {
	wanted := c.fallback
	if deadline, ok := ctx.Deadline(); ok {
		wanted = max(time.Until(deadline), 0) + budgetSlack
	}
	// On the connection's own terms, not the caller's: a pragma cancelled by the very
	// deadline it is about to install would leave the previous value in place, which is
	// the one case this exists to prevent.
	_, _ = c.exec.ExecContext(context.Background(),
		"PRAGMA busy_timeout = "+strconv.FormatInt(wanted.Milliseconds(), 10), nil)
}

func (c *budgetedConn) ExecContext(
	ctx context.Context, query string, args []driver.NamedValue,
) (driver.Result, error) {
	c.budget(ctx)
	// Unwrapped on purpose, unlike the forwards below: this is the statement's own
	// failure, and every classifier above reads it with `errors.Is` against the context
	// sentinels.
	return c.exec.ExecContext(ctx, query, args) //nolint:wrapcheck // see above
}

func (c *budgetedConn) QueryContext(
	ctx context.Context, query string, args []driver.NamedValue,
) (driver.Rows, error) {
	c.budget(ctx)
	return c.query.QueryContext(ctx, query, args) //nolint:wrapcheck // same as ExecContext above
}

func (c *budgetedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	// Budgeted like any statement, because with `_txlock=immediate` this is one: the
	// write lock is taken at BEGIN rather than at the first write, which is what keeps
	// two transactions from deadlocking on a lock upgrade. Left unbudgeted, a `BindDevice`
	// against a writer in another process took 10.08s under a 300ms deadline, measured
	// with this file's own wrapper otherwise in place.
	c.budget(ctx)
	tx, err := c.begin.BeginTx(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("store: sqlite begin: %w", err)
	}
	return tx, nil
}

func (c *budgetedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	// Deliberately not budgeted. A prepared statement's `ExecContext` is the statement's
	// own and never reaches this connection's, so a budget set here would be set for the
	// prepare and not for the execution -- a ceiling that looks applied and is not.
	// Nothing in this repository, in the pinned whatsmeow's `sqlstore`, or in the
	// `dbutil` it uses calls `PrepareContext` on this handle, and `database/sql` only
	// prepares on its own when the driver implements neither `ExecerContext` nor
	// `QueryerContext`, which this one does. `TestNothingHereReachesThePreparePath` is
	// the fence.
	stmt, err := c.prepare.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("store: sqlite prepare: %w", err)
	}
	return stmt, nil
}

func (c *budgetedConn) ResetSession(ctx context.Context) error {
	if err := c.reset.ResetSession(ctx); err != nil {
		return fmt.Errorf("store: sqlite reset session: %w", err)
	}
	return nil
}

func (c *budgetedConn) IsValid() bool { return c.valid.IsValid() }

func (c *budgetedConn) Ping(ctx context.Context) error {
	if err := c.ping.Ping(ctx); err != nil {
		return fmt.Errorf("store: sqlite ping: %w", err)
	}
	return nil
}
