package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// The kinds of display name an account has of its own. They are stored separately because
// they change separately: a push name arrives on an app-state sync or on a message the
// account sent, a verified name on a business detail change, and one being behind says
// nothing about the other.
const (
	UnfiledPushName     = "push"
	UnfiledVerifiedName = "verified"
)

// UnfiledName is a display name of the account's own that did not reach the contact table,
// kept so the next process knows the row is behind it.
//
// Stale is what the row was holding when it refused the write, and it is what makes this
// answerable rather than a guess. The question a rebuilt session has to settle is "which
// of these two copies is newer", which nothing in the record or the row says; the question
// this turns it into is "has anything touched the row since", which the row answers itself.
// A row still holding Stale has not moved, so Name is still the newer of the two. A row
// holding anything else was written after this was filed, and this copy is the older one
// and goes.
type UnfiledName struct {
	// Name is what the account calls itself and what the row would not take.
	Name string
	// Stale is what the row was holding instead, at the moment it refused.
	Stale string
}

// PutUnfiledName records that a name did not reach the contact table.
//
// Written when the write fails rather than before it is attempted. Both go to the same
// database, so a store that is refusing one is usually refusing the other: writing first
// would buy a window no wider than the two statements and pay for it on every rename. What
// this covers is the failure that is about the row rather than about the database -- a
// refused write, a statement that timed out under load -- which is the failure a restart
// can still be holding the answer to.
func (s *Scoped) PutUnfiledName(ctx context.Context, kind, name, stale string) error {
	if err := s.fence.held(); err != nil {
		return err
	}
	return s.container.putUnfiledName(ctx, s.sid, kind, name, stale, time.Now())
}

// DropUnfiledName forgets a name, because the row took it or because the row moved on.
func (s *Scoped) DropUnfiledName(ctx context.Context, kind string) error {
	if err := s.fence.held(); err != nil {
		return err
	}
	return s.container.dropUnfiledName(ctx, s.sid, kind)
}

// UnfiledName reads back what this session was holding that the table would not take.
func (s *Scoped) UnfiledName(ctx context.Context, kind string) (UnfiledName, bool, error) {
	return s.container.unfiledName(ctx, s.sid, kind)
}

func (c *Container) putUnfiledName(ctx context.Context, sid, kind, name, stale string, now time.Time) error {
	if sid == "" || kind == "" || name == "" {
		return fmt.Errorf("store: an unfiled name needs a session, a kind and a name, got %q, %q and %q", sid, kind, name)
	}
	const upsert = `
		INSERT INTO wac_own_name (sid, kind, name, stale, marked_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (sid, kind) DO UPDATE SET
			name = excluded.name, stale = excluded.stale, marked_at = excluded.marked_at`
	if _, err := c.db.ExecContext(ctx, c.rebind(upsert), sid, kind, name, stale, now.UnixMilli()); err != nil {
		return fmt.Errorf("store: record the unfiled %s name of %s: %w", kind, sid, err)
	}
	return nil
}

func (c *Container) dropUnfiledName(ctx context.Context, sid, kind string) error {
	if _, err := c.db.ExecContext(ctx,
		c.rebind(`DELETE FROM wac_own_name WHERE sid = ? AND kind = ?`), sid, kind); err != nil {
		return fmt.Errorf("store: forget the unfiled %s name of %s: %w", kind, sid, err)
	}
	return nil
}

func (c *Container) unfiledName(ctx context.Context, sid, kind string) (UnfiledName, bool, error) {
	var held UnfiledName
	err := c.db.QueryRowContext(ctx,
		c.rebind(`SELECT name, stale FROM wac_own_name WHERE sid = ? AND kind = ?`), sid, kind,
	).Scan(&held.Name, &held.Stale)
	if errors.Is(err, sql.ErrNoRows) {
		return UnfiledName{}, false, nil
	}
	if err != nil {
		return UnfiledName{}, false, fmt.Errorf("store: read the unfiled %s name of %s: %w", kind, sid, err)
	}
	return held, true, nil
}
