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
// Held is what the table was holding under every address the account answers under, at the
// moment the write failed, and it is what makes this answerable rather than a guess. The
// question a rebuilt session has to settle is "which of these two copies is newer", which
// nothing in the record or the rows says; the question this turns it into is "has anything
// written a name here since", which the rows answer themselves. Rows still holding Held
// have not moved, so Name is still the newer copy. Anything else was written after this was
// filed, and what is kept here is the older one and goes.
type UnfiledName struct {
	// Name is what the account calls itself and what the table would not take.
	Name string
	// Held is every row's value at the moment of the failure, as one opaque string. Every
	// row, because the retry writes under every address: one that a read would not have
	// answered from is still one this would overwrite.
	Held string
}

// PutUnfiledName records that a name did not reach the contact table.
//
// Written when the write fails rather than before it is attempted. Both go to the same
// database, so a store that is refusing one is usually refusing the other: writing first
// would buy a window no wider than the two statements and pay for it on every rename. What
// this covers is the failure that is about the row rather than about the database -- a
// refused write, a statement that timed out under load -- which is the failure a restart
// can still be holding the answer to.
func (s *Scoped) PutUnfiledName(ctx context.Context, kind, name, held string) error {
	if err := s.fence.held(); err != nil {
		return err
	}
	return s.container.putUnfiledName(ctx, s.sid, kind, name, held, time.Now())
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

func (c *Container) putUnfiledName(ctx context.Context, sid, kind, name, held string, now time.Time) error {
	if sid == "" || kind == "" || name == "" {
		return fmt.Errorf("store: an unfiled name needs a session, a kind and a name, got %q, %q and %q", sid, kind, name)
	}
	const upsert = `
		INSERT INTO wac_own_name (sid, kind, name, held, marked_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (sid, kind) DO UPDATE SET
			name = excluded.name, held = excluded.held, marked_at = excluded.marked_at`
	if _, err := c.db.ExecContext(ctx, c.rebind(upsert), sid, kind, name, held, now.UnixMilli()); err != nil {
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
	var kept UnfiledName
	err := c.db.QueryRowContext(ctx,
		c.rebind(`SELECT name, held FROM wac_own_name WHERE sid = ? AND kind = ?`), sid, kind,
	).Scan(&kept.Name, &kept.Held)
	if errors.Is(err, sql.ErrNoRows) {
		return UnfiledName{}, false, nil
	}
	if err != nil {
		return UnfiledName{}, false, fmt.Errorf("store: read the unfiled %s name of %s: %w", kind, sid, err)
	}
	return kept, true, nil
}
