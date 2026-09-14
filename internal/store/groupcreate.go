package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// GroupCreation is what one attempt at making a group left behind: the intent, written
// before anything was asked of WhatsApp, and the group it turned into, written after.
//
// The two halves are the point. Every other command this connector serves converges on a
// retry, so the record of it only has to exist eventually; `group.create` is the one whose
// second run makes a second group, and a record written only after the fact is no cover for
// a process that dies in between. The intent is what a later attempt has to go on, and what
// it says is: an attempt under this name was begun at this instant, for a group with this
// subject. That is enough to look, which is all that was ever missing -- a group is findable
// on WhatsApp, unlike a message that may or may not have been sent.
type GroupCreation struct {
	// Subject is what the group was to be called, and what a later attempt looks for.
	Subject string
	// StartedAt is when the intent was written, which is necessarily before WhatsApp was
	// asked. A group of this account created before it cannot be this attempt's.
	StartedAt time.Time
	// JID names the group, once there is one. Empty on an attempt that has not finished:
	// either it is still running, or whatever was running it is gone.
	JID string
}

// Done reports whether this attempt got as far as naming the group it made.
func (c GroupCreation) Done() bool { return c.JID != "" }

// BeginGroupCreate writes the intent to make a group, and reports what is already on
// record under this name.
//
// Written before the group is asked for, and that order is the whole mechanism: a record
// written afterwards is exactly the one a crash in between does not leave. It never
// overwrites, because a second call under the same name is a redelivery and the first
// call's intent -- in particular the instant it began -- is the one that bounds the search.
func (s *Scoped) BeginGroupCreate(ctx context.Context, attempt, subject string, now time.Time) (GroupCreation, bool, error) {
	if err := s.fence.held(); err != nil {
		return GroupCreation{}, false, err
	}
	if attempt == "" {
		return GroupCreation{}, false, fmt.Errorf("store: a group creation needs a name to be filed under")
	}
	const insert = `
		INSERT INTO wac_group_create (sid, attempt, subject, started_at) VALUES (?, ?, ?, ?)
		ON CONFLICT (sid, attempt) DO NOTHING`
	done, err := s.container.db.ExecContext(ctx, s.container.rebind(insert),
		s.sid, attempt, subject, now.UnixMilli())
	if err != nil {
		return GroupCreation{}, false, fmt.Errorf("store: begin the group creation %s of %s: %w", attempt, s.sid, err)
	}
	if written, err := done.RowsAffected(); err == nil && written == 1 {
		// Nothing was there, so nothing has been attempted: this delivery is the first.
		return GroupCreation{Subject: subject, StartedAt: now}, false, nil
	}
	// A row was already there. Read rather than assumed, because what bounds the search is
	// the first delivery's instant and subject, not this one's.
	return s.container.groupCreation(ctx, s.sid, attempt)
}

// FinishGroupCreate names the group an attempt made.
//
// Only the first naming stands. A later attempt that reconciled its way to a group and a
// first attempt that came back slowly are both writing what they believe the answer is, and
// two answers to one command have to be one answer.
func (s *Scoped) FinishGroupCreate(ctx context.Context, attempt, jid string) error {
	if err := s.fence.held(); err != nil {
		return err
	}
	if jid == "" {
		return fmt.Errorf("store: a finished group creation needs the group it made")
	}
	const name = `UPDATE wac_group_create SET group_jid = ? WHERE sid = ? AND attempt = ? AND group_jid IS NULL`
	if _, err := s.container.db.ExecContext(ctx, s.container.rebind(name),
		jid, s.sid, attempt); err != nil {
		return fmt.Errorf("store: finish the group creation %s of %s: %w", attempt, s.sid, err)
	}
	return nil
}

// GroupCreation reads what is on record for one attempt.
func (s *Scoped) GroupCreation(ctx context.Context, attempt string) (GroupCreation, bool, error) {
	return s.container.groupCreation(ctx, s.sid, attempt)
}

func (c *Container) groupCreation(ctx context.Context, sid, attempt string) (GroupCreation, bool, error) {
	const read = `SELECT subject, started_at, group_jid FROM wac_group_create WHERE sid = ? AND attempt = ?`
	var (
		found   GroupCreation
		started int64
		jid     sql.NullString
	)
	err := c.db.QueryRowContext(ctx, c.rebind(read), sid, attempt).Scan(&found.Subject, &started, &jid)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return GroupCreation{}, false, nil
	case err != nil:
		return GroupCreation{}, false, fmt.Errorf("store: read the group creation %s of %s: %w", attempt, sid, err)
	}
	found.StartedAt, found.JID = time.UnixMilli(started), jid.String
	return found, true, nil
}

// SweepGroupCreations drops the attempts begun before the cutoff, and reports how many.
//
// They are kept only for as long as a redelivery of the command can still arrive, which is
// the transport's business and not this table's: the cutoff is the caller's. Without a sweep
// the row count is the number of groups the deployment has ever made.
func (c *Container) SweepGroupCreations(ctx context.Context, before time.Time) (int64, error) {
	const drop = `DELETE FROM wac_group_create WHERE started_at < ?`
	done, err := c.db.ExecContext(ctx, c.rebind(drop), before.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("store: sweep the group creations: %w", err)
	}
	swept, err := done.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: count the group creations swept: %w", err)
	}
	return swept, nil
}
