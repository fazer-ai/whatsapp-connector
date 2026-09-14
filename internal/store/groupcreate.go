package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// GroupCreation is what one attempt at making a group left behind: the intent, written
// before anything was asked of WhatsApp, and the group it turned into, written once
// WhatsApp said which one that is.
//
// The two halves are the point. Every other command this connector serves converges on a
// retry, so the record of it only has to exist eventually; `group.create` is the one whose
// second run makes a second group, and a record written only after the fact is no cover for
// a process that dies in between.
//
// What ties the halves together is the key. The creation goes out carrying it, WhatsApp
// echoes it on the notification that announces the group, and a notification nobody
// acknowledged is redelivered to whoever holds the session next -- measured on the bench,
// in `probe131b`. So the instance that took a dead one's session over learns exactly which
// group it made, rather than inferring it from a subject and a timestamp.
type GroupCreation struct {
	// Key is what the creation was sent under, and what WhatsApp echoes back.
	Key string
	// Subject is what the group was to be called.
	Subject string
	// StartedAt is when the intent was written, necessarily before WhatsApp was asked.
	StartedAt time.Time
	// JID names the group, once WhatsApp has said which one. Empty on an attempt that has
	// not got there: either it is still running, or whatever was running it is gone.
	JID string
}

// Done reports whether this attempt knows which group it made.
func (c GroupCreation) Done() bool { return c.JID != "" }

// BeginGroupCreate writes the intent to make a group under this key, and reports what is
// already on record for it.
//
// Written before the group is asked for, and that order is the whole mechanism: a record
// written afterwards is exactly the one a crash in between does not leave. It never
// overwrites, so a second delivery of one command reads the first delivery's key -- the
// only key WhatsApp will ever echo for it.
func (s *Scoped) BeginGroupCreate(
	ctx context.Context, attempt, key, subject string, now time.Time,
) (GroupCreation, bool, error) {
	if err := s.fence.held(); err != nil {
		return GroupCreation{}, false, err
	}
	if attempt == "" || key == "" {
		return GroupCreation{}, false, fmt.Errorf("store: a group creation needs an attempt and a key")
	}
	// One statement, and that is what makes it safe: writing the intent and reading back
	// whatever was already there as two statements leaves a gap the sweep can delete the
	// row in, and the read would then answer "nothing is on record" about an attempt that
	// had one -- sending the caller off to create under a key nothing is filed against.
	//
	// What the conflict updates is only `touched_at`, and only forward. The key, the
	// subject and the instant are the first delivery's and stay its own; the clock is
	// pushed out because being asked about is what says the command is still being
	// delivered, which is the same reasoning `redisx.Idempotency.Recall` applies to the
	// ledger entry this record stands behind. Without it the two clocks run independently:
	// a command retried for longer than the retention outlives the only record that can
	// say which group it already made.
	const claim = `
		INSERT INTO wac_group_create (sid, attempt, create_key, subject, started_at, touched_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (sid, attempt) DO UPDATE SET touched_at = ?
		RETURNING create_key, subject, started_at, group_jid`
	began, asked := now.UnixMilli(), time.Now().UnixMilli()
	var (
		found   GroupCreation
		started int64
		jid     sql.NullString
	)
	if err := s.container.db.QueryRowContext(ctx, s.container.rebind(claim),
		s.sid, attempt, key, subject, began, began, asked).
		Scan(&found.Key, &found.Subject, &started, &jid); err != nil {
		return GroupCreation{}, false, fmt.Errorf("store: begin the group creation %s of %s: %w", attempt, s.sid, err)
	}
	found.StartedAt, found.JID = time.UnixMilli(started), jid.String
	// Whose key came back is what says which of the two happened. Keys are drawn at random
	// per delivery, so a row answering with this delivery's key is the row this delivery
	// just wrote, and any other key belongs to a delivery that came first -- and that one's
	// key is the one WhatsApp will echo.
	return found, found.Key != key, nil
}

// FinishGroupCreate names the group an attempt made, by the name it was filed under.
//
// Only the first naming stands. One command has one answer, and both the creation's own
// reply and WhatsApp's notification can arrive carrying it.
func (s *Scoped) FinishGroupCreate(ctx context.Context, attempt, jid string) error {
	if err := s.fence.held(); err != nil {
		return err
	}
	if jid == "" {
		return fmt.Errorf("store: a finished group creation needs the group it made")
	}
	const name = `
		UPDATE wac_group_create SET group_jid = ?, touched_at = ?
		WHERE sid = ? AND attempt = ? AND group_jid IS NULL`
	if _, err := s.container.db.ExecContext(ctx, s.container.rebind(name),
		jid, time.Now().UnixMilli(), s.sid, attempt); err != nil {
		return fmt.Errorf("store: finish the group creation %s of %s: %w", attempt, s.sid, err)
	}
	return nil
}

// FinishGroupCreateByKey names the group for whichever attempt was sent under this key, and
// reports whether there was one waiting for it.
//
// This is the path WhatsApp's own notification takes, and the key is what makes it exact:
// the notification carries back what the creation went out with, so the group in hand
// belongs to that attempt and to no other -- including when the attempt was made by an
// instance that is no longer running, which is the case this whole table exists for.
func (s *Scoped) FinishGroupCreateByKey(ctx context.Context, key, jid string) (bool, error) {
	if err := s.fence.held(); err != nil {
		return false, err
	}
	if key == "" || jid == "" {
		return false, fmt.Errorf("store: naming a group by its key needs both the key and the group")
	}
	const name = `
		UPDATE wac_group_create SET group_jid = ?, touched_at = ?
		WHERE sid = ? AND create_key = ? AND group_jid IS NULL`
	done, err := s.container.db.ExecContext(ctx, s.container.rebind(name),
		jid, time.Now().UnixMilli(), s.sid, key)
	if err != nil {
		return false, fmt.Errorf("store: finish the group creation under %s of %s: %w", key, s.sid, err)
	}
	named, err := done.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: count what naming the creation under %s of %s settled: %w", key, s.sid, err)
	}
	return named > 0, nil
}

// AbandonGroupCreate forgets an attempt that certainly made nothing.
//
// The intent exists to say "a group may have been made and nobody has written down which
// one". An attempt WhatsApp refused says no such thing, so the row is a redelivery's cover
// for a group that does not exist: left behind, it would have every later request for it
// wait on a notification that is never coming.
//
// Refuses to forget an attempt that already named a group. That one is the answer a
// redelivery is owed, and this call is not the one that decides it was wrong.
func (s *Scoped) AbandonGroupCreate(ctx context.Context, attempt string) error {
	if err := s.fence.held(); err != nil {
		return err
	}
	const drop = `DELETE FROM wac_group_create WHERE sid = ? AND attempt = ? AND group_jid IS NULL`
	if _, err := s.container.db.ExecContext(ctx, s.container.rebind(drop), s.sid, attempt); err != nil {
		return fmt.Errorf("store: abandon the group creation %s of %s: %w", attempt, s.sid, err)
	}
	return nil
}

// GroupCreation reads what is on record for one attempt.
func (s *Scoped) GroupCreation(ctx context.Context, attempt string) (GroupCreation, bool, error) {
	return s.container.groupCreation(ctx, s.sid, attempt)
}

func (c *Container) groupCreation(ctx context.Context, sid, attempt string) (GroupCreation, bool, error) {
	const read = `
		SELECT create_key, subject, started_at, group_jid FROM wac_group_create
		WHERE sid = ? AND attempt = ?`
	var (
		found   GroupCreation
		started int64
		jid     sql.NullString
	)
	err := c.db.QueryRowContext(ctx, c.rebind(read), sid, attempt).
		Scan(&found.Key, &found.Subject, &started, &jid)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return GroupCreation{}, false, nil
	case err != nil:
		return GroupCreation{}, false, fmt.Errorf("store: read the group creation %s of %s: %w", attempt, sid, err)
	}
	found.StartedAt, found.JID = time.UnixMilli(started), jid.String
	return found, true, nil
}

// SweepGroupCreations drops the attempts last written before the cutoff, and reports how
// many. Without a sweep the row count is the number of groups the deployment has ever made.
//
// By when the row was last written rather than when it began: what it covers is a
// redelivery, and a delivery can sit pending for as long as nobody acknowledges it, so an
// attempt still waiting to hear which group it made needs its row as much as a fresh one.
//
// What is left, stated rather than hidden: a redelivery arriving after both the ledger and
// this record have forgotten the command makes a second group, and no finite retention
// removes that. What bounds it in practice is the client's own `MAXLEN` trim on the command
// stream, which is a length and not a time and so is not this table's to measure.
func (c *Container) SweepGroupCreations(ctx context.Context, before time.Time) (int64, error) {
	const drop = `DELETE FROM wac_group_create WHERE touched_at < ?`
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
