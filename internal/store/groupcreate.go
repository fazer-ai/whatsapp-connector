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
	const name = `
		UPDATE wac_group_create SET group_jid = ?, settled_at = ?
		WHERE sid = ? AND attempt = ? AND group_jid IS NULL`
	if _, err := s.container.db.ExecContext(ctx, s.container.rebind(name),
		jid, time.Now().UnixMilli(), s.sid, attempt); err != nil {
		return fmt.Errorf("store: finish the group creation %s of %s: %w", attempt, s.sid, err)
	}
	return nil
}

// AbandonGroupCreate forgets an attempt that definitely made nothing.
//
// The intent exists to say "a group may have been made and nobody wrote down which one".
// An attempt WhatsApp refused made nothing, so it says no such thing, and leaving it open
// would be worse than useless: an open attempt is what stops a later one at the same name
// from reconciling, so two refused requests would block every retry of either, for good --
// neither can settle, and the sweep does not take open rows.
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

// GroupsClaimedByOtherAttempts answers which groups this session has already recorded as
// made by some attempt other than the one named.
//
// A search for the group an attempt made can only take one nothing else has claimed.
// Without this, two requests for a group by the same name interleave badly: the first
// writes its intent and dies before creating anything, the second creates its group, and
// the first's retry finds that group, answers with it, and files it as its own -- so one
// request is answered with another's conversation and the creation it asked for never
// happens.
func (s *Scoped) GroupsClaimedByOtherAttempts(ctx context.Context, except string) (map[string]struct{}, error) {
	const read = `SELECT group_jid FROM wac_group_create WHERE sid = ? AND attempt <> ? AND group_jid IS NOT NULL`
	rows, err := s.container.db.QueryContext(ctx, s.container.rebind(read), s.sid, except)
	if err != nil {
		return nil, fmt.Errorf("store: read the groups %s has already made: %w", s.sid, err)
	}
	defer func() { _ = rows.Close() }()
	claimed := map[string]struct{}{}
	for rows.Next() {
		var jid string
		if err := rows.Scan(&jid); err != nil {
			return nil, fmt.Errorf("store: read the groups %s has already made: %w", s.sid, err)
		}
		claimed[jid] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read the groups %s has already made: %w", s.sid, err)
	}
	return claimed, nil
}

// OtherAttemptsStillOpen reports whether this session has another attempt at a group by
// this subject that has not settled.
//
// It is what says a search cannot be trusted. Two unfinished attempts for the same subject
// make a group that matches both, and nothing on WhatsApp separates them: subject, creator
// and instant are the same evidence for each. Answering one of them with that group hands a
// request another's conversation and skips the creation it asked for, silently. Refusing is
// the honest answer, and it is one a retry can recover from once the other attempt settles.
func (s *Scoped) OtherAttemptsStillOpen(ctx context.Context, except, subject string) (bool, error) {
	const read = `
		SELECT 1 FROM wac_group_create
		WHERE sid = ? AND attempt <> ? AND subject = ? AND group_jid IS NULL LIMIT 1`
	var open int
	err := s.container.db.QueryRowContext(ctx, s.container.rebind(read), s.sid, except, subject).Scan(&open)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("store: read the open group creations of %s: %w", s.sid, err)
	}
	return true, nil
}

// SweepGroupCreations drops the attempts that settled before the cutoff, and reports how
// many. Without a sweep the row count is the number of groups the deployment has ever made.
//
// Two things it deliberately does not do. It does not go by when an attempt began, because
// what the row covers is a redelivery, and a delivery can sit pending for as long as nobody
// acknowledges it: an attempt begun long ago whose command is still in flight needs its row
// exactly as much as a fresh one. And it never drops an attempt that has not settled, which
// is the only row here that cannot be reconstructed -- something asked WhatsApp for a group
// and did not get as far as writing down what it made, and the intent is the only thing that
// says where to look. One row per crash in that window is a price worth paying for it.
//
// What is left, stated rather than hidden: a redelivery arriving after both the ledger and
// this record have forgotten the command makes a second group, and no finite retention
// removes that. What bounds it in practice is the client's own `MAXLEN` trim on the command
// stream, which is a length and not a time and so is not this table's to measure.
func (c *Container) SweepGroupCreations(ctx context.Context, before time.Time) (int64, error) {
	// A settled row is not only an answer, it is also what rules its group out of another
	// attempt's search. Dropping it while an attempt at the same name is still open would
	// hand that attempt this group -- the retry would find it, no longer see it ruled out,
	// and report success with a conversation another request made. So a claim outlives the
	// cutoff for as long as anything could still match it.
	const drop = `
		DELETE FROM wac_group_create
		WHERE settled_at IS NOT NULL AND settled_at < ?
		  AND NOT EXISTS (
			SELECT 1 FROM wac_group_create open_attempt
			WHERE open_attempt.sid = wac_group_create.sid
			  AND open_attempt.subject = wac_group_create.subject
			  AND open_attempt.group_jid IS NULL)`
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
