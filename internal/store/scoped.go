package store

import (
	"context"
	"time"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// Scoped is this container as one session sees it: its own id, and the fence that says
// whether this instance still owns it.
//
// It exists because the fence kept being forgotten. Three separate ways of getting a write
// past it were found in review, and every one was a call site that did not know it had to
// ask: the save that ends a pairing, the device a rebuild adopts, the mapping the pairing
// callback writes. A fence a caller has to remember is a fence with as many holes as it has
// callers, so the session-scoped writes moved behind one object that has the fence in it
// and cannot be built without one.
//
// Reads are not fenced, here as everywhere: a session that lost its lease looking at its own
// rows stops nothing, and refusing them would only turn a handover into a burst of errors on
// the way out. Which call is a read is a judgement and not a naming rule, and Device is the
// one that caught it out: it clears a mapping that points at a device nothing holds, and that
// clearing is fenced even though nothing in the name says the call writes.
type Scoped struct {
	container *Container
	sid       string
	fence     *Fence
}

// For returns the container as the session named here sees it, with a fence of its own.
//
// One fence per session and not per device: a session outlives its devices -- a logout or a
// mapping that went stale sends it back for another -- and what Close drops has to be the
// one every device it ever built stood behind.
//
// Every call makes a fence, so a session takes one handle and keeps it. Two handles for one
// session are two fences, and dropping one leaves the other up. That is also why this is not
// memoised: a session reopened after losing its lease must not inherit the fence that was
// dropped under it.
func (c *Container) For(sid string) *Scoped {
	return &Scoped{container: c, sid: sid, fence: &Fence{}}
}

// SID is the session this handle belongs to.
func (s *Scoped) SID() string { return s.sid }

// Drop fences every write from here on, including the ones the device makes.
func (s *Scoped) Drop() { s.fence.Drop() }

// Dropped reports whether the fence is down.
func (s *Scoped) Dropped() bool { return s.fence.Dropped() }

// Device returns the device this session should connect with, standing behind this session's
// fence.
//
// Mostly a read, and refused outright when the fence is down and the mapping turns out to
// point at a device that is not there: clearing that mapping is a write, and it is the new
// owner's mapping by the time a handed-on session gets here.
func (s *Scoped) Device(ctx context.Context) (*store.Device, error) {
	return s.container.device(ctx, s.sid, s.fence)
}

// Bind records which device this session paired.
func (s *Scoped) Bind(ctx context.Context, jid types.JID) error {
	if err := s.fence.held(); err != nil {
		return err
	}
	return s.container.bind(ctx, s.sid, jid)
}

// Forget deletes this session's device and the mapping to it.
func (s *Scoped) Forget(ctx context.Context) error {
	if err := s.fence.held(); err != nil {
		return err
	}
	return s.container.forget(ctx, s.sid)
}

// JID is the device this session paired, if it has paired.
func (s *Scoped) JID(ctx context.Context) (types.JID, bool, error) {
	return s.container.jid(ctx, s.sid)
}

// PutMediaPart keeps the coordinates a message's file can be fetched again from.
//
// The session on the row is this handle's, whatever the caller put there. A part carries a
// session of its own because that is the shape of the table, and a caller free to fill it in
// is a caller free to file one session's file under another's.
func (s *Scoped) PutMediaPart(ctx context.Context, part *MediaPart, now time.Time) error {
	if err := s.fence.held(); err != nil {
		return err
	}
	kept := *part
	kept.SID = s.sid
	return s.container.putMediaPart(ctx, &kept, now)
}

// MediaPart reads back what PutMediaPart kept for one message.
func (s *Scoped) MediaPart(ctx context.Context, messageID string) (MediaPart, bool, error) {
	return s.container.mediaPart(ctx, s.sid, messageID)
}

// PutPlaceholder holds a bubble this session has scheduled and not yet decided, so a
// process that ends inside the window does not take the decision with it.
//
// Fenced: the row is read by whichever instance picks the session up next, and a write
// from an instance that has lost the session would hand its successor a bubble for a
// message that has already been answered somewhere else. A placeholder published for a
// message that did arrive is permanent, because the client deduplicates on the id.
func (s *Scoped) PutPlaceholder(ctx context.Context, held *Placeholder) error {
	if err := s.fence.held(); err != nil {
		return err
	}
	waiting := *held
	waiting.SID = s.sid
	return s.container.putPlaceholder(ctx, &waiting)
}

// DropPlaceholder forgets a bubble that has been decided, whichever way it went.
//
// Fenced like the write. Deleting somebody else's row is how a bubble goes missing on
// the very handoff this exists to survive.
func (s *Scoped) DropPlaceholder(ctx context.Context, messageID string) error {
	if err := s.fence.held(); err != nil {
		return err
	}
	return s.container.dropPlaceholder(ctx, s.sid, messageID)
}

// Placeholders lists the bubbles this session left undecided, oldest deadline first.
func (s *Scoped) Placeholders(ctx context.Context) ([]Placeholder, error) {
	return s.container.placeholders(ctx, s.sid)
}
