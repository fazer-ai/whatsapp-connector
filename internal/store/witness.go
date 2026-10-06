package store

import (
	"context"
	"sync"
)

// Writes is a witness to the store writes made under one context: it keeps the first that
// failed, whether the fence refused it or the database did.
//
// It exists for the caller whatsmeow does not tell. `DownloadHistorySync` stores what it
// keeps of a dump -- the message secrets, the privacy tokens, the pairs of number and LID,
// the push names -- and logs a failure to store any of it rather than returning it, so a
// dump whose secrets are gone comes back as a dump that arrived whole (#350). Those writes
// carry the context the download was given, and every one of them passes through the
// fence, so the fence is where the failure can still be seen.
type Writes struct {
	mu  sync.Mutex
	err error
}

type writesKey struct{}

// WatchWrites returns a context whose store writes the returned witness sees. Only the
// writes made under that context or one derived from it: another session's, or this
// session's own made under another context, are not this caller's to answer for.
func WatchWrites(ctx context.Context) (context.Context, *Writes) {
	writes := &Writes{}
	return context.WithValue(ctx, writesKey{}, writes), writes
}

// Err is the first write under the watched context that failed, or nil when none did.
func (w *Writes) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

// witnessed tells the context's witness, if it has one, about a write that failed, and
// returns the error unchanged.
func witnessed(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if writes, ok := ctx.Value(writesKey{}).(*Writes); ok {
		writes.mu.Lock()
		if writes.err == nil {
			writes.err = err
		}
		writes.mu.Unlock()
	}
	return err
}
