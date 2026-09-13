package media

import (
	"testing"
	"time"
)

// Handing a blob out and evicting one are two operations on the same file, and the lock
// between them is what keeps the second from happening in the middle of the first.
//
// The sweep reads a blob's time during its walk and reads it again, under the write lock,
// immediately before removing: a blob collected since the walk is one somebody is using,
// and is kept. A hand-out that does not take the read lock can land its own renewal
// between those two reads -- after the recheck and before the unlink -- and the blob is
// removed having just been renewed, by a caller that has already been told it is good for
// another whole TTL. What the caller published is then an address that answers 404, which
// is the pair this lock exists to prevent.
//
// Asserted by holding the eviction's own lock and watching the hand-out wait for it,
// which is the only place the two can be separated on purpose. Internal because the lock
// is, and there is nothing on the package's surface that distinguishes a hand-out that
// waits from one that does not.
func TestHandingABlobOutWaitsForAnEvictionRatherThanLandingInsideIt(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		hand func(s *Store, id string) error
	}{
		{"Touch", func(s *Store, id string) error {
			_, _, err := s.Touch(id)
			return err
		}},
		{"Open", func(s *Store, id string) error {
			body, _, err := s.Open(id)
			if err == nil {
				_ = body.Close()
			}
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store, err := New(Options{Root: t.TempDir()})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = store.Close() })
			stored, err := store.Receive(t.Context(), &Blob{}, func(file File) error {
				_, err := file.Write([]byte("os bytes do arquivo"))
				return err
			})
			if err != nil {
				t.Fatalf("Receive: %v", err)
			}

			// What the sweep holds while it decides whether this blob goes.
			store.collecting.Lock()
			answered := make(chan error, 1)
			go func() { answered <- tc.hand(store, stored.ID) }()

			// A window rather than an instant, because "it did not answer" is the whole
			// assertion and there is nothing to wait on: the call under test is the one
			// that must not make progress. Short, because a failure here is immediate --
			// without the lock the call returns in microseconds.
			select {
			case err := <-answered:
				store.collecting.Unlock()
				t.Fatalf("the blob was handed out (%v) while an eviction held the store", err)
			case <-time.After(50 * time.Millisecond):
			}

			store.collecting.Unlock()
			select {
			case err := <-answered:
				if err != nil {
					t.Fatalf("the blob was not handed out after the eviction let go: %v", err)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("the blob was never handed out after the eviction let go")
			}
		})
	}
}
