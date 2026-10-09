package whatsmeow

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/libsignal/keys/identity"
	"go.mau.fi/libsignal/protocol"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/fazer-ai/whatsapp-connector/internal/testwait"
)

// stallingBuffer is a device store whose first read for a decryption does not come back
// until the context it was given ends, or the test lets it go: a database that stopped
// answering in the middle of an offer.
type stallingBuffer struct {
	store.EventBuffer
	entered  chan struct{}
	released chan struct{}
	ended    chan error
}

func (b *stallingBuffer) GetBufferedEvent(ctx context.Context, _ [32]byte) (*store.BufferedEvent, error) {
	close(b.entered)
	select {
	case <-ctx.Done():
		b.ended <- ctx.Err()
		return nil, ctx.Err()
	case <-b.released:
		b.ended <- nil
		return nil, nil
	}
}

// encryptedOffer is an audio offer whose call key arrives the way WhatsApp sends it, as a
// Signal message in an <enc>: one that parses, so decrypting it reaches the store.
func encryptedOffer(t *testing.T, callID string) *waEvents.CallOffer {
	t.Helper()
	ratchet, err := ecc.GenerateKeyPair()
	if err != nil {
		t.Fatalf("ratchet key: %v", err)
	}
	ours, err := ecc.GenerateKeyPair()
	if err != nil {
		t.Fatalf("identity key: %v", err)
	}
	theirs, err := ecc.GenerateKeyPair()
	if err != nil {
		t.Fatalf("identity key: %v", err)
	}
	message, err := protocol.NewSignalMessage(3, 0, 0, make([]byte, 32), ratchet.PublicKey(), []byte{1, 2, 3},
		identity.NewKey(theirs.PublicKey()), identity.NewKey(ours.PublicKey()), store.SignalProtobufSerializer.SignalMessage)
	if err != nil {
		t.Fatalf("signal message: %v", err)
	}
	enc := waBinary.Node{Tag: "enc", Attrs: waBinary.Attrs{"type": "msg", "v": "2"}, Content: message.Serialize()}
	return &waEvents.CallOffer{
		BasicCallMeta: callMeta(callID),
		Data:          &waBinary.Node{Tag: "offer", Content: []waBinary.Node{{Tag: "audio"}, enc}},
	}
}

// Decrypting an offer reads the store from inside whatsmeow's event dispatch, and a
// session cannot finish closing while that dispatch is still running. So the read is on
// the session's lifetime: closing the session ends a read that stalled, instead of the
// close waiting for a database that may never answer.
func TestClosingTheSessionEndsAnOfferStuckInTheStore(t *testing.T) {
	t.Parallel()
	session := newCallSession(t)
	// The test session's first client carries no meowcaller; a rebuild gives it one.
	if err := session.rebuild(t.Context()); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	client := session.current()
	stalled := &stallingBuffer{
		EventBuffer: client.Store.EventBuffer,
		entered:     make(chan struct{}),
		released:    make(chan struct{}),
		ended:       make(chan error, 1),
	}
	t.Cleanup(func() { close(stalled.released) })
	client.Store.EventBuffer = stalled

	dispatched := make(chan struct{})
	go func() {
		defer close(dispatched)
		//nolint:staticcheck // SA1019: the dispatch a real socket drives is this one
		client.DangerousInternals().DispatchEvent(encryptedOffer(t, "call-1"))
	}()
	select {
	case <-stalled.entered:
	case <-time.After(testwait.Budget):
		t.Fatal("the offer never reached the store")
	}

	session.cancel()

	select {
	case err := <-stalled.ended:
		if err == nil {
			t.Fatal("the stalled read ended because the test let it go, not because the session closed")
		}
	case <-time.After(testwait.Budget):
		t.Fatal("closing the session left the offer's store read running")
	}
	select {
	case <-dispatched:
	case <-time.After(testwait.Budget):
		t.Fatal("the dispatch did not return once the read ended")
	}
}
