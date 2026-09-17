package redisstream

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// What a claim knows about an entry, which is the half only XPENDING can report.
func TestAClaimSaysWhoHeldTheEntryAndHowOftenItWentOut(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	ctx := context.Background()

	taking, err := New(client, Options{Instance: "inst-a", Block: 20 * time.Millisecond, ClaimMinIdle: time.Millisecond, ReadCount: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dead, err := New(client, Options{Instance: "inst-dead", Block: 20 * time.Millisecond, ClaimMinIdle: time.Millisecond, ReadCount: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	stream := client.Keys().Commands("s1")
	if _, err := dead.Read(ctx, []string{"s1"}); err != nil {
		t.Fatalf("priming Read: %v", err)
	}
	writeOne(t, client, stream, "s1")
	if took, err := dead.Read(ctx, []string{"s1"}); err != nil || len(took) != 1 {
		t.Fatalf("the peer read %d commands (err=%v), want 1", len(took), err)
	}
	time.Sleep(5 * time.Millisecond)

	claimed, err := taking.claim(ctx, []string{stream}, time.Millisecond)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("the claim took %d commands (err=%v), want 1", len(claimed), err)
	}
	if got := claimed[0].TakenFrom; got != "inst-dead" {
		t.Errorf("the claim says it took the entry from %q, want inst-dead: without the name, "+
			"a fleet that is busy and one instance that stopped answering read the same", got)
	}
	if !claimed[0].DeliveredBefore {
		t.Error("an entry a claim took back says it is arriving new")
	}
	// Twice: once to the peer that died with it, once to this claim. XPENDING answers
	// before the XCLAIM that follows it, and XCLAIM is what makes it two.
	if got := claimed[0].Deliveries; got != 2 {
		t.Errorf("the claim says the entry had been delivered %d times, want 2: the count "+
			"promises to include this delivery, and XPENDING answers before the claim that makes it", got)
	}
}
