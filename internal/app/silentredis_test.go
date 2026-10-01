package app_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/silentnet"
)

// A whole connector whose Redis goes silent takes its sessions down when their leases
// run out, not when Redis comes back (#353). Every step of the heartbeat waits on Redis,
// so nothing on it can do this; before the fix the socket stayed open, the store refused
// every write from the end of the lease, and the messages that arrived meanwhile were
// counted delivered by WhatsApp and published by nobody.
//
// The bound is the key's own expiry in Redis, which is when a peer could take the
// account: the socket has to be down before then.
func TestASilentRedisTakesTheSessionDownBeforeItsKeyCouldBeTaken(t *testing.T) {
	server := miniredis.RunT(t)
	relay := silentnet.New(t, server.Addr())
	const ttl = 7 * time.Second
	connector := start(t, relay.Addr(), "inst-a", map[string]string{
		"WAC_LEASE_TTL": ttl.String(), "WAC_CLAIM_MIN_IDLE": (ttl + ttl/2).String(),
	})
	// Registered after start, so it runs before the shutdown start registered: a
	// connector stopping against a silent Redis waits out every hand-back.
	t.Cleanup(relay.Restore)
	c := newClient(t, server.Addr())

	const sid = "2f1c6f0e-0000-4000-8000-000000000353"
	c.send(context.Background(), c.key.Control(), &protocol.Command{
		V: protocol.Version, ID: "wake-353", Type: protocol.CommandSessionWake, SID: sid,
		TS: time.Now().UnixMilli(), Payload: json.RawMessage(`{"desired":"connected"}`),
	})
	waitFor(t, "the session to be adopted", func() bool { return connector.Sessions() == 1 })

	relay.Mute()
	muted := time.Now()
	// The last renewal that landed was before the mute, so the key outlives it by at least
	// the TTL less one heartbeat; the margin is what the lease gives up ahead of that.
	before := muted.Add(ttl - cluster.DefaultRenewMargin/2)
	for connector.Sessions() != 0 {
		if time.Now().After(before) {
			t.Fatalf("the session was still running %s after Redis went silent, past where its key could be taken", time.Since(muted).Round(time.Millisecond))
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("down %s after Redis went silent", time.Since(muted).Round(time.Millisecond))
}
