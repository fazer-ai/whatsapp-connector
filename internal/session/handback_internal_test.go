package session

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// A shutdown owes every lease this instance holds, and not only the ones of sessions still
// running: a session stopped earlier whose hand-back did not reach Redis is queued, and
// StopAll retries it. What it reports has to count that one too, or a stop that leaves it
// to expire says it had nothing to hand back (#360).
func TestStopAllCountsTheQueuedHandBacksItRetries(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		redisAtStop    bool
		wantUnreturned int
	}{
		{"redis back by the stop", true, 0},
		{"redis still away at the stop", false, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := miniredis.RunT(t)
			hop := stallable(t, server.Addr())
			rdb := redis.NewClient(&redis.Options{Addr: hop.addr(), ContextTimeoutEnabled: true})
			t.Cleanup(func() { _ = rdb.Close() })
			client := redisx.Wrap(rdb, "wa:", 8)
			manager := NewManager(&ManagerConfig{
				Instance: "inst-a", Engine: fake.New(),
				Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
				Publisher: quietPublisher{}, Replier: quietReplier{},
				NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
			})

			const queued, running = "9c2b7d1e-0000-4000-8000-000000036a01", "9c2b7d1e-0000-4000-8000-000000036a02"
			ctx := context.Background()
			for _, sid := range []string{queued, running} {
				if _, err := manager.Adopt(ctx, sid); err != nil {
					t.Fatalf("Adopt %s: %v", sid, err)
				}
			}

			// The first one stopped earlier, with a hand-back that did not reach Redis.
			hop.stall()
			handing, cancelHanding := context.WithTimeout(ctx, 300*time.Millisecond)
			manager.Release(handing, queued)
			cancelHanding()
			if !manager.handingBack(queued) {
				t.Fatal("the failed hand-back left nothing queued, so there is no case to test")
			}
			if tc.redisAtStop {
				hop.resume()
			}

			stopping, cancelStop := context.WithTimeout(ctx, 600*time.Millisecond)
			defer cancelStop()
			got := manager.StopAll(stopping)
			hop.resume()

			want := HandBack{Owed: 2, Unreturned: tc.wantUnreturned}
			if got != want {
				t.Fatalf("StopAll = %+v, want %+v", got, want)
			}
			if tc.redisAtStop {
				for _, sid := range []string{queued, running} {
					if server.Exists("wa:lease:" + sid) {
						t.Errorf("StopAll counted %s as handed back and its lease is still there", sid)
					}
				}
			}
		})
	}
}
