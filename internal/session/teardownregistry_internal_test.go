package session

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// The list of accounts adopted to serve a teardown empties, in every ending it has.
//
// It is the one piece of this hand-back that no outcome shows. An entry left behind after
// the account is gone changes nothing a client or an operator can see: the sweep asks
// about a session this instance no longer runs, is told no, and asks again on the next
// tick, forever, with the map growing by one account per teardown the fleet ever refuses.
// Every test around this one would stay green.
var errEngineRefusedTeardown = errors.New("fake: this account will not be torn down")

func TestTheListOfAccountsAdoptedForATeardownEmpties(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	engine := fake.New()
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: engine,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.New(io.Discard),
	})
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })
	answering, stopAnswering := context.WithCancel(ctx)
	stopped := manager.Answer(answering)
	t.Cleanup(func() { stopAnswering(); <-stopped })

	registered := func() int {
		manager.forDeleteMu.Lock()
		defer manager.forDeleteMu.Unlock()
		return len(manager.forDelete)
	}

	for _, tc := range []struct {
		name     string
		deadline time.Time
		before   func(sid string)
	}{
		{name: "the teardown was refused for arriving late", deadline: time.Now().Add(-time.Minute)},
		{name: "the teardown worked", deadline: time.Time{}},
		{name: "the engine refused the teardown", deadline: time.Time{}, before: func(sid string) {
			opened, err := engine.Open(ctx, sid)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			engine.Session(sid)
			opened.(*fake.Session).FailDelete(errEngineRefusedTeardown)
		}},
	} {
		sid := "sess-registry-" + tc.name[:6]
		if tc.before != nil {
			tc.before(sid)
		}
		command := protocol.Command{V: protocol.Version, Type: protocol.CommandSessionDelete, SID: sid, ID: "c-" + sid}
		if !tc.deadline.IsZero() {
			command.Deadline = tc.deadline.UnixMilli()
		}
		done := make(chan struct{})
		manager.Dispatch(&transport.Delivery{
			Command: command,
			Ack:     func(context.Context) error { close(done); return nil },
			Release: func() {}, Forfeit: func() {},
		})
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: the teardown was never acknowledged", tc.name)
		}
		manager.RenewAll(ctx, time.Now().Add(time.Minute))
		manager.SweepRetired(ctx, time.Now().Add(time.Minute))

		// The engine-refused account stays adopted on purpose, and its registration must
		// still be gone: what keeps an account is not a list entry, it is the session.
		if got := registered(); got != 0 {
			t.Fatalf("%s: %d account(s) still listed as adopted for a teardown; the sweep asks about them on every tick forever", tc.name, got)
		}
	}
}
