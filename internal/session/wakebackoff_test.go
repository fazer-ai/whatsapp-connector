package session_test

import (
	"context"
	"errors"
	"io"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/session"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// errCannotOpen is the deterministic refusal these tests are about: an account the
// engine will not open, however many times it is asked. A build WhatsApp turns away and
// a device row that cannot be read both look like this from here.
var errCannotOpen = errors.New("engine: this account will not open")

// shutEngine refuses every Open and counts how many were attempted. The count is the
// subject: what the loop in #241 costs is not the entry left pending, it is this number
// climbing once per claim beat against an account that cannot come up.
type shutEngine struct{ opens atomic.Int64 }

func (e *shutEngine) Open(context.Context, string) (engine.Session, error) {
	e.opens.Add(1)
	return nil, errCannotOpen
}

func (e *shutEngine) Close() error { return nil }

// backoffHarness is newHarnessLogging with an engine of the test's choosing, which the
// shared one does not offer because every other test wants a fake that works.
type backoffHarness struct {
	quarantine *cluster.Quarantine
	engine     *shutEngine
	manager    *session.Manager
}

func newBackoffHarness(t *testing.T) backoffHarness {
	t.Helper()
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	client := redisx.Wrap(rdb, "wa:", 8)
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	shut := &shutEngine{}
	rec := newRecorder()
	quarantine := cluster.NewQuarantine(client, nil)
	var ids atomic.Int64
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: shut, Leases: leases, Publisher: rec, Replier: rec,
		Ledger:     &ledger{inner: redisx.NewIdempotency(client, 0)},
		Quarantine: quarantine,
		NewID:      func() string { return "evt-" + strconv.FormatInt(ids.Add(1), 10) },
		Logger:     zerolog.New(io.Discard),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })
	answering(t, manager)
	return backoffHarness{quarantine: quarantine, engine: shut, manager: manager}
}

// TestAnAdoptionThatCannotOpenStrikesTheQuarantine is the first half of #241: the fleet
// already has a backoff for an account that will not come back, and adoption was the one
// door into it that never knocked. A session that fails to resume strikes it, a retired
// one swept strikes it, and one that never opened at all accrued nothing -- so the wake
// behind it was retried at full speed for as long as the state lasted.
func TestAnAdoptionThatCannotOpenStrikesTheQuarantine(t *testing.T) {
	t.Parallel()
	h := newBackoffHarness(t)
	const sid = "sess-shut"

	if _, err := h.manager.Adopt(t.Context(), sid); !errors.Is(err, errCannotOpen) {
		t.Fatalf("Adopt: got %v, want %v", err, errCannotOpen)
	}

	waiting, err := h.quarantine.Waiting(t.Context(), []string{sid})
	if err != nil {
		t.Fatalf("Waiting: %v", err)
	}
	if _, found := waiting[sid]; !found {
		t.Fatalf("an adoption that could not open the account left no strike; the fleet will retry it at full speed")
	}
}

// TestAWakeThatCameRoundAgainLeavesAQuarantinedAccountAlone is the second half. The
// quarantine gates what the connector does on its own and nothing else -- a client that
// asks for a connection gets one, which PROTOCOL.md promises in as many words -- and a
// wake handed out a second time is not a client asking. It is this fleet repeating an
// attempt it already made.
func TestAWakeThatCameRoundAgainLeavesAQuarantinedAccountAlone(t *testing.T) {
	t.Parallel()
	h := newBackoffHarness(t)
	const sid = "sess-shut"

	if _, err := h.quarantine.Strike(t.Context(), sid); err != nil {
		t.Fatalf("Strike: %v", err)
	}
	before := h.engine.opens.Load()

	var forfeited atomic.Bool
	wake := &transport.Delivery{
		Command:         protocol.Command{Type: protocol.CommandSessionWake, SID: sid, ID: "cmd-1"},
		DeliveredBefore: true,
		Forfeit:         func() { forfeited.Store(true) },
	}
	if pending := h.manager.Dispatch(wake); pending {
		t.Fatalf("Dispatch left the wake on the reader rather than taking it onto the answer goroutine")
	}
	waitFor(t, "the wake to be given back", forfeited.Load)

	if opened := h.engine.opens.Load() - before; opened != 0 {
		t.Fatalf("a wake that came round again tried the engine %d times on an account the fleet is leaving alone", opened)
	}
}

// TestAWakeReadForTheFirstTimeAdoptsAQuarantinedAccount is the fence in front of the
// test above. The promise in PROTOCOL.md is that the operator is never held behind the
// backoff: they are the one party who may know why the last attempt failed, and the
// quarantine exists to pace the fleet, not to stand in front of the person fixing it.
func TestAWakeReadForTheFirstTimeAdoptsAQuarantinedAccount(t *testing.T) {
	t.Parallel()
	h := newBackoffHarness(t)
	const sid = "sess-shut"

	if _, err := h.quarantine.Strike(t.Context(), sid); err != nil {
		t.Fatalf("Strike: %v", err)
	}
	before := h.engine.opens.Load()

	wake := &transport.Delivery{
		Command: protocol.Command{Type: protocol.CommandSessionWake, SID: sid, ID: "cmd-1"},
		Forfeit: func() {},
	}
	if pending := h.manager.Dispatch(wake); pending {
		t.Fatalf("Dispatch left the wake on the reader rather than taking it onto the answer goroutine")
	}
	waitFor(t, "the engine to be asked", func() bool { return h.engine.opens.Load() > before })
}
