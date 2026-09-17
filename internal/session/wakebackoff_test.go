package session_test

import (
	"context"
	"errors"
	"io"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

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
	rdb        *redis.Client
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
	return backoffHarness{quarantine: quarantine, engine: shut, manager: manager, rdb: rdb}
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

	var forfeited, acked atomic.Bool
	wake := &transport.Delivery{
		Command:         protocol.Command{Type: protocol.CommandSessionWake, SID: sid, ID: "cmd-1"},
		DeliveredBefore: true,
		Forfeit:         func() { forfeited.Store(true) },
		Ack:             func(context.Context) error { acked.Store(true); return nil },
	}
	if pending := h.manager.Dispatch(wake); pending {
		t.Fatalf("Dispatch left the wake on the reader rather than taking it onto the answer goroutine")
	}
	waitFor(t, "the wake to be given back", forfeited.Load)

	if opened := h.engine.opens.Load() - before; opened != 0 {
		t.Fatalf("a wake that came round again tried the engine %d times on an account the fleet is leaving alone", opened)
	}
	// Given back, and given back rather than retired. Waiting out a backoff and being
	// retired look the same for a minute and stop looking the same after that: the wake is
	// the only thing that starts a session with no `desired` row, so a retired one is an
	// account left paired, owned by nobody and silent.
	if acked.Load() {
		t.Fatalf("a wake was retired while the account waits out its backoff; nothing is left to start that session")
	}
}

// failHGet makes exactly the read the quarantine does fail, and nothing else: leases are
// SET and scripts, the instance registry is HGETALL, and only `Waiting` sends HGET.
type failHGet struct{ on atomic.Bool }

func (h *failHGet) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *failHGet) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if !h.on.Load() {
			return next(ctx, cmds)
		}
		for _, cmd := range cmds {
			if cmd.Name() == "hget" {
				return errors.New("redis: refusing to answer")
			}
		}
		return next(ctx, cmds)
	}
}

func (h *failHGet) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.on.Load() && cmd.Name() == "hget" {
			return errors.New("redis: refusing to answer")
		}
		return next(ctx, cmd)
	}
}

// TestAWakeIsTriedWhenTheQuarantineCannotBeRead fences which way this fails. Not knowing
// whether an account is being left alone is a reason to try it, not a reason to hold it:
// the backoff paces what the connector does on its own, and a read that did not come back
// is no evidence about the account. Held the other way, one unreachable Redis would stop
// the fleet starting any session that had ever failed once.
func TestAWakeIsTriedWhenTheQuarantineCannotBeRead(t *testing.T) {
	t.Parallel()
	h := newBackoffHarness(t)
	const sid = "sess-shut"

	if _, err := h.quarantine.Strike(t.Context(), sid); err != nil {
		t.Fatalf("Strike: %v", err)
	}
	refusing := &failHGet{}
	refusing.on.Store(true)
	h.rdb.AddHook(refusing)
	before := h.engine.opens.Load()

	wake := &transport.Delivery{
		Command:         protocol.Command{Type: protocol.CommandSessionWake, SID: sid, ID: "cmd-1"},
		DeliveredBefore: true,
		Forfeit:         func() {},
		Ack:             func(context.Context) error { return nil },
	}
	if pending := h.manager.Dispatch(wake); pending {
		t.Fatalf("Dispatch left the wake on the reader rather than taking it onto the answer goroutine")
	}
	waitFor(t, "the engine to be asked", func() bool { return h.engine.opens.Load() > before })
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

// TestATeardownThatArrivedLateIsRefused is the other half of what the contract now says
// about the control stream, and it is the half that costs a client something. A
// `session.delete` travels there but is not carried out there: the connector adopts the
// account so that the account's own executor can tear it down, and that executor checks
// both ceilings like it does for any other command.
//
// Which makes the client obligation real rather than decorative. A teardown that arrives
// after its deadline is answered `expired` and the account stays linked -- the device left
// on somebody's phone with nothing saying so, which is the harm `deadline` and
// `max_runtime_ms` were split apart to keep separate. The contract now tells clients to
// bound a teardown with `max_runtime_ms` alone, and this is the behaviour that sentence
// describes.
func TestATeardownThatArrivedLateIsRefused(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	const sid = "sess-going"

	var acked atomic.Bool
	del := &transport.Delivery{
		Command: protocol.Command{
			Type: protocol.CommandSessionDelete, SID: sid, ID: "cmd-late",
			ReplyTo:  "wa:reply:cmd-late",
			Deadline: time.Now().Add(-time.Minute).UnixMilli(),
		},
		Ack: func(context.Context) error { acked.Store(true); return nil },
	}
	if pending := h.manager.Dispatch(del); pending {
		t.Fatalf("Dispatch left the delete on the reader rather than taking it onto the answer goroutine")
	}
	waitFor(t, "the teardown to be answered", func() bool {
		_, answered := h.recorder.reply("wa:reply:cmd-late")
		return answered
	})

	reply, _ := h.recorder.reply("wa:reply:cmd-late")
	if reply.OK {
		t.Fatalf("a teardown past its deadline was carried out; the contract tells clients it is refused")
	}
	if reply.Error == nil || reply.Error.Code != protocol.ErrorExpired {
		t.Fatalf("got %+v, want error code %q", reply.Error, protocol.ErrorExpired)
	}
}

// TestALatePingIsRefused is the third answer the control stream gives to a deadline, and
// the reason the three differ is what refusing costs.
//
// A wake refused is an account nobody starts. A teardown refused before adoption is one
// nothing tore down. A ping refused is nothing at all: it asks what this instance is running
// now, so an answer produced after the caller stopped waiting is a true sentence about the
// wrong instant, written to a reply list that has very likely expired. The count is what
// makes it worse than useless -- `ok:true` with a number from a moment nobody asked about is
// the shape of an answer, so an operator reading it has no way to tell it is stale.
func TestALatePingIsRefused(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	var acked atomic.Bool
	ping := &transport.Delivery{
		Command: protocol.Command{
			Type: protocol.CommandAdminPing, ID: "cmd-late-ping",
			ReplyTo:  "wa:reply:cmd-late-ping",
			Deadline: time.Now().Add(-time.Minute).UnixMilli(),
		},
		Ack: func(context.Context) error { acked.Store(true); return nil },
	}
	if pending := h.manager.Dispatch(ping); pending {
		t.Fatalf("Dispatch left the ping on the reader rather than taking it onto the answer goroutine")
	}
	waitFor(t, "the ping to be answered", func() bool {
		_, answered := h.recorder.reply("wa:reply:cmd-late-ping")
		return answered
	})

	reply, _ := h.recorder.reply("wa:reply:cmd-late-ping")
	if reply.OK {
		t.Fatalf("a ping past its deadline was answered ok:true with a count from another instant")
	}
	if reply.Error == nil || reply.Error.Code != protocol.ErrorExpired {
		t.Fatalf("got %+v, want error code %q", reply.Error, protocol.ErrorExpired)
	}
	// Retired, not left pending. Nothing is waiting for it and nothing starts it again:
	// unlike a wake, a ping nobody answers costs the fleet nothing to lose.
	waitFor(t, "the ping to be retired", acked.Load)
}

// TestAPingInTimeStillAnswers is the control next to the test above: the refusal has to turn
// on the deadline having passed and on nothing else.
func TestAPingInTimeStillAnswers(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	ping := &transport.Delivery{
		Command: protocol.Command{
			Type: protocol.CommandAdminPing, ID: "cmd-live-ping",
			ReplyTo:  "wa:reply:cmd-live-ping",
			Deadline: time.Now().Add(time.Minute).UnixMilli(),
		},
		Ack: func(context.Context) error { return nil },
	}
	if pending := h.manager.Dispatch(ping); pending {
		t.Fatalf("Dispatch left the ping on the reader rather than taking it onto the answer goroutine")
	}
	waitFor(t, "the ping to be answered", func() bool {
		_, answered := h.recorder.reply("wa:reply:cmd-live-ping")
		return answered
	})

	if reply, _ := h.recorder.reply("wa:reply:cmd-live-ping"); !reply.OK {
		t.Fatalf("a ping inside its deadline was refused: %+v", reply.Error)
	}
}

// stallQuarantine holds every write the quarantine makes, for as long as it is told to.
// `HIncrBy` is the first of them and the one Strike blocks on before anything else runs.
type stallQuarantine struct {
	for_ time.Duration
	on   atomic.Bool
}

func (h *stallQuarantine) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *stallQuarantine) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *stallQuarantine) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.on.Load() && cmd.Name() == "hincrby" {
			select {
			case <-time.After(h.for_):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return next(ctx, cmd)
	}
}

// TestAStalledStrikeStillLetsThePeerHaveTheAccount is the two-instance regression for the
// window this round put in front of a hand-back.
//
// An adoption that cannot open the account does two things on the way out: it records the
// failure, so the fleet's backoff starts, and it gives the lease back, so a peer whose
// build may not be broken can try. Both run inside one two-second budget, and the strike
// goes first. `ackTimeout` is also two seconds, so before `failing` learned to take a share
// a Redis that stopped answering would let the strike spend the whole thing and hand the
// release a context that had already expired -- leaving a lease held for a session that
// never opened, with every peer blocked on it until a later tick.
//
// Measured from the peer's side, because that is where it hurts: what matters is not that
// `abandon` was called, it is that instance B can take the account.
func TestAStalledStrikeStillLetsThePeerHaveTheAccount(t *testing.T) {
	t.Parallel()
	h := newBackoffHarness(t)
	const sid = "sess-shut"

	stall := &stallQuarantine{for_: 90 * time.Second}
	stall.on.Store(true)
	h.rdb.AddHook(stall)

	if _, err := h.manager.Adopt(t.Context(), sid); !errors.Is(err, errCannotOpen) {
		t.Fatalf("Adopt: got %v, want %v", err, errCannotOpen)
	}

	peer := cluster.NewLeases(redisx.Wrap(h.rdb, "wa:", 8), "inst-b", cluster.Options{})
	if _, err := peer.Acquire(t.Context(), sid); err != nil {
		t.Fatalf("the peer could not take an account the first instance never opened: %v", err)
	}
}
