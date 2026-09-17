package session_test

import (
	"context"
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
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/session"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// teardownHarness is the shared one plus the server behind it. What #249 is about is
// partly what the connector left in Redis, a lease and an epoch counter, and the shared
// harness hands out neither.
type teardownHarness struct {
	server   *miniredis.Miniredis
	keys     redisx.Keys
	clock    *steppingClock
	leases   *cluster.Leases
	engine   *fake.Engine
	recorder *recorder
	manager  *session.Manager
}

func newTeardownHarness(t *testing.T) teardownHarness {
	t.Helper()
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	client := redisx.Wrap(rdb, "wa:", 8)
	clock := &steppingClock{now: time.Date(2026, 9, 17, 21, 0, 0, 0, time.UTC)}
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{Clock: clock})
	fakeEngine := fake.New()
	rec := newRecorder()
	var ids atomic.Int64
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fakeEngine, Leases: leases, Publisher: rec, Replier: rec,
		Ledger:     &ledger{inner: redisx.NewIdempotency(client, 0)},
		Quarantine: cluster.NewQuarantine(client, nil),
		NewID:      func() string { return "evt-" + strconv.FormatInt(ids.Add(1), 10) },
		Logger:     zerolog.New(io.Discard),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })
	answering(t, manager)
	return teardownHarness{
		server: server, keys: client.Keys(), clock: clock, leases: leases,
		engine: fakeEngine, recorder: rec, manager: manager,
	}
}

// heartbeat is the tick a running instance takes, and it is where an account the
// connector decided to give back actually goes back: the goroutine that answers commands
// may not stop a session from inside the command it is answering.
func (h teardownHarness) heartbeat(t *testing.T) {
	t.Helper()
	// Wall clock, not the lease's: the deadline a tick gives its hand-backs is a
	// context deadline, and the stepping clock only drives how fresh a renewal looks.
	by := time.Now().Add(time.Minute)
	h.manager.RenewAll(context.Background(), by)
	h.manager.SweepRetired(context.Background(), by)
}

func (h teardownHarness) epoch(t *testing.T, sid string) string {
	t.Helper()
	value, err := h.server.Get(h.keys.LeaseEpoch(sid))
	if err != nil {
		return ""
	}
	return value
}

func (h teardownHarness) answered(t *testing.T, id string) protocol.Reply {
	t.Helper()
	waitFor(t, "the command to be answered", func() bool {
		_, ok := h.recorder.reply("wa:reply:" + id)
		return ok
	})
	reply, _ := h.recorder.reply("wa:reply:" + id)
	return reply
}

func teardown(sid, id string, deadline time.Time) *transport.Delivery {
	command := protocol.Command{
		V: protocol.Version, Type: protocol.CommandSessionDelete, SID: sid, ID: id,
		ReplyTo: "wa:reply:" + id,
	}
	if !deadline.IsZero() {
		command.Deadline = deadline.UnixMilli()
	}
	return &transport.Delivery{
		Command: command,
		Ack:     func(context.Context) error { return nil },
		Release: func() {},
		Forfeit: func() {},
	}
}

// engineSession widens a fake session to the interface Open answers with, so the two can
// be compared without the test caring which concrete type came back.
func engineSession(s *fake.Session) engine.Session { return s }

func refusalCode(reply protocol.Reply) protocol.ErrorCode {
	if reply.Error == nil {
		return ""
	}
	return reply.Error.Code
}

// TestATeardownRefusedForArrivingLateDoesNotLeaveTheAccountAdopted is #249 itself.
//
// A `session.delete` travels the control stream and is not carried out there: the
// connector adopts the account so the account's own executor can tear it down, and that
// executor is where the deadline is checked. When it refuses, the adoption has already
// happened, and nothing gave it back: the connector ends up holding an account, with a
// lease it renews forever, because of a command it turned down. `SweepRetired` does not
// collect it, because the session is not retired, it is idle.
//
// What the account costs is not a socket. `engine.Open` prepares a client and does not
// dial, which the comment on `takeForDelete` already said and the contract's own
// paragraph got wrong. It costs a lease pinned to this instance until the process dies,
// an account in `wac_sessions_running` that nobody asked to be running, and the memory of
// a whatsmeow client plus the device row behind it.
func TestATeardownRefusedForArrivingLateDoesNotLeaveTheAccountAdopted(t *testing.T) {
	t.Parallel()
	h := newTeardownHarness(t)
	const sid = "sess-249-late"

	if h.manager.Count() != 0 {
		t.Fatalf("given: %d sessions running, want 0", h.manager.Count())
	}
	epochBefore := h.epoch(t, sid)

	if pending := h.manager.Dispatch(teardown(sid, "cmd-late", time.Now().Add(-time.Minute))); pending {
		t.Fatal("Dispatch left the delete on the reader rather than taking it onto the answer goroutine")
	}
	reply := h.answered(t, "cmd-late")
	adopted, opened := h.engine.Session(sid)
	h.heartbeat(t)

	if reply.OK || refusalCode(reply) != protocol.ErrorExpired {
		t.Fatalf("the refusal changed: got ok=%v code=%q, want ok=false code=%q", reply.OK, refusalCode(reply), protocol.ErrorExpired)
	}
	if opened && adopted.Deleted() != 0 {
		t.Fatalf("the account was torn down %d time(s) by a command that was refused", adopted.Deleted())
	}
	if h.manager.Count() != 0 {
		t.Fatalf("the connector is running %d account(s) because of a teardown it refused; nothing asked for this one and no sweep collects it", h.manager.Count())
	}
	if _, owned := h.leases.Owned(sid); owned {
		t.Fatal("the lease is still held for an account the connector refused to tear down; it is renewed on every heartbeat and no peer can take it")
	}
	if h.server.Exists(h.keys.Lease(sid)) {
		t.Fatal("the lease key is still in Redis, so the account belongs to this instance until the process dies")
	}
	// Asked of the engine rather than of its registry: the fake hands the same session
	// back while it is open and builds a new one once it has been closed, which is the
	// only place "the client and its device row were let go" shows.
	if reopened, err := h.engine.Open(t.Context(), sid); err != nil {
		t.Fatalf("Open after the refusal: %v", err)
	} else if opened && reopened == engineSession(adopted) {
		t.Fatal("the engine session opened for the refused teardown is still the live one; the client and its device row stay in memory")
	}
	// The counter may have gone up, and must not have gone down or vanished: the
	// adoption really happened, so the epoch really moved, and #247's fence exists
	// because deleting it destroys the fencing token of an account paired again.
	if after := h.epoch(t, sid); after < epochBefore {
		t.Fatalf("the epoch counter went backwards, %q to %q", epochBefore, after)
	}
}

// TestAnAccountSomebodyWantedUpSurvivesALateTeardown is the risk the issue names for
// this design, and the reason the trigger is not "the teardown was refused".
//
// What licenses giving the account back is this adoption having *created* the session.
// An account already running was adopted for some other reason, by somebody who wanted it
// up; a late teardown for it is refused exactly the same way, and dropping it there would
// disconnect a live account on the strength of a command that changed nothing.
func TestAnAccountSomebodyWantedUpSurvivesALateTeardown(t *testing.T) {
	t.Parallel()
	h := newTeardownHarness(t)
	const sid = "sess-249-wanted"

	if _, err := h.manager.Adopt(t.Context(), sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	running, ok := h.engine.Session(sid)
	if !ok {
		t.Fatal("given: the engine has no session for an account just adopted")
	}
	epochBefore := h.epoch(t, sid)

	if pending := h.manager.Dispatch(teardown(sid, "cmd-wanted", time.Now().Add(-time.Minute))); pending {
		t.Fatal("Dispatch left the delete on the reader")
	}
	reply := h.answered(t, "cmd-wanted")
	h.heartbeat(t)

	if reply.OK || refusalCode(reply) != protocol.ErrorExpired {
		t.Fatalf("got ok=%v code=%q, want the same refusal as any late teardown", reply.OK, refusalCode(reply))
	}
	if h.manager.Count() != 1 {
		t.Fatalf("an account somebody wanted up was dropped by a teardown that was refused: %d running, want 1", h.manager.Count())
	}
	if _, owned := h.leases.Owned(sid); !owned {
		t.Fatal("the lease of a live account was handed back because of a command that did nothing")
	}
	if running.Deleted() != 0 {
		t.Fatal("the account was torn down by a refused teardown")
	}
	if after := h.epoch(t, sid); after != epochBefore {
		t.Fatalf("the epoch of a session that never changed hands moved, %q to %q", epochBefore, after)
	}
	// And it is still serving, which is the difference between an entry in a map and an
	// account: a status for it is answered by the session that was there all along.
	status := &transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, Type: protocol.CommandSessionStatus, SID: sid, ID: "cmd-wanted-status",
			ReplyTo: "wa:reply:cmd-wanted-status",
		},
		Ack: func(context.Context) error { return nil },
	}
	h.manager.Dispatch(status)
	if got := h.answered(t, "cmd-wanted-status"); !got.OK {
		t.Fatalf("the account stopped serving commands after a refused teardown: %+v", got.Error)
	}
}

// TestALateTeardownDeliveredAgainDoesNotPileUpAdoptions is the shape the fleet actually
// sees. A command is delivered again when its holder dies or its acknowledgement is lost,
// and a teardown whose deadline has passed is refused every time. Each refusal must be
// retired rather than left pending, and each must leave the instance holding nothing:
// otherwise the fleet spends an adoption per beat on an account nobody asked about, which
// is the loop #241 was about.
func TestALateTeardownDeliveredAgainDoesNotPileUpAdoptions(t *testing.T) {
	t.Parallel()
	h := newTeardownHarness(t)
	const sid = "sess-249-again"

	var acked, released, forfeited atomic.Int64
	for round := range 5 {
		id := "cmd-again-" + strconv.Itoa(round)
		delivery := teardown(sid, id, time.Now().Add(-time.Minute))
		delivery.Command.ID = "cmd-again"
		delivery.Command.ReplyTo = "wa:reply:" + id
		delivery.DeliveredBefore = round > 0
		delivery.Ack = func(context.Context) error { acked.Add(1); return nil }
		delivery.Release = func() { released.Add(1) }
		delivery.Forfeit = func() { forfeited.Add(1) }

		h.manager.Dispatch(delivery)
		if reply := h.answered(t, id); reply.OK || refusalCode(reply) != protocol.ErrorExpired {
			t.Fatalf("delivery %d: got ok=%v code=%q", round, reply.OK, refusalCode(reply))
		}
	}
	// One tick, after all five, and deliberately not one between each: the hand-back waits
	// for a tick, so deliveries two to five land on the session the first one adopted and
	// are handed to its executor directly. A licence that counted commands rather than
	// refused teardowns would read those repeats as an account somebody came to want, and
	// this account would then never go back.
	h.heartbeat(t)

	if acked.Load() != 5 || released.Load() != 0 || forfeited.Load() != 0 {
		t.Fatalf("a late teardown was left pending: acked=%d released=%d forfeited=%d, want 5/0/0; pending it comes back on its own and the loop never ends",
			acked.Load(), released.Load(), forfeited.Load())
	}
	if h.manager.Count() != 0 {
		t.Fatalf("%d account(s) left running after five refused teardowns", h.manager.Count())
	}
	if h.server.Exists(h.keys.Lease(sid)) {
		t.Fatal("the lease is still held after five refused teardowns")
	}
}

// TestATeardownTheEngineRefusedKeepsTheAccount is the other side of the selectivity, and
// the reason it is not "give the account back whenever the teardown did not happen".
//
// A teardown the executor reached and the engine could not carry out is an attempt, not a
// refusal before the fact: the client is told it failed and sends it again, and the
// account being adopted already is what makes that second attempt free. Measured on the
// base: a redelivery finds the session in the map, so `Dispatch` hands it straight to the
// executor without a second adoption and without a second epoch.
func TestATeardownTheEngineRefusedKeepsTheAccount(t *testing.T) {
	t.Parallel()
	h := newTeardownHarness(t)
	const sid = "sess-249-engine"

	if _, err := h.manager.Adopt(t.Context(), sid); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	opened, ok := h.engine.Session(sid)
	if !ok {
		t.Fatal("given: no engine session to arm")
	}
	opened.FailDelete(errCannotOpen)

	h.manager.Dispatch(teardown(sid, "cmd-engine", time.Time{}))
	reply := h.answered(t, "cmd-engine")
	h.heartbeat(t)

	if reply.OK {
		t.Fatal("given: the teardown was supposed to fail in the engine")
	}
	if refusalCode(reply) == protocol.ErrorExpired {
		t.Fatalf("given: wanted a failure from the engine, got the deadline refusal")
	}
	if h.manager.Count() != 1 {
		t.Fatalf("the account was given back after a teardown that was attempted and failed; the client's retry now pays for another adoption and another epoch: %d running, want 1", h.manager.Count())
	}
	if _, owned := h.leases.Owned(sid); !owned {
		t.Fatal("the lease went back after an attempted teardown failed")
	}
}

// TestAnAccountWantedAfterTheRefusalIsNotGivenBack is the clause that makes the hand-back
// safe rather than merely correct on a quiet instance.
//
// The refusal is answered on the session's own executor and the account goes back on the
// next heartbeat, and the fleet does not stop in between: a command for the same account
// can arrive on the reader's goroutine while the refusal is still going out, and once it
// has been carried out the account is one somebody wants up. What licenses the hand-back
// is the teardown being the only thing that ran, and nothing else.
func TestAnAccountWantedAfterTheRefusalIsNotGivenBack(t *testing.T) {
	t.Parallel()
	h := newTeardownHarness(t)
	const sid = "sess-249-wanted-after"

	h.manager.Dispatch(teardown(sid, "cmd-after", time.Now().Add(-time.Minute)))
	if reply := h.answered(t, "cmd-after"); refusalCode(reply) != protocol.ErrorExpired {
		t.Fatalf("given: got %q, want the late refusal", refusalCode(reply))
	}
	if h.manager.Count() != 1 {
		t.Fatal("given: the adoption for the teardown did not happen, so there is nothing to keep")
	}

	// Answered, so the session has carried out something other than the refused teardown
	// by the time the heartbeat looks.
	wanted := &transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, Type: protocol.CommandSessionStatus, SID: sid, ID: "cmd-after-status",
			ReplyTo: "wa:reply:cmd-after-status",
		},
		Ack: func(context.Context) error { return nil },
	}
	h.manager.Dispatch(wanted)
	if reply := h.answered(t, "cmd-after-status"); !reply.OK {
		t.Fatalf("given: the status was not served: %+v", reply.Error)
	}
	h.heartbeat(t)

	if h.manager.Count() != 1 {
		t.Fatal("an account was handed back although a command had been carried out on it since the refused teardown; whoever sent that command is talking to an instance that has let the account go")
	}
	if _, owned := h.leases.Owned(sid); !owned {
		t.Fatal("the lease went back under a session that had served a command since the refusal")
	}
}

// TestAnAccountStillRunningACommandIsNotGivenBack is the same guard one step later, for
// the command that arrived after the heartbeat had already decided.
//
// The pass that lists the accounts to give back and the stop that follows are not one
// step, and a command can be taken off the queue in between: stopping then would answer
// a client that its command failed on an account this instance had just told itself
// nothing wanted. The door is shut and the question asked again, with the answer that
// matters being "is anything running", which is what `claimIdle` is for.
func TestAnAccountStillRunningACommandIsNotGivenBack(t *testing.T) {
	t.Parallel()
	h := newTeardownHarness(t)
	const sid = "sess-249-busy"

	h.manager.Dispatch(teardown(sid, "cmd-busy", time.Now().Add(-time.Minute)))
	if reply := h.answered(t, "cmd-busy"); refusalCode(reply) != protocol.ErrorExpired {
		t.Fatalf("given: got %q, want the late refusal", refusalCode(reply))
	}
	adopted, ok := h.engine.Session(sid)
	if !ok {
		t.Fatal("given: no engine session for the account the teardown was adopted for")
	}

	// Held inside the engine, so the session is in the middle of a command for the whole
	// of the heartbeat rather than for a moment the test has to win a race against.
	release := adopted.Hold()
	busy := &transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, Type: protocol.CommandSessionStatus, SID: sid, ID: "cmd-busy-status",
			ReplyTo: "wa:reply:cmd-busy-status",
		},
		Ack: func(context.Context) error { return nil },
	}
	h.manager.Dispatch(busy)
	waitFor(t, "the held command to reach the engine", func() bool { return len(adopted.Commands()) > 0 })

	h.heartbeat(t)
	stillRunning := h.manager.Count()
	release()

	if stillRunning != 1 {
		t.Fatal("an account was stopped while it was carrying out a command; the client is told its command failed on an account nothing had decided to give up")
	}
	if reply := h.answered(t, "cmd-busy-status"); !reply.OK {
		t.Fatalf("the command running through the hand-back was not answered: %+v", reply.Error)
	}
}

// TestATeardownAdoptedForAndRefusedByTheEngineKeepsTheAccount is where the selectivity is
// actually decided, and the earlier engine test is not: there the account was already
// running, so nothing registered an adoption to undo. Here the teardown is the reason the
// account was opened at all, and it still must stay.
//
// A teardown the engine refused is an attempt. The client is told it failed and sends it
// again, and the account being adopted already is what makes that second attempt cost
// nothing: `Dispatch` finds the session in the map and hands the redelivery straight to
// the executor, with no second lease and no second epoch. Handing the account back on
// that refusal would charge the retry for both, every time.
func TestATeardownAdoptedForAndRefusedByTheEngineKeepsTheAccount(t *testing.T) {
	t.Parallel()
	h := newTeardownHarness(t)
	const sid = "sess-249-adopted-engine"

	// Armed before the adoption: the fake hands the same session to whoever opens the
	// sid next, which is the manager a moment later, so the teardown fails on the very
	// session `takeForDelete` adopts. The manager has no session for the account, so the
	// delete still travels the control path.
	prepared, err := h.engine.Open(t.Context(), sid)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	armed, ok := h.engine.Session(sid)
	if !ok || engineSession(armed) != prepared {
		t.Fatal("given: the fake did not hand back the session just opened")
	}
	armed.FailDelete(errCannotOpen)
	if h.manager.Count() != 0 {
		t.Fatal("given: the manager is already running the account, so the delete would not take the control path")
	}

	h.manager.Dispatch(teardown(sid, "cmd-adopted-engine", time.Time{}))
	reply := h.answered(t, "cmd-adopted-engine")
	h.heartbeat(t)

	if reply.OK {
		t.Fatal("given: the teardown was supposed to fail in the engine")
	}
	if refusalCode(reply) == protocol.ErrorExpired {
		t.Fatalf("given: wanted the engine's failure, got the deadline refusal")
	}
	if armed.Deleted() == 0 {
		t.Fatal("given: the engine was never asked to tear the account down")
	}
	if h.manager.Count() != 1 {
		t.Fatalf("the account was handed back after a teardown that was attempted and failed; the client's retry now pays for another adoption and another epoch: %d running, want 1", h.manager.Count())
	}
	if _, owned := h.leases.Owned(sid); !owned {
		t.Fatal("the lease went back after an attempted teardown failed, so the retry has to win it again")
	}
}

// TestAHandBackThatRanOutOfTickIsTriedAgain keeps the registration honest at the one
// ending that is nobody's fault.
//
// Nothing else will ever look at this session: it is not retired, so no sweep lists it,
// and its lease is renewed for as long as the instance lives. A tick whose window ran out
// before it reached this account has to leave it on the list, or the one thing that would
// have given the account back has forgotten it.
func TestAHandBackThatRanOutOfTickIsTriedAgain(t *testing.T) {
	t.Parallel()
	h := newTeardownHarness(t)
	const sid = "sess-249-retry"

	h.manager.Dispatch(teardown(sid, "cmd-retry", time.Now().Add(-time.Minute)))
	if reply := h.answered(t, "cmd-retry"); refusalCode(reply) != protocol.ErrorExpired {
		t.Fatalf("given: got %q, want the late refusal", refusalCode(reply))
	}
	if h.manager.Count() != 1 {
		t.Fatal("given: the adoption for the teardown did not happen")
	}

	// A tick with no window left: every hand-back it owes is left for the next one.
	spent := time.Now().Add(-time.Second)
	h.manager.RenewAll(context.Background(), spent)
	h.manager.SweepRetired(context.Background(), spent)
	if h.manager.Count() != 1 {
		t.Fatal("given: the account went back on a tick that had no window to do it in")
	}

	h.heartbeat(t)
	if h.manager.Count() != 0 {
		t.Fatalf("the account was never given back: the tick that ran out forgot it, and nothing else lists this session: %d running", h.manager.Count())
	}
	if h.server.Exists(h.keys.Lease(sid)) {
		t.Fatal("the lease is still held")
	}
}

// TestAServedAccountIsNotGivenBackByASecondLateTeardown is the trap the second round of
// review found, and it is the one that would have closed a live socket.
//
// A late teardown adopts the account, a client's command is then carried out on it, and a
// second late teardown arrives. Both teardowns were refused, so a rule that re-armed on
// the running total would fold the client's command into the new baseline and hand the
// account back with the client still talking to it. What the count has to say is not "how
// much has happened" but "has anything happened that was not a refused teardown".
func TestAServedAccountIsNotGivenBackByASecondLateTeardown(t *testing.T) {
	t.Parallel()
	h := newTeardownHarness(t)
	const sid = "sess-249-served"

	h.manager.Dispatch(teardown(sid, "cmd-served-1", time.Now().Add(-time.Minute)))
	if reply := h.answered(t, "cmd-served-1"); refusalCode(reply) != protocol.ErrorExpired {
		t.Fatalf("given: got %q, want the late refusal", refusalCode(reply))
	}

	// A client comes along and uses the account, before any tick has taken it back.
	h.manager.Dispatch(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, Type: protocol.CommandSessionStatus, SID: sid, ID: "cmd-served-status",
			ReplyTo: "wa:reply:cmd-served-status",
		},
		Ack: func(context.Context) error { return nil },
	})
	if reply := h.answered(t, "cmd-served-status"); !reply.OK {
		t.Fatalf("given: the status was not served: %+v", reply.Error)
	}

	// And a second teardown, also late, also refused.
	h.manager.Dispatch(teardown(sid, "cmd-served-2", time.Now().Add(-time.Minute)))
	if reply := h.answered(t, "cmd-served-2"); refusalCode(reply) != protocol.ErrorExpired {
		t.Fatalf("got %q, want the late refusal", refusalCode(reply))
	}
	h.heartbeat(t)

	if h.manager.Count() != 1 {
		t.Fatal("an account a client had used was handed back because two teardowns in a row were refused; the client is talking to an instance that has let it go")
	}
	if _, owned := h.leases.Owned(sid); !owned {
		t.Fatal("the lease of an account a client had used went back")
	}
}

// TestAWakeKeepsAnAccountTheHandBackWasAboutToTake is the same trap reached by the one
// path that leaves no trace in the count.
//
// A `session.wake` for an account already running is answered by the manager itself: it
// adopts, finds the session, acknowledges, and nothing reaches the executor. So the count
// the hand-back watches does not move, and without something saying otherwise the next
// tick would give away an account the fleet has just been told to keep up.
func TestAWakeKeepsAnAccountTheHandBackWasAboutToTake(t *testing.T) {
	t.Parallel()
	h := newTeardownHarness(t)
	const sid = "sess-249-woken"

	h.manager.Dispatch(teardown(sid, "cmd-woken", time.Now().Add(-time.Minute)))
	if reply := h.answered(t, "cmd-woken"); refusalCode(reply) != protocol.ErrorExpired {
		t.Fatalf("given: got %q, want the late refusal", refusalCode(reply))
	}

	var acked atomic.Bool
	h.manager.Dispatch(&transport.Delivery{
		Command: protocol.Command{V: protocol.Version, Type: protocol.CommandSessionWake, SID: sid, ID: "cmd-woken-wake"},
		Ack:     func(context.Context) error { acked.Store(true); return nil },
		Release: func() {}, Forfeit: func() {},
	})
	waitFor(t, "the wake to be answered", acked.Load)
	h.heartbeat(t)

	if h.manager.Count() != 1 {
		t.Fatal("an account a wake had just claimed was handed back; whoever sent the wake was told it is running here, and the connect behind it now has no owner")
	}
	if _, owned := h.leases.Owned(sid); !owned {
		t.Fatal("the lease went back under an account a wake had just taken")
	}
}
