package session

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/testwait"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// The list of accounts adopted to serve a teardown empties, in every ending it has.
//
// It is the one piece of this hand-back that no outcome shows. An entry left behind after
// the account is gone changes nothing a client or an operator can see: the sweep asks
// about a session this instance no longer runs, is told no, and asks again on the next
// tick, forever, with the map growing by one account per teardown the fleet ever refuses.
// Every test around this one would stay green.
// settled waits until the bookkeeping for a command has actually run.
//
// An acknowledgement is not that moment. `run` publishes the answer, acknowledges, and
// only then returns, is counted out by `doneWith`, and reports itself: a test that ticks
// the heartbeat on seeing the ack is asking about a registration that may not have been
// written yet, and fails for scheduling rather than for behaviour.
func settled(t *testing.T, manager *Manager, sid string, want func(adoptedForDelete, bool) bool) {
	t.Helper()
	deadline := time.Now().Add(testwait.Budget)
	for time.Now().Before(deadline) {
		manager.forDeleteMu.Lock()
		entry, listed := manager.forDelete[sid]
		manager.forDeleteMu.Unlock()
		if want(entry, listed) {
			return
		}
		time.Sleep(testwait.Poll)
	}
	t.Fatalf("the bookkeeping for %s never reached the state this test is about", sid)
}

func armed(entry adoptedForDelete, listed bool) bool { return listed && entry.refused }
func unlisted(_ adoptedForDelete, listed bool) bool  { return !listed }

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
		// Waited for rather than assumed: the acknowledgement above proves `Ack` ran, not
		// that the executor has finished with the command and said so.
		if tc.deadline.IsZero() {
			settled(t, manager, sid, unlisted)
		} else {
			settled(t, manager, sid, armed)
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

// A hand-back that could not be attempted keeps its place on the list.
//
// Nothing else will ever look at this session: it is not retired, so no sweep lists it,
// and its lease is renewed for as long as the instance lives. The gate used here is the
// one a real tick meets most often, an adoption of the same account under way, and it is
// taken directly because the alternative -- racing a real adoption -- is what makes a test
// like this flaky rather than exact.
func TestAHandBackAnAdoptionWasInTheWayOfKeepsItsPlace(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.New(io.Discard),
	})
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })
	answering, stopAnswering := context.WithCancel(ctx)
	stopped := manager.Answer(answering)
	t.Cleanup(func() { stopAnswering(); <-stopped })

	const sid = "sess-registry-inway"
	acked := make(chan struct{})
	manager.Dispatch(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, Type: protocol.CommandSessionDelete, SID: sid, ID: "c-inway",
			Deadline: time.Now().Add(-time.Minute).UnixMilli(),
		},
		Ack:     func(context.Context) error { close(acked); return nil },
		Release: func() {}, Forfeit: func() {},
	})
	select {
	case <-acked:
	case <-time.After(5 * time.Second):
		t.Fatal("the teardown was never acknowledged")
	}
	settled(t, manager, sid, armed)

	// The gate a tick meets: this account is being adopted, so the hand-back cannot run.
	if !manager.tryHoldHanding(sid) {
		t.Fatal("given: something already holds the turn for this account")
	}
	manager.RenewAll(ctx, time.Now().Add(time.Minute))
	if manager.Count() != 1 {
		t.Fatal("given: the account went back although the turn for it was held")
	}
	manager.forDeleteMu.Lock()
	listed := len(manager.forDelete)
	manager.forDeleteMu.Unlock()
	if listed != 1 {
		t.Fatal("a hand-back that could not be attempted was struck off the list; nothing else lists this session, so the account is owned for the life of the process")
	}

	manager.dropHanding(sid)
	manager.RenewAll(ctx, time.Now().Add(time.Minute))
	if manager.Count() != 0 {
		t.Fatalf("the account was never given back on the tick that could run: %d running", manager.Count())
	}
}

// An account a client came to use leaves the list, rather than being asked about forever.
//
// The claim would refuse it every tick anyway, on the count: nothing is lost by retrying.
// What is lost is bounded growth. An account a client keeps using is one nobody will ever
// take back, so an entry left in place is one more turn taken and given back on every
// heartbeat for the life of the session, and the list only ever grows.
func TestAnAccountAClientCameToUseLeavesTheList(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.New(io.Discard),
	})
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })
	answering, stopAnswering := context.WithCancel(ctx)
	stopped := manager.Answer(answering)
	t.Cleanup(func() { stopAnswering(); <-stopped })

	const sid = "sess-registry-used"
	carry := func(command protocol.Command) {
		t.Helper()
		acked := make(chan struct{})
		manager.Dispatch(&transport.Delivery{
			Command: command,
			Ack:     func(context.Context) error { close(acked); return nil },
			Release: func() {}, Forfeit: func() {},
		})
		select {
		case <-acked:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s was never acknowledged", command.Type)
		}
	}

	carry(protocol.Command{
		V: protocol.Version, Type: protocol.CommandSessionDelete, SID: sid, ID: "c-used-delete",
		Deadline: time.Now().Add(-time.Minute).UnixMilli(),
	})
	carry(protocol.Command{V: protocol.Version, Type: protocol.CommandSessionStatus, SID: sid, ID: "c-used-status"})
	settled(t, manager, sid, unlisted)

	manager.RenewAll(ctx, time.Now().Add(time.Minute))
	if manager.Count() != 1 {
		t.Fatal("given: the account a client had used was handed back")
	}
	manager.forDeleteMu.Lock()
	listed := len(manager.forDelete)
	manager.forDeleteMu.Unlock()
	if listed != 0 {
		t.Fatal("an account nobody will ever take back is still listed; every heartbeat from here takes a turn for it and gives it straight back, and the list only grows")
	}
}

// The count is what the executor did, and it is read at the one instant nothing else can
// speak for: a hand-back has been decided on and is about to shut the session's door.
//
// Both halves are asserted here rather than through a running fleet, because both are
// about a number that no outcome shows. A command carried out has to move it, or an
// account a client is using is handed away; a command refused before it ran must not, or
// an account nobody ever asked for is pinned to this instance for the life of the process.
func TestTheCountFollowsWhatWasCarriedOutAndNotWhatArrived(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		err   error
		moves bool
	}{
		{name: "a command that worked", moves: true},
		{name: "a command the engine refused", err: errEngineRefusedTeardown, moves: true},
		{name: "a command refused for arriving late", err: protocol.NewError(protocol.ErrorExpired, "late")},
		{name: "a command refused for a stale lease", err: protocol.NewError(protocol.ErrorOwnedElsewhere, "moved")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ran(tc.err); got != tc.moves {
				t.Fatalf("ran(%v) = %v, want %v", tc.err, got, tc.moves)
			}
			if got := carriedOut(codeOfErr(tc.err)); tc.err != nil && got != tc.moves {
				t.Fatalf("carriedOut(%q) = %v, want %v; the two sides of this rule disagree",
					codeOfErr(tc.err), got, tc.moves)
			}
		})
	}
}

func codeOfErr(err error) protocol.ErrorCode {
	if err == nil {
		return ""
	}
	return asProtocolError(err).Code
}

// A hand-back decided on a tick ago is dropped when the account stopped being ours to give
// back in between.
//
// The pass that lists the accounts and the stop that follows are a gate apart, and a
// `session.wake` landing in that gap is answered with this very session and acknowledged:
// it takes the entry off the list and leaves nothing in the count to notice, because
// nothing reached the executor. Asked directly, because winning that race from a test is
// what makes a test like this flaky rather than exact.
func TestAHandBackIsDroppedWhenTheAccountStoppedBeingOursToGiveBack(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.New(io.Discard),
	})
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })

	const sid = "sess-registry-gone"
	session, created, err := manager.adopt(ctx, sid, wanted)
	if err != nil || !created {
		t.Fatalf("adopt: %v created=%v", err, created)
	}
	entry := adoptedForDelete{session: session, refused: true, carried: session.carriedSoFar()}

	// Still listed: the account goes back.
	manager.adoptedToDelete(sid, session)
	manager.forDeleteMu.Lock()
	manager.forDelete[sid] = entry
	manager.forDeleteMu.Unlock()
	if !manager.releaseIdle(ctx, sid, entry) {
		t.Fatal("given: a listed hand-back was refused")
	}

	// And now the same decision, with the account no longer listed: a wake took it while
	// the pass was walking.
	session, created, err = manager.adopt(ctx, sid, wanted)
	if err != nil || !created {
		t.Fatalf("adopt again: %v created=%v", err, created)
	}
	entry = adoptedForDelete{session: session, refused: true, carried: session.carriedSoFar()}
	if manager.releaseIdle(ctx, sid, entry) {
		t.Fatal("an account that had been taken off the list was handed back anyway; whoever took it was told it is running here, and the connect behind that has no owner")
	}
	if manager.Count() != 1 {
		t.Fatalf("%d running, want the account still here", manager.Count())
	}
}

// A teardown that has not been answered yet is not a hand-back waiting to happen.
//
// `takeForDelete` lists the account before it offers the command, so there is an instant
// with nothing running, nothing queued and a count of zero: everything `claimIdle` asks
// about says yes, and the account would go back before its teardown was ever offered. The
// teardown would then be left pending, having never been attempted or refused.
func TestAnUnansweredTeardownIsNotHandedBack(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.New(io.Discard),
	})
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })

	const sid = "sess-registry-unanswered"
	session, created, err := manager.adopt(ctx, sid, wanted)
	if err != nil || !created {
		t.Fatalf("adopt: %v created=%v", err, created)
	}
	// Exactly the state `takeForDelete` leaves behind between listing the account and
	// offering the command to it.
	manager.adoptedToDelete(sid, session)

	manager.RenewAll(ctx, time.Now().Add(time.Minute))

	if manager.Count() != 1 {
		t.Fatal("an account was handed back before its teardown had been offered to it; the teardown is left pending having never been attempted")
	}
	manager.forDeleteMu.Lock()
	listed := len(manager.forDelete)
	manager.forDeleteMu.Unlock()
	if listed != 1 {
		t.Fatal("the account stopped being listed although nothing had answered its teardown")
	}
}

// The resume sweep is the second route by which an account becomes wanted without a client
// command reaching it, and it is reached by a race rather than in the ordinary way.
//
// `Resume` refuses outright for an account this instance already runs, so it cannot see
// one adopted for a teardown in the ordinary course. What it can do is ask about an
// account nobody was running, and have `takeForDelete` adopt it on the answer goroutine
// before the synthesised connect gets there: `reconnect` then finds the session already
// present. The record the client wrote says this account should be up, so the undoing is
// called off, and the connect behind it is about to put the socket in the air.
func TestAResumeThatFoundTheAccountAlreadyAdoptedCallsOffTheHandBack(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.New(io.Discard),
	})
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })

	const sid = "sess-registry-resumed"
	session, created, err := manager.adopt(ctx, sid, wanted)
	if err != nil || !created {
		t.Fatalf("adopt: %v created=%v", err, created)
	}
	manager.adoptedToDelete(sid, session)
	manager.forDeleteMu.Lock()
	manager.forDelete[sid] = adoptedForDelete{session: session, refused: true, carried: session.carriedSoFar()}
	manager.forDeleteMu.Unlock()

	manager.reconnect(ctx, &transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, Type: protocol.CommandSessionConnect, SID: sid, ID: "c-resume",
			Payload: []byte(`{"pairing":"resume"}`),
		},
		Ack: func(context.Context) error { return nil }, Release: func() {}, Internal: true,
	})

	manager.forDeleteMu.Lock()
	listed := len(manager.forDelete)
	manager.forDeleteMu.Unlock()
	if listed != 0 {
		t.Fatal("a resume found the account already adopted and the undoing was not called off; the next heartbeat takes the account away from under the connect that was about to put it in the air")
	}
	manager.RenewAll(ctx, time.Now().Add(time.Minute))
	if manager.Count() != 1 {
		t.Fatalf("the account was handed back after a resume asked for it: %d running", manager.Count())
	}
}

// And the count is actually kept: the rule above decides what counts, this is the session
// doing it.
//
// Split from the rule on purpose. A session that stopped counting altogether would pass
// every test of the rule and every test of the predicate that reads it, because a count
// that never moves is exactly what "nothing has happened here" looks like. The two halves
// fail for different reasons and neither covers the other.
func TestASessionCountsTheCommandsItCarriedOut(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.New(io.Discard),
	})
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })

	const sid = "sess-registry-counting"
	session, created, err := manager.adopt(ctx, sid, wanted)
	if err != nil || !created {
		t.Fatalf("adopt: %v created=%v", err, created)
	}

	carry := func(id string, deadline time.Time) {
		t.Helper()
		command := protocol.Command{V: protocol.Version, Type: protocol.CommandSessionStatus, SID: sid, ID: id}
		if !deadline.IsZero() {
			command.Deadline = deadline.UnixMilli()
		}
		acked := make(chan struct{})
		if offer := session.Offer(&transport.Delivery{
			Command: command,
			Ack:     func(context.Context) error { close(acked); return nil },
			Release: func() {},
		}); offer != OfferAccepted {
			t.Fatalf("%s: the session refused the command (%v)", id, offer)
		}
		select {
		case <-acked:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s was never acknowledged", id)
		}
	}

	carry("c-count-1", time.Time{})
	waitForCount(t, session, 1, "a command that was carried out did not move the count; an account a client is using reads as one nothing has happened to, and the heartbeat takes it away")
	carry("c-count-2", time.Now().Add(-time.Minute))
	// Given a moment to be wrong: the assertion is that it stays put, so waiting for the
	// acknowledgement of a later command is what makes it more than a race won by luck.
	carry("c-count-3", time.Time{})
	waitForCount(t, session, 2, "a command refused for arriving late moved the count; an account nobody asked for is pinned to this instance for the life of the process")
}

func waitForCount(t *testing.T, session *Session, want int64, why string) {
	t.Helper()
	deadline := time.Now().Add(testwait.Budget)
	for time.Now().Before(deadline) {
		if session.carriedSoFar() == want {
			return
		}
		time.Sleep(testwait.Poll)
	}
	t.Fatalf("%s (count is %d, want %d)", why, session.carriedSoFar(), want)
}

// The rule that decides what an account adopted for a teardown is worth, asked directly.
//
// Three of these outcomes cannot be produced from outside on an account this path adopted,
// and each says on its own row why not. That is the point of writing it down: a table
// asked directly is the one part of a suite that nothing can contradict, so if today's
// reading of a state is wrong the table is wrong with it and in the same direction. The
// reason is what goes stale first. On the day a new route makes one of these reachable,
// that sentence is false and the row is up for re-reading, instead of the table going on
// agreeing with itself.
func TestWhatEachEndingOfATeardownIsWorth(t *testing.T) {
	t.Parallel()

	const (
		armed = "armed: the account goes back"
		kept  = "kept: the account stays and stops being ours to undo"
		held  = "held: nothing happened, the decision stands"
	)
	for _, tc := range []struct {
		name     string
		code     protocol.ErrorCode
		teardown bool
		happened bool
		want     string
	}{
		{name: "a teardown refused for arriving late", code: protocol.ErrorExpired, teardown: true, want: armed},
		{name: "a teardown answered out of the record", teardown: true, want: armed},
		// Not producible from outside on an account this path adopted: `takeForDelete`
		// takes the lease and offers the command in the same breath, so the renewal would
		// have to age past `ttl - margin` in between.
		{name: "a teardown refused on a stale renewal", code: protocol.ErrorOwnedElsewhere, teardown: true, want: kept},
		// Not producible from outside on an account this path adopted unless the engine
		// is armed before the adoption exists, which is a fake's trick rather than a
		// sequence a fleet produces.
		{name: "a teardown the engine refused", code: protocol.ErrorInternal, teardown: true, happened: true, want: kept},
		{name: "a teardown that worked", teardown: true, happened: true, want: kept},
		{name: "a command that worked", happened: true, want: kept},
		// Not producible from outside on an account this path adopted: a record for a
		// command other than the teardown means a session that ran it, and this account
		// has only ever had the one opened for the teardown.
		{name: "a command answered out of the record", want: kept},
		{name: "a command refused for arriving late", code: protocol.ErrorExpired, want: held},
		{name: "a command refused on a stale renewal", code: protocol.ErrorOwnedElsewhere, want: held},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := miniredis.RunT(t)
			rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { _ = rdb.Close() })
			client := redisx.Wrap(rdb, "wa:", 8)
			manager := NewManager(&ManagerConfig{
				Instance: "inst-a", Engine: fake.New(),
				Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
				Publisher: quietPublisher{}, Replier: quietReplier{},
				NewID: func() string { return "evt" }, Logger: zerolog.New(io.Discard),
			})
			ctx := context.Background()
			t.Cleanup(func() { manager.StopAll(ctx) })

			sid := "sess-ending"
			session, created, err := manager.adopt(ctx, sid, toTearDown)
			if err != nil || !created {
				t.Fatalf("adopt: %v created=%v", err, created)
			}

			manager.afterCommand(sid, session, tc.code, tc.teardown, tc.happened)

			manager.forDeleteMu.Lock()
			entry, listed := manager.forDelete[sid]
			manager.forDeleteMu.Unlock()

			got := kept
			switch {
			case listed && entry.refused:
				got = armed
			case listed:
				got = held
			}
			if got != tc.want {
				t.Fatalf("%s\ngot  %s\nwant %s", tc.name, got, tc.want)
			}
			if tc.want == armed && entry.carried != session.carriedSoFar() {
				t.Fatalf("the hand-back was armed at %d, and the session has carried out %d", entry.carried, session.carriedSoFar())
			}
		})
	}
}

// A report from a session that is no longer the one running the account changes nothing.
//
// A lease that expires has its session taken out of the map before `Stop` has returned,
// and without the adoption gate held, so the answer goroutine can put a replacement in
// place while the old executor is still finishing its last command. That report arrives
// late and is about a session nobody runs. Matched on the account alone it would arm or
// erase the replacement's registration, and the teardown the replacement was adopted for
// would then leave the account owned for good.
//
// Asked directly: winning that race from a test means killing a lease mid-command and
// hoping the schedule cooperates, which is a coin toss dressed as an assertion.
func TestAReportFromASupersededSessionChangesNothing(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.New(io.Discard),
	})
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })

	const sid = "sess-registry-superseded"
	gone, created, err := manager.adopt(ctx, sid, toTearDown)
	if err != nil || !created {
		t.Fatalf("adopt: %v created=%v", err, created)
	}
	// The lease goes, the session goes with it, and a replacement is adopted for a
	// teardown of its own. Through `Release` rather than `stopSession` alone, because the
	// lease has to go back for the next adoption to be able to win it.
	manager.Release(ctx, sid)
	replacement, created, err := manager.adopt(ctx, sid, toTearDown)
	if err != nil || !created {
		t.Fatalf("adopt again: %v created=%v", err, created)
	}
	if gone == replacement {
		t.Fatal("given: the replacement is the same session")
	}

	// The old executor finishes its last command and reports. Both endings, because the
	// two branches of the rule erase and arm respectively, and a stale report must do
	// neither.
	manager.afterCommand(sid, gone, "", false, true)
	manager.forDeleteMu.Lock()
	entry, listed := manager.forDelete[sid]
	manager.forDeleteMu.Unlock()
	if !listed || entry.session != replacement {
		t.Fatal("a report from a superseded session erased the replacement's registration; the teardown it was adopted for now leaves the account owned for good")
	}
	if entry.refused {
		t.Fatal("a report from a superseded session armed the replacement's hand-back")
	}

	manager.afterCommand(sid, gone, protocol.ErrorExpired, true, false)
	manager.forDeleteMu.Lock()
	entry, listed = manager.forDelete[sid]
	manager.forDeleteMu.Unlock()
	if !listed || entry.refused {
		t.Fatal("a superseded session's refusal armed a hand-back for the session that replaced it; the next tick stops an account whose own teardown has not been answered")
	}
}

// A tick that ran out of window leaves the hand-back it owes on the list.
//
// Nothing else will ever look at this session: it is not retired, so no sweep lists it,
// and its lease is renewed for as long as the instance lives. A tick whose window was
// spent before it reached this account has to leave it listed, or the one thing that
// would have given the account back has forgotten it.
//
// Here rather than beside the other endings, because the given is the part that can go
// missing: from outside the package the only moment a test can wait for is the client's
// answer, which goes out before the registration is written, and a spent tick that runs
// with nothing armed yet proves nothing at all. `settled` waits for the registration
// itself, so the tick under test meets the state the test is named for.
func TestATickThatRanOutOfWindowKeepsTheHandBackListed(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	manager := NewManager(&ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: quietReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.New(io.Discard),
	})
	ctx := context.Background()
	t.Cleanup(func() { manager.StopAll(ctx) })
	answering, stopAnswering := context.WithCancel(ctx)
	stopped := manager.Answer(answering)
	t.Cleanup(func() { stopAnswering(); <-stopped })

	const sid = "sess-registry-spent"
	acked := make(chan struct{})
	manager.Dispatch(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, Type: protocol.CommandSessionDelete, SID: sid, ID: "c-spent",
			Deadline: time.Now().Add(-time.Minute).UnixMilli(),
		},
		Ack:     func(context.Context) error { close(acked); return nil },
		Release: func() {}, Forfeit: func() {},
	})
	select {
	case <-acked:
	case <-time.After(5 * time.Second):
		t.Fatal("the teardown was never acknowledged")
	}
	settled(t, manager, sid, armed)

	// A tick with no window left for anything but the renewals themselves.
	spent := time.Now().Add(-time.Second)
	manager.RenewAll(ctx, spent)

	if manager.Count() != 1 {
		t.Fatal("given: the account went back on a tick that had no window to do it in")
	}
	manager.forDeleteMu.Lock()
	entry, listed := manager.forDelete[sid]
	manager.forDeleteMu.Unlock()
	if !listed || !entry.refused {
		t.Fatal("a hand-back the tick had no time for was struck off the list; nothing else lists this session, so the account is owned for the life of the process")
	}

	manager.RenewAll(ctx, time.Now().Add(time.Minute))
	if manager.Count() != 0 {
		t.Fatalf("the account was never given back on the tick that could run: %d running", manager.Count())
	}
}

// No way out of the session map leaves a registration behind, including the ones nothing
// ever armed.
//
// The list of accounts adopted to serve a teardown is a second map under the same sid, and
// the third leak of this round was a way out of the first that the second never heard
// about: a lease lost between the adoption and the executor reaching the command. The tick
// drops the session and stops it, stopping releases what is queued instead of running it,
// so nothing reports how that teardown ended and the entry is never armed. A sweep that
// filtered on the mark first could not reach it again -- one entry per teardown that loses
// that race, each holding a stopped session and the whatsmeow client behind it, for the
// life of the process, with every other test in this package staying green.
//
// Answering that by writing the mark on one more path would be an enumeration, and the
// next path to end a session without passing through it leaks the same way. What the sweep
// asks instead is whether the session is still the one in the map, which no new way out
// can be written around. The ways are still worth counting, because the compiler says
// nothing when one is added: this asks the source how many there are and fails when the
// answer changes, then drives each one it knows about.
func TestNoWayOutOfTheSessionMapLeavesARegistrationBehind(t *testing.T) {
	t.Parallel()

	intoTheMap := map[string]bool{"adopt": true}
	outOfTheMap := map[string]bool{"stopSession": true, "drop": true}

	// The whole package and not `manager.go`, which is where the map happens to live
	// today. `m.sessions` is the package's, so a file added tomorrow could put a session
	// in it or take one out without this fence saying a word -- and a fence that holds
	// only as long as nobody rearranges the files is one that lets go on the day it is
	// for.
	fset := token.NewFileSet()
	var files []*ast.File
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files = append(files, parsed)
	}
	if len(files) < 2 {
		t.Fatalf("the package parsed as %d file(s); this fence is reading the wrong directory and would pass on an empty one", len(files))
	}
	// `delete` is handed the map itself and an assignment is handed a place in it, so the
	// two are matched apart rather than through one predicate that would quietly stop
	// matching either.
	theMap := func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		return ok && selector.Sel.Name == "sessions"
	}
	aPlaceInTheMap := func(node ast.Node) bool {
		index, ok := node.(*ast.IndexExpr)
		return ok && theMap(index.X)
	}
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch stmt := node.(type) {
				case *ast.AssignStmt:
					if len(stmt.Lhs) == 1 && aPlaceInTheMap(stmt.Lhs[0]) && !intoTheMap[fn.Name.Name] {
						t.Errorf("%s puts a session in the map, and this test did not know about it: a session that enters the map somewhere new is one the registration it may carry was not written beside", fn.Name.Name)
					}
				case *ast.CallExpr:
					builtin, ok := stmt.Fun.(*ast.Ident)
					if !ok || builtin.Name != "delete" || len(stmt.Args) == 0 || !theMap(stmt.Args[0]) {
						return true
					}
					if !outOfTheMap[fn.Name.Name] {
						t.Errorf("%s takes a session out of the map, and this test did not know about it: add a case below and prove no registration survives it, or the account it was adopted for is held for the life of the process", fn.Name.Name)
					}
					delete(outOfTheMap, fn.Name.Name)
				}
				return true
			})
		}
	}
	for name := range outOfTheMap {
		t.Errorf("%s no longer takes a session out of the map; this fence is watching a way out that is not there any more", name)
	}

	for _, tc := range []struct {
		name string
		// arm says the teardown was answered and the account is waiting to go back, which
		// is the state the sweep was written for. The other half of the table is the one
		// it was not: adopted, never answered, and already gone.
		arm  bool
		exit func(t *testing.T, manager *Manager, sid string, session *Session)
	}{
		{
			name: "stopped with the account still listed",
			exit: func(_ *testing.T, manager *Manager, sid string, _ *Session) { manager.stopSession(sid) },
		},
		{
			name: "stopped after the teardown was refused",
			arm:  true,
			exit: func(_ *testing.T, manager *Manager, sid string, _ *Session) { manager.stopSession(sid) },
		},
		{
			name: "lease lost before the teardown was answered",
			exit: func(t *testing.T, manager *Manager, sid string, session *Session) {
				dropped, still := manager.drop(sid, session)
				if !still {
					t.Fatal("given: the session in the map was not the one just adopted")
				}
				dropped.Stop()
			},
		},
		{
			name: "lease lost after the teardown was refused",
			arm:  true,
			exit: func(t *testing.T, manager *Manager, sid string, session *Session) {
				dropped, still := manager.drop(sid, session)
				if !still {
					t.Fatal("given: the session in the map was not the one just adopted")
				}
				dropped.Stop()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := miniredis.RunT(t)
			rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { _ = rdb.Close() })
			client := redisx.Wrap(rdb, "wa:", 8)
			manager := NewManager(&ManagerConfig{
				Instance: "inst-a", Engine: fake.New(),
				Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
				Publisher: quietPublisher{}, Replier: quietReplier{},
				NewID: func() string { return "evt" }, Logger: zerolog.New(io.Discard),
			})
			ctx := context.Background()
			t.Cleanup(func() { manager.StopAll(ctx) })

			const sid = "sess-registry-wayout"
			session, created, err := manager.adopt(ctx, sid, toTearDown)
			if err != nil || !created {
				t.Fatalf("adopt: %v created=%v", err, created)
			}
			if tc.arm {
				// The executor's own report, called where the executor calls it: a
				// teardown turned away for arriving late, having run nothing.
				manager.afterCommand(sid, session, protocol.ErrorExpired, true, false)
			}
			manager.forDeleteMu.Lock()
			entry, listed := manager.forDelete[sid]
			manager.forDeleteMu.Unlock()
			if !listed || entry.refused != tc.arm {
				t.Fatalf("given: listed=%v refused=%v, want listed=true refused=%v", listed, entry.refused, tc.arm)
			}

			tc.exit(t, manager, sid, session)
			manager.RenewAll(ctx, time.Now().Add(time.Minute))

			manager.forDeleteMu.Lock()
			_, listed = manager.forDelete[sid]
			manager.forDeleteMu.Unlock()
			if listed {
				t.Fatal("an account whose session left the map is still listed to be handed back; the entry and the stopped session it holds outlive every account they were about, and nothing else ever walks this list")
			}
		})
	}
}

// A peer that reads the wake for an account this instance is about to give back no longer
// retires it for good (#259).
//
// Between the refusal arming an account and the heartbeat releasing it, this instance owns
// the account and has written no mark, so a peer's acquisition is answered with the ordinary
// `not_owner`. The peer still acknowledges the wake -- it cannot tell this owner from one
// that keeps the account -- but leaves it owed to the owner, and the release that the next
// beat makes puts it back on the control stream. Read from there by the peer, it adopts.
//
// It used to be the other way, and this test pinned it: the wake was acknowledged into
// nothing, the account went back owned by nobody, and a `session.connect` that followed was
// left pending on both instances (`acked=0 released=2`).
func TestAWakeAPeerReadsWhileTheAccountIsAboutToGoBackReturnsWithTheRelease(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	ctx := context.Background()

	holder, peer := twoOverOneRedis(t, client)

	const sid = "sess-registry-peerwake"
	acked := make(chan struct{})
	holder.Dispatch(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, Type: protocol.CommandSessionDelete, SID: sid, ID: "c-peerwake",
			Deadline: time.Now().Add(-time.Minute).UnixMilli(),
		},
		Ack:     func(context.Context) error { close(acked); return nil },
		Release: func() {}, Forfeit: func() {},
	})
	select {
	case <-acked:
	case <-time.After(5 * time.Second):
		t.Fatal("given: the teardown was never acknowledged")
	}
	settled(t, holder, sid, armed)

	wakeThroughTheWindow(t, holder, peer, rdb, sid, func() {
		holder.RenewAll(ctx, time.Now().Add(time.Minute))
	})
}

// The hand-back the base already had, the same way: a session the engine retired is let go
// on the next beat, and a wake a peer read in between comes back with the release.
func TestAWakeAPeerReadsWhileARetiredSessionIsAboutToGoBackReturnsWithTheRelease(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	ctx := context.Background()

	holder, peer := twoOverOneRedis(t, client)

	const sid = "sess-retired-peerwake"
	if _, err := holder.Adopt(ctx, sid); err != nil {
		t.Fatalf("given: Adopt: %v", err)
	}
	engine := holder.engine.(*fake.Engine)
	session, ok := engine.Session(sid)
	if !ok {
		t.Fatal("given: no engine session")
	}
	session.EmitLast(protocol.EventSessionLoggedOut, map[string]any{"reason": "logged_out"})
	// The managed session's own reading, which is what the sweep asks: the fake records its
	// last word at once, and the pump that retires the session reads it on its own goroutine.
	retired := func() bool {
		holder.mu.Lock()
		managed, ok := holder.sessions[sid]
		holder.mu.Unlock()
		return ok && managed.Retired()
	}
	deadline := time.Now().Add(testwait.Budget)
	for time.Now().Before(deadline) && !retired() {
		time.Sleep(testwait.Poll)
	}
	if !retired() {
		t.Fatal("given: the session was never retired")
	}

	wakeThroughTheWindow(t, holder, peer, rdb, sid, func() {
		holder.RenewAll(ctx, time.Now().Add(time.Minute))
		holder.SweepRetired(ctx, time.Now().Add(time.Minute))
	})
}

// And an owner that keeps the account puts nothing back: the wake is acknowledged, as it
// always was, and no entry reaches the control stream however many beats go by. This is
// what keeps a peer from bouncing wakes for an account somebody runs, which is #241.
func TestAWakeForAnAccountItsOwnerKeepsIsNotPutBack(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	ctx := context.Background()

	holder, peer := twoOverOneRedis(t, client)
	const sid = "sess-kept-peerwake"
	if _, err := holder.Adopt(ctx, sid); err != nil {
		t.Fatalf("given: Adopt: %v", err)
	}

	for range 3 {
		if got := dispatchWake(t, peer, sid, "c-wake-kept"); got != "acked" {
			t.Fatalf("a wake for an account its owner runs ended %s, want acknowledged", got)
		}
		holder.RenewAll(ctx, time.Now().Add(time.Minute))
		holder.SweepRetired(ctx, time.Now().Add(time.Minute))
	}
	if woken := controlWakes(t, rdb); len(woken) != 0 {
		t.Fatalf("an owner that kept the account put %d wakes back on the control stream", len(woken))
	}
	if holder.Count() != 1 || peer.Count() != 0 {
		t.Fatalf("ownership moved: holder runs %d, peer runs %d", holder.Count(), peer.Count())
	}
}

// twoOverOneRedis is two instances sharing one Redis, each answering its own sessions.
func twoOverOneRedis(t *testing.T, client *redisx.Client) (holder, peer *Manager) {
	t.Helper()
	ctx := context.Background()
	ids := atomic.Int64{}
	instance := func(name string) *Manager {
		manager := NewManager(&ManagerConfig{
			Instance: name, Engine: fake.New(),
			Leases:    cluster.NewLeases(client, name, cluster.Options{}),
			Publisher: quietPublisher{}, Replier: quietReplier{},
			NewID:  func() string { return fmt.Sprintf("%s-%d", name, ids.Add(1)) },
			Logger: zerolog.New(io.Discard),
		})
		t.Cleanup(func() { manager.StopAll(ctx) })
		answering, stopAnswering := context.WithCancel(ctx)
		stopped := manager.Answer(answering)
		t.Cleanup(func() { stopAnswering(); <-stopped })
		return manager
	}
	return instance("inst-a"), instance("inst-b")
}

// dispatchWake hands a wake to manager and says how it ended: acked, released or forfeited.
func dispatchWake(t *testing.T, manager *Manager, sid, id string) string {
	t.Helper()
	ended := make(chan string, 1)
	manager.Dispatch(&transport.Delivery{
		Command: protocol.Command{V: protocol.Version, Type: protocol.CommandSessionWake, SID: sid, ID: id},
		Ack:     func(context.Context) error { ended <- "acked"; return nil },
		Release: func() { ended <- "released" },
		Forfeit: func() { ended <- "forfeited" },
	})
	select {
	case got := <-ended:
		return got
	case <-time.After(testwait.Budget):
		t.Fatalf("the wake %s was never settled", id)
		return ""
	}
}

// controlWakes reads the wakes on the control stream, oldest first.
func controlWakes(t *testing.T, rdb *redis.Client) []protocol.Command {
	t.Helper()
	entries, err := rdb.XRange(context.Background(), "wa:control", "-", "+").Result()
	if err != nil {
		t.Fatalf("read the control stream: %v", err)
	}
	var wakes []protocol.Command
	for _, entry := range entries {
		fields := map[string]string{}
		for name, value := range entry.Values {
			fields[name], _ = value.(string)
		}
		command, err := protocol.ParseCommand(fields)
		if err != nil {
			t.Fatalf("an entry on the control stream does not parse as a command: %v (%v)", err, entry.Values)
		}
		if command.Type == protocol.CommandSessionWake {
			wakes = append(wakes, command)
		}
	}
	return wakes
}

// wakeThroughTheWindow is the two-instance sequence both hand-backs share: the peer reads a
// wake while holder still owns sid and has not marked it, beat lets the account go, and the
// wake that comes back on the control stream makes the peer the owner, under a newer epoch.
func wakeThroughTheWindow(t *testing.T, holder, peer *Manager, rdb *redis.Client, sid string, beat func()) {
	t.Helper()
	before, ok := holder.leases.Owned(sid)
	if !ok {
		t.Fatal("given: the holder does not own the account")
	}

	if got := dispatchWake(t, peer, sid, "c-wake"); got != "acked" {
		t.Fatalf("the peer ended the wake %s; with the account owned it is acknowledged", got)
	}
	if peer.Count() != 0 {
		t.Fatalf("the peer adopted an account the holder owns: %d running", peer.Count())
	}
	if woken := controlWakes(t, rdb); len(woken) != 0 {
		t.Fatalf("a wake went back on the control stream before the account was let go: %v", woken)
	}

	beat()
	if holder.Count() != 0 {
		t.Fatalf("given: the account was not given back: %d running", holder.Count())
	}

	woken := controlWakes(t, rdb)
	if len(woken) != 1 {
		t.Fatalf("the release put %d wakes back on the control stream, want the one the peer acknowledged", len(woken))
	}
	if woken[0].SID != sid || woken[0].ID == "c-wake" {
		t.Fatalf("the wake put back is %+v, want one for %s under an id of its own", woken[0], sid)
	}
	if got := dispatchWake(t, peer, sid, woken[0].ID); got != "acked" {
		t.Fatalf("the wake put back ended %s at the peer, want it to adopt", got)
	}
	after, ok := peer.leases.Owned(sid)
	if !ok || peer.Count() != 1 {
		t.Fatalf("the peer does not own the account after the wake came back: owned=%v running=%d", ok, peer.Count())
	}
	if after.Epoch <= before.Epoch {
		t.Fatalf("the account changed hands under epoch %d, not above the holder's %d", after.Epoch, before.Epoch)
	}
}

// The two answers that leave the wake pending rather than owed (#259): the account was let
// go between the adoption that failed and the wake being left, so it is free and the wake
// is what starts it; or its owner has already marked the hand-back, and the wake waits for
// the release as it always did. Acknowledged in either, the wake would be retired into an
// account nobody will start.
func TestAWakeThatCannotBeOwedIsLeftPending(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	ctx := context.Background()
	holder, peer := twoOverOneRedis(t, client)

	if _, err := holder.leases.Acquire(ctx, "s-back"); err != nil {
		t.Fatalf("given: %v", err)
	}
	if err := holder.leases.MarkHandingBack(ctx, "s-back"); err != nil {
		t.Fatalf("given: %v", err)
	}
	for sid, why := range map[string]string{
		"s-free": "an account nobody holds",
		"s-back": "an account whose owner marked the hand-back",
	} {
		var acked, released, forfeited int
		delivery := &transport.Delivery{
			Command: protocol.Command{V: protocol.Version, Type: protocol.CommandSessionWake, SID: sid, ID: "c-" + sid},
			Ack:     func(context.Context) error { acked++; return nil },
			Release: func() { released++ },
			Forfeit: func() { forfeited++ },
		}
		if peer.oweWake(ctx, delivery) {
			t.Fatalf("%s: the wake was reported owed, so it would be acknowledged", why)
		}
		if released != 1 || acked != 0 || forfeited != 0 {
			t.Fatalf("%s: acked=%d released=%d forfeited=%d, want it released", why, acked, released, forfeited)
		}
	}
}
