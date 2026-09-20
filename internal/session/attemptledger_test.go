package session_test

import (
	"context"
	"encoding/json"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/session"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
	"github.com/fazer-ai/whatsapp-connector/internal/store/storetest"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// An engine that applies the effect and only then reports failure, which is the shape
// #168 measured live: a `group.name.set` answered `not_connected` with the subject already
// applied at WhatsApp. Counting the effect apart from the call is the whole point -- a
// count of calls cannot tell a command that was refused on the doorstep from one that did
// the work and lost the answer.
type landedEngine struct {
	inner  engine.Engine
	answer func(effects *atomic.Int64) (json.RawMessage, error)

	mu      sync.Mutex
	opened  map[string]*landedSession
	effects atomic.Int64
}

type landedSession struct {
	engine.Session
	owner *landedEngine
}

func newLandedEngine(answer func(effects *atomic.Int64) (json.RawMessage, error)) *landedEngine {
	return &landedEngine{inner: fake.New(), answer: answer, opened: map[string]*landedSession{}}
}

func (e *landedEngine) Open(ctx context.Context, sid string) (engine.Session, error) {
	inner, err := e.inner.Open(ctx, sid)
	if err != nil {
		return nil, err
	}
	opened := &landedSession{Session: inner, owner: e}
	e.mu.Lock()
	e.opened[sid] = opened
	e.mu.Unlock()
	return opened, nil
}

func (e *landedEngine) Close() error { return e.inner.Close() }

func (e *landedEngine) applied() int64 { return e.effects.Load() }

func (s *landedSession) Execute(context.Context, *protocol.Command) (json.RawMessage, error) {
	return s.owner.answer(&s.owner.effects)
}

// The teardowns go to the engine's own methods rather than through Execute, so a double
// that only answered Execute would measure the fake on the one path `not_attempted` is
// actually produced on.
func (s *landedSession) Logout(context.Context) error {
	_, err := s.owner.answer(&s.owner.effects)
	return err
}

// replies keeps what each command was answered with.
type replies struct {
	mu   sync.Mutex
	seen map[string]protocol.Reply
}

func newReplies() *replies { return &replies{seen: map[string]protocol.Reply{}} }

func (r *replies) Publish(context.Context, *protocol.Event) error { return nil }

func (r *replies) Reply(_ context.Context, to string, reply protocol.Reply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen[to] = reply
	return nil
}

func (r *replies) get(to string) (protocol.Reply, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reply, ok := r.seen[to]
	return reply, ok
}

// instanceOn builds one instance of the fleet. The database is named rather than made,
// because two instances of a deployment share one: given one each, a record kept in the
// store would look invisible across a handover without being.
func instanceOn(t *testing.T, name string, rdb *redis.Client, eng engine.Engine, rec *replies, dsn string) *session.Manager {
	t.Helper()
	client := redisx.Wrap(rdb, "wa:", 8)
	container, err := store.Open(t.Context(), dsn, store.AlwaysOwned, zerolog.Nop())
	if err != nil {
		t.Fatalf("store.Open for %s: %v", name, err)
	}
	t.Cleanup(func() { _ = container.Close() })
	var ids atomic.Int64
	manager := session.NewManager(&session.ManagerConfig{
		Instance: name, Engine: eng,
		Leases:    cluster.NewLeases(client, name, cluster.Options{}),
		Publisher: rec, Replier: rec,
		Store:  container,
		Ledger: redisx.NewIdempotency(client, 0),
		NewID:  func() string { return name + "-evt-" + strconv.FormatInt(ids.Add(1), 10) },
		Logger: zerolog.New(io.Discard),
	})
	ctx, stop := context.WithCancel(context.Background())
	stopped := manager.Answer(ctx)
	t.Cleanup(func() { stop(); <-stopped; manager.StopAll(context.Background()) })
	return manager
}

const attemptSID = "9c2b7d1e-0000-4000-8000-000000000282"

// A participant update, which is the command this is worst for: adding somebody a second
// time on top of state that has since moved is not reapplying the same group name.
func participantAdd(id, key string) *protocol.Command {
	return &protocol.Command{
		V: protocol.Version, ID: id, Type: protocol.CommandGroupParticipantsUpdate,
		SID: attemptSID, ReplyTo: id, IdempotencyKey: key,
		Payload: json.RawMessage(`{"group":{"kind":"group","id":"120363000000000001"},
			"action":"add","participants":[{"kind":"phone","id":"5511999990002"}]}`),
	}
}

func deliver(t *testing.T, manager *session.Manager, rec *replies, command *protocol.Command) protocol.Reply {
	t.Helper()
	var acked atomic.Bool
	manager.Dispatch(&transport.Delivery{Command: *command,
		Ack: func(context.Context) error { acked.Store(true); return nil }})
	waitFor(t, "the reply to "+command.ID, func() bool { _, ok := rec.get(command.ReplyTo); return ok })
	reply, _ := rec.get(command.ReplyTo)
	return reply
}

// Invariant 5, on the half the ledger could not see before: a command that ran and whose
// answer never came back is answered rather than carried out a second time.
//
// The first attempt is answered with what the engine said, because at that moment nothing
// has been redelivered and the caller is owed the engine's own reason. The second is
// answered `timeout`, which is the contract's word for an outcome nobody here knows and
// nothing afterwards will -- and not `not_settled`, which promises the connector will know
// shortly and is true of a `group.create` alone.
func TestAnAttemptNobodyCanSpeakForIsAnsweredRatherThanCarriedOutAgain(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	eng := newLandedEngine(func(effects *atomic.Int64) (json.RawMessage, error) {
		effects.Add(1) // the participant was added at WhatsApp
		// Marked, because the write was already on its way when the socket went: that is
		// the one thing the engine knows and the code alone cannot say.
		return nil, engine.MayHaveLanded(
			protocol.NewError(protocol.ErrorNotConnected, "the socket died after the write"))
	})
	rec := newReplies()
	manager := instanceOn(t, "inst-a", rdb, eng, rec, storetest.New(t).URL)
	if _, err := manager.Adopt(context.Background(), attemptSID); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	first := deliver(t, manager, rec, participantAdd("c1", "k1"))
	second := deliver(t, manager, rec, participantAdd("c2", "k1"))

	if got := eng.applied(); got != 1 {
		t.Errorf("the participant was added %d times, want once: a redelivery carried out the "+
			"effect again, which is what #282 is about", got)
	}
	if first.OK {
		t.Error("the first attempt reported success for a command the engine refused")
	}
	if second.OK {
		t.Error("the redelivery reported success for a command nobody can speak for")
	}
	if second.Error == nil || second.Error.Code != protocol.ErrorTimeout {
		t.Errorf("the redelivery answered %+v, want %s: it is the word for an outcome nobody "+
			"here knows and nothing afterwards will", second.Error, protocol.ErrorTimeout)
	}
}

// And the attempt crosses a handover, because that is where the dangerous bugs live: the
// second instance has none of the first one's memory and reads the same Redis.
func TestAnAttemptIsVisibleToTheInstanceThatTakesTheSessionOver(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	first := redis.NewClient(&redis.Options{Addr: server.Addr()})
	second := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = first.Close(); _ = second.Close() })
	shared := storetest.New(t).URL

	landed := func(effects *atomic.Int64) (json.RawMessage, error) {
		effects.Add(1)
		return nil, engine.MayHaveLanded(
			protocol.NewError(protocol.ErrorNotConnected, "the socket died after the write"))
	}
	engA, engB := newLandedEngine(landed), newLandedEngine(landed)
	recA, recB := newReplies(), newReplies()

	a := instanceOn(t, "inst-a", first, engA, recA, shared)
	if _, err := a.Adopt(context.Background(), attemptSID); err != nil {
		t.Fatalf("inst-a Adopt: %v", err)
	}
	if reply := deliver(t, a, recA, participantAdd("c1", "k1")); reply.OK {
		t.Fatal("the first instance reported success for a command the engine refused")
	}
	a.StopAll(context.Background())

	b := instanceOn(t, "inst-b", second, engB, recB, shared)
	if _, err := b.Adopt(context.Background(), attemptSID); err != nil {
		t.Fatalf("inst-b Adopt: %v", err)
	}
	reply := deliver(t, b, recB, participantAdd("c2", "k1"))

	if got := engA.applied() + engB.applied(); got != 1 {
		t.Errorf("the participant was added %d times across the two instances, want once", got)
	}
	if got := engB.applied(); got != 0 {
		t.Errorf("the instance that took the session over carried the command out %d times: "+
			"the attempt the first one recorded did not reach it", got)
	}
	if reply.Error == nil || reply.Error.Code != protocol.ErrorTimeout {
		t.Errorf("the second instance answered %+v, want %s", reply.Error, protocol.ErrorTimeout)
	}
}

// A refusal the connector is certain about takes the attempt back off, so the retry does
// the whole thing. Without this the ledger would turn a recoverable failure into a
// permanent one: the command never runs, under that key, ever.
func TestACommandRefusedBeforeItReachedWhatsAppIsCarriedOutOnTheRetry(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	var refuse atomic.Bool
	refuse.Store(true)
	eng := newLandedEngine(func(effects *atomic.Int64) (json.RawMessage, error) {
		if refuse.Load() {
			// A pre-flight refusal: nothing was written, so it carries no mark and the
			// attempt comes back off.
			return nil, fakeRefusal()
		}
		effects.Add(1)
		return json.RawMessage(`{"added":1}`), nil
	})
	rec := newReplies()
	manager := instanceOn(t, "inst-a", rdb, eng, rec, storetest.New(t).URL)
	if _, err := manager.Adopt(context.Background(), attemptSID); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	if reply := deliver(t, manager, rec, participantAdd("c1", "k1")); reply.OK {
		t.Fatal("the engine's refusal was reported as a success")
	}
	refuse.Store(false)
	retry := deliver(t, manager, rec, participantAdd("c2", "k1"))

	if !retry.OK {
		t.Fatalf("the retry of a command that never reached WhatsApp was refused: %+v, and a "+
			"send refused this way would never go out under that id again", retry.Error)
	}
	if got := eng.applied(); got != 1 {
		t.Errorf("the effect happened %d times, want once on the retry", got)
	}
}

// fakeRefusal is what the fake engine answers a command that needs a live socket, which
// carries engine.ErrNeverSent the way the whatsmeow engine's own pre-flight refusal does.
func fakeRefusal() error { return fake.NotConnected() }

// `group.create` keeps a record of its own attempts, and this one must not stand in front
// of it.
//
// Its recovery is the thing `contract/PROTOCOL.md` promises: a retry under the same key
// returns the group the first attempt made, or `not_settled` while WhatsApp's notification
// is still deciding which request made which. That only happens if the command reaches the
// engine, so answering the redelivery from the ledger -- right for every other command,
// because nothing else can ever say what theirs did -- would replace a group with a
// refusal for as long as the record lived.
func TestACreationRedeliveredStillReachesItsOwnRecovery(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	eng := newLandedEngine(func(effects *atomic.Int64) (json.RawMessage, error) {
		effects.Add(1)
		return nil, protocol.NewError(protocol.ErrorNotConnected, "the socket died after the write")
	})
	rec := newReplies()
	manager := instanceOn(t, "inst-a", rdb, eng, rec, storetest.New(t).URL)
	if _, err := manager.Adopt(context.Background(), attemptSID); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	create := func(id string) *protocol.Command {
		return &protocol.Command{
			V: protocol.Version, ID: id, Type: protocol.CommandGroupCreate,
			SID: attemptSID, ReplyTo: id, IdempotencyKey: "k-create",
			Payload: json.RawMessage(`{"subject":"Obras","participants":[]}`),
		}
	}
	if reply := deliver(t, manager, rec, create("c1")); reply.OK {
		t.Fatal("the first creation reported success for a command the engine refused")
	}
	second := deliver(t, manager, rec, create("c2"))

	if got := eng.applied(); got != 2 {
		t.Errorf("the engine saw the creation %d times, want twice: the redelivery was "+
			"answered from the ledger instead of reaching the recovery that consults the "+
			"store, so a group that was made would never be handed back", got)
	}
	if second.Error != nil && second.Error.Code == protocol.ErrorTimeout {
		t.Error("the redelivery of a creation was answered from the attempt ledger")
	}
}

// A teardown the connector refused before the socket is retryable, which is the case the
// word `not_attempted` was added for.
//
// `session.logout` and `session.delete` answer it when whatsmeow's socket lock was never
// free, so WhatsApp was never told and the device is exactly as linked as it was. Held
// against a retry, the account stays linked with nothing on this side able to unlink it.
func TestATeardownTheConnectorNeverAttemptedIsCarriedOutOnTheRetry(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	var refuse atomic.Bool
	refuse.Store(true)
	eng := newLandedEngine(func(effects *atomic.Int64) (json.RawMessage, error) {
		if refuse.Load() {
			return nil, protocol.NewError(protocol.ErrorNotAttempted, "the request was never sent to WhatsApp")
		}
		effects.Add(1)
		return nil, nil
	})
	rec := newReplies()
	manager := instanceOn(t, "inst-a", rdb, eng, rec, storetest.New(t).URL)
	if _, err := manager.Adopt(context.Background(), attemptSID); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	unlink := func(id string) *protocol.Command {
		return &protocol.Command{
			V: protocol.Version, ID: id, Type: protocol.CommandSessionLogout,
			SID: attemptSID, ReplyTo: id, IdempotencyKey: "k-logout",
			Payload: json.RawMessage(`{}`),
		}
	}
	if reply := deliver(t, manager, rec, unlink("c1")); reply.OK {
		t.Fatal("a logout the connector never attempted reported success")
	}
	refuse.Store(false)
	retry := deliver(t, manager, rec, unlink("c2"))

	if !retry.OK {
		t.Fatalf("the retry of a teardown that was never attempted was refused: %+v, and the "+
			"device stays linked with nothing on this side able to unlink it", retry.Error)
	}
	if got := eng.applied(); got != 1 {
		t.Errorf("the teardown happened %d times, want once on the retry", got)
	}
}
