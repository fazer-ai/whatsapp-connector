package session_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
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
)

// holdEngine stops inside `Connect` or `Disconnect` and stays there until the test lets
// it go, which is the only way to stand in the instant the two tests below are about.
//
// A double and not the fake engine, because what is being held is the engine call itself:
// the fake returns, and "returned" is precisely the state these tests must not be in.
type holdEngine struct {
	events      chan engine.Emission
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	holdConnect bool
}

func newHoldEngine() *holdEngine {
	return &holdEngine{
		events: make(chan engine.Emission), entered: make(chan struct{}), release: make(chan struct{}),
	}
}

func (e *holdEngine) Open(context.Context, string) (engine.Session, error) { return e, nil }
func (e *holdEngine) Events() <-chan engine.Emission                       { return e.events }
func (e *holdEngine) Finished() uint64                                     { return 0 }
func (e *holdEngine) Connect(ctx context.Context, _ engine.ConnectRequest) error {
	if !e.holdConnect {
		return nil
	}
	e.once.Do(func() { close(e.entered) })
	select {
	case <-e.release:
	case <-ctx.Done():
	}
	return nil
}
func (e *holdEngine) Logout(context.Context) error { return nil }
func (e *holdEngine) Delete(context.Context) error { return nil }

func (e *holdEngine) Execute(context.Context, *protocol.Command) (json.RawMessage, error) {
	return json.RawMessage(`{}`), nil
}

func (e *holdEngine) Disconnect(ctx context.Context) error {
	e.once.Do(func() { close(e.entered) })
	select {
	case <-e.release:
	case <-ctx.Done():
	}
	return nil
}

func (e *holdEngine) Close() error { close(e.events); return nil }

// The record is down before the socket is, and a test can stand in between and say so.
//
// The ordering is the whole of what this write is worth. A record written after the engine
// call is a record missing exactly when the instance dies inside that call, and both calls
// are network operations with no ceiling of their own: a hang-up that waits on WhatsApp,
// and a connect that is a handshake. Die in the middle of a disconnect and the account the
// operator turned off is left looking like one the sweep should bring back; die in the
// middle of a connect and the account nobody switched off is left with nothing saying it
// should be up.
//
// Written because the claim that this could not be tested was wrong, and it was this
// branch's own claim. A mutant that moves the write after the engine call while still
// writing on both outcomes survived the suite in both directions, and the round that found
// it justified the survivor as a window no test can reach. It is reachable: an engine that
// stops inside the call holds the window open, and the table is readable while it is.
//
// The lenient mutant is the one that matters here. The blunt form -- engine first, return
// early when it fails -- is a different defect and dies on the two tests about a refusal
// being remembered; this one agrees with the delivery on every outcome and differs only on
// when, which is what makes it the form that would survive a refactor.
func TestTheRowIsAlreadyDownWhileTheSocketIsStillClosing(t *testing.T) {
	container := openStore(t)
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	client := redisx.Wrap(rdb, "wa:", 8)
	held := newHoldEngine()
	rec := newRecorder()
	var ids atomic.Int64
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: held,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: rec, Replier: rec, Store: container,
		NewID:  func() string { return "evt-" + strconv.FormatInt(ids.Add(1), 10) },
		Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { close(held.release); manager.StopAll(context.Background()) })

	ctx := context.Background()
	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	var acked atomic.Bool
	manager.Dispatch(delivery(&protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandSessionConnect, SID: "s1", ReplyTo: "c1",
		Payload: json.RawMessage(`{"pairing":"qr"}`),
	}, &acked))
	waitFor(t, "the connect to be answered", func() bool { _, ok := rec.reply("c1"); return ok })

	manager.Dispatch(delivery(&protocol.Command{
		V: protocol.Version, ID: "c2", Type: protocol.CommandSessionDisconnect, SID: "s1", ReplyTo: "c2",
		Payload: json.RawMessage(`{}`),
	}, &acked))

	select {
	case <-held.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the engine's Disconnect was never reached, so nothing was measured")
	}

	var desired string
	switch err := container.DB().QueryRowContext(ctx,
		`SELECT desired FROM wac_session_desired WHERE sid = 's1'`).Scan(&desired); {
	case errors.Is(err, sql.ErrNoRows):
		desired = "no row at all"
	case err != nil:
		t.Fatalf("read the desired state: %v", err)
	}
	if desired != "disconnected" {
		t.Fatalf("while the socket is still closing the row reads %q, want \"disconnected\".\n"+
			"An instance that dies inside the hang-up leaves an account the operator turned "+
			"off looking like one the sweep should bring back.", desired)
	}
}

// The same instant on the way up, and the reason it gets its own test rather than a
// subtest: the two orderings are two lines in two branches, and a fence that held only one
// of them would leave the other free to move.
func TestTheRowIsAlreadyUpWhileTheHandshakeIsStillRunning(t *testing.T) {
	container := openStore(t)
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	client := redisx.Wrap(rdb, "wa:", 8)
	held := newHoldEngine()
	held.holdConnect = true
	rec := newRecorder()
	var ids atomic.Int64
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: held,
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: rec, Replier: rec, Store: container,
		NewID:  func() string { return "evt-" + strconv.FormatInt(ids.Add(1), 10) },
		Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { close(held.release); manager.StopAll(context.Background()) })

	ctx := context.Background()
	if _, err := manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	var acked atomic.Bool
	manager.Dispatch(delivery(&protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandSessionConnect, SID: "s1", ReplyTo: "c1",
		Payload: json.RawMessage(`{"pairing":"qr","groups":true}`),
	}, &acked))

	select {
	case <-held.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the engine's Connect was never reached, so nothing was measured")
	}

	var desired string
	switch err := container.DB().QueryRowContext(ctx,
		`SELECT desired FROM wac_session_desired WHERE sid = 's1'`).Scan(&desired); {
	case errors.Is(err, sql.ErrNoRows):
		desired = "no row at all"
	case err != nil:
		t.Fatalf("read the desired state: %v", err)
	}
	if desired != "connected" {
		t.Fatalf("while the handshake is still running the row reads %q, want \"connected\".\n"+
			"A connect is a network handshake with no ceiling by default, so an instance that "+
			"dies inside it leaves an account nothing brings back.", desired)
	}
}
