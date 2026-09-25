package session_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/session"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
	"github.com/fazer-ai/whatsapp-connector/internal/store/storetest"
)

// What the connector knows about an account after the instance running it is gone.
//
// A lease dies with its holder and a wake is a frame read once, so without this a paired
// account comes up unowned and stays down: the inbox shows `open` and nothing arrives,
// which is what fazer-ai/chatwoot#577 measured on a live deployment.
//
// Here rather than in an engine, which is where it was until #266. One engine wrote it and
// the other did not, so an account running on anything but whatsmeow left nothing for the
// sweep to read -- and the suite stayed green, because every test that asserted the row
// ran the engine that happened to write it. What the row holds is the client's request,
// not a fact about WhatsApp, and this is the layer the request arrives at.
//
// Both directions, because the second is what keeps the recovery from undoing an operator:
// a session turned off has to stay off across a restart, and a record that only ever said
// "connected" would dial it again.
//
// The subscription, the call policy and the proxy travel with it. A resume has no other way of
// learning them: the connect a sweep synthesises is not a frame a client sent, so what it
// does not carry is absent rather than defaulted, and the session comes back acknowledging
// group traffic it publishes nowhere.
func TestAConnectIsRememberedWithWhatItAskedFor(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	h := newHarnessWithStore(t, container)
	ctx := context.Background()

	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	connect := &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandSessionConnect, SID: "s1", ReplyTo: "c1",
		Payload: json.RawMessage(`{"pairing":"qr","groups":true,"calls":{"auto_reject":true},` +
			`"proxy":{"url":"socks5://user:secret@10.0.0.1:1080"}}`),
	}
	var acked atomic.Bool
	h.manager.Dispatch(delivery(connect, &acked))
	waitFor(t, "the connect to be answered", func() bool { _, ok := h.recorder.reply("c1"); return ok })

	// Read through `Wanted`, which is what the sweep reads, rather than off the table: it
	// joins the desired row with the pairing, so a row nobody can act on reads as no row
	// at all and this assertion means what it says.
	wanted, err := container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(wanted) != 1 || wanted[0].SID != "s1" {
		t.Fatalf("after a connect the sweep would bring back %v, want just s1.\n"+
			"Nothing anywhere says this account should be in the air, so the instance that "+
			"comes after this one leaves it down: no error, no warning, an empty list.", wanted)
	}
	if !wanted[0].Groups || !wanted[0].CallAutoReject {
		t.Fatalf("the row carries groups=%v auto_reject=%v, want both.\n"+
			"A resume synthesises its connect from this row and nothing else, so what is "+
			"missing here is a session that comes back deaf to the traffic its client asked "+
			"for, acknowledging it and publishing it nowhere.",
			wanted[0].Groups, wanted[0].CallAutoReject)
	}
	if wanted[0].Proxy != "socks5://user:secret@10.0.0.1:1080" {
		t.Fatalf("the row carries the proxy %q.\n"+
			"The resume dials through whatever this says, so a proxy missing here is an account "+
			"that comes back from this instance's own address -- the one its client asked not "+
			"to be seen from.", wanted[0].Proxy)
	}

	disconnect := &protocol.Command{
		V: protocol.Version, ID: "c2", Type: protocol.CommandSessionDisconnect, SID: "s1", ReplyTo: "c2",
		Payload: json.RawMessage(`{}`),
	}
	h.manager.Dispatch(delivery(disconnect, &acked))
	waitFor(t, "the disconnect to be answered", func() bool { _, ok := h.recorder.reply("c2"); return ok })

	wanted, err = container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(wanted) != 0 {
		t.Fatalf("after a disconnect the sweep would still bring back %v.\n"+
			"An operator turned this session off and the next sweep dials the socket they "+
			"just closed, which is the recovery undoing a person.", wanted)
	}
}

func openStore(t *testing.T) *store.Container {
	t.Helper()

	container, err := store.Open(t.Context(), storetest.New(t).URL, store.AlwaysOwned, zerolog.Nop())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return container
}

// newHarnessWithStore is newHarness with the store wired, which is what a deployment has
// and what every other harness here deliberately does without.
func newHarnessWithStore(t *testing.T, container *store.Container) harness {
	t.Helper()
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	client := redisx.Wrap(rdb, "wa:", 8)
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	fakeEngine := fake.New(fake.WithStore(container))
	rec := newRecorder()
	book := &ledger{inner: redisx.NewIdempotency(client, 0)}
	var ids atomic.Int64
	quarantine := cluster.NewQuarantine(client, nil)
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fakeEngine, Leases: leases, Publisher: rec, Replier: rec,
		Ledger:     book,
		Store:      container,
		Quarantine: quarantine,
		NewID:      func() string { return "evt-" + strconv.FormatInt(ids.Add(1), 10) },
		Logger:     zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })
	answering(t, manager)
	return harness{
		leases: leases, quarantine: quarantine, engine: fakeEngine,
		recorder: rec, ledger: book, manager: manager,
	}
}

// An account that left does not come back, which is this defect with the sign reversed.
//
// The whole of #266 is "the sweep has nothing to read". The moment an engine starts
// writing, the opposite becomes possible and costs the same: an account WhatsApp no
// longer knows, brought back every pass, for ever. Nothing looked here before, because
// before this there was nothing to clean up -- the engine every test runs wrote no rows
// at all.
//
// Both commands, because they end a session by different routes and either one leaving
// the row behind is the same silence: `session.logout` hands the credentials back to
// WhatsApp, `session.delete` unlinks and forgets.
//
// Asked of `Wanted` rather than of a row count, because what matters is what the sweep
// reads: it joins the request with the pairing, so whichever half is missing is enough.
func TestAnAccountThatLeftIsNotBroughtBack(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		command protocol.CommandType
	}{
		{name: "logout", command: protocol.CommandSessionLogout},
		{name: "delete", command: protocol.CommandSessionDelete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			container := openStore(t)
			h := newHarnessWithStore(t, container)
			ctx := context.Background()

			// Two accounts, and the second one is not decoration: a run where the sweep
			// brings back nothing proves nothing, because on the base it brings back
			// nothing either and the negative half passes for free. `s2` is the positive
			// control, in the same run and the same condition.
			for _, sid := range []string{"s1", "s2"} {
				if _, err := h.manager.Adopt(ctx, sid); err != nil {
					t.Fatalf("Adopt %s: %v", sid, err)
				}
				connect := &protocol.Command{
					V: protocol.Version, ID: "c-" + sid, Type: protocol.CommandSessionConnect,
					SID: sid, ReplyTo: "c-" + sid, Payload: json.RawMessage(`{"pairing":"qr"}`),
				}
				var acked atomic.Bool
				h.manager.Dispatch(delivery(connect, &acked))
				waitFor(t, "the connect of "+sid+" to be answered", func() bool {
					_, ok := h.recorder.reply("c-" + sid)
					return ok
				})
			}

			ending := &protocol.Command{
				V: protocol.Version, ID: "end", Type: tc.command, SID: "s1", ReplyTo: "end",
				Payload: json.RawMessage(`{}`),
			}
			var acked atomic.Bool
			h.manager.Dispatch(delivery(ending, &acked))
			waitFor(t, "the "+tc.name+" to be answered", func() bool { _, ok := h.recorder.reply("end"); return ok })

			wanted, err := container.Wanted(ctx)
			if err != nil {
				t.Fatalf("Wanted: %v", err)
			}
			var brought []string
			for _, row := range wanted {
				brought = append(brought, row.SID)
			}
			if slices.Contains(brought, "s1") {
				t.Fatalf("after a %s the sweep would still bring back s1 (it reads %v).\n"+
					"The credentials are gone and the record of the request outlived them, so "+
					"every pass dials an account WhatsApp does not know any more, for as long "+
					"as the row lives.", tc.name, brought)
			}
			if !slices.Contains(brought, "s2") {
				t.Fatalf("the sweep reads %v, and s2 is not in it.\n"+
					"s2 was never ended and is the control for this test: without it, a "+
					"delivery that recorded nothing at all would pass the assertion above "+
					"and prove nothing.", brought)
			}
		})
	}
}

// The record is written before the engine is called, and a refusal does not undo it.
//
// This is the ordering the engine's own comment defended when the write lived there:
// after the point the request stops being one this layer might refuse, and before
// anything that changes the session. Written the other way round, the record is missing
// for exactly as long as the engine takes -- and a connect is a network handshake that
// has no ceiling by default, so "as long as the engine takes" is unbounded. An instance
// that dies in that window leaves an account nothing brings back, which is #266 returning
// through a smaller door.
//
// Asserted as an outcome and never as a call order. "PutDesired came before Connect" is a
// statement about the shape of a function and dies at the next refactor; "a connect the
// engine refused still left the record" is the promise, and it is what a reader of the
// table sees.
//
// It is also a behaviour change, and a deliberate one: before #266 a connect the engine
// refused left nothing, and now it leaves a request the sweep will retry. That is the
// better answer -- the client asked for this session, and a device that would not open
// now is the kind of thing a later attempt fixes -- and the quarantine is what stops an
// account that fails for ever from being asked for for ever.
func TestAConnectTheEngineRefusedIsStillRemembered(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	h := newHarnessWithStore(t, container)
	ctx := context.Background()

	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	// Paired first, so the row this asserts on is one `Wanted` can return: the refusal
	// below is about the connect, not about the account being unknown.
	opened, err := h.engine.Open(ctx, "s1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	fakeSession, ok := opened.(*fake.Session)
	if !ok {
		t.Fatalf("the harness engine handed back %T, want a *fake.Session", opened)
	}
	first := &protocol.Command{
		V: protocol.Version, ID: "c0", Type: protocol.CommandSessionConnect, SID: "s1", ReplyTo: "c0",
		Payload: json.RawMessage(`{"pairing":"qr"}`),
	}
	var acked atomic.Bool
	h.manager.Dispatch(delivery(first, &acked))
	waitFor(t, "the pairing connect to be answered", func() bool { _, ok := h.recorder.reply("c0"); return ok })

	// Turned off, so the row says the opposite of what the refused connect will ask for
	// and the assertion cannot pass on what the first connect left behind.
	down := &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandSessionDisconnect, SID: "s1", ReplyTo: "c1",
		Payload: json.RawMessage(`{}`),
	}
	h.manager.Dispatch(delivery(down, &acked))
	waitFor(t, "the disconnect to be answered", func() bool { _, ok := h.recorder.reply("c1"); return ok })
	if wanted, err := container.Wanted(ctx); err != nil || len(wanted) != 0 {
		t.Fatalf("the given is wrong: after a disconnect the sweep reads %v (err=%v), want nothing", wanted, err)
	}

	fakeSession.FailConnect(errors.New("fake: this connect is refused"))
	refused := &protocol.Command{
		V: protocol.Version, ID: "c2", Type: protocol.CommandSessionConnect, SID: "s1", ReplyTo: "c2",
		Payload: json.RawMessage(`{"pairing":"resume","groups":true}`),
	}
	h.manager.Dispatch(delivery(refused, &acked))
	waitFor(t, "the refused connect to be answered", func() bool { _, ok := h.recorder.reply("c2"); return ok })

	wanted, err := container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(wanted) != 1 || wanted[0].SID != "s1" {
		t.Fatalf("a connect the engine refused left the sweep reading %v, want s1.\n"+
			"The record is being written after the engine is called, so everything the "+
			"engine takes -- a network handshake with no ceiling by default -- is a window "+
			"in which this instance dying leaves an account nothing brings back.", wanted)
	}
	if !wanted[0].Groups {
		t.Fatal("the refused connect was remembered without the subscription it asked for, " +
			"so the resume that retries it would bring the account back deaf to groups")
	}
}

// And the same promise on the way down: a disconnect the engine could not carry out is
// still a disconnect somebody asked for.
//
// The asymmetry matters. A record written after the engine returns is missing for the
// whole of the hang-up, and an instance that dies there leaves a row saying the account
// should be up -- so the next sweep dials the socket an operator had just asked to close,
// and the recovery undoes a person. The request is the fact; whether the socket obeyed is
// a different one.
func TestADisconnectTheEngineRefusedIsStillRemembered(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	h := newHarnessWithStore(t, container)
	ctx := context.Background()

	if _, err := h.manager.Adopt(ctx, "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	opened, err := h.engine.Open(ctx, "s1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	fakeSession, ok := opened.(*fake.Session)
	if !ok {
		t.Fatalf("the harness engine handed back %T, want a *fake.Session", opened)
	}

	connect := &protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandSessionConnect, SID: "s1", ReplyTo: "c1",
		Payload: json.RawMessage(`{"pairing":"qr"}`),
	}
	var acked atomic.Bool
	h.manager.Dispatch(delivery(connect, &acked))
	waitFor(t, "the connect to be answered", func() bool { _, ok := h.recorder.reply("c1"); return ok })
	if wanted, err := container.Wanted(ctx); err != nil || len(wanted) != 1 {
		t.Fatalf("the given is wrong: after a connect the sweep reads %v (err=%v), want s1", wanted, err)
	}

	fakeSession.FailDisconnect(errors.New("fake: the socket would not close"))
	down := &protocol.Command{
		V: protocol.Version, ID: "c2", Type: protocol.CommandSessionDisconnect, SID: "s1", ReplyTo: "c2",
		Payload: json.RawMessage(`{}`),
	}
	h.manager.Dispatch(delivery(down, &acked))
	waitFor(t, "the refused disconnect to be answered", func() bool { _, ok := h.recorder.reply("c2"); return ok })

	wanted, err := container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(wanted) != 0 {
		t.Fatalf("a disconnect the engine refused left the sweep reading %v, want nothing.\n"+
			"The record is being written after the engine is called, so an instance that "+
			"dies during the hang-up leaves a row saying this account should be up, and the "+
			"next sweep dials the socket the operator had just asked to close.", wanted)
	}
}

// Two sessions are two accounts, and this is the assertion that makes the one above mean
// something.
//
// `wac_session_device` carries a UNIQUE index over `account`, and the store binds on the
// account rather than on the device, because re-pairing issues a new device for the same
// number. So an engine that pairs every session to one number does not fail: the second
// bind displaces the first, in silence, and a fleet of ten accounts is one account with
// nine ghosts. That is what blocked #264's mass-adoption measurement.
//
// Measured, and it is why this test exists rather than being folded into the one above:
// with the number a constant, `TestAnAccountThatLeftIsNotBroughtBack` still passes. Its
// negative half asks that a logged-out session is absent, and a displaced one is absent
// too -- for the wrong reason, and with no control able to tell the difference. A live
// mutant found that; the assertion below is what kills it.
func TestTwoSessionsArePairedToTwoAccounts(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	h := newHarnessWithStore(t, container)
	ctx := context.Background()

	for _, sid := range []string{"s1", "s2"} {
		if _, err := h.manager.Adopt(ctx, sid); err != nil {
			t.Fatalf("Adopt %s: %v", sid, err)
		}
		connect := &protocol.Command{
			V: protocol.Version, ID: "c-" + sid, Type: protocol.CommandSessionConnect,
			SID: sid, ReplyTo: "c-" + sid, Payload: json.RawMessage(`{"pairing":"qr"}`),
		}
		var acked atomic.Bool
		h.manager.Dispatch(delivery(connect, &acked))
		waitFor(t, "the connect of "+sid+" to be answered", func() bool {
			_, ok := h.recorder.reply("c-" + sid)
			return ok
		})
	}

	first, firstBound, err := container.For("s1").JID(ctx)
	if err != nil {
		t.Fatalf("read the account of s1: %v", err)
	}
	second, secondBound, err := container.For("s2").JID(ctx)
	if err != nil {
		t.Fatalf("read the account of s2: %v", err)
	}
	if !firstBound || !secondBound {
		t.Fatalf("after two pairings the accounts are bound=%v and bound=%v, want both.\n"+
			"One session's pairing displaced the other's: the store matches on the account, "+
			"and two sessions on one number are one row that the second bind takes over.",
			firstBound, secondBound)
	}
	if first.User == second.User {
		t.Fatalf("both sessions paired to %s.\n"+
			"The UNIQUE index over `account` means the second bind displaces the first in "+
			"silence, so a bench that creates ten accounts measures one. The number has to "+
			"come from the session id.", first.User)
	}

	// And the sweep sees two, which is the reading that matters: a pairing row nobody can
	// join to a request is not an account this connector can bring back.
	wanted, err := container.Wanted(ctx)
	if err != nil {
		t.Fatalf("Wanted: %v", err)
	}
	if len(wanted) != 2 {
		t.Fatalf("the sweep would bring back %v, want both sessions", wanted)
	}
}
