package session_test

import (
	"context"
	"encoding/json"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/session"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
	"github.com/fazer-ai/whatsapp-connector/internal/store/storetest"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
)

// pairedAs records an account the way a finished pairing leaves it: a device, and the
// connection its client asked for.
func pairedAs(t *testing.T, container *store.Container, sid string, wants store.Wants) {
	t.Helper()
	jid, err := waTypes.ParseJID(fake.PhoneFor(sid) + ":12@" + waTypes.DefaultUserServer)
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	if err := container.For(sid).Bind(t.Context(), jid); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := container.For(sid).PutDesiredConnected(t.Context(), wants); err != nil {
		t.Fatalf("PutDesiredConnected: %v", err)
	}
}

func wake(h harness, sid, payload string, acked *atomic.Bool) {
	h.manager.Dispatch(delivery(&protocol.Command{
		V: protocol.Version, ID: "w-" + sid, Type: protocol.CommandSessionWake, SID: sid,
		Payload: json.RawMessage(payload),
	}, acked))
}

// A `session.wake` on its own puts an account in the air when its client asked for it
// to be connected, which is what the contract has always said it does. It used to adopt
// and stop there: the account held its lease, sat on this instance's list of sessions,
// and stayed down for good, because the sweep that brings accounts back asks only about
// the ones nobody runs (#273).
//
// The `open` is looked for under the adopter's epoch. The shard carries what the previous
// owner published too, and asking whether there is an `open` at all answered yes for a
// session that never dialled.
func TestAWakePutsAnAccountItsClientWantedConnectedInTheAir(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	asked := store.Wants{Groups: true, CallAutoReject: true, Proxy: "socks5://user:secret@10.0.0.1:1080"}
	pairedAs(t, container, "s1", asked)
	h := newHarnessWithStore(t, container)

	var acked atomic.Bool
	wake(h, "s1", `{"desired":"connected"}`, &acked)
	waitFor(t, "the wake to be acknowledged", acked.Load)

	lease, owned := h.leases.Owned("s1")
	if !owned {
		t.Fatal("the wake was acknowledged and this instance holds no lease on the account")
	}
	waitFor(t, "an open under the adopter's epoch", func() bool {
		for _, event := range h.recorder.published() {
			if event.SID == "s1" && event.Epoch == lease.Epoch && event.Type == protocol.EventSessionState &&
				stateOf(t, &event) == "open" {
				return true
			}
		}
		return false
	})

	engineSession, _ := h.engine.Session("s1")
	request, connected := engineSession.Asked()
	if !connected {
		t.Fatal("the engine was never handed a connect")
	}
	// What the sweep would have sent, because the wake stands where the sweep would have
	// stood had nobody been running the account: the mode, and what the record remembers.
	want := engine.ConnectRequest{
		Pairing: "resume", Groups: true,
		Calls: &engine.CallsRequest{AutoReject: true}, Proxy: &engine.ProxyRequest{URL: asked.Proxy},
	}
	if render(t, request) != render(t, want) {
		t.Fatalf("the wake connected with %s, want %s", render(t, request), render(t, want))
	}
}

// Everything short of "connected, by a client, with a device" adopts and stops, which is
// what the wake did before and all it can do: it carries no connect of its own, so the
// record is the only licence to dial.
func TestAWakeThatIsNotALicenceToDialOnlyAdopts(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		given   func(t *testing.T, container *store.Container)
		payload string
	}{
		"no record at all": {
			given:   func(*testing.T, *store.Container) {},
			payload: `{"desired":"connected"}`,
		},
		"a client that turned the session off": {
			given: func(t *testing.T, container *store.Container) {
				pairedAs(t, container, "s1", store.Wants{})
				if err := container.For("s1").PutDesiredDisconnected(t.Context()); err != nil {
					t.Fatalf("PutDesiredDisconnected: %v", err)
				}
			},
			payload: `{"desired":"connected"}`,
		},
		"a pairing nobody finished": {
			given: func(t *testing.T, container *store.Container) {
				if err := container.For("s1").PutDesiredConnected(t.Context(), store.Wants{}); err != nil {
					t.Fatalf("PutDesiredConnected: %v", err)
				}
			},
			payload: `{"desired":"connected"}`,
		},
		"a wake asking for it to be down": {
			given:   func(t *testing.T, container *store.Container) { pairedAs(t, container, "s1", store.Wants{}) },
			payload: `{"desired":"disconnected"}`,
		},
		"a wake that names no state": {
			given:   func(t *testing.T, container *store.Container) { pairedAs(t, container, "s1", store.Wants{}) },
			payload: `{}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			container := openStore(t)
			tc.given(t, container)
			before, _, err := container.For("s1").Standing(t.Context())
			if err != nil {
				t.Fatalf("Standing: %v", err)
			}
			h := newHarnessWithStore(t, container)

			var acked atomic.Bool
			wake(h, "s1", tc.payload, &acked)
			waitFor(t, "the wake to be acknowledged", acked.Load)
			if got := h.manager.SIDs(); len(got) != 1 || got[0] != "s1" {
				t.Fatalf("running sessions = %v, want [s1]: the wake still adopts", got)
			}

			// A command behind the wake, answered by the same executor a connect would
			// have run on: once it answers, anything the wake queued has run.
			h.manager.Dispatch(delivery(&protocol.Command{
				V: protocol.Version, ID: "st", Type: protocol.CommandSessionStatus, SID: "s1", ReplyTo: "st",
				Payload: json.RawMessage(`{}`),
			}, &atomic.Bool{}))
			waitFor(t, "a reply to the status", func() bool { _, ok := h.recorder.reply("st"); return ok })

			engineSession, _ := h.engine.Session("s1")
			if got := engineSession.Connects(); got != 0 {
				t.Fatalf("the wake dialled %d times, want none", got)
			}
			after, _, err := container.For("s1").Standing(t.Context())
			if err != nil {
				t.Fatalf("Standing: %v", err)
			}
			if after != before {
				t.Fatalf("the wake rewrote the record from %+v to %+v", before, after)
			}
		})
	}
}

// A wake for a session this instance already runs hands it the same connect, and it is
// the one that retries a session whose last dial failed: the contract tells a client whose
// session does not come up to send another wake, and a wake that found the session here
// and did nothing would have made that advice a no-op.
func TestAWakeForASessionAlreadyHereConnectsItAgain(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	pairedAs(t, container, "s1", store.Wants{})
	h := newHarnessWithStore(t, container)
	if _, err := h.manager.Adopt(context.Background(), "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	lease, _ := h.leases.Owned("s1")

	var acked atomic.Bool
	wake(h, "s1", `{"desired":"connected"}`, &acked)
	waitFor(t, "the session to be connected", func() bool {
		engineSession, ok := h.engine.Session("s1")
		return ok && engineSession.Connected()
	})
	if again, _ := h.leases.Owned("s1"); again.Epoch != lease.Epoch {
		t.Fatalf("the epoch moved from %d to %d: the wake adopted the account a second time",
			lease.Epoch, again.Epoch)
	}
}

// What decides the dial is read before the account is adopted. Adopted first, a store
// that did not answer would leave the account on this instance and down, and the
// redelivered wake would find it running -- the same state #273 was, reached by a
// different road.
func TestAWakeWhoseRecordCannotBeReadIsLeftPendingUnadopted(t *testing.T) {
	t.Parallel()

	target := storetest.New(t)
	container, err := store.Open(t.Context(), target.URL, store.AlwaysOwned, zerolog.Nop())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })
	pairedAs(t, container, "s1", store.Wants{})
	if _, err := target.Pool(t).ExecContext(t.Context(),
		`ALTER TABLE wac_session_desired RENAME TO wac_session_desired_away`); err != nil {
		t.Fatalf("take the table away: %v", err)
	}
	h := newHarnessWithStore(t, container)

	var acked, forfeited atomic.Bool
	h.manager.Dispatch(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "w1", Type: protocol.CommandSessionWake, SID: "s1",
			Payload: json.RawMessage(`{"desired":"connected"}`),
		},
		Ack:     func(context.Context) error { acked.Store(true); return nil },
		Release: func() {},
		Forfeit: func() { forfeited.Store(true) },
	})
	waitFor(t, "the wake to be given back", forfeited.Load)
	if acked.Load() {
		t.Fatal("a wake whose record could not be read was acknowledged, so nothing will retry it")
	}
	if got := h.manager.SIDs(); len(got) != 0 {
		t.Fatalf("running sessions = %v, want none: adopted without knowing whether to dial, "+
			"the account sits here down and the redelivery finds it already running", got)
	}
}

func stateOf(t *testing.T, event *protocol.Event) string {
	t.Helper()
	var payload struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatalf("decode a session.state: %v", err)
	}
	return payload.State
}

// The connect a wake queues acts on what the client asks for when it runs, not on what
// the record said when the wake read it. A disconnect already queued ahead of it is
// carried out first, and the wake's copy of the record then says `connected` about an
// account its client has just turned off: carried out on that copy, the resume dials the
// socket the disconnect closed and writes `connected` back over the disconnect.
func TestAWakeDoesNotUndoADisconnectQueuedAheadOfIt(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	pairedAs(t, container, "s1", store.Wants{Proxy: "socks5://user:secret@10.0.0.1:1080"})
	h := newHarnessWithStore(t, container)
	if _, err := h.manager.Adopt(context.Background(), "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, _ := h.engine.Session("s1")

	// A command in flight holds the executor, so what is dispatched next waits behind it
	// in the order it was dispatched: the disconnect, then whatever the wake queues.
	release := engineSession.Hold()
	defer release()
	h.manager.Dispatch(delivery(&protocol.Command{
		V: protocol.Version, ID: "held", Type: protocol.CommandSessionStatus, SID: "s1", ReplyTo: "held",
		Payload: json.RawMessage(`{}`),
	}, &atomic.Bool{}))
	waitFor(t, "the held command to reach the engine", func() bool { return len(engineSession.Commands()) == 1 })
	h.manager.Dispatch(delivery(&protocol.Command{
		V: protocol.Version, ID: "d1", Type: protocol.CommandSessionDisconnect, SID: "s1", ReplyTo: "d1",
		Payload: json.RawMessage(`{}`),
	}, &atomic.Bool{}))
	var acked atomic.Bool
	wake(h, "s1", `{"desired":"connected"}`, &acked)
	waitFor(t, "the wake to be acknowledged", acked.Load)
	release()

	waitFor(t, "a reply to the disconnect", func() bool { _, ok := h.recorder.reply("d1"); return ok })
	h.manager.Dispatch(delivery(&protocol.Command{
		V: protocol.Version, ID: "st", Type: protocol.CommandSessionStatus, SID: "s1", ReplyTo: "st",
		Payload: json.RawMessage(`{}`),
	}, &atomic.Bool{}))
	waitFor(t, "a reply to the status", func() bool { _, ok := h.recorder.reply("st"); return ok })

	if got := engineSession.Connects(); got != 0 {
		t.Fatalf("the account was dialled %d times after its client turned it off", got)
	}
	if _, wanted, err := container.WantedSession(t.Context(), "s1"); err != nil || wanted {
		t.Fatalf("after a disconnect and the wake behind it the record says wanted=%v (err %v), "+
			"want the disconnect standing", wanted, err)
	}
}

// A wake whose connect finds no room on the session is not acknowledged. Acknowledged, the
// client's ask is retired with nothing queued to carry it out, and nothing else comes for
// an account this instance runs.
func TestAWakeWhoseConnectFindsNoRoomIsLeftPending(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	pairedAs(t, container, "s1", store.Wants{})
	h := newHarnessWithStore(t, container)
	if _, err := h.manager.Adopt(context.Background(), "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, _ := h.engine.Session("s1")

	release := engineSession.Hold()
	defer release()
	status := func(id string) {
		h.manager.Dispatch(delivery(&protocol.Command{
			V: protocol.Version, ID: id, Type: protocol.CommandSessionStatus, SID: "s1",
			Payload: json.RawMessage(`{}`),
		}, &atomic.Bool{}))
	}
	status("held")
	waitFor(t, "the held command to reach the engine", func() bool { return len(engineSession.Commands()) == 1 })
	for i := range session.DefaultQueueDepth {
		status("fill-" + strconv.Itoa(i))
	}

	var acked, forfeited atomic.Bool
	h.manager.Dispatch(&transport.Delivery{
		Command: protocol.Command{
			V: protocol.Version, ID: "w1", Type: protocol.CommandSessionWake, SID: "s1",
			Payload: json.RawMessage(`{"desired":"connected"}`),
		},
		Ack:     func(context.Context) error { acked.Store(true); return nil },
		Release: func() {},
		Forfeit: func() { forfeited.Store(true) },
	})
	waitFor(t, "the wake to be given back", forfeited.Load)
	if acked.Load() {
		t.Fatal("a wake whose connect was never queued was acknowledged, so nothing will carry it out")
	}
}

// The same, with a connect naming another proxy queued ahead of the wake's: the account
// ends on the proxy its client asked for last. Carried out on the copy the wake read, the
// resume would put the old proxy back on the socket and write it back over the new one.
func TestAWakeDoesNotRestoreAProxyAConnectAheadOfItReplaced(t *testing.T) {
	t.Parallel()

	container := openStore(t)
	const before, after = "socks5://user:secret@10.0.0.1:1080", "http://10.0.0.2:3128"
	pairedAs(t, container, "s1", store.Wants{Proxy: before})
	h := newHarnessWithStore(t, container)
	if _, err := h.manager.Adopt(context.Background(), "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	engineSession, _ := h.engine.Session("s1")

	release := engineSession.Hold()
	defer release()
	h.manager.Dispatch(delivery(&protocol.Command{
		V: protocol.Version, ID: "held", Type: protocol.CommandSessionStatus, SID: "s1",
		Payload: json.RawMessage(`{}`),
	}, &atomic.Bool{}))
	waitFor(t, "the held command to reach the engine", func() bool { return len(engineSession.Commands()) == 1 })
	h.manager.Dispatch(delivery(&protocol.Command{
		V: protocol.Version, ID: "c1", Type: protocol.CommandSessionConnect, SID: "s1", ReplyTo: "c1",
		Payload: json.RawMessage(`{"pairing":"resume","proxy":{"url":"` + after + `"}}`),
	}, &atomic.Bool{}))
	var acked atomic.Bool
	wake(h, "s1", `{"desired":"connected"}`, &acked)
	waitFor(t, "the wake to be acknowledged", acked.Load)
	release()

	waitFor(t, "a reply to the connect", func() bool { _, ok := h.recorder.reply("c1"); return ok })
	h.manager.Dispatch(delivery(&protocol.Command{
		V: protocol.Version, ID: "st", Type: protocol.CommandSessionStatus, SID: "s1", ReplyTo: "st",
		Payload: json.RawMessage(`{}`),
	}, &atomic.Bool{}))
	waitFor(t, "a reply to the status", func() bool { _, ok := h.recorder.reply("st"); return ok })

	if request, _ := engineSession.Asked(); request.ProxyURL() != after {
		t.Fatalf("the last connect the engine was handed goes through %q, want %q", request.ProxyURL(), after)
	}
	if standing, _, err := container.For("s1").Standing(t.Context()); err != nil || standing.Proxy != after {
		t.Fatalf("the record names %q (err %v), want the proxy the client asked for last", standing.Proxy, err)
	}
}

// A resume whose second reading of the record fails still goes by what the client asked
// for. Failing it would strand the account: it is adopted, so the sweep does not ask about
// it, and nothing retires a resume that never started. What changed since the account was
// taken over ran on this instance's executor, so that is what decides: a disconnect queued
// ahead still keeps the account down, and with nothing ahead the queued request stands.
func TestAResumeWhoseRecordCannotBeReadAgainGoesByWhatThisInstanceKnows(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		ahead string // a lifecycle command queued ahead of the wake's connect, if any
		dials int
	}{
		"nothing asked here since":  {dials: 1},
		"a disconnect queued ahead": {ahead: `{"type":"session.disconnect"}`, dials: 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			target := storetest.New(t)
			container, err := store.Open(t.Context(), target.URL, store.AlwaysOwned, zerolog.Nop())
			if err != nil {
				t.Fatalf("store.Open: %v", err)
			}
			t.Cleanup(func() { _ = container.Close() })
			pairedAs(t, container, "s1", store.Wants{Groups: true, CallAutoReject: true})
			h := newHarnessWithStore(t, container)
			if _, err := h.manager.Adopt(context.Background(), "s1"); err != nil {
				t.Fatalf("Adopt: %v", err)
			}
			engineSession, _ := h.engine.Session("s1")

			release := engineSession.Hold()
			defer release()
			h.manager.Dispatch(delivery(&protocol.Command{
				V: protocol.Version, ID: "held", Type: protocol.CommandSessionStatus, SID: "s1",
				Payload: json.RawMessage(`{}`),
			}, &atomic.Bool{}))
			waitFor(t, "the held command to reach the engine", func() bool { return len(engineSession.Commands()) == 1 })
			if tc.ahead != "" {
				h.manager.Dispatch(delivery(&protocol.Command{
					V: protocol.Version, ID: "d1", Type: protocol.CommandSessionDisconnect, SID: "s1",
					Payload: json.RawMessage(`{}`),
				}, &atomic.Bool{}))
			}
			var acked atomic.Bool
			wake(h, "s1", `{"desired":"connected"}`, &acked)
			waitFor(t, "the wake to be acknowledged", acked.Load)
			// The record goes away between the wake's reading and the connect's.
			if _, err := target.Pool(t).ExecContext(t.Context(),
				`ALTER TABLE wac_session_desired RENAME TO wac_session_desired_away`); err != nil {
				t.Fatalf("take the table away: %v", err)
			}
			release()

			h.manager.Dispatch(delivery(&protocol.Command{
				V: protocol.Version, ID: "st", Type: protocol.CommandSessionStatus, SID: "s1", ReplyTo: "st",
				Payload: json.RawMessage(`{}`),
			}, &atomic.Bool{}))
			waitFor(t, "a reply to the status", func() bool { _, ok := h.recorder.reply("st"); return ok })
			if got := engineSession.Connects(); got != tc.dials {
				t.Fatalf("the account was dialled %d times, want %d", got, tc.dials)
			}
			// And with what the client asked for, which is all the queued copy carries.
			if request, dialled := engineSession.Asked(); dialled && (!request.Groups || request.Calls == nil) {
				t.Fatalf("the resume went out as %s, without the subscription and call policy it was queued with",
					render(t, request))
			}
		})
	}
}

// A disconnect whose row could not be written is still a disconnect. The executor that ran
// it knows, and the row -- which still says `connected` -- is older than that: a wake that
// reads it and believes it would dial the account its client has just turned off.
func TestAWakeDoesNotUndoADisconnectThatCouldNotBeRecorded(t *testing.T) {
	t.Parallel()

	target := storetest.New(t)
	container, err := store.Open(t.Context(), target.URL, store.AlwaysOwned, zerolog.Nop())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })
	pairedAs(t, container, "s1", store.Wants{})
	h := newHarnessWithStore(t, container)
	if _, err := h.manager.Adopt(context.Background(), "s1"); err != nil {
		t.Fatalf("Adopt: %v", err)
	}

	// Writes to the row refused from here on, reads left alone.
	refuse := []string{
		`CREATE TRIGGER wac_refuse_insert BEFORE INSERT ON wac_session_desired BEGIN SELECT RAISE(ABORT, 'refused'); END`,
		`CREATE TRIGGER wac_refuse_update BEFORE UPDATE ON wac_session_desired BEGIN SELECT RAISE(ABORT, 'refused'); END`,
	}
	if target.Postgres() {
		refuse = []string{
			`CREATE FUNCTION wac_refuse() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'refused'; END $$ LANGUAGE plpgsql`,
			`CREATE TRIGGER wac_refuse BEFORE INSERT OR UPDATE ON wac_session_desired FOR EACH ROW EXECUTE FUNCTION wac_refuse()`,
		}
	}
	for _, statement := range refuse {
		if _, err := target.Pool(t).ExecContext(t.Context(), statement); err != nil {
			t.Fatalf("refuse writes: %v", err)
		}
	}

	h.manager.Dispatch(delivery(&protocol.Command{
		V: protocol.Version, ID: "d1", Type: protocol.CommandSessionDisconnect, SID: "s1", ReplyTo: "d1",
		Payload: json.RawMessage(`{}`),
	}, &atomic.Bool{}))
	waitFor(t, "a reply to the disconnect", func() bool { _, ok := h.recorder.reply("d1"); return ok })
	if _, wanted, err := container.WantedSession(t.Context(), "s1"); err != nil || !wanted {
		t.Fatalf("the row reads wanted=%v (err %v); this test needs it still saying connected", wanted, err)
	}

	var acked atomic.Bool
	wake(h, "s1", `{"desired":"connected"}`, &acked)
	waitFor(t, "the wake to be acknowledged", acked.Load)
	h.manager.Dispatch(delivery(&protocol.Command{
		V: protocol.Version, ID: "st", Type: protocol.CommandSessionStatus, SID: "s1", ReplyTo: "st",
		Payload: json.RawMessage(`{}`),
	}, &atomic.Bool{}))
	waitFor(t, "a reply to the status", func() bool { _, ok := h.recorder.reply("st"); return ok })

	engineSession, _ := h.engine.Session("s1")
	if got := engineSession.Connects(); got != 0 {
		t.Fatalf("the account was dialled %d times after a disconnect this instance carried out", got)
	}
}
