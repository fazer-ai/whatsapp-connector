package app

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow/proto/waAdv"
	waTypes "go.mau.fi/whatsmeow/types"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/session"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
)

// The whole point of the sweep, in the state a deployment reaches on every restart: the
// account is paired, the client asked for it to be connected, and no instance is running
// it. Nothing else in the fleet notices -- the lease died with the instance that held it,
// the wake that would have started it was read once and is gone, and the client polls
// nothing.
func TestTheResumeSweepBringsBackAnAccountNobodyIsRunning(t *testing.T) {
	t.Parallel()

	connector, container, engine, server := newResumeConnector(t)
	wantConnected(t, container, "sid-1", "5511999990001")

	connector.resumeOnce(t.Context())

	waitFor(t, "the account to be taken and connected", func() bool {
		account, ok := engine.Session("sid-1")
		return ok && account.Connected()
	})
	if !server.Exists(redisx.NewKeys("wa:", 8).Resume("sid-1")) {
		t.Fatal("the attempt left no mark, so every instance in the fleet would make it again on its own next pass")
	}
	// And an account this instance is already running is not asked for again: the sweep
	// is about accounts nobody has, and a connect offered to a live session would dial a
	// socket that is already up.
	if connector.manager.Resume("sid-1") {
		t.Fatal("an account this instance is running was queued for a resume")
	}
}

// The interval is what an account spends unowned after a restart, and the first tick is
// an interval away: a sweeper that waited for it would add half a minute of silence to
// every deploy, which is the case this exists for.
func TestTheResumeSweepMakesItsFirstPassOnTheWayIn(t *testing.T) {
	t.Parallel()

	connector, container, engine, _ := newResumeConnector(t)
	wantConnected(t, container, "sid-1", "5511999990001")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stopped := connector.resumeWanted(ctx)

	// Well inside the interval, so the only pass that can have run is the one the loop
	// makes before its first tick.
	waitFor(t, "the account to come back before the first tick", func() bool {
		account, ok := engine.Session("sid-1")
		return ok && account.Connected()
	})

	cancel()
	<-stopped
}

// An account somebody is already running is not brought back, and is not even asked
// about: the lease is read for the whole list in one batch, and a session that has one is
// out before the attempt. Without that, an instance in a fleet of several would try to
// acquire most of the fleet's accounts on every pass.
func TestTheResumeSweepLeavesAnAccountAPeerIsRunning(t *testing.T) {
	t.Parallel()

	connector, container, engine, server := newResumeConnector(t)
	wantConnected(t, container, "sid-1", "5511999990001")

	peer := cluster.NewLeases(redisx.Wrap(redis.NewClient(&redis.Options{Addr: server.Addr()}), "wa:", 8),
		"inst-b", cluster.Options{})
	if _, err := peer.Acquire(t.Context(), "sid-1"); err != nil {
		t.Fatalf("the peer could not take the account: %v", err)
	}

	connector.resumeOnce(t.Context())

	if _, ok := engine.Session("sid-1"); ok {
		t.Fatal("an account another instance is running was opened here as well, which is two sockets on one number")
	}
	if server.Exists(redisx.NewKeys("wa:", 8).Resume("sid-1")) {
		t.Fatal("the sweep spent a turn on an account that is plainly running; the lease filter is what keeps that traffic off the fleet")
	}
}

// The mark is what makes an account that cannot come back cost one attempt per window
// rather than one per instance per pass: a session that fails to connect is retired by
// its engine, the lease goes back, and the next pass would find it free again at once.
func TestTheResumeSweepWaitsOutTheMarkAnotherAttemptLeft(t *testing.T) {
	t.Parallel()

	connector, container, engine, server := newResumeConnector(t)
	// Two accounts, and the marked one is asked about first: the pass reads them in a
	// defined order and the adoptions are carried out in the order they were queued. So
	// the second one being back is proof that the first one was considered and skipped,
	// rather than proof that nothing has happened yet.
	wantConnected(t, container, "sid-a-cold", "5511999990001")
	wantConnected(t, container, "sid-b-warm", "5511999990002")
	server.Set(redisx.NewKeys("wa:", 8).Resume("sid-a-cold"), "inst-b")

	connector.resumeOnce(t.Context())

	waitFor(t, "the account with no mark to come back", func() bool {
		account, ok := engine.Session("sid-b-warm")
		return ok && account.Connected()
	})
	if _, ok := engine.Session("sid-a-cold"); ok {
		t.Fatal("an account somebody else had just tried was tried again inside the cool-off")
	}
}

// A restart of a large deployment is the case that decides the shape: every account is
// unowned at once, and an instance that took all of them would open hundreds of sockets
// to WhatsApp in the same second, on a process that has just started.
func TestTheResumeSweepAsksForOneBatchAtATime(t *testing.T) {
	t.Parallel()

	connector, container, _, server := newResumeConnector(t)
	for i := range resumeBatch + 3 {
		wantConnected(t, container, fmt.Sprintf("sid-%02d", i), fmt.Sprintf("55119999900%02d", i))
	}

	connector.resumeOnce(t.Context())

	// The marks are taken inside the pass, so counting them says what the pass asked for
	// without waiting on the goroutine that carries the adoptions out.
	asked := 0
	for i := range resumeBatch + 3 {
		if server.Exists(redisx.NewKeys("wa:", 8).Resume(fmt.Sprintf("sid-%02d", i))) {
			asked++
		}
	}
	if asked != resumeBatch {
		t.Fatalf("the pass asked for %d accounts, want %d: a restart would dial every account at once", asked, resumeBatch)
	}
}

// An account the fleet has agreed to leave alone is not asked for, and the backoff is
// the only thing that says so: the turn mark expires in a minute, so without this an
// account that can never connect costs the fleet an attempt a minute for the life of the
// deployment.
func TestTheResumeSweepLeavesAQuarantinedAccountAlone(t *testing.T) {
	t.Parallel()

	connector, container, engine, _ := newResumeConnector(t)
	// Two accounts again, the quarantined one first, so that the second coming back is
	// proof that the first was considered and skipped rather than proof that nothing has
	// happened yet.
	wantConnected(t, container, "sid-a-broken", "5511999990001")
	wantConnected(t, container, "sid-b-fine", "5511999990002")
	if _, err := connector.quarantine.Strike(t.Context(), "sid-a-broken"); err != nil {
		t.Fatalf("Strike: %v", err)
	}

	connector.resumeOnce(t.Context())

	waitFor(t, "the healthy account to come back", func() bool {
		account, ok := engine.Session("sid-b-fine")
		return ok && account.Connected()
	})
	if _, ok := engine.Session("sid-a-broken"); ok {
		t.Fatal("an account the fleet is waiting out was tried anyway; the backoff buys nothing")
	}
}

// newResumeConnector builds the instance the sweep runs on: a real store, a real lease
// set, and the fake engine, all over one Redis.
func newResumeConnector(t *testing.T) (*Connector, *store.Container, *fake.Engine, *miniredis.Miniredis) {
	t.Helper()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", 8)
	leases := cluster.NewLeases(client, "inst-a", cluster.Options{})
	engine := fake.New()

	quarantine := cluster.NewQuarantine(client, nil)
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: engine, Leases: leases,
		Publisher: quietPublisher{}, Replier: quietReplier{},
		Quarantine: quarantine,
		NewID:      func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })
	answering(t, manager)

	container := openTestStore(t)
	connector := &Connector{
		cfg: Config{Instance: "inst-a"}, log: zerolog.Nop(),
		client: client, leases: leases, quarantine: quarantine, manager: manager, store: container,
	}
	return connector, container, engine, server
}

// wantConnected pairs a session and records that a client asked for it to be connected,
// which is the state the sweep reads.
func wantConnected(t *testing.T, container *store.Container, sid, phone string) {
	t.Helper()

	jid, err := waTypes.ParseJID(phone + ":12@" + waTypes.DefaultUserServer)
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	device := container.Devices().NewDevice()
	device.ID = &jid
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details: make([]byte, 32), AccountSignature: make([]byte, 64),
		AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64),
	}
	if err := container.Devices().PutDevice(t.Context(), device); err != nil {
		t.Fatalf("PutDevice: %v", err)
	}
	if err := container.For(sid).Bind(t.Context(), jid); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := container.For(sid).PutDesired(t.Context(), store.DesiredConnected); err != nil {
		t.Fatalf("PutDesired: %v", err)
	}
}
