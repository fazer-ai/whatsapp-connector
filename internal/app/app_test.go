package app_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/app"
	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/session"
	"github.com/fazer-ai/whatsapp-connector/internal/transport/redisstream"
)

// client is the Chatwoot side, reduced to what M0 has to prove: it wakes a session,
// sends a command and waits for the answer, and reads the event stream.
type client struct {
	t   *testing.T
	rdb *redis.Client
	key redisx.Keys
}

func newClient(t *testing.T, addr string) *client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	return &client{t: t, rdb: rdb, key: redisx.NewKeys("wa:", 8)}
}

func (c *client) send(ctx context.Context, stream string, command *protocol.Command) {
	c.t.Helper()
	fields, err := command.Fields()
	if err != nil {
		c.t.Fatalf("render command: %v", err)
	}
	values := make(map[string]any, len(fields))
	for key, value := range fields {
		values[key] = value
	}
	if err := c.rdb.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: values}).Err(); err != nil {
		c.t.Fatalf("XAdd: %v", err)
	}
}

// await blocks on the reply list the same way the Ruby side does.
func (c *client) await(ctx context.Context, commandID string, timeout time.Duration) protocol.Reply {
	c.t.Helper()
	answer, err := c.rdb.BLPop(ctx, timeout, c.key.Reply(commandID)).Result()
	if err != nil {
		c.t.Fatalf("BLPop for %s: %v", commandID, err)
	}
	var reply protocol.Reply
	if err := json.Unmarshal([]byte(answer[1]), &reply); err != nil {
		c.t.Fatalf("unmarshal reply: %v", err)
	}
	return reply
}

func (c *client) events(ctx context.Context, sid string) []protocol.Event {
	c.t.Helper()
	entries, err := c.rdb.XRange(ctx, c.key.EventsOf(sid), "-", "+").Result()
	if err != nil {
		c.t.Fatalf("XRange: %v", err)
	}
	events := make([]protocol.Event, 0, len(entries))
	for _, entry := range entries {
		fields := make(map[string]string, len(entry.Values))
		for key, value := range entry.Values {
			if text, ok := value.(string); ok {
				fields[key] = text
			}
		}
		event, parseErr := protocol.ParseEvent(fields)
		if parseErr != nil {
			c.t.Fatalf("parse event: %v", parseErr)
		}
		events = append(events, event)
	}
	return events
}

// start runs one connector against the given Redis and stops it when the test ends.
func start(t *testing.T, addr, instance string, env map[string]string) *app.Connector {
	t.Helper()
	t.Setenv("REDIS_URL", "redis://"+addr)
	t.Setenv("WAC_INSTANCE", instance)
	t.Setenv("WAC_ENGINE", "fake")
	// Port 0 so two instances in one test do not fight over a port, and a short
	// heartbeat so a lease renewal happens within the test's lifetime.
	t.Setenv("WAC_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("WAC_HEARTBEAT", "200ms")
	for name, value := range env {
		t.Setenv(name, value)
	}

	cfg, err := app.LoadConfig("test-host")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	connector, err := app.New(&cfg, zerolog.New(os.Stderr).Level(zerolog.ErrorLevel))
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = connector.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the connector did not shut down")
		}
	})
	return connector
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// The M0 milestone, end to end: a client wakes a session, the fleet picks it up, a
// command comes back answered, and the events land on the session's shard in order.
func TestFleetAdoptsASessionAndAnswersACommand(t *testing.T) {
	server := miniredis.RunT(t)
	connector := start(t, server.Addr(), "inst-a", nil)
	c := newClient(t, server.Addr())
	ctx := context.Background()

	const sid = "2f1c6f0e-0000-4000-8000-000000000001"
	c.send(ctx, c.key.Control(), &protocol.Command{
		V: protocol.Version, ID: "wake-1", Type: protocol.CommandSessionWake, SID: sid,
		TS: time.Now().UnixMilli(), Payload: json.RawMessage(`{"desired":"connected"}`),
	})
	waitFor(t, "the session to be adopted", func() bool { return connector.Sessions() == 1 })

	c.send(ctx, c.key.Commands(sid), &protocol.Command{
		V: protocol.Version, ID: "status-1", Type: protocol.CommandSessionStatus, SID: sid,
		TS: time.Now().UnixMilli(), ReplyTo: "status-1", Payload: json.RawMessage(`{}`),
	})

	reply := c.await(ctx, "status-1", 10*time.Second)
	if !reply.OK {
		t.Fatalf("reply = %+v, want ok", reply)
	}
	if reply.ID != "status-1" {
		t.Errorf("reply id = %q, want status-1", reply.ID)
	}
}

// Ownership is exclusive, and a second instance must not run a session the first one
// holds: two live sockets on one WhatsApp account is what the lease exists to stop.
func TestOnlyOneInstanceRunsASession(t *testing.T) {
	server := miniredis.RunT(t)
	first := start(t, server.Addr(), "inst-a", nil)
	second := start(t, server.Addr(), "inst-b", nil)
	c := newClient(t, server.Addr())

	const sid = "2f1c6f0e-0000-4000-8000-000000000002"
	c.send(context.Background(), c.key.Control(), &protocol.Command{
		V: protocol.Version, ID: "wake-2", Type: protocol.CommandSessionWake, SID: sid,
		TS: time.Now().UnixMilli(), Payload: json.RawMessage(`{"desired":"connected"}`),
	})

	waitFor(t, "one of the two to adopt the session", func() bool {
		return first.Sessions()+second.Sessions() == 1
	})
	// Held for a few heartbeats: a second instance that adopts late would show up as
	// the total climbing to two after the first tick.
	time.Sleep(600 * time.Millisecond)
	if total := first.Sessions() + second.Sessions(); total != 1 {
		t.Fatalf("%d instances run the session, want exactly 1", total)
	}
}

// A command nobody owns has to stay pending rather than be swallowed by whichever
// instance happened to read it.
func TestCommandForAnUnownedSessionIsNotAnswered(t *testing.T) {
	server := miniredis.RunT(t)
	start(t, server.Addr(), "inst-a", nil)
	c := newClient(t, server.Addr())
	ctx := context.Background()

	const sid = "2f1c6f0e-0000-4000-8000-000000000003"
	c.send(ctx, c.key.Commands(sid), &protocol.Command{
		V: protocol.Version, ID: "orphan-1", Type: protocol.CommandSessionStatus, SID: sid,
		TS: time.Now().UnixMilli(), ReplyTo: "orphan-1", Payload: json.RawMessage(`{}`),
	})

	// BLPop returning nothing within the window is the assertion: the command was left
	// for the instance that will own the session.
	if _, err := c.rdb.BLPop(ctx, 500*time.Millisecond, c.key.Reply("orphan-1")).Result(); err == nil {
		t.Fatal("a command for a session nobody owns was answered")
	}
}

func TestPublishedEventsCarryTheOwnersEpochInOrder(t *testing.T) {
	server := miniredis.RunT(t)
	connector := start(t, server.Addr(), "inst-a", nil)
	c := newClient(t, server.Addr())
	ctx := context.Background()

	const sid = "2f1c6f0e-0000-4000-8000-000000000004"
	c.send(ctx, c.key.Control(), &protocol.Command{
		V: protocol.Version, ID: "wake-4", Type: protocol.CommandSessionWake, SID: sid,
		TS: time.Now().UnixMilli(), Payload: json.RawMessage(`{"desired":"connected"}`),
	})
	waitFor(t, "the session to be adopted", func() bool { return connector.Sessions() == 1 })

	c.send(ctx, c.key.Commands(sid), &protocol.Command{
		V: protocol.Version, ID: "logout-1", Type: protocol.CommandSessionLogout, SID: sid,
		TS: time.Now().UnixMilli(), ReplyTo: "logout-1", Payload: json.RawMessage(`{}`),
	})
	c.await(ctx, "logout-1", 10*time.Second)

	waitFor(t, "an event on the session's shard", func() bool { return len(c.events(ctx, sid)) > 0 })
	for i, event := range c.events(ctx, sid) {
		if event.SID != sid {
			t.Errorf("event %d has sid %q, want %s", i, event.SID, sid)
		}
		if event.Epoch != 1 {
			t.Errorf("event %d has epoch %d, want the first owner's 1", i, event.Epoch)
		}
		if event.Seq != uint64(i+1) {
			t.Errorf("event %d has seq %d, want %d", i, event.Seq, i+1)
		}
		if event.Inst != "inst-a" {
			t.Errorf("event %d has inst %q, want inst-a", i, event.Inst)
		}
	}
}

// A fleet whose shard count disagrees hashes sessions onto streams nobody reads them
// from, and there is no recovering from that after the fact.
func TestAnInstanceWithADifferentShardCountRefusesToStart(t *testing.T) {
	server := miniredis.RunT(t)
	start(t, server.Addr(), "inst-a", nil)

	t.Setenv("WAC_EVENT_SHARDS", "16")
	cfg, err := app.LoadConfig("test-host")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if _, err := app.New(&cfg, zerolog.Nop()); err == nil {
		t.Fatal("an instance with a different shard count started")
	}
}

// A whatsmeow engine with nowhere to keep a pairing is a deployment that asks every
// session to scan a QR code on every restart while reporting itself healthy. Refusing
// at startup is what turns that into a message an operator sees once.
func TestTheRealEngineRefusesToStartWithoutADatabase(t *testing.T) {
	t.Setenv("WAC_ENGINE", "whatsmeow")
	t.Setenv("WAC_DATABASE_URL", "")

	if _, err := app.LoadConfig("connector-test"); err == nil {
		t.Fatal("the whatsmeow engine started with no database url")
	}

	t.Setenv("WAC_DATABASE_URL", "sqlite:wa.db")
	cfg, err := app.LoadConfig("connector-test")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.DatabaseURL != "sqlite:wa.db" {
		t.Fatalf("the config carries %q", cfg.DatabaseURL)
	}
}

// An instance that dies between reading a wake and acting on it leaves that wake in the
// group's pending list, where no later read will ever see it: `>` only returns entries
// nobody has taken. The same thing happens on purpose when this connector cannot adopt
// a woken session and leaves the wake for someone else. Either way, the session stays
// unowned until something claims the entry, so the loop has to.
func TestAWakeLeftPendingIsPickedUpAgain(t *testing.T) {
	server := miniredis.RunT(t)
	c := newClient(t, server.Addr())
	ctx := context.Background()

	const sid = "2f1c6f0e-0000-4000-8000-000000000009"
	control := c.key.Control()

	// The shape an instance leaves behind when it is killed mid-command: the group
	// exists, the wake was read by a consumer that is now gone, and nothing acked it.
	if err := c.rdb.XGroupCreateMkStream(ctx, control, redisstream.ConsumerGroup, "0").Err(); err != nil {
		t.Fatalf("create the group: %v", err)
	}
	c.send(ctx, control, &protocol.Command{
		V: protocol.Version, ID: "wake-pending", Type: protocol.CommandSessionWake, SID: sid,
		TS: time.Now().UnixMilli(), Payload: json.RawMessage(`{"desired":"connected"}`),
	})
	taken, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: redisstream.ConsumerGroup, Consumer: "inst-dead", Streams: []string{control, ">"}, Count: 10,
	}).Result()
	if err != nil {
		t.Fatalf("read as the instance that died: %v", err)
	}
	if len(taken) != 1 || len(taken[0].Messages) != 1 {
		t.Fatalf("the dead instance read %v, want one wake", taken)
	}

	// A live instance now starts. A plain read will never show it this entry.
	connector := start(t, server.Addr(), "inst-a", map[string]string{"WAC_LEASE_TTL": "7s", "WAC_CLAIM_MIN_IDLE": "7500ms"})
	waitFor(t, "the pending wake to be reclaimed and acted on", func() bool {
		return connector.Sessions() == 1
	})
}

// Fitting inside the lease is not enough on its own. Reading, dispatching and renewing
// are one goroutine, but everything outside the heartbeat branch is cut when the next
// renewal is due, so it cannot push a renewal by more than one period however many
// steps it is made of. What sits outside that deadline is the branch's own tail: the
// hand-backs, with their share of the lease, and a reclaim whose passes divide one
// heartbeat. One period plus that tail past the lease and every session on the
// instance is acquired by a peer while this one still holds their sockets open, which
// is the one thing the lease exists to prevent.
func TestAHeartbeatAndTheHandBackTailHaveToFitInsideTheLease(t *testing.T) {
	t.Setenv("WAC_LEASE_TTL", "30s")
	t.Setenv("WAC_CLAIM_MIN_IDLE", "45s")

	// A 30s lease is fresh for 28s of its life, the margin over again is held back as
	// the room the renewals and the announcement work in, and 10s belongs to the
	// hand-back tail: 16s is the first heartbeat that does not fit.
	for _, heartbeat := range []string{"16s", "25s"} {
		t.Setenv("WAC_HEARTBEAT", heartbeat)
		if _, err := app.LoadConfig("connector-test"); err == nil {
			t.Fatalf("a %s heartbeat started against a 30s lease, so the tick that renews a lease "+
				"can come after it has expired", heartbeat)
		}
	}

	// And 15s starts: the bound is one period plus the tail against the fresh
	// lifetime less its working slack, not an enumeration of the loop's steps, which
	// is exactly what a check that billed the read and the batch to the gap on top of
	// the period used to refuse.
	t.Setenv("WAC_HEARTBEAT", "15s")
	if _, err := app.LoadConfig("connector-test"); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
}

// A wake is acknowledged when adoption finds the session already owned, because in a
// fleet that means somebody else is running it. That answer is only true while the
// owner is alive: an instance that reads a wake, wins the lease and dies before
// acknowledging leaves a lease that outlives it by up to the whole TTL, and a peer
// claiming inside that window retires the only wake there was. Refusing the
// configuration is what keeps the window from existing.
func TestTheReclaimDelayHasToOutlastALease(t *testing.T) {
	t.Setenv("WAC_LEASE_TTL", "30s")

	for _, idle := range []string{"29s", "30s"} {
		t.Setenv("WAC_CLAIM_MIN_IDLE", idle)
		if _, err := app.LoadConfig("connector-test"); err == nil {
			t.Fatalf("a reclaim delay of %s started against a 30s lease", idle)
		}
	}

	t.Setenv("WAC_CLAIM_MIN_IDLE", "31s")
	if _, err := app.LoadConfig("connector-test"); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	// And the default is derived rather than fixed, so raising the lease raises it too.
	os.Unsetenv("WAC_CLAIM_MIN_IDLE")
	t.Setenv("WAC_LEASE_TTL", "2m")
	cfg, err := app.LoadConfig("connector-test")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.ClaimMinIdle <= cfg.LeaseTTL {
		t.Fatalf("the default reclaim delay is %s against a %s lease", cfg.ClaimMinIdle, cfg.LeaseTTL)
	}
}

// Commands for one session are ordered by being on one stream read by one consumer. A
// command its previous owner read and abandoned is off that stream until somebody
// reclaims it, so an instance that adopts the session and reads `>` first hands over
// commands that arrived later and runs them out of order. The example is the one that
// costs an operator something: a `session.disconnect` landing behind the
// `session.connect` that replaced it leaves the account in the state nobody asked for.
func TestASessionIsDrainedBeforeAnythingNewerIsReadForIt(t *testing.T) {
	server := miniredis.RunT(t)
	c := newClient(t, server.Addr())
	ctx := context.Background()

	const sid = "2f1c6f0e-0000-4000-8000-00000000000a"
	commands := c.key.Commands(sid)

	// The previous owner read a disconnect and died before carrying it out.
	if err := c.rdb.XGroupCreateMkStream(ctx, commands, redisstream.ConsumerGroup, "0").Err(); err != nil {
		t.Fatalf("create the group: %v", err)
	}
	c.send(ctx, commands, &protocol.Command{
		V: protocol.Version, ID: "disconnect-old", Type: protocol.CommandSessionDisconnect, SID: sid,
		TS: time.Now().UnixMilli(), Payload: json.RawMessage(`{}`),
	})
	taken, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: redisstream.ConsumerGroup, Consumer: "inst-dead", Streams: []string{commands, ">"}, Count: 10,
	}).Result()
	if err != nil || len(taken) != 1 || len(taken[0].Messages) != 1 {
		t.Fatalf("the dead instance read %v (err=%v), want one command", taken, err)
	}

	// And then the operator reconnected, which is the command that must win.
	c.send(ctx, commands, &protocol.Command{
		V: protocol.Version, ID: "connect-new", Type: protocol.CommandSessionConnect, SID: sid,
		TS: time.Now().UnixMilli(), ReplyTo: "connect-new", Payload: json.RawMessage(`{"pairing":"resume"}`),
	})
	c.send(ctx, c.key.Control(), &protocol.Command{
		V: protocol.Version, ID: "wake-order", Type: protocol.CommandSessionWake, SID: sid,
		TS: time.Now().UnixMilli(), Payload: json.RawMessage(`{"desired":"connected"}`),
	})

	connector := start(t, server.Addr(), "inst-a", map[string]string{
		"WAC_LEASE_TTL": "7s", "WAC_CLAIM_MIN_IDLE": "7500ms",
	})
	waitFor(t, "the session to be adopted", func() bool { return connector.Sessions() == 1 })

	if reply := c.await(ctx, "connect-new", 10*time.Second); !reply.OK {
		t.Fatalf("the connect was refused: %+v", reply)
	}

	// Held past the reclaim delay: without the drain, the abandoned disconnect comes
	// back on a later heartbeat and undoes the connect that replaced it.
	deadline := time.Now().Add(9 * time.Second)
	for time.Now().Before(deadline) {
		c.send(ctx, commands, &protocol.Command{
			V: protocol.Version, ID: "status-" + strconv.FormatInt(time.Now().UnixNano(), 10),
			Type: protocol.CommandSessionStatus, SID: sid, TS: time.Now().UnixMilli(),
			ReplyTo: "status-check", Payload: json.RawMessage(`{}`),
		})
		reply := c.await(ctx, "status-check", 5*time.Second)
		if !reply.OK {
			t.Fatalf("session.status was refused: %+v", reply)
		}
		var status map[string]any
		if err := json.Unmarshal(reply.Result, &status); err != nil {
			t.Fatalf("unmarshal the status: %v", err)
		}
		if status["connection"] != "open" {
			t.Fatalf("connection=%v after the abandoned disconnect came back, want open", status["connection"])
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// One claim takes at most ReadCount entries per stream, so a session whose previous
// owner left more than that behind is only half drained by a single pass. Reading `>`
// after that half hands over commands that arrived later and runs them ahead of the
// rest, which is the same reordering as draining nothing at all — just harder to see.
func TestASessionWithALongBacklogIsDrainedBeforeItIsRead(t *testing.T) {
	server := miniredis.RunT(t)
	c := newClient(t, server.Addr())
	ctx := context.Background()

	const sid = "2f1c6f0e-0000-4000-8000-00000000000b"
	// More than one claim can take, so finishing the drain needs several passes.
	const backlog = redisstream.DefaultReadCount + 5
	// And the whole of it, plus the connect behind it, has to fit the session's queue
	// with the executor not having run at all: the loop offers everything a drain claims
	// before the executor is guaranteed a turn, and an offer past the bound is refused
	// `rate_limited`. When the two constants met exactly, this test held a margin of five
	// slots it never waited for, and lost it on a loaded runner -- a CI failure on a PR
	// that touched nothing in here. Checked loudly, so a change to either constant fails
	// on this line instead of narrowing the margin back into a race.
	if backlog+1 > session.DefaultQueueDepth {
		t.Fatalf("a backlog of %d and the connect behind it do not fit a queue of %d: "+
			"the test would be racing the executor for the missing slots", backlog, session.DefaultQueueDepth)
	}
	commands := c.key.Commands(sid)

	if err := c.rdb.XGroupCreateMkStream(ctx, commands, redisstream.ConsumerGroup, "0").Err(); err != nil {
		t.Fatalf("create the group: %v", err)
	}
	for i := range backlog {
		c.send(ctx, commands, &protocol.Command{
			V: protocol.Version, ID: "disconnect-" + strconv.Itoa(i), Type: protocol.CommandSessionDisconnect,
			SID: sid, TS: time.Now().UnixMilli(), Payload: json.RawMessage(`{}`),
		})
	}
	taken, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: redisstream.ConsumerGroup, Consumer: "inst-dead", Streams: []string{commands, ">"}, Count: backlog,
	}).Result()
	if err != nil || len(taken) != 1 || len(taken[0].Messages) != backlog {
		t.Fatalf("the dead instance read %v (err=%v), want %d commands", taken, err, backlog)
	}

	// The operator's reconnect, which arrived after every one of them.
	c.send(ctx, commands, &protocol.Command{
		V: protocol.Version, ID: "connect-after", Type: protocol.CommandSessionConnect, SID: sid,
		TS: time.Now().UnixMilli(), ReplyTo: "connect-after", Payload: json.RawMessage(`{"pairing":"resume"}`),
	})
	c.send(ctx, c.key.Control(), &protocol.Command{
		V: protocol.Version, ID: "wake-backlog", Type: protocol.CommandSessionWake, SID: sid,
		TS: time.Now().UnixMilli(), Payload: json.RawMessage(`{"desired":"connected"}`),
	})

	connector := start(t, server.Addr(), "inst-a", map[string]string{
		"WAC_LEASE_TTL": "7s", "WAC_CLAIM_MIN_IDLE": "7500ms",
	})
	waitFor(t, "the session to be adopted", func() bool { return connector.Sessions() == 1 })

	if reply := c.await(ctx, "connect-after", 15*time.Second); !reply.OK {
		t.Fatalf("the connect was refused: %+v", reply)
	}

	// Held past the reclaim delay: a disconnect left behind by the drain comes back on a
	// later heartbeat and undoes the connect that replaced it.
	deadline := time.Now().Add(9 * time.Second)
	for time.Now().Before(deadline) {
		c.send(ctx, commands, &protocol.Command{
			V: protocol.Version, ID: "status-" + strconv.FormatInt(time.Now().UnixNano(), 10),
			Type: protocol.CommandSessionStatus, SID: sid, TS: time.Now().UnixMilli(),
			ReplyTo: "backlog-check", Payload: json.RawMessage(`{}`),
		})
		reply := c.await(ctx, "backlog-check", 5*time.Second)
		if !reply.OK {
			t.Fatalf("session.status was refused: %+v", reply)
		}
		var status map[string]any
		if err := json.Unmarshal(reply.Result, &status); err != nil {
			t.Fatalf("unmarshal the status: %v", err)
		}
		if status["connection"] != "open" {
			t.Fatalf("connection=%v after a disconnect the drain left behind came back, want open", status["connection"])
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// A non-positive lease is accepted by cluster.NewLeases, which substitutes its own
// default, so the leases would go on working while every timing rule is checked
// against a value the cluster never uses. Each of these has to be refused by name,
// at startup, rather than surface as an arithmetic complaint about a duration that
// runs backwards.
func TestATimingThatCannotWorkIsRefusedAtStartup(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"a lease of no length": {
			"WAC_LEASE_TTL": "0s", "WAC_CLAIM_MIN_IDLE": "1s", "WAC_HEARTBEAT": "1s",
		},
		"a lease that runs backwards": {
			"WAC_LEASE_TTL": "-30s", "WAC_CLAIM_MIN_IDLE": "1s", "WAC_HEARTBEAT": "1s",
		},
		"a heartbeat of no length": {
			"WAC_LEASE_TTL": "30s", "WAC_CLAIM_MIN_IDLE": "45s", "WAC_HEARTBEAT": "0s",
		},
		"a heartbeat the lease cannot outlast": {
			"WAC_LEASE_TTL": "30s", "WAC_CLAIM_MIN_IDLE": "45s", "WAC_HEARTBEAT": "30s",
		},
		// A read waits a share of the heartbeat and Redis counts that wait in whole
		// milliseconds. Under one there is nothing to wait, so every read is skipped and
		// the instance ticks along alive without ever consuming a command or a wake.
		"a heartbeat that leaves no room to read": {
			"WAC_LEASE_TTL": "30s", "WAC_CLAIM_MIN_IDLE": "45s", "WAC_HEARTBEAT": "1ms",
		},
	} {
		t.Run(name, func(t *testing.T) {
			for key, value := range env {
				t.Setenv(key, value)
			}
			if _, err := app.LoadConfig("connector-test"); err == nil {
				t.Fatalf("%s started", name)
			}
		})
	}

	// And the timing a deployment actually runs still starts.
	for key, value := range map[string]string{
		"WAC_LEASE_TTL": "30s", "WAC_CLAIM_MIN_IDLE": "45s", "WAC_HEARTBEAT": "5s",
	} {
		t.Setenv(key, value)
	}
	if _, err := app.LoadConfig("connector-test"); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
}

// The media endpoint hands out message contents, and the token is the only thing in
// front of it. A deployment that sets a store and forgets the token would open it to
// anything that can reach the port, so it is refused at startup rather than served.
func TestAMediaStoreWithNoTokenIsRefused(t *testing.T) {
	t.Setenv("WAC_INSTANCE", "inst-a")
	t.Setenv("WAC_MEDIA_ROOT", t.TempDir())
	t.Setenv("WAC_MEDIA_TOKEN", "")

	if _, err := app.LoadConfig("host"); err == nil {
		t.Fatal("a media store with no token was accepted")
	}
}

// A size is what an operator sizing a volume is working in, so the suffixes are the
// powers of two and a value that is not a size fails at startup rather than falling back
// to a default nobody asked for.
func TestMediaSizesAreReadAsSizes(t *testing.T) {
	t.Setenv("WAC_INSTANCE", "inst-a")
	t.Setenv("WAC_MEDIA_ROOT", t.TempDir())
	t.Setenv("WAC_MEDIA_TOKEN", "s3cret")

	t.Setenv("WAC_MEDIA_QUOTA", "4 GiB")
	t.Setenv("WAC_MEDIA_MAX_BLOB", "50MiB")
	cfg, err := app.LoadConfig("host")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.MediaQuota != 4<<30 {
		t.Fatalf("the quota read as %d, want 4 GiB", cfg.MediaQuota)
	}
	if cfg.MediaMaxBlob != 50<<20 {
		t.Fatalf("the per-blob cap read as %d, want 50 MiB", cfg.MediaMaxBlob)
	}

	// The last one is what wraps the multiplication: positive going in, and negative or
	// a small positive coming out, which media.New then reads as unset and replaces with
	// a default nobody asked for.
	for _, bad := range []string{"lots", "-1", "0", "4GB", "4 gib", "9223372036854775807 GiB"} {
		t.Setenv("WAC_MEDIA_QUOTA", bad)
		if _, err := app.LoadConfig("host"); err == nil {
			t.Fatalf("WAC_MEDIA_QUOTA=%q was accepted", bad)
		}
	}
}

// A non-positive TTL parses as a duration and is then silently replaced by the store's
// own default, so a deployment that asked for something else keeps a day of blobs and is
// told nothing. The setting is honoured or the instance does not start.
func TestANonPositiveMediaTTLIsRefused(t *testing.T) {
	t.Setenv("WAC_INSTANCE", "inst-a")
	t.Setenv("WAC_MEDIA_ROOT", t.TempDir())
	t.Setenv("WAC_MEDIA_TOKEN", "s3cret")

	for _, bad := range []string{"0s", "-1h"} {
		t.Setenv("WAC_MEDIA_TTL", bad)
		if _, err := app.LoadConfig("host"); err == nil {
			t.Fatalf("WAC_MEDIA_TTL=%q was accepted", bad)
		}
	}
}

// Keeping how to fetch a file again for less time than the blob itself lives leaves a
// window where the reference has lapsed and the message cannot be recovered either,
// which is the exact failure the retention exists to prevent.
func TestARefetchRetentionShorterThanTheBlobsThemselvesIsRefused(t *testing.T) {
	t.Setenv("WAC_INSTANCE", "inst-a")
	t.Setenv("WAC_MEDIA_ROOT", t.TempDir())
	t.Setenv("WAC_MEDIA_TOKEN", "s3cret")
	t.Setenv("WAC_MEDIA_TTL", "48h")
	t.Setenv("WAC_MEDIA_REFETCH_TTL", "24h")

	if _, err := app.LoadConfig("host"); err == nil {
		t.Fatal("a retention shorter than the blobs it outlasts was accepted")
	}
}

// An upgrade must not refuse a deployment over a setting it has never heard of. An
// operator keeping blobs for longer than the default retention was configured correctly
// before this variable existed, and a fixed fallback would have the relation check below
// stop their connector from starting.
func TestADeploymentKeepingBlobsLongerThanTheDefaultRetentionStillStarts(t *testing.T) {
	t.Setenv("WAC_INSTANCE", "inst-a")
	t.Setenv("WAC_MEDIA_ROOT", t.TempDir())
	t.Setenv("WAC_MEDIA_TOKEN", "s3cret")
	t.Setenv("WAC_MEDIA_TTL", "720h")

	cfg, err := app.LoadConfig("host")
	if err != nil {
		t.Fatalf("a deployment that was valid before this setting existed was refused: %v", err)
	}
	if cfg.MediaRefetch < cfg.MediaTTL {
		t.Fatalf("blobs are kept %s and their coordinates %s, which lapses first", cfg.MediaTTL, cfg.MediaRefetch)
	}
}

// And a deployment that keeps blobs for less than the default retention keeps the
// default, rather than following the TTL down to something shorter than a week.
func TestAShortBlobTTLDoesNotDragTheRetentionDownWithIt(t *testing.T) {
	t.Setenv("WAC_INSTANCE", "inst-a")
	t.Setenv("WAC_MEDIA_ROOT", t.TempDir())
	t.Setenv("WAC_MEDIA_TOKEN", "s3cret")
	t.Setenv("WAC_MEDIA_TTL", "1h")

	cfg, err := app.LoadConfig("host")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.MediaRefetch != app.DefaultMediaRefetch {
		t.Fatalf("the retention is %s, want the default %s", cfg.MediaRefetch, app.DefaultMediaRefetch)
	}
}

// The same reasoning as the media TTL: a non-positive duration parses, and a retention
// nobody honours is one an operator finds out about from a message that lost its file.
func TestANonPositiveRefetchRetentionIsRefused(t *testing.T) {
	t.Setenv("WAC_INSTANCE", "inst-a")
	t.Setenv("WAC_MEDIA_ROOT", t.TempDir())
	t.Setenv("WAC_MEDIA_TOKEN", "s3cret")

	for _, bad := range []string{"0s", "-1h"} {
		t.Setenv("WAC_MEDIA_REFETCH_TTL", bad)
		if _, err := app.LoadConfig("host"); err == nil {
			t.Fatalf("WAC_MEDIA_REFETCH_TTL=%q was accepted", bad)
		}
	}
}

// Nothing waits on this sweep to free anything, so it is bounded loosely on both ends:
// often enough that a row does not outlive its retention by much, rarely enough that a
// fleet does not spend its day deleting nothing from the same table.
func TestTheRefetchSweepCadenceIsBoundedAtBothEnds(t *testing.T) {
	t.Parallel()

	cases := map[time.Duration]time.Duration{
		7 * 24 * time.Hour:  time.Hour,
		30 * 24 * time.Hour: time.Hour,
		2 * time.Hour:       6 * time.Minute,
		10 * time.Minute:    time.Minute,
		time.Second:         time.Minute,
	}
	for ttl, want := range cases {
		if got := app.PartSweep(ttl); got != want {
			t.Fatalf("a %s retention is swept every %s, want %s", ttl, got, want)
		}
	}
}

// One blob larger than the whole budget evicts the cache and then itself, which is a
// deployment that looks configured and caches nothing.
func TestAPerBlobCapLargerThanTheQuotaIsRefused(t *testing.T) {
	t.Setenv("WAC_INSTANCE", "inst-a")
	t.Setenv("WAC_MEDIA_ROOT", t.TempDir())
	t.Setenv("WAC_MEDIA_TOKEN", "s3cret")
	t.Setenv("WAC_MEDIA_QUOTA", "10MiB")
	t.Setenv("WAC_MEDIA_MAX_BLOB", "100MiB")

	if _, err := app.LoadConfig("host"); err == nil {
		t.Fatal("a per-blob cap larger than the whole quota was accepted")
	}
}

// The run loop has an exit the context knows nothing about: an HTTP server that cannot
// listen ends it while nothing has cancelled anything. A sweeper watching only that
// context would still be ticking, and waiting for it hangs the process on exactly the
// startup failure it is trying to report.
func TestAStartupFailureIsReportedRatherThanHungOn(t *testing.T) {
	var listener net.ListenConfig
	occupied, err := listener.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = occupied.Close() })

	server := miniredis.RunT(t)
	t.Setenv("WAC_INSTANCE", "inst-a")
	t.Setenv("REDIS_URL", "redis://"+server.Addr())
	t.Setenv("WAC_HTTP_ADDR", occupied.Addr().String())
	t.Setenv("WAC_MEDIA_ROOT", t.TempDir())
	t.Setenv("WAC_MEDIA_TOKEN", "s3cret")

	cfg, err := app.LoadConfig("host")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	connector, err := app.New(&cfg, zerolog.Nop())
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}

	// Background rather than a cancellable context, which is what the binary passes:
	// the point is that nothing cancels this and the run still has to come back.
	done := make(chan error, 1)
	go func() { done <- connector.Run(context.Background()) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a connector that could not listen reported a clean run")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the run never came back: a startup failure is being waited on rather than reported")
	}
}

// A fixed cadence ignores a TTL shorter than itself: a deployment asking to keep media
// for a second would keep it for a minute, which is the opposite of what a short
// retention is set for. The walk costs real time on a large cache, so it follows the TTL
// down only to a floor and never above a minute.
func TestTheSweepCadenceFollowsTheRetention(t *testing.T) {
	t.Parallel()

	cases := map[time.Duration]time.Duration{
		24 * time.Hour:   time.Minute,
		10 * time.Minute: time.Minute,
		time.Minute:      30 * time.Second,
		10 * time.Second: 5 * time.Second,
		time.Second:      time.Second,
		time.Millisecond: time.Second,
	}
	for ttl, want := range cases {
		if got := app.BlobSweep(ttl); got != want {
			t.Fatalf("a %s retention is swept every %s, want %s", ttl, got, want)
		}
	}
}

// The media endpoint reaches a client only if every piece between the setting and the
// socket is wired: the store is built, the handler is given the token, and the routes are
// registered. Each of those is covered on its own; this is the one that would notice a
// connector that reads WAC_MEDIA_ROOT and serves nothing.
func TestTheMediaEndpointIsServedByARunningConnector(t *testing.T) {
	server := miniredis.RunT(t)
	t.Setenv("WAC_INSTANCE", "inst-a")
	t.Setenv("REDIS_URL", "redis://"+server.Addr())
	t.Setenv("WAC_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("WAC_MEDIA_ROOT", t.TempDir())
	t.Setenv("WAC_MEDIA_TOKEN", "s3cret")

	cfg, err := app.LoadConfig("host")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	connector, err := app.New(&cfg, zerolog.Nop())
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}

	// The mux the running server serves, exercised without a socket: a port of zero is
	// only known after Start, and what is being checked is the wiring rather than the
	// listener.
	handler := connector.Handler()
	const blob = "/media/blob_000102030405060708090a0b"

	withToken := httptest.NewRecorder()
	authorized := httptest.NewRequestWithContext(t.Context(), http.MethodGet, blob, http.NoBody)
	authorized.Header.Set("Authorization", "Bearer s3cret")
	handler.ServeHTTP(withToken, authorized)
	if withToken.Code != http.StatusNotFound {
		t.Fatalf("a blob that is not there answered %d, want 404: the route is not reaching the store", withToken.Code)
	}

	withoutToken := httptest.NewRecorder()
	handler.ServeHTTP(withoutToken, httptest.NewRequestWithContext(t.Context(), http.MethodGet, blob, http.NoBody))
	if withoutToken.Code != http.StatusUnauthorized {
		t.Fatalf("an unauthenticated request answered %d, want 401: the token is not reaching the handler", withoutToken.Code)
	}
}

// The bind host and the advertised host answer different questions: one says which
// interface to accept on, the other says how the rest of the deployment reaches this
// instance. Pasting the first into the second built `http://<instance>127.0.0.1:8080`
// for anything but the two spellings of "every interface", and a blob is published under
// it after the message has been acknowledged, so what that costs is the file.
func TestTheAdvertisedAddressIsDerivedFromThePortAndNotFromTheBindHost(t *testing.T) {
	for _, tc := range []struct {
		addr string
		want string
	}{
		{":8080", "http://inst-a:8080"},
		{"0.0.0.0:8080", "http://inst-a:8080"},
		{"127.0.0.1:8080", "http://inst-a:8080"},
		{"[::]:8080", "http://inst-a:8080"},
		{"[::1]:9000", "http://inst-a:9000"},
	} {
		t.Run(tc.addr, func(t *testing.T) {
			t.Setenv("WAC_INSTANCE", "inst-a")
			t.Setenv("WAC_HTTP_ADDR", tc.addr)

			cfg, err := app.LoadConfig("host")
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			if cfg.AdvertiseURL != tc.want {
				t.Fatalf("binding %s advertises %q, want %q", tc.addr, cfg.AdvertiseURL, tc.want)
			}
		})
	}

	// An address with no port at all is a deployment that will not listen either, and
	// deriving something from it would only move the failure to the first media message.
	t.Run("an address that is not one", func(t *testing.T) {
		t.Setenv("WAC_INSTANCE", "inst-a")
		t.Setenv("WAC_HTTP_ADDR", "8080")

		if _, err := app.LoadConfig("host"); err == nil {
			t.Fatal("an address with no port was accepted")
		}
	})

	// A colon with nothing after it is worse than an address that is not one: it splits
	// without complaint and a listener takes any free port, so the instance comes up,
	// answers on a port nobody was told about, and publishes blobs under a bare colon
	// that every client reads as port 80.
	for _, addr := range []string{":", "127.0.0.1:"} {
		t.Run("a colon with no port after it: "+addr, func(t *testing.T) {
			t.Setenv("WAC_INSTANCE", "inst-a")
			t.Setenv("WAC_HTTP_ADDR", addr)

			if _, err := app.LoadConfig("host"); err == nil {
				t.Fatal("an address a listener answers on any free port was accepted")
			}
		})
	}

	// An instance addressed by an IPv6 literal is written in brackets. Pasting a colon
	// in builds `http://2001:db8::1:8080`, which parses -- as that host and that port --
	// and is a URL no client can dial.
	t.Run("an instance addressed by an IPv6 literal", func(t *testing.T) {
		t.Setenv("WAC_INSTANCE", "2001:db8::1")
		t.Setenv("WAC_HTTP_ADDR", ":8080")

		cfg, err := app.LoadConfig("host")
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}
		if cfg.AdvertiseURL != "http://[2001:db8::1]:8080" {
			t.Fatalf("an IPv6 instance advertises %q, want it in brackets", cfg.AdvertiseURL)
		}
	})

	// And an address the operator gave is taken as given: it is the one they can say
	// something the derivation cannot know, such as the host in front of the fleet.
	t.Run("one the operator said", func(t *testing.T) {
		t.Setenv("WAC_INSTANCE", "inst-a")
		t.Setenv("WAC_HTTP_ADDR", "127.0.0.1:8080")
		t.Setenv("WAC_ADVERTISE_URL", "https://gateway.internal/inst-a")

		cfg, err := app.LoadConfig("host")
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}
		if cfg.AdvertiseURL != "https://gateway.internal/inst-a" {
			t.Fatalf("the operator's own address became %q", cfg.AdvertiseURL)
		}
	})
}

// The store has two consumers and the endpoint is only one of them: a session downloads
// the file of an inbound message straight into it, which is why the store is built
// before the engine rather than after it.
//
// Nothing observable from out here separates an engine that was handed the store from
// one that was not, except the one refusal the engine has: a store it can publish no
// address for. So that refusal is what says the store reached it, and the connector
// built the other way round is the control that says the refusal is not something else.
func TestTheBlobStoreReachesTheEngineAndNotOnlyTheEndpoint(t *testing.T) {
	server := miniredis.RunT(t)
	t.Setenv("WAC_INSTANCE", "inst-a")
	t.Setenv("REDIS_URL", "redis://"+server.Addr())
	t.Setenv("WAC_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("WAC_MEDIA_ROOT", t.TempDir())
	t.Setenv("WAC_MEDIA_TOKEN", "s3cret")
	t.Setenv("WAC_ENGINE", "whatsmeow")
	t.Setenv("WAC_DATABASE_URL", "sqlite:"+filepath.Join(t.TempDir(), "wa.db"))
	// Said rather than derived, because the bind port here is zero: the listener reads
	// that as any free port, and a blob published under it names one nobody can come
	// back to.
	t.Setenv("WAC_ADVERTISE_URL", "http://inst-a:8080")

	cfg, err := app.LoadConfig("host")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if _, err := app.New(&cfg, zerolog.Nop()); err != nil {
		t.Fatalf("a connector with everything the media path needs was refused: %v", err)
	}

	// Blanked rather than left unset, because LoadConfig derives one for a deployment
	// that gives none. A blob lives on the disk of whoever downloaded it, so a session
	// that cannot name its own instance has no reference to hand a client.
	//
	// On a database of its own: the two connectors each open a pool, and SQLite holds
	// one writer per file however many pools ask.
	cfg.AdvertiseURL = ""
	cfg.DatabaseURL = "sqlite:" + filepath.Join(t.TempDir(), "wa.db")
	if _, err := app.New(&cfg, zerolog.Nop()); err == nil {
		t.Fatal("a connector was built whose sessions hold a store they can publish no address for")
	}
}

// And the other way: a connector with no media root serves no media at all, so a client
// that reaches the wrong instance hears 404 rather than a 401 it would read as an
// operational problem to escalate.
func TestAConnectorWithNoMediaRootServesNoMedia(t *testing.T) {
	server := miniredis.RunT(t)
	t.Setenv("WAC_INSTANCE", "inst-a")
	t.Setenv("REDIS_URL", "redis://"+server.Addr())
	t.Setenv("WAC_MEDIA_ROOT", "")

	cfg, err := app.LoadConfig("host")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	connector, err := app.New(&cfg, zerolog.Nop())
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}

	rec := httptest.NewRecorder()
	connector.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(
		t.Context(), http.MethodGet, "/media/blob_000102030405060708090a0b", http.NoBody))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("an instance with no media store answered %d, want 404", rec.Code)
	}
}

// Sending does not go through the blob cache, so an instance told to keep nothing still
// has to be able to send. A cap that only existed alongside a media root would make
// every such instance refuse every file, which is a deployment that looks configured and
// cannot send an attachment.
func TestTheSendCapDoesNotDependOnHavingABlobStore(t *testing.T) {
	t.Setenv("WAC_INSTANCE", "inst-a")

	cfg, err := app.LoadConfig("host")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.MediaRoot != "" {
		t.Fatal("this test is about an instance with no media root, and it has one")
	}
	if cfg.MediaSendMax != media.DefaultSendMax {
		t.Fatalf("the send cap read as %d with no media root, want %d", cfg.MediaSendMax, media.DefaultSendMax)
	}

	t.Setenv("WAC_MEDIA_SEND_MAX", "16 MiB")
	cfg, err = app.LoadConfig("host")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.MediaSendMax != 16<<20 {
		t.Fatalf("the send cap read as %d, want 16 MiB", cfg.MediaSendMax)
	}
}

// Zero is not "no limit": it is an instance that refuses every file it is asked to send,
// which is a setting an operator would find out about from an agent whose attachment
// never goes out.
func TestANonPositiveSendCapIsRefused(t *testing.T) {
	t.Setenv("WAC_INSTANCE", "inst-a")

	for _, bad := range []string{"0", "-1", "lots", "16MB"} {
		t.Setenv("WAC_MEDIA_SEND_MAX", bad)
		if _, err := app.LoadConfig("host"); err == nil {
			t.Fatalf("WAC_MEDIA_SEND_MAX=%q was accepted", bad)
		}
	}
}
