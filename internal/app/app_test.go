package app_test

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/app"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
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
	connector := start(t, server.Addr(), "inst-a", map[string]string{"WAC_LEASE_TTL": "1s", "WAC_CLAIM_MIN_IDLE": "1500ms"})
	waitFor(t, "the pending wake to be reclaimed and acted on", func() bool {
		return connector.Sessions() == 1
	})
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
		"WAC_LEASE_TTL": "3s", "WAC_CLAIM_MIN_IDLE": "3500ms",
	})
	waitFor(t, "the session to be adopted", func() bool { return connector.Sessions() == 1 })

	if reply := c.await(ctx, "connect-new", 10*time.Second); !reply.OK {
		t.Fatalf("the connect was refused: %+v", reply)
	}

	// Held past the reclaim delay: without the drain, the abandoned disconnect comes
	// back on a later heartbeat and undoes the connect that replaced it.
	deadline := time.Now().Add(5 * time.Second)
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
