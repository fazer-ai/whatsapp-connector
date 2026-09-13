package redisstream_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx/redisxtest"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
	"github.com/fazer-ai/whatsapp-connector/internal/transport/redisstream"
)

// A read Redis carried out and whose answer never reached this process is the case these
// tests are about. XREADGROUP moves what it answers with into the consumer's pending list
// before the answer leaves the server, so a lost answer leaves commands pending under
// this instance's name that this process never saw. `>` will not return them again, since
// somebody has taken them, and a claim will not look at them for a whole ClaimMinIdle: 45
// seconds with the defaults, during which newer commands for the same session are read
// and run ahead of them (#202).
//
// The answer goes missing in two ways, and they fail differently. Held past the window,
// the read comes back with the deadline's error. Dropped with the connection, go-redis
// sends the read again on a fresh one, gets nothing, and the read comes back empty with
// no error at all -- so nothing that waits to see a failure can be what recovers them.

// cutClaimMinIdle is long enough that nothing in these tests can come back through a
// claim. Whatever returns, returned through a read.
const cutClaimMinIdle = 30 * time.Second

// cutWindow is the heartbeat the app tests run with, which is the deadline a read gets.
const cutWindow = 200 * time.Millisecond

// cutFleet reaches Redis two ways: the transport under test through a proxy the test can
// interfere with, and the test itself directly, so that writing a command or looking at
// the pending list is never what the proxy catches.
type cutFleet struct {
	fleet
	proxy *redisxtest.Proxy
	via   *redisx.Client
	fake  bool
}

// cutBackends runs a test against miniredis and, when one is named, against a real Redis:
// reading a consumer's own pending history is exactly the kind of semantics a double can
// get subtly wrong, and production runs the real thing.
func cutBackends(t *testing.T, run func(t *testing.T, f cutFleet)) {
	t.Helper()
	for _, backend := range []struct {
		name  string
		fleet func(t *testing.T) fleet
	}{
		{"miniredis", newFleet},
		{"redis", realFleet},
	} {
		t.Run(backend.name, func(t *testing.T) {
			direct := backend.fleet(t)
			// Everything the test's own client connects with -- credentials, database, TLS
			// -- except where it connects to. ContextTimeoutEnabled as redisx.New sets it:
			// without it the window would stop bounding the read at the socket, and the
			// answer could never be cut off.
			options := *direct.rdb.Options()
			proxy := redisxtest.Listen(t, options.Addr)
			options.Addr = proxy.Addr()
			options.ContextTimeoutEnabled = true
			rdb := redis.NewClient(&options)
			t.Cleanup(func() { _ = rdb.Close() })
			run(t, cutFleet{
				fleet: direct, proxy: proxy, via: redisx.Wrap(rdb, direct.client.Keys().Prefix(), shards),
				fake: backend.name == "miniredis",
			})
		})
	}
}

func (f cutFleet) streams(t *testing.T, instance string) *redisstream.Streams {
	t.Helper()
	return f.streamsReading(t, instance, 0)
}

// streamsWith is the transport under test with options of the test's own.
func (f cutFleet) streamsWith(t *testing.T, opts *redisstream.Options) *redisstream.Streams {
	t.Helper()
	streams, err := redisstream.New(f.via, *opts)
	if err != nil {
		t.Fatalf("redisstream.New: %v", err)
	}
	return streams
}

// streamsReading is streams taking at most count entries per stream a read.
func (f cutFleet) streamsReading(t *testing.T, instance string, count int64) *redisstream.Streams {
	t.Helper()
	return f.streamsWith(t, &redisstream.Options{
		Instance: instance, Block: 50 * time.Millisecond, ClaimMinIdle: cutClaimMinIdle, ReadCount: count,
	})
}

// read is one pass of the connector's loop: a read bounded by one window.
func read(t *testing.T, streams *redisstream.Streams, sids ...string) ([]transport.Delivery, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), cutWindow)
	defer cancel()
	return streams.Read(ctx, sids)
}

// loseTheAnswer has the next answer carrying marker go missing on its way to the
// transport, the way named, while the transport reads sids, and returns what that read
// handed out. It checks the stimulus rather than trusting it: the answer was caught, and
// unless the read already handed the command out, it sits pending under the reader's name
// -- a test that went on without that would pass on a read that simply never happened.
//
// Held past the window, the read fails and hands out nothing. Dropped with the connection,
// go-redis sends it again, and the same read may already hand the command out from the
// history it reads after.
func (f cutFleet) loseTheAnswer(t *testing.T, how string, streams *redisstream.Streams, instance, marker string, sids ...string) []transport.Delivery {
	t.Helper()

	var caught <-chan struct{}
	release := make(chan struct{})
	switch how {
	case "held past the window":
		caught = f.proxy.Hold(marker, release)
	case "dropped with the connection":
		caught = f.proxy.Drop(marker)
	default:
		t.Fatalf("no way to lose an answer called %q", how)
	}
	delivered, err := read(t, streams, sids...)
	close(release)

	select {
	case <-caught:
	default:
		t.Fatalf("the proxy never saw an answer carrying %s (read err=%v)", marker, err)
	}
	if how == "held past the window" && len(delivered) != 0 {
		t.Fatalf("the read whose answer was held past its window handed out %v (err=%v)", ids(delivered), err)
	}
	if !slices.Contains(ids(delivered), marker) {
		if holder := f.pendingUnder(t, marker, sids...); holder != instance {
			t.Fatalf("%s is pending under %q after its answer was lost, want %q", marker, holder, instance)
		}
	}
	return delivered
}

// writeCommandAt is writeCommand at an entry id of the test's choosing.
func writeCommandAt(t *testing.T, f fleet, stream, id string, cmd *protocol.Command) {
	t.Helper()
	fields, err := cmd.Fields()
	if err != nil {
		t.Fatalf("render command: %v", err)
	}
	values := make(map[string]any, len(fields))
	for key, value := range fields {
		values[key] = value
	}
	if err := f.client.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, ID: id, Values: values}).Err(); err != nil {
		t.Fatalf("XAdd %s: %v", id, err)
	}
}

// pendingUnder names the consumer a command is pending under, or "" when it is not pending.
func (f cutFleet) pendingUnder(t *testing.T, commandID string, sids ...string) string {
	t.Helper()
	ctx := context.Background()
	for _, stream := range f.streamsOf(sids...) {
		pending, err := f.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: stream, Group: redisstream.ConsumerGroup, Start: "-", End: "+", Count: 100,
		}).Result()
		if err != nil {
			t.Fatalf("XPENDING %s: %v", stream, err)
		}
		for _, entry := range pending {
			messages, err := f.rdb.XRange(ctx, stream, entry.ID, entry.ID).Result()
			if err != nil || len(messages) != 1 {
				t.Fatalf("XRANGE %s %s: %v", stream, entry.ID, err)
			}
			if messages[0].Values["id"] == commandID {
				return entry.Consumer
			}
		}
	}
	return ""
}

func (f cutFleet) streamsOf(sids ...string) []string {
	streams := []string{f.client.Keys().Control()}
	for _, sid := range sids {
		streams = append(streams, f.client.Keys().Commands(sid))
	}
	return streams
}

func ids(deliveries []transport.Delivery) []string {
	out := make([]string, 0, len(deliveries))
	for i := range deliveries {
		out = append(out, deliveries[i].Command.ID)
	}
	return out
}

func ackAll(t *testing.T, deliveries []transport.Delivery) {
	t.Helper()
	for i := range deliveries {
		if err := deliveries[i].Ack(context.Background()); err != nil {
			t.Fatalf("ack %s: %v", deliveries[i].Command.ID, err)
		}
	}
}

// A command whose read lost its answer is handed out by the very next read, once, and as a
// command read for the first time: nobody has run it and its sender is still waiting.
func TestACommandWhoseReadLostItsAnswerIsHandedOutByTheNextRead(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		for _, tc := range []struct {
			name, how string
			stream    func(f cutFleet) string
			command   *protocol.Command
		}{
			{
				name: "a session's command, its answer held past the window", how: "held past the window",
				stream:  func(f cutFleet) string { return f.client.Keys().Commands("s1") },
				command: command("cut-held", "s1", ""),
			},
			{
				name: "a session's command, its connection dropped", how: "dropped with the connection",
				stream:  func(f cutFleet) string { return f.client.Keys().Commands("s1") },
				command: command("cut-dropped", "s1", ""),
			},
			{
				// The control stream is in every read, and a wake lost there is a session that
				// runs nowhere for the whole delay.
				name: "a wake, its answer held past the window", how: "held past the window",
				stream: func(f cutFleet) string { return f.client.Keys().Control() },
				command: &protocol.Command{
					V: protocol.Version, ID: "cut-wake", Type: protocol.CommandSessionWake, SID: "s1",
					TS: 1787000000000, Payload: []byte(`{"desired":"connected"}`),
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				streams := f.streams(t, "inst-"+tc.command.ID)
				if _, err := read(t, streams, "s1"); err != nil {
					t.Fatalf("priming read: %v", err)
				}
				writeCommand(t, f.fleet, tc.stream(f), tc.command)
				delivered := f.loseTheAnswer(t, tc.how, streams, "inst-"+tc.command.ID, tc.command.ID, "s1")

				if len(delivered) == 0 {
					var err error
					if delivered, err = read(t, streams, "s1"); err != nil {
						t.Fatalf("the read after: %v", err)
					}
				}
				if got := ids(delivered); !slices.Equal(got, []string{tc.command.ID}) {
					t.Fatalf("by the read after the lost answer, handed out %v, want [%s]", got, tc.command.ID)
				}
				if delivered[0].Redelivered {
					t.Error("handed out as redelivered, want it read for the first time: its sender is still waiting on it")
				}
				ackAll(t, delivered)
				if holder := f.pendingUnder(t, tc.command.ID, "s1"); holder != "" {
					t.Fatalf("%s still pending under %q after its ack", tc.command.ID, holder)
				}
				if again, err := read(t, streams, "s1"); err != nil || len(again) != 0 {
					t.Fatalf("a later read handed out %v (err=%v), want nothing", ids(again), err)
				}
			})
		}
	})
}

// Per-session order is what one stream read by one consumer is for. The connect whose
// answer was lost has to run before the disconnect written after it, or the account ends
// connected when the operator's last word was to disconnect it.
func TestACommandWhoseReadLostItsAnswerRunsBeforeTheOneWrittenAfterIt(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		streams := f.streams(t, "inst-a")
		stream := f.client.Keys().Commands("s1")
		if _, err := read(t, streams, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		writeCommand(t, f.fleet, stream, command("order-connect", "s1", ""))
		f.loseTheAnswer(t, "held past the window", streams, "inst-a", "order-connect", "s1")
		writeCommand(t, f.fleet, stream, command("order-disconnect", "s1", ""))

		var order []string
		for range 5 {
			delivered, err := read(t, streams, "s1")
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			order = append(order, ids(delivered)...)
			ackAll(t, delivered)
		}
		if want := []string{"order-connect", "order-disconnect"}; !slices.Equal(order, want) {
			t.Fatalf("handed out %v, want %v", order, want)
		}
	})
}

// A read sent again after its connection died is a read of whatever is next. With one
// entry a read, the answer carrying the connect is dropped and the read go-redis sends in
// its place comes back with the disconnect behind it, without an error: handing out what
// that answer carried would run the disconnect first and leave the connect below every
// mark this process keeps, where only a claim finds it.
func TestAReadSentAgainDoesNotHandOutWhatArrivedAfterTheCommandItLost(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		streams := f.streamsReading(t, "inst-a", 1)
		stream := f.client.Keys().Commands("s1")
		if _, err := read(t, streams, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		writeCommand(t, f.fleet, stream, command("resent-connect", "s1", ""))
		writeCommand(t, f.fleet, stream, command("resent-disconnect", "s1", ""))
		order := ids(f.loseTheAnswer(t, "dropped with the connection", streams, "inst-a", "resent-connect", "s1"))

		for range 5 {
			delivered, err := read(t, streams, "s1")
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			order = append(order, ids(delivered)...)
			ackAll(t, delivered)
		}
		if want := []string{"resent-connect", "resent-disconnect"}; !slices.Equal(order, want) {
			t.Fatalf("handed out %v, want %v", order, want)
		}
	})
}

// The read that recovers a lost answer can lose its own. What it was recovering stays
// pending and is recovered by the read after, still ahead of what was written since.
func TestARecoveryThatLosesItsOwnAnswerIsTriedAgain(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		streams := f.streams(t, "inst-a")
		stream := f.client.Keys().Commands("s1")
		if _, err := read(t, streams, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		writeCommand(t, f.fleet, stream, command("twice-connect", "s1", ""))
		f.loseTheAnswer(t, "held past the window", streams, "inst-a", "twice-connect", "s1")
		writeCommand(t, f.fleet, stream, command("twice-disconnect", "s1", ""))
		f.loseTheAnswer(t, "held past the window", streams, "inst-a", "twice-connect", "s1")

		var order []string
		for range 5 {
			delivered, err := read(t, streams, "s1")
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			order = append(order, ids(delivered)...)
			ackAll(t, delivered)
		}
		if want := []string{"twice-connect", "twice-disconnect"}; !slices.Equal(order, want) {
			t.Fatalf("handed out %v, want %v", order, want)
		}
	})
}

// An instance whose reads have stopped arriving is the one a claim exists to route
// around: a wake it took and never saw has to reach a healthy peer once it has sat for the
// claim delay. Reading the history back must not stand in the way. A read that delivered
// the wake again each time would set its idle time back to zero on every attempt, answer
// lost or not, and the peer's claim would never find it old enough.
func TestReadingBackAWakeWhoseAnswersKeepGettingLostLeavesItForAPeer(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		// Longer than the held answer each lost read leaves behind, so a read that set the
		// wake's idle time back to zero would leave it too young for the peer.
		const claimDelay = 3 * cutWindow

		sick := f.streams(t, "inst-sick")
		if _, err := read(t, sick, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		wake := &protocol.Command{
			V: protocol.Version, ID: "starved-wake", Type: protocol.CommandSessionWake, SID: "s9",
			TS: 1787000000000, Payload: []byte(`{"desired":"connected"}`),
		}
		writeCommand(t, f.fleet, f.client.Keys().Control(), wake)
		delivered := time.Now()
		f.loseTheAnswer(t, "held past the window", sick, "inst-sick", "starved-wake", "s1")

		// Every read after it loses its answer too, and each one did reach the server: the
		// history it read carried the wake.
		for range 3 {
			f.loseTheAnswer(t, "held past the window", sick, "inst-sick", "starved-wake", "s1")
		}
		// The age is the subject: the wake has sat for the claim delay since `>` delivered it.
		if left := claimDelay - time.Since(delivered); left > 0 {
			time.Sleep(left)
		}

		peer, err := redisstream.New(f.client, redisstream.Options{Instance: "inst-peer", ClaimMinIdle: claimDelay})
		if err != nil {
			t.Fatalf("redisstream.New: %v", err)
		}
		claimed, err := peer.ClaimControl(context.Background())
		if err != nil {
			t.Fatalf("ClaimControl: %v", err)
		}
		if got := ids(claimed); !slices.Equal(got, []string{"starved-wake"}) {
			t.Fatalf("the healthy peer claimed %v, want the wake the sick instance never saw", got)
		}
	})
}

// A wake read back late is handed out late, and a claim goes by idle time. Nothing on the
// read path resets that age, so a wake older than ReadBackMaxAge is not handed out by the
// read that recovers it: handed out, it would be claimable by a peer while this instance is
// still adopting its session, and the peer, finding a live lease, would retire the only wake
// there was. It is left to a claim, which resets the age as it hands it out.
func TestAWakeReadBackOlderThanItsMaxAgeIsLeftToAClaim(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		const claimDelay = 400 * time.Millisecond

		adopter := f.streamsWith(t, &redisstream.Options{
			Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: claimDelay, ReadBackMaxAge: claimDelay / 4,
		})
		if _, err := read(t, adopter, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		writeCommand(t, f.fleet, f.client.Keys().Control(), &protocol.Command{
			V: protocol.Version, ID: "late-wake", Type: protocol.CommandSessionWake, SID: "s9",
			TS: 1787000000000, Payload: []byte(`{"desired":"connected"}`),
		})
		f.loseTheAnswer(t, "held past the window", adopter, "inst-a", "late-wake", "s1")
		// The age is the subject: the wake sits unseen here past the claim delay.
		time.Sleep(claimDelay + claimDelay/2)

		if delivered, err := read(t, adopter, "s1"); err != nil || len(delivered) != 0 {
			t.Fatalf("the read back handed out %v (err=%v), want the old wake left to a claim", ids(delivered), err)
		}
		peer, err := redisstream.New(f.client, redisstream.Options{Instance: "inst-peer", ClaimMinIdle: claimDelay})
		if err != nil {
			t.Fatalf("redisstream.New: %v", err)
		}
		claimed, err := peer.ClaimControl(context.Background())
		if err != nil || !slices.Equal(ids(claimed), []string{"late-wake"}) {
			t.Fatalf("the peer claimed %v (err=%v), want the wake nobody here handed out", ids(claimed), err)
		}
	})
}

// The age limit is for what a read recovers. A wake the same read's `>` carried was
// delivered a moment ago, and is handed out however small the limit: otherwise a tight
// claim delay would leave every wake to a claim.
func TestAWakeTheSameReadCarriedIsHandedOutWhateverItsMaxAge(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		streams := f.streamsWith(t, &redisstream.Options{
			Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: cutClaimMinIdle, ReadBackMaxAge: time.Millisecond,
		})
		if _, err := read(t, streams, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		writeCommand(t, f.fleet, f.client.Keys().Control(), &protocol.Command{
			V: protocol.Version, ID: "carried-wake", Type: protocol.CommandSessionWake, SID: "s9",
			TS: 1787000000000, Payload: []byte(`{"desired":"connected"}`),
		})
		// The answer to `>` is held inside the window, so the wake has aged well past the
		// limit by the time the same read reads it back.
		release := make(chan struct{})
		caught := f.proxy.Hold("carried-wake", release)
		time.AfterFunc(50*time.Millisecond, func() { close(release) })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		delivered, err := streams.Read(ctx, []string{"s1"})
		select {
		case <-caught:
		default:
			t.Fatal("the answer carrying the wake was never held")
		}
		if err != nil || !slices.Equal(ids(delivered), []string{"carried-wake"}) {
			t.Fatalf("handed out %v (err=%v), want the wake its own `>` carried", ids(delivered), err)
		}
	})
}

// The age limit is the control stream's alone. A peer claims a session's stream only once it
// holds the session's lease, so a session command recovered late is handed out whatever its
// age, and in order: leaving it to a claim is the reordering this change exists to undo.
func TestASessionCommandReadBackLateIsStillHandedOutInOrder(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		streams := f.streamsWith(t, &redisstream.Options{
			Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: cutClaimMinIdle, ReadBackMaxAge: time.Millisecond,
		})
		stream := f.client.Keys().Commands("s1")
		if _, err := read(t, streams, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		writeCommand(t, f.fleet, stream, command("aged-connect", "s1", ""))
		f.loseTheAnswer(t, "held past the window", streams, "inst-a", "aged-connect", "s1")
		writeCommand(t, f.fleet, stream, command("aged-disconnect", "s1", ""))

		var order []string
		for range 5 {
			delivered, err := read(t, streams, "s1")
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			order = append(order, ids(delivered)...)
			ackAll(t, delivered)
		}
		if want := []string{"aged-connect", "aged-disconnect"}; !slices.Equal(order, want) {
			t.Fatalf("handed out %v, want %v", order, want)
		}
	})
}

// Entry ids are unique within a stream, not across streams, and two written in the same
// millisecond on different streams share one. What the `>` carried on a session stream says
// nothing about the control stream's entry with the same id, which is still the wake a read
// lost long ago.
func TestAWakeSharingAnIDWithWhatTheReadCarriedIsStillLeftToAClaim(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		streams := f.streamsWith(t, &redisstream.Options{
			Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: cutClaimMinIdle, ReadBackMaxAge: cutWindow / 2,
		})
		if _, err := read(t, streams, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		const shared = "5-1"
		writeCommandAt(t, f.fleet, f.client.Keys().Control(), shared, &protocol.Command{
			V: protocol.Version, ID: "twin-wake", Type: protocol.CommandSessionWake, SID: "s9",
			TS: 1787000000000, Payload: []byte(`{"desired":"connected"}`),
		})
		f.loseTheAnswer(t, "held past the window", streams, "inst-a", "twin-wake", "s1")
		writeCommandAt(t, f.fleet, f.client.Keys().Commands("s1"), shared, command("twin-status", "s1", ""))

		delivered, err := read(t, streams, "s1")
		if err != nil || !slices.Equal(ids(delivered), []string{"twin-status"}) {
			t.Fatalf("handed out %v (err=%v), want only the session command the read carried", ids(delivered), err)
		}
	})
}

// Claims run on another goroutine than reads, and so do the acknowledgements of what they
// hand out. A read pages this consumer's history in one trip and decides what to skip after
// it answers, so whatever a claim does in between has to be visible to that decision.
func TestAReadRacingAClaimHandsOutNothingTheClaimDoes(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		adopter, dead := f.streams(t, "inst-a"), f.streams(t, "inst-dead")
		for _, streams := range []*redisstream.Streams{adopter, dead} {
			if _, err := read(t, streams, "s1"); err != nil {
				t.Fatalf("priming read: %v", err)
			}
		}
		stream := f.client.Keys().Commands("s1")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		t.Run("acknowledged while the page is on its way", func(t *testing.T) {
			writeCommand(t, f.fleet, stream, command("acked-claim", "s1", ""))
			if taken, err := read(t, dead, "s1"); err != nil || len(taken) != 1 {
				t.Fatalf("the peer read %v (err=%v), want the command", ids(taken), err)
			}
			claimed, err := adopter.ClaimSessions(ctx, []string{"s1"})
			if err != nil || !slices.Equal(ids(claimed), []string{"acked-claim"}) {
				t.Fatalf("claimed %v (err=%v), want the peer's command", ids(claimed), err)
			}

			release := make(chan struct{})
			caught := f.proxy.Hold("acked-claim", release)
			var delivered []transport.Delivery
			done := make(chan error, 1)
			go func() {
				var err error
				delivered, err = adopter.Read(ctx, []string{"s1"})
				done <- err
			}()
			<-caught
			if err := claimed[0].Ack(ctx); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			close(release)
			if err := <-done; err != nil || len(delivered) != 0 {
				t.Fatalf("the read handed out %v (err=%v), want nothing: the claim ran it", ids(delivered), err)
			}
		})

		t.Run("claimed while the page is on its way", func(t *testing.T) {
			writeCommand(t, f.fleet, stream, command("racing-claim", "s1", ""))
			if taken, err := read(t, dead, "s1"); err != nil || len(taken) != 1 {
				t.Fatalf("the peer read %v (err=%v), want the command", ids(taken), err)
			}

			// The claim's answer is held, so the claim has moved the command here and not yet
			// heard that it did.
			release := make(chan struct{})
			caught := f.proxy.Hold("racing-claim", release)
			var claimed []transport.Delivery
			done := make(chan error, 1)
			go func() {
				var err error
				claimed, err = adopter.ClaimSessions(ctx, []string{"s1"})
				done <- err
			}()
			<-caught
			delivered, err := adopter.Read(ctx, []string{"s1"})
			close(release)
			if claimErr := <-done; claimErr != nil || !slices.Equal(ids(claimed), []string{"racing-claim"}) {
				t.Fatalf("claimed %v (err=%v), want the peer's command", ids(claimed), claimErr)
			}
			if err != nil || len(delivered) != 0 {
				t.Fatalf("the read handed out %v (err=%v), want nothing: the claim hands it out", ids(delivered), err)
			}
			ackAll(t, claimed)
		})

		t.Run("claimed and acknowledged after the page was sent", func(t *testing.T) {
			// The command is pending here past the mark, from a read that lost its answer, and
			// the page carrying it is on its way when a claim takes it, runs it and
			// acknowledges it.
			writeCommand(t, f.fleet, stream, command("late-acked", "s1", ""))
			f.loseTheAnswer(t, "held past the window", adopter, "inst-a", "late-acked", "s1")

			release := make(chan struct{})
			caught := f.proxy.Hold("late-acked", release)
			var delivered []transport.Delivery
			done := make(chan error, 1)
			go func() {
				var err error
				delivered, err = adopter.Read(ctx, []string{"s1"})
				done <- err
			}()
			<-caught
			claimed, err := adopter.ClaimSessions(ctx, []string{"s1"})
			if err != nil || !slices.Equal(ids(claimed), []string{"late-acked"}) {
				t.Fatalf("claimed %v (err=%v), want the command the lost read left", ids(claimed), err)
			}
			ackAll(t, claimed)
			close(release)
			if err := <-done; err != nil || len(delivered) != 0 {
				t.Fatalf("the read handed out %v (err=%v), want nothing: the claim ran it", ids(delivered), err)
			}
		})
	})
}

// A claim whose answer is lost has still moved what it took here, reset its idle time, and
// handed nothing out. What it took stays a claim's: the next read does not hand it out, and a
// later claim does, as a redelivery. A read handing it out would present a command that has
// been round the pending list as one that just arrived, and a full session queue refuses and
// retires such a command on the strength of a caller who may have stopped listening. The same
// holds for what was kept apart before, by a claim that handed it out and had it given back.
func TestWhatAClaimMovedHereWithoutHearingItStaysAClaims(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		const claimDelay = cutWindow / 2

		adopter := f.streamsWith(t, &redisstream.Options{Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: claimDelay})
		dead := f.streams(t, "inst-dead")
		for _, streams := range []*redisstream.Streams{adopter, dead} {
			if _, err := read(t, streams, "s1"); err != nil {
				t.Fatalf("priming read: %v", err)
			}
		}
		stream := f.client.Keys().Commands("s1")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		abandon := func(t *testing.T, commandID string) {
			t.Helper()
			writeCommand(t, f.fleet, stream, command(commandID, "s1", ""))
			if taken, err := read(t, dead, "s1"); err != nil || len(taken) != 1 {
				t.Fatalf("the peer read %v (err=%v), want the command", ids(taken), err)
			}
		}
		stillAClaims := func(t *testing.T, commandID string, claim func(context.Context) ([]transport.Delivery, error)) {
			t.Helper()
			if holder := f.pendingUnder(t, commandID, "s1"); holder != "inst-a" {
				t.Fatalf("%s is pending under %q after the claim, want inst-a", commandID, holder)
			}
			if delivered, err := read(t, adopter, "s1"); err != nil || len(delivered) != 0 {
				t.Fatalf("the next read handed out %v (err=%v), want nothing: it is a claim's", ids(delivered), err)
			}
			// The age is the subject: the claim that reset it needs the delay to pass again.
			time.Sleep(claimDelay + claimDelay/2)
			again, err := claim(ctx)
			if err != nil || !slices.Equal(ids(again), []string{commandID}) || !again[0].Redelivered {
				t.Fatalf("the next claim took %v (err=%v), want %s as a redelivery", ids(again), err, commandID)
			}
			ackAll(t, again)
		}
		claimSessions := func(ctx context.Context) ([]transport.Delivery, error) {
			return adopter.ClaimSessions(ctx, []string{"s1"})
		}
		reclaim := func(ctx context.Context) ([]transport.Delivery, error) {
			return adopter.Claim(ctx, []string{"s1"})
		}

		t.Run("its answer held past the window", func(t *testing.T) {
			abandon(t, "held-claim")
			release := make(chan struct{})
			caught := f.proxy.Hold("held-claim", release)
			window, stop := context.WithTimeout(ctx, cutWindow)
			claimed, err := claimSessions(window)
			stop()
			close(release)
			select {
			case <-caught:
			default:
				t.Fatalf("the claim's answer was never held (claimed %v, err=%v)", ids(claimed), err)
			}
			if err == nil || len(claimed) != 0 {
				t.Fatalf("the claim whose answer was lost handed out %v (err=%v)", ids(claimed), err)
			}
			stillAClaims(t, "held-claim", claimSessions)
		})

		t.Run("its answer dropped with the connection", func(t *testing.T) {
			// Sent again by go-redis on a fresh connection, the claim finds the command it
			// already moved too young to take, and comes back empty with no error.
			if f.fake {
				t.Skip("miniredis's XCLAIM never checks the min idle time, so the claim sent again takes the command a second time")
			}
			abandon(t, "dropped-claim")
			time.Sleep(claimDelay + claimDelay/2)
			caught := f.proxy.Drop("dropped-claim")
			claimed, err := reclaim(ctx)
			select {
			case <-caught:
			default:
				t.Fatalf("the claim's answer was never dropped (claimed %v, err=%v)", ids(claimed), err)
			}
			if len(claimed) != 0 {
				t.Fatalf("the claim whose answer was dropped handed out %v (err=%v)", ids(claimed), err)
			}
			stillAClaims(t, "dropped-claim", reclaim)
		})

		t.Run("a command already claimed and given back", func(t *testing.T) {
			abandon(t, "given-back")
			claimed, err := claimSessions(ctx)
			if err != nil || !slices.Equal(ids(claimed), []string{"given-back"}) {
				t.Fatalf("claimed %v (err=%v), want the peer's command", ids(claimed), err)
			}
			claimed[0].Release()
			release := make(chan struct{})
			caught := f.proxy.Hold("given-back", release)
			window, stop := context.WithTimeout(ctx, cutWindow)
			lost, err := claimSessions(window)
			stop()
			close(release)
			select {
			case <-caught:
			default:
				t.Fatalf("the claim's answer was never held (claimed %v, err=%v)", ids(lost), err)
			}
			stillAClaims(t, "given-back", claimSessions)
		})
	})
}

// The idle time a page carries is the entry's age when Redis ran the script, and the answer
// can take a while to come back. A wake that was young then may be past ReadBackMaxAge by
// the time the read hands it out, and the time it spent on the way counts against it.
func TestAWakeWhosePageTookLongToArriveCountsTheTripInItsAge(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		const maxAge = 2 * cutWindow

		streams := f.streamsWith(t, &redisstream.Options{
			Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: cutClaimMinIdle, ReadBackMaxAge: maxAge,
		})
		if _, err := read(t, streams, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		writeCommand(t, f.fleet, f.client.Keys().Control(), &protocol.Command{
			V: protocol.Version, ID: "slow-page-wake", Type: protocol.CommandSessionWake, SID: "s9",
			TS: 1787000000000, Payload: []byte(`{"desired":"connected"}`),
		})
		f.loseTheAnswer(t, "held past the window", streams, "inst-a", "slow-page-wake", "s1")

		// The page is held for the whole max age: however young the wake was when the script
		// ran, it is older than that when the page arrives.
		release := make(chan struct{})
		caught := f.proxy.Hold("slow-page-wake", release)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		type result struct {
			delivered []transport.Delivery
			err       error
		}
		done := make(chan result, 1)
		go func() {
			delivered, err := streams.Read(ctx, []string{"s1"})
			done <- result{delivered, err}
		}()
		<-caught
		time.Sleep(maxAge)
		close(release)
		if got := <-done; got.err != nil || len(got.delivered) != 0 {
			t.Fatalf("handed out %v (err=%v), want the wake left to a claim", ids(got.delivered), got.err)
		}
	})
}

// A consumer group recreated -- by an operator, or by any instance that found it gone -- starts
// again at the beginning of the stream, and `>` hands this consumer entries at or below the
// mark its old group left. The mark belongs to that group: kept, it would have the history
// skip the older commands `>` moved in while it hands out the newer ones.
func TestAGroupRecreatedUnderTheReadStartsItsHistoryOver(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		streams := f.streams(t, "inst-a")
		stream := f.client.Keys().Commands("s1")
		if _, err := read(t, streams, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		writeCommand(t, f.fleet, stream, command("before-reset", "s1", ""))
		delivered, err := read(t, streams, "s1")
		if err != nil || !slices.Equal(ids(delivered), []string{"before-reset"}) {
			t.Fatalf("handed out %v (err=%v), want the first command", ids(delivered), err)
		}
		writeCommand(t, f.fleet, stream, command("after-reset", "s1", ""))

		ctx := context.Background()
		if err := f.client.XGroupDestroy(ctx, stream, redisstream.ConsumerGroup).Err(); err != nil {
			t.Fatalf("XGROUP DESTROY: %v", err)
		}
		if err := f.client.XGroupCreate(ctx, stream, redisstream.ConsumerGroup, "0").Err(); err != nil {
			t.Fatalf("XGROUP CREATE: %v", err)
		}

		var order []string
		for range 3 {
			delivered, err := read(t, streams, "s1")
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			order = append(order, ids(delivered)...)
		}
		if want := []string{"before-reset", "after-reset"}; !slices.Equal(order, want) {
			t.Fatalf("handed out %v after the group was recreated, want %v", order, want)
		}
	})
}

// A read hands out what it read back, and a producer trimming the stream can take an entry
// away between `>` answering with it and the history reading it back. The answer carried
// the payload; losing the entry from the stream must not lose the command with it, whether
// the history reads it back in the same read or, with a page already full of older entries,
// in a later one.
func TestACommandTrimmedAfterItsReadAnsweredIsStillHandedOut(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		trim := func(stream string) {
			t.Helper()
			if err := f.client.XTrimMaxLen(ctx, stream, 0).Err(); err != nil {
				t.Fatalf("XTRIM: %v", err)
			}
		}

		t.Run("read back by the same read", func(t *testing.T) {
			streams := f.streams(t, "inst-a")
			stream := f.client.Keys().Commands("s1")
			if _, err := read(t, streams, "s1"); err != nil {
				t.Fatalf("priming read: %v", err)
			}
			writeCommand(t, f.fleet, stream, command("trimmed-at-once", "s1", ""))

			// The answer is held, so the stream is trimmed after `>` ran and before the
			// history does.
			release := make(chan struct{})
			caught := f.proxy.Hold("trimmed-at-once", release)
			type result struct {
				delivered []transport.Delivery
				err       error
			}
			done := make(chan result, 1)
			go func() {
				delivered, err := streams.Read(ctx, []string{"s1"})
				done <- result{delivered, err}
			}()
			<-caught
			trim(stream)
			close(release)
			got := <-done
			if got.err != nil || !slices.Equal(ids(got.delivered), []string{"trimmed-at-once"}) {
				t.Fatalf("handed out %v (err=%v), want the command the answer carried", ids(got.delivered), got.err)
			}
		})

		t.Run("read back by a later read", func(t *testing.T) {
			streams := f.streamsReading(t, "inst-b", 1)
			stream := f.client.Keys().Commands("s2")
			if _, err := read(t, streams, "s2"); err != nil {
				t.Fatalf("priming read: %v", err)
			}
			writeCommand(t, f.fleet, stream, command("lost-first", "s2", ""))
			f.loseTheAnswer(t, "held past the window", streams, "inst-b", "lost-first", "s2")
			writeCommand(t, f.fleet, stream, command("trimmed-later", "s2", ""))

			// One entry a page: this read's `>` carries the newer command, and its page is the
			// older one.
			delivered, err := read(t, streams, "s2")
			if err != nil || !slices.Equal(ids(delivered), []string{"lost-first"}) {
				t.Fatalf("handed out %v (err=%v), want the older command first", ids(delivered), err)
			}
			ackAll(t, delivered)
			trim(stream)
			delivered, err = read(t, streams, "s2")
			if err != nil || !slices.Equal(ids(delivered), []string{"trimmed-later"}) {
				t.Fatalf("handed out %v (err=%v), want the command an earlier answer carried", ids(delivered), err)
			}
		})

		// A wake past ReadBackMaxAge is left to a claim, but a claim of an entry gone from the
		// stream finds no payload and retires it as unreadable. The payload is here: handed out
		// late, the wake runs; left to the claim, it is lost.
		t.Run("a wake read back past its max age", func(t *testing.T) {
			streams := f.streamsWith(t, &redisstream.Options{
				Instance: "inst-c", Block: 50 * time.Millisecond, ClaimMinIdle: cutClaimMinIdle, ReadBackMaxAge: cutWindow / 2,
			})
			control := f.client.Keys().Control()
			if _, err := read(t, streams, "s3"); err != nil {
				t.Fatalf("priming read: %v", err)
			}
			writeCommand(t, f.fleet, control, &protocol.Command{
				V: protocol.Version, ID: "trimmed-wake", Type: protocol.CommandSessionWake, SID: "s9",
				TS: 1787000000000, Payload: []byte(`{"desired":"connected"}`),
			})

			// `>` answers, and the history read after it loses its answer past the window.
			passed := make(chan struct{})
			close(passed)
			carried := f.proxy.Hold("trimmed-wake", passed)
			release := make(chan struct{})
			paged := f.proxy.Hold("trimmed-wake", release)
			delivered, err := read(t, streams, "s3")
			close(release)
			for name, trap := range map[string]<-chan struct{}{"the answer to `>`": carried, "the page": paged} {
				select {
				case <-trap:
				default:
					t.Fatalf("%s carrying the wake was never caught (handed out %v, err=%v)", name, ids(delivered), err)
				}
			}
			if len(delivered) != 0 {
				t.Fatalf("the read whose page was lost handed out %v", ids(delivered))
			}

			// The age is the subject: the wake is past its max age when it is read back.
			time.Sleep(cutWindow / 2)
			trim(control)
			delivered, err = read(t, streams, "s3")
			if err != nil || !slices.Equal(ids(delivered), []string{"trimmed-wake"}) {
				t.Fatalf("handed out %v (err=%v), want the wake whose payload an earlier answer carried", ids(delivered), err)
			}
		})
	})
}

// A max age as long as the claim delay would hand a recovered wake out already claimable.
func TestAReadBackMaxAgeNotShorterThanTheClaimDelayIsRefused(t *testing.T) {
	f := newFleet(t)
	for _, age := range []time.Duration{cutClaimMinIdle, 2 * cutClaimMinIdle} {
		if _, err := redisstream.New(f.client, redisstream.Options{
			Instance: "inst-a", ClaimMinIdle: cutClaimMinIdle, ReadBackMaxAge: age,
		}); err == nil {
			t.Errorf("ReadBackMaxAge %s with ClaimMinIdle %s was accepted", age, cutClaimMinIdle)
		}
	}
}

// A claim hands out entries past the mark: a peer's, or one a peer gave back with its age
// put back, which is claimable at once. Taking one says nothing about the entries before it,
// and one of those may be what a lost answer left here. Moving the mark to the claimed entry
// would hide that one from every read back, and leave it to wait the claim delay again.
func TestAClaimPastALostAnswerDoesNotHideItFromTheNextRead(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		a := f.streams(t, "inst-a")
		b, err := redisstream.New(f.client, redisstream.Options{
			Instance: "inst-b", Block: 50 * time.Millisecond, ClaimMinIdle: cutClaimMinIdle,
		})
		if err != nil {
			t.Fatalf("redisstream.New: %v", err)
		}
		for _, streams := range []*redisstream.Streams{a, b} {
			if _, err := read(t, streams, "s1"); err != nil {
				t.Fatalf("priming read: %v", err)
			}
		}
		control := f.client.Keys().Control()
		wake := func(id string) *protocol.Command {
			return &protocol.Command{
				V: protocol.Version, ID: id, Type: protocol.CommandSessionWake, SID: "s9",
				TS: 1787000000000, Payload: []byte(`{"desired":"connected"}`),
			}
		}

		writeCommand(t, f.fleet, control, wake("gap-first"))
		f.loseTheAnswer(t, "held past the window", a, "inst-a", "gap-first", "s1")

		writeCommand(t, f.fleet, control, wake("gap-second"))
		given, err := read(t, b, "s1")
		if err != nil || !slices.Equal(ids(given), []string{"gap-second"}) {
			t.Fatalf("inst-b read %v (err=%v), want [gap-second]", ids(given), err)
		}
		given[0].Release()
		// inst-b's next pass puts the age back on what it gave back, which makes it
		// claimable by anybody at once.
		if _, err := b.Claim(context.Background(), nil); err != nil {
			t.Fatalf("inst-b Claim: %v", err)
		}
		claimed, err := a.ClaimControl(context.Background())
		if err != nil || !slices.Equal(ids(claimed), []string{"gap-second"}) {
			t.Fatalf("inst-a claimed %v (err=%v), want [gap-second]", ids(claimed), err)
		}

		var after []string
		for range 3 {
			delivered, err := read(t, a, "s1")
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			after = append(after, ids(delivered)...)
		}
		if want := []string{"gap-first"}; !slices.Equal(after, want) {
			t.Fatalf("after claiming gap-second, inst-a's reads handed out %v, want %v", after, want)
		}
	})
}

// What this process was handed and has not finished with is still pending under its
// name, exactly like what a lost answer left there. Recovering the second must not hand
// out the first again: not a command still running (invariant 5), and not one given back
// unrun or forfeited, which belong to a claim and would otherwise jump back to the front
// of the queue on every read.
func TestRecoveringALostAnswerHandsOutNothingThisProcessWasAlreadyGiven(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		streams := f.streams(t, "inst-a")
		stream := f.client.Keys().Commands("s1")
		writeCommand(t, f.fleet, stream, command("given-running", "s1", ""))
		writeCommand(t, f.fleet, stream, command("given-back", "s1", ""))
		writeCommand(t, f.fleet, stream, command("given-forfeited", "s1", ""))

		var given []transport.Delivery
		for len(given) < 3 {
			delivered, err := read(t, streams, "s1")
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if len(delivered) == 0 {
				t.Fatalf("handed out %v and then nothing, want all three", ids(given))
			}
			given = append(given, delivered...)
		}
		given[1].Release()
		given[2].Forfeit()

		writeCommand(t, f.fleet, stream, command("given-lost", "s1", ""))
		f.loseTheAnswer(t, "held past the window", streams, "inst-a", "given-lost", "s1")

		var after []string
		for range 5 {
			delivered, err := read(t, streams, "s1")
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			after = append(after, ids(delivered)...)
			ackAll(t, delivered)
		}
		if want := []string{"given-lost"}; !slices.Equal(after, want) {
			t.Fatalf("after the lost answer the reads handed out %v, want only %v", after, want)
		}
	})
}

// A consumer's pending history is its own, and recovery reads nothing else. A command
// pending under another instance may be running there right now: taking it before the
// claim delay is how a peer's work runs twice.
func TestRecoveringALostAnswerTakesNothingPendingUnderAnotherInstance(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		stream := f.client.Keys().Commands("s1")
		streams := f.streams(t, "inst-a")
		if _, err := read(t, streams, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		writeCommand(t, f.fleet, stream, command("peer-running", "s1", ""))
		if taken, err := f.rdb.XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Group: redisstream.ConsumerGroup, Consumer: "inst-b", Streams: []string{stream, ">"}, Count: 1, Block: -1,
		}).Result(); err != nil || len(taken) != 1 || len(taken[0].Messages) != 1 {
			t.Fatalf("inst-b read %v (err=%v), want the one command", taken, err)
		}

		writeCommand(t, f.fleet, stream, command("mine-lost", "s1", ""))
		f.loseTheAnswer(t, "held past the window", streams, "inst-a", "mine-lost", "s1")

		var after []string
		for range 3 {
			delivered, err := read(t, streams, "s1")
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			after = append(after, ids(delivered)...)
		}
		if want := []string{"mine-lost"}; !slices.Equal(after, want) {
			t.Fatalf("inst-a handed out %v, want only %v", after, want)
		}
		if holder := f.pendingUnder(t, "peer-running", "s1"); holder != "inst-b" {
			t.Fatalf("the peer's command is pending under %q, want it left with inst-b", holder)
		}
	})
}

// A session this instance stops reading -- its lease went to somebody else -- is not
// recovered into this instance on the way out. What its lost answer left pending belongs
// to the new owner, whose drain takes it at once.
func TestALostAnswerForASessionNoLongerReadIsLeftForItsNewOwner(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		old := f.streams(t, "inst-a")
		if _, err := read(t, old, "s1", "s9"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		writeCommand(t, f.fleet, f.client.Keys().Commands("s1"), command("kept-s1", "s1", ""))
		writeCommand(t, f.fleet, f.client.Keys().Commands("s9"), command("moved-s9", "s9", ""))
		f.loseTheAnswer(t, "held past the window", old, "inst-a", "moved-s9", "s1", "s9")

		var kept []string
		for range 3 {
			delivered, err := read(t, old, "s1")
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			kept = append(kept, ids(delivered)...)
		}
		if want := []string{"kept-s1"}; !slices.Equal(kept, want) {
			t.Fatalf("inst-a, reading only s1, handed out %v, want only %v", kept, want)
		}

		adopted, err := f.streams(t, "inst-b").ClaimSessions(context.Background(), []string{"s9"})
		if err != nil {
			t.Fatalf("ClaimSessions: %v", err)
		}
		if got := ids(adopted); !slices.Equal(got, []string{"moved-s9"}) {
			t.Fatalf("the new owner's drain took %v, want [moved-s9]", got)
		}
	})
}

// The mark recovery reads past is the newest entry this process was handed, and newest is
// by entry id, not by when it was handed. A claim hands out older entries after newer
// ones, and ids share a millisecond once a client writes fast enough that the sequence
// runs past nine. Either way a mark that went backwards would have recovery hand out,
// a second time, a command this process is still running.
func TestTheMarkRecoveryReadsPastNeverGoesBackwards(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		for _, tc := range []struct {
			name, sid string
			// given leaves this process running two commands on sid.
			given func(t *testing.T, f cutFleet, streams *redisstream.Streams, sid string)
		}{
			{
				name: "a claim hands out something older after something newer", sid: "s-claimed",
				given: func(t *testing.T, f cutFleet, streams *redisstream.Streams, sid string) {
					stream := f.client.Keys().Commands(sid)
					writeCommand(t, f.fleet, stream, command("mark-older", sid, ""))
					if _, err := f.rdb.XReadGroup(context.Background(), &redis.XReadGroupArgs{
						Group: redisstream.ConsumerGroup, Consumer: "inst-dead", Streams: []string{stream, ">"}, Count: 1, Block: -1,
					}).Result(); err != nil {
						t.Fatalf("inst-dead read: %v", err)
					}
					writeCommand(t, f.fleet, stream, command("mark-newer", sid, ""))
					newer, err := read(t, streams, sid)
					if err != nil || !slices.Equal(ids(newer), []string{"mark-newer"}) {
						t.Fatalf("read %v (err=%v), want [mark-newer]", ids(newer), err)
					}
					older, err := streams.ClaimSessions(context.Background(), []string{sid})
					if err != nil || !slices.Equal(ids(older), []string{"mark-older"}) {
						t.Fatalf("claimed %v (err=%v), want [mark-older]", ids(older), err)
					}
				},
			},
			{
				name: "two ids in one millisecond whose sequences only compare as numbers", sid: "s-sequence",
				given: func(t *testing.T, f cutFleet, streams *redisstream.Streams, sid string) {
					stream := f.client.Keys().Commands(sid)
					for _, entry := range []struct{ id, command string }{{"1-9", "mark-nine"}, {"1-10", "mark-ten"}} {
						fields, err := command(entry.command, sid, "").Fields()
						if err != nil {
							t.Fatalf("render: %v", err)
						}
						values := make(map[string]any, len(fields))
						for key, value := range fields {
							values[key] = value
						}
						if err := f.rdb.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, ID: entry.id, Values: values}).Err(); err != nil {
							t.Fatalf("XAdd %s: %v", entry.id, err)
						}
					}
					both, err := read(t, streams, sid)
					if err != nil || !slices.Equal(ids(both), []string{"mark-nine", "mark-ten"}) {
						t.Fatalf("read %v (err=%v), want [mark-nine mark-ten]", ids(both), err)
					}
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				streams := f.streams(t, "inst-a")
				if _, err := read(t, streams, tc.sid); err != nil {
					t.Fatalf("priming read: %v", err)
				}
				tc.given(t, f, streams, tc.sid)

				writeCommand(t, f.fleet, f.client.Keys().Commands(tc.sid), command("mark-lost-"+tc.sid, tc.sid, ""))
				f.loseTheAnswer(t, "held past the window", streams, "inst-a", "mark-lost-"+tc.sid, tc.sid)

				var after []string
				for range 3 {
					delivered, err := read(t, streams, tc.sid)
					if err != nil {
						t.Fatalf("read: %v", err)
					}
					after = append(after, ids(delivered)...)
				}
				if want := []string{"mark-lost-" + tc.sid}; !slices.Equal(after, want) {
					t.Fatalf("with two commands still running, the reads handed out %v, want only %v", after, want)
				}
			})
		}
	})
}
