package redisstream_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/transport/redisstream"
)

// An entry read back out of this consumer's pending history says it has been handed out
// before, whatever its age.
//
// The two facts are different and the difference is the whole point of the metric built
// on this. `Redelivered` is the narrower one -- old enough that a claim would have taken
// it, so its sender has probably stopped listening -- and it is a conjunction: not fresh
// AND past the claim delay. The loop this instrument exists for is the other one: an
// entry whose answer went missing comes back on the very next block, so its idle never
// grows, the conjunction is false every single time, and a fleet retrying forever is
// indistinguishable from a quiet one.
//
// So the claim delay here is `cutClaimMinIdle`, far longer than the test: nothing in it
// can ever be old enough to be `Redelivered`, and what is left is exactly the fact the
// old field could not carry.
func TestAnEntryReadBackFromHistorySaysItWasDeliveredBefore(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		streams := f.streamsWith(t, &redisstream.Options{
			Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: cutClaimMinIdle, ReadBackMaxAge: cutWindow,
		})
		stream := f.client.Keys().Commands("s1")
		if _, err := read(t, streams, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}

		writeCommand(t, f.fleet, stream, command("lost-connect", "s1", ""))
		// The `>` carried it and the answer never arrived, so Redis has it pending under
		// this consumer while the transport never saw it: the state a read back exists for.
		f.loseTheAnswer(t, "held past the window", streams, "inst-a", "lost-connect", "s1")

		again, err := read(t, streams, "s1")
		if err != nil || len(again) != 1 {
			t.Fatalf("the read back handed out %v (err=%v), want lost-connect", ids(again), err)
		}
		if !again[0].DeliveredBefore {
			t.Error("an entry read back out of the pending history says it is arriving new: " +
				"this is the loop the redelivery metric exists for, and it would count zero")
		}
		if again[0].Redelivered {
			t.Errorf("an entry idle for milliseconds against a %s claim delay says it is a "+
				"redelivery a claim would have taken: the two facts have stopped being "+
				"different, and the narrower one is the one nothing ever sees", cutClaimMinIdle)
		}
		if again[0].TakenFrom != "" {
			t.Errorf("a read says it took the entry from %q: nothing took it from anybody, "+
				"and a name here would count this instance as a peer that stopped answering",
				again[0].TakenFrom)
		}
		ackAll(t, again)
	})
}

// A command arriving for the first time carries none of it, which is what keeps the
// metric from counting every delivery the fleet makes.
func TestACommandArrivingNewSaysNothingAboutHavingBeenDeliveredBefore(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		streams := f.streamsWith(t, &redisstream.Options{
			Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: cutClaimMinIdle,
		})
		writeCommand(t, f.fleet, stream(f, "s1"), command("first-connect", "s1", ""))

		delivered, err := read(t, streams, "s1")
		if err != nil || len(delivered) != 1 {
			t.Fatalf("the read handed out %v (err=%v), want first-connect", ids(delivered), err)
		}
		if delivered[0].DeliveredBefore {
			t.Error("a command nobody had been handed says it had been delivered before")
		}
		if got := delivered[0].Deliveries; got != 0 {
			t.Errorf("a read says the entry had been delivered %d times: only a claim can "+
				"read that, and a number invented here would land in the histogram", got)
		}
		ackAll(t, delivered)
	})
}

func stream(f cutFleet, sid string) string {
	return f.client.Keys().Commands(sid)
}

// A claim reports the count Redis listed, and adds nothing for the claim it is about
// to send.
//
// Adding one would be right exactly when the claim reaches Redis once, and go-redis
// sends a command again when the answer to it never arrives: XCLAIM runs a second time,
// increments the delivery counter a second time, and hands the caller a successful claim
// with no sign that anything was retried. That happens during connection trouble, which
// is the state the metric built on this exists to make visible, so the arithmetic would
// be short precisely where the number matters.
//
// The retry is staged rather than argued: the first XCLAIM goes out on the transport's
// own client with its answer cut, and Redis is asked what that left behind. Then the
// transport claims the same entry, and what it reports has to be what Redis listed.
func TestAClaimReportsTheListedCountAndNotAGuessAboutTheClaimItself(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		if f.fake {
			// The mechanism under test is go-redis reacting to a connection that died
			// mid-answer against a server that has already run the command, counted by
			// the server's own delivery counter. A double cannot stand in for either half.
			t.Skip("a retried XCLAIM and its delivery counter are a real-Redis question")
		}
		ctx := context.Background()
		dead := f.streamsWith(t, &redisstream.Options{
			Instance: "inst-dead", Block: 50 * time.Millisecond, ClaimMinIdle: time.Millisecond,
		})
		taking := f.streamsWith(t, &redisstream.Options{
			Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: time.Millisecond,
		})
		st := stream(f, "s1")
		if _, err := read(t, dead, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		writeCommand(t, f.fleet, st, command("cut-claim", "s1", ""))
		if took, err := read(t, dead, "s1"); err != nil || len(took) != 1 {
			t.Fatalf("the peer read %v (err=%v), want cut-claim", ids(took), err)
		}
		entry := pendingEntryOf(t, f, st, "cut-claim")
		if entry.RetryCount != 1 {
			t.Fatalf("Redis counts %d deliveries after one read, want 1", entry.RetryCount)
		}

		// On the client the transport itself uses, so what is being staged is the retry
		// the transport would get, not one arranged out of its way.
		caught := f.proxy.Drop("cut-claim")
		if _, err := f.via.XClaim(ctx, &redis.XClaimArgs{
			Stream: st, Group: redisstream.ConsumerGroup, Consumer: "inst-cut", MinIdle: 0,
			Messages: []string{entry.ID},
		}).Result(); err != nil {
			t.Fatalf("the claim whose answer was cut came back with %v, and a retry that "+
				"reached Redis is what this test is about", err)
		}
		select {
		case <-caught:
		default:
			t.Fatal("the proxy never cut an answer carrying cut-claim")
		}
		staged := pendingEntryOf(t, f, st, "cut-claim").RetryCount
		if staged != 3 {
			t.Fatalf("Redis counts %d deliveries after a claim whose answer was cut, want 3: "+
				"one read plus the claim sent twice. Without the second one there is no retry "+
				"here and this test has stopped being about anything", staged)
		}

		claimed, err := taking.ClaimSessions(ctx, []string{"s1"})
		if err != nil || len(claimed) != 1 {
			t.Fatalf("the claim took %v (err=%v), want cut-claim", ids(claimed), err)
		}
		if got := claimed[0].Deliveries; got != staged {
			t.Errorf("the claim reports %d deliveries and Redis listed %d. Anything but the "+
				"listed count is a guess about a claim still on the wire, and one that assumes "+
				"the claim landed once is short by every retry", got, staged)
		}
		if got := claimed[0].TakenFrom; got != "inst-cut" {
			t.Errorf("the claim says it took the entry from %q, want inst-cut", got)
		}
		ackAll(t, claimed)
	})
}

// pendingEntryOf is what Redis says right now about the entry carrying commandID.
func pendingEntryOf(t *testing.T, f cutFleet, stream, commandID string) redis.XPendingExt {
	t.Helper()
	ctx := context.Background()
	rows, err := f.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream, Group: redisstream.ConsumerGroup, Start: "-", End: "+", Count: 100,
	}).Result()
	if err != nil {
		t.Fatalf("XPENDING %s: %v", stream, err)
	}
	for _, row := range rows {
		messages, err := f.rdb.XRange(ctx, stream, row.ID, row.ID).Result()
		if err != nil || len(messages) != 1 {
			t.Fatalf("XRANGE %s %s: %v", stream, row.ID, err)
		}
		if messages[0].Values["id"] == commandID {
			return row
		}
	}
	t.Fatalf("%s is not pending on %s", commandID, stream)
	return redis.XPendingExt{}
}
