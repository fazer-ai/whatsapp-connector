package redisstream_test

import (
	"testing"
	"time"

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
