package redisstream_test

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/transport"
	"github.com/fazer-ai/whatsapp-connector/internal/transport/redisstream"
)

// loadedTrip is how long a loaded machine can take to get the read back to Redis: a host
// under a make check beside twenty CPU loops took long enough to lose an answer this way
// (#341). Well inside the pass's window, so what it costs is time and not the pass.
const loadedTrip = 60 * time.Millisecond

// slowReadBack delays every read back of the history once armed, the way a loaded machine
// delays the trip, and counts the ones it delayed: a run in which it never fired proves
// nothing about the trip.
type slowReadBack struct {
	armed   atomic.Bool
	delayed atomic.Int32
}

func (*slowReadBack) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *slowReadBack) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.armed.Load() && readsBack(cmd) {
			h.delayed.Add(1)
			select {
			case <-time.After(loadedTrip):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return next(ctx, cmd)
	}
}

func (*slowReadBack) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// readsBack says whether a command is the history script. By its hash alone: the reads
// before the hook is armed have put the script in the server's cache, so what goes out
// armed is EVALSHA and never the source.
func readsBack(cmd redis.Cmder) bool {
	args := cmd.Args()
	return len(args) >= 2 && (cmd.Name() == "evalsha" || cmd.Name() == "evalsha_ro") &&
		fmt.Sprint(args[1]) == redisstream.ReadBackScript
}

// A lost answer recovered by a read whose trip to Redis was slow is still this process's.
//
// A read back leaves to a claim whatever has been pending longer than this process has been
// up, as its predecessor's, and counts the uptime from when the page is sent. The trip after
// that is on the entry's side of the comparison, so an entry handed to this process a few
// milliseconds after it started looks older than the process as soon as the trip takes
// longer than those milliseconds. That is the safe side on purpose, and in production it only
// shows in the moments after a restart. A test starts its transport a few milliseconds before
// losing an answer, though, and on a loaded host the mark then moved past the entry and the
// test waited for a claim that never came in time.
func TestALostAnswerReadBackOnASlowTripIsStillHandedOut(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		slow := &slowReadBack{}
		f.through.AddHook(slow)
		streams := f.streams(t, "inst-a")
		stream := f.client.Keys().Commands("s1")
		if _, err := read(t, streams, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}

		writeCommand(t, f.fleet, stream, command("lost-on-a-slow-trip", "s1", ""))
		f.loseTheAnswer(t, "held past the window", streams, "inst-a", "lost-on-a-slow-trip", "s1")
		slow.armed.Store(true)

		var after []transport.Delivery
		for range 3 {
			delivered, err := read(t, streams, "s1")
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			after = append(after, delivered...)
			ackAll(t, delivered)
		}
		if slow.delayed.Load() == 0 {
			t.Fatal("no read back was slowed down, so this says nothing about a slow trip")
		}
		if want := []string{"lost-on-a-slow-trip"}; !slices.Equal(ids(after), want) {
			t.Fatalf("after a read back that took %s to reach Redis, the reads handed out %v, want %v: "+
				"the entry was taken for a predecessor's and left to a claim", loadedTrip, ids(after), want)
		}
	})
}
