package redisstream_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	// through is the client under via, for a hook a test puts on the transport's side of
	// the proxy only.
	through *redis.Client
	fake    bool
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
			rdb.AddHook(passReads{proxy: proxy})
			t.Cleanup(func() { _ = rdb.Close() })
			run(t, cutFleet{
				fleet: direct, proxy: proxy, via: redisx.Wrap(rdb, direct.client.Keys().Prefix(), shards),
				through: rdb, fake: backend.name == "miniredis",
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
//
// One that went out, that is. A pass whose window is over before it can name a block
// comes back empty and without an error without touching Redis (`blockWithin`), which in
// the loop costs nothing, since the next pass does the work; here it is a goroutine the
// scheduler kept waiting, and a test that expects a delivery would read it as a verdict
// (#328). So a pass that sent no XREADGROUP is run again with a new window. One that did
// is never repeated for being empty, however late its caller gets the CPU back: an empty
// pass stays a result for the tests that expect nothing.
//
// Whether it went out is counted, not inferred from the clock: the scheduler that starves
// a pass before it goes out can as easily starve one after it came back, and the time
// left would then call a finished read starved (#333). The count comes from passReads,
// which every client these tests build carries; one that does not makes every empty pass
// look starved, and the helper fails loudly rather than passing on it.
//
// A pass that went out can also come back with its window spent, and that is either the
// test's doing or the machine's. The tests that lose an answer on purpose hold it in the
// proxy past the window, and the pass that fails that way is exactly what they assert. On
// a loaded machine the same failure comes with nothing held, from a trip that simply took
// longer than the window (#334), and like the starved pass it is no verdict: the loop's
// next pass reads back what it left pending. So a spent pass is run again when the proxy
// the client goes through neither stepped in nor stood ready to while it ran.
func read(t *testing.T, streams *redisstream.Streams, sids ...string) ([]transport.Delivery, error) {
	t.Helper()
	return readWithin(t, cutWindow, streams, sids...)
}

// readWithin is read with a window of the caller's, for the test that has to watch it
// give up.
func readWithin(t testing.TB, window time.Duration, streams *redisstream.Streams, sids ...string) ([]transport.Delivery, error) {
	t.Helper()
	for range starvedPasses {
		delivered, again, err := pass(streams, window, sids)
		if !again {
			return delivered, err
		}
	}
	// Errorf rather than Fatalf: some callers read from a goroutine of their own.
	t.Errorf("read: %d passes in a row came back with their window spent, before they could "+
		"go out or with nothing held in the proxy, so none of them read anything", starvedPasses)
	return nil, errors.New("read: every pass was starved of its window")
}

// starvedPasses bounds how many times read runs a pass the machine starved. A machine
// that starves this many windows in a row is not going to be waited out, and a test that
// hung instead of failing would say less.
const starvedPasses = 5

// pass is a single read, and whether the machine rather than the test decided how it came
// out: it never went out, or it had its window spent with the proxy idle, whether on the
// read itself or earlier, on the XGROUP that creates the groups.
func pass(streams *redisstream.Streams, window time.Duration, sids []string) (delivered []transport.Delivery, again bool, err error) {
	sent := &passSent{}
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), passKey{}, sent), window)
	defer cancel()
	delivered, err = streams.Read(ctx, sids)
	spent := errors.Is(err, transport.ErrWindowSpent) && sent.unprovoked()
	if sent.reads.Load() == 0 {
		return delivered, (len(delivered) == 0 && err == nil) || spent, err
	}
	return delivered, spent, err
}

// passKey carries a pass's own count through the client's hooks, so that passes running
// in parallel on one client count apart.
type passKey struct{}

type passSent struct {
	reads atomic.Int64

	// What the proxy had done and stood ready to do when the pass sent its first command.
	began  sync.Once
	proxy  *redisxtest.Proxy
	caught uint64
	busy   bool
}

// unprovoked reports whether the proxy the pass went through left it alone: nothing
// armed or held when it began or when it ended, and nothing caught in between. A client
// with no proxy in front has nobody to provoke anything.
func (s *passSent) unprovoked() bool {
	if s.proxy == nil {
		return true
	}
	return !s.busy && !s.proxy.Busy() && s.proxy.Caught() == s.caught
}

// passReads counts each XREADGROUP against the pass whose context sent it, and has the
// pass note the state of proxy, the one the client goes through, before its first command
// leaves. A client that reaches Redis directly carries it with no proxy.
type passReads struct{ proxy *redisxtest.Proxy }

func (passReads) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h passReads) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.countRead(ctx, cmd)
		return next(ctx, cmd)
	}
}

func (h passReads) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.countRead(ctx, cmd)
		}
		return next(ctx, cmds)
	}
}

func (h passReads) countRead(ctx context.Context, cmd redis.Cmder) {
	sent, ok := ctx.Value(passKey{}).(*passSent)
	if !ok {
		return
	}
	sent.began.Do(func() {
		if h.proxy != nil {
			sent.proxy, sent.caught, sent.busy = h.proxy, h.proxy.Caught(), h.proxy.Busy()
		}
	})
	if cmd.Name() == "xreadgroup" {
		sent.reads.Add(1)
	}
}

// spendTheWindow holds the first XGROUP a transport sends until its window is over, the
// way a goroutine the scheduler left waiting would reach the check after it, and answers
// every XGROUP without sending it: the groups already stand, so nothing is lost, and the
// read that follows finds no room for a block.
type spendTheWindow struct{ spent atomic.Int64 }

func (*spendTheWindow) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *spendTheWindow) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() != "xgroup" {
			return next(ctx, cmd)
		}
		if h.spent.CompareAndSwap(0, 1) {
			<-ctx.Done()
		}
		return nil
	}
}

func (*spendTheWindow) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// countReads counts the XREADGROUPs sent, which is how many passes went out.
type countReads struct{ sent atomic.Int64 }

func (*countReads) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *countReads) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "xreadgroup" {
			h.sent.Add(1)
		}
		return next(ctx, cmd)
	}
}

func (*countReads) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// A read whose window is over before it can go out comes back empty and without an error,
// by design: it moved nothing, and the next pass of the loop does the work. The helper
// takes one pass as the unit, so a machine loaded enough to starve one goroutine for most
// of a window failed a test that expected a delivery (#328). A pass that did go out and
// found nothing is left alone: that is what the tests that expect nothing assert.
func TestAPassThatNeverWentOutIsReadAgain(t *testing.T) {
	t.Parallel()

	cutBackends(t, func(t *testing.T, f cutFleet) {
		// The groups stand before the transport under test starts, so the XGROUP it sends
		// is only its own cache catching up.
		if _, err := read(t, f.fleet.streams(t, "inst-primer"), "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		writeCommand(t, f.fleet, f.client.Keys().Commands("s1"), command("starved", "s1", ""))
		hook := &spendTheWindow{}
		f.rdb.AddHook(hook)

		delivered, err := read(t, f.fleet.streams(t, "inst-a"), "s1")
		if hook.spent.Load() != 1 {
			t.Fatalf("the window was spent %d times, want once", hook.spent.Load())
		}
		if err != nil || !slices.Equal(ids(delivered), []string{"starved"}) {
			t.Fatalf("handed out %v (err=%v) after a pass that never went out, want the command", ids(delivered), err)
		}
	})
}

// A pass can also go out and have the machine, not the test, spend its window: under a
// load average of 10 the answer of the read back was still on its way when the window
// ran out, and a priming read failed the test with ErrWindowSpent (#334). Nothing here
// held that answer, so the pass is no verdict, and the loop's next pass is what reads the
// command back.
func TestAPassTheMachineCutIsReadAgain(t *testing.T) {
	t.Parallel()

	cutBackends(t, func(t *testing.T, f cutFleet) {
		streams := f.streams(t, "inst-a")
		if _, err := read(t, streams, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		writeCommand(t, f.fleet, f.client.Keys().Commands("s1"), command("cut-by-the-machine", "s1", ""))
		cut := &cutTheReadBack{}
		f.through.AddHook(cut)

		delivered, err := read(t, streams, "s1")
		if cut.cut.Load() != 1 {
			t.Fatalf("the machine cut %d passes, want one", cut.cut.Load())
		}
		if err != nil || !slices.Equal(ids(delivered), []string{"cut-by-the-machine"}) {
			t.Fatalf("handed out %v (err=%v) after a pass the machine cut, want the command", ids(delivered), err)
		}
	})
}

// What decides whether a spent pass is run again is whether the proxy stepped in while it
// ran, and each way it can have done so is a clause of its own: an answer still held when
// the pass began, a trap armed while it ran and not yet sprung, an answer caught in
// between. Any one of them makes the pass the test's doing.
func TestAPassIsTheTestsDoingWheneverTheProxySteppedIn(t *testing.T) {
	t.Parallel()

	cutBackends(t, func(t *testing.T, f cutFleet) {
		ctx := context.Background()
		// springOn sends marker through the proxy and waits for the trap to catch it, then
		// returns a channel closed once the ECHO is done. An ECHO that finishes with the trap
		// still armed (a server that refused it, a URL that reaches nothing) fails the case,
		// rather than leaving it waiting for an answer that will never come.
		springOn := func(t *testing.T, marker string, caught <-chan struct{}) <-chan struct{} {
			t.Helper()
			sent := make(chan error, 1)
			go func() { sent <- f.through.Echo(ctx, marker).Err() }()
			finished := make(chan struct{})
			select {
			case <-caught:
				go func() {
					<-sent
					close(finished)
				}()
			case err := <-sent:
				select {
				case <-caught:
					close(finished)
				default:
					t.Fatalf("ECHO %s finished (err=%v) without the proxy catching it", marker, err)
				}
			}
			return finished
		}
		for _, tc := range []struct {
			name   string
			during func(t *testing.T, begin func())
			want   bool
		}{
			{"nothing armed, held or caught", func(_ *testing.T, begin func()) { begin() }, true},
			{"an answer still held when it began", func(t *testing.T, begin func()) {
				release := make(chan struct{})
				done := springOn(t, "held-at-begin", f.proxy.Hold("held-at-begin", release))
				begin()
				close(release)
				<-done
			}, false},
			{"an answer caught while it ran", func(t *testing.T, begin func()) {
				begin()
				<-springOn(t, "dropped-meanwhile", f.proxy.Drop("dropped-meanwhile"))
			}, false},
			{"an answer caught before a later command of the same pass", func(t *testing.T, begin func()) {
				begin()
				<-springOn(t, "dropped-early", f.proxy.Drop("dropped-early"))
				// The pass goes on sending: what it began with is still what counts.
				begin()
			}, false},
			// Last: the trap it arms is never sprung, and the proxy stays busy after it.
			{"a trap armed while it ran", func(_ *testing.T, begin func()) {
				begin()
				f.proxy.Hold("armed-meanwhile", make(chan struct{}))
			}, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				// Every case starts from an idle proxy, or it would be judging the last one's.
				if f.proxy.Busy() {
					t.Fatalf("the proxy is still busy from an earlier case")
				}
				sent := &passSent{}
				passCtx := context.WithValue(ctx, passKey{}, sent)
				tc.during(t, func() { passReads{proxy: f.proxy}.countRead(passCtx, redis.NewCmd(passCtx, "ping")) })
				if got := sent.unprovoked(); got != tc.want {
					t.Fatalf("unprovoked = %v, want %v", got, tc.want)
				}
			})
		}
		// A client that reaches Redis directly has nobody in front to provoke anything.
		t.Run("no proxy in front", func(t *testing.T) {
			sent := &passSent{}
			passCtx := context.WithValue(ctx, passKey{}, sent)
			passReads{}.countRead(passCtx, redis.NewCmd(passCtx, "ping"))
			if !sent.unprovoked() {
				t.Fatalf("a pass with no proxy in front was taken for the test's doing")
			}
		})
	})
}

// The window can also run out earlier, on the XGROUP that creates the groups, before any
// XREADGROUP goes out: the verifier saw a priming read fail with "create group on ...:
// context deadline exceeded" under load (#334). It is the same machine cut, one step
// sooner, and it comes back as whichever error the deadline took on the way.
func TestAGroupCreationTheMachineCutIsTriedAgain(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"as the context's deadline", context.DeadlineExceeded},
		{"as the socket's timeout", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cutBackends(t, func(t *testing.T, f cutFleet) {
				cut := &cutTheGroupCreation{err: tc.err}
				f.through.AddHook(cut)

				if _, err := read(t, f.streams(t, "inst-a"), "s1"); err != nil {
					t.Fatalf("a priming read whose XGROUP the machine cut failed: %v", err)
				}
				if cut.cut.Load() != 1 {
					t.Fatalf("the machine cut %d group creations, want one", cut.cut.Load())
				}
			})
		})
	}
}

// cutTheGroupCreation keeps the first XGROUP from going out until the window is over, and
// fails it with err.
type cutTheGroupCreation struct {
	err error
	cut atomic.Int64
}

func (*cutTheGroupCreation) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *cutTheGroupCreation) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "xgroup" && h.cut.CompareAndSwap(0, 1) {
			<-ctx.Done()
			return h.err
		}
		return next(ctx, cmd)
	}
}

func (*cutTheGroupCreation) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// cutTheReadBack lets the first read back after a `>` reach Redis, then keeps its answer
// until the window is over and fails it the way the socket's deadline does: what a
// loaded machine does to a pass without anything in the proxy's hands.
type cutTheReadBack struct {
	reads atomic.Int64
	cut   atomic.Int64
}

func (*cutTheReadBack) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *cutTheReadBack) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "xreadgroup" {
			h.reads.Add(1)
		}
		err := next(ctx, cmd)
		if h.reads.Load() > 0 && (cmd.Name() == "evalsha" || cmd.Name() == "eval") && h.cut.CompareAndSwap(0, 1) {
			<-ctx.Done()
			return &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
		}
		return err
	}
}

func (*cutTheReadBack) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// failures is a test that keeps what it was told to fail with, so that a helper's own
// failure can be asserted on instead of failing the test watching it.
type failures struct {
	testing.TB
	told []string
}

func (f *failures) Helper() {}

func (f *failures) Errorf(format string, args ...any) {
	f.told = append(f.told, fmt.Sprintf(format, args...))
}

// A machine that never leaves a window for the read is not waited out: the helper gives
// up after a bounded number of passes and says why, rather than hanging or handing its
// caller an empty pass as if it were an answer.
func TestAReadStarvedOfEveryWindowFailsSayingSo(t *testing.T) {
	t.Parallel()

	cutBackends(t, func(t *testing.T, f cutFleet) {
		streams := f.fleet.streams(t, "inst-a")
		reads := &countReads{}
		f.rdb.AddHook(reads)
		watched := &failures{TB: t}

		delivered, err := readWithin(watched, 0, streams, "s1")
		if err == nil || len(delivered) != 0 {
			t.Errorf("a read with no window handed out %v (err=%v), want an error", ids(delivered), err)
		}
		if len(watched.told) != 1 || !strings.Contains(watched.told[0], "window spent") {
			t.Errorf("the helper failed the test with %q, want one failure naming the spent window", watched.told)
		}
		if got := reads.sent.Load(); got != 0 {
			t.Errorf("%d passes went out without a window", got)
		}
	})
}

// The other half: a pass that went out and found nothing is the answer, and is not asked
// again. Repeating it would make every test that expects nothing wait out one more block
// and then pass, whatever the transport did.
func TestAnEmptyPassThatWentOutIsNotReadAgain(t *testing.T) {
	t.Parallel()

	cutBackends(t, func(t *testing.T, f cutFleet) {
		streams := f.fleet.streams(t, "inst-a")
		if _, err := read(t, streams, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		reads := &countReads{}
		f.rdb.AddHook(reads)

		delivered, err := read(t, streams, "s1")
		if err != nil || len(delivered) != 0 {
			t.Fatalf("handed out %v (err=%v) from a stream with nothing on it", ids(delivered), err)
		}
		if got := reads.sent.Load(); got != 1 {
			t.Fatalf("an empty pass that went out was read %d times, want once", got)
		}
	})
}

// And however late the caller gets the CPU back: a pass that went out and found nothing,
// whose goroutine was then kept waiting past the end of its window, is still an answer.
func TestAnEmptyPassWhoseCallerResumesLateIsNotReadAgain(t *testing.T) {
	t.Parallel()

	cutBackends(t, func(t *testing.T, f cutFleet) {
		streams := f.fleet.streams(t, "inst-a")
		if _, err := read(t, streams, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		late := &resumeLate{}
		f.rdb.AddHook(late)

		delivered, err := read(t, streams, "s1")
		if err != nil || len(delivered) != 0 {
			t.Fatalf("handed out %v (err=%v) from a stream with nothing on it", ids(delivered), err)
		}
		if got := late.reads.Load(); got != 1 {
			t.Fatalf("an empty pass whose caller resumed after its window was read %d times, want once", got)
		}
	})
}

// resumeLate counts the XREADGROUPs, and holds the answer of the trip that closes a pass,
// the read back that follows the `>`, until the window is over: what the read looks like
// from a goroutine descheduled once it is done.
type resumeLate struct{ reads atomic.Int64 }

func (*resumeLate) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *resumeLate) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "xreadgroup" {
			h.reads.Add(1)
		}
		err := next(ctx, cmd)
		if h.reads.Load() > 0 && (cmd.Name() == "evalsha" || cmd.Name() == "eval") {
			<-ctx.Done()
		}
		return err
	}
}

func (*resumeLate) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
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
			select {
			case <-caught:
			case err := <-done:
				t.Fatalf("the read finished without a page carrying the command (handed out %v, err=%v)", ids(delivered), err)
			}
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
			select {
			case <-caught:
			case err := <-done:
				t.Fatalf("the claim finished without its answer carrying the command (claimed %v, err=%v)", ids(claimed), err)
			}
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
			select {
			case <-caught:
			case err := <-done:
				t.Fatalf("the read finished without a page carrying the command (handed out %v, err=%v)", ids(delivered), err)
			}
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
// slowResend is how long a loaded machine took to send a claim again on a fresh connection
// after the first one's answer was dropped, and patientDelay a claim delay that outlasts it
// with room to spare.
const (
	slowResend   = 3 * cutWindow / 2
	patientDelay = 5 * cutWindow
)

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
		stillAClaims := func(t *testing.T, commandID string, claim func(context.Context) ([]transport.Delivery, error), delay time.Duration) {
			t.Helper()
			if holder := f.pendingUnder(t, commandID, "s1"); holder != "inst-a" {
				t.Fatalf("%s is pending under %q after the claim, want inst-a", commandID, holder)
			}
			if delivered, err := read(t, adopter, "s1"); err != nil || len(delivered) != 0 {
				t.Fatalf("the next read handed out %v (err=%v), want nothing: it is a claim's", ids(delivered), err)
			}
			// The age is the subject: the claim that reset it needs the delay to pass again.
			time.Sleep(delay + delay/2)
			again, err := claim(ctx)
			if err != nil || !slices.Equal(ids(again), []string{commandID}) || !again[0].Redelivered {
				t.Fatalf("the next claim took %v (err=%v), want %s as a redelivery", ids(again), err, commandID)
			}
			ackAll(t, again)
		}
		claimSessions := func(ctx context.Context) ([]transport.Delivery, error) {
			return adopter.ClaimSessions(ctx, []string{"s1"})
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
			stillAClaims(t, "held-claim", claimSessions, claimDelay)
		})

		t.Run("its answer dropped with the connection", func(t *testing.T) {
			// Sent again by go-redis on a fresh connection, the claim finds the command it
			// already moved too young to take, and comes back empty with no error.
			if f.fake {
				t.Skip("miniredis's XCLAIM never checks the min idle time, so the claim sent again takes the command a second time")
			}
			// On a loaded machine the dial and the resend take a while (#334). The client
			// here waits that long on purpose before it sends again, and the claim delay is
			// long enough that the resend still finds the command too young.
			options := *f.through.Options()
			options.MinRetryBackoff, options.MaxRetryBackoff = slowResend, slowResend
			rdb := redis.NewClient(&options)
			rdb.AddHook(passReads{proxy: f.proxy})
			t.Cleanup(func() { _ = rdb.Close() })
			slow, err := redisstream.New(redisx.Wrap(rdb, f.client.Keys().Prefix(), shards),
				redisstream.Options{Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: claimDelay})
			if err != nil {
				t.Fatalf("redisstream.New: %v", err)
			}
			reclaim := func(ctx context.Context) ([]transport.Delivery, error) {
				return slow.Claim(ctx, []string{"s1"})
			}
			// Primed first: a transport's first look at a stream asks for its last entry,
			// which carries the marker, and the trap would spring on that instead.
			if _, err := read(t, slow, "s1"); err != nil {
				t.Fatalf("priming read: %v", err)
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
			stillAClaims(t, "dropped-claim", reclaim, claimDelay)
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
			stillAClaims(t, "given-back", claimSessions, claimDelay)
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
		select {
		case <-caught:
		case got := <-done:
			t.Fatalf("the read finished without a page carrying the wake (handed out %v, err=%v)", ids(got.delivered), got.err)
		}
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
			select {
			case <-caught:
			case got := <-done:
				t.Fatalf("the read finished without its answer carrying the command (handed out %v, err=%v)", ids(got.delivered), got.err)
			}
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

// A command recovered promptly is a first delivery: nobody has run it and its sender is
// still waiting. One that sat unseen past the claim delay is what a claim would have handed
// out as a redelivery, and it is one: its sender may have given up, and a full session queue
// must leave it pending rather than refuse it on the strength of a caller still listening.
func TestACommandRecoveredPastTheClaimDelayIsARedelivery(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		const claimDelay = 2 * cutWindow

		streams := f.streamsWith(t, &redisstream.Options{Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: claimDelay})
		stream := f.client.Keys().Commands("s1")
		if _, err := read(t, streams, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}

		writeCommand(t, f.fleet, stream, command("prompt", "s1", ""))
		f.loseTheAnswer(t, "held past the window", streams, "inst-a", "prompt", "s1")
		delivered, err := read(t, streams, "s1")
		if err != nil || !slices.Equal(ids(delivered), []string{"prompt"}) || delivered[0].Redelivered {
			t.Fatalf("handed out %v (err=%v), want the command recovered promptly as a first delivery", ids(delivered), err)
		}
		ackAll(t, delivered)

		writeCommand(t, f.fleet, stream, command("stranded", "s1", ""))
		f.loseTheAnswer(t, "held past the window", streams, "inst-a", "stranded", "s1")
		// The age is the subject: the command sits unseen past the claim delay.
		time.Sleep(claimDelay)
		delivered, err = read(t, streams, "s1")
		if err != nil || !slices.Equal(ids(delivered), []string{"stranded"}) || !delivered[0].Redelivered {
			t.Fatalf("handed out %v (err=%v), want the command stranded past the claim delay as a redelivery", ids(delivered), err)
		}
		ackAll(t, delivered)
	})
}

// A process restarted under the same instance name reads the same pending list, and what its
// predecessor was handed and never acknowledged is on it. That is not a lost answer of this
// process: a wake among it may name a session whose lease the predecessor still holds, and
// handed out now it would find that lease live and be retired. The claim delay outlasts a
// lease, which is why such entries are a claim's.
func TestWhatAPredecessorUnderTheSameNameLeftPendingIsLeftToAClaim(t *testing.T) {
	cutBackends(t, func(t *testing.T, f cutFleet) {
		const claimDelay = 2 * cutWindow

		predecessor := f.streamsWith(t, &redisstream.Options{Instance: "inst-x", Block: 50 * time.Millisecond, ClaimMinIdle: claimDelay})
		if _, err := read(t, predecessor, "s1"); err != nil {
			t.Fatalf("priming read: %v", err)
		}
		writeCommand(t, f.fleet, f.client.Keys().Control(), &protocol.Command{
			V: protocol.Version, ID: "inherited-wake", Type: protocol.CommandSessionWake, SID: "s9",
			TS: 1787000000000, Payload: []byte(`{"desired":"connected"}`),
		})
		writeCommand(t, f.fleet, f.client.Keys().Commands("s1"), command("inherited-status", "s1", ""))
		if taken, err := read(t, predecessor, "s1"); err != nil || len(taken) != 2 {
			t.Fatalf("the predecessor read %v (err=%v), want both commands", ids(taken), err)
		}

		// The predecessor dies holding both. Idle times are whole milliseconds, so the restart
		// is a few of them later.
		time.Sleep(5 * time.Millisecond)
		restarted := f.streamsWith(t, &redisstream.Options{Instance: "inst-x", Block: 50 * time.Millisecond, ClaimMinIdle: claimDelay})
		if delivered, err := read(t, restarted, "s1"); err != nil || len(delivered) != 0 {
			t.Fatalf("the restarted process handed out %v (err=%v), want what its predecessor held left to a claim", ids(delivered), err)
		}

		// And a claim does take them, once the delay has passed.
		time.Sleep(claimDelay)
		ctx := context.Background()
		wakes, err := restarted.ClaimControl(ctx)
		if err != nil || !slices.Equal(ids(wakes), []string{"inherited-wake"}) {
			t.Fatalf("the claim took %v (err=%v), want the wake", ids(wakes), err)
		}
		sessions, err := restarted.Claim(ctx, []string{"s1"})
		if err != nil || !slices.Equal(ids(sessions), []string{"inherited-status"}) {
			t.Fatalf("the claim took %v (err=%v), want the session command", ids(sessions), err)
		}
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
