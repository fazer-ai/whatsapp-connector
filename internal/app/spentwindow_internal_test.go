package app

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	"github.com/fazer-ai/whatsapp-connector/internal/observability"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx/redisxtest"
	"github.com/fazer-ai/whatsapp-connector/internal/session"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
	"github.com/fazer-ai/whatsapp-connector/internal/transport/redisstream"
)

// heldAnswer is a connector whose Redis is reached through a proxy, so a test can hold the
// answer to one read past the window it was given. The client carries production's
// `ContextTimeoutEnabled`, which is what makes the socket's deadline the window's own.
type heldAnswer struct {
	connector *Connector
	client    *redisx.Client
	proxy     *redisxtest.Proxy
	said      *bytes.Buffer
}

func withHeldAnswers(t *testing.T, sid string, options ...func(*redis.Options)) *heldAnswer {
	t.Helper()

	server := miniredis.RunT(t)
	proxy := redisxtest.Listen(t, server.Addr())
	settings := &redis.Options{Addr: proxy.Addr(), ContextTimeoutEnabled: true}
	for _, option := range options {
		option(settings)
	}
	rdb := redis.NewClient(settings)
	t.Cleanup(func() { _ = rdb.Close() })
	client := redisx.Wrap(rdb, "wa:", DefaultEventShards)

	// A block short enough to leave the window room for the answer, and a claim delay
	// long enough that nothing can come back that way: what returns, returns by a read.
	streams, err := redisstream.New(client, redisstream.Options{
		Instance: "inst-a", Block: 50 * time.Millisecond, ClaimMinIdle: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("redisstream.New: %v", err)
	}
	manager := session.NewManager(&session.ManagerConfig{
		Instance: "inst-a", Engine: fake.New(),
		Leases:    cluster.NewLeases(client, "inst-a", cluster.Options{}),
		Publisher: quietPublisher{}, Replier: &orderedReplier{},
		NewID: func() string { return "evt" }, Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { manager.StopAll(context.Background()) })
	if _, err := manager.Adopt(context.Background(), sid); err != nil {
		t.Fatalf("Adopt(%s): %v", sid, err)
	}
	manager.TakeNewlyAdopted()

	said := &bytes.Buffer{}
	return &heldAnswer{
		connector: &Connector{
			metrics: observability.New(),
			cfg:     Config{LeaseTTL: time.Minute, Heartbeat: time.Second},
			log:     zerolog.New(said).Level(zerolog.DebugLevel),
			manager: manager, streams: streams,
		},
		client: client, proxy: proxy, said: said,
	}
}

// skewedWindow is the losing side of the race this issue is about, made deterministic: a
// window whose deadline is `at` -- which is the deadline the socket takes -- and whose
// Done and Err only follow `lag` later. Asking the context who won gives the wrong answer
// for that whole interval, which on a loaded box is where these failures come from.
type skewedWindow struct {
	context.Context

	deadline time.Time
}

func (w skewedWindow) Deadline() (time.Time, bool) { return w.deadline, true }

func windowSkewedBy(t *testing.T, at, lag time.Duration) context.Context {
	t.Helper()

	late, cancel := context.WithDeadline(context.Background(), time.Now().Add(at+lag))
	t.Cleanup(cancel)
	return skewedWindow{Context: late, deadline: time.Now().Add(at)}
}

// A read that spends its window is not a failed read, and production is where the two are
// told apart wrongly: the socket deadline and the window deadline are the same instant,
// fired by different timers, and whenever the netpoll timer wins `ctx.Err()` is still nil.
// Counting that as a failure grows `wac_command_reads_failed_total` for a reason that is
// not a failure, and three in a row print an alarm about an instance that is serving fine.
func TestAWindowSpentWaitingForAnAnswerIsNotAFailedRead(t *testing.T) {
	t.Parallel()

	const sid = "2f1c6f0e-0000-4000-8000-000000000901"
	bench := withHeldAnswers(t, sid)
	writeStatus(t, bench.client, sid, "c209-s1")

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	caught := bench.proxy.Hold("c209-s1", release)

	window := windowSkewedBy(t, 200*time.Millisecond, 200*time.Millisecond)
	bench.connector.readCommands(window)

	select {
	case <-caught:
	default:
		t.Fatal("the answer this test holds was never caught, so no window was spent waiting for it")
	}
	if failures := gathered(t, bench.connector, "wac_command_reads_failed_total"); failures != 0 {
		t.Fatalf("a spent window counted %v failed read(s), want 0:\n%s", failures, bench.said)
	}
	if bytes.Contains(bench.said.Bytes(), []byte("failed to read commands")) {
		t.Fatalf("a spent window was reported as a failed read:\n%s", bench.said)
	}
}

// The other half, and the one a careless fix breaks: a window that ends with nothing read
// must not move the clock that says when commands were last read either. That gauge is
// what an operator has to see an instance that has gone quiet, and a fix that calls a
// spent window a success leaves it advancing on an instance reading nothing at all.
func TestAWindowSpentWaitingForAnAnswerIsNotASuccessfulReadEither(t *testing.T) {
	t.Parallel()

	const sid = "2f1c6f0e-0000-4000-8000-000000000903"
	bench := withHeldAnswers(t, sid)
	writeStatus(t, bench.client, sid, "c209-s3")

	before := gathered(t, bench.connector, "wac_command_read_last_success_timestamp_seconds")

	release := make(chan struct{})
	caught := bench.proxy.Hold("c209-s3", release)
	bench.connector.readCommands(windowSkewedBy(t, 200*time.Millisecond, 200*time.Millisecond))
	select {
	case <-caught:
	default:
		t.Fatal("the answer this test holds was never caught, so no window was spent waiting for it")
	}
	if after := gathered(t, bench.connector, "wac_command_read_last_success_timestamp_seconds"); after != before {
		t.Fatalf("a spent window moved the last-success clock from %v to %v", before, after)
	}
	close(release)

	// And a read that does come back moves it, so the gauge is still measuring something.
	window, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	bench.connector.readCommands(window)
	if after := gathered(t, bench.connector, "wac_command_read_last_success_timestamp_seconds"); after <= before {
		t.Fatalf("a read that came back left the last-success clock at %v, want past %v", after, before)
	}
}

// answeredWith is a transport whose read comes back with one error, chosen by the test.
// It stands in for the client where the client cannot be made to produce the case: the
// negatives below need a real net timeout and a real non-timeout, on a window the test
// decides the state of.
type answeredWith struct{ err error }

func (s answeredWith) Read(context.Context, []string) ([]transport.Delivery, error) {
	return nil, s.err
}
func (answeredWith) Claim(context.Context, []string) ([]transport.Delivery, error) { return nil, nil }
func (answeredWith) ClaimControl(context.Context) ([]transport.Delivery, error)    { return nil, nil }
func (answeredWith) ClaimSessions(context.Context, []string) ([]transport.Delivery, error) {
	return nil, nil
}

// A timeout error of the kind the network returns, which is what the classification has to
// recognise: `os.ErrDeadlineExceeded` is what a socket past its deadline gives, and
// net.OpError is the shape go-redis hands up.
func socketTimeout() error {
	return &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
}

// The negative, and the reason the fix cannot simply stop counting timeouts: a read that
// times out while its window still has time left is a Redis that did not answer, and that
// is a failure whoever asks. Measured with a transport standing in for the client, because
// the real one cannot produce it: go-redis gives a blocking command `block + 10s`, so the
// deadline it takes is always the window's, whatever ReadTimeout the client carries.
func TestATimeoutWithTheWindowStillOpenIsStillAFailedRead(t *testing.T) {
	t.Parallel()

	connector, _, said := muted(t)
	connector.streams = answeredWith{err: socketTimeout()}
	// A window with seconds to spare, so nothing here is about a deadline running out.
	window, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	connector.readCommands(window)

	if failures := gathered(t, connector, "wac_command_reads_failed_total"); failures != 1 {
		t.Fatalf("a timeout inside an open window counted %v failure(s), want 1:\n%s", failures, said)
	}
	if !bytes.Contains(said.Bytes(), []byte("failed to read commands")) {
		t.Fatalf("a timeout inside an open window was not reported:\n%s", said)
	}
}

// The other half of the negative: past the deadline, only a timeout is the window running
// out. A Redis that refuses the read answers at once and would answer the same with the
// whole window ahead of it, so a classification that goes by the clock alone would hide
// every refusal that happened to land late in a window.
func TestAnErrorThatIsNotATimeoutIsAFailedReadEvenPastTheDeadline(t *testing.T) {
	t.Parallel()

	connector, _, said := muted(t)
	connector.streams = answeredWith{err: errors.New("READONLY You can't write against a read only replica")}
	// Deadline already past, and a context that has not caught up with it: the state the
	// spent-window branch exists for, with an error that is not one.
	connector.readCommands(windowSkewedBy(t, -time.Millisecond, time.Minute))

	if failures := gathered(t, connector, "wac_command_reads_failed_total"); failures != 1 {
		t.Fatalf("a refusal past the deadline counted %v failure(s), want 1:\n%s", failures, said)
	}
	if !bytes.Contains(said.Bytes(), []byte("failed to read commands")) {
		t.Fatalf("a refusal past the deadline was not reported:\n%s", said)
	}
}

// The review's case, and the reason the window that ran out has a sentinel of its own: a
// dial that gave up on the client's own timeout comes back carrying
// `context.DeadlineExceeded`, with the window wide open. Suppressing everything that
// carries that value would hide a Redis this instance never reached, which is the outage
// the alarm exists for.
func TestADialThatGaveUpIsAFailedReadEvenThoughItCarriesADeadline(t *testing.T) {
	t.Parallel()

	connector, _, said := muted(t)
	connector.streams = answeredWith{
		err: &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded},
	}
	window, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	connector.readCommands(window)

	if failures := gathered(t, connector, "wac_command_reads_failed_total"); failures != 1 {
		t.Fatalf("a dial that gave up counted %v failure(s), want 1:\n%s", failures, said)
	}
	if !bytes.Contains(said.Bytes(), []byte("failed to read commands")) {
		t.Fatalf("a dial that gave up was not reported:\n%s", said)
	}
}
