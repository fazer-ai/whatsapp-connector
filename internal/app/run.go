package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/engine"
	"github.com/fazer-ai/whatsapp-connector/internal/engine/fake"
	meow "github.com/fazer-ai/whatsapp-connector/internal/engine/whatsmeow"
	"github.com/fazer-ai/whatsapp-connector/internal/httpserver"
	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/observability"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/session"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
	"github.com/fazer-ai/whatsapp-connector/internal/transport"
	"github.com/fazer-ai/whatsapp-connector/internal/transport/redisstream"
)

// Version is the build's version, set by the linker in a release build.
var Version = "dev"

// StartupTimeout bounds the work New does before it can serve: dialling Redis, and
// bringing the device store's schema up.
const StartupTimeout = 60 * time.Second

// ShutdownGrace is how long a stopping instance is given to release its sessions. It
// is the reason `stop_grace_period` in the compose file is generous: a release is a
// peer picking the session up in seconds instead of waiting out the lease.
const ShutdownGrace = 20 * time.Second

// Connector is a running instance.
type Connector struct {
	cfg        Config
	log        zerolog.Logger
	metrics    *observability.Metrics
	client     *redisx.Client
	leases     *cluster.Leases
	quarantine *cluster.Quarantine
	registry   *cluster.Registry
	manager    *session.Manager
	engine     engine.Engine
	store      *store.Container
	// resumePasses counts the finished sweeps, and is read by tests that have to know a
	// pass is over before they act.
	resumePasses atomic.Uint64
	streams      commandStreams
	// seen is when each label value last had something counted against it, for the two
	// metrics whose label values have no ceiling of their own: a session id, and the name
	// of a consumer a claim took work back from. Neither is bounded -- nothing limits how
	// many sessions an instance adopts over its life, and an instance name defaults to
	// the hostname, so a fleet of constant size still coins a new one every time a
	// replica is replaced.
	//
	// Dropped on going quiet rather than on the session going away, and that distinction
	// is the whole point of the metric: a wake for a session this instance cannot adopt
	// is exactly the case it exists for, and that session is in nobody's owned list.
	// Evicting by ownership deleted its series on every heartbeat, so the one series that
	// mattered read as a run of resets, or was missed by a scrape altogether.
	seenMu sync.Mutex
	seen   map[seenLabel]time.Time
	http   *httpserver.Server
	blobs  *media.Store

	// reclaimCursor is where the next reclaim pass starts. Read and written only by the
	// loop goroutine, which is also the only one that reclaims.
	reclaimCursor int

	// silentReads is how many command reads have failed in a row, and silentSince is when
	// that run began. Read and written only by the loop goroutine, which is the only one
	// that reads commands.
	silentReads  int
	silentSince  time.Time
	saidItIsMute bool

	// partSwept is signalled after every completed pass of the refetch sweep, and only
	// the tests set it. What they have to know is that the loop made its pass, and the
	// two ways of learning that without a signal are both wrong: polling the table on a
	// clock is the wall-clock synchronisation AGENTS.md rules out, and calling one pass
	// directly stops exercising the loop, which is the thing under test.
	partSwept chan<- struct{}
}

// New builds a connector: it dials Redis, agrees with the fleet on the shard count,
// and prepares everything without listening or reading yet.
//
//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func New(cfg *Config, log zerolog.Logger) (connector *Connector, err error) {
	client, err := redisx.New(redisx.Config{
		URL: cfg.RedisURL, Password: cfg.RedisPass, Prefix: cfg.RedisPrefix, Shards: cfg.EventShards,
	})
	if err != nil {
		return nil, err
	}
	if err := client.Ping(context.Background(), 5*time.Second); err != nil {
		return nil, err
	}
	if err := client.ClaimMeta(context.Background(), redisx.Meta{
		ProtocolMin: protocol.MinVersion, ProtocolMax: protocol.Version, Shards: cfg.EventShards,
	}); err != nil {
		return nil, err
	}

	// Bounded: an unreachable database or a migration that will not finish would
	// otherwise leave the process starting forever, which an orchestrator reads as a
	// container that is simply slow.
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), StartupTimeout)
	defer cancelStartup()

	// The blob store comes first: a session downloads the file of an inbound message
	// straight into it, so the engine has to be built knowing whether there is one.
	//
	// Only when the deployment gave it somewhere to write. An instance with no blob
	// store does not register the endpoint at all, so a client that reaches it hears
	// 404 and asks the session for the bytes again, which is what it does for a blob
	// that has aged out anyway.
	var (
		blobs       *media.Store
		blobHandler http.Handler
		mediaOpts   meow.MediaOptions
	)
	// Set whether or not there is a blob root: sending a file does not go through the
	// cache, and an instance told to keep nothing still has to be able to send.
	mediaOpts.SendMax = cfg.MediaSendMax
	if cfg.MediaRoot != "" {
		blobs, err = media.New(media.Options{
			Root: cfg.MediaRoot, TTL: cfg.MediaTTL,
			Quota: cfg.MediaQuota, MaxBlob: cfg.MediaMaxBlob, BlockSize: cfg.MediaBlockSize,
		})
		if err != nil {
			return nil, err
		}
		// Opening the store before the engine also put it before every remaining way
		// this function gives up, and the store holds its root directory open. Let go
		// of unless a connector is actually returned to hold it.
		defer func() {
			if connector == nil {
				_ = blobs.Close()
			}
		}()
		blobHandler = media.Handler(media.HandlerOptions{Blobs: blobs, Token: cfg.MediaToken})
		// Assigned only in here. A nil *media.Store put in the interface is not a nil
		// interface, and every session would then take the store path and fail on it.
		mediaOpts.Blobs, mediaOpts.BaseURL = blobs, cfg.AdvertiseURL
	}

	// Before the engine, because the store the engine opens fences every write against
	// them: a write is refused once this instance's claim has run out, and not only once
	// it has been told so.
	leases := cluster.NewLeases(client, cfg.Instance, cluster.Options{TTL: cfg.LeaseTTL})

	// Built before the engine, which is the one thing here that takes a metric rather
	// than being read by one: the engine reports how long its emissions queued, and
	// there was no route from that package to this set until now (#221, #226).
	metrics := observability.New()
	// Registered here rather than inside observability.New, because the value it reports
	// lives in Redis and that package deliberately knows about nothing but Prometheus.
	metrics.Registry.MustRegister(redisx.NewStreamLag(client))

	waEngine, devices, err := newEngine(
		startupCtx, cfg, leases.Owns, mediaOpts, queueingInto(metrics), log)
	if err != nil {
		return nil, err
	}

	streams, err := redisstream.New(client, streamOptions(cfg, &log))
	if err != nil {
		return nil, err
	}

	quarantine := cluster.NewQuarantine(client, nil)
	watch := &watching{metrics: metrics}
	manager := session.NewManager(&session.ManagerConfig{
		Instance: cfg.Instance, Engine: waEngine, Leases: leases,
		Publisher: countingPublisher{to: streams, metrics: metrics}, Replier: streams,
		Ledger:     redisx.NewIdempotency(client, 0),
		Watch:      watch,
		Store:      devices,
		Quarantine: quarantine,
		NewID:      newFrameID, Logger: log,
	})

	c := &Connector{
		cfg: *cfg, log: log, metrics: metrics, client: client, leases: leases, quarantine: quarantine,
		registry: cluster.NewRegistry(client, 3*cfg.Heartbeat), manager: manager, engine: waEngine,
		store: devices, blobs: blobs,
	}
	watch.owner = c

	c.http = httpserver.New(httpserver.Options{
		Addr: cfg.HTTPAddr, Health: c, Registry: metrics.Registry,
		Version: Version, Instance: cfg.Instance, Media: blobHandler,
	})
	c.streams = streams
	return c, nil
}

// commandStreams is the half of the transport the loop takes commands from.
//
// An interface for two reasons, and the loop reaches these four calls from both. The
// bound it grants its optional work is granted here and nowhere else, so a test that
// reads it off anything downstream is inferring it -- it used to be inferred from the
// deadline an `admin.ping` was answered under, which worked only while the manager
// answered a ping on the goroutine that dispatched it, and stopped meaning anything the
// moment that stopped being true. And a session is held out of the read the way a real
// one is held, by a drain that cannot take its stream over, so what happens to the
// sessions beside it is watched directly rather than through a command's fate, which is
// a consequence several paths reach.
type commandStreams interface {
	Read(ctx context.Context, sids []string) ([]transport.Delivery, error)
	Claim(ctx context.Context, sids []string) ([]transport.Delivery, error)
	ClaimControl(ctx context.Context) ([]transport.Delivery, error)
	ClaimSessions(ctx context.Context, sids []string) ([]transport.Delivery, error)
}

// Ready reports whether this instance can serve: Redis, without which it can neither
// publish nor be told anything, and the device store when the engine has one.
func (c *Connector) Ready(ctx context.Context) error {
	if err := c.client.Ping(ctx, 2*time.Second); err != nil {
		return err
	}
	if c.store == nil {
		return nil
	}
	// The database is not only a startup dependency: adopting a session reads its
	// device and pairing one writes it. Reporting ready without it has an orchestrator
	// send this instance sessions it cannot open.
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return c.store.Ping(pingCtx)
}

// Handler is what the HTTP server serves, so a test can exercise the routes this
// instance actually registered without going through a socket.
func (c *Connector) Handler() http.Handler { return c.http.Handler() }

// Sessions is how many sessions this instance runs.
func (c *Connector) Sessions() int { return c.manager.Count() }

// ResumePasses is how many sweeps over the accounts that should be in the air this
// instance has finished. Exposed for the same reason Sessions is: a test that has to know
// a pass has already happened cannot see it any other way, and the alternative is waiting
// on the clock, which this repository does not do to synchronise.
func (c *Connector) ResumePasses() uint64 { return c.resumePasses.Load() }

// Run serves until the context ends or a signal arrives, then releases the sessions.
func (c *Connector) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	httpErr := make(chan error, 1)
	go func() { httpErr <- c.http.Start() }()

	// On a context of its own rather than on the one the loop watches, because the loop
	// has an exit the context knows nothing about: an HTTP server that cannot listen
	// ends the run while nothing has cancelled anything. Waiting on a sweeper that is
	// still ticking would hang the process on exactly the startup failure it is trying
	// to report.
	// Two sweepers on two contexts, because their orders around the shutdown are
	// opposite ones. The blob sweep must not walk a directory the shutdown is still
	// writing to, so it stops after. The database sweep holds a statement on the pool
	// the shutdown closes, so it stops before: sql.DB.Close waits for what is in flight,
	// and a sweeper still ticking behind it answers ErrConnDone on every pass.
	sweepingBlobs, stopBlobSweep := context.WithCancel(ctx)
	swept := c.sweepBlobs(sweepingBlobs)
	sweepingParts, stopPartSweep := context.WithCancel(ctx)
	sweptParts := c.sweepMediaParts(sweepingParts)

	// On a context of its own like the two above, and stopped before the answering
	// goroutine below for the reason that one is stopped before the shutdown: it queues
	// adoptions there, and an adoption queued while the instance is going away is an
	// account taken by one that is about to hand everything back.
	resuming, stopResuming := context.WithCancel(ctx)
	resumed := c.resumeWanted(resuming)

	// On a context of its own, like the sweepers and for the same reason: the loop has
	// an exit the context knows nothing about, and waiting on a goroutine nothing has
	// cancelled would hang the process on exactly the startup failure it is reporting.
	answering, stopAnswering := context.WithCancel(ctx)
	answered := c.manager.Answer(answering)

	c.log.Info().
		Str("addr", c.cfg.HTTPAddr).
		Str("engine", c.cfg.Engine).
		Int("shards", c.cfg.EventShards).
		Msg("connector is up")

	err := c.loop(ctx, httpErr)
	// Before the shutdown, which closes the pool this one is querying.
	stopPartSweep()
	<-sweptParts

	// Before the answering goroutine, because what it does is queue work onto it, and
	// before the shutdown, because its own pass reads the database the shutdown closes.
	stopResuming()
	<-resumed

	// Before the shutdown as well, and for both of the things it closes: an adoption in
	// flight is reading the device store, and every acknowledgement is a round trip to
	// Redis. Stopping it here also means the hand-back below is not racing a wake that
	// would adopt a session back onto an instance that is going away.
	stopAnswering()
	<-answered

	c.shutdown()
	// After the loop, so the sweep is not walking a directory the shutdown is still
	// writing to, and waited for, so the process does not exit mid-rename.
	stopBlobSweep()
	<-swept
	if c.blobs != nil {
		// After the sweeper has stopped, so nothing is walking the directory through a
		// handle that is being closed.
		if closeErr := c.blobs.Close(); closeErr != nil {
			c.log.Warn().Err(closeErr).Msg("could not close the media store")
		}
	}
	return err
}

// loop is the instance's single scheduler: it reads commands, hands them to the
// manager, and on every tick renews the leases and says it is alive.
//
// Reading and renewing share a goroutine, so everything that is not the tick's own
// work runs under one deadline: when the next renewal is due. The bound used to be a
// sum of per-step budgets, and the sum came out wrong three times running (#7 lists
// them), because it was a list somebody had to remember in full. A deadline derived
// from the tick cannot be wrong that way: however many steps the optional work grows,
// together they cannot outlive the period they started in.
func (c *Connector) loop(ctx context.Context, httpErr <-chan error) error {
	heartbeat := time.NewTicker(c.cfg.Heartbeat)
	defer heartbeat.Stop()

	c.announce(ctx)
	due := time.Now().Add(c.cfg.Heartbeat)
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-httpErr:
			return serverEnded(err)
		case <-heartbeat.C:
			due = c.tick(ctx)
		default:
			window, cancel := context.WithDeadline(ctx, due)
			c.readCommands(window)
			spent := window.Err() != nil
			cancel()
			if !spent || ctx.Err() != nil {
				continue
			}
			// The window is gone, and the next thing this goroutine owes the fleet is
			// a renewal. Falling back to the ticker is what keeps a spent window from
			// becoming a hot loop: a read whose deadline has already passed comes back
			// empty immediately, and reading again on it would hammer Redis from here
			// to the tick.
			select {
			case <-ctx.Done():
				return nil
			case err := <-httpErr:
				return serverEnded(err)
			case <-heartbeat.C:
				due = c.tick(ctx)
			}
		}
	}
}

// tick is the heartbeat branch: renewals first, and nothing before them. It returns
// when the renewal after this one is due, which is the deadline everything outside
// this branch runs under.
func (c *Connector) tick(ctx context.Context) time.Time {
	due := time.Now().Add(c.cfg.Heartbeat)
	// One deadline for both, because both hand leases back and the tail the startup check
	// prices is one tail.
	handBackBy := c.manager.HandBackBy()
	c.manager.RenewAll(ctx, handBackBy)
	c.manager.SweepRetired(ctx, handBackBy)
	c.reclaimCommands(ctx)
	c.announce(ctx)
	c.metrics.SessionsRunning.Set(float64(c.manager.Count()))
	c.forgetLabelsGoneQuiet(time.Now())
	return due
}

// serverEnded turns the HTTP server's exit into the loop's own result.
func serverEnded(err error) error {
	if err != nil {
		return fmt.Errorf("app: http server: %w", err)
	}
	return nil
}

// The bounds on how often the media cache is walked. What the cadence decides is how
// long a blob outlives the age it was supposed to be kept for, and how long the disk
// stays over quota after a burst; what it costs is a walk of the whole cache.
//
// A minute is the ceiling because that is short enough for both against any ordinary
// TTL. The floor is there because the cadence follows the TTL down: a deployment that
// asks for a minute of retention and is swept once a minute keeps its media for two, so
// a short TTL has to be swept often to mean anything, and a second is as often as this
// is willing to walk a cache that may be large.
const (
	blobSweepMax = time.Minute
	blobSweepMin = time.Second
)

// BlobSweep is how often to walk, for a store keeping blobs for ttl. Half the TTL, so a
// blob outlives it by at most half again rather than by a whole fixed interval.
func BlobSweep(ttl time.Duration) time.Duration {
	return min(max(ttl/2, blobSweepMin), blobSweepMax)
}

// sweepBlobs drops the media nobody collected, on a goroutine of its own. The returned
// channel closes once it has stopped.
//
// Not on the heartbeat, and this is the point rather than tidiness: that goroutine is
// what renews every lease this instance holds, and a walk of a cache with many files on
// a slow disk can outlast a lease. Peers would then adopt sessions whose sockets are
// still open here, which is the one invariant nothing downstream can recover from. The
// sweep has no deadline it must meet, so it gets its own goroutine and can take as long
// as the disk makes it take.
func (c *Connector) sweepBlobs(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	if c.blobs == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(BlobSweep(c.cfg.MediaTTL))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			dropped, freed, err := c.blobs.Sweep(ctx)
			switch {
			case errors.Is(err, context.Canceled):
				// The sweep gave up because the process is stopping, which is what was
				// asked of it.
				return
			case err != nil:
				c.log.Warn().Err(err).Msg("could not sweep the media store")
			case dropped > 0:
				c.log.Debug().Int("blobs", dropped).Int64("bytes", freed).
					Msg("dropped media nobody collected")
			}
		}
	}()
	return done
}

// PartSweep is how often to sweep, for a deployment keeping refetch metadata for ttl.
//
// An hour at most, and a twentieth of the retention below that. The bound is loose on
// purpose: unlike a blob cache, nothing is waiting on this to free anything, and a row
// outliving its retention by an hour costs a row. What it must not do is run so often
// that a fleet of instances spends its day deleting nothing from the same table.
func PartSweep(ttl time.Duration) time.Duration {
	return min(max(ttl/20, time.Minute), time.Hour)
}

// sweepMediaParts drops the refetch metadata that has outlived its retention.
//
// Its own goroutine for the same reason the blob sweep has one: it must not sit on the
// heartbeat, which is what renews every lease this instance holds. A DELETE over a large
// table on a busy database is not a thing to put in front of a lease.
//
// Every instance sweeps, and they sweep the same rows. That is not a race worth
// coordinating away: the statement is idempotent, the loser deletes nothing, and a
// leader election for a DELETE would be more machinery than the problem.
func (c *Connector) sweepMediaParts(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	if c.store == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(PartSweep(c.cfg.MediaRefetch))
		defer ticker.Stop()
		for {
			// Swept before the first tick rather than after it. The cadence runs to an
			// hour and every restart builds a new ticker, so a fleet that ships several
			// times a day would reach the first tick on none of its instances: the
			// retention an operator configured would be a setting nothing ever enforces,
			// and the table would grow for the life of the deployment. The blob sweep
			// does not need this because its cadence tops out at a minute.
			if done := c.sweepPartsOnce(ctx); done {
				return
			}
			if c.partSwept != nil {
				// Never blocking: a listener that has stopped reading must not be able
				// to hold the sweep still.
				select {
				case c.partSwept <- struct{}{}:
				default:
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return done
}

// The shape of the resume sweep. Together they say how fast a fleet comes back: one pass
// every interval, at most this many accounts asked for per pass per instance, so a
// hundred sessions are all asked for inside a minute and a half on a single instance and
// proportionally faster on more.
//
// The batch is what keeps a restart from being a stampede. Every session brought back
// dials WhatsApp, and an instance that adopted every account it found at once would open
// hundreds of sockets in the same second, on a process that has just started.
//
// The cool-off is fleet-wide and is what makes an account that cannot connect cost one
// attempt per window instead of one per instance per pass: a session that fails to come
// back is retired by its engine, the lease goes back, and without the mark the next pass
// would find it free and try again immediately, forever. It is not quarantine -- a
// session that is permanently unable to connect still costs an attempt a minute, which is
// fazer-ai/whatsapp-connector#102 -- it is the floor under the retry.
const (
	resumeInterval = 30 * time.Second
	resumeBatch    = 8
	resumeCooloff  = time.Minute
)

// resumeWanted brings back the sessions a client asked to have connected and that no
// instance is running, on a goroutine of its own. The returned channel closes once it has
// stopped.
//
// Not on the heartbeat, and for the reason the two sweeps above are not: that goroutine
// renews every lease this instance holds, and this pass reads a database and talks to
// Redis. A pass that took longer than a lease would have peers adopt accounts whose
// sockets are open here.
//
// What it exists for is the state nothing else notices. A lease dies with the instance
// that held it, a `session.wake` is a frame read once, and the client polls nothing:
// after a connector restart every paired account is unowned, the inbox still shows
// `open`, and nothing arrives until somebody opens it and presses connect. Measured on a
// production deployment (fazer-ai/chatwoot#577).
func (c *Connector) resumeWanted(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	if c.store == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		// Before the first tick rather than after it, and then again on the catch-up ramp
		// before settling into the interval. The case this is for is the instance that has
		// just started: waiting out an interval first would add it to the time an account
		// spends unowned after every deploy, and one pass on the way in is not enough,
		// because that pass runs while the instance being replaced is still the owner.
		for _, wait := range resumeCatchUp {
			c.resumeOnce(ctx)
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		ticker := time.NewTicker(resumeInterval)
		defer ticker.Stop()
		for {
			c.resumeOnce(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return done
}

// resumeCatchUp is how long the sweep waits between its first passes, before settling
// into resumeInterval for the rest of an instance's life. It exists for one measured case,
// and that case is every release.
//
// A deploy here is a rolling one: the replacement container starts, the one it replaces is
// signalled about seven tenths of a second later, and takes another second to let its
// leases go. So the incoming instance makes its pass on the way in while the outgoing one
// still owns everything, finds nothing anywhere, and on the steady interval alone does not
// look again for thirty seconds. Measured in production: the new container said
// `connector is up` at 13:15:50, the old one said `connector is down` at 13:15:51, and the
// account came back at 13:16:20. The one early pass was early by a second, and the account
// paid thirty for it.
//
// A ramp rather than a shorter interval, because the two do not cost the same: this spends
// five extra passes in the first half minute of an instance's life, once, where a shorter
// interval would spend them for as long as the fleet runs. And a ramp rather than one
// retry, because what it is racing is a shutdown whose length is the number of sessions
// the outgoing instance has to close, not a constant.
//
// The fleet asking in chorus is already handled and is not made worse: every pass goes
// through the `wa:resume:<sid>` cool-off, so one account is asked about once per window
// however many instances are sweeping.
var resumeCatchUp = []time.Duration{
	time.Second,
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
	16 * time.Second,
}

// resumeOnce makes one pass over the accounts that should be in the air.
func (c *Connector) resumeOnce(ctx context.Context) {
	// Counted on the way out, whatever the pass decided, so a reader waiting on it knows
	// the pass is over rather than merely started.
	defer c.resumePasses.Add(1)

	// Bounded, and on a context of its own: the pass has nothing waiting on it, and a
	// database or a Redis that hangs would otherwise hold this goroutine for as long as
	// it takes rather than for as long as a pass is worth.
	pass, cancel := context.WithTimeout(ctx, resumeInterval)
	defer cancel()

	wanted, err := c.store.Wanted(pass)
	if err != nil {
		c.log.Warn().Err(err).Msg("could not read which sessions should be connected")
		return
	}
	running := c.manager.SIDs()
	candidates := make([]string, 0, len(wanted))
	// What each client asked to receive, kept beside the list because everything between
	// here and the resume speaks in session ids alone: the leases and the backoff are
	// asked about a set of accounts, not about what any of them subscribed to.
	subscription := make(map[string]store.Wants, len(wanted))
	for _, session := range wanted {
		subscription[session.SID] = session.Wants
		if !slices.Contains(running, session.SID) {
			candidates = append(candidates, session.SID)
		}
	}
	free, err := c.leases.Unleased(pass, candidates)
	if err != nil {
		c.log.Warn().Err(err).Msg("could not tell which sessions are running somewhere")
		return
	}
	// The accounts the fleet has agreed to leave alone for now, because the last
	// attempts at them failed. Read for the whole list in one batch, like the leases: it
	// is the same question asked of every candidate.
	//
	// A read that fails is not a reason to skip the pass. What it costs is attempts at
	// accounts that were going to be left alone, which is the behaviour this had before
	// there was a backoff at all; what skipping costs is every healthy account staying
	// down because one read did not answer.
	waiting, err := c.waitingOut(pass, free)
	if err != nil {
		c.log.Warn().Err(err).Msg("could not read which sessions the fleet is leaving alone; trying them all")
	}

	asked := 0
	for _, sid := range free {
		if until, held := waiting[sid]; held {
			c.log.Debug().Str("sid", sid).Time("until", until).
				Msg("leaving a session that keeps failing alone")
			continue
		}
		if asked >= resumeBatch {
			return
		}
		// The mark is taken before the attempt, and taking it is what wins the turn: two
		// instances reading the same free account in the same second would otherwise both
		// adopt, and the loser's adoption is an account handed straight back.
		//
		// `SETNX` and a separate read, rather than the one `SET NX GET` that would answer
		// both: `NX` with `GET` is a syntax error before Redis 7.0, and measured on
		// 6.2.24 it is the whole pass that dies, because an error here aborts the loop
		// and the next pass makes the same call. That is every account in the fleet
		// staying down for good behind one WARN a pass, which is this defect made worse
		// rather than fixed. This repository declares no minimum Redis version and
		// `SETNX` needs none, so the second read is the price of not quietly setting one.
		won, err := c.client.SetNX(pass, c.client.Keys().Resume(sid), c.cfg.Instance, resumeCooloff).Result()
		if err != nil {
			c.log.Warn().Err(err).Str("sid", sid).Msg("could not take the turn to bring a session back")
			return
		}
		if !won && !c.tookTurnFromAnInstanceThatIsGone(pass, sid) {
			continue
		}
		if c.manager.Resume(sid, subscription[sid]) {
			asked++
			c.log.Info().Str("sid", sid).Msg("bringing back a session that should be connected and that nobody is running")
		}
	}
}

// tookTurnFromAnInstanceThatIsGone takes the resume turn of an account whose turn belongs
// to an instance that is no longer in the fleet, and reports whether it got it.
//
// The mark paces retries, and it does that by naming whoever is trying. When that instance
// dies the name stops meaning anything, but the mark keeps standing for the rest of its
// minute, and every survivor's pass skips the account on its account. The information that
// settles it is already in Redis and is already right: `wa:instance:<name>` is refreshed by
// the heartbeat and lives `3 * WAC_HEARTBEAT`, fifteen seconds by default, so from fifteen
// seconds after the last beat the registry knows what the mark does not. The gap between
// the two is the whole defect, and at the defaults it is forty-five seconds long.
//
// Asked only for the accounts the SETNX did not win, which is what keeps the ordinary pass
// exactly as expensive as it was: an account whose turn is free costs one command, as
// before, and an account that is already being skipped costs the two reads that say why.
//
// A live instance whose beat is late by more than the registry's TTL reads as gone here,
// and its turn is taken. That is a real cost and it is the smaller one: what the mark buys
// is that two instances do not both try, and what happens when they do is that the lease
// arbitrates and the loser hands its adoption straight back. Two attempts, never two
// sockets on one account. Weighed against an account nobody may touch for the rest of a
// minute, on the path where the fleet has just lost a process, it is worth it.
func (c *Connector) tookTurnFromAnInstanceThatIsGone(ctx context.Context, sid string) bool {
	mark := c.client.Keys().Resume(sid)
	holder, err := c.client.Get(ctx, mark).Result()
	switch {
	case errors.Is(err, redis.Nil):
		// Expired between the write that was refused and this read. The next pass finds
		// it free, which is the outcome this function is for, so there is nothing to take.
		return false
	case err != nil:
		c.log.Warn().Err(err).Str("sid", sid).Msg("could not read who holds the turn for a session")
		return false
	}
	if holder == c.cfg.Instance {
		// This instance's own turn, from a pass that has not finished with it. Not a
		// stranger's to take, and not a reason to start a second attempt behind the first.
		return false
	}
	alive, err := c.client.Exists(ctx, c.client.Keys().Instance(holder)).Result()
	if err != nil {
		c.log.Warn().Err(err).Str("sid", sid).Str("holder", holder).
			Msg("could not tell whether the instance holding a turn is still in the fleet")
		return false
	}
	if alive > 0 {
		return false
	}
	// Compare-and-set against the name that was read, not a plain overwrite: between the
	// read and here the mark may have expired and been taken by a third instance, and
	// stamping over that one is the collision the mark exists to prevent, arrived at by
	// the code meant to repair it.
	took, err := takeTurnFromGone.Run(
		ctx, c.client, []string{mark}, holder, c.cfg.Instance, resumeCooloff.Milliseconds(),
	).Int()
	if err != nil {
		c.log.Warn().Err(err).Str("sid", sid).Str("holder", holder).
			Msg("could not take over the turn of an instance that is gone")
		return false
	}
	if took == 1 {
		c.log.Info().Str("sid", sid).Str("holder", holder).
			Msg("taking over the resume turn of an instance that is no longer in the fleet")
	}
	return took == 1
}

// takeTurnFromGone replaces a resume turn only while it still belongs to the instance the
// caller found holding it. Same shape as dropOwnResumeMark, and as releaseScript in
// internal/cluster, for the same reason: read and write have to be one step.
var takeTurnFromGone = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3])
return 1
`)

// waitingOut is the quarantine read, with the instance that has none answering "none".
func (c *Connector) waitingOut(ctx context.Context, sids []string) (map[string]time.Time, error) {
	if c.quarantine == nil {
		return nil, nil
	}
	return c.quarantine.Waiting(ctx, sids)
}

// sweepPartsOnce makes one pass and reports whether the sweeper should stop.
func (c *Connector) sweepPartsOnce(ctx context.Context) bool {
	dropped, err := c.store.SweepMediaParts(ctx, time.Now().Add(-c.cfg.MediaRefetch))
	switch {
	case errors.Is(err, context.Canceled):
		return true
	case err != nil:
		c.log.Warn().Err(err).Msg("could not sweep what was kept to fetch files again")
	case dropped > 0:
		c.log.Debug().Int64("messages", dropped).
			Msg("dropped what was kept to fetch the files of messages past their retention")
	}
	// The same pass drops the record of group creations old enough that no redelivery of
	// the command can still arrive. Bounded by the ledger's own memory rather than by the
	// media retention: what these rows cover is a redelivered command, and a command stops
	// being redelivered when the ledger stops answering for it. Without a sweep the row
	// count is the number of groups the deployment has ever made.
	begun, err := c.store.SweepGroupCreations(ctx, time.Now().Add(-groupCreateRetention))
	switch {
	case errors.Is(err, context.Canceled):
		return true
	case err != nil:
		c.log.Warn().Err(err).Msg("could not sweep the record of group creations")
	case begun > 0:
		c.log.Debug().Int64("attempts", begun).Msg("dropped the record of group creations past their retention")
	}
	return false
}

// groupCreateRetention is how long the record of a creation is kept. The ledger's own
// window, doubled: a record that outlives the redelivery it covers costs a row, and one
// that does not costs a second group.
const groupCreateRetention = 2 * redisx.DefaultIdempotencyTTL

// reclaimCommands takes over what nobody acknowledged: what another instance read
// before it was killed, and what this one deliberately left pending when it could not
// adopt a woken session.
//
// Without it `>` is the only thing ever read, and an entry that reaches the group's
// pending list stays there. A wake left pending on purpose would then never come back,
// so the session it named would stay unowned until a client happened to send another
// command, and the pending list would grow for as long as the instance ran.
func (c *Connector) reclaimCommands(ctx context.Context) {
	// The control stream on a pass of its own, and first. `session.wake` lives there,
	// and a wake is what puts a session from a dead instance back on an instance. Taken
	// alongside a window of session streams it is taken last, under whatever deadline
	// they left, and released along with them when any one of them fails — so the
	// command that exists for a Redis having a bad minute would be the one command that
	// never runs during one.
	c.reclaimPass(ctx, func(pass context.Context) ([]transport.Delivery, error) {
		return c.streams.ClaimControl(pass)
	})
	c.reclaimPass(ctx, func(pass context.Context) ([]transport.Delivery, error) {
		return c.streams.Claim(pass, c.nextReclaimWindow())
	})
}

// reclaimPass takes over one set of streams and dispatches what it took, under a deadline
// of its own.
//
// Bounded twice over, because this runs on the goroutine that renews every lease this
// instance holds. It costs at least one round trip per stream, so an instance with many
// sessions, or a Redis having a bad minute, could spend longer here than a lease survives
// — and the sessions whose leases went unrenewed are then acquired by peers while this
// instance still holds their sockets open.
//
// So: a window of streams rather than all of them, moving on each pass so every session
// is reached within a few heartbeats, and a deadline that leaves the two passes together
// inside one heartbeat. One heartbeat is the figure the adoption bound is already sized
// against — a single adoption may delay one renewal and no more — so this is the largest
// the reclaim can be without moving that line, and cutting it further only means an
// adoption that never reaches the bound it was given.
func (c *Connector) reclaimPass(ctx context.Context, take func(context.Context) ([]transport.Delivery, error)) {
	pass, cancel := context.WithTimeout(ctx, c.cfg.Heartbeat/reclaimPasses)
	defer cancel()

	deliveries, err := take(pass)
	// Counted whatever it found, and before the error check on purpose: a pass that
	// returns nothing is the ordinary case, and without a series that moves anyway it
	// reads exactly like a build where nobody writes any of this. A counter family with
	// no children is absent from the exposition entirely.
	c.metrics.CommandReclaimPasses.Inc()
	if err != nil {
		if ctx.Err() == nil {
			c.log.Error().Err(err).Msg("failed to reclaim commands")
		}
		return
	}
	// The pass's own deadline, not the loop's: the claim and the dispatch of what it
	// took share one bound, so a claim that spent most of the pass leaves the dispatch
	// only the rest. What does not fit is released, keeps its age, and comes back on
	// the next tick.
	c.dispatchWithin(pass, deliveries)
}

// readBlockShare is the fraction of a heartbeat a read waits on Redis before answering
// "nothing this round". What stops a read from outliving the tick is the deadline it is
// given, which the transport shortens its block to fit; this is the ceiling on that, so
// a quiet period costs a couple of clean round trips rather than a connection killed on
// its deadline every time.
const readBlockShare = 2

// readBlock is how long the transport waits for a command before answering "nothing
// this round".
func readBlock(heartbeat time.Duration) time.Duration { return heartbeat / readBlockShare }

// streamOptions is how this instance reads its commands.
func streamOptions(cfg *Config, log *zerolog.Logger) redisstream.Options {
	return redisstream.Options{
		Instance: cfg.Instance, ClaimMinIdle: cfg.ClaimMinIdle, Block: readBlock(cfg.Heartbeat),
		// A wake recovered by a read is handed out with the age it already has, and the
		// claim delay outlives a lease by exactly this much: a peer must not be able to
		// claim it before the lease this instance takes for it could have expired. Half of
		// that slack, so the other half stays what it is for a wake read on arrival -- room
		// for the adoption between the hand-out and the lease.
		ReadBackMaxAge: (cfg.ClaimMinIdle - cfg.LeaseTTL) / 2,
		Logger:         *log,
	}
}

// dispatchWithin carries out a batch, and stops when the caller's deadline runs out.
//
// Nothing in the batch blocks any more: a session's command is offered to that session's
// queue and the three the manager answers itself are queued on its own goroutine, so a
// whole batch costs a handful of channel sends. The deadline is still read, because a
// window already spent when the batch arrives should not start work the tick is about to
// interrupt -- but it is now a guard against a window that had already run out rather
// than a budget the dispatch can exhaust. What is not dispatched is released, so it
// stays pending and comes back on a later pass.
func (c *Connector) dispatchWithin(ctx context.Context, deliveries []transport.Delivery) bool {
	c.measure(deliveries)

	// The sessions something in this batch was left pending for. A batch is one stream's
	// worth of commands in order, so a session that gave one back may not have a later
	// one carried out behind it: the queue that had no room for the first can free a slot
	// between two entries, and then the newer one runs and the older waits for a drain.
	// The undrained mark only keeps the next read away, and the rest of a batch already
	// in hand is this side of it.
	var held []string
	for i := range deliveries {
		if ctx.Err() != nil {
			for rest := i; rest < len(deliveries); rest++ {
				// Through the manager rather than straight to the delivery: a command
				// for a session this instance runs, given back unrun, leaves an older
				// entry pending on that session's stream, and nothing newer may be read
				// for it until a claim has taken that stream over.
				c.manager.GiveBack(&deliveries[rest])
			}
			c.log.Warn().Int("left", len(deliveries)-i).
				Msg("a batch of commands ran out of its window; the rest stays pending")
			return false
		}
		sid := deliveries[i].Command.SID
		if sid != "" && slices.Contains(held, sid) {
			c.manager.GiveBack(&deliveries[i])
			continue
		}
		if c.manager.Dispatch(&deliveries[i]) && sid != "" {
			held = append(held, sid)
		}
	}
	return true
}

// reclaimPasses is how many independent passes one heartbeat's reclaim is made of: the
// control stream and the session window. Each gets half of what the reclaim as a whole
// may spend, so keeping them apart costs the goroutine nothing.
const reclaimPasses = 2

// maxReclaimStreams is how many session streams one reclaim pass looks at. The control
// stream is reclaimed on every heartbeat regardless, on a pass of its own: it is where a
// wake lands, and a wake nobody takes is a session nobody runs.
const maxReclaimStreams = 16

// nextReclaimWindow is the slice of owned sessions this pass covers. It rotates, so a
// fleet member holding hundreds of sessions still reaches all of them, a few heartbeats
// apart, without any one pass holding up a renewal.
func (c *Connector) nextReclaimWindow() []string {
	return c.windowOver(c.manager.SIDs())
}

func (c *Connector) windowOver(sids []string) []string {
	if len(sids) <= maxReclaimStreams {
		return sids
	}
	// SIDs comes off a map, so the order is not stable between calls and the cursor
	// cannot index into it meaningfully. Sorting is what makes the rotation cover
	// everything rather than resampling at random.
	slices.Sort(sids)

	start := c.reclaimCursor % len(sids)
	c.reclaimCursor = (start + maxReclaimStreams) % len(sids)

	window := make([]string, 0, maxReclaimStreams)
	for i := range maxReclaimStreams {
		window = append(window, sids[(start+i)%len(sids)])
	}
	return window
}

// maxDrainPasses bounds one attempt at emptying a newly adopted session's pending list.
// A session with more waiting than a few passes can take is one whose remaining entries
// are better left to the next iteration than paid for on this goroutine all at once.
const maxDrainPasses = 4

// drainAdopted takes over what the previous owner of a freshly adopted session left
// pending, and returns the sessions it could not finish.
//
// Those must not be read from until they are drained. A pending entry is off the stream
// until somebody claims it, so a `>` read hands over commands that arrived after it, and
// per-session order is the one thing that stream is for.
//
// Bounded by the caller's window rather than a budget of its own: it walks every
// adopted stream, up to maxDrainPasses times, on the goroutine that renews every lease
// this instance holds, and the window ends when the next renewal is due. A drain the
// window cuts short picks up where it stopped on the next one.
func (c *Connector) drainAdopted(ctx context.Context) []string {
	adopted := c.manager.TakeNewlyAdopted()
	if len(adopted) == 0 {
		return nil
	}

	for range maxDrainPasses {
		if ctx.Err() != nil {
			c.manager.ReturnAdopted(adopted)
			return adopted
		}
		deliveries, err := c.streams.ClaimSessions(ctx, adopted)
		if err != nil {
			if ctx.Err() == nil {
				c.log.Error().Err(err).Msg("failed to drain what a newly adopted session had pending")
			}
			c.manager.ReturnAdopted(adopted)
			return adopted
		}
		if len(deliveries) == 0 {
			return nil
		}
		// One pass takes at most ReadCount per stream, so a session with a long backlog
		// needs several before anything newer may be read for it. A pass cut short by
		// the window has left entries pending, so the session is not drained either.
		if !c.dispatchWithin(ctx, deliveries) {
			c.manager.ReturnAdopted(adopted)
			return adopted
		}
	}

	c.manager.ReturnAdopted(adopted)
	return adopted
}

// readCommands drains what newly adopted sessions have pending and reads what is newer.
func (c *Connector) readCommands(ctx context.Context) {
	// Taken before the drain, and the order is the point rather than tidiness. A session
	// adopted between the two calls would otherwise be absent from the set the drain
	// takes and present in this list, and `>` would hand over something newer for it
	// ahead of everything its previous owner left pending. Read first, it is simply not
	// in this list at all: the drain takes it, and it joins the read a tick later with
	// its backlog already taken over.
	sids := c.manager.SIDs()

	// Before the read, not after: a session adopted a moment ago may have commands its
	// previous owner abandoned, and reading `>` for it first would hand over what
	// arrived later and run it out of order.
	undrained := c.drainAdopted(ctx)

	if len(undrained) > 0 {
		// Left out of this read rather than skipping the read altogether: the control
		// stream and every other session carry on, and these come back as soon as their
		// backlog is taken over.
		sids = slices.DeleteFunc(sids, func(sid string) bool { return slices.Contains(undrained, sid) })
	}
	// Whatever is left of the window is what the read gets. The transport cuts its own
	// block down to fit a deadline rather than refusing a window it finds too narrow,
	// so a tick that spent most of its period still reads, and reads promptly: a gate
	// here could only refuse, and one refusing every time is a read that never runs.
	deliveries, err := c.streams.Read(ctx, sids)
	if err != nil {
		c.commandReadFailed(ctx, err)
		return
	}
	c.commandReadSucceeded()
	c.dispatchWithin(ctx, deliveries)
}

// seenLabel is one label value of one metric, which is the grain eviction works at.
type seenLabel struct {
	metric string
	value  string
}

const (
	// sessionLabel and consumerLabel name the two label values without a ceiling.
	sessionLabel  = "sid"
	consumerLabel = "from"

	// The values of the source label. Named because eviction has to spell the same set
	// the counting does, and a set spelled twice is a set that drifts -- which it did
	// the moment a third value was added to a `DeleteLabelValues` pair written by hand.
	// `everySource` below is the one list, and a fence keeps it honest.
	sourceRead  = "read"
	sourceClaim = "claim"
	// The third case, and the one the issue's question 3 describes. A wake the fleet
	// cannot adopt is given back unrun; `rememberAge` stamps it with the claim delay so
	// that the next claim takes it rather than the read leaving it pending forever, and
	// it comes back with XPENDING naming this instance as the holder. Measured at this
	// HEAD on the bench that reproduces the sealed scenario s8: fifty stuck sessions,
	// 6450 deliveries in four minutes and twenty-five seconds, every one of them this
	// instance taking back its own.
	//
	// Folded into `claim` it made two readings false at once: an operator reading
	// "reclaimed" saw a fleet taking work off each other when nothing of the sort was
	// happening, and the `from` label accused every instance of having stopped answering.
	sourceRestored = "restored"

	// labelQuiet is how long a label value goes uncounted before its series is dropped.
	//
	// Long, because the failure it has to avoid is a series that comes and goes: a
	// counter that disappears and reappears reads as a reset, and a run of resets is
	// exactly what a fleet retrying one command forever would look like if this were
	// tight. Short enough that what a process has stopped seeing leaves within the hour.
	labelQuiet = 30 * time.Minute
)

// everySource is every value the source label takes, which is what eviction walks. A
// value counted and not listed here keeps its series for the life of the process, and
// nothing about the exposition would say so.
var everySource = []string{sourceRead, sourceClaim, sourceRestored}

// noteLabel records that something was counted against a label value just now.
func (c *Connector) noteLabel(metric, value string, at time.Time) {
	c.seenMu.Lock()
	defer c.seenMu.Unlock()

	if c.seen == nil {
		c.seen = make(map[seenLabel]time.Time)
	}
	c.seen[seenLabel{metric: metric, value: value}] = at
}

// forgetLabelsGoneQuiet drops the series of label values nothing has been counted against
// for a while. Once a heartbeat, off what was ever counted, because there is no teardown
// hook to hang it on and what is being counted outlives any one session: a session this
// instance never owned, a peer that no longer exists.
func (c *Connector) forgetLabelsGoneQuiet(now time.Time) {
	c.seenMu.Lock()
	defer c.seenMu.Unlock()

	for label, last := range c.seen {
		if now.Sub(last) < labelQuiet {
			continue
		}
		switch label.metric {
		case sessionLabel:
			// By the whole label set rather than DeletePartialMatch, which walks the
			// vector once per call: a batch of sessions expiring together would then be
			// quadratic, on the goroutine that renews every lease this instance holds.
			// That goroutine is already bounded twice over for the same reason.
			for _, source := range everySource {
				c.metrics.CommandsDeliveredAgain.DeleteLabelValues(source, label.value)
			}
		case consumerLabel:
			c.metrics.CommandsReclaimed.DeleteLabelValues(label.value)
		}
		delete(c.seen, label)
	}
}

// forgetSession drops what was counted against a session this instance has stopped
// running, at the moment it stops.
//
// The eviction by silence below is the backstop and it is not enough on its own: it
// keeps a series for `labelQuiet` after the last count, which for a session this
// instance no longer has means the set of series reads as "every session it ever had"
// for half an hour. The sealed scenario for this asks for a few heartbeats.
//
// Keyed on the session stopping and not on this instance not owning it, and the
// difference is the whole reason an earlier attempt was reverted: a sweep over "sessions
// I do not own" deletes the series of a `session.wake` the fleet cannot adopt, which
// names a session nobody holds, and that is the one series this metric was built for.
// This fires once, for a session that was being run here and is not any more, which is
// the event `wac_leases_lost_total` counts; a session this instance never had never
// reaches it.
func (c *Connector) forgetSession(sid string) {
	if sid == "" {
		return
	}
	c.seenMu.Lock()
	delete(c.seen, seenLabel{metric: sessionLabel, value: sid})
	c.seenMu.Unlock()

	for _, source := range everySource {
		c.metrics.CommandsDeliveredAgain.DeleteLabelValues(source, sid)
	}
}

// noSession labels what came off the control stream, which names no session of its own.
// A constant rather than the empty string: an empty label value and an absent label read
// the same in some tooling, and this one is a real category.
const noSession = "-"

// measure records what the fleet is handing out a second time.
//
// Here rather than in the transport, and on receipt rather than after dispatch. In the
// transport it would need a metrics dependency the layer does not have and says why it
// does not have; after dispatch it would count what this instance chose to run rather
// than what the fleet handed it, and a batch cut short by its window would go unmeasured
// for exactly the sessions whose commands are not running.
//
// Every path that dispatches comes through here: the read, the reclaim of the control
// stream, the reclaim of session streams, and the drain of a newly adopted session.
func (c *Connector) measure(deliveries []transport.Delivery) {
	now := time.Now()
	for i := range deliveries {
		d := &deliveries[i]
		// Taken back from this instance itself, which is what a wake given back unrun
		// and aged comes back as. It is a claim, and it is not a reclaim from a peer.
		own := d.TakenFrom != "" && d.TakenFrom == c.cfg.Instance
		if d.TakenFrom != "" {
			// A claim: only XPENDING reports who held it and how many times it went out.
			// The count goes in whoever held it, because "one command forever" is the
			// question it answers and that is the same question either way.
			if d.Deliveries > 0 {
				c.metrics.CommandRedeliveries.Observe(float64(d.Deliveries))
			}
			// The consumer label separates a busy fleet from one instance that stopped
			// answering, so it takes the name of a peer and nobody else. This instance
			// naming itself there is a false positive on that alarm, and it fires hardest
			// during the incident the metric exists to show.
			if !own {
				c.metrics.CommandsReclaimed.WithLabelValues(d.TakenFrom).Inc()
				c.noteLabel(consumerLabel, d.TakenFrom, now)
			}
		}
		if !d.DeliveredBefore {
			continue
		}
		source := sourceRead
		switch {
		case own:
			source = sourceRestored
		case d.TakenFrom != "":
			source = sourceClaim
		}
		sid := d.Command.SID
		if sid == "" {
			sid = noSession
		}
		c.metrics.CommandsDeliveredAgain.WithLabelValues(source, sid).Inc()
		c.noteLabel(sessionLabel, sid, now)
	}
}

// silentReadsBeforeAlarm is how many command reads have to fail in a row before the
// instance says it is not serving, instead of reporting one more error.
//
// One failure is ordinary: a window spent, a connection the server closed, a blip. A run
// of them is not, and nothing above this could tell the two apart -- while it lasts, this
// instance carries out nothing for any session it owns, answers nobody, and goes on
// reporting itself ready and holding every lease. On CI it lasted long enough to take a
// test down and the only trace was four identical error lines (#178).
const silentReadsBeforeAlarm = 3

// commandReadFailed records a failed read and decides whether this is one more error or
// an instance that has stopped serving.
//
// It also waits before letting the loop try again, and that is the point rather than
// politeness: a read that fails at once -- Redis refusing the connection, a broken pipe --
// returns immediately, the window it ran under is still open, and the loop goes straight
// back in. That is a spin against a dependency already in trouble, for as long as the
// window lasts. Backing off by a quarter of the heartbeat leaves a few attempts per
// window and cannot outlive it, since the wait ends with the window it was given.
func (c *Connector) commandReadFailed(ctx context.Context, err error) {
	// A window that ran out is the deadline working, not the read failing: the tick
	// hands out what is left of its period, and a read that spends all of it comes back
	// with the context's error and nothing to report.
	//
	// The error is asked before the context, because the context is the half that can be
	// wrong here. The socket carries the window's own deadline, so the two fire together
	// and `ctx.Err()` is set by whichever timer ran first; the transport decides by the
	// clock instead and names it in the error (#209). Only that sentinel: a dial that
	// gave up on its own timeout also carries `context.DeadlineExceeded`, with the window
	// wide open, and suppressing that one would hide a Redis this instance never reached.
	if errors.Is(err, transport.ErrWindowSpent) || ctx.Err() != nil {
		return
	}

	c.metrics.CommandReadsFailed.Inc()
	c.silentReads++
	if c.silentReads == 1 {
		c.silentSince = time.Now()
	}
	switch {
	case c.silentReads < silentReadsBeforeAlarm:
		c.log.Error().Err(err).Msg("failed to read commands")
	case !c.saidItIsMute:
		c.saidItIsMute = true
		c.log.Error().Err(err).Int("failures", c.silentReads).
			Dur("silent_for", time.Since(c.silentSince)).Int("sessions", c.manager.Count()).
			Msg("this instance has stopped reading commands: every session it owns is unserved until reads come back")
	default:
		// Said once. Repeating it per read would bury the recovery line in the same log
		// it is read from, and the metric is what carries how long this has gone on.
		c.log.Debug().Err(err).Int("failures", c.silentReads).Msg("still not reading commands")
	}

	backoff := c.cfg.Heartbeat / 4
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

// commandReadSucceeded closes a run of failures, and says so when there was one to close.
func (c *Connector) commandReadSucceeded() {
	c.metrics.CommandReadLastSuccess.SetToCurrentTime()
	if c.silentReads == 0 {
		return
	}
	if c.saidItIsMute {
		c.log.Warn().Int("failures", c.silentReads).Dur("silent_for", time.Since(c.silentSince)).
			Msg("reading commands again")
	}
	c.silentReads = 0
	c.saidItIsMute = false
}

func (c *Connector) announce(ctx context.Context) {
	err := c.registry.Announce(ctx, &cluster.Presence{
		Instance: c.cfg.Instance, Version: Version,
		ProtocolMin: protocol.MinVersion, ProtocolMax: protocol.Version,
		AdvertiseURL: c.cfg.AdvertiseURL, MediaToken: c.cfg.MediaToken,
		Sessions: c.manager.Count(),
	})
	if err != nil && ctx.Err() == nil {
		c.log.Warn().Err(err).Msg("failed to announce this instance")
	}
}

// shutdown gives the sessions back rather than letting their leases expire, which is
// the difference between a peer picking them up now and one TTL from now.
func (c *Connector) shutdown() {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), ShutdownGrace)
	defer cancel()

	// Read before the stops, because StopAll empties it: these are the accounts this
	// instance is about to make ownerless, and after the call there is nothing left to ask.
	giving := c.manager.SIDs()
	c.manager.StopAll(ctx)
	c.clearTheFloorUnderRetry(ctx, giving)
	if err := c.registry.Withdraw(ctx, c.cfg.Instance); err != nil {
		c.log.Warn().Err(err).Msg("failed to withdraw this instance")
	}
	if err := c.http.Shutdown(ctx); err != nil {
		c.log.Warn().Err(err).Msg("failed to stop the http server")
	}
	if err := c.engine.Close(); err != nil {
		c.log.Warn().Err(err).Msg("failed to close the engine")
	}
	if c.store != nil {
		if err := c.store.Close(); err != nil {
			c.log.Warn().Err(err).Msg("failed to close the store")
		}
	}
	if err := c.client.Close(); err != nil {
		c.log.Warn().Err(err).Msg("failed to close the redis client")
	}
	c.log.Info().Msg("connector is down")
}

// clearTheFloorUnderRetry drops the resume cool-off of every account this instance has
// just given up, so the fleet may bring them back at once.
//
// The mark paces failure. Its comment says so: it is "the floor under the retry", there so
// that an account whose engine keeps retiring it is not dialled again on every pass by
// every instance. An account released because its instance was asked to stop has not
// failed at anything, and holding the fleet off it for a minute is the floor being applied
// to the one case it was not written for.
//
// This is what made a deploy expensive, and the size of it was hidden by chance. The
// outgoing instance takes the mark when it first brings an account back, and the mark
// outlives the release by whatever is left of its minute; the successor then finds the
// lease free, cannot take the turn, and waits. Measured in production the wait came to
// thirty seconds, because that deployment's marks were minutes old by the time it was
// redeployed -- a release that follows an earlier one closely would have paid more.
//
// After the releases, never before: a mark dropped while this instance still holds the
// lease invites a peer to take a turn it cannot use, and that turn is the one thing the
// mark exists to arbitrate.
//
// Logged rather than returned, one by one. The process is leaving either way, and a mark
// that could not be dropped expires on its own within the minute -- which is the behaviour
// this replaces, so a failure here costs exactly what the deploy used to cost.
func (c *Connector) clearTheFloorUnderRetry(ctx context.Context, sids []string) {
	for _, sid := range sids {
		if err := dropOwnResumeMark.Run(
			ctx, c.client, []string{c.client.Keys().Resume(sid)}, c.cfg.Instance,
		).Err(); err != nil && !errors.Is(err, redis.Nil) {
			c.log.Warn().Err(err).Str("sid", sid).
				Msg("could not clear the resume cool-off of an account this instance gave up")
		}
	}
}

// dropOwnResumeMark deletes the cool-off only while it is still this instance's, which is
// what stops a shutdown erasing a turn that has already moved on.
//
// The mark carries the name of whoever took it, and by the time a shutdown gets here the
// one this instance took may be a minute old and gone: a peer that swept in between holds
// its own, with an adoption still in flight behind it. An unconditional delete would hand
// that account back to the next pass while the peer is still bringing it up, which is two
// instances asked for one account -- the single thing this mark exists to arbitrate.
//
// Same shape as releaseScript in internal/cluster for the same reason: read and delete
// have to be one step, or the value can change between them.
var dropOwnResumeMark = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call("DEL", KEYS[1])
return 1
`)

// newEngine builds the WhatsApp side, and opens the store when a database is configured.
//
// The store is the connector's rather than the engine's: it holds what each client asked
// for, the media parts and the group creations, and the resume sweep is the pass that
// reads it. It is opened in here only because whatsmeow's own tables share the one pool
// (see the comment on store.OpenWith), and that accident used to decide who got a store:
// the fake engine returned nil and took `resumeWanted` and `sweepMediaParts` down with
// it, so an instance configured with a database ran without persistence and said nothing
// (#265). It follows the database now, not the engine. Whoever built it has to close it.
//
//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func newEngine(
	ctx context.Context, cfg *Config, owned store.Ownership, blobs meow.MediaOptions,
	queueing meow.Queueing, log zerolog.Logger,
) (engine.Engine, *store.Container, error) {
	var devices *store.Container
	if cfg.DatabaseURL != "" {
		opened, err := store.OpenWith(ctx, cfg.DatabaseURL, owned, log,
			store.Options{MaxConns: cfg.DatabaseConns})
		if err != nil {
			return nil, nil, err
		}
		devices = opened
	}

	// The container is this function's until it is handed over, so every path that does
	// not hand it over closes it: an engine that refused to be built never took it, and
	// neither did a name this build does not know.
	switch cfg.Engine {
	case EngineFake:
		// With the store, so that a deployment on this engine leaves behind what the
		// sweep needs to bring an account back. It is not a store the fake reads: the
		// only thing it writes is the pairing, which `store.Wanted` joins the desired
		// row against, and without it an account that paired is one nothing can resume.
		// A nil container is a `serve` with no database url, which this engine allows
		// and which is a fleet that does not come back on its own by construction.
		return fake.New(fake.WithStore(devices)), devices, nil
	case EngineWhatsmeow:
		// Config refuses a whatsmeow engine with no database url, so devices is set.
		waEngine, err := meow.New(devices,
			meow.Options{DeviceName: cfg.DeviceName, Media: blobs, Queueing: queueing}, log)
		if err != nil {
			_ = devices.Close()
			return nil, nil, err
		}
		return waEngine, devices, nil
	default:
		if devices != nil {
			_ = devices.Close()
		}
		return nil, nil, fmt.Errorf("app: unknown engine %q", cfg.Engine)
	}
}

// newFrameID mints the id every frame carries. Random rather than sequential: ids from
// two instances share one stream, and a counter would collide across them.
func newFrameID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		// crypto/rand failing is the process being unable to do anything safely.
		panic(fmt.Sprintf("app: read random: %v", err))
	}
	return hex.EncodeToString(raw[:])
}

// Hostname is the default instance name: in a container it is the container id, which
// is unique per replica and stable for its life.
func Hostname() string {
	name, err := os.Hostname()
	if err != nil {
		return ""
	}
	return name
}

// queueing reports the engine's back pressure into the metric set.
//
// The adapter lives here and not in the engine because `internal/engine/whatsmeow` has
// no business knowing what Prometheus is, and not in `internal/observability` because
// that package deliberately knows about nothing else. It is three lines, and the shape
// of those three lines is the route whose absence left #221 unmeasurable and three of
// the six metrics in #226 registered and counting nothing.
type queueing struct{ metrics *observability.Metrics }

func queueingInto(metrics *observability.Metrics) meow.Queueing {
	return queueing{metrics: metrics}
}

func (q queueing) Emitted(waited time.Duration, depth int) {
	q.metrics.EmissionWait.Observe(waited.Seconds())
	q.metrics.InboxDepth.Observe(float64(depth))
}

func (q queueing) Dropped(eventType protocol.EventType) {
	q.metrics.EmissionsDropped.WithLabelValues(string(eventType)).Inc()
}

// countingPublisher counts what actually reached the client, by event type.
//
// A wrapper and not a line inside the session, because the count this metric promises is
// "published", and the only place that is known is the far side of the call that
// publishes. Counted after the error check for the same reason: an event the stream
// refused is not an event the client got, and counting it would make the one number that
// could tell an incident "arriving and not published" from "not arriving" answer the
// wrong one -- which is the failure #226 describes, in a build where it had no writer at
// all.
type countingPublisher struct {
	to      transport.Publisher
	metrics *observability.Metrics
}

func (c countingPublisher) Publish(ctx context.Context, event *protocol.Event) error {
	if err := c.to.Publish(ctx, event); err != nil {
		return err
	}
	c.metrics.EventsPublished.WithLabelValues(string(event.Type)).Inc()
	return nil
}

// watching reports what the session layer measured into the metric set.
//
// Same shape and same reason as `queueing` above: the adapter is here because this is
// where the registry is, and `internal/session` has no business knowing what Prometheus
// is. Between the two of them they are the route whose absence left three metrics
// registered and never written (#226).
// watching is what the session layer reports to. It reaches the connector because one
// of the things it reports, a lease this instance stopped running a session over, is the
// moment a series labelled by that session stops being about a session this instance has.
// Assigned after the connector exists, because the manager needs the watcher to be built.
type watching struct {
	metrics *observability.Metrics
	owner   *Connector
}

func (w *watching) CommandDone(kind protocol.CommandType, outcome string, took time.Duration) {
	w.metrics.CommandDuration.WithLabelValues(commandLabel(kind), outcome).Observe(took.Seconds())
}

// commandLabel keeps the type label inside the contract's own list.
//
// `ParseCommand` does not check the type against anything -- a command this build has no
// handler for is refused later, by name -- so the string on a frame is whatever the client
// wrote. Straight into a label that is a series per distinct value, kept for the life of
// the process: one malformed frame per new name is enough to grow this connector's memory
// and its Prometheus series without bound, and the commands do not even have to succeed.
//
// The other two labels this build uses do not have the problem and were checked rather
// than assumed: `EventsPublished` and `EmissionsDropped` are both event types this
// connector chose itself, and `outcome` is a contract error code, which unknown values
// already degrade to `internal` before they arrive here.
func commandLabel(kind protocol.CommandType) string {
	if !kind.Valid() {
		return "unknown"
	}
	return string(kind)
}

func (w *watching) StateDecided(took time.Duration) {
	w.metrics.StateNoticeDelay.Observe(took.Seconds())
}

func (w *watching) LeaseLost(sid string) {
	w.metrics.LeasesLost.Inc()
	if w.owner != nil {
		w.owner.forgetSession(sid)
	}
}
