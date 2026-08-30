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
	"syscall"
	"time"

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
	cfg      Config
	log      zerolog.Logger
	metrics  *observability.Metrics
	client   *redisx.Client
	leases   *cluster.Leases
	registry *cluster.Registry
	manager  *session.Manager
	engine   engine.Engine
	store    *store.Container
	streams  *redisstream.Streams
	http     *httpserver.Server
	blobs    *media.Store

	// reclaimCursor is where the next reclaim pass starts. Read and written only by the
	// loop goroutine, which is also the only one that reclaims.
	reclaimCursor int

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

	waEngine, devices, err := newEngine(startupCtx, cfg, mediaOpts, log)
	if err != nil {
		return nil, err
	}

	streams, err := redisstream.New(client, redisstream.Options{
		Instance: cfg.Instance, ClaimMinIdle: cfg.ClaimMinIdle, Block: readBlock(cfg.Heartbeat),
	})
	if err != nil {
		return nil, err
	}

	metrics := observability.New()
	leases := cluster.NewLeases(client, cfg.Instance, cluster.Options{TTL: cfg.LeaseTTL})
	manager := session.NewManager(&session.ManagerConfig{
		Instance: cfg.Instance, Engine: waEngine, Leases: leases,
		Publisher: streams, Replier: streams, Ledger: redisx.NewIdempotency(client, 0),
		NewID: newFrameID, Logger: log,
	})

	c := &Connector{
		cfg: *cfg, log: log, metrics: metrics, client: client, leases: leases,
		registry: cluster.NewRegistry(client, 3*cfg.Heartbeat), manager: manager, engine: waEngine,
		store: devices, blobs: blobs,
	}

	c.http = httpserver.New(httpserver.Options{
		Addr: cfg.HTTPAddr, Health: c, Registry: metrics.Registry,
		Version: Version, Instance: cfg.Instance, Media: blobHandler,
	})
	c.streams = streams
	return c, nil
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

	c.log.Info().
		Str("addr", c.cfg.HTTPAddr).
		Str("engine", c.cfg.Engine).
		Int("shards", c.cfg.EventShards).
		Msg("connector is up")

	err := c.loop(ctx, httpErr)
	// Before the shutdown, which closes the pool this one is querying.
	stopPartSweep()
	<-sweptParts

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
			// Only a read whose block fits the window is started. One cut off by the
			// deadline mid-block is worse than one skipped: the server may have just
			// moved a command into this consumer's pending list when the connection
			// dies, and an entry pending here with no local record of the delivery is
			// reclaimable by nobody until the full claim delay has passed.
			spent := time.Until(due) < readBlock(c.cfg.Heartbeat)
			if !spent {
				window, cancel := context.WithDeadline(ctx, due)
				c.readCommands(window)
				spent = window.Err() != nil
				cancel()
			}
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
	c.manager.RenewAll(ctx)
	c.reclaimCommands(ctx)
	c.announce(ctx)
	c.metrics.SessionsRunning.Set(float64(c.manager.Count()))
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
	return false
}

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
// "nothing this round". The tick window is what stops a read from outliving the tick;
// this is granularity, so that a quiet period costs a couple of clean round trips
// rather than a connection killed on its deadline every time.
const readBlockShare = 2

// readBlock is how long the transport waits for a command before answering "nothing
// this round".
func readBlock(heartbeat time.Duration) time.Duration { return heartbeat / readBlockShare }

// dispatchWithin carries out a batch, and stops when the caller's deadline runs out.
//
// Reclaimed deliveries are mostly wakes, and a wake is the one command that blocks: it
// adopts a session, which reads the store. A batch of them can therefore run for
// several times the adoption bound, on the goroutine that renews every lease this
// instance holds — and the sessions whose renewals it delays are handed to peers while
// this instance still holds their sockets open. The deadline is the caller's own — a
// reclaim pass's share of the heartbeat, or the tick window — rather than a budget
// opened here, which would stack on whatever the caller had already spent. What is not
// dispatched is released, so it stays pending and comes back on a later pass.
func (c *Connector) dispatchWithin(ctx context.Context, deliveries []transport.Delivery) bool {
	for i := range deliveries {
		if ctx.Err() != nil {
			for rest := i; rest < len(deliveries); rest++ {
				if deliveries[rest].Release != nil {
					deliveries[rest].Release()
				}
			}
			c.log.Warn().Int("left", len(deliveries)-i).
				Msg("a batch of commands ran out of its window; the rest stays pending")
			return false
		}
		c.manager.Dispatch(ctx, &deliveries[i])
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

func (c *Connector) readCommands(ctx context.Context) {
	// Before the read, not after: a session adopted a moment ago may have commands its
	// previous owner abandoned, and reading `>` for it first would hand over what
	// arrived later and run it out of order.
	undrained := c.drainAdopted(ctx)

	sids := c.manager.SIDs()
	if len(undrained) > 0 {
		// Left out of this read rather than skipping the read altogether: the control
		// stream and every other session carry on, and these come back as soon as their
		// backlog is taken over.
		sids = slices.DeleteFunc(sids, func(sid string) bool { return slices.Contains(undrained, sid) })
	}
	deliveries, err := c.streams.Read(ctx, sids)
	if err != nil {
		if ctx.Err() == nil {
			c.log.Error().Err(err).Msg("failed to read commands")
		}
		return
	}
	c.dispatchWithin(ctx, deliveries)
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

	c.manager.StopAll(ctx)
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

// newEngine builds the WhatsApp side. The store comes back with it because only the
// whatsmeow engine has one, and whoever built it has to close it.
//
//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func newEngine(
	ctx context.Context, cfg *Config, blobs meow.MediaOptions, log zerolog.Logger,
) (engine.Engine, *store.Container, error) {
	switch cfg.Engine {
	case EngineFake:
		return fake.New(), nil, nil
	case EngineWhatsmeow:
		devices, err := store.Open(ctx, cfg.DatabaseURL, log)
		if err != nil {
			return nil, nil, err
		}
		waEngine, err := meow.New(devices, meow.Options{DeviceName: cfg.DeviceName, Media: blobs}, log)
		if err != nil {
			// The container is this function's until it is handed over, and an engine
			// that refused to be built never took it.
			_ = devices.Close()
			return nil, nil, err
		}
		return waEngine, devices, nil
	default:
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
