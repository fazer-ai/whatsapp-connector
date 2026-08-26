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
}

// New builds a connector: it dials Redis, agrees with the fleet on the shard count,
// and prepares everything without listening or reading yet.
//
//nolint:gocritic // zerolog.Logger is designed to be copied; every With() returns one by value
func New(cfg *Config, log zerolog.Logger) (*Connector, error) {
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

	waEngine, devices, err := newEngine(startupCtx, cfg, log)
	if err != nil {
		return nil, err
	}

	streams, err := redisstream.New(client, redisstream.Options{
		Instance: cfg.Instance, ClaimMinIdle: cfg.ClaimMinIdle, Block: ReadBlock(cfg.Heartbeat),
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
		store: devices,
	}

	// Only when the deployment gave it somewhere to write. An instance with no blob
	// store does not register the endpoint at all, so a client that reaches it hears
	// 404 and asks the session for the bytes again, which is what it does for a blob
	// that has aged out anyway.
	var blobHandler http.Handler
	if cfg.MediaRoot != "" {
		blobs, err := media.New(media.Options{
			Root: cfg.MediaRoot, TTL: cfg.MediaTTL,
			Quota: cfg.MediaQuota, MaxBlob: cfg.MediaMaxBlob,
		})
		if err != nil {
			return nil, err
		}
		c.blobs = blobs
		blobHandler = media.Handler(media.HandlerOptions{Blobs: blobs, Token: cfg.MediaToken})
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
	sweeping, stopSweeping := context.WithCancel(ctx)
	swept := c.sweepBlobs(sweeping)

	c.log.Info().
		Str("addr", c.cfg.HTTPAddr).
		Str("engine", c.cfg.Engine).
		Int("shards", c.cfg.EventShards).
		Msg("connector is up")

	err := c.loop(ctx, httpErr)
	c.shutdown()
	// After the loop, so the sweep is not walking a directory the shutdown is still
	// writing to, and waited for, so the process does not exit mid-rename.
	stopSweeping()
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
// Reading and renewing share a goroutine because a read that blocks is bounded by the
// transport's block interval, which is well under the lease TTL: a separate goroutine
// would buy nothing and would need the manager's map to be safe for one more writer.
func (c *Connector) loop(ctx context.Context, httpErr <-chan error) error {
	heartbeat := time.NewTicker(c.cfg.Heartbeat)
	defer heartbeat.Stop()

	c.announce(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-httpErr:
			if err != nil {
				return fmt.Errorf("app: http server: %w", err)
			}
			return nil
		case <-heartbeat.C:
			c.manager.RenewAll(ctx)
			c.reclaimCommands(ctx)
			c.announce(ctx)
			c.metrics.SessionsRunning.Set(float64(c.manager.Count()))
		default:
			c.readCommands(ctx)
		}
	}
}

// blobSweep is how often the media cache is walked. It is far shorter than the TTL and
// far longer than the heartbeat: what it bounds is how long the disk stays over quota
// after a burst, and walking a large cache costs real time.
const blobSweep = time.Minute

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
		ticker := time.NewTicker(blobSweep)
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
	// The pass's own deadline, not the loop's. dispatchWithin would otherwise open a
	// fresh budget on top of whatever the claim spent, and a heartbeat configured close
	// to the lease makes that sum longer than the lease this goroutine is renewing. What
	// does not fit is released, keeps its age, and comes back on the next tick.
	c.dispatchWithin(pass, deliveries)
}

// dispatchShare is the fraction of the lease one batch of commands may hold the loop
// for. The configuration is checked against it, because it is the renewal that follows
// the batch that has to still be inside the lease.
const dispatchShare = 3

// readBlockShare is the fraction of a heartbeat a read may wait on Redis for. Derived
// rather than fixed, because it is the same goroutine again: a read that blocks past the
// tick delays every renewal behind it by however long it blocked, and a fixed five
// seconds is most of a heartbeat on one deployment and none of it on another.
const readBlockShare = 2

// ReadBlock is how long the transport waits for a command before answering "nothing this
// round". Exported because the configuration is checked against it and the check has to
// be the same arithmetic as the wiring.
func ReadBlock(heartbeat time.Duration) time.Duration { return heartbeat / readBlockShare }

// budget is how long one batch of commands may hold the loop. It is a third of the
// lease, so a batch that spends all of it still leaves two thirds for the renewal that
// follows.
func (c *Connector) budget() time.Duration { return c.cfg.LeaseTTL / dispatchShare }

// dispatchWithin carries out what a reclaim took, and stops when it has spent as long
// as this goroutine can afford.
//
// Reclaimed deliveries are mostly wakes, and a wake is the one command that blocks: it
// adopts a session, which reads the store. A batch of them can therefore run for
// several times the adoption bound, on the goroutine that renews every lease this
// instance holds — and the sessions whose renewals it delays are handed to peers while
// this instance still holds their sockets open. What is not dispatched is released, so
// it stays pending and comes back on a later pass.
func (c *Connector) dispatchWithin(ctx context.Context, deliveries []transport.Delivery) bool {
	budget, cancel := context.WithTimeout(ctx, c.budget())
	defer cancel()

	for i := range deliveries {
		if budget.Err() != nil {
			for rest := i; rest < len(deliveries); rest++ {
				if deliveries[rest].Release != nil {
					deliveries[rest].Release()
				}
			}
			c.log.Warn().Int("left", len(deliveries)-i).
				Msg("a batch of commands ran out of its budget; the rest stays pending")
			return false
		}
		// The budget, not the loop's own context. A wake starting just before the
		// deadline would otherwise run its whole adoption past it, which is most of the
		// budget again on the goroutine that has renewals waiting behind it.
		c.manager.Dispatch(budget, &deliveries[i])
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
func (c *Connector) drainAdopted(ctx context.Context) []string {
	adopted := c.manager.TakeNewlyAdopted()
	if len(adopted) == 0 {
		return nil
	}

	// The same budget as a dispatch batch, for the same reason: this walks every
	// adopted stream, up to maxDrainPasses times, on the goroutine that renews every
	// lease this instance holds.
	pass, cancel := context.WithTimeout(ctx, c.budget())
	defer cancel()

	for range maxDrainPasses {
		if pass.Err() != nil {
			c.manager.ReturnAdopted(adopted)
			return adopted
		}
		deliveries, err := c.streams.ClaimSessions(pass, adopted)
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
		// the budget has left entries pending, so the session is not drained either.
		// The drain's own deadline, not the loop's: dispatchWithin would otherwise open a
		// fresh budget on every one of these passes, and a drain that already spent its
		// claim time would go on holding the goroutine that renews every lease this
		// instance holds for several budgets more.
		if !c.dispatchWithin(pass, deliveries) {
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
func newEngine(ctx context.Context, cfg *Config, log zerolog.Logger) (engine.Engine, *store.Container, error) {
	switch cfg.Engine {
	case EngineFake:
		return fake.New(), nil, nil
	case EngineWhatsmeow:
		devices, err := store.Open(ctx, cfg.DatabaseURL, log)
		if err != nil {
			return nil, nil, err
		}
		return meow.New(devices, cfg.DeviceName, log), devices, nil
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
