package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
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
	"github.com/fazer-ai/whatsapp-connector/internal/observability"
	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/session"
	"github.com/fazer-ai/whatsapp-connector/internal/store"
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

	streams, err := redisstream.New(client, redisstream.Options{Instance: cfg.Instance, ClaimMinIdle: cfg.ClaimMinIdle})
	if err != nil {
		return nil, err
	}

	metrics := observability.New()
	leases := cluster.NewLeases(client, cfg.Instance, cluster.Options{TTL: cfg.LeaseTTL})
	manager := session.NewManager(&session.ManagerConfig{
		Instance: cfg.Instance, Engine: waEngine, Leases: leases,
		Publisher: streams, Replier: streams, NewID: newFrameID, Logger: log,
	})

	c := &Connector{
		cfg: *cfg, log: log, metrics: metrics, client: client, leases: leases,
		registry: cluster.NewRegistry(client, 3*cfg.Heartbeat), manager: manager, engine: waEngine,
		store: devices,
	}
	c.http = httpserver.New(httpserver.Options{
		Addr: cfg.HTTPAddr, Health: c, Registry: metrics.Registry,
		Version: Version, Instance: cfg.Instance,
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

	c.log.Info().
		Str("addr", c.cfg.HTTPAddr).
		Str("engine", c.cfg.Engine).
		Int("shards", c.cfg.EventShards).
		Msg("connector is up")

	err := c.loop(ctx, httpErr)
	c.shutdown()
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

// reclaimCommands takes over what nobody acknowledged: what another instance read
// before it was killed, and what this one deliberately left pending when it could not
// adopt a woken session.
//
// Without it `>` is the only thing ever read, and an entry that reaches the group's
// pending list stays there. A wake left pending on purpose would then never come back,
// so the session it named would stay unowned until a client happened to send another
// command, and the pending list would grow for as long as the instance ran.
func (c *Connector) reclaimCommands(ctx context.Context) {
	// Bounded twice over, because this runs on the goroutine that renews every lease
	// this instance holds. It costs at least one round trip per stream, so an instance
	// with many sessions, or a Redis having a bad minute, could spend longer here than a
	// lease survives — and the sessions whose leases went unrenewed are then acquired by
	// peers while this instance still holds their sockets open.
	//
	// So: a window of streams rather than all of them, moving on each pass so every
	// session is reached within a few heartbeats, and a deadline well inside the
	// heartbeat for the whole thing.
	pass, cancel := context.WithTimeout(ctx, c.cfg.Heartbeat/2)
	defer cancel()

	deliveries, err := c.streams.Claim(pass, c.nextReclaimWindow())
	if err != nil {
		if ctx.Err() == nil {
			c.log.Error().Err(err).Msg("failed to reclaim commands")
		}
		return
	}
	for i := range deliveries {
		c.manager.Dispatch(ctx, &deliveries[i])
	}
}

// maxReclaimStreams is how many session streams one reclaim pass looks at. The control
// stream is read on every pass regardless: it is where a wake lands, and a wake nobody
// takes is a session nobody runs.
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

// drainAdopted takes over what the previous owner of a freshly adopted session left
// pending, before this instance reads anything newer for it.
func (c *Connector) drainAdopted(ctx context.Context) {
	adopted := c.manager.TakeNewlyAdopted()
	if len(adopted) == 0 {
		return
	}
	deliveries, err := c.streams.ClaimSessions(ctx, adopted)
	if err != nil {
		if ctx.Err() == nil {
			c.log.Error().Err(err).Msg("failed to drain what a newly adopted session had pending")
		}
		return
	}
	for i := range deliveries {
		c.manager.Dispatch(ctx, &deliveries[i])
	}
}

func (c *Connector) readCommands(ctx context.Context) {
	// Before the read, not after: a session adopted a moment ago may have commands its
	// previous owner abandoned, and reading `>` for it first would hand over what
	// arrived later and run it out of order.
	c.drainAdopted(ctx)

	sids := c.manager.SIDs()
	deliveries, err := c.streams.Read(ctx, sids)
	if err != nil {
		if ctx.Err() == nil {
			c.log.Error().Err(err).Msg("failed to read commands")
		}
		return
	}
	for i := range deliveries {
		c.manager.Dispatch(ctx, &deliveries[i])
	}
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
