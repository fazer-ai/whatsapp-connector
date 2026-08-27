// Package app wires the connector together: configuration, the run loop, and the
// shutdown that gives sessions back to the fleet instead of letting them expire.
package app

import (
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// Config is the whole configuration, read from the environment.
//
// Every name is prefixed `WAC_` except the Redis address, which is deliberately
// `REDIS_URL`: the connector shares one Redis with its client and reads the same
// variable the client's compose file already sets, so the two cannot be pointed at
// different servers by an operator setting only one of them.
type Config struct {
	Instance     string
	RedisURL     string
	RedisPass    string
	RedisPrefix  string
	EventShards  int
	Engine       string
	DatabaseURL  string
	DeviceName   string
	HTTPAddr     string
	AdvertiseURL string
	MediaToken   string
	// MediaRoot is where this instance keeps the bytes of inbound media. Empty turns
	// the blob store off, and with it the media endpoint: an instance with nowhere to
	// put a file publishes messages without a blob to fetch rather than filling a
	// directory it was not given.
	MediaRoot    string
	MediaTTL     time.Duration
	MediaQuota   int64
	MediaMaxBlob int64
	// MediaBlockSize is the allocation unit of the volume the media root sits on, which
	// is what the quota is counted in. A volume formatted with larger units than the
	// default undercharges every file by the difference.
	MediaBlockSize int64
	// MediaRefetch is how long a message can still be asked for its file again. It is a
	// retention decision rather than a cache one: what is kept for that long is the key
	// to somebody's file, one row per media message the deployment received.
	//
	// The default is longer than any blob lives and shorter than WhatsApp keeps the
	// file, which is the window where the answer can be anything but a failure. Longer
	// than that retains keys to files WhatsApp has already dropped.
	MediaRefetch time.Duration
	LogLevel     string
	LeaseTTL     time.Duration
	Heartbeat    time.Duration
	// ClaimMinIdle is how long a command has to sit unacknowledged before another
	// instance takes it over. It bounds how long a session stays unowned after the
	// instance that woke it died, and it has to stay comfortably above the time a
	// healthy instance spends carrying one command out, or a slow adoption gets stolen
	// from underneath itself.
	ClaimMinIdle time.Duration
}

// The engines this build can run.
const (
	EngineFake      = "fake"
	EngineWhatsmeow = "whatsmeow"
)

// DefaultDeviceName is what the account's linked-devices list shows for a session this
// connector paired. It is fleet-wide rather than per session, because whatsmeow keeps
// device properties process-wide.
const DefaultDeviceName = "fazer.ai"

// DefaultEventShards is how many event streams a fleet publishes to. It is fleet-wide
// and effectively permanent: changing it re-hashes every session onto a different
// stream, so an instance that disagrees with what is recorded refuses to start.
const DefaultEventShards = 8

// DefaultMediaRefetch is how long a message can still be asked for its file again.
//
// It is sized by what has to be survivable, not by what WhatsApp does: a deploy landing
// between an event and its download, a queue that fell behind, the client's own retry
// ladder, and an operator noticing a broken attachment a few days later and asking for
// it again. A week covers all of those and costs a few hundred bytes a message.
//
// It is not sized larger because what is kept is the key to somebody's file. WhatsApp
// stops serving the file at some point of its own, and past that this table would be
// retaining keys to bytes nobody can fetch, which is cost with no answer behind it.
//
// A floor rather than the value: a deployment keeping blobs for longer than this keeps
// the coordinates at least that long too, or the reference lapses while the message is
// still unrecoverable.
const DefaultMediaRefetch = 7 * 24 * time.Hour

// LoadConfig reads the environment. It fails on a value it cannot parse rather than
// falling back to a default, because a misspelled number in a deployment is a bug to
// see at startup, not a setting silently ignored.
func LoadConfig(hostname string) (Config, error) {
	shards, err := envInt("WAC_EVENT_SHARDS", DefaultEventShards)
	if err != nil {
		return Config{}, err
	}
	leaseTTL, err := envDuration("WAC_LEASE_TTL", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	heartbeat, err := envDuration("WAC_HEARTBEAT", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	// Derived from the lease rather than fixed, because the two are one rule: see the
	// check below.
	claimMinIdle, err := envDuration("WAC_CLAIM_MIN_IDLE", leaseTTL+leaseTTL/2)
	if err != nil {
		return Config{}, err
	}
	mediaTTL, err := envDuration("WAC_MEDIA_TTL", media.DefaultTTL)
	if err != nil {
		return Config{}, err
	}
	// Read after the blob TTL and never below it, which is the same rule as the check
	// further down and the reason this cannot be a constant: a deployment already
	// keeping blobs for longer than the default would otherwise be refused on upgrade by
	// a setting it has never heard of, over a relation it did nothing to break. An
	// explicitly shorter one is still refused, because that is an operator asking for
	// the window this exists to close.
	mediaRefetch, err := envDuration("WAC_MEDIA_REFETCH_TTL", max(DefaultMediaRefetch, mediaTTL))
	if err != nil {
		return Config{}, err
	}
	mediaQuota, err := envBytes("WAC_MEDIA_QUOTA", media.DefaultQuota)
	if err != nil {
		return Config{}, err
	}
	mediaMaxBlob, err := envBytes("WAC_MEDIA_MAX_BLOB", media.DefaultMaxBlob)
	if err != nil {
		return Config{}, err
	}
	mediaBlockSize, err := envBytes("WAC_MEDIA_BLOCK_SIZE", media.DefaultBlockSize)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		Instance:       envString("WAC_INSTANCE", hostname),
		RedisURL:       envString("REDIS_URL", ""),
		RedisPass:      envString("REDIS_PASSWORD", ""),
		RedisPrefix:    envString("WAC_REDIS_PREFIX", redisx.DefaultPrefix),
		EventShards:    shards,
		Engine:         envString("WAC_ENGINE", "fake"),
		DatabaseURL:    envString("WAC_DATABASE_URL", ""),
		DeviceName:     envString("WAC_DEVICE_NAME", DefaultDeviceName),
		HTTPAddr:       envString("WAC_HTTP_ADDR", ":8080"),
		AdvertiseURL:   envString("WAC_ADVERTISE_URL", ""),
		MediaToken:     envString("WAC_MEDIA_TOKEN", ""),
		MediaRoot:      envString("WAC_MEDIA_ROOT", ""),
		MediaTTL:       mediaTTL,
		MediaRefetch:   mediaRefetch,
		MediaQuota:     mediaQuota,
		MediaMaxBlob:   mediaMaxBlob,
		MediaBlockSize: mediaBlockSize,
		LogLevel:       envString("WAC_LOG_LEVEL", "info"),
		LeaseTTL:       leaseTTL,
		Heartbeat:      heartbeat,
		ClaimMinIdle:   claimMinIdle,
	}
	if cfg.Instance == "" {
		return Config{}, fmt.Errorf("app: WAC_INSTANCE is empty and the hostname is unknown")
	}
	if cfg.LeaseTTL <= 0 {
		// cluster.NewLeases substitutes its own default for a non-positive TTL, so the
		// leases would work while everything derived from this value here would not: the
		// dispatch budget is a third of it, and a zero budget is a context that has
		// already expired, so every command is released the moment it is read. An
		// instance that reads work and does none of it, reporting itself healthy.
		return Config{}, fmt.Errorf("app: WAC_LEASE_TTL must be positive, got %s", cfg.LeaseTTL)
	}
	if cfg.Heartbeat <= 0 {
		return Config{}, fmt.Errorf("app: WAC_HEARTBEAT must be positive, got %s", cfg.Heartbeat)
	}
	if cfg.Heartbeat >= cfg.LeaseTTL {
		// Leases are renewed on the heartbeat and on nothing else, so a heartbeat that
		// does not fit inside the TTL is one where every lease is already stale when the
		// tick that would have renewed it arrives: every session on the instance is
		// stopped, every tick, for as long as it runs.
		return Config{}, fmt.Errorf(
			"app: WAC_HEARTBEAT (%s) must be shorter than WAC_LEASE_TTL (%s), or a lease expires "+
				"before the tick that would renew it", cfg.Heartbeat, cfg.LeaseTTL)
	}
	if cfg.Heartbeat+ReadBlock(cfg.Heartbeat)+cfg.LeaseTTL/dispatchShare >= cfg.LeaseTTL {
		// Fitting inside the TTL is not enough on its own. Reading, dispatching and
		// renewing are one goroutine, so the renewal that follows a read comes a
		// heartbeat, plus however long that read waited on Redis, plus however long its
		// batch took, after the one before it. Longer than the lease and the leases are
		// gone by then: peers acquire the accounts while this instance goes on holding
		// their sockets open, which is the one thing the lease exists to prevent. A read
		// that starts just before a tick is an ordinary minute, not a corner, so this is
		// a deployment that breaks under load rather than one that never works.
		return Config{}, fmt.Errorf(
			"app: WAC_HEARTBEAT (%s) plus a blocking read (%s) plus the dispatch budget (%s) must be "+
				"shorter than WAC_LEASE_TTL (%s), or a batch delays the renewal past the lease it is renewing",
			cfg.Heartbeat, ReadBlock(cfg.Heartbeat), cfg.LeaseTTL/dispatchShare, cfg.LeaseTTL)
	}
	if cfg.ClaimMinIdle <= cfg.LeaseTTL {
		// A wake is acknowledged when adoption finds the session already owned, because
		// in a fleet that means somebody else is running it. That is only true while the
		// owner is alive. An instance that reads a wake, wins the lease and dies before
		// acknowledging leaves a lease that outlives it by up to the whole TTL, and a
		// peer claiming the wake inside that window is told "already owned" about an
		// instance that is gone, acknowledges the only wake there was, and leaves the
		// session unowned for good. Claiming strictly later than the lease can survive is
		// what makes that answer true.
		return Config{}, fmt.Errorf(
			"app: WAC_CLAIM_MIN_IDLE (%s) must be longer than WAC_LEASE_TTL (%s), or a wake can be "+
				"reclaimed while the lease that answered it is still alive", cfg.ClaimMinIdle, cfg.LeaseTTL)
	}
	if cfg.EventShards <= 0 {
		return Config{}, fmt.Errorf("app: WAC_EVENT_SHARDS must be positive, got %d", cfg.EventShards)
	}
	if cfg.Engine == EngineWhatsmeow && cfg.DatabaseURL == "" {
		// A whatsmeow engine with nowhere to keep a pairing would ask every session to
		// scan a QR code on every restart, which is a deployment that looks healthy and
		// is not.
		return Config{}, fmt.Errorf("app: WAC_DATABASE_URL is required when WAC_ENGINE is %q", EngineWhatsmeow)
	}
	if cfg.MediaTTL <= 0 {
		// media.New substitutes its own default for a non-positive TTL, so a deployment
		// that asked for something else would keep a day's worth of blobs and report
		// nothing about it. The setting is honoured or the instance does not start.
		return Config{}, fmt.Errorf("app: WAC_MEDIA_TTL must be positive, got %s", cfg.MediaTTL)
	}
	if cfg.MediaRefetch <= 0 {
		return Config{}, fmt.Errorf("app: WAC_MEDIA_REFETCH_TTL must be positive, got %s", cfg.MediaRefetch)
	}
	if cfg.MediaRefetch < cfg.MediaTTL {
		// The whole point of keeping how to fetch a file again is to answer after the
		// blob has gone. Retaining it for less than the blob lives leaves a window where
		// the reference has lapsed and the message cannot be recovered either, which is
		// the failure this exists to prevent.
		return Config{}, fmt.Errorf(
			"app: WAC_MEDIA_REFETCH_TTL (%s) is shorter than WAC_MEDIA_TTL (%s), so a file would stop being "+
				"recoverable before the blob naming it expires", cfg.MediaRefetch, cfg.MediaTTL)
	}
	if cfg.MediaMaxBlob > cfg.MediaQuota {
		// One blob would evict everything else and then itself. media.New refuses this
		// as well; saying so here is what puts it in front of an operator alongside the
		// other two settings rather than inside a subsystem's constructor.
		return Config{}, fmt.Errorf(
			"app: WAC_MEDIA_MAX_BLOB (%d) is larger than WAC_MEDIA_QUOTA (%d), so a blob at the cap "+
				"evicts the whole cache and then itself", cfg.MediaMaxBlob, cfg.MediaQuota)
	}
	if cfg.MediaRoot != "" && cfg.MediaToken == "" {
		// The endpoint hands out message contents, and its only guard is the token. A
		// store with no token would serve them to anything that can reach the port, so
		// the deployment is refused rather than quietly opened.
		return Config{}, fmt.Errorf("app: WAC_MEDIA_TOKEN is required when WAC_MEDIA_ROOT is set")
	}
	if cfg.AdvertiseURL == "" {
		// From the port alone, never from the host the listener binds to. The two answer
		// different questions: a bind host says which interface to accept on, and the
		// advertised host says how the rest of the deployment reaches this instance.
		// Pasting the first into the second built `http://<instance>127.0.0.1:8080` for
		// anything but the two spellings of "every interface", which resolves nowhere —
		// and a blob is published under it after the message has been acknowledged, so
		// what that costs is the file.
		_, port, err := net.SplitHostPort(cfg.HTTPAddr)
		if err != nil {
			return Config{}, fmt.Errorf("app: WAC_HTTP_ADDR %q is not an address to listen on: %w", cfg.HTTPAddr, err)
		}
		if port == "" {
			// `:` and `host:` are addresses to listen on: they split without complaint
			// and a listener given either takes whatever port is free. There is nothing
			// to advertise then, and the derived address would carry a bare colon that
			// every client reads as port 80.
			return Config{}, fmt.Errorf(
				"app: WAC_HTTP_ADDR %q names no port, and a listener given none takes any free one, "+
					"which is not a port the rest of the deployment can be told to come back to", cfg.HTTPAddr)
		}
		// JoinHostPort rather than pasting a colon in: an instance addressed by an IPv6
		// literal is written in brackets, and without them `2001:db8::1:8080` still
		// parses, as that host and that port, into a URL no client can dial.
		cfg.AdvertiseURL = "http://" + net.JoinHostPort(cfg.Instance, port)
	}
	return cfg, nil
}

// envBytes reads a size. Plain digits are bytes, and the usual suffixes are the powers
// of two rather than of ten, because the setting is a disk budget and that is the unit
// an operator sizing a volume is working in.
func envBytes(name string, fallback int64) (int64, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	scale := int64(1)
	for suffix, factor := range map[string]int64{"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30} {
		if trimmed, found := strings.CutSuffix(raw, suffix); found {
			raw, scale = strings.TrimSpace(trimmed), factor
			break
		}
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("app: %s must be a size in bytes, optionally suffixed KiB, MiB or GiB: %w", name, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("app: %s must be positive, got %d", name, value)
	}
	if value > math.MaxInt64/scale {
		// Checked before the multiplication rather than after: past this the product
		// wraps, and a budget that wrapped is either negative, which reads as unset and
		// is replaced by a default, or a small positive number nobody asked for.
		return 0, fmt.Errorf("app: %s is larger than this can count in bytes", name)
	}
	return value * scale, nil
}

func envString(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok && value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) (int, error) {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("app: %s is not a number: %q", name, raw)
	}
	return value, nil
}

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("app: %s is not a duration: %q", name, raw)
	}
	return value, nil
}
