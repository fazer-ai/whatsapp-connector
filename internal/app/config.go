// Package app wires the connector together: configuration, the run loop, and the
// shutdown that gives sessions back to the fleet instead of letting them expire.
package app

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

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

	cfg := Config{
		Instance:     envString("WAC_INSTANCE", hostname),
		RedisURL:     envString("REDIS_URL", ""),
		RedisPass:    envString("REDIS_PASSWORD", ""),
		RedisPrefix:  envString("WAC_REDIS_PREFIX", redisx.DefaultPrefix),
		EventShards:  shards,
		Engine:       envString("WAC_ENGINE", "fake"),
		DatabaseURL:  envString("WAC_DATABASE_URL", ""),
		DeviceName:   envString("WAC_DEVICE_NAME", DefaultDeviceName),
		HTTPAddr:     envString("WAC_HTTP_ADDR", ":8080"),
		AdvertiseURL: envString("WAC_ADVERTISE_URL", ""),
		MediaToken:   envString("WAC_MEDIA_TOKEN", ""),
		LogLevel:     envString("WAC_LOG_LEVEL", "info"),
		LeaseTTL:     leaseTTL,
		Heartbeat:    heartbeat,
		ClaimMinIdle: claimMinIdle,
	}
	if cfg.Instance == "" {
		return Config{}, fmt.Errorf("app: WAC_INSTANCE is empty and the hostname is unknown")
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
	if cfg.AdvertiseURL == "" {
		cfg.AdvertiseURL = "http://" + cfg.Instance + strings.TrimPrefix(cfg.HTTPAddr, "0.0.0.0")
	}
	return cfg, nil
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
