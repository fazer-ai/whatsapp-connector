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

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/media"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/session"
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
	// MediaSendMax is the largest file this instance will send. It is deliberately not
	// the blob cap: that one bounds what an inbound file costs this instance's disk, and
	// an instance given no blob root at all still sends. What this bounds is a fetch
	// from an address the caller chose and an upload to WhatsApp, neither of which
	// touches the cache.
	MediaSendMax int64
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
	mediaSendMax, err := envBytes("WAC_MEDIA_SEND_MAX", media.DefaultSendMax)
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
		MediaSendMax:   mediaSendMax,
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
		// leases would go on working while every rule below is checked against a value
		// the cluster never uses. Refused by name, so an operator reads "the lease must
		// be positive" rather than a timing complaint about a lease of no length.
		return Config{}, fmt.Errorf("app: WAC_LEASE_TTL must be positive, got %s", cfg.LeaseTTL)
	}
	if cfg.Heartbeat <= 0 {
		return Config{}, fmt.Errorf("app: WAC_HEARTBEAT must be positive, got %s", cfg.Heartbeat)
	}
	if readBlock(cfg.Heartbeat) < time.Millisecond {
		// A read waits a share of the heartbeat, and Redis takes that wait in whole
		// milliseconds: under one it cannot be expressed, so every read is skipped as
		// having no room for its own answer. The instance would tick, renew, announce
		// and report itself alive while consuming nothing at all -- not a command, not
		// even a wake, so no session it does not already run would ever start. Refused
		// by name, because a heartbeat this short is a deployment mistake and not a
		// timing to degrade under.
		return Config{}, fmt.Errorf(
			"app: WAC_HEARTBEAT (%s) leaves a read less than the millisecond Redis can wait for, "+
				"so no command would ever be read", cfg.Heartbeat)
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
	if cfg.Heartbeat+cfg.LeaseTTL/session.ReleaseShare >= cfg.LeaseTTL-2*cluster.DefaultRenewMargin {
		// Fitting inside the TTL is not enough on its own. Reading, dispatching and
		// renewing are one goroutine, so everything between two renewals delays the
		// second -- but everything outside the heartbeat branch is cut when the next
		// renewal is due, so however many steps that work grows, it cannot push the
		// renewal by more than one period. What can is the branch's own tail: the
		// hand-backs, which get their share of the lease, and a reclaim whose passes
		// divide one heartbeat between them.
		//
		// Against the lease's fresh lifetime, and with the margin held back over
		// again. A lease answers "do I still own this" no from a margin before it
		// expires, so the lifetime the bounded work may spend ends there -- and the
		// unbounded work between two renewals (the renewal round trip and the
		// announcement) needs room of its own, which is exactly what the margin was
		// sized to be: a pause plus a round trip. A bound allowed to press against the
		// cutoff leaves that work no room at all, and the cost is an instance whose
		// sessions drop their own events, and whose key a peer can take on a modest
		// Redis delay, while this one goes on holding their sockets open -- the one
		// thing the lease exists to prevent.
		//
		// That the unbounded half is a fixed number of round trips rather than one per
		// session is what makes a rule written in constants able to hold: the renewals
		// go out in one pipelined batch (cluster.RenewMany), so nothing here is
		// proportional to how many sessions the instance carries.
		//
		// What the reserve does not cover, and cannot: an acknowledgement runs on a
		// deadline of its own so that a command already carried out is retired even when
		// the window it ran in is gone, and it runs on this goroutine. One started near
		// the end of a window can hold the tick up to its own timeout past it. Pricing
		// that worst case in would make short leases unconfigurable for a ceiling only a
		// degraded Redis reaches, so it is left out here and carried by #79, which moves
		// those acknowledgements off this goroutine instead.
		//
		// Derived from the bounds that actually sit outside the tick, not summed from
		// the loop's steps. The sum was wrong three times running -- each rewrite
		// remembered the parcels somebody could name and forgot one nobody did -- and a
		// forgotten parcel here is not a failed check, it is a lease lost under load.
		return Config{}, fmt.Errorf(
			"app: WAC_HEARTBEAT (%s) plus the lease hand-back tail (%s) must be shorter than "+
				"WAC_LEASE_TTL (%s) minus twice the renew margin (%s), or a renewal can land on a "+
				"lease that has already gone stale", cfg.Heartbeat, cfg.LeaseTTL/session.ReleaseShare,
			cfg.LeaseTTL, cluster.DefaultRenewMargin)
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
	if cfg.MediaSendMax <= 0 {
		return Config{}, fmt.Errorf(
			"app: WAC_MEDIA_SEND_MAX must be positive, got %d: zero is not "+
				"\"no limit\", it is an instance that refuses every file it is asked to send", cfg.MediaSendMax)
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
