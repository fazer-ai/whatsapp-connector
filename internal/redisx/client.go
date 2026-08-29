package redisx

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config is everything needed to reach the Redis both sides share.
type Config struct {
	// URL is a redis:// or rediss:// address. Empty means localhost.
	URL string
	// Password overrides whatever the URL carries, for deployments that pass the
	// two separately (the Chatwoot compose file is one).
	Password string
	// Prefix namespaces every key. Empty means DefaultPrefix.
	Prefix string
	// Shards is how many event streams the fleet publishes to.
	Shards int
}

// Client is a Redis connection plus the key layout it is used with. They travel
// together because using one with another fleet's keys is the failure mode that
// looks like "no events" rather than like an error.
type Client struct {
	*redis.Client
	keys Keys
}

// Keys is the layout this client reads and writes.
func (c *Client) Keys() Keys { return c.keys }

// New dials Redis and returns the client. It does not talk to the server: use Ping
// for that, so a caller decides for itself whether an unreachable Redis is fatal.
func New(cfg Config) (*Client, error) {
	url := cfg.URL
	if url == "" {
		url = "redis://127.0.0.1:6379"
	}
	options, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redisx: parse url: %w", err)
	}
	if cfg.Password != "" {
		options.Password = cfg.Password
	}
	// Without this go-redis hands the connection a background context instead of the
	// caller's, so every deadline in this codebase stops at the point the command reaches
	// the socket and the read and write timeouts take over from there. Everything that
	// bounds a call by a context -- a ping, a reclaim pass, a dispatch budget, the
	// remaining life of a transient event -- would then be a bound on getting the command
	// sent rather than on the command, and an event published after it stopped being true
	// is the one place where late and wrong are the same thing.
	options.ContextTimeoutEnabled = true
	shards := cfg.Shards
	if shards <= 0 {
		return nil, fmt.Errorf("redisx: shards must be positive, got %d", shards)
	}
	return &Client{Client: redis.NewClient(options), keys: NewKeys(cfg.Prefix, shards)}, nil
}

// Wrap adapts an already-built Redis client, which is how tests point the fleet at
// miniredis without going through a URL.
func Wrap(rdb *redis.Client, prefix string, shards int) *Client {
	return &Client{Client: rdb, keys: NewKeys(prefix, shards)}
}

// Ping reports whether the server answers within the timeout.
func (c *Client) Ping(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := c.Client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redisx: ping: %w", err)
	}
	return nil
}
