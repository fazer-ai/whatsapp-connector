package cluster

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// DefaultHeartbeat is how often an instance says it is alive; the registry entry
// expires at three times that, so one missed beat is not a funeral.
const DefaultHeartbeat = 5 * time.Second

// Presence is what an instance advertises about itself.
//
// The protocol range is the load-bearing part: a client compares it with its own and
// refuses to talk when the two do not overlap, which is what stops a connector from
// publishing frames its client cannot read.
type Presence struct {
	Instance     string
	Version      string
	ProtocolMin  int
	ProtocolMax  int
	AdvertiseURL string
	MediaToken   string
	Sessions     int
}

// Registry keeps the fleet's view of who is running.
type Registry struct {
	client *redisx.Client
	ttl    time.Duration
}

// NewRegistry returns the registry. The TTL is how long an entry outlives its last
// heartbeat.
func NewRegistry(client *redisx.Client, ttl time.Duration) *Registry {
	if ttl <= 0 {
		ttl = 3 * DefaultHeartbeat
	}
	return &Registry{client: client, ttl: ttl}
}

// Announce writes this instance's entry and refreshes its expiry. Called on start and
// on every heartbeat: an entry that stops being refreshed disappears, which is how a
// killed instance leaves the fleet without anything having to notice it died.
func (r *Registry) Announce(ctx context.Context, presence *Presence) error {
	keys := r.client.Keys()
	key := keys.Instance(presence.Instance)
	fields := map[string]any{
		"version":      presence.Version,
		"protocol_min": presence.ProtocolMin,
		"protocol_max": presence.ProtocolMax,
		"sessions":     presence.Sessions,
		"updated_at":   time.Now().UnixMilli(),
	}
	if presence.AdvertiseURL != "" {
		fields["advertise_url"] = presence.AdvertiseURL
	}
	if presence.MediaToken != "" {
		fields["media_token"] = presence.MediaToken
	}

	_, err := r.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, key, fields)
		pipe.PExpire(ctx, key, r.ttl)
		pipe.SAdd(ctx, keys.Instances(), presence.Instance)
		return nil
	})
	if err != nil {
		return fmt.Errorf("cluster: announce %s: %w", presence.Instance, err)
	}
	return nil
}

// Withdraw removes this instance from the fleet, which a clean shutdown does so peers
// do not wait out the TTL to notice.
func (r *Registry) Withdraw(ctx context.Context, instance string) error {
	keys := r.client.Keys()
	_, err := r.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, keys.Instance(instance))
		pipe.SRem(ctx, keys.Instances(), instance)
		return nil
	})
	if err != nil {
		return fmt.Errorf("cluster: withdraw %s: %w", instance, err)
	}
	return nil
}

// Live lists the instances that still have an entry, and prunes the set of the ones
// that do not.
//
// The set and the entries expire differently on purpose: the entry has the TTL, the
// set is a plain set, so the set is the list of instances that ever announced and the
// entries are the ones alive now. Reading one without the other is how a dead instance
// stays in an operator's list forever.
func (r *Registry) Live(ctx context.Context) ([]Presence, error) {
	keys := r.client.Keys()
	names, err := r.client.SMembers(ctx, keys.Instances()).Result()
	if err != nil {
		return nil, fmt.Errorf("cluster: list instances: %w", err)
	}

	live := make([]Presence, 0, len(names))
	var stale []string
	for _, name := range names {
		fields, entryErr := r.client.HGetAll(ctx, keys.Instance(name)).Result()
		switch {
		case errors.Is(entryErr, redis.Nil), len(fields) == 0:
			stale = append(stale, name)
			continue
		case entryErr != nil:
			return nil, fmt.Errorf("cluster: read instance %s: %w", name, entryErr)
		}
		live = append(live, Presence{
			Instance:     name,
			Version:      fields["version"],
			ProtocolMin:  atoi(fields["protocol_min"]),
			ProtocolMax:  atoi(fields["protocol_max"]),
			AdvertiseURL: fields["advertise_url"],
			MediaToken:   fields["media_token"],
			Sessions:     atoi(fields["sessions"]),
		})
	}

	if len(stale) > 0 {
		if err := r.client.SRem(ctx, keys.Instances(), toAny(stale)...).Err(); err != nil {
			return nil, fmt.Errorf("cluster: prune instances: %w", err)
		}
	}
	return live, nil
}

func atoi(raw string) int {
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return value
}

func toAny(values []string) []any {
	out := make([]any, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}
