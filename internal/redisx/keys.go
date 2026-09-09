// Package redisx holds what every other package needs to speak to Redis: the key
// layout the contract fixes, and the client that serves it.
//
// The key names live here rather than being written inline where they are used
// because they are shared with the Chatwoot side: a name spelled from memory at one
// call site is a stream nobody reads.
package redisx

import (
	"hash/fnv"
	"strconv"
	"strings"
)

// DefaultPrefix is what both sides use unless the deployment overrides it. The
// override exists so one Redis can host two independent fleets; it is not a
// security boundary.
const DefaultPrefix = "wa:"

// Keys renders every key of the contract under one prefix.
type Keys struct {
	prefix string
	shards int
}

// NewKeys returns the key set for a prefix and a shard count. A prefix without its
// separator gets one, so both "wa" and "wa:" name the same fleet: the alternative is
// a deployment that half-matches its client and reads an empty stream forever.
func NewKeys(prefix string, shards int) Keys {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	if prefix[len(prefix)-1] != ':' {
		prefix += ":"
	}
	return Keys{prefix: prefix, shards: shards}
}

// Prefix is the normalised prefix, separator included.
func (k Keys) Prefix() string { return k.prefix }

// Shards is how many event streams the fleet publishes to.
func (k Keys) Shards() int { return k.shards }

// Meta is the hash a starting instance checks itself against.
func (k Keys) Meta() string { return k.prefix + "meta" }

// ShardOf places a session on its event stream. The mapping has to be identical on
// both sides and stable forever: it is what guarantees that all events of one
// session land in one stream, read by one consumer, in order. Changing the hash or
// the shard count reorders history, which is why the count is published in Meta and
// an instance that disagrees refuses to start.
func (k Keys) ShardOf(sid string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(sid))
	return int(h.Sum32() % uint32(k.shards)) //nolint:gosec // shards is small and positive
}

// Events is the stream a session's events are published to.
func (k Keys) Events(shard int) string { return k.prefix + "events:" + strconv.Itoa(shard) }

// EventsOf is Events for the shard a session belongs to.
func (k Keys) EventsOf(sid string) string { return k.Events(k.ShardOf(sid)) }

// EventsLease is the key a client holds to be the single reader of a shard.
func (k Keys) EventsLease(shard int) string { return k.Events(shard) + ":lease" }

// Commands is the stream a client writes commands for one session to.
func (k Keys) Commands(sid string) string { return k.prefix + "cmd:" + sid }

// Control is the stream for commands no single session owns.
func (k Keys) Control() string { return k.prefix + "control" }

// Reply is the list one RPC reply is pushed to.
func (k Keys) Reply(commandID string) string { return k.prefix + "reply:" + commandID }

// IsReply reports whether a key names a reply list of this fleet, and nothing else.
//
// A command carries the key it wants its answer on, so the value is a client's to
// choose and this connector's to check: everything else under the prefix is fleet
// state -- the session set, the leases, the streams -- and an answer written at one of
// those names would put a TTL on it. Naming an id is required too, since the bare
// namespace is not a destination.
func (k Keys) IsReply(key string) bool {
	return strings.HasPrefix(key, k.prefix+"reply:") && len(key) > len(k.prefix)+len("reply:")
}

// Sessions is the set of every session the fleet knows about.
func (k Keys) Sessions() string { return k.prefix + "sessions" }

// Session is the snapshot hash of one session.
func (k Keys) Session(sid string) string { return k.prefix + "session:" + sid }

// Lease is the key naming the instance that owns a session.
func (k Keys) Lease(sid string) string { return k.prefix + "lease:" + sid }

// LeaseEpoch is the counter incremented on every ownership change.
func (k Keys) LeaseEpoch(sid string) string { return k.prefix + "lease-epoch:" + sid }

// Handoff asks the current owner to release a session.
func (k Keys) Handoff(sid string) string { return k.prefix + "handoff:" + sid }

// HandBack names the instance that holds a session's lease and is giving it up.
//
// The lease says who owns an account; this says that whoever owns it has stopped running
// it and is on the way to letting go. A wake landing in between finds the lease taken and
// would otherwise be acknowledged as somebody else's -- and once the release lands, the
// account is owned by nobody with the one wake that would have started it retired.
func (k Keys) HandBack(sid string) string { return k.prefix + "handback:" + sid }

// Cooldown keeps a session from being reclaimed the instant it was released.
func (k Keys) Cooldown(sid string) string { return k.prefix + "cooldown:" + sid }

// Quarantine holds a session that keeps failing to connect.
func (k Keys) Quarantine(sid string) string { return k.prefix + "quarantine:" + sid }

// Instances is the set of live connector instances.
func (k Keys) Instances() string { return k.prefix + "instances" }

// Instance is the registry entry of one connector instance.
func (k Keys) Instance(inst string) string { return k.prefix + "instance:" + inst }

// Consumer is the heartbeat of one client consumer.
func (k Keys) Consumer(cid string) string { return k.prefix + "consumer:" + cid }

// Cursor is where a client records how far it has read a session.
func (k Keys) Cursor(sid string) string { return k.prefix + "cursor:" + sid }

// Idempotency is the record of a command that has already been carried out.
func (k Keys) Idempotency(sid, key string) string { return k.prefix + "idem:" + sid + ":" + key }

// DLQEvents holds events a client could not process.
func (k Keys) DLQEvents() string { return k.prefix + "dlq:events" }

// DLQCommands holds commands the connector could not carry out.
func (k Keys) DLQCommands() string { return k.prefix + "dlq:commands" }
