package redisstream

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// groupCache remembers which command streams already have the consumer group, so the
// read loop does not pay a round trip per stream per iteration.
//
// The cache can only be wrong in the safe direction. A group that was created and then
// deleted (an operator, a flush) is missing from Redis while the cache says it is
// there; the read that follows fails with NOGROUP, and the next call recreates it
// because a failed ensure does not record anything.
type groupCache struct {
	mu      sync.Mutex
	ensured map[string]struct{}
}

func newGroupCache() *groupCache {
	return &groupCache{ensured: make(map[string]struct{})}
}

func (c *groupCache) ensure(ctx context.Context, client *redisx.Client, streams []string) error {
	for _, stream := range streams {
		c.mu.Lock()
		_, done := c.ensured[stream]
		c.mu.Unlock()
		if done {
			continue
		}

		// MKSTREAM so a session whose client has not written a command yet still has a
		// stream to read from, and `0` so a command published before this instance
		// started is still delivered rather than skipped.
		err := client.XGroupCreateMkStream(ctx, stream, ConsumerGroup, "0").Err()
		if err != nil && !isBusyGroup(err) {
			return fmt.Errorf("redisstream: create group on %s: %w", stream, err)
		}

		c.mu.Lock()
		c.ensured[stream] = struct{}{}
		c.mu.Unlock()
	}
	return nil
}

// forget drops a stream from the cache so the next ensure recreates its group. Called
// when a read comes back NOGROUP, which means the group went away under us.
func (c *groupCache) forget(stream string) {
	c.mu.Lock()
	delete(c.ensured, stream)
	c.mu.Unlock()
}

func isBusyGroup(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP")
}

func isNoGroup(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "NOGROUP")
}

func marshalReply(reply protocol.Reply) (string, error) {
	body, err := json.Marshal(reply)
	if err != nil {
		return "", fmt.Errorf("redisstream: marshal reply %s: %w", reply.ID, err)
	}
	return string(body), nil
}

func (c *groupCache) forgetAll(streams []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, stream := range streams {
		delete(c.ensured, stream)
	}
}
