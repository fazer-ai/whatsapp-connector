package redisstream

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// reportTrimmed says so when a `MAXLEN` trim has taken commands out of a stream before
// this group was handed them.
//
// Nothing else will. A client writes commands with `MAXLEN ~`, so Redis drops the oldest
// entries on its own and the consumer group is never consulted: the publisher got an id
// back, the group goes on from its last-delivered-id, and the next read simply returns
// whatever survived. Measured on Redis 8, on a group that had been handed 3 of 10 entries
// and a stream then trimmed to 2: the next read returns 9 and 10 and skips five commands
// with no error anywhere, `lag` falls from 7 to 2 as if the work had been done, and
// `entries-read` then climbs past the five nobody saw.
//
// Asked once per stream per process, where this instance first touches it, which is also
// when a backlog would have formed: the account was running nowhere until now.
func (s *Streams) reportTrimmed(ctx context.Context, streams []string) {
	for _, stream := range streams {
		lost, found := trimmedAway(ctx, s.client, stream)
		if !found {
			continue
		}
		s.opts.Logger.Warn().
			Str("stream", stream).
			Int64("commands_lost", lost).
			Msg("commands were trimmed out of this stream before they were delivered")
	}
}

func trimmedAway(ctx context.Context, client *redisx.Client, stream string) (int64, bool) {
	groups, err := client.XInfoGroups(ctx, stream).Result()
	if err != nil {
		// Not answered with: this is a diagnosis and failing it must not fail the read it
		// runs in front of. The stream may also have gone away between the group ensure
		// and here, which is not a cut.
		return 0, false
	}
	var group *redis.XInfoGroup
	for i := range groups {
		if groups[i].Name == ConsumerGroup {
			group = &groups[i]
			break
		}
	}
	if group == nil {
		return 0, false
	}
	info, err := client.XInfoStream(ctx, stream).Result()
	if err != nil {
		return 0, false
	}
	return wasCut(info.EntriesAdded, group.EntriesRead, info.Length, group.Lag)
}

// wasCut works out how many entries were dropped before the group was handed them, from
// the counters that can answer it, and reports whether that is knowable at all.
//
// `entries-added` counts every entry the stream has ever taken and never goes down, so
// `added - read` is what the group is still owed. What it can still be given is what is
// left in the stream, and anything owed beyond that no longer exists: it was trimmed
// while undelivered. Every part of that is a count, and none of it is arithmetic on ids.
//
// **Comparing the oldest surviving id against the last-delivered one does not work**, and
// it is the obvious thing to reach for: it is what #176 proposed and it is wrong. A trim
// that removes only entries the group already read leaves the oldest surviving entry later
// than the last delivered one, which is the same reading a real cut produces -- so the
// heuristic reports a loss on every ordinary trim, which for a stream at its cap is every
// trim there is. Ids also cannot say whether anything ever sat between two of them, so no
// comparison of two of them can tell a gap from a pair that was always adjacent.
//
// `lag` is what says the counters can be trusted. Redis answers it with null when it
// cannot work the group's position out -- which is exactly when `entries-read` is unknown
// too -- and go-redis turns that null into -1, while leaving `entries-read` at zero where
// a caller cannot tell it from a group that has read nothing. Without this the two are
// confused, and a group carried across an upgrade from Redis 6 would have its whole
// history reported as lost the first time an instance adopted it.
//
// A server too old to report `entries-added` at all needs no guard of its own: it answers
// zero, nothing can be owed, and the arithmetic says nothing on its own.
func wasCut(added, read, length, lag int64) (int64, bool) {
	if lag < 0 {
		return 0, false
	}
	owed := added - read
	if owed <= length {
		return 0, false
	}
	return owed - length, true
}
