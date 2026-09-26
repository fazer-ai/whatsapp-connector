package redisx

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// lagScrapeTimeout bounds the whole of one scrape, every shard together.
//
// Short, and shorter than a Prometheus scrape interval by a wide margin, because the
// alternative to a late answer here is no answer: a scrape that hangs holds the metrics
// handler, and an operator watching an incident loses every other metric this instance
// publishes along with this one.
const lagScrapeTimeout = 2 * time.Second

// StreamLag reports how far behind the clients reading the event shards are.
//
// A Collector rather than a set of gauges kept current by a loop, because the value
// lives in Redis and not here. A loop would publish its last reading for as long as the
// loop stayed broken, which reads on a dashboard exactly like a client that is keeping
// up; not answering is the honest failure and the one an operator can see.
//
// Every instance reports the same number for a given shard, because lag is a property of
// the stream and not of whoever asked. That is deliberate: deduplicating in the query
// (`max by (stream, group)`) costs one line of PromQL, and electing one instance to
// report instead would buy a coordination problem -- who elects, what happens when the
// elected one dies, how long the shard goes unmeasured in between -- to avoid it.
type StreamLag struct {
	client *Client

	lag          *prometheus.Desc
	lagUnknown   *prometheus.Desc
	pending      *prometheus.Desc
	consumers    *prometheus.Desc
	scrapeFailed *prometheus.Desc
}

// NewStreamLag returns the collector. Register it on the metrics registry.
func NewStreamLag(client *Client) *StreamLag {
	perGroup := []string{"stream", "group"}
	return &StreamLag{
		client: client,
		lag: prometheus.NewDesc("wac_stream_lag",
			"Entries added to an event shard that this consumer group has not read yet.",
			perGroup, nil),
		lagUnknown: prometheus.NewDesc("wac_stream_lag_unknown",
			"1 when Redis could not compute the lag for this group, in which case wac_stream_lag is not reported at all.",
			perGroup, nil),
		pending: prometheus.NewDesc("wac_stream_pending",
			"Entries this consumer group has taken and not acknowledged.",
			perGroup, nil),
		consumers: prometheus.NewDesc("wac_stream_consumers",
			"Consumers currently in this group.",
			perGroup, nil),
		scrapeFailed: prometheus.NewDesc("wac_stream_scrape_failed",
			"1 when the group information for this shard could not be read.",
			[]string{"stream"}, nil),
	}
}

// Describe is deliberately empty, which makes this an unchecked collector.
//
// The label values are whatever groups exist on a shard at scrape time: a client names
// its own group in the contract, and this connector never creates it. Declaring the
// descriptors here would have the registry reject a second collector that happened to
// describe the same metric, which is a duplicate-registration check this cannot pass
// and does not need.
func (s *StreamLag) Describe(chan<- *prometheus.Desc) {}

// Collect reads every shard's consumer groups and reports one set of samples per group.
func (s *StreamLag) Collect(out chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), lagScrapeTimeout)
	defer cancel()

	for shard := range s.client.Keys().Shards() {
		stream := s.client.Keys().Events(shard)
		groups, err := s.groups(ctx, stream)
		if err != nil {
			// Includes the stream not existing yet, which is an ordinary state for a
			// fleet that has not published to this shard: there is nothing to say about
			// groups that are not there, and saying "failed" is the honest word for it
			// either way, because this instance does not know which one it is.
			out <- prometheus.MustNewConstMetric(s.scrapeFailed, prometheus.GaugeValue, 1, stream)
			continue
		}
		out <- prometheus.MustNewConstMetric(s.scrapeFailed, prometheus.GaugeValue, 0, stream)
		for _, group := range groups {
			out <- prometheus.MustNewConstMetric(s.pending, prometheus.GaugeValue,
				float64(group.pending), stream, group.name)
			out <- prometheus.MustNewConstMetric(s.consumers, prometheus.GaugeValue,
				float64(group.consumers), stream, group.name)

			if !group.lagKnown {
				out <- prometheus.MustNewConstMetric(s.lagUnknown, prometheus.GaugeValue, 1, stream, group.name)
				continue
			}
			out <- prometheus.MustNewConstMetric(s.lagUnknown, prometheus.GaugeValue, 0, stream, group.name)
			out <- prometheus.MustNewConstMetric(s.lag, prometheus.GaugeValue, group.lag, stream, group.name)
		}
	}
}

// groupReading is what one consumer group reports about itself.
type groupReading struct {
	name               string
	pending, consumers int64
	lag                float64
	lagKnown           bool
}

// groups reads a stream's consumer groups from the reply itself rather than through
// go-redis's XInfoGroup, because the question that matters here is one the struct cannot
// answer: whether the server reported a lag at all. Redis before 7.0 has no `lag` field,
// and the struct leaves it at zero, which is "the client is up to date" on every group of
// a server that never said so (#320).
func (s *StreamLag) groups(ctx context.Context, stream string) ([]groupReading, error) {
	reply, err := s.client.Do(ctx, "XINFO", "GROUPS", stream).Slice()
	if err != nil {
		return nil, err
	}
	groups := make([]groupReading, 0, len(reply))
	for _, item := range reply {
		fields, ok := fieldsOf(item)
		if !ok {
			return nil, fmt.Errorf("redisx: XINFO GROUPS %s answered a group as %T", stream, item)
		}
		name, _ := fields["name"].(string)
		pending, _ := fields["pending"].(int64)
		consumers, _ := fields["consumers"].(int64)
		lag, known := lagOf(fields)
		groups = append(groups, groupReading{name: name, pending: pending, consumers: consumers, lag: lag, lagKnown: known})
	}
	return groups, nil
}

// fieldsOf reads one group's fields, which RESP3 carries as a map and RESP2 as a flat list
// of names and values.
func fieldsOf(item any) (map[string]any, bool) {
	fields := map[string]any{}
	switch reply := item.(type) {
	case map[any]any:
		for name, value := range reply {
			if name, ok := name.(string); ok {
				fields[name] = value
			}
		}
	case []any:
		for i := 0; i+1 < len(reply); i += 2 {
			if name, ok := reply[i].(string); ok {
				fields[name] = reply[i+1]
			}
		}
	default:
		return nil, false
	}
	return fields, true
}

// lagOf reads the lag a group reported, and says whether it reported one at all.
//
// Two answers are not a lag, and neither is publishable as one. Redis answers null when
// it cannot work the lag out without walking the stream, which is what an XTRIM or an XDEL
// leaves behind. And Redis before 7.0 does not answer the field at all. Raw, either would
// put a guess on a panel; as zero, they put "the client is up to date" on one, which is
// the worse of the two because it is the answer an operator stops looking after. So an
// unknown lag is not reported as a lag, and `wac_stream_lag_unknown` carries the fact
// that it could not be read.
func lagOf(fields map[string]any) (float64, bool) {
	reported, ok := fields["lag"].(int64)
	if !ok || reported < 0 {
		return 0, false
	}
	return float64(reported), true
}
