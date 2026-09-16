package redisx

import (
	"context"
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
		groups, err := s.client.XInfoGroups(ctx, stream).Result()
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
				float64(group.Pending), stream, group.Name)
			out <- prometheus.MustNewConstMetric(s.consumers, prometheus.GaugeValue,
				float64(group.Consumers), stream, group.Name)

			lag, known := lagOf(group.Lag)
			if !known {
				out <- prometheus.MustNewConstMetric(s.lagUnknown, prometheus.GaugeValue, 1, stream, group.Name)
				continue
			}
			out <- prometheus.MustNewConstMetric(s.lagUnknown, prometheus.GaugeValue, 0, stream, group.Name)
			out <- prometheus.MustNewConstMetric(s.lag, prometheus.GaugeValue, lag, stream, group.Name)
		}
	}
}

// lagOf reads the lag Redis reported, and says whether it reported one at all.
//
// Redis answers the lag as nil when it cannot work it out without walking the stream,
// which is what an XTRIM or an XDEL leaves behind, and go-redis carries that through as
// -1. Neither value is publishable: raw, it puts a negative on a panel; as zero, it puts
// "the client is up to date" on one, which is the worse of the two because it is the
// answer an operator stops looking after. So an unknown lag is not reported as a lag at
// all, and `wac_stream_lag_unknown` carries the fact that it could not be read.
func lagOf(reported int64) (float64, bool) {
	if reported < 0 {
		return 0, false
	}
	return float64(reported), true
}
