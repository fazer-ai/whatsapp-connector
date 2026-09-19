package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// A census is what every live instance said it was running, at one moment.
//
// # Why the bench needs one at all
//
// Operational invariant 1 has an observable that the event streams cannot give. Two
// instances holding one lease do not publish under one epoch: every acquisition runs an
// `INCR`, so the second holder gets a *higher* epoch than the first and each `(sid, epoch)`
// still has exactly one publisher. Read only through the streams, a fleet where every
// instance owns every session looks like a fleet with a lot of ownership changes.
//
// MEASURED on this tree: under a mutant that opens the fleet's three ownership guards, one
// run produced 42 `(sid, epoch)` pairs over 4 sessions -- against 16 on the clean tree --
// and the per-epoch check stayed green through all of it.
//
// So the bench counts. The fleet runs N distinct sessions; the sum of what its instances
// say they are running cannot exceed N, because a session is run by one instance. That is
// the second observable the issue names, and it is the one that sees this.
//
// # What a census cannot do
//
// It reads over HTTP, so an instance that does not answer -- frozen, dead, still coming up
// -- contributes nothing. The count can therefore be lower than the truth and never higher,
// which is the direction that makes it safe: a total above N was really above N, and a
// total at N may be hiding a holder nobody could ask. The assertion says so rather than
// claiming the sum is the fleet.
type fleetSample struct {
	at     time.Time
	phase  string
	byInst map[string]int
	total  int
	silent []string
}

type census struct {
	samples []fleetSample
}

// take reads every instance handed to it and records one sample. Errors are not failures:
// an instance that cannot be reached is named in the sample as silent, because "did not
// answer" and "answered zero" are different facts and only one of them is a reading.
func (c *census) take(ctx context.Context, phase string, live []*instance) {
	sample := fleetSample{at: time.Now(), phase: phase, byInst: map[string]int{}}
	for _, one := range live {
		found, err := one.metrics(ctx, "wac_sessions_running")
		if err != nil {
			sample.silent = append(sample.silent, one.name)
			continue
		}
		count := int(found["wac_sessions_running"])
		sample.byInst[one.name] = count
		sample.total += count
	}
	c.samples = append(c.samples, sample)
}

// worst returns the sample with the highest total, which is the one that decides the
// claim, and the number of samples behind the answer.
func (c *census) worst() (highest fleetSample, samples int) {
	for _, sample := range c.samples {
		if sample.total > highest.total {
			highest = sample
		}
	}
	return highest, len(c.samples)
}

func (s fleetSample) String() string {
	names := make([]string, 0, len(s.byInst))
	for name := range s.byInst {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s=%d", name, s.byInst[name]))
	}
	out := fmt.Sprintf("na fase %q: %s, somando %d", s.phase, strings.Join(parts, " "), s.total)
	if len(s.silent) > 0 {
		out += fmt.Sprintf(" (nao responderam: %s)", strings.Join(s.silent, ", "))
	}
	return out
}
