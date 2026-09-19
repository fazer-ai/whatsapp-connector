package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/protocol"
)

// A steady stream of commands, for as long as ownership is moving.
//
// # Why bursts are not load
//
// The bench used to put a batch of sends in flight, kill the owner and then wait. Between
// the batch and the kill the fleet goes quiet, so whoever takes a session over takes one
// that is not publishing anything -- and the assertions about ordering then run over
// events that could not have interleaved.
//
// MEASURED with mutants: under a mutant that lets two instances hold one lease, the
// overlap lasts about one renewal tick, because the loser finds out on its next renewal.
// One second of two owners publishes nothing at all when nobody is asking the session to
// do anything, so the epoch-regression check -- the one observable that sees an overlap
// this short -- never had an event to see it in. The run came out green over a fleet where
// every instance owned every session.
//
// So the load runs through the whole of the handover and the frozen phase: every session
// is asked for something several times a second, and an instance that keeps publishing
// after losing the lease writes into the same stream as the one that took it.
//
// # What it is not
//
// Not a throughput measurement. Nothing here times a send or counts a rate: it exists to
// keep the fleet busy while ownership moves, and the numbers it produces are the series
// the assertions run over.
type load struct {
	sent    atomic.Int64
	stop    context.CancelFunc
	stopped sync.WaitGroup
}

// startLoad asks every session for something, over and over, until stop is called.
//
// The replies are left on their lists: what a duplicated side effect looks like from out
// here is two answers to one command that do not agree, and popping them would throw away
// the evidence. They carry a TTL, so they are read before the run's long phases.
func startLoad(ctx context.Context, active *run, cl *client, sids []string, every time.Duration) *load {
	running, stop := context.WithCancel(ctx)
	gen := &load{stop: stop}

	for _, sid := range sids {
		gen.stopped.Add(1)
		go func(sid string) {
			defer gen.stopped.Done()
			ticker := time.NewTicker(every)
			defer ticker.Stop()
			for {
				select {
				case <-running.Done():
					return
				case <-ticker.C:
				}
				n := gen.sent.Add(1)
				id := fmt.Sprintf("carga-%s-%s-%d", active.id, shortSID(sid), n)
				payload := fmt.Sprintf(`{"message_id":%q,"to":{"kind":"phone","id":"5511999990002"},`+
					`"content":{"type":"text","body":"carga continua %d"}}`, id, n)
				// A failure here is not a failure of the run: the load exists to keep the
				// fleet busy, and a command that did not reach Redis simply did not add to
				// the stream. What the assertions read is what landed.
				_ = cl.send(running, cl.keys.Commands(sid), &protocol.Command{
					V: protocol.Version, ID: id, Type: protocol.CommandMessageSend, SID: sid,
					TS: time.Now().UnixMilli(), ReplyTo: cl.keys.Reply(id),
					Payload: json.RawMessage(payload),
				})
			}
		}(sid)
	}
	return gen
}

// end stops the load and waits for its goroutines, so nothing is still writing to a stream
// the assertions are about to read.
func (l *load) end() int64 {
	l.stop()
	l.stopped.Wait()
	return l.sent.Load()
}
