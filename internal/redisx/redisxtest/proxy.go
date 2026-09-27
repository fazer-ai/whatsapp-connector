// Package redisxtest puts a connection a test can interfere with between a client and
// Redis.
//
// A read that Redis carried out and the client never heard back from is the failure it
// exists for. The server has already moved the entries it answered with into the
// consumer's pending list by the time the answer is lost, so what a test needs is not a
// server that refuses, which miniredis can do on its own, but an answer that goes
// missing on the way back: held past the caller's deadline, or dropped with the
// connection under it.
package redisxtest

import (
	"bytes"
	"context"
	"io"
	"net"
	"slices"
	"sync"
	"testing"
)

// Proxy relays every connection to the server it was started for, and lets a test catch
// one answer on its way back to the client.
type Proxy struct {
	addr string
	done chan struct{}

	mu    sync.Mutex
	traps []*trap
	open  map[net.Conn]struct{}

	caught  uint64     // answers caught so far, held or dropped
	holding int        // answers caught and not yet released
	settled *sync.Cond // broadcast when holding falls to zero
}

// trap is one answer a test asked to catch. The first answer from the server containing
// marker springs it, and it springs once.
type trap struct {
	marker  []byte
	release <-chan struct{} // nil drops the connection instead of holding the answer
	caught  chan struct{}
}

// Listen starts a proxy in front of target, stopped when the test ends.
func Listen(t testing.TB, target string) *Proxy {
	t.Helper()

	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("redisxtest: listen: %v", err)
	}
	proxy := &Proxy{addr: listener.Addr().String(), done: make(chan struct{}), open: make(map[net.Conn]struct{})}
	proxy.settled = sync.NewCond(&proxy.mu)

	// Every connection is closed from here rather than left to whoever holds its other end:
	// the server usually outlives the proxy in a test's cleanup order, and a relay blocked
	// reading from it would keep the wait below from ever returning.
	var conns sync.WaitGroup
	t.Cleanup(func() {
		_ = listener.Close()
		close(proxy.done)
		proxy.mu.Lock()
		for conn := range proxy.open {
			_ = conn.Close()
		}
		proxy.mu.Unlock()
		conns.Wait()
	})
	// The accept loop is in the group too: a relay it registers after an Accept that raced
	// the listener's close is then added while the count is still above zero, which is
	// what lets the wait above start before it.
	conns.Go(func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			conns.Go(func() { proxy.relay(client, target) })
		}
	})
	return proxy
}

// markerReach is how much of an answer stays in view after it has been relayed, and so the
// longest marker that is recognised however TCP splits it.
const markerReach = 256

// Addr is where a client should connect instead of the server.
func (p *Proxy) Addr() string { return p.addr }

// State is what the proxy has done and stands ready to do, read at one instant: how many
// answers it has caught so far, held or dropped, whether a trap is waiting for its answer,
// and whether an answer it caught is still held. Read together because a trap springs
// between any two separate reads, and a caller comparing them would see one without the
// other.
type State struct {
	Caught  uint64
	Armed   bool
	Holding bool
}

// State reads the proxy's state.
func (p *Proxy) State() State {
	p.mu.Lock()
	defer p.mu.Unlock()
	return State{Caught: p.caught, Armed: len(p.traps) > 0, Holding: p.holding > 0}
}

// AwaitNothingHeld returns once no caught answer is held any longer, or with ctx's error.
// Closing a release only lets the relay go on when it next runs; a test that reads the
// state right after, as the first command of its next read does, has to wait for that.
func (p *Proxy) AwaitNothingHeld(ctx context.Context) error {
	stop := context.AfterFunc(ctx, func() {
		p.mu.Lock()
		p.settled.Broadcast()
		p.mu.Unlock()
	})
	defer stop()
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.holding > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.settled.Wait()
	}
	return nil
}

// Hold keeps the next answer containing marker from reaching the client until release is
// closed. Everything behind it on that connection waits too, so what the client reads
// stays in the order the server wrote it. The returned channel closes when the answer is
// caught, which is how a test knows the stimulus landed rather than assuming it did.
func (p *Proxy) Hold(marker string, release <-chan struct{}) <-chan struct{} {
	return p.arm(marker, release)
}

// Drop closes the connection that the next answer containing marker was on, without
// letting the answer through. To the client it is a connection that died after the
// server had carried the command out.
func (p *Proxy) Drop(marker string) <-chan struct{} {
	return p.arm(marker, nil)
}

func (p *Proxy) arm(marker string, release <-chan struct{}) <-chan struct{} {
	if marker == "" || len(marker) > markerReach {
		panic("redisxtest: a marker has to be between 1 and 256 bytes")
	}
	caught := make(chan struct{})
	p.mu.Lock()
	p.traps = append(p.traps, &trap{marker: []byte(marker), release: release, caught: caught})
	p.mu.Unlock()
	return caught
}

// released is a held answer let through, or given up on when the proxy stops.
func (p *Proxy) released() {
	p.mu.Lock()
	p.holding--
	if p.holding == 0 {
		p.settled.Broadcast()
	}
	p.mu.Unlock()
}

// spring takes the first trap what the server just wrote matches, if any. seen is the
// chunk that just arrived with the end of what came before it on the same connection in
// front, the first relayed bytes of it, so a marker TCP delivered in two pieces is still
// recognised. A marker has to end in the new chunk: one wholly inside what was already
// relayed belongs to an answer the client has, and matching it would catch whatever
// unrelated answer the connection carries next.
func (p *Proxy) spring(seen []byte, relayed int) *trap {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, candidate := range p.traps {
		if bytes.Contains(seen[max(0, relayed-len(candidate.marker)+1):], candidate.marker) {
			p.traps = append(p.traps[:i], p.traps[i+1:]...)
			// Counted in the same step that disarms the trap, so that the proxy is never
			// seen idle between the two.
			p.caught++
			if candidate.release != nil {
				p.holding++
			}
			return candidate
		}
	}
	return nil
}

// track registers a connection for the cleanup to close, and reports false when the
// proxy is already stopping, in which case the connection has been closed here.
func (p *Proxy) track(conn net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.done:
		_ = conn.Close()
		return false
	default:
		p.open[conn] = struct{}{}
		return true
	}
}

func (p *Proxy) forget(conn net.Conn) {
	p.mu.Lock()
	delete(p.open, conn)
	p.mu.Unlock()
	_ = conn.Close()
}

func (p *Proxy) relay(client net.Conn, target string) {
	if !p.track(client) {
		return
	}
	defer p.forget(client)
	var dialer net.Dialer
	server, err := dialer.DialContext(context.Background(), "tcp", target)
	if err != nil || !p.track(server) {
		return
	}
	defer p.forget(server)

	go func() {
		_, _ = io.Copy(server, client)
		_ = server.Close()
	}()

	buf := make([]byte, 64<<10)
	var tail []byte
	for {
		n, err := server.Read(buf)
		if n > 0 {
			seen := slices.Concat(tail, buf[:n])
			relayed := len(tail)
			// Longer than any marker a test names, so the piece of one that ended the last
			// chunk is always still here when the rest arrives.
			tail = append([]byte(nil), seen[max(0, len(seen)-markerReach):]...)
			if sprung := p.spring(seen, relayed); sprung != nil {
				close(sprung.caught)
				if sprung.release == nil {
					return
				}
				select {
				case <-sprung.release:
					p.released()
				case <-p.done:
					p.released()
					return
				}
			}
			if _, err := client.Write(buf[:n]); err != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}
