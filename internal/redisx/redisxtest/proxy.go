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
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			conns.Go(func() { proxy.relay(client, target) })
		}
	}()
	return proxy
}

// Addr is where a client should connect instead of the server.
func (p *Proxy) Addr() string { return p.addr }

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
	caught := make(chan struct{})
	p.mu.Lock()
	p.traps = append(p.traps, &trap{marker: []byte(marker), release: release, caught: caught})
	p.mu.Unlock()
	return caught
}

// spring takes the first trap this chunk of an answer matches, if any.
func (p *Proxy) spring(chunk []byte) *trap {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, candidate := range p.traps {
		if bytes.Contains(chunk, candidate.marker) {
			p.traps = append(p.traps[:i], p.traps[i+1:]...)
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
	for {
		n, err := server.Read(buf)
		if n > 0 {
			if sprung := p.spring(buf[:n]); sprung != nil {
				close(sprung.caught)
				if sprung.release == nil {
					return
				}
				select {
				case <-sprung.release:
				case <-p.done:
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
