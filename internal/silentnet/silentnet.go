// Package silentnet is a TCP relay that can stop answering without closing anything.
//
// That is what a paused or partitioned Redis looks like, and it is not what a closed one
// looks like: a closed server fails each call at once, a silent one holds each call for
// the client's whole timeout (#353). Only tests import it.
package silentnet

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
)

// Relay forwards to a target until muted, then swallows every byte both ways.
type Relay struct {
	listener net.Listener
	target   string
	dialer   net.Dialer
	muted    atomic.Bool
	wg       sync.WaitGroup

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

// New starts a relay to target, closed when the test ends.
func New(t *testing.T, target string) *Relay {
	t.Helper()
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("silentnet: listen: %v", err)
	}
	r := &Relay{listener: listener, target: target, conns: map[net.Conn]struct{}{}}
	r.wg.Add(1)
	go r.accept()
	t.Cleanup(func() {
		_ = listener.Close()
		r.wg.Wait()
	})
	return r
}

// Addr is where a client connects.
func (r *Relay) Addr() string { return r.listener.Addr().String() }

// Mute stops forwarding. Connections stay open: writes succeed and reads never return.
func (r *Relay) Mute() { r.muted.Store(true) }

// Restore forwards again and drops every connection that was open, whose requests were
// swallowed and will never be answered: a client reconnects to a Redis that answers.
func (r *Relay) Restore() {
	r.muted.Store(false)
	r.mu.Lock()
	for conn := range r.conns {
		_ = conn.Close()
	}
	r.mu.Unlock()
}

func (r *Relay) track(conns ...net.Conn) {
	r.mu.Lock()
	for _, conn := range conns {
		r.conns[conn] = struct{}{}
	}
	r.mu.Unlock()
}

func (r *Relay) accept() {
	defer r.wg.Done()
	for {
		in, err := r.listener.Accept()
		if err != nil {
			return
		}
		// Not tied to the test's context: the relay outlives the parts of a test that
		// finish before its cleanup, and it is torn down by closing the listener.
		out, err := r.dialer.DialContext(context.Background(), "tcp", r.target)
		if err != nil {
			_ = in.Close()
			continue
		}
		r.track(in, out)
		r.wg.Add(2)
		go r.pipe(in, out)
		go r.pipe(out, in)
	}
}

func (r *Relay) pipe(from, to net.Conn) {
	defer r.wg.Done()
	defer func() { _ = from.Close(); _ = to.Close() }()
	buf := make([]byte, 4096)
	for {
		n, err := from.Read(buf)
		if err != nil {
			return
		}
		if r.muted.Load() {
			continue
		}
		if _, err := to.Write(buf[:n]); err != nil {
			return
		}
	}
}
