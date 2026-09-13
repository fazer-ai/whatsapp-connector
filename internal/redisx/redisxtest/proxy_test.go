package redisxtest_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx/redisxtest"
)

// TCP may hand a marker over in two reads. A proxy that only looked inside each read would
// let that answer through, and a test waiting for the trap would fail on a transport doing
// nothing wrong.
func TestAMarkerSplitAcrossTwoWritesIsStillCaught(t *testing.T) {
	const first, second = "answer carrying split-", "marker\n"

	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	// The second piece is written only once the client holds the first, which the proxy can
	// only have relayed after a read that ended there: the two cannot share a read.
	firstArrived := make(chan struct{})
	served := make(chan struct{})
	go func() {
		defer close(served)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = conn.Write([]byte(first))
		<-firstArrived
		_, _ = conn.Write([]byte(second))
	}()

	proxy := redisxtest.Listen(t, listener.Addr().String())
	caught := proxy.Drop("split-marker")

	var dialer net.Dialer
	conn, err := dialer.DialContext(context.Background(), "tcp", proxy.Addr())
	if err != nil {
		t.Fatalf("dial the proxy: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	got := make([]byte, len(first))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("the first piece never arrived: %v", err)
	}
	close(firstArrived)
	rest, _ := io.ReadAll(conn)
	<-served

	select {
	case <-caught:
	default:
		t.Fatalf("the marker went through in two pieces uncaught; after the first piece the client read %q", rest)
	}
	if len(rest) != 0 {
		t.Fatalf("the answer was dropped and the client still read %q after the first piece", rest)
	}
}

// What stays in view to catch a split marker is an answer the client already has. A trap
// armed after it went through has to wait for the next answer that carries the marker,
// not spring on whatever the connection carries next: a test would then hold an unrelated
// answer, believe its stimulus landed, and pass on a race that never happened.
func TestAMarkerAlreadyRelayedDoesNotSpringATrapArmedAfterIt(t *testing.T) {
	const (
		before    = "answer carrying late-marker\n"
		unrelated = "unrelated answer\n"
		after     = "late-marker again\n"
	)

	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	next := make(chan string)
	served := make(chan struct{})
	go func() {
		defer close(served)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		for piece := range next {
			_, _ = conn.Write([]byte(piece))
		}
	}()

	proxy := redisxtest.Listen(t, listener.Addr().String())
	var dialer net.Dialer
	conn, err := dialer.DialContext(context.Background(), "tcp", proxy.Addr())
	if err != nil {
		t.Fatalf("dial the proxy: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	receive := func(want string) error {
		got := make([]byte, len(want))
		if _, err := io.ReadFull(conn, got); err != nil {
			return err
		}
		if string(got) != want {
			t.Fatalf("the client read %q, want %q", got, want)
		}
		return nil
	}

	next <- before
	if err := receive(before); err != nil {
		t.Fatalf("the first answer never arrived: %v", err)
	}
	caught := proxy.Drop("late-marker")
	next <- unrelated
	if err := receive(unrelated); err != nil {
		t.Fatalf("the answer after the relayed marker was caught instead of relayed: %v", err)
	}
	next <- after
	close(next)
	rest, _ := io.ReadAll(conn)
	<-served
	select {
	case <-caught:
	default:
		t.Fatalf("the next answer carrying the marker went through uncaught; the client read %q", rest)
	}
}
