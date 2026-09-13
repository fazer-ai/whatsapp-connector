package redisxtest_test

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx/redisxtest"
)

// TCP may hand a marker over in two reads. A proxy that only looked inside each read would
// let that answer through, and a test waiting for the trap would fail on a transport doing
// nothing wrong.
func TestAMarkerSplitAcrossTwoWritesIsStillCaught(t *testing.T) {
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		// Apart in time, so they cannot arrive as one read on the other side.
		_, _ = conn.Write([]byte("answer carrying split-"))
		time.Sleep(50 * time.Millisecond)
		_, _ = conn.Write([]byte("marker\n"))
		time.Sleep(time.Second)
	}()

	proxy := redisxtest.Listen(t, listener.Addr().String())
	caught := proxy.Drop("split-marker")

	var dialer net.Dialer
	conn, err := dialer.DialContext(context.Background(), "tcp", proxy.Addr())
	if err != nil {
		t.Fatalf("dial the proxy: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, _ := bufio.NewReader(conn).ReadString('\n')

	select {
	case <-caught:
	default:
		t.Fatalf("the marker went through in two pieces uncaught; the client read %q", line)
	}
	if line == "answer carrying split-marker\n" {
		t.Fatal("the answer was dropped and still reached the client whole")
	}
}
