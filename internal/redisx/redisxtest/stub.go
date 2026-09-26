package redisxtest

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// Stub is a server that answers `HELLO` the way a test says and every other command
// with a plain success, and remembers which commands it was sent.
//
// It stands in for a server miniredis cannot be: one of another version, or one whose
// `HELLO` is not what a Redis at or above the floor answers. What it records is how a
// test tells a refusal before anything was written from one after.
type Stub struct {
	addr string

	mu   sync.Mutex
	seen []string
}

// NewStub starts a stub whose answer to `HELLO` is hello, verbatim RESP, stopped when the
// test ends.
func NewStub(t testing.TB, hello string) *Stub {
	t.Helper()

	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("redisxtest: listen: %v", err)
	}
	stub := &Stub{addr: listener.Addr().String()}

	var conns sync.WaitGroup
	var mu sync.Mutex
	open := make(map[net.Conn]struct{})
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		for conn := range open {
			_ = conn.Close()
		}
		mu.Unlock()
		conns.Wait()
	})
	conns.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			open[conn] = struct{}{}
			mu.Unlock()
			conns.Go(func() {
				defer func() {
					mu.Lock()
					delete(open, conn)
					mu.Unlock()
					_ = conn.Close()
				}()
				stub.serve(conn, hello)
			})
		}
	})
	return stub
}

// Addr is where the stub listens.
func (s *Stub) Addr() string { return s.addr }

// Seen is every command the stub was sent, upper-cased, in the order they arrived.
func (s *Stub) Seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seen...)
}

func (s *Stub) serve(conn net.Conn, hello string) {
	reader := bufio.NewReader(conn)
	for {
		args, err := readCommand(reader)
		if err != nil {
			return
		}
		name := strings.ToUpper(args[0])
		s.mu.Lock()
		s.seen = append(s.seen, name)
		s.mu.Unlock()
		answer := "+OK\r\n"
		switch name {
		case "HELLO":
			answer = hello
		case "PING":
			answer = "+PONG\r\n"
		}
		if _, err := io.WriteString(conn, answer); err != nil {
			return
		}
	}
}

// readCommand reads one command in the form every client sends: an array of bulk strings.
func readCommand(reader *bufio.Reader) ([]string, error) {
	count, err := readHeader(reader, '*')
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, count)
	for range count {
		size, err := readHeader(reader, '$')
		if err != nil {
			return nil, err
		}
		value := make([]byte, size+2)
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}
		args = append(args, string(value[:size]))
	}
	if len(args) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	return args, nil
}

func readHeader(reader *bufio.Reader, kind byte) (int, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return 0, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" || line[0] != kind {
		return 0, io.ErrUnexpectedEOF
	}
	return strconv.Atoi(line[1:])
}
