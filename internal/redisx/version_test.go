package redisx_test

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx/redisxtest"
)

// helloMap is a HELLO reply as RESP3 carries it, from alternating names and values.
func helloMap(fields ...string) string {
	return fmt.Sprintf("%%%d\r\n", len(fields)/2) + bulks(fields)
}

// helloList is the same reply as RESP2 carries it: one flat list.
func helloList(fields ...string) string {
	return fmt.Sprintf("*%d\r\n", len(fields)) + bulks(fields)
}

func bulks(fields []string) string {
	var out strings.Builder
	for _, field := range fields {
		fmt.Fprintf(&out, "$%d\r\n%s\r\n", len(field), field)
	}
	return out.String()
}

func stubClient(t *testing.T, hello string, protocol int) (*redisx.Client, *redisxtest.Stub) {
	t.Helper()
	stub := redisxtest.NewStub(t, hello)
	rdb := redis.NewClient(&redis.Options{Addr: stub.Addr(), Protocol: protocol, MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	return redisx.Wrap(rdb, "wa:", 8), stub
}

// The floor is 6.2, and what decides it is the major and the minor: a patch or a build
// suffix does not move a server across it, and a newer major is above it whatever its
// minor. Under both protocols, because the reply is a map under one and a flat list under
// the other, and a reader of one shape finds no version in the other.
func TestTheServerVersionIsHeldToTheFloor(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		fields  []string
		refused bool
		names   []string // what a refusal has to say
	}{
		{name: "the floor itself", fields: []string{"server", "redis", "version", "6.2.0"}},
		{name: "a patch above the floor", fields: []string{"server", "redis", "version", "6.2.24"}},
		{name: "a newer major with a lower minor", fields: []string{"server", "redis", "version", "7.0.15"}},
		{name: "the newest", fields: []string{"server", "redis", "version", "8.4.0"}},
		{name: "a two digit major", fields: []string{"server", "redis", "version", "10.0.0"}},
		{name: "valkey, judged by its version", fields: []string{"server", "valkey", "version", "7.2.4"}},
		{name: "one minor below", fields: []string{"server", "redis", "version", "6.0.20"}, refused: true, names: []string{"6.0.20", "6.2"}},
		{name: "the minor just below", fields: []string{"server", "redis", "version", "6.1.9"}, refused: true, names: []string{"6.1.9", "6.2"}},
		{name: "an older major with a higher minor", fields: []string{"server", "redis", "version", "5.9.0"}, refused: true, names: []string{"5.9.0", "6.2"}},
		{name: "no version at all", fields: []string{"server", "redis", "proto", "3"}, refused: true, names: []string{"without a version", "6.2"}},
		{name: "a version that is not one", fields: []string{"server", "redis", "version", "abc"}, refused: true, names: []string{`"abc"`, "6.2"}},
		{name: "an empty version", fields: []string{"server", "redis", "version", ""}, refused: true, names: []string{`""`, "6.2"}},
		{name: "a major alone", fields: []string{"server", "redis", "version", "7"}, refused: true, names: []string{`"7"`, "6.2"}},
		{name: "a minor that is not a number", fields: []string{"server", "redis", "version", "6.x.1"}, refused: true, names: []string{`"6.x.1"`, "6.2"}},
	}
	for _, tc := range cases {
		for _, protocol := range []int{3, 2} {
			t.Run(fmt.Sprintf("%s/resp%d", tc.name, protocol), func(t *testing.T) {
				t.Parallel()

				hello := helloMap(tc.fields...)
				if protocol == 2 {
					hello = helloList(tc.fields...)
				}
				client, _ := stubClient(t, hello, protocol)
				err := client.RequireServerVersion(t.Context(), 5*time.Second)
				if !tc.refused {
					if err != nil {
						t.Fatalf("a server at or above the floor was refused: %v", err)
					}
					return
				}
				if err == nil {
					t.Fatal("a server it cannot hold to the floor was accepted")
				}
				for _, want := range tc.names {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("the refusal does not say %q: %v", want, err)
					}
				}
			})
		}
	}
}

// Redis 5 does not know HELLO. The client falls back to the older protocol on its own and
// every other command works, so the refusal has to come from the question itself, and it
// has to say which question, not read as a connection that failed.
func TestAServerThatDoesNotKnowHelloIsRefused(t *testing.T) {
	t.Parallel()

	client, stub := stubClient(t, "-ERR unknown command 'HELLO', with args beginning with: '3'\r\n", 3)
	if err := client.Ping(t.Context(), 5*time.Second); err != nil {
		t.Fatalf("the stub does not answer a ping, so it cannot stand in for Redis 5: %v", err)
	}
	err := client.RequireServerVersion(t.Context(), 5*time.Second)
	if err == nil {
		t.Fatalf("a server that does not answer HELLO was accepted; it was sent %v", stub.Seen())
	}
	for _, want := range []string{"HELLO", "6.2"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
}

// Every other test in the repository runs against miniredis, and a floor it did not pass
// would stop every one of them at startup.
func TestMiniredisIsAboveTheFloor(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := redisx.Wrap(rdb, "wa:", 8).RequireServerVersion(t.Context(), 5*time.Second); err != nil {
		t.Fatalf("miniredis is refused: %v", err)
	}
}

// The server `make test-redis` names, which CI points at the newest Redis and at the floor.
// Both have to start: a check that refused the floor itself would refuse every deployment
// that followed the README.
func TestARealServerAtOrAboveTheFloorIsAccepted(t *testing.T) {
	t.Parallel()

	url := os.Getenv("WAC_TEST_REDIS_URL")
	if url == "" {
		t.Skip("set WAC_TEST_REDIS_URL to run this against a real Redis (see 'make test-redis')")
	}
	client, err := redisx.New(redisx.Config{URL: url, Shards: 8})
	if err != nil {
		t.Fatalf("redisx.New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.RequireServerVersion(t.Context(), 5*time.Second); err != nil {
		t.Fatalf("the server WAC_TEST_REDIS_URL names was refused: %v", err)
	}
}
