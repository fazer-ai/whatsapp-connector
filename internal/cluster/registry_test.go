package cluster_test

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/cluster"
	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

func newRegistry(t *testing.T) (*miniredis.Miniredis, *cluster.Registry) {
	t.Helper()
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return server, cluster.NewRegistry(redisx.Wrap(rdb, "wa:", 8), 15*time.Second)
}

func presence(instance string) *cluster.Presence {
	return &cluster.Presence{
		Instance: instance, Version: "1.0.0", ProtocolMin: 1, ProtocolMax: 1,
		AdvertiseURL: "http://" + instance + ":8080", MediaToken: "t0ken", Sessions: 2,
	}
}

func TestAnnounceMakesAnInstanceVisible(t *testing.T) {
	t.Parallel()

	_, registry := newRegistry(t)
	ctx := context.Background()

	if err := registry.Announce(ctx, presence("inst-a")); err != nil {
		t.Fatalf("Announce: %v", err)
	}

	live, err := registry.Live(ctx)
	if err != nil {
		t.Fatalf("Live: %v", err)
	}
	if len(live) != 1 {
		t.Fatalf("Live returned %d instances, want 1", len(live))
	}
	got := live[0]
	if got.Instance != "inst-a" || got.Version != "1.0.0" || got.ProtocolMax != 1 || got.Sessions != 2 {
		t.Errorf("presence = %+v, want the announced one", got)
	}
	if got.AdvertiseURL != "http://inst-a:8080" || got.MediaToken != "t0ken" {
		t.Errorf("presence = %+v, want the media plane details it announced", got)
	}
}

// An instance that stops beating has to leave the fleet on its own: nothing runs when
// a process is killed, so the expiry is the only thing that can notice.
func TestAnInstanceThatStopsBeatingDisappears(t *testing.T) {
	t.Parallel()

	server, registry := newRegistry(t)
	ctx := context.Background()

	if err := registry.Announce(ctx, presence("inst-a")); err != nil {
		t.Fatalf("Announce: %v", err)
	}
	server.FastForward(16 * time.Second)

	live, err := registry.Live(ctx)
	if err != nil {
		t.Fatalf("Live: %v", err)
	}
	if len(live) != 0 {
		t.Fatalf("Live returned %+v, want nothing after the entry expired", live)
	}
	if server.Exists("wa:instances") {
		t.Error("the instance set still names a dead instance, so an operator's list never empties")
	}
}

func TestWithdrawLeavesImmediately(t *testing.T) {
	t.Parallel()

	_, registry := newRegistry(t)
	ctx := context.Background()

	if err := registry.Announce(ctx, presence("inst-a")); err != nil {
		t.Fatalf("Announce: %v", err)
	}
	if err := registry.Withdraw(ctx, "inst-a"); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}

	live, err := registry.Live(ctx)
	if err != nil {
		t.Fatalf("Live: %v", err)
	}
	if len(live) != 0 {
		t.Fatalf("Live returned %+v after a withdrawal", live)
	}
}

func TestLiveListsEveryRunningInstance(t *testing.T) {
	t.Parallel()

	_, registry := newRegistry(t)
	ctx := context.Background()

	for _, instance := range []string{"inst-a", "inst-b"} {
		if err := registry.Announce(ctx, presence(instance)); err != nil {
			t.Fatalf("Announce(%s): %v", instance, err)
		}
	}

	live, err := registry.Live(ctx)
	if err != nil {
		t.Fatalf("Live: %v", err)
	}
	if len(live) != 2 {
		t.Fatalf("Live returned %d instances, want 2", len(live))
	}
}

// Whether an instance carries the voice of calls is what a client reads before it offers
// them, so the hash says it in both directions, and a run under the same name that came
// back without the port overwrites what the previous one said rather than inheriting it.
func TestAnnounceSaysWhetherTheInstanceCarriesCalls(t *testing.T) {
	t.Parallel()

	server, registry := newRegistry(t)
	ctx := context.Background()

	with := presence("inst-a")
	with.Calls = true
	if err := registry.Announce(ctx, with); err != nil {
		t.Fatalf("Announce: %v", err)
	}
	if got := server.HGet("wa:instance:inst-a", "calls"); got != "true" {
		t.Fatalf("calls = %q after announcing with the socket open, want \"true\"", got)
	}

	if err := registry.Announce(ctx, presence("inst-a")); err != nil {
		t.Fatalf("Announce: %v", err)
	}
	if got := server.HGet("wa:instance:inst-a", "calls"); got != "false" {
		t.Fatalf("calls = %q after the same instance announced without the socket, want \"false\"", got)
	}
	live, err := registry.Live(ctx)
	if err != nil {
		t.Fatalf("Live: %v", err)
	}
	if len(live) != 1 || live[0].Calls {
		t.Fatalf("Live = %+v, want one instance that does not carry calls", live)
	}

	with.Instance = "inst-b"
	if err := registry.Announce(ctx, with); err != nil {
		t.Fatalf("Announce: %v", err)
	}
	live, err = registry.Live(ctx)
	if err != nil {
		t.Fatalf("Live: %v", err)
	}
	carries := map[string]bool{}
	for _, p := range live {
		carries[p.Instance] = p.Calls
	}
	if want := map[string]bool{"inst-a": false, "inst-b": true}; !maps.Equal(carries, want) {
		t.Fatalf("calls per instance = %v, want %v", carries, want)
	}
}
