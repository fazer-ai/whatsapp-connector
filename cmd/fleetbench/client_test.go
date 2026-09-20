package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// A wait that nobody can interrupt is a run that sits on a dead socket after Ctrl-C.
//
// MEASURED against the pinned go-redis: a cancellation arriving while a BLPOP is already
// in flight does not reach it, with or without `ContextTimeoutEnabled` -- the call comes
// back after its own 60 s. The connectors are killed at once and the cleanup that drops
// this run's database and keys does not even start until then, which is how an interrupted
// run leaves exactly the leftovers nobody goes looking for.
//
// So the wait is sliced, and this is the test that says the slicing works: the wait below
// asks for a minute and has to come back in about the time the cancel takes.
func TestTheSlicedWaitNoticesACancelledContext(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	began := time.Now()
	_, err := blockingPop(ctx, rdb, "wac-fleetbench-lista-que-ninguem-preenche", time.Minute)
	took := time.Since(began)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a espera voltou com %v, e o que a encerrou devia ser o cancelamento", err)
	}
	// Generous on purpose: what is being measured is "slices" against "the whole minute",
	// and a loaded machine can take a while over one slice. A regression here is tens of
	// seconds, not milliseconds.
	if took > 10*time.Second {
		t.Errorf("a espera levou %s para notar o cancelamento, e ela existe justamente para nao "+
			"segurar a corrida ate o fim do proprio timeout", took.Round(time.Millisecond))
	}
}

// And a wait that nobody answers still ends on its own deadline, rather than running
// forever one slice at a time.
func TestTheSlicedWaitStillHonoursItsOwnDeadline(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	began := time.Now()
	_, err := blockingPop(t.Context(), rdb, "wac-fleetbench-lista-que-ninguem-preenche", 2*time.Second)
	took := time.Since(began)

	if !errors.Is(err, redis.Nil) {
		t.Fatalf("a espera que esgotou o proprio prazo voltou com %v, e quem a le espera redis.Nil", err)
	}
	if took < 2*time.Second {
		t.Errorf("a espera de 2s voltou em %s, entao ela nao esperou o que lhe foi pedido", took)
	}
}

// The answer comes back when it arrives, and it is the answer.
func TestTheSlicedWaitReturnsTheValue(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	key := "wac-fleetbench-reply"
	go func() {
		time.Sleep(100 * time.Millisecond)
		server.Lpush(key, `{"v":1,"ok":true}`)
	}()

	answer, err := blockingPop(t.Context(), rdb, key, 10*time.Second)
	if err != nil {
		t.Fatalf("a espera falhou: %v", err)
	}
	if len(answer) != 2 || answer[0] != key || answer[1] != `{"v":1,"ok":true}` {
		t.Errorf("a espera devolveu %q", answer)
	}
}
