package redisx_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/fazer-ai/whatsapp-connector/internal/redisx"
)

// withServer hands back the miniredis alongside the client, because what these tests are
// about is what is left in Redis and for how long, which the client cannot be asked.
func withServer(t *testing.T) (*redisx.Client, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return redisx.Wrap(rdb, "wa:", 8), server
}

// The three states a command can be in, and the order they are decided in.
func TestTheLedgerTellsAnAttemptFromAResultAndBothFromNothing(t *testing.T) {
	t.Parallel()

	client, _ := withServer(t)
	ledger := redisx.NewIdempotency(client, time.Hour)
	ctx := t.Context()

	result, done, attempted, err := ledger.Recall(ctx, "s1", "k1")
	if err != nil {
		t.Fatalf("Recall of a key nobody has heard of: %v", err)
	}
	if done || attempted || result != nil {
		t.Fatalf("a key nobody has heard of read as done=%v attempted=%v result=%s", done, attempted, result)
	}

	if err := ledger.Reserve(ctx, "s1", "k1"); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	_, done, attempted, err = ledger.Recall(ctx, "s1", "k1")
	if err != nil {
		t.Fatalf("Recall after Reserve: %v", err)
	}
	if done {
		t.Error("an attempt with no result read as a command that finished")
	}
	if !attempted {
		t.Error("an attempt did not read as one, so a redelivery would carry the command out again")
	}

	if err := ledger.Remember(ctx, "s1", "k1", json.RawMessage(`{"added":1}`)); err != nil {
		t.Fatalf("Remember: %v", err)
	}
	result, done, attempted, err = ledger.Recall(ctx, "s1", "k1")
	if err != nil {
		t.Fatalf("Recall after Remember: %v", err)
	}
	if !done || !attempted {
		t.Errorf("a finished command read as done=%v attempted=%v", done, attempted)
	}
	if string(result) != `{"added":1}` {
		t.Errorf("the record answered %s", result)
	}
}

// Release is what keeps a refusal recoverable.
func TestReleaseLeavesTheKeyAsNobodyHavingHeardOfIt(t *testing.T) {
	t.Parallel()

	client, _ := withServer(t)
	ledger := redisx.NewIdempotency(client, time.Hour)
	ctx := t.Context()

	if err := ledger.Reserve(ctx, "s1", "k1"); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if err := ledger.Release(ctx, "s1", "k1"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	_, done, attempted, err := ledger.Recall(ctx, "s1", "k1")
	if err != nil {
		t.Fatalf("Recall after Release: %v", err)
	}
	if done || attempted {
		t.Errorf("a released attempt read as done=%v attempted=%v, so the retry would be "+
			"refused for work that never happened", done, attempted)
	}
}

// Reserve does not restart the clock, and Recall does not push it out. Both halves,
// because either one on its own makes the record immortal: a redelivery is exactly what
// asks about an attempt, so a clock that either asking or re-reserving refreshes never
// runs out, and the command answers `timeout` for ever (#277 measured that shape on the
// transport's own entries).
func TestTheAttemptsClockRunsFromTheAttemptAndNothingPushesIt(t *testing.T) {
	t.Parallel()

	client, server := withServer(t)
	const ttl = time.Hour
	ledger := redisx.NewIdempotency(client, ttl)
	ctx := t.Context()
	attempt := client.Keys().Attempt("s1", "k1")

	if err := ledger.Reserve(ctx, "s1", "k1"); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if got := server.TTL(attempt); got != ttl {
		t.Fatalf("a fresh attempt expires in %v, want %v: an attempt with no expiry never "+
			"stops answering", got, ttl)
	}

	server.FastForward(30 * time.Minute)
	if _, _, attempted, err := ledger.Recall(ctx, "s1", "k1"); err != nil || !attempted {
		t.Fatalf("Recall half way through the attempt's life: attempted=%v err=%v", attempted, err)
	}
	if err := ledger.Reserve(ctx, "s1", "k1"); err != nil {
		t.Fatalf("a second Reserve, which is what a redelivery does: %v", err)
	}
	if got := server.TTL(attempt); got > ttl-30*time.Minute {
		t.Errorf("after half an hour and a redelivery the attempt expires in %v, which is "+
			"further out than the %v it had left: something is pushing its clock", got, ttl-30*time.Minute)
	}

	server.FastForward(31 * time.Minute)
	if _, _, attempted, err := ledger.Recall(ctx, "s1", "k1"); err != nil || attempted {
		t.Errorf("past its expiry the attempt still reads as one: attempted=%v err=%v", attempted, err)
	}
}

// A result settles the attempt, so what is left is one record and not two.
func TestRememberTakesTheAttemptItWasCarriedOutUnderOffTheBooks(t *testing.T) {
	t.Parallel()

	client, server := withServer(t)
	ledger := redisx.NewIdempotency(client, time.Hour)
	ctx := t.Context()

	if err := ledger.Reserve(ctx, "s1", "k1"); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if err := ledger.Remember(ctx, "s1", "k1", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatalf("Remember: %v", err)
	}
	if server.Exists(client.Keys().Attempt("s1", "k1")) {
		t.Error("the attempt is still on the books after the result that settles it")
	}
	if !server.Exists(client.Keys().Idempotency("s1", "k1")) {
		t.Error("the result is not on the books")
	}
}
