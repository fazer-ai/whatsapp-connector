package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// The two servers this bench cannot do without, and why each refusal is loud.
//
// A bench that starts without them does not measure less, it measures nothing while
// printing the shape of a measurement, and the shape is what somebody reads. Both checks
// happen before a single process is started, so a refusal costs nothing to recover from.
const (
	databaseVar = "WAC_TEST_DATABASE_URL"
	redisVar    = "WAC_TEST_REDIS_URL"
)

// preflight answers with the two servers this run will use, or with the reason it cannot
// run at all. Every error it returns is a setup error, which is the outcome this bench
// keeps apart from a broken invariant on purpose: one is a machine that is not ready and
// the other is a defect in the connector.
type servers struct {
	databaseURL string
	redisURL    string
}

func preflight(ctx context.Context) (servers, error) {
	database, err := requirePostgres(os.Getenv(databaseVar))
	if err != nil {
		return servers{}, err
	}
	redisURL, err := requireRedis(ctx, os.Getenv(redisVar))
	if err != nil {
		return servers{}, err
	}
	return servers{databaseURL: database, redisURL: redisURL}, nil
}

// requirePostgres refuses anything that is not a PostgreSQL server, and says why rather
// than just that.
//
// SQLite is the refusal that matters. The suite runs against it every day and it is the
// obvious thing to reach for, but two processes do not share an SQLite file, and the
// fencing that operational invariant 1 promises -- a lost lease stops the store writes --
// lives in the store. A bench that accepted SQLite would run two connectors against two
// different databases and call the result a fleet, and every invariant it then asserted
// would be asserted over a fleet that never existed.
func requirePostgres(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", fmt.Errorf(`%s is unset or empty. It names the PostgreSQL server the fleet shares:
  %[1]s=postgres://wac:wac@127.0.0.1:55432/wac?sslmode=disable make bench-fleet
Start one with:
  docker run --rm -d --name wac-bench-pg -p 55432:5432 \
    -e POSTGRES_USER=wac -e POSTGRES_PASSWORD=wac -e POSTGRES_DB=wac postgres:18-alpine
(any free port will do; 55432 only avoids whatever is already on 5432)`, databaseVar)
	}
	scheme, _, _ := strings.Cut(raw, ":")
	switch strings.ToLower(scheme) {
	case "postgres", "postgresql":
		if _, err := url.Parse(raw); err != nil {
			return "", fmt.Errorf("%s is not a URL this bench can read: %w", databaseVar, err)
		}
		return raw, nil
	}
	//nolint:revive,staticcheck // a multi-line message for whoever ran the bench, not a clause in a wrapped error
	return "", fmt.Errorf(`%s names %q, and this bench runs against PostgreSQL only.
Two processes do not share an SQLite file, and the fencing operational invariant 1
promises -- a lost lease stops the store writes -- lives in the store. Against SQLite
this bench would start two connectors on two separate databases and call them a fleet:
every invariant it then asserted would hold over a fleet that never existed, and it
would say so in green.`, databaseVar, raw)
}

// requireRedis refuses a Redis it cannot reach, and reaches for it rather than trusting
// the variable: a URL that parses and answers nothing is the same outage with a later
// symptom, and the later symptom is a phase timing out with no idea why.
func requireRedis(ctx context.Context, raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", fmt.Errorf(`%s is unset or empty. It names the Redis the fleet shares:
  %s=redis://127.0.0.1:56379/0 make bench-fleet
Start one with:
  docker run --rm -d --name wac-bench-redis -p 56379:6379 redis:8-alpine`, redisVar, redisVar)
	}
	options, err := redis.ParseURL(raw)
	if err != nil {
		return "", fmt.Errorf("%s is not a Redis URL this bench can read: %w", redisVar, err)
	}
	// Cancellation has to reach the socket here too: the ping below runs against a server
	// that may be reachable and not answering, and without this the five-second context
	// above would not be what ends the wait.
	options.ContextTimeoutEnabled = true
	client := redis.NewClient(options)
	defer func() { _ = client.Close() }()

	reach, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(reach).Err(); err != nil {
		//nolint:revive,staticcheck // same: a multi-line message for whoever ran the bench
		return "", fmt.Errorf(`%s names %q and nothing answered there: %w
The fleet's leases, streams and registry all live in this server, so there is no half
of this bench that runs without it.`, redisVar, raw, err)
	}
	return raw, nil
}

// errSetup marks every error that is about the machine rather than about the connector.
// It is what keeps the two apart at the exit code, which is the only part of this output
// a script reads.
var errSetup = errors.New("setup")

// errInvariantBroken marks a run that stopped because it had already found a defect,
// rather than because the machine was not ready.
//
// The two have to be told apart at the point the run gives up, not at the point it is
// printed. A fleet running more sessions than exist cannot go on to the phases below it --
// there is no starting state to measure a recovery against -- and reporting that as SETUP
// INCOMPLETO would file the clearest evidence of a broken lease under "the machine was not
// ready", which is the category a reader skips.
var errInvariantBroken = errors.New("invariante quebrada")
