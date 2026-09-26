package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	_ "github.com/lib/pq"
)

// The two names the run's PostgreSQL connections announce themselves as, so the pool
// reading can tell the fleet's connections from the reader taking the measurement.
//
// Counted POSITIVELY, by the fleet's name, and not as "everything that is not the bench".
// Exclusion made the number depend on a string nobody in the run controls: the fleet's URL
// used to carry whatever `application_name` the caller put in `WAC_TEST_DATABASE_URL`, or
// inherited through `PGAPPNAME`, and one that happened to equal the bench's own left every
// connector connection excluded, the pool reading at zero and the ceiling check passing
// over a fleet it had not counted. Both names are set by this run on the URL it builds,
// which is what makes them names and not guesses.
const (
	benchApplicationName = "fleetbench"
	fleetApplicationName = "fleetbench-connector"
)

// The three timings every process of a run is started with. They are named in the run's
// own output, because each of the measurements is a number about them as much as about
// the fleet.
const (
	benchHeartbeat    = time.Second
	benchLeaseTTL     = 30 * time.Second
	benchClaimMinIdle = 45 * time.Second
)

// benchMaxConns is the PostgreSQL pool ceiling every process of a run is started with.
const benchMaxConns = 20

// Everything one run of this bench owns, and nothing it does not.
//
// The isolation is by identifier and never by wildcard: a database named after this run,
// a Redis key prefix named after this run, and processes killed by the pids this run
// captured. The machine this runs on has other people's fleets on the same Redis and the
// same PostgreSQL, and `FLUSHDB` would take them; `SCRIPT FLUSH` would take them from
// every database at once, because the script cache is per server and not per database.
// So this file has no flush of any kind, which is a thing to check rather than to trust:
// `TestNoFlushReachesTheSharedRedis` walks the package's calls and fails on any of them.
// It reads calls through the AST rather than grepping for the word, because a grep matches
// this very sentence: a check that reports its own prose as the thing it was looking for
// can never come back empty, and one that can never fail is not a check.
type run struct {
	id       string
	prefix   string
	database string // the database this run created, by name
	adminURL string // the server, as given
	redisURL string // the Redis, as given
	fleetURL string // the database this run created, as a URL, for the connector
	benchURL string // the same database, tagged `application_name=fleetbench`
	rdb      *redis.Client
}

// runURLs derives the two connection strings of a run from the one the caller supplied.
//
// Out of `newRun` so a test reaches it: everything it decides is decided ABOUT a string
// somebody else wrote, and each of the three edits below exists because trusting that
// string produced a wrong number or a write in the wrong place.
func runURLs(supplied, database string) (fleetURL, benchURL string, err error) {
	parsed, err := url.Parse(supplied)
	if err != nil {
		return "", "", fmt.Errorf("read %s back: %w", databaseVar, err)
	}
	// The path alone does not decide the database. `lib/pq` accepts `dbname` and
	// `database` as query parameters and lets them override what the path says, so a
	// `WAC_TEST_DATABASE_URL` carrying either would have every connector of this run
	// migrate and write into the shared database while the cleanup dropped the empty one
	// this run created. Stripped rather than trusted, because the variable comes from
	// whoever ran the bench.
	base := parsed.Query()
	base.Del("dbname")
	base.Del("database")
	// Set and not preserved, for the same reason and with a sharper edge: the pool reading
	// counts by this name, so a caller's `application_name` reaching the fleet would make
	// the count answer about somebody else's string -- and one that happened to equal the
	// bench's own left the reading at zero over a fleet it had not counted.
	base.Set("application_name", fleetApplicationName)
	parsed.RawQuery = base.Encode()
	parsed.Path = "/" + database

	// The bench's own connections carry a name of their own, so the pool measurement can
	// tell them apart from the fleet's. Without it, the run's own reader counts as part of
	// what the connector holds, and the error grows with however many readings a phase
	// happens to take.
	tagged := *parsed
	query := tagged.Query()
	query.Set("application_name", benchApplicationName)
	tagged.RawQuery = query.Encode()
	return parsed.String(), tagged.String(), nil
}

func newRun(ctx context.Context, s servers) (*run, error) {
	// A name for this run's database, prefix and instances. Nothing about it is a
	// secret: it exists so two runs on one machine do not collide.
	id := fmt.Sprintf("%d%04d", time.Now().Unix()%100000, rand.IntN(10000)) //nolint:gosec // a run name, not a secret
	r := &run{
		id:       id,
		prefix:   "wacbench" + id + ":",
		database: "wacbench" + id,
		adminURL: s.databaseURL,
		redisURL: s.redisURL,
	}

	admin, err := sql.Open("postgres", s.databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open the PostgreSQL server: %w", err)
	}
	defer func() { _ = admin.Close() }()
	// The name is built here, from a timestamp and a random number, so it cannot carry
	// anything a caller supplied: `CREATE DATABASE` takes no placeholders.
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE "`+r.database+`"`); err != nil {
		return nil, fmt.Errorf("create the database for this run (%s): %w", r.database, err)
	}

	fleetURL, benchURL, err := runURLs(s.databaseURL, r.database)
	if err != nil {
		return nil, err
	}
	r.fleetURL, r.benchURL = fleetURL, benchURL

	options, err := redis.ParseURL(s.redisURL)
	if err != nil {
		return nil, fmt.Errorf("read %s back: %w", redisVar, err)
	}
	// Cancelling the context has to reach the socket, and go-redis only does that with
	// this on.
	//
	// Off (its default), a blocking read such as the BLPOP this bench waits a reply on
	// ignores the cancellation and runs to its own timeout: a Ctrl-C during that wait kills
	// the connectors at once and then leaves the run sitting there for up to a minute
	// before the cleanup that drops its database and keys even starts.
	options.ContextTimeoutEnabled = true
	r.rdb = redis.NewClient(options)
	return r, nil
}

// cleanup gives back exactly what this run took: its own keys, one SCAN at a time, and
// its own database, by the name it created.
//
// Reported rather than silent. A run that leaves keys behind is a run whose next sibling
// starts against a fleet it did not build, and the symptom of that is an invariant
// assertion over somebody else's sids.
func (r *run) cleanup(ctx context.Context) []string {
	var trouble []string
	var cursor uint64
	deleted := 0
	for {
		keys, next, err := r.rdb.Scan(ctx, cursor, r.prefix+"*", 500).Result()
		if err != nil {
			trouble = append(trouble, fmt.Sprintf("scan for %s*: %v", r.prefix, err))
			break
		}
		if len(keys) > 0 {
			if err := r.rdb.Del(ctx, keys...).Err(); err != nil {
				trouble = append(trouble, fmt.Sprintf("delete %d keys of this run: %v", len(keys), err))
			}
			deleted += len(keys)
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	_ = r.rdb.Close()

	admin, err := sql.Open("postgres", r.adminURL)
	if err != nil {
		trouble = append(trouble, fmt.Sprintf("reopen the server to drop %s: %v", r.database, err))
		return trouble
	}
	defer func() { _ = admin.Close() }()
	drop, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(drop, `DROP DATABASE IF EXISTS "`+r.database+`" WITH (FORCE)`); err != nil {
		trouble = append(trouble, fmt.Sprintf("drop %s: %v", r.database, err))
	}
	return trouble
}

// connectorEnv is what every process of this run is started with. The engine is the fake
// one, by the issue's decision 2, and it is in the output of every run so that no number
// here is ever read as a number about whatsmeow.
func (r *run) connectorEnv(shards int) map[string]string {
	return map[string]string{
		"WAC_ENGINE":       "fake",
		"WAC_DATABASE_URL": r.fleetURL,
		"REDIS_URL":        r.redisURL,
		"WAC_REDIS_PREFIX": r.prefix,
		"WAC_EVENT_SHARDS": fmt.Sprint(shards),
		"WAC_LOG_LEVEL":    "info",
		// A second, against the five a deployment defaults to.
		//
		// Not to make the fleet look fast: it is what stops the first measurement being
		// about the heartbeat's phase instead of about the connector. A session asked for
		// between two ticks waits for the next one, so "time until the sessions connect"
		// came out anywhere in a five second window -- 0.01 s and 2.6 s on two runs of the
		// same tree, which is a number nobody can paste into an issue. The tick is named
		// in the run's own notes, because a measurement whose conditions are not printed
		// is a measurement that cannot be repeated.
		"WAC_HEARTBEAT": benchHeartbeat.String(),
		// Pinned rather than left to the defaults, because the adoption number is
		// unreadable without them: bringing the sessions back after a hard kill costs a
		// lease TTL, so "32 s" is the TTL plus a tick and not a property of the fleet.
		// A number whose conditions are not printed cannot be pasted into an issue, and
		// the run prints these three next to it.
		"WAC_LEASE_TTL":      benchLeaseTTL.String(),
		"WAC_CLAIM_MIN_IDLE": benchClaimMinIdle.String(),
		// Pinned for the same reason, and because the issue asks for the pool reading to
		// be about a cap: "4 connections" says nothing until it is 4 out of something.
		"WAC_DATABASE_MAX_CONNS": strconv.Itoa(benchMaxConns),
		// A session that publishes nothing while it runs is a session no assertion about
		// the order of two publishers can be made over. MEASURED: without this, a run
		// carrying 2304 commands produced 12 events, every one of them from an adoption,
		// so the check that sees an old owner publishing after a new one had no event to
		// see it in. The real engine publishes a receipt for every message it sends.
		"WAC_FAKE_RECEIPT_PER_SEND": "1",
	}
}

// snapshotKeys is every key on this Redis before the run starts.
//
// Taken because the check below has to tell "somebody else's key, which was already
// there" from "a key this run wrote outside its own prefix", and after the fact those
// look identical. This Redis is shared with whatever else is running on the machine.
func (r *run) snapshotKeys(ctx context.Context) (map[string]bool, error) {
	before := map[string]bool{}
	var cursor uint64
	for {
		keys, next, err := r.rdb.Scan(ctx, cursor, "*", 500).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			before[key] = true
		}
		if next == 0 {
			return before, nil
		}
		cursor = next
	}
}

// unprefixed reports every key on this Redis that does not belong to this run and did not
// exist before it, which is what makes "the bench does not touch anyone else's fleet"
// checkable instead of promised.
func (r *run) unprefixed(ctx context.Context, before map[string]bool) ([]string, error) {
	var strayed []string
	var cursor uint64
	for {
		keys, next, err := r.rdb.Scan(ctx, cursor, "*", 500).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if strings.HasPrefix(key, r.prefix) || before[key] {
				continue
			}
			strayed = append(strayed, key)
		}
		if next == 0 {
			return strayed, nil
		}
		cursor = next
	}
}
