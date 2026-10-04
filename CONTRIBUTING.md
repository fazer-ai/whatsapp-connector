# Contributing

Requirements: Go (version in `go.mod`) and
[golangci-lint](https://golangci-lint.run/) v2. Most tests bring their own doubles
(`miniredis`, SQLite), and two passes do not: the suite against a real PostgreSQL, which
is the dialect a deployment runs, and the packages that talk to Redis against a real one,
for what miniredis answers differently from a server. `make check` runs all of it and
needs both servers; `make check-offline` is the half that needs nothing listening.

CI runs the Redis pass twice: against `redis:8`, and against `redis:6.2`, the oldest
version this connector supports, because a command form newer than the floor passes
against the newest server and miniredis alike. `make check` asks for one Redis and does
not run the floor pass. To run it locally, point `WAC_TEST_REDIS_URL` at a 6.2:

```bash
docker run -d --rm -p 56362:6379 redis:6.2-alpine
WAC_TEST_REDIS_URL=redis://localhost:56362/0 make test-redis
```

```bash
make setup          # git hooks + module download
make check-offline  # lint, go mod tidy, and the suite against SQLite

# Everything CI enforces, which needs a server for each of the two dialect passes.
# Any free port will do; these avoid whatever is already on 5432 and 6379. The PostgreSQL
# is the one CI starts, without durability: every test creates and drops a database, and a
# durable server spends the pass on that (#342).
make test-postgres-server
docker run -d --rm -p 56379:6379 redis:8-alpine
WAC_TEST_DATABASE_URL=postgres://wac:wac@localhost:55432/wac?sslmode=disable \
WAC_TEST_REDIS_URL=redis://localhost:56379/0 make check
make help    # every target
```

`make setup` points git at the versioned hooks in `.githooks/`: `pre-commit` refuses
unformatted Go and runs the contract test, `commit-msg` enforces Conventional
Commits.

## The fleet bench

Leases, epochs, shards, `seq` and fencing are the half of this connector that only two
processes under load can disprove, and the suite runs in one. `make bench-fleet` starts a
real fleet against a real PostgreSQL and a real Redis, kills the owner with commands in
flight, and asserts the operational invariants over what reached the streams:

```bash
WAC_TEST_DATABASE_URL=postgres://wac:wac@localhost:55432/wac?sslmode=disable \
WAC_TEST_REDIS_URL=redis://localhost:56379/0 make bench-fleet
```

It runs with `WAC_ENGINE=fake`, and the run says so in its own output, so that no number
it prints is ever read as a number about the real engine. The fake is not a shortcut
around the thing being measured: none of the machinery these assertions are about knows
which engine is behind the session. What the choice costs is that **whatsmeow under an
ownership change is not covered by this bench and cannot be** -- pairing a real account
needs a physical device (`NEEDS_PHYSICAL_DEVICE`), and no run of it ever touches a real
WhatsApp account. So a green run says the fleet's own machinery holds across processes;
it does not say whether a real socket, a real pairing and a real message survive an
ownership change. That half has no measurement here.

It answers in four exit codes, because a script reads the code and not the prose:

| code | outcome | what it means |
|---|---|---|
| 0 | `VERDE` | every assertion held |
| 1 | `INVARIANTE QUEBRADA` | an operational invariant is broken: a defect |
| 2 | `SETUP INCOMPLETO` | the machine was not ready: not a defect, and not a pass |
| 3 | `MEDIDA FORA DA FAIXA` | a measurement fell outside a range somebody declared |

One code for all of them is what turns a bad capacity number into a rejection and a broken
invariant into "that number again", so they are kept apart on purpose. **`make` cannot keep
them apart**: GNU make exits 2 for any failing recipe, whatever the recipe's own code was.
A script that needs the three failures told apart runs the binary the target builds:

```bash
go build -o bin/fleetbench ./cmd/fleetbench
WAC_TEST_DATABASE_URL=… WAC_TEST_REDIS_URL=… bin/fleetbench; echo $?
```

`make bench-fleet` prints the code it got before it exits, and make's own
`*** [bench-fleet] Error 3` names it too, so a human reading the output sees which of the
three it was. What neither of them changes is the status make leaves behind, which is 2.

It is deliberately outside `make check`: it builds a binary, starts processes and waits on
real clocks, which is minutes rather than the seconds `check` is allowed on every change.
The exemption is recorded in `internal/toolchain`, where the suite reads it.

## Changing the protocol

1. Edit `contract/schema/protocol.schema.json`.
2. Add or update the golden frame under `contract/fixtures/`.
3. Update `internal/protocol` accordingly.
4. `make contract` — it fails if any of the three lags behind, and if a type has no
   fixture.
5. Bump `contract/PROTOCOL_VERSION` **only** for a breaking change, and say so in the
   PR's *Protocol impact* section: clients re-pin their vendored copy from that ref.

## Layout

```
contract/                 protocol v1: JSON Schema + golden fixtures (source of truth)
cmd/connector/            the binary: serve, healthcheck, version
internal/protocol/        Go binding for the contract: frames, type catalog, error codes
internal/redisx/          the Redis key layout, and the session to shard mapping
internal/transport/       publish, read commands, reply — Redis Streams behind an interface
internal/cluster/         leases, epochs and the instance registry: who owns a session
internal/session/         one account: the event pump and the per-session command queue
internal/engine/          the WhatsApp side behind an interface, plus a fake for tests
internal/observability/   the redacting logger and the metric set
internal/store/           the device store and which session paired which device
internal/media/           the blob cache for inbound media, and the endpoint that serves it
internal/httpserver/      /healthz, /readyz, /metrics
internal/app/             configuration and the run loop that ties them together
```
