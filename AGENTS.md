# whatsapp-connector Development Guidelines

Go service that owns WhatsApp sessions and exchanges canonical events and commands
with its clients over Redis Streams. The wire contract in `contract/` is the source
of truth for both sides; this repository is the side that produces events.

## Build / Test / Lint

- **Setup a fresh clone**: `make setup` (installs the git hooks and downloads modules)
- **Lint**: `make lint` — `make fmt` rewrites what is auto-fixable
- **Test**: `make test` (race detector on) — a single one with `go test ./internal/protocol -run TestName`
- **Contract only**: `make contract`
- **Everything CI enforces**: `make check`
- **Toolchain**: Go as declared in `go.mod`; `golangci-lint` v2

## Layout

```
cmd/connector/       binary: serve, migrate, doctor, healthcheck, session subcommands
contract/            protocol vN: JSON Schema + golden fixtures (language neutral, no Go here)
internal/protocol/   Go binding for the contract: frames, type catalog, error codes
internal/transport/  how frames travel (Redis Streams today, HTTP standalone later)
internal/cluster/    leases, epochs, quarantine, rebalance — who owns a session
internal/session/    session lifecycle, state machine, per-session FIFO executor
internal/engine/     the WhatsApp side behind an interface (whatsmeow, plus a fake for tests)
internal/store/      Postgres/SQLite persistence, fenced against a lost lease
internal/media/      blob store, inbound download, outbound fetch
```

## The contract comes first

- `contract/` is language neutral. Never put Go (or any other language) files in it:
  clients vendor the directory verbatim and compare checksums, so an extra file
  shows up as drift on their side.
- Changing a frame means changing `contract/schema/protocol.schema.json`, adding or
  updating a fixture in `contract/fixtures/`, and updating `internal/protocol`. The
  contract test fails if any of the three lags behind.
- **Additive changes** (a new event type, a new optional field) do not bump
  `contract/PROTOCOL_VERSION`. Clients are written to ignore what they do not know.
- **Breaking changes** bump it, and the connector keeps serving the previous major
  for one release cycle: `MinVersion`..`Version` is advertised in the instance
  registry and a client refuses to talk when the ranges do not overlap.
- After a `contract/` change lands, the Chatwoot side re-pins its copy with
  `bundle exec rails whatsapp:contract:sync[<ref>]`. Say so in the PR description.
- Addresses on the wire are canonical (`{kind, id}`), never raw JIDs; timestamps are
  epoch milliseconds; media never travels inside a frame.

## Code Style

- Standard Go style, enforced by `golangci-lint` (formatting included). Do not hand
  format around it.
- **Errors**: wrap with `%w` and enough context to locate the session
  (`fmt.Errorf("send %s: %w", sid, err)`). Sentinel errors for what callers branch
  on. Everything that crosses the wire degrades to a `protocol.ErrorCode`; unknown
  codes become `internal` rather than leaking raw text into a client's UI.
- **Context**: every blocking call takes a `context.Context` and honours it. No
  naked `time.Sleep` in production paths; use a timer plus `ctx.Done()`.
- **Concurrency**: one goroutine owns one session. Cross-goroutine state goes through
  the session executor or the store, not through shared mutable structs.
- **Logging**: structured (`zerolog`), always with `sid` and, where relevant,
  `epoch`, `type` and `cmd_id`. Never log message bodies, media contents or auth
  state; phone numbers are redacted by the logger, do not bypass it.
- Prefer the smallest production-ready change. Do not add speculative guards,
  fallbacks or retries unless a caller can actually reach that state.
- When an impossible or misconfigured state means a deployment bug, fail loudly
  instead of silently degrading.
- Remove dead code instead of commenting it out; do not keep two versions of the
  same logic side by side.

## Testing

- New behaviour ships with tests: happy path plus the edge cases that motivated it.
  Bugfixes get a regression test when the fix is not cosmetic.
- Table-driven tests, `t.Run` subtests, and fixtures from `contract/` wherever a
  frame is involved.
- The WhatsApp side is behind `engine.Engine`: unit tests use the fake engine, never
  a real socket. Redis-backed tests use `miniredis`, except the handful of stream
  tests that need `XAUTOCLAIM` semantics a fake does not reproduce.
- Tests must be deterministic and safe under `-race`: no wall-clock sleeps to
  synchronise, no reliance on map iteration order, no shared global state.
- Anything touching ownership (leases, epochs, fencing) needs a test with two
  simulated instances. That is where the dangerous bugs live.

## Operational invariants

These are load-bearing. If a change makes one of them false, it is a design change,
not an implementation detail:

1. One instance owns a session at a time, arbitrated by a Redis lease. Losing the
   lease disconnects the socket immediately and fences the store writes.
2. Every event carries the owner's `epoch`. Clients drop session state from a stale
   epoch, so the epoch must increase on every ownership change.
3. `seq` is monotonic per `(sid, epoch)`, and a session always lands on the same
   event shard, which is what gives the client ordering.
4. The WhatsApp ack for an inbound message is sent only after the event is published.
   Losing Redis must cost a redelivery, never a message.
5. Commands are idempotent by `message_id` (sends) or `idempotency_key` (everything
   else): a redelivered command must not duplicate a side effect.

## Commit Messages

- Conventional Commits: `type(scope): subject`, enforced by the `commit-msg` hook.
- Scope is the package or area: `feat(protocol):`, `fix(cluster):`, `chore(ci):`.
- Don't reference Claude or any agent in commit messages.

## Pull Requests

- Start with a short paragraph on what changes for the operator or for the client.
- Fill the **Protocol impact** section of the template; it is what tells the Chatwoot
  side whether it has to move.
- Default merge: `gh pr merge <n> --squash`.
- CI (`lint`, `test`) must be green before merging.

## Related repositories

- `fazer-ai/chatwoot` — the client: `app/services/whatsapp/session/` consumes these
  events and produces these commands.
- `fazer-ai/baileys-api` — the previous generation (Node/Baileys), frozen. Its
  `src/cluster/` is the reference implementation for the lease protocol ported here.
