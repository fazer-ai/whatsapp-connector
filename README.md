# whatsapp-connector

<a href="https://fazer.ai?utm_source=github&utm_medium=en&utm_campaign=whatsapp-connector"><img alt="fazer.ai logo" src="https://framerusercontent.com/images/HqY9djLTzyutSKnuLLqBr92KbM.png?scale-down-to=256" height="75"/></a>

Session-based WhatsApp connector for [fazer.ai Chatwoot](https://github.com/fazer-ai/chatwoot).
One process owns N WhatsApp sessions, speaks the WhatsApp multi-device protocol
through [whatsmeow](https://github.com/tulir/whatsmeow), and exchanges canonical
events and commands with its clients over Redis Streams.

> [!IMPORTANT]
> **Status: M2 is in, both ways.** A session pairs with a real WhatsApp account, resumes
> across restarts, publishes the text messages that arrive on it, and sends text back:
> quotes, mentions and the chat's disappearing-message timer included. `groups` on the
> connect decides whether group chats come with them. An inbound image, video, audio,
> document or sticker is downloaded as it arrives, kept in this instance's blob cache and
> published as a reference the client fetches over HTTP; a file WhatsApp will not serve
> again is announced with `media.download_failed` so the bubble says the attachment is
> unavailable rather than loading forever. A blob lives on the instance that downloaded it
> and for a bounded time, and `message.download_media` fetches the file again from the
> coordinates kept beside the message, so an attachment survives the instance being
> replaced between the event and the client's fetch.
>
> Outbound, a media message names a URL this connector fetches, with whatever headers open
> it, and streams to WhatsApp without holding the file in memory; a location goes out as a
> pin with the name and the street beside it, and contacts as a card or a stack of them,
> with the vCard written here when the caller only has a name and a number. Anything the
> caller's own address answers is separated into what it has to fix and what is worth
> another go, because a client told the wrong one either retries forever or gives up on a
> file that would have arrived.
>
> A file sent to be seen once is announced as unavailable and never kept: a blob is served
> for as long as anybody keeps asking for it, so storing one would turn something the
> sender expected to disappear into something the account holds indefinitely. WhatsApp
> usually does not hand one to a linked device at all, and what it sends instead reaches
> the inbox as nothing until
> [#20](https://github.com/fazer-ai/whatsapp-connector/issues/20) lands. What this build
> cannot render is left unacknowledged on WhatsApp's side, with its plaintext buffered so
> the redelivery can still be read: the
> account keeps the message and delivers it again once there is somewhere to put it. A
> number paired on this build therefore still accumulates a backlog of everything it cannot
> render (see [Roadmap](#roadmap)).
>
> Around the messages, what a conversation looks like while nobody is writing one: the
> ticks a message collects on their way to being read, a read mark this account can set
> on somebody else's, the typing and recording indicators both ways, and whether a
> contact is at their phone. A `composing` or a `recording` is the one shape here that
> describes a moment rather than a fact, and it is the only thing this connector drops
> when it goes stale instead of retrying it: delivered a minute late it is somebody shown
> typing who stopped long ago, and the state that would have corrected it went out while
> the stale one was still on its way. The stop that ends a burst is not like that, and
> neither is an availability -- both hold until something says otherwise. None of it
> survives a reconnect, though: WhatsApp forgets the availability and the subscriptions
> alike, so a client resubscribes and republishes what it wants shown
> ([#46](https://github.com/fazer-ai/whatsapp-connector/issues/46)).
>
> A message is acknowledged to WhatsApp only after its event reaches the stream, so
> losing Redis costs a redelivery and never a message. The client deduplicates on the
> message id, which is what makes that trade safe. A send is answered from the other
> end of the same trade: what a command did is remembered under
> `wa:idem:<sid>:<key>`, so a redelivery is answered with the first run's result
> instead of being carried out again.

## Why a separate service

A WhatsApp session is a long-lived, stateful socket with its own reconnect and
key-rotation lifecycle. That does not fit a request/response Rails process or a
Sidekiq job, and it does not want the release cadence of an application either: the
WhatsApp protocol moves roughly twice a month, so this ships as its own image with
its own hotfix cadence.

Keeping the WhatsApp side behind an explicit contract also means the client never
sees a JID, a protobuf, or a library-specific shape. The same canonical events reach
Chatwoot whether they came from this connector or from a hosted WhatsApp API
translated on the client side.

## How it fits together

```
        ┌──────────────────────────────┐          ┌────────────────────────────┐
        │ client (fazer-ai/chatwoot)   │          │ whatsapp-connector         │
        │                              │          │                            │
        │  consumer  ◀── wa:events:<n> ─┼──────────┼── publisher                │
        │  client    ─── wa:cmd:<sid> ─▶┼──────────┼─▶ session executor         │
        │            ◀── wa:reply:<id> ─┼──────────┼── (RPC answers)            │
        └──────────────────────────────┘          │      │                     │
                        Redis                     │      ▼                     │
                                                  │  engine (whatsmeow)        │
                                                  │  store (Postgres/SQLite)   │
                                                  └────────────────────────────┘
```

A session is owned by exactly one instance at a time, arbitrated by a Redis lease.
Every event carries the owner's `epoch`, and `seq` is monotonic per `(sid, epoch)`,
so a client can drop anything it has already seen or that comes from a stale owner.

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


## Protocol

See [`contract/README.md`](contract/README.md) for the frame shapes, the Redis key
map and the compatibility rules. In short:

- **Events** (connector → client) describe what happened: `message.received`,
  `session.state`, `pairing.qr`, `group.updated`, ...
- **Commands** (client → connector) ask for something: `message.send`,
  `session.connect`, `group.participants.update`, ... RPC commands get a single
  answer on `wa:reply:<command id>`; the rest are fire and forget and report failures
  as a `command.failed` event.
- **Addresses** are canonical (`{kind: phone|lid|group|..., id}`), timestamps are
  epoch milliseconds, and media never travels inside a frame.
- `contract/PROTOCOL_VERSION` is a major version. Additive changes do not bump it; a
  connector serves the current major and the one before it.

## Development

Requirements: Go (version in `go.mod`) and
[golangci-lint](https://golangci-lint.run/) v2. The tests bring their own Redis
(`miniredis`), so nothing has to be running to `make check`. Postgres joins with the
store in M1.

```bash
make setup   # git hooks + module download
make check   # what CI enforces: lint + tests
make help    # every target
```

`make setup` points git at the versioned hooks in `.githooks/`: `pre-commit` refuses
unformatted Go and runs the contract test, `commit-msg` enforces Conventional
Commits.

### Running one

```bash
docker run --rm -p 6379:6379 redis:7-alpine   # in another terminal
REDIS_URL=redis://127.0.0.1:6379 WAC_ENGINE=fake go run ./cmd/connector serve
```

`WAC_ENGINE=fake` pairs instantly with nothing behind it, publishes the same frames a
real session would, and answers the commands the contract's result table names. It is
what the tests run against, and what an operator can point a client at to see the whole
path work without a phone.

`WAC_ENGINE=whatsmeow` is the real thing, and it needs somewhere to keep a pairing:

```bash
REDIS_URL=redis://127.0.0.1:6379 \
  WAC_ENGINE=whatsmeow WAC_DATABASE_URL=sqlite:wa.db \
  go run ./cmd/connector serve
```

Postgres (`postgres://…`) is what a fleet runs, since a session that moves between
instances has to find its device wherever it lands. SQLite is the single-instance case.
Starting the whatsmeow engine without a database is refused rather than defaulted: a
connector with nowhere to keep a pairing asks every session to scan a QR code on every
restart, and reports itself healthy while doing it.

### Configuration

| Variable | Default | What it is |
|---|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379` | The Redis shared with the client. Deliberately not `WAC_`-prefixed: both sides read the same variable, so they cannot be pointed at different servers |
| `REDIS_PASSWORD` | — | Overrides the password in the URL, for deployments that pass the two separately |
| `WAC_INSTANCE` | the hostname | This instance's id. In a container the hostname is the container id, which is unique per replica |
| `WAC_REDIS_PREFIX` | `wa:` | Namespaces every key, so one Redis can host two independent fleets |
| `WAC_EVENT_SHARDS` | `8` | How many event streams the fleet publishes to. Fleet-wide and effectively permanent: an instance that disagrees with what is recorded refuses to start |
| `WAC_ENGINE` | `fake` | `whatsmeow` for a real account, `fake` for a fleet with nothing behind it |
| `WAC_DATABASE_URL` | none | Where pairings live. `postgres://…`, `sqlite:…` or `file:…`. Required by the `whatsmeow` engine |
| `WAC_DEVICE_NAME` | `fazer.ai` | What the account's linked-devices list shows. Fleet-wide, not per session: whatsmeow keeps device properties process-wide |
| `WAC_HTTP_ADDR` | `:8080` | Where `/healthz`, `/readyz` and `/metrics` listen |
| `WAC_ADVERTISE_URL` | derived | How clients reach this instance for media |
| `WAC_MEDIA_ROOT` | unset | Where inbound media is cached. Unset turns the store and the endpoint off, and every media message is then published with `media.download_failed` behind it |
| `WAC_MEDIA_TOKEN` | unset | Bearer token the media endpoint requires. Required whenever `WAC_MEDIA_ROOT` is set: the endpoint hands out message contents |
| `WAC_MEDIA_TTL` | `24h` | How long a blob is kept without being collected. The cache is walked every half of it, between a second and a minute, so a short TTL is swept often |
| `WAC_MEDIA_QUOTA` | `2GiB` | Disk the blobs may take, counted in whole blocks and including each blob's description. Over it, the least recently collected go first |
| `WAC_MEDIA_MAX_BLOB` | `100MiB` | The largest single file this instance keeps |
| `WAC_MEDIA_BLOCK_SIZE` | `4KiB` | The allocation unit of the volume the cache sits on. Set it to match a filesystem formatted with larger units, or every file is undercharged against the quota |
| `WAC_MEDIA_SEND_MAX` | `100MiB` | The largest file this instance will send. Independent of the cache: an instance given no `WAC_MEDIA_ROOT` at all still sends, and the caller's own declared size is refused against this before anything is fetched. WhatsApp's own ceiling is per type and moves on its own schedule, so a file past that is refused by WhatsApp with an answer the caller is told. In practice the binding limit is the command's own deadline rather than this: the supported client allows 18 seconds for the fetch and the upload together, which no file near this cap fits into ([#29](https://github.com/fazer-ai/whatsapp-connector/issues/29)) |
| `WAC_MEDIA_REFETCH_TTL` | `168h`, or `WAC_MEDIA_TTL` when that is longer | How long a message can still be asked for its file again, after the blob it was published with has gone. Must be at least `WAC_MEDIA_TTL`, which is why the default follows it up. What is kept for that long is a row per media message holding the key to the file, so it is retention rather than cache |
| `WAC_LEASE_TTL` | `30s` | How long a session lease survives without a renewal |
| `WAC_HEARTBEAT` | `5s` | How often leases are renewed and the instance re-announces. Also bounds how long a read waits on Redis (half a heartbeat), and has to leave room for the read and the batch before it: `1.5 × heartbeat + lease/3 < lease` |
| `WAC_CLAIM_MIN_IDLE` | `1.5 × lease` | How long a command sits unacknowledged before another instance takes it over. Must exceed `WAC_LEASE_TTL` |
| `WAC_LOG_LEVEL` | `info` | zerolog level |

### Changing the protocol

1. Edit `contract/schema/protocol.schema.json`.
2. Add or update the golden frame under `contract/fixtures/`.
3. Update `internal/protocol` accordingly.
4. `make contract` — it fails if any of the three lags behind, and if a type has no
   fixture.
5. Bump `contract/PROTOCOL_VERSION` **only** for a breaking change, and say so in the
   PR's *Protocol impact* section: clients re-pin their vendored copy from that ref.

## Roadmap

| Milestone | Scope |
|---|---|
| **M0** ✅ | Skeleton, Redis Streams transport, lease/ownership port, fake engine, health and metrics, Docker image, publish pipeline |
| **M1** ✅ | whatsmeow engine: QR and code pairing, session state, logout/ban/outdated handling, the device store. Reconnect backoff and the store-level fence are still open |
| **M2** ✅ | Messages in and out (text, media, location, contact, reaction, edit, revoke, quoted, mentions), receipts, read marks, chat presence, account presence, idempotent sends. All of them are in both ways, and a body this build has no arm for arrives as a placeholder rather than disappearing. What it leaves behind is in the issues rather than here: a message WhatsApp will not hand to a linked device still reaches nobody ([#20](https://github.com/fazer-ai/whatsapp-connector/issues/20)), and presence does not survive a reconnect ([#46](https://github.com/fazer-ai/whatsapp-connector/issues/46)) |
| **M3** | Groups, contacts, calls |
| **M4** | Multi-instance under load, quarantine, metrics/lag/DLQ, operations docs |
| **M5** | Pairing code, passkey relay, per-session proxy, account limits |
| **M6** | Opt-in history sync |

## License

MIT. See [`LICENSE`](LICENSE).
