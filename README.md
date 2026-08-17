# whatsapp-connector

<a href="https://fazer.ai?utm_source=github&utm_medium=en&utm_campaign=whatsapp-connector"><img alt="fazer.ai logo" src="https://framerusercontent.com/images/HqY9djLTzyutSKnuLLqBr92KbM.png?scale-down-to=256" height="75"/></a>

Session-based WhatsApp connector for [fazer.ai Chatwoot](https://github.com/fazer-ai/chatwoot).
One process owns N WhatsApp sessions, speaks the WhatsApp multi-device protocol
through [whatsmeow](https://github.com/tulir/whatsmeow), and exchanges canonical
events and commands with its clients over Redis Streams.

> [!IMPORTANT]
> **Status: contract and Go binding only.** The service itself lands in milestones
> M0..M6 (see [Roadmap](#roadmap)). `contract/` is published first because both
> sides of the wire are written against it.

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
contract/            protocol v1: JSON Schema + golden fixtures (source of truth)
internal/protocol/   Go binding for the contract: frames, type catalog, error codes
```

The rest of the tree (`cmd/`, `internal/{transport,cluster,session,engine,store,media}`)
arrives with M0 onward.

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
[golangci-lint](https://golangci-lint.run/) v2. Redis and Postgres join once the
service itself exists.

```bash
make setup   # git hooks + module download
make check   # what CI enforces: lint + tests
make help    # every target
```

`make setup` points git at the versioned hooks in `.githooks/`: `pre-commit` refuses
unformatted Go and runs the contract test, `commit-msg` enforces Conventional
Commits.

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
| **M0** | Skeleton, Redis Streams transport, lease/ownership port, fake engine, health and metrics, Docker image, publish pipeline |
| **M1** | whatsmeow engine: QR pairing, session state, logout/ban/outdated handling, own reconnect loop, fenced Postgres store |
| **M2** | Messages in and out (text, media, location, contact, reaction, edit, revoke, quoted, mentions), receipts, read marks, chat presence, idempotent sends |
| **M3** | Groups, presence, contacts, calls |
| **M4** | Multi-instance under load, quarantine, metrics/lag/DLQ, operations docs |
| **M5** | Pairing code, passkey relay, per-session proxy, account limits |
| **M6** | Opt-in history sync |

## License

MIT. See [`LICENSE`](LICENSE).
