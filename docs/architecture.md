# Architecture

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

See [`contract/PROTOCOL.md`](../contract/PROTOCOL.md) for the frame shapes, the Redis key
map and the compatibility rules. In short:

- **Events** (connector → client) describe what happened: `message.received`,
  `message.receipt`, `session.state`, `pairing.qr`, ... The catalog is the contract's,
  so it is wider than this build: the types nothing here produces yet are marked in
  `internal/protocol/types.go`.
- **Commands** (client → connector) ask for something: `message.send`,
  `session.connect`, `group.participants.update`, ... RPC commands get a single
  answer on `wa:reply:<command id>`; the rest are fire and forget and report failures
  as a `command.failed` event.
- **Addresses** are canonical (`{kind: phone|lid|group|..., id}`), timestamps are
  epoch milliseconds, and media never travels inside a frame.
- `contract/PROTOCOL_VERSION` is a major version. Additive changes do not bump it; a
  connector serves the current major and the one before it.
