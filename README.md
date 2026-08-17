# whatsapp-connector

Session-based WhatsApp connector for [fazer.ai Chatwoot](https://github.com/fazer-ai/chatwoot).
One process owns N WhatsApp sessions, speaks the WhatsApp multi-device protocol
through [whatsmeow](https://github.com/tulir/whatsmeow), and exchanges canonical
events and commands with Chatwoot over Redis Streams.

Status: **contract only**. The Go implementation lands in milestones M0..M6 (see the
implementation plan); `contract/` is published first because both sides are written
against it.

## Why a separate service

WhatsApp sessions need a long-lived, stateful connection with its own reconnect and
key-rotation lifecycle, which does not fit a request/response Rails process or a
Sidekiq job. Keeping it in its own image also decouples its release cadence (the
WhatsApp protocol moves ~twice a month) from the Chatwoot release cadence.

## Layout

```
contract/    protocol v1: JSON Schema + golden fixtures (source of truth)
```

## License

MIT. See `LICENSE`.
