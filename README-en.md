<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset=".github/brand/logo-dark.png">
  <source media="(prefers-color-scheme: light)" srcset=".github/brand/logo-light.png">
  <img src=".github/brand/logo-light.png" alt="fazer.ai" width="200">
</picture>

<h1>fazer.ai WhatsApp Connector</h1>

<p>Connect WhatsApp to Chatwoot fazer.ai by QR code.</p>
<p>Sessions on your server, without a paid third-party API.</p>

[Português (Brasil)](README.md) · **English**

[![Release](https://img.shields.io/github/v/release/fazer-ai/whatsapp-connector)](https://github.com/fazer-ai/whatsapp-connector/releases)
[![Docker image](https://img.shields.io/badge/ghcr.io-whatsapp--connector-2496ED?logo=docker&logoColor=fff)](https://github.com/fazer-ai/whatsapp-connector/pkgs/container/whatsapp-connector)
![License: MIT](https://img.shields.io/badge/license-MIT-3B82F6)
![Go](https://img.shields.io/badge/Go-00ADD8?logo=go&logoColor=fff)

</div>

> [!IMPORTANT]
> This channel is in beta and appears in Chatwoot as “WhatsApp (native)” with a beta badge. The installation administrator enables it for each account, following the installation steps below. The connection is unofficial and pairs as a linked device, just like WhatsApp Web. For Meta's official API, use Chatwoot's WhatsApp Cloud inbox. Report problems in [this repository's issues](https://github.com/fazer-ai/whatsapp-connector/issues).

## About

fazer.ai WhatsApp Connector is a Go service that keeps WhatsApp sessions for multiple accounts running in a single process. Each session acts as a linked device on the account, using the multi-device protocol through the [whatsmeow](https://github.com/tulir/whatsmeow) library.

The connector exchanges events and commands with [Chatwoot fazer.ai](https://github.com/fazer-ai/chatwoot) through the Redis server the installation already uses. This communication follows the versioned contract in [`contract/`](contract/). Chatwoot receives canonical events without depending on the library's internals.

A WhatsApp session keeps a socket open for long periods and handles its own reconnection. Because the WhatsApp protocol changes frequently, the connector has its own image and release cycle. It can be updated without waiting for a Chatwoot release.

## What it does

### Connection

- Pair by QR code or by entering an 8-character code on your phone.
- When WhatsApp requests passkey confirmation, the challenge is forwarded to Chatwoot and the confirmation code is shown to the operator.
- Sessions resume automatically after a connector restart, without pairing again.
- Per-session proxies using http, https, or socks5. If the proxy goes down, the session never connects directly.
- Optional import of your phone's message history when connecting, plus requests for older messages in a conversation.

### Messages

- Send and receive text, images, video, audio, documents, stickers, locations, and contacts.
- Quoted replies, mentions, reactions, editing, and deletion.
- Respects the conversation's disappearing message timer.
- Incoming media is downloaded immediately and delivered to Chatwoot over HTTP. If the file is no longer on WhatsApp's server, the connector asks the sender's phone to upload it again.
- Outgoing media is streamed from the URL provided by Chatwoot, without loading the entire file into memory.
- View-once media appears as unavailable and is never stored.

### Conversations

- Delivery and read receipts, plus the option to mark messages as read.
- Typing and audio recording indicators in both directions, along with contact presence.

### Groups

- Create and rename groups, change their description, photo, and settings, or leave them.
- Add, remove, promote, and demote participants.
- Invite links and join requests.

### Contacts and calls

- Check whether a number is on WhatsApp and retrieve a contact's profile and photo.
- Incoming calls appear in the inbox, with optional automatic rejection.

## Built to keep messages from getting lost

- The connector acknowledges a message to WhatsApp only after publishing it to Redis. If Redis goes down, WhatsApp redelivers the message.
- When Redis returns, the connector closes its own WhatsApp connection so unacknowledged messages are redelivered immediately. It does not have to wait for the next connection drop.
- Commands are idempotent: a send command repeated by the queue does not send the message twice.
- Multiple instances can run together. Each session has one owner at a time, determined by a Redis lease. If an instance goes down, another takes over the session.
- If an instance loses access to Redis for longer than its lease, it closes the session itself. Another can take over without both handling the session at once.
- Prometheus metrics at `/metrics` and healthcheck endpoints at `/healthz` and `/readyz`, on port 8080.

## Install with Chatwoot fazer.ai

### Requirements

- A recent version of Chatwoot fazer.ai. The “WhatsApp (native)” provider is included in both editions.
- The same Redis server as Chatwoot, version 6.2 or newer.
- A database for pairings: a dedicated PostgreSQL database for the connector when running multiple instances, or SQLite for a single instance.

### Image

Use the public `ghcr.io/fazer-ai/whatsapp-connector:latest` image. Release tags such as `0.5.0` and `0.5` are also available. To pin a version, use its corresponding tag.

### Connector variables

At a minimum, set these variables to match your installation:

```bash
REDIS_URL=redis://redis:6379
WAC_ENGINE=whatsmeow
WAC_DATABASE_URL=postgres://wac:password@postgres:5432/wac
WAC_MEDIA_ROOT=/data/media
WAC_MEDIA_TOKEN=a-long-random-token
```

- `REDIS_URL` deliberately uses the same variable name and server as Chatwoot.
- Without `WAC_MEDIA_ROOT`, no incoming media reaches Chatwoot. When it is set, `WAC_MEDIA_TOKEN` is required. Chatwoot reads the token automatically from the connector's registry entry in Redis.
- `WAC_EVENT_SHARDS`, which defaults to 16, must match Chatwoot's `WHATSAPP_CONNECTOR_EVENT_SHARDS`.
- Use persistent storage for `WAC_MEDIA_ROOT` and the database. Pairings survive restarts because they are stored in the database.
- The full variable table is in [docs/operations.md](docs/operations.md).

### In Chatwoot

```bash
WHATSAPP_CONNECTOR_ENABLED=true
```

This variable enables the entire integration, including the event consumer running in Sidekiq.

### Enable the channel for an account

During the beta, the channel is visible only to accounts enabled individually. The installation administrator enables it in Chatwoot's Rails console, deliberately outside the admin interface:

```ruby
Account.find(ACCOUNT_ID).update!(whatsapp_native_enabled: true)
```

### Create the inbox

In Chatwoot, go to **Settings > Inboxes > Add Inbox > WhatsApp > WhatsApp (native)**. Pair by QR code or by entering the code on your phone.

### Migrate an existing inbox

You can convert an existing inbox, such as Baileys, to “WhatsApp (native)” without losing the inbox's conversation history:

```bash
bundle exec rails "whatsapp:providers:convert[INBOX_ID,native]"
APPLY=1 bundle exec rails "whatsapp:providers:convert[INBOX_ID,native]"
```

The first line previews the changes. The second performs the conversion. Then pair again in the inbox.

To install through Coolify, follow the [Chatwoot fazer.ai deployment guide](https://github.com/fazer-ai/chatwoot/blob/main/docker/README-coolify-deploy.md), written in Portuguese. It includes the connector.

## Known limitations

- Automatic call rejection does not stop ringing for someone calling through WhatsApp Web. The caller's browser keeps ringing.
- Numbers using Coexistence, with the WhatsApp Business app and official API on the same number, do not receive calls, groups, broadcast lists, disappearing messages, view-once media, or live locations. This is a platform restriction.
- Message types this version does not yet recognize, such as polls, appear in the conversation as a notice that something was sent, without the content.
- See [docs/limitations.md](docs/limitations.md) for details. Unresolved limitations and upcoming features are tracked in the [repository's issues](https://github.com/fazer-ai/whatsapp-connector/issues) and [docs/roadmap.md](docs/roadmap.md).

## Documentation

- [Architecture](docs/architecture.md): service organization, session ownership, and a protocol overview.
- [Operations](docs/operations.md): running the connector, all variables, and supported Redis versions.
- [Known limitations](docs/limitations.md): restrictions and behaviors that affect usage.
- [Roadmap](docs/roadmap.md): what is ready and what remains.
- [Protocol](contract/PROTOCOL.md): the client contract, in English.
- [Contributing](CONTRIBUTING.md): development, testing, and changing the protocol.

## Development

Use the Go version declared in `go.mod` and golangci-lint v2. To prepare the environment and run the checks that do not require servers:

```bash
make setup
make check-offline
```

`make check` runs all CI checks and requires PostgreSQL and Redis. See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## Support and community

- Installation and usage questions: [Lucas Moreira Community Q&A](https://www.lucasmoreira.ai/c/perguntas-e-respostas).
- Bugs and feature requests: [GitHub issues](https://github.com/fazer-ai/whatsapp-connector/issues).

## License

MIT license, copyright (c) 2026 FAZER.AI LTDA. See [LICENSE](LICENSE). The connector uses whatsmeow, licensed under the Mozilla Public License 2.0. Third-party components retain their respective licenses.

## Links

- [Chatwoot fazer.ai](https://github.com/fazer-ai/chatwoot)
- [whatsmeow](https://github.com/tulir/whatsmeow)
- [fazer.ai](https://fazer.ai)

Maintained by fazer.ai.
