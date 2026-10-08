# Deploying next to Chatwoot

There are two ways to run the connector with Chatwoot fazer.ai, and both are exercised by an example compose in [`examples/`](../examples/). They share everything on the wire: the same Redis, the same contract, the same registry entry Chatwoot reads the media token from. What differs is who starts the process and where its configuration comes from.

| | Embedded (default) | Separate container |
|---|---|---|
| Who runs it | the Sidekiq container, next to the worker | a service of its own, from `ghcr.io/fazer-ai/whatsapp-connector` |
| Chatwoot settings | none: on unless `WHATSAPP_CONNECTOR_ENABLED=false` | `WHATSAPP_CONNECTOR_EMBEDDED=false` |
| Connector version | the one the Chatwoot image pins | whatever tag the service names |
| Pairings | a database of its own on Chatwoot's PostgreSQL, created on the first start | `WAC_DATABASE_URL`, set by the operator |
| Media token | generated at every container start | `WAC_MEDIA_TOKEN`, set by the operator |
| Example | [`docker-compose.embedded.yaml`](../examples/docker-compose.embedded.yaml) | [`docker-compose.separate.yaml`](../examples/docker-compose.separate.yaml) |

Both examples read their secrets from a `.env` built from [`examples/.env.example`](../examples/.env.example):

```bash
cp examples/.env.example .env     # fill in the secrets: openssl rand -hex 32
docker compose -f examples/docker-compose.embedded.yaml --env-file .env up -d --wait
```

Chatwoot then answers on port 3000. Creating the inbox, and removing the channel from an account, are the same in both modes, and the [README](../README-en.md#install-with-chatwoot-fazerai) covers them.

## Embedded in Sidekiq

The Chatwoot image copies the connector's static binary from a pinned release of this image (`FROM ghcr.io/fazer-ai/whatsapp-connector:<version>` in its Dockerfile, moved by Dependabot). Unless `WHATSAPP_CONNECTOR_ENABLED=false`, the worker container's entrypoint hands the Sidekiq command to a supervisor, `docker/entrypoints/helpers/whatsapp_connector.rb` in the Chatwoot repository, which becomes PID 1 and starts both:

- **Sidekiq is the container.** When it exits the connector is stopped and the container exits with Sidekiq's status, so restart policies and healthchecks behave as they did without the connector.
- **The connector is restarted, Sidekiq is not touched.** A connector that exits is started again after a backoff that doubles from one second up to thirty, and starts over once a run has lasted a minute.
- **SIGTERM goes to both at once.** The connector hands its session leases back to Redis while Sidekiq drains its jobs. Give the container a stop grace period long enough for both; the example uses 30 seconds.

The connector's configuration is derived from Chatwoot's. Any `WAC_*` variable set on the container wins over the derived value.

| Connector variable | Derived from |
|---|---|
| `REDIS_URL`, `REDIS_PASSWORD` | Chatwoot's own, read as they are |
| `WAC_ENGINE` | `whatsmeow` |
| `WAC_DATABASE_URL` | Chatwoot's database connection (`DATABASE_URL`, else the `POSTGRES_*` variables), pointed at a database named after Chatwoot's with `_whatsapp_connector` appended: `chatwoot_production_whatsapp_connector`. `sslmode` follows the URL, then `PGSSLMODE`, then `prefer`, which is what Chatwoot's own driver does |
| `WAC_MEDIA_ROOT` | `whatsapp-connector` under the container's temporary directory. A cache, so it does not need to survive the container, and deliberately not on a volume replicas share: each connector sweeps the temporary files it does not know of, including another replica's downloads in progress |
| `WAC_MEDIA_TOKEN` | random, generated when the container starts and kept across connector restarts |
| `WAC_EVENT_SHARDS` | `WHATSAPP_CONNECTOR_EVENT_SHARDS` |
| `WAC_REDIS_PREFIX` | `WHATSAPP_CONNECTOR_REDIS_PREFIX` |

The database is created on the first start, which needs a PostgreSQL user allowed to create databases. The superuser the official `postgres` image creates is. When the user is not, create the database by hand with that name and owner, or set `WAC_DATABASE_URL`. Until the database is reachable the connector is retried with the same backoff, and Sidekiq runs regardless.

Every Sidekiq replica runs a connector of its own, and they share the database the way any fleet does: one owner per session at a time, arbitrated by the Redis lease.

## As a separate container

Set `WHATSAPP_CONNECTOR_EMBEDDED=false` on the Chatwoot containers and run the connector as a service of its own. This is the arrangement for updating the connector without waiting for a Chatwoot release, for running it on another host, or for scaling it apart from Sidekiq. The configuration is the operator's, with [operations.md](operations.md#configuration) as the reference; the example keeps pairings in SQLite on a volume, which is enough for one instance.

## WhatsApp calls

Calls answered or placed in the agent's browser carry their voice through one UDP port of the connector, `WAC_CALLS_UDP_PORT`, and the agents' browsers send to it directly. Both examples use port `40000` and publish it as `40000:40000/udp`: in the embedded mode on the Sidekiq container, which is where the connector runs, and in the separate mode on the connector's own service. A firewall in front of the host has to let that UDP port in as well. Set `WAC_CALLS_PUBLIC_IP` to the address the browsers reach the host at. Both examples require it, because inside a container the connector only knows the container's own address, which no browser can reach; outside a container it can be left unset when the browsers reach the host at its own address, and is needed behind NAT. Leaving `WAC_CALLS_UDP_PORT` unset leaves calls off, and nothing else changes. Each instance of a fleet needs a port of its own that the browsers can reach, because a call is carried by the instance that owns the session. [operations.md](operations.md#configuration) has the details.

A call costs the connector about a tenth of a CPU core while it lasts, for the transcoding between WhatsApp's codec and the browser's in both directions. Measured on the live bench of #383, on an Apple M5 Pro already under heavy load: 2.5 s of CPU over 30 s of one call with the browser sending a tone and G.722 negotiated, against 0.01 s over 20 s with the session idle. Size a host for its expected number of simultaneous calls on that figure.

## Switching an existing installation

An installation that already runs the connector as a separate service and upgrades to a Chatwoot image that embeds it **must set `WHATSAPP_CONNECTOR_EMBEDDED=false` before the upgrade**. Without it, the Sidekiq container starts a second connector with an empty database of its own, and the two compete for the same sessions through the same Redis.

Moving from a separate connector to the embedded one carries no pairing across by itself: the embedded connector starts with an empty database, so each inbox would have to be paired again. When the separate connector keeps its pairings in PostgreSQL, keep them by setting `WAC_DATABASE_URL` on the Sidekiq container to that database, stopping the separate service, and only then removing `WHATSAPP_CONNECTOR_EMBEDDED=false`.
