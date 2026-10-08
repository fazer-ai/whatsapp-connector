# Running the connector

Next to Chatwoot, the connector runs either inside the Sidekiq container or as a service of its own: [deployment.md](deployment.md) has both, with an example compose for each. This page is about the connector itself.

## Running one

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

Give each connector a PostgreSQL database of its own. Its tables go in whichever schema
the connection's `search_path` resolves to, and whatsmeow's upgrade looks for its version
table in every schema it can see: a `whatsmeow_version` in a schema that path does not
reach, whether another connector's or another application's, makes the connector refuse
to start rather than guess which one is its own. So does a path that puts another schema in
front of the one holding the tables, since new tables would go there and the store would be
read from two places.

## Configuration

| Variable | Default | What it is |
|---|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379` | The Redis shared with the client, 6.2 or newer (see below). Deliberately not `WAC_`-prefixed: both sides read the same variable, so they cannot be pointed at different servers |
| `REDIS_PASSWORD` | — | Overrides the password in the URL, for deployments that pass the two separately |
| `WAC_INSTANCE` | the hostname | This instance's id. In a container the hostname is the container id, which is unique per replica |
| `WAC_REDIS_PREFIX` | `wa:` | Namespaces every key, so one Redis can host two independent fleets |
| `WAC_EVENT_SHARDS` | `16` | How many event streams the fleet publishes to. Fleet-wide and effectively permanent: an instance that disagrees with what is recorded refuses to start |
| `WAC_ENGINE` | `fake` | `whatsmeow` for a real account, `fake` for a fleet with nothing behind it |
| `WAC_DATABASE_URL` | none | Where pairings live. `postgres://…`, `sqlite:…` or `file:…`. Required by the `whatsmeow` engine |
| `WAC_DATABASE_MAX_CONNS` | `20` | Ceiling on the Postgres pool. There is one pool per process, shared by every session on it, and a connection is taken per query rather than held per account, so this bounds concurrent queries and not paired numbers. Left uncapped, `database/sql` opens one per concurrent query and a burst can reach Postgres's own `max_connections`, which refuses connections to every other application on that server. Starting takes one more, outside this ceiling and only until the schema is checked and upgraded, because the instances that open one database at once take turns at it. Ignored for SQLite, where a file holds one writer whatever the pool says |
| `WAC_DEVICE_NAME` | `Chrome` | What the account's linked-devices list shows, paired with a CHROME platform so the entry reads like the web session it behaves as. A browser's name because that is what the list is full of, and a row naming a product nobody recognises is the one part of the handshake that says out loud this is not a browser. Fleet-wide, not per session: whatsmeow keeps device properties process-wide |
| `WAC_HTTP_ADDR` | `:8080` | Where `/healthz`, `/readyz` and `/metrics` listen |
| `WAC_ADVERTISE_URL` | derived | How clients reach this instance for media |
| `WAC_MEDIA_ROOT` | unset | Where inbound media is cached. Unset turns the store and the endpoint off, and every media message is then published with `media.download_failed` behind it |
| `WAC_MEDIA_TOKEN` | unset | Bearer token the media endpoint requires. Required whenever `WAC_MEDIA_ROOT` is set: the endpoint hands out message contents |
| `WAC_MEDIA_TTL` | `24h` | How long a blob is kept without being collected. The cache is walked every half of it, between a second and a minute, so a short TTL is swept often |
| `WAC_MEDIA_QUOTA` | `2GiB` | Disk the blobs may take, counted in whole blocks and including each blob's description. Over it, the least recently collected go first |
| `WAC_MEDIA_MAX_BLOB` | `100MiB` | The largest single file this instance keeps, and the most an inbound download is allowed to move. The file is written straight to the cache and a transfer is refused at the byte past this, so what a sender who understates its length can make this instance hold is bounded here rather than by whatever the far end decides to serve |
| `WAC_MEDIA_BLOCK_SIZE` | `4KiB` | The allocation unit of the volume the cache sits on. Set it to match a filesystem formatted with larger units, or every file is undercharged against the quota |
| `WAC_MEDIA_SEND_MAX` | `100MiB` | The largest file this instance will send. Independent of the cache: an instance given no `WAC_MEDIA_ROOT` at all still sends, and the caller's own declared size is refused against this before anything is fetched. WhatsApp's own ceiling is per type and moves on its own schedule, so a file past that is refused by WhatsApp with an answer the caller is told. The binding limit is the command's own deadline rather than this, so a caller that sends a fixed one never reaches this cap however high it is set: what has to fit inside that deadline is the fetch, the encryption pass and the upload together. The supported client used to allow 18 seconds for every send and now sizes a send carrying a file from the length it declares ([fazer-ai/chatwoot#479](https://github.com/fazer-ai/chatwoot/pull/479)); a client that does not is still bounded by whatever it sends ([#29](https://github.com/fazer-ai/whatsapp-connector/issues/29)) |
| `WAC_MEDIA_FETCH_HOSTS` | unset | The hosts a file to send may be fetched from, as `host` or `host:port`, separated by commas: for Chatwoot, the host of its `INTERNAL_HOST_URL` (for example `rails:3000`), plus the storage host when attachments are served from S3 or another bucket through a redirect. A host named without a port is allowed on any port; with one, only on that port, counting `80` for `http` and `443` for `https`. Compared by name, case-insensitively, on the address the client sends and again on every redirect, so a listed host that redirects outside the list is refused at that hop. A refused send fails with `invalid_payload` before the host is dialled, and is not retried. Unset, any host is fetched, which is what every instance did before the setting existed; the link-local range and the cloud metadata addresses are refused either way. A value that is not a list of hosts (a scheme, a path, a port that is not a number) stops the instance at startup ([#31](https://github.com/fazer-ai/whatsapp-connector/issues/31)) |
| `WAC_MEDIA_REFETCH_TTL` | `168h`, or `WAC_MEDIA_TTL` when that is longer | How long a message can still be asked for its file again, after the blob it was published with has gone. Must be at least `WAC_MEDIA_TTL`, which is why the default follows it up. What is kept for that long is a row per media message holding the key to the file, so it is retention rather than cache |
| `WAC_CALLS_UDP_PORT` | unset | The one UDP port the voice of every WhatsApp call answered or placed in a browser goes through (`calls.answer`, `call.accept`, `call.start`). The agents' browsers must reach it, so the deployment has to publish it as UDP. Opened when the instance starts, and a port that is taken stops the start. Unset leaves calls off: the socket is not opened, meowcaller is not installed on any session, and a session asking for `calls.answer` gets offers without `sdp` and the call commands answered `unsupported`. The examples use `40000` |
| `WAC_CALLS_PUBLIC_IP` | unset | The addresses a browser is told to send call media to, separated by commas, in place of this host's own: the public IP of a host behind a 1:1 NAT. Unset announces the host's interface addresses. Call media is IPv4 only. Set without `WAC_CALLS_UDP_PORT`, or with a value that is not an IPv4 address, it stops the instance at startup |
| `WAC_LEASE_TTL` | `30s` | How long a session lease survives without a renewal |
| `WAC_HEARTBEAT` | `5s` | How often leases are renewed and the instance re-announces. Also bounds how long a read waits on Redis (half a heartbeat), and has to leave room for the read and the batch before it: `1.5 × heartbeat + lease/3 < lease` |
| `WAC_CLAIM_MIN_IDLE` | `1.5 × lease` | How long a command sits unacknowledged before another instance takes it over. Must exceed `WAC_LEASE_TTL` |
| `WAC_LOG_LEVEL` | `info` | zerolog level |

## Which Redis

Redis 6.2 or newer. `XAUTOCLAIM`, which the transport reclaims unanswered commands with,
arrived in 6.2, so that is the floor. The connector asks the server its version with
`HELLO` when it starts and refuses to start below 6.2, or when the server does not say
which version it runs, naming both. It does that before it writes anything to Redis, so a
refused instance leaves no trace in the fleet. A server that speaks the Redis protocol
under another name, such as Valkey, is held to the version it reports.

Two things need 7.0 and are absent on 6.2, where the server does not report the counters
they read: the warning when a `MAXLEN` trim cut commands nobody was handed, and the lag of
a consumer group. On 6.2 every group reports `wac_stream_lag_unknown 1` and no
`wac_stream_lag` sample; `wac_stream_pending` and `wac_stream_consumers` are reported as
on any other version.

## The one key that never expires

`wa:lease-epoch:<sid>` has no TTL, deliberately, and `contract/PROTOCOL.md` says why: it
is the fencing token a client uses to tell the current owner of a session from a previous
one, so a counter that restarted would let a stale owner out-rank the live one and
overwrite its state. Do not give it an `EXPIRE`. `internal/cluster/lease_test.go` fails if
anyone does.

What that costs is a key of roughly sixty bytes for every session id this deployment ever
adopted and did not end through a `session.delete` that ran: ids a `session.wake` named and
nothing ever paired, accounts deleted before this rule existed, and accounts whose delete
was delivered twice, since the second delivery is answered from the command record while
the adoption behind it writes the counter back. Count them with:

```sh
redis-cli --scan --pattern 'wa:lease-epoch:*' | wc -l
```

That number only grows. It is not comparable to `wa:lease:*`, which counts the sessions an
instance owns at that second and is smaller than the live fleet whenever anything is
disconnected; the number to compare it against is how many sessions the client still has an
inbox for, and only the client has that.

Reclaiming the difference is not something this service can do on its own. An account
deleted and an account waiting to be paired again leave the same traces here, which is no
rows at all, and deleting the counter of the second breaks it silently. So a sweep belongs
on the client side or nowhere, and today it is nowhere
([#159](https://github.com/fazer-ai/whatsapp-connector/issues/159)).
