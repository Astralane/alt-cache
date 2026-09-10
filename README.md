# Astralane ALT cache

Rust library and in-memory service, version **4.2.1**. Live account updates use
**processed** commitment. This is provisional state, not finalized state.

## Data flow

1. Fetch the current ALT keys with an RPC data slice.
2. Open a Yellowstone **processed** stream. Subscribe by ALT owner and by a
   compact Cuckoo filter of the known keys.
3. Fetch one full processed snapshot while buffering stream updates.
4. Apply buffered updates newer than the snapshot and mark the cache ready after
   processed slot progress reaches the catch-up target.
5. Serve local reads. Publish ordered upserts and deletions to subscribers.

The first source is primary. Every source has a concurrent slot-only health
subscription. Only the active source supplies account writes. On failure, the
service rotates through the configured sources and builds a new snapshot.
It does not compare write versions from different validators.

A source change, dead-slot notification, transport failure, or stalled slot
progress makes the cache unavailable. Recovery changes the epoch. Subscribers
must replace their local state, not merge snapshots across epochs.

The exact-key filter reports closures and owner changes. The owner filter reports
new tables. New table keys are added to the exact filter. Full reconciliation
runs once per `reconcile_after_secs`. It makes the cache unavailable while it
rebuilds.

## Source requirements

- RPC supports the Helius `getProgramAccountsV2` method with `base64`,
  `dataSlice`, pagination, and processed commitment.
- Yellowstone supports Cuckoo account filters and processed account and slot
  updates. Historical replay is not required.
- Pair each RPC URL with the same validator's Yellowstone endpoint. RPC genesis
  hashes must match across sources. The service cannot prove that an RPC URL and
  a gRPC URL refer to the same validator.
- Processed account and slot notification ordering must follow the Yellowstone
  server contract.

Processed forks can expose provisional account changes before the server reports
a dead slot. The service resets on the notification; it is not a bank-qualified
fork store. Consumers that need exact bank-specific address resolution must use
their bank's account view.

## Run

Copy `config.example.toml` to a private `config.toml`. Set the named environment
variables. Remove `token_env` for endpoints without a token. Do not put secrets
in repository files.

```sh
cargo test --locked
cargo build --release --locked
./target/release/astralane-alt-cache config.toml
```

The package version starts at 4.2.1. The ALT decoder uses the Solana interface
and RPC types. SDK component versions are not Agave versions.
No validator process or Agave runtime is embedded.

## JSON-RPC

POST to `/`. Methods:

- `getLookupTable`, params `["<base58 pubkey>"]`: complete account or `null`.
- `getSnapshot`, params `[{"limit":1000,"after":"...","epoch":"...",
  "sequence":1}]`: an ordered snapshot page. Omit `after`, `epoch`, and
  `sequence` on the first page. Pass the returned epoch and sequence on later
  pages. Restart if the service returns `-32002`.
- `getHealth`, params `[]`: readiness and source progress.

```sh
curl http://127.0.0.1:8090/ -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"getHealth","params":[]}'
```

Reads carry an epoch, sequence and processed slot. Account values use Solana's
`UiAccount` JSON shape with `base64+zstd` data. Preserve 64-bit integers when
decoding JSON.
An unready cache returns error `-32001`. Unknown accounts return `null` only
while the cache is ready.

## gRPC

Contract: `proto/alt.proto`, package `astralane.alt.v1`.

- `Get`: one full account. The account payload is Yellowstone's
  `SubscribeUpdateAccountInfo` message.
- `Subscribe { updates: false }`: stream a complete snapshot, then end.
- `Subscribe { updates: true }`: stream a snapshot, then live updates.

Build a new local map from `SNAPSHOT_ACCOUNT` messages. Atomically install it at
`SNAPSHOT_END`. Snapshot messages share the snapshot sequence. Each subsequent
upsert/delete increments it. Replace the local map after **any** stream error.
Do not resume by sequence: there is no durable event log.

The snapshot and subscription start under the same read lock. Writers cannot
fall into a gap between them. Slow consumers receive `OUT_OF_RANGE`, not a
silently incomplete stream. Source resets produce `ABORTED`.
There are at most 16 concurrent gRPC subscribers. Updates use a bounded ring.

## Operations

- `getHealth`: reports readiness, the active source, and each monitor's slot.
- Standard gRPC health service: query `astralane.alt.v1.AltCache`.
- Each network service has one named OS thread and one current-thread async
  runtime. Stdout uses compact text.
  Files use daily rotating JSON. Settings match the
  relay's `logging.stdout` and `logging.file` layout.
- Optional Slack-compatible webhook. Delivery uses a bounded queue, five-second
  request timeout, and a one-minute rate limit. Alerts are best effort. Logs
  remain the source of truth. Upstream URLs and transport errors are not logged
  because they can contain credentials.
- SIGTERM/SIGINT marks the store unavailable and stops the process. Data is
  in memory only; every restart performs recovery.

Bind to loopback or a trusted private interface behind a firewall. This version
has no inbound authentication or TLS. Put a TLS/auth proxy in front of remote
access. Never expose the ports directly to the public Internet.

## Verification status

Unit and local API tests cover version ordering, tombstones, epoch resets,
snapshot pagination, snapshot-to-stream handoff, subscriber lag, exact-key
filter construction, and readiness. Live RPC pagination, closure delivery, and
failover need an integration run against the deployment endpoints before
production use.
