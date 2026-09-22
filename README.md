# Astralane ALT cache

Rust library and in-memory service, version **4.2.2**. Live account updates use
**confirmed** commitment.

## Data flow

1. Open one Yellowstone stream per configured gRPC source, subscribing to ALT
   accounts and confirmed slot notifications.
2. Fetch one full confirmed snapshot while buffering active-source updates.
3. Stage account updates by slot and publish them only after that slot is
   confirmed.
4. Serve complete or paginated immutable confirmed snapshots.

The bootstrap RPC is independent of the gRPC sources. It must be Helius or a
compatible provider implementing paginated `getProgramAccountsV2`. Every gRPC
source runs an account subscription on its own OS thread and current-thread
async runtime. The sources send account events through one bounded channel to a
dedicated state updater. Yellowstone ping, slot, or account traffic keeps a
source alive; a silent source times out after
`yellowstone_idle_timeout_secs`. The first gRPC source is primary; the updater
observes all sources but applies account writes only from the active source. On
failure, it rotates through the configured gRPC sources and builds a new
snapshot from the bootstrap RPC.
It does not compare write versions from different validators.

A source change, transport failure, or source timeout makes the cache unavailable
until recovery installs a new snapshot.

ALT closure drains the account and clears its data without changing its owner,
so the owner subscription also reports closures. A full refresh runs once per
`full_refresh_interval_secs`. It makes the cache unavailable while it rebuilds.

## Source requirements

- The bootstrap RPC supports the Helius-compatible `getProgramAccountsV2`
  method with `base64+zstd`, pagination, `withContext`, and confirmed
  commitment. The first page's context slot is the bootstrap slot.
- Yellowstone supports owner-filtered account updates, confirmed slot updates,
  and subscription pings. Historical replay is not required.
- The RPC and Yellowstone endpoints may be supplied by different providers, but
  they must serve the same cluster.
- Account and confirmed-slot notification ordering must follow the Yellowstone server
  contract.

## Run

Copy `config.example.toml` to a private `config.toml`. A bootstrap RPC or gRPC
source accepts either `url` or `url_env`. A gRPC source accepts either `token`
or `token_env`, and both token fields may be omitted when authentication is not
required. Do not specify a direct value and its `_env` alternative together.
Treat a config containing direct tokens or credential-bearing URLs as a secret;
do not commit it.

```sh
cargo test --locked
cargo build --release --locked
./target/release/astralane-alt-cache config.toml
```

The package version tracks stable Agave 4.2.2. The ALT decoder uses the Solana
interface and RPC types. SDK component versions are not Agave versions.
No validator process or Agave runtime is embedded.

## Library

`AltCache` bootstraps from this service's `getProgramAccounts` JSON-RPC method,
then maintains a process-local `Arc<DashMap<...>>` from one active Yellowstone
stream. The stream is opened before the snapshot request and account and
confirmed-slot updates are buffered during the fetch, so the snapshot-to-live
handoff has no gap. Yellowstone sources are tried in configuration order and
rotated after a failure. Recovery builds a fresh map and atomically replaces the
active map.

```rust
use astralane_alt_cache::{AltCache, AltCacheConfig, YellowstoneSourceConfig};
use solana_address::Address;

let config = AltCacheConfig::new(
    "http://127.0.0.1:8090",
    vec![
        YellowstoneSourceConfig::new("https://yellowstone-primary.example.com")
            .with_token(primary_token),
        YellowstoneSourceConfig::new("https://yellowstone-secondary.example.com")
            .with_token(secondary_token),
    ],
);

let cache = AltCache::connect(config).await?;
let key: Address = "ALT_ADDRESS".parse()?;
let table: Option<solana_message::AddressLookupTableAccount> = cache.get(&key)?;
```

`AltCache::connect` returns only after the initial snapshot and buffered updates
have been installed. `get` returns an error while the local Yellowstone feed is
recovering, `None` for a missing table, or Solana's standard
`AddressLookupTableAccount`. Dropping the last clone of `AltCache` stops its
background task.

## JSON-RPC

POST to `/`. Methods:

- `getProgramAccounts`, params `["AddressLookupTab1e1111111111111111111111111",
  {"encoding":"base64+zstd"}]`: the complete ALT state using Solana's standard
  program-account response shape.
- `getProgramAccountsV2`, params
  `["AddressLookupTab1e1111111111111111111111111", {"limit":1000,
  "paginationKey":"...","encoding":"base64+zstd"}]`: the same state using
  Helius-compatible cursor pagination. Omit `paginationKey` on the first page.
- `getHealth`, params `[]`: readiness and source progress.

```sh
curl http://127.0.0.1:8090/ -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"getHealth","params":[]}'
```

Both account methods accept `base58`, `base64`, or `base64+zstd` encoding,
  `dataSlice`, `minContextSlot`, and `withContext`. The default encoding is
`base64+zstd`. The cache only serves the ALT program at confirmed commitment.
Filters, `jsonParsed`, and `changedSinceSlot` are rejected. An unready cache
returns error `-32001`.

The non-paginated method captures one immutable map root before encoding, so
updates continue while a large response is serialized. For V2, the pagination
key contains the confirmed slot and last account key. Every page therefore uses
the same retained immutable snapshot even if newer slots are confirmed while a
client is paging.

## Operations

- `getHealth`: reports readiness and the active source.
- The bootstrap completion log reports accumulated response size, fetch time,
  JSON deserialization time, ALT account decoding time, and total snapshot time.
- Each Yellowstone source, the state updater, and each inbound network service
  has one named OS thread and one current-thread async runtime. Stdout uses
  compact text.
  Files use daily rotating JSON. Settings match the
  relay's `logging.stdout` and `logging.file` layout.
- Optional native Slack and Discord webhooks. Each can use `url` or `url_env`.
  Delivery uses one bounded queue, a five-second request timeout, and a
  one-minute rate limit. Alerts are sent to every configured destination and
  remain best effort. Logs remain the source of truth. The gRPC base URL
  identifies its source in health, logs, and alerts. Failure details are logged,
  but authentication tokens and credential-bearing RPC URLs are not.
- SIGTERM/SIGINT cancels the services and stops the process. Data is in memory
  only; every restart performs recovery.
- A permanent service returning unexpectedly is unrecoverable and terminates
  the process with exit status 1 so the process supervisor can restart it.

Bind to loopback or a trusted private interface behind a firewall. This version
has no inbound authentication or TLS. Put a TLS/auth proxy in front of remote
access. Never expose the ports directly to the public Internet.

## Verification status

Unit and local API tests cover version ordering, tombstones, state replacement,
stable JSON-RPC pagination, owner-filter construction, and readiness.
Live RPC pagination, closure delivery, and failover need an integration run
against the deployment endpoints before production use.
