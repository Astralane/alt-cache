use crate::{
    config::{Config, Source, secret},
    logging::Alerts,
    store::{Account, Key, KeyedUiAccount, Store, parse_key, program_id},
};
use anyhow::{Context, Result, bail, ensure};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use solana_address_lookup_table_interface::program;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::{
    cuckoo::CuckooFilter,
    prelude::{
        CommitmentLevel, SlotStatus, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterSlots, SubscribeRequestPing, SubscribeUpdate,
        subscribe_update::UpdateOneof,
    },
};

const PAGE_SIZE: usize = 10_000;

async fn rpc(client: &reqwest::Client, url: &str, method: &str, params: Value) -> Result<Value> {
    let reply: Value = client
        .post(url)
        .json(&json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(reply.get("error").is_none(), "RPC request failed");
    reply.get("result").cloned().context("RPC result missing")
}

#[derive(Deserialize)]
struct PagedAccounts {
    accounts: Vec<KeyedUiAccount>,
    #[serde(rename = "paginationKey")]
    pagination_key: Option<String>,
}

async fn get_slot(client: &reqwest::Client, url: &str) -> Result<u64> {
    rpc(client, url, "getSlot", json!([{"commitment":"processed"}]))
        .await?
        .as_u64()
        .context("slot missing")
}

async fn fetch_accounts(
    client: &reqwest::Client,
    url: &str,
    keys_only: bool,
) -> Result<(u64, Vec<KeyedUiAccount>)> {
    let mut config = json!({"commitment":"processed", "encoding":"base64+zstd"});
    if keys_only {
        config["dataSlice"] = json!({"offset":0, "length":0});
    }
    let slot = get_slot(client, url).await?;
    config["limit"] = PAGE_SIZE.into();
    let mut accounts = Vec::new();
    let mut cursor: Option<String> = None;
    let mut seen = HashSet::new();
    loop {
        if let Some(value) = &cursor {
            config["paginationKey"] = Value::String(value.clone());
        }
        let page: PagedAccounts = serde_json::from_value(
            rpc(
                client,
                url,
                "getProgramAccountsV2",
                json!([program_id(), config]),
            )
            .await?,
        )?;
        accounts.extend(page.accounts);
        let Some(next) = page.pagination_key else {
            break;
        };
        ensure!(seen.insert(next.clone()), "RPC repeated its pagination key");
        cursor = Some(next);
    }
    Ok((slot, accounts))
}

async fn fetch_keys(client: &reqwest::Client, url: &str) -> Result<HashSet<Key>> {
    let (_, accounts) = fetch_accounts(client, url, true).await?;
    accounts
        .into_iter()
        .map(|entry| parse_key(&entry.pubkey))
        .collect()
}

async fn bootstrap(
    client: &reqwest::Client,
    url: &str,
) -> Result<(u64, BTreeMap<Key, Arc<Account>>)> {
    let (slot, entries) = fetch_accounts(client, url, false).await?;
    let mut accounts = BTreeMap::new();
    for entry in entries {
        if entry.account.lamports == 0 {
            continue;
        }
        let key = parse_key(&entry.pubkey)?;
        let account = Account::from_rpc(entry.pubkey, entry.account, slot)?;
        accounts.insert(key, Arc::new(account));
    }
    Ok((slot, accounts))
}

struct TrackedKeys {
    keys: HashSet<Key>,
    filter: CuckooFilter<Key>,
}

impl TrackedKeys {
    fn new(keys: HashSet<Key>) -> Result<Self> {
        let headroom = (keys.len() / 4).max(4_096);
        let mut filter = CuckooFilter::with_capacity(keys.len() + headroom)?;
        for key in &keys {
            filter.insert(key)?;
        }
        Ok(Self { keys, filter })
    }

    fn insert(&mut self, key: Key) -> Result<bool> {
        if self.keys.contains(&key) {
            return Ok(false);
        }
        self.filter.insert(&key)?;
        self.keys.insert(key);
        Ok(true)
    }

    fn request(&self) -> SubscribeRequest {
        SubscribeRequest {
            accounts: HashMap::from([
                (
                    "alt-owner".into(),
                    SubscribeRequestFilterAccounts {
                        owner: vec![program_id()],
                        ..Default::default()
                    },
                ),
                (
                    "alt-keys".into(),
                    SubscribeRequestFilterAccounts {
                        cuckoo_accounts_filter: Some((&self.filter).into()),
                        ..Default::default()
                    },
                ),
            ]),
            slots: HashMap::from([(
                "slots".into(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(false),
                    interslot_updates: Some(true),
                },
            )]),
            commitment: Some(CommitmentLevel::Processed as i32),
            ..Default::default()
        }
    }
}

pub async fn run(config: Arc<Config>, store: Arc<Store>, alerts: Alerts, stop: CancellationToken) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let mut genesis = None;
    let mut index = 0;
    loop {
        let source = &config.sources[index];
        store.invalidate();
        tracing::info!(source = source.name, "starting source recovery");
        let result = tokio::select! {
            _ = stop.cancelled() => break,
            result = session(&config, source, &client, &store, &mut genesis) => result,
        };
        store.invalidate();
        if result.is_err() {
            tracing::warn!(
                source = source.name,
                "source session failed; trying next source"
            );
            alerts.send(format!(
                "ALT cache source {} failed; readiness is false",
                source.name
            ));
            index = (index + 1) % config.sources.len();
        }
        tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(2)) => {} }
    }
    store.invalidate();
}

async fn connect(source: &Source) -> Result<GeyserGrpcClient> {
    let grpc_url = secret(&source.grpc_url_env)?;
    let mut builder = GeyserGrpcClient::build_from_shared(grpc_url.clone())?
        .x_token(source.token_env.as_deref().map(secret).transpose()?)?
        .connect_timeout(Duration::from_secs(10));
    if grpc_url.starts_with("https://") {
        builder =
            builder.tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())?;
    }
    Ok(builder.connect().await?)
}

async fn session(
    config: &Config,
    source: &Source,
    client: &reqwest::Client,
    store: &Store,
    genesis: &mut Option<String>,
) -> Result<()> {
    let url = secret(&source.rpc_url_env)?;
    let cluster = rpc(client, &url, "getGenesisHash", json!([]))
        .await?
        .as_str()
        .context("invalid genesis hash")?
        .to_owned();
    if let Some(expected) = genesis {
        ensure!(*expected == cluster, "source cluster mismatch");
    } else {
        *genesis = Some(cluster);
    }

    let mut tracked = TrackedKeys::new(fetch_keys(client, &url).await?)?;
    let mut grpc = connect(source).await?;
    let (requests, receiver) = mpsc::channel(8);
    requests.send(tracked.request()).await?;
    let mut stream = grpc
        .geyser
        .subscribe(tokio_stream::wrappers::ReceiverStream::new(receiver))
        .await?
        .into_inner();

    let snapshot = bootstrap(client, &url);
    tokio::pin!(snapshot);
    let mut buffered = VecDeque::with_capacity(config.stream_capacity.min(4_096));
    let (baseline, accounts) = loop {
        tokio::select! {
            result = &mut snapshot => break result?,
            update = stream.next() => {
                let update = update.context("source stream closed")??;
                if matches!(update.update_oneof, Some(UpdateOneof::Ping(_))) {
                    send_ping(&requests)?;
                } else {
                    ensure!(buffered.len() < config.stream_capacity, "recovery update buffer full");
                    buffered.push_back(update);
                }
            }
        }
    };
    let target = get_slot(client, &url).await?;
    let mut filter_changed = false;
    for key in accounts.keys() {
        filter_changed |= tracked.insert(*key)?;
    }
    if filter_changed {
        requests.try_send(tracked.request())?;
    }
    store.install(source.name.clone(), baseline, accounts);

    let reconcile = tokio::time::sleep(Duration::from_secs(config.reconcile_after_secs));
    let watchdog = tokio::time::sleep(Duration::from_secs(config.stale_after_secs));
    tokio::pin!(reconcile, watchdog);
    let mut last_slot = baseline;
    while let Some(update) = buffered.pop_front() {
        handle_update(
            update,
            baseline,
            target,
            config.stale_after_secs,
            store,
            &mut tracked,
            &requests,
            &mut last_slot,
            watchdog.as_mut(),
            source,
        )?;
    }

    loop {
        let update = tokio::select! {
            _ = &mut reconcile => {
                tracing::info!(source = source.name, "scheduled reconciliation");
                return Ok(());
            },
            _ = &mut watchdog => bail!("source slot progress stalled"),
            update = stream.next() => update.context("source stream closed")??,
        };
        handle_update(
            update,
            baseline,
            target,
            config.stale_after_secs,
            store,
            &mut tracked,
            &requests,
            &mut last_slot,
            watchdog.as_mut(),
            source,
        )?;
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_update(
    update: SubscribeUpdate,
    baseline: u64,
    target: u64,
    stale_after_secs: u64,
    store: &Store,
    tracked: &mut TrackedKeys,
    requests: &mpsc::Sender<SubscribeRequest>,
    last_slot: &mut u64,
    mut watchdog: std::pin::Pin<&mut tokio::time::Sleep>,
    source: &Source,
) -> Result<()> {
    match update.update_oneof {
        Some(UpdateOneof::Ping(_)) => send_ping(requests)?,
        Some(UpdateOneof::Account(update)) => {
            let account = update.account.context("account body missing")?;
            let key: Key = account
                .pubkey
                .as_slice()
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid account key"))?;
            let owner: Key = account
                .owner
                .as_slice()
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid owner key"))?;
            let write_version = account.write_version;
            let is_alt = owner == program::id().to_bytes();
            if !is_alt && !tracked.keys.contains(&key) {
                return Ok(());
            }
            let value = if account.lamports == 0 || !is_alt {
                None
            } else {
                if tracked.insert(key)? {
                    requests.try_send(tracked.request())?;
                }
                Some(Account::from_yellowstone(update.slot, account)?)
            };
            store.apply(key, update.slot, write_version, value);
        }
        Some(UpdateOneof::Slot(update)) => {
            if update.status == SlotStatus::SlotDead as i32 && update.slot > baseline {
                bail!("processed fork invalidated");
            }
            if update.status == SlotStatus::SlotProcessed as i32 && update.slot > *last_slot {
                *last_slot = update.slot;
                watchdog
                    .as_mut()
                    .reset(tokio::time::Instant::now() + Duration::from_secs(stale_after_secs));
                if store.progress(update.slot, target) {
                    tracing::info!(
                        source = source.name,
                        baseline,
                        slot = update.slot,
                        "cache ready"
                    );
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn send_ping(requests: &mpsc::Sender<SubscribeRequest>) -> Result<()> {
    requests.try_send(SubscribeRequest {
        ping: Some(SubscribeRequestPing { id: 1 }),
        ..Default::default()
    })?;
    Ok(())
}

// Standbys consume slot notifications only. Account write versions stay source-local.
pub async fn monitor(source: Source, store: Arc<Store>, stop: CancellationToken) {
    loop {
        let result: Result<()> = async {
            let mut client = connect(&source).await?;
            let (mut requests, mut stream) = client
                .subscribe_with_request(Some(SubscribeRequest {
                    slots: HashMap::from([(
                        "health".into(),
                        SubscribeRequestFilterSlots {
                            filter_by_commitment: Some(true),
                            interslot_updates: Some(false),
                        },
                    )]),
                    commitment: Some(CommitmentLevel::Processed as i32),
                    ..Default::default()
                }))
                .await?;
            loop {
                let update = tokio::select! {
                    _ = stop.cancelled() => return Ok(()),
                    update = tokio::time::timeout(Duration::from_secs(15), stream.next()) => {
                        update?.context("standby closed")??
                    }
                };
                if let Some(UpdateOneof::Slot(slot)) = update.update_oneof {
                    store.source_progress(&source.name, slot.slot);
                } else if matches!(update.update_oneof, Some(UpdateOneof::Ping(_))) {
                    use futures::SinkExt;
                    requests
                        .send(SubscribeRequest {
                            ping: Some(SubscribeRequestPing { id: 1 }),
                            ..Default::default()
                        })
                        .await?;
                }
            }
        }
        .await;
        if result.is_err() {
            tracing::warn!(source = source.name, "standby connection lost");
        }
        tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(2)) => {} }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_subscription_tracks_owner_and_existing_keys() {
        let key = [7; 32];
        let tracked = TrackedKeys::new(HashSet::from([key])).unwrap();
        let request = tracked.request();
        assert_eq!(request.accounts["alt-owner"].owner, vec![program_id()]);
        let wire = request.accounts["alt-keys"]
            .cuckoo_accounts_filter
            .as_ref()
            .unwrap();
        assert!(CuckooFilter::<Key>::from(wire).contains(&key));
    }
}
