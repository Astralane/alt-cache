use crate::{
    config::{Config, secret},
    logging,
    store::{Account, Key, KeyedUiAccount, StateUpdater, Store, parse_key, program_id},
    yellowstone::Event,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use solana_address_lookup_table_interface::program;
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use yellowstone_grpc_proto::prelude::SubscribeUpdateAccount;

const PAGE_SIZE: usize = 10_000;

#[derive(Deserialize)]
struct JsonRpcResponse<T> {
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

async fn rpc<T: DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    method: &str,
    params: impl Serialize,
) -> Result<T> {
    let reply: JsonRpcResponse<T> = client
        .post(url)
        .json(&json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if let Some(error) = reply.error {
        anyhow::bail!("RPC error {}: {}", error.code, error.message);
    }
    reply.result.context("RPC result missing")
}

#[derive(Deserialize)]
struct PagedAccounts {
    accounts: Vec<KeyedUiAccount>,
    #[serde(rename = "paginationKey")]
    pagination_key: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProgramAccountsConfig<'a> {
    commitment: &'static str,
    encoding: &'static str,
    limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pagination_key: Option<&'a str>,
}

async fn get_slot(client: &reqwest::Client, url: &str) -> Result<u64> {
    rpc(client, url, "getSlot", json!([{"commitment":"confirmed"}])).await
}

async fn bootstrap(
    client: &reqwest::Client,
    url: &str,
) -> Result<(u64, BTreeMap<Key, Arc<Account>>)> {
    let slot = get_slot(client, url).await?;
    let mut cursor: Option<String> = None;
    let mut seen = HashSet::new();
    let mut accounts = BTreeMap::new();
    loop {
        let config = ProgramAccountsConfig {
            commitment: "confirmed",
            encoding: "base64+zstd",
            limit: PAGE_SIZE,
            pagination_key: cursor.as_deref(),
        };
        let page: PagedAccounts = rpc(
            client,
            url,
            "getProgramAccountsV2",
            json!([program_id(), config]),
        )
        .await?;
        for entry in page.accounts {
            if entry.account.lamports == 0 {
                continue;
            }
            let key = parse_key(&entry.pubkey)?;
            let account = Account::from_rpc(entry.pubkey, entry.account)?;
            accounts.insert(key, Arc::new(account));
        }
        let Some(next) = page.pagination_key else {
            break;
        };
        ensure!(seen.insert(next.clone()), "RPC repeated its pagination key");
        cursor = Some(next);
    }

    Ok((slot, accounts))
}

fn next_connected(active: usize, connected: &HashSet<usize>, count: usize) -> Option<usize> {
    (1..=count)
        .map(|offset| (active + offset) % count)
        .find(|source| connected.contains(source))
}

enum ActiveEvent {
    Account(Box<SubscribeUpdateAccount>),
    ConfirmedSlot(u64),
}

pub async fn run(
    config: Arc<Config>,
    store: Arc<Store>,
    mut events: mpsc::Receiver<Event>,
    stop: CancellationToken,
) -> Result<()> {
    let bootstrap_rpc_url = secret(&config.bootstrap_rpc.url_env)?;
    let sources: Vec<_> = config
        .grpc_sources
        .iter()
        .map(|source| secret(&source.url_env))
        .collect::<Result<_>>()?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()?;
    let mut updater = StateUpdater::new(store);
    let mut connected = HashSet::new();
    let mut active = 0;

    'recover: loop {
        updater.invalidate();
        if !connected.contains(&active)
            && let Some(source) = next_connected(active, &connected, sources.len())
        {
            active = source;
        }
        while !connected.contains(&active) {
            let event = tokio::select! {
                _ = stop.cancelled() => break 'recover,
                event = events.recv() => event.context("all Yellowstone sources stopped")?,
            };
            match event {
                Event::Connected { source } => {
                    connected.insert(source);
                    active = source;
                }
                Event::Disconnected { source } => {
                    connected.remove(&source);
                }
                Event::Account { .. } | Event::ConfirmedSlot { .. } => {}
            }
        }

        let source = sources[active].clone();
        tracing::info!(source, "starting state recovery");
        let bootstrap_fut = bootstrap(&client, &bootstrap_rpc_url);
        tokio::pin!(bootstrap_fut);
        let mut buffered = VecDeque::with_capacity(config.stream_capacity.min(4_096));
        let bootstrap_result = loop {
            tokio::select! {
                _ = stop.cancelled() => break 'recover,
                result = &mut bootstrap_fut => break result,
                event = events.recv() => {
                    match event.context("all Yellowstone sources stopped")? {
                        Event::Connected { source } => { connected.insert(source); }
                        Event::Disconnected { source: failed } => {
                            connected.remove(&failed);
                            if failed == active {
                                if let Some(source) = next_connected(active, &connected, sources.len()) {
                                    active = source;
                                }
                                continue 'recover;
                            }
                        }
                        Event::Account { source, update } => {
                            if source == active {
                                ensure!(buffered.len() < config.stream_capacity, "recovery update buffer full");
                                buffered.push_back(ActiveEvent::Account(update));
                            }
                        }
                        Event::ConfirmedSlot { source, slot } => {
                            if source == active {
                                ensure!(buffered.len() < config.stream_capacity, "recovery update buffer full");
                                buffered.push_back(ActiveEvent::ConfirmedSlot(slot));
                            }
                        }
                    }
                }
            }
        };
        let (bootstrap_slot, accounts) = match bootstrap_result {
            Ok(value) => value,
            Err(_) => {
                logging::alert("ALT cache bootstrap RPC failed; readiness is false");
                tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(2)) => continue,
                }
            }
        };
        updater.install(source.clone(), bootstrap_slot, accounts);

        let reconcile = tokio::time::sleep(Duration::from_secs(config.reconcile_after_secs));
        tokio::pin!(reconcile);
        while let Some(event) = buffered.pop_front() {
            let result = match event {
                ActiveEvent::Account(update) => apply(*update, &mut updater),
                ActiveEvent::ConfirmedSlot(slot) => {
                    updater.confirm(slot);
                    Ok(())
                }
            };
            if result.is_err() {
                if let Some(source) = next_connected(active, &connected, sources.len()) {
                    active = source;
                }
                continue 'recover;
            }
        }
        updater.finish_recovery();
        tracing::info!(source, bootstrap_slot, "cache ready");

        loop {
            let event = tokio::select! {
                _ = stop.cancelled() => break 'recover,
                _ = &mut reconcile => {
                    tracing::info!(source, "scheduled reconciliation");
                    continue 'recover;
                }
                event = events.recv() => event.context("all Yellowstone sources stopped")?,
            };
            match event {
                Event::Connected { source } => {
                    connected.insert(source);
                }
                Event::Disconnected { source: failed } => {
                    connected.remove(&failed);
                    if failed == active {
                        if let Some(source) = next_connected(active, &connected, sources.len()) {
                            active = source;
                        }
                        logging::alert(format!(
                            "ALT cache source {source} failed; readiness is false"
                        ));
                        continue 'recover;
                    }
                }
                Event::Account {
                    source: event_source,
                    update,
                } => {
                    if event_source == active && apply(*update, &mut updater).is_err() {
                        if let Some(source) = next_connected(active, &connected, sources.len()) {
                            active = source;
                        }
                        continue 'recover;
                    }
                }
                Event::ConfirmedSlot {
                    source: event_source,
                    slot,
                } => {
                    if event_source == active {
                        updater.confirm(slot);
                    }
                }
            }
        }
    }
    updater.invalidate();
    Ok(())
}

fn apply(update: SubscribeUpdateAccount, updater: &mut StateUpdater) -> Result<()> {
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
    let value = if account.lamports == 0 || owner != program::id().to_bytes() {
        None
    } else {
        Some(Account::from_yellowstone(account)?)
    };
    updater.queue(key, update.slot, write_version, value)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_connected_wraps_and_skips_disconnected_sources() {
        let connected = HashSet::from([0, 3]);
        assert_eq!(next_connected(0, &connected, 4), Some(3));
        assert_eq!(next_connected(3, &connected, 4), Some(0));
    }

    #[test]
    fn next_connected_returns_current_only_when_it_is_the_only_option() {
        let connected = HashSet::from([2]);
        assert_eq!(next_connected(2, &connected, 4), Some(2));
        assert_eq!(next_connected(0, &HashSet::new(), 4), None);
    }
}
