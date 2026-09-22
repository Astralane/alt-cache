use crate::{
    store::{KeyedUiAccount, parse_key, program_id},
    yellowstone,
};
use anyhow::{Context, Result, ensure};
use arc_swap::ArcSwap;
use dashmap::DashMap;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::json;
use solana_address::Address;
use solana_address_lookup_table_interface::{program, state::AddressLookupTable};
use solana_message::AddressLookupTableAccount;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tonic::Streaming;
use yellowstone_grpc_proto::prelude::{
    SlotStatus, SubscribeRequest, SubscribeUpdate, SubscribeUpdateAccount,
    subscribe_update::UpdateOneof,
};

const RETRY_DELAY: Duration = Duration::from_secs(2);

/// Connection settings for a consumer-side ALT cache.
pub struct AltCacheConfig {
    pub json_rpc_url: String,
    pub yellowstone_url: String,
    pub yellowstone_token: Option<String>,
    pub stale_after: Duration,
    pub update_buffer_capacity: usize,
}

impl AltCacheConfig {
    pub fn new(json_rpc_url: impl Into<String>, yellowstone_url: impl Into<String>) -> Self {
        Self {
            json_rpc_url: json_rpc_url.into(),
            yellowstone_url: yellowstone_url.into(),
            yellowstone_token: None,
            stale_after: Duration::from_secs(15),
            update_buffer_capacity: 4_096,
        }
    }
}

/// A local ALT map bootstrapped from the service and maintained from Yellowstone.
#[derive(Clone)]
pub struct AltCache {
    inner: Arc<Inner>,
}

struct Inner {
    tables: Arc<ArcSwap<DashMap<Address, AddressLookupTableAccount>>>,
    ready: Arc<AtomicBool>,
    confirmed_slot: Arc<AtomicU64>,
    stop: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.get_mut().unwrap().take() {
            task.abort();
        }
    }
}

impl AltCache {
    /// Builds the initial state before returning, then keeps it current in a background task.
    pub async fn connect(config: AltCacheConfig) -> Result<Self> {
        ensure!(
            config.update_buffer_capacity > 0,
            "update_buffer_capacity must be positive"
        );
        ensure!(
            !config.stale_after.is_zero(),
            "stale_after must be positive"
        );
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?;
        let session = recover(&http, &config).await?;
        let tables = Arc::new(ArcSwap::from(session.tables.clone()));
        let ready = Arc::new(AtomicBool::new(true));
        let confirmed_slot = Arc::new(AtomicU64::new(session.confirmed_slot));
        let stop = CancellationToken::new();
        let inner = Arc::new(Inner {
            tables: tables.clone(),
            ready: ready.clone(),
            confirmed_slot: confirmed_slot.clone(),
            stop: stop.clone(),
            task: Mutex::new(None),
        });
        let task = tokio::spawn(run(
            http,
            config,
            session,
            tables,
            ready,
            confirmed_slot,
            stop,
        ));
        *inner.task.lock().unwrap() = Some(task);
        Ok(Self { inner })
    }

    /// Returns a Solana message-compatible lookup table account.
    pub fn get(&self, key: &Address) -> Result<Option<AddressLookupTableAccount>> {
        ensure!(self.is_ready(), "ALT cache is not ready");
        let tables = self.inner.tables.load();
        Ok(tables.get(key).map(|account| account.value().clone()))
    }

    pub fn is_ready(&self) -> bool {
        self.inner.ready.load(Ordering::Acquire)
    }

    pub fn confirmed_slot(&self) -> u64 {
        self.inner.confirmed_slot.load(Ordering::Acquire)
    }
}

struct Session {
    bootstrap_slot: u64,
    confirmed_slot: u64,
    tables: Arc<DashMap<Address, AddressLookupTableAccount>>,
    pending: BTreeMap<u64, Vec<SubscribeUpdateAccount>>,
    requests: mpsc::Sender<SubscribeRequest>,
    stream: Streaming<SubscribeUpdate>,
}

async fn run(
    http: reqwest::Client,
    config: AltCacheConfig,
    mut session: Session,
    tables: Arc<ArcSwap<DashMap<Address, AddressLookupTableAccount>>>,
    ready: Arc<AtomicBool>,
    confirmed_slot: Arc<AtomicU64>,
    stop: CancellationToken,
) {
    loop {
        let result = follow(&mut session, &confirmed_slot, &stop, config.stale_after).await;
        if stop.is_cancelled() {
            return;
        }
        ready.store(false, Ordering::Release);
        tracing::warn!(error = ?result.err(), "ALT client Yellowstone feed disconnected");
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(RETRY_DELAY) => {}
            }
            match recover(&http, &config).await {
                Ok(recovered) => {
                    tables.store(recovered.tables.clone());
                    confirmed_slot.store(recovered.confirmed_slot, Ordering::Release);
                    ready.store(true, Ordering::Release);
                    session = recovered;
                    break;
                }
                Err(error) => tracing::warn!(?error, "ALT client recovery failed"),
            }
        }
    }
}

async fn recover(http: &reqwest::Client, config: &AltCacheConfig) -> Result<Session> {
    let mut grpc =
        yellowstone::connect(&config.yellowstone_url, config.yellowstone_token.as_deref()).await?;
    let (requests, receiver) = mpsc::channel(8);
    requests.send(yellowstone::subscribe_request()).await?;
    let mut stream = grpc
        .geyser
        .subscribe(tokio_stream::wrappers::ReceiverStream::new(receiver))
        .await?
        .into_inner();
    let snapshot = fetch_snapshot(http, &config.json_rpc_url);
    tokio::pin!(snapshot);
    let mut buffered = VecDeque::with_capacity(config.update_buffer_capacity.min(4_096));
    let (slot, tables) = loop {
        tokio::select! {
            snapshot = &mut snapshot => break snapshot?,
            update = stream.next() => {
                let update = update.context("Yellowstone stream closed during bootstrap")??;
                match update.update_oneof {
                    Some(UpdateOneof::Ping(_)) => yellowstone::send_ping(&requests)?,
                    Some(UpdateOneof::Account(update)) => {
                        ensure!(
                            buffered.len() < config.update_buffer_capacity,
                            "ALT client bootstrap update buffer full"
                        );
                        buffered.push_back(BufferedEvent::Account(update));
                    }
                    Some(UpdateOneof::Slot(update))
                        if update.status == SlotStatus::SlotConfirmed as i32 =>
                    {
                        ensure!(
                            buffered.len() < config.update_buffer_capacity,
                            "ALT client bootstrap update buffer full"
                        );
                        buffered.push_back(BufferedEvent::ConfirmedSlot(update.slot));
                    }
                    _ => {}
                }
            }
        }
    };
    let mut session = Session {
        bootstrap_slot: slot,
        confirmed_slot: slot,
        tables,
        pending: BTreeMap::new(),
        requests,
        stream,
    };
    for event in buffered {
        match event {
            BufferedEvent::Account(update) => queue_update(&mut session, update, true)?,
            BufferedEvent::ConfirmedSlot(slot) => {
                confirm(&mut session, slot)?;
            }
        }
    }
    let confirmed_slot = session.confirmed_slot;
    apply_pending_through(&mut session, confirmed_slot)?;
    Ok(session)
}

enum BufferedEvent {
    Account(SubscribeUpdateAccount),
    ConfirmedSlot(u64),
}

async fn follow(
    session: &mut Session,
    confirmed_slot: &AtomicU64,
    stop: &CancellationToken,
    stale_after: Duration,
) -> Result<()> {
    loop {
        let update = tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            update = tokio::time::timeout(stale_after, session.stream.next()) => {
                update?.context("Yellowstone stream closed")??
            }
        };
        match update.update_oneof {
            Some(UpdateOneof::Ping(_)) => yellowstone::send_ping(&session.requests)?,
            Some(UpdateOneof::Account(update)) => {
                queue_update(session, update, false)?;
            }
            Some(UpdateOneof::Slot(update))
                if update.status == SlotStatus::SlotConfirmed as i32
                    && confirm(session, update.slot)? =>
            {
                confirmed_slot.store(update.slot, Ordering::Release);
            }
            _ => {}
        }
    }
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
struct SnapshotResponse {
    context: SnapshotContext,
    value: Vec<KeyedUiAccount>,
}

#[derive(Deserialize)]
struct SnapshotContext {
    slot: u64,
}

async fn fetch_snapshot(
    http: &reqwest::Client,
    url: &str,
) -> Result<(u64, Arc<DashMap<Address, AddressLookupTableAccount>>)> {
    let response: RpcResponse<SnapshotResponse> = http
        .post(url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getProgramAccounts",
            "params": [program_id(), {"commitment":"confirmed", "encoding":"base64", "withContext":true}]
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if let Some(error) = response.error {
        anyhow::bail!("snapshot RPC error {}: {}", error.code, error.message);
    }
    let snapshot = response.result.context("snapshot RPC result missing")?;
    let tables = Arc::new(DashMap::with_capacity(snapshot.value.len()));
    for entry in snapshot.value {
        let key = Address::from(parse_key(&entry.pubkey)?);
        let data = entry
            .account
            .data
            .decode()
            .context("invalid ALT account data")?;
        let table = AddressLookupTable::deserialize(&data)
            .map_err(|_| anyhow::anyhow!("invalid ALT account"))?;
        tables.insert(
            key,
            AddressLookupTableAccount {
                key,
                addresses: table.addresses.into_owned(),
            },
        );
    }
    Ok((snapshot.context.slot, tables))
}

fn apply_update(
    tables: &DashMap<Address, AddressLookupTableAccount>,
    update: SubscribeUpdateAccount,
) -> Result<()> {
    let account = update.account.context("Yellowstone account body missing")?;
    let key = Address::try_from(account.pubkey.as_slice())?;
    if account.lamports == 0 || account.owner.as_slice() != program::id().as_ref() {
        tables.remove(&key);
        return Ok(());
    }
    let table = AddressLookupTable::deserialize(&account.data)
        .map_err(|_| anyhow::anyhow!("invalid ALT account"))?;
    tables.insert(
        key,
        AddressLookupTableAccount {
            key,
            addresses: table.addresses.into_owned(),
        },
    );
    Ok(())
}

fn queue_update(
    session: &mut Session,
    update: SubscribeUpdateAccount,
    recovering: bool,
) -> Result<()> {
    if update.slot < session.bootstrap_slot {
        return Ok(());
    }
    ensure!(
        recovering || update.slot > session.confirmed_slot,
        "account update arrived after its slot was confirmed"
    );
    session.pending.entry(update.slot).or_default().push(update);
    Ok(())
}

fn confirm(session: &mut Session, slot: u64) -> Result<bool> {
    if slot <= session.confirmed_slot {
        return Ok(false);
    }
    apply_pending_through(session, slot)?;
    session.confirmed_slot = slot;
    Ok(true)
}

fn apply_pending_through(session: &mut Session, slot: u64) -> Result<()> {
    let slots: Vec<_> = session
        .pending
        .range(..=slot)
        .map(|(slot, _)| *slot)
        .collect();
    for slot in slots {
        for update in session.pending.remove(&slot).unwrap() {
            apply_update(&session.tables, update)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_address_lookup_table_interface::state::LookupTableMeta;
    use yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo;

    fn account_update(key: Address, lamports: u64) -> SubscribeUpdateAccount {
        let table = AddressLookupTable {
            meta: LookupTableMeta::default(),
            addresses: std::borrow::Cow::Owned(vec![Address::from([9; 32])]),
        };
        SubscribeUpdateAccount {
            account: Some(SubscribeUpdateAccountInfo {
                pubkey: key.to_bytes().to_vec(),
                lamports,
                owner: program::id().to_bytes().to_vec(),
                executable: false,
                rent_epoch: 0,
                data: table.serialize_for_tests().unwrap(),
                write_version: 1,
                txn_signature: None,
            }),
            slot: 42,
            is_startup: false,
        }
    }

    #[test]
    fn yellowstone_updates_upsert_and_remove_tables() {
        let key = Address::from([1; 32]);
        let tables = DashMap::new();
        apply_update(&tables, account_update(key, 1)).unwrap();
        assert_eq!(
            tables.get(&key).unwrap().addresses,
            vec![Address::from([9; 32])]
        );
        apply_update(&tables, account_update(key, 0)).unwrap();
        assert!(!tables.contains_key(&key));
    }
}
