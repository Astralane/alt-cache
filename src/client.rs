use crate::{
    proto::{SnapshotRequest, alt_snapshot_client::AltSnapshotClient},
    yellowstone,
};
use anyhow::{Context, Result, ensure};
use arc_swap::ArcSwap;
use dashmap::{DashMap, mapref::entry::Entry};
use futures::StreamExt;
use serde::Deserialize;
use solana_address::Address;
use solana_address_lookup_table_interface::{program, state::AddressLookupTable};
use solana_message::AddressLookupTableAccount;
use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tonic::{Streaming, transport::Endpoint};
use yellowstone_grpc_proto::prelude::{
    SlotStatus, SubscribeRequest, SubscribeUpdate, SubscribeUpdateAccount,
    subscribe_update::UpdateOneof,
};

const RETRY_DELAY: Duration = Duration::from_secs(2);
const SNAPSHOT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const YELLOWSTONE_IDLE_TIMEOUT: Duration = Duration::from_secs(15);

/// Bootstrap and live-source endpoints used to initialize an ALT cache.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AltConfig {
    pub snapshot_grpc: Vec<String>,
    pub yellowstone_grpc: Vec<YellowstoneGrpcConfig>,
}

/// One Yellowstone endpoint and its optional authentication token.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YellowstoneGrpcConfig {
    pub url: String,
    pub token: Option<String>,
}

impl YellowstoneGrpcConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            token: None,
        }
    }

    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
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
    pub async fn connect(config: AltConfig) -> Result<Self> {
        ensure!(
            !config.snapshot_grpc.is_empty(),
            "at least one snapshot gRPC URL is required"
        );
        ensure!(
            !config.yellowstone_grpc.is_empty(),
            "at least one Yellowstone source is required"
        );
        let mut snapshot_urls = HashSet::new();
        ensure!(
            config
                .snapshot_grpc
                .iter()
                .all(|url| !url.is_empty() && snapshot_urls.insert(url)),
            "snapshot gRPC URLs must be non-empty and unique"
        );
        let mut yellowstone_urls = HashSet::new();
        ensure!(
            config
                .yellowstone_grpc
                .iter()
                .all(|source| !source.url.is_empty() && yellowstone_urls.insert(&source.url)),
            "Yellowstone source URLs must be non-empty and unique"
        );
        let session = establish_session(&config, 0).await?;
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
        let task = tokio::spawn(run(config, session, tables, ready, confirmed_slot, stop));
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
    source_index: usize,
    confirmed_slot: u64,
    tables: Arc<DashMap<Address, AddressLookupTableAccount>>,
    requests: mpsc::Sender<SubscribeRequest>,
    stream: Streaming<SubscribeUpdate>,
}

async fn run(
    config: AltConfig,
    mut session: Session,
    tables: Arc<ArcSwap<DashMap<Address, AddressLookupTableAccount>>>,
    ready: Arc<AtomicBool>,
    confirmed_slot: Arc<AtomicU64>,
    stop: CancellationToken,
) {
    loop {
        let result = follow(
            &mut session,
            &confirmed_slot,
            &stop,
            YELLOWSTONE_IDLE_TIMEOUT,
        )
        .await;
        if stop.is_cancelled() {
            return;
        }
        ready.store(false, Ordering::Release);
        tracing::warn!(
            source = session.source_index,
            error = ?result.err(),
            "ALT client Yellowstone feed disconnected"
        );
        let next_source = (session.source_index + 1) % config.yellowstone_grpc.len();
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(RETRY_DELAY) => {}
            }
            match establish_session(&config, next_source).await {
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

async fn establish_session(config: &AltConfig, start_source_index: usize) -> Result<Session> {
    let mut last_error = None;
    for offset in 0..config.yellowstone_grpc.len() {
        let source_index = (start_source_index + offset) % config.yellowstone_grpc.len();
        match establish_session_from_source(config, source_index).await {
            Ok(session) => return Ok(session),
            Err(error) => {
                tracing::warn!(
                    source = source_index,
                    ?error,
                    "ALT client source unavailable"
                );
                last_error = Some(error);
            }
        }
    }
    Err(last_error.context("all Yellowstone sources failed")?)
}

async fn establish_session_from_source(config: &AltConfig, source_index: usize) -> Result<Session> {
    let source = &config.yellowstone_grpc[source_index];
    let mut grpc = yellowstone::connect(&source.url, source.token.as_deref()).await?;
    let (requests, receiver) = mpsc::channel(8);
    requests.send(yellowstone::subscribe_request()).await?;
    let mut stream = grpc
        .geyser
        .subscribe(tokio_stream::wrappers::ReceiverStream::new(receiver))
        .await?
        .into_inner();
    let tables = Arc::new(DashMap::new());
    // Live updates apply as they arrive. Tables only grow until closed, so the older snapshot
    // filling the same map can't undo them, and nothing needs to be held back.
    let mut confirmed_slot = 0;
    let snapshot_slot = {
        let snapshot = fetch_snapshot_from_any(&config.snapshot_grpc, &tables);
        tokio::pin!(snapshot);
        loop {
            tokio::select! {
                snapshot = &mut snapshot => break snapshot?,
                update = stream.next() => {
                    let update = update.context("Yellowstone stream closed during bootstrap")??;
                    match update.update_oneof {
                        Some(UpdateOneof::Ping(_)) => yellowstone::send_ping(&requests)?,
                        Some(UpdateOneof::Account(update)) => apply_update(&tables, update)?,
                        Some(UpdateOneof::Slot(update))
                            if update.status == SlotStatus::SlotConfirmed as i32 =>
                        {
                            confirmed_slot = confirmed_slot.max(update.slot);
                        }
                        _ => {}
                    }
                }
            }
        }
    };
    Ok(Session {
        source_index,
        confirmed_slot: snapshot_slot.max(confirmed_slot),
        tables,
        requests,
        stream,
    })
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
            Some(UpdateOneof::Account(update)) => apply_update(&session.tables, update)?,
            Some(UpdateOneof::Slot(update))
                if update.status == SlotStatus::SlotConfirmed as i32
                    && update.slot > session.confirmed_slot =>
            {
                session.confirmed_slot = update.slot;
                confirmed_slot.store(update.slot, Ordering::Release);
            }
            _ => {}
        }
    }
}

async fn fetch_snapshot(
    url: &str,
    tables: &DashMap<Address, AddressLookupTableAccount>,
) -> Result<u64> {
    let started = Instant::now();
    let channel = Endpoint::from_shared(url.to_owned())?
        .connect_timeout(SNAPSHOT_CONNECT_TIMEOUT)
        .http2_adaptive_window(true)
        .connect()
        .await?;
    let mut client = AltSnapshotClient::new(channel);
    let mut stream = client
        .stream_snapshot(SnapshotRequest {})
        .await?
        .into_inner();
    let mut download = SnapshotDownload::new(tables);
    while let Some(chunk) = stream.message().await? {
        download.push(chunk)?;
    }
    let snapshot = download.finish()?;
    tracing::info!(
        snapshot_slot = snapshot.slot,
        chunks = snapshot.chunks,
        accounts = snapshot.accounts,
        payload_bytes = snapshot.payload_bytes,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "ALT client snapshot fetched"
    );
    Ok(snapshot.slot)
}

struct SnapshotDownload<'a> {
    tables: &'a DashMap<Address, AddressLookupTableAccount>,
    slot: Option<u64>,
    total_accounts: Option<u64>,
    received: u64,
    chunks: u64,
    payload_bytes: u64,
}

struct CompletedSnapshot {
    slot: u64,
    accounts: u64,
    chunks: u64,
    payload_bytes: u64,
}

impl<'a> SnapshotDownload<'a> {
    fn new(tables: &'a DashMap<Address, AddressLookupTableAccount>) -> Self {
        Self {
            tables,
            slot: None,
            total_accounts: None,
            received: 0,
            chunks: 0,
            payload_bytes: 0,
        }
    }

    fn push(&mut self, chunk: crate::proto::SnapshotChunk) -> Result<()> {
        self.chunks += 1;
        let first_slot = *self.slot.get_or_insert(chunk.confirmed_slot);
        ensure!(
            chunk.confirmed_slot == first_slot,
            "snapshot slot changed from {first_slot} to {}",
            chunk.confirmed_slot
        );
        let expected_accounts = *self.total_accounts.get_or_insert(chunk.total_accounts);
        ensure!(
            chunk.total_accounts == expected_accounts,
            "snapshot account count changed from {expected_accounts} to {}",
            chunk.total_accounts
        );
        for account in chunk.accounts {
            self.received += 1;
            self.payload_bytes += (account.pubkey.len() + account.data.len()) as u64;
            let key = Address::try_from(account.pubkey.as_slice())?;
            let table = AddressLookupTable::deserialize(&account.data)
                .map_err(|_| anyhow::anyhow!("invalid ALT account"))?;
            insert_unless_older(self.tables, key, table.addresses.into_owned());
        }
        Ok(())
    }

    fn finish(self) -> Result<CompletedSnapshot> {
        let slot = self.slot.context("snapshot gRPC stream was empty")?;
        let total_accounts = self.total_accounts.unwrap();
        // Counted, not taken from the map: live updates may have added tables to it meanwhile.
        ensure!(
            self.received == total_accounts,
            "snapshot ended after {} of {total_accounts} accounts",
            self.received
        );
        Ok(CompletedSnapshot {
            slot,
            accounts: self.received,
            chunks: self.chunks,
            payload_bytes: self.payload_bytes,
        })
    }
}

async fn fetch_snapshot_from_any(
    urls: &[String],
    tables: &DashMap<Address, AddressLookupTableAccount>,
) -> Result<u64> {
    let mut last_error = None;
    for (index, url) in urls.iter().enumerate() {
        match fetch_snapshot(url, tables).await {
            Ok(snapshot) => return Ok(snapshot),
            Err(error) => {
                tracing::warn!(
                    bootstrap = index,
                    error = %format!("{error:#}"),
                    "ALT client bootstrap unavailable"
                );
                last_error = Some(error);
            }
        }
    }
    Err(last_error.context("all snapshot gRPC URLs failed")?)
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
    insert_unless_older(tables, key, table.addresses.into_owned());
    Ok(())
}

/// Lookup tables only grow until closed, so a shorter copy is older: late, replayed or snapshot
/// writes never replace a newer cached table.
fn insert_unless_older(
    tables: &DashMap<Address, AddressLookupTableAccount>,
    key: Address,
    addresses: Vec<Address>,
) {
    match tables.entry(key) {
        Entry::Occupied(cached) if cached.get().addresses.len() >= addresses.len() => {}
        entry => {
            entry.insert(AddressLookupTableAccount { key, addresses });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{AltAccount, SnapshotChunk};
    use solana_address_lookup_table_interface::state::LookupTableMeta;
    use yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo;

    fn table_data(addresses: &[Address]) -> Vec<u8> {
        AddressLookupTable {
            meta: LookupTableMeta::default(),
            addresses: std::borrow::Cow::Owned(addresses.to_vec()),
        }
        .serialize_for_tests()
        .unwrap()
    }

    fn account_update(
        key: Address,
        lamports: u64,
        addresses: &[Address],
    ) -> SubscribeUpdateAccount {
        SubscribeUpdateAccount {
            account: Some(SubscribeUpdateAccountInfo {
                pubkey: key.to_bytes().to_vec(),
                lamports,
                owner: program::id().to_bytes().to_vec(),
                executable: false,
                rent_epoch: 0,
                data: table_data(addresses),
                write_version: 1,
                txn_signature: None,
            }),
            slot: 42,
            is_startup: false,
        }
    }

    #[test]
    fn config_accepts_multiple_snapshot_urls() {
        let config = AltConfig {
            snapshot_grpc: vec!["http://primary".into(), "http://secondary".into()],
            yellowstone_grpc: vec![YellowstoneGrpcConfig {
                url: "https://yellowstone".into(),
                token: Some("token".into()),
            }],
        };
        assert_eq!(
            config.snapshot_grpc,
            vec!["http://primary", "http://secondary"]
        );
        assert_eq!(config.yellowstone_grpc[0].url, "https://yellowstone");
        assert_eq!(config.yellowstone_grpc[0].token.as_deref(), Some("token"));
    }

    #[test]
    fn alt_config_deserializes() {
        let config: AltConfig = toml::from_str(
            r#"
                snapshot_grpc = ["http://primary", "http://secondary"]

                [[yellowstone_grpc]]
                url = "https://yellowstone-primary"
                token = "secret"

                [[yellowstone_grpc]]
                url = "https://yellowstone-secondary"
            "#,
        )
        .unwrap();

        assert_eq!(config.snapshot_grpc.len(), 2);
        assert_eq!(config.yellowstone_grpc.len(), 2);
        assert_eq!(config.yellowstone_grpc[0].token.as_deref(), Some("secret"));
        assert_eq!(config.yellowstone_grpc[1].token, None);
    }

    #[test]
    fn snapshot_download_decodes_and_validates_the_stream() {
        let tables = DashMap::new();
        let mut download = SnapshotDownload::new(&tables);
        download
            .push(SnapshotChunk {
                confirmed_slot: 42,
                total_accounts: 1,
                accounts: vec![AltAccount {
                    pubkey: vec![1; 32],
                    data: table_data(&[Address::from([9; 32])]),
                }],
            })
            .unwrap();
        let snapshot = download.finish().unwrap();
        assert_eq!(snapshot.slot, 42);
        assert_eq!((snapshot.chunks, snapshot.accounts), (1, 1));
        assert_eq!(
            tables.get(&Address::from([1; 32])).unwrap().addresses,
            vec![Address::from([9; 32])]
        );
    }

    #[test]
    fn a_snapshot_never_shrinks_a_table_written_live_meanwhile() {
        let (key, created) = (Address::from([1; 32]), Address::from([2; 32]));
        let (a, b) = (Address::from([8; 32]), Address::from([9; 32]));
        let tables = DashMap::new();
        // Live updates land in the same map while the snapshot downloads.
        apply_update(&tables, account_update(key, 1, &[a, b])).unwrap();
        apply_update(&tables, account_update(created, 1, &[a])).unwrap();
        let mut download = SnapshotDownload::new(&tables);
        download
            .push(SnapshotChunk {
                confirmed_slot: 42,
                total_accounts: 1,
                accounts: vec![AltAccount {
                    pubkey: key.to_bytes().to_vec(),
                    data: table_data(&[a]),
                }],
            })
            .unwrap();

        // Complete even though the map holds more tables than the snapshot sent.
        assert_eq!(download.finish().unwrap().accounts, 1);
        assert_eq!(tables.get(&key).unwrap().addresses, vec![a, b]);
        assert_eq!(tables.get(&created).unwrap().addresses, vec![a]);
    }

    #[test]
    fn late_or_replayed_updates_never_shrink_a_table() {
        let key = Address::from([1; 32]);
        let (a, b) = (Address::from([8; 32]), Address::from([9; 32]));
        let tables = DashMap::new();
        apply_update(&tables, account_update(key, 1, &[a, b])).unwrap();
        apply_update(&tables, account_update(key, 1, &[a])).unwrap();
        assert_eq!(tables.get(&key).unwrap().addresses, vec![a, b]);
    }

    #[test]
    fn yellowstone_updates_upsert_and_remove_tables() {
        let key = Address::from([1; 32]);
        let tables = DashMap::new();
        apply_update(&tables, account_update(key, 1, &[Address::from([9; 32])])).unwrap();
        assert_eq!(
            tables.get(&key).unwrap().addresses,
            vec![Address::from([9; 32])]
        );
        apply_update(&tables, account_update(key, 0, &[Address::from([9; 32])])).unwrap();
        assert!(!tables.contains_key(&key));
    }
}
