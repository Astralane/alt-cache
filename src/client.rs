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
    collections::{BTreeMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tonic::Streaming;
use yellowstone_grpc_proto::prelude::{
    SlotStatus, SubscribeRequest, SubscribeUpdate, SubscribeUpdateAccount,
    subscribe_update::UpdateOneof,
};

const RETRY_DELAY: Duration = Duration::from_secs(2);
const YELLOWSTONE_IDLE_TIMEOUT: Duration = Duration::from_secs(15);
const UPDATE_BUFFER_CAPACITY: usize = 4_096;
const SNAPSHOT_PAGE_SIZE: usize = 100_000;

/// Bootstrap and live-source endpoints used to initialize an ALT cache.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AltConfig {
    pub rpc: Vec<String>,
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
        ensure!(!config.rpc.is_empty(), "at least one RPC URL is required");
        ensure!(
            !config.yellowstone_grpc.is_empty(),
            "at least one Yellowstone source is required"
        );
        let mut rpc_urls = HashSet::new();
        ensure!(
            config
                .rpc
                .iter()
                .all(|url| !url.is_empty() && rpc_urls.insert(url)),
            "RPC URLs must be non-empty and unique"
        );
        let mut yellowstone_urls = HashSet::new();
        ensure!(
            config
                .yellowstone_grpc
                .iter()
                .all(|source| !source.url.is_empty() && yellowstone_urls.insert(&source.url)),
            "Yellowstone source URLs must be non-empty and unique"
        );
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?;
        let session = recover(&http, &config, 0).await?;
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
    source_index: usize,
    bootstrap_slot: u64,
    confirmed_slot: u64,
    tables: Arc<DashMap<Address, AddressLookupTableAccount>>,
    pending: BTreeMap<u64, Vec<SubscribeUpdateAccount>>,
    requests: mpsc::Sender<SubscribeRequest>,
    stream: Streaming<SubscribeUpdate>,
}

async fn run(
    http: reqwest::Client,
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
            match recover(&http, &config, next_source).await {
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

async fn recover(
    http: &reqwest::Client,
    config: &AltConfig,
    first_source: usize,
) -> Result<Session> {
    let mut last_error = None;
    for offset in 0..config.yellowstone_grpc.len() {
        let source_index = (first_source + offset) % config.yellowstone_grpc.len();
        match recover_from_source(http, config, source_index).await {
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

async fn recover_from_source(
    http: &reqwest::Client,
    config: &AltConfig,
    source_index: usize,
) -> Result<Session> {
    let source = &config.yellowstone_grpc[source_index];
    let mut grpc = yellowstone::connect(&source.url, source.token.as_deref()).await?;
    let (requests, receiver) = mpsc::channel(8);
    requests.send(yellowstone::subscribe_request()).await?;
    let mut stream = grpc
        .geyser
        .subscribe(tokio_stream::wrappers::ReceiverStream::new(receiver))
        .await?
        .into_inner();
    let snapshot = fetch_snapshot_from_any(http, &config.rpc);
    tokio::pin!(snapshot);
    let mut buffered = VecDeque::with_capacity(UPDATE_BUFFER_CAPACITY);
    let (slot, tables) = loop {
        tokio::select! {
            snapshot = &mut snapshot => break snapshot?,
            update = stream.next() => {
                let update = update.context("Yellowstone stream closed during bootstrap")??;
                match update.update_oneof {
                    Some(UpdateOneof::Ping(_)) => yellowstone::send_ping(&requests)?,
                    Some(UpdateOneof::Account(update)) => {
                        ensure!(
                            buffered.len() < UPDATE_BUFFER_CAPACITY,
                            "ALT client bootstrap update buffer full"
                        );
                        buffered.push_back(BufferedEvent::Account(update));
                    }
                    Some(UpdateOneof::Slot(update))
                        if update.status == SlotStatus::SlotConfirmed as i32 =>
                    {
                        ensure!(
                            buffered.len() < UPDATE_BUFFER_CAPACITY,
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
        source_index,
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
    value: SnapshotPage,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotPage {
    accounts: Vec<KeyedUiAccount>,
    pagination_key: Option<String>,
}

#[derive(Deserialize)]
struct SnapshotContext {
    slot: u64,
}

async fn fetch_snapshot(
    http: &reqwest::Client,
    url: &str,
) -> Result<(u64, Arc<DashMap<Address, AddressLookupTableAccount>>)> {
    let started = Instant::now();
    let tables = Arc::new(DashMap::with_capacity(SNAPSHOT_PAGE_SIZE));
    let mut snapshot_slot = None;
    let mut pagination_key: Option<String> = None;
    let mut seen = HashSet::new();
    let mut pages = 0_u64;
    let mut response_bytes = 0_u64;
    loop {
        let response = http
            .post(url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getProgramAccountsV2",
                "params": [program_id(), {
                    "commitment": "confirmed",
                    "encoding": "base64+zstd",
                    "withContext": true,
                    "limit": SNAPSHOT_PAGE_SIZE,
                    "paginationKey": pagination_key.as_deref(),
                }]
            }))
            .send()
            .await
            .map_err(redact_rpc_error)?
            .error_for_status()
            .map_err(redact_rpc_error)?;
        let body = response.bytes().await.map_err(redact_rpc_error)?;
        pages += 1;
        response_bytes += body.len() as u64;
        let response: RpcResponse<SnapshotResponse> = serde_json::from_slice(&body)?;
        drop(body);
        if let Some(error) = response.error {
            anyhow::bail!("snapshot RPC error {}: {}", error.code, error.message);
        }
        let snapshot = response.result.context("snapshot RPC result missing")?;
        let slot = snapshot.context.slot;
        let first_slot = *snapshot_slot.get_or_insert(slot);
        ensure!(
            slot == first_slot,
            "snapshot slot changed from {first_slot} to {slot}"
        );
        let page = snapshot.value;
        for entry in page.accounts {
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
        let Some(next) = page.pagination_key else {
            break;
        };
        ensure!(
            seen.insert(next.clone()),
            "snapshot RPC repeated its pagination key"
        );
        pagination_key = Some(next);
    }
    let snapshot_slot = snapshot_slot.context("snapshot RPC context missing")?;
    tracing::info!(
        snapshot_slot,
        pages,
        accounts = tables.len(),
        response_bytes,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "ALT client snapshot fetched"
    );
    Ok((snapshot_slot, tables))
}

async fn fetch_snapshot_from_any(
    http: &reqwest::Client,
    urls: &[String],
) -> Result<(u64, Arc<DashMap<Address, AddressLookupTableAccount>>)> {
    let mut last_error = None;
    for (index, url) in urls.iter().enumerate() {
        match fetch_snapshot(http, url).await {
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
    Err(last_error.context("all bootstrap URLs failed")?)
}

fn redact_rpc_error(error: reqwest::Error) -> anyhow::Error {
    anyhow::Error::new(error.without_url())
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
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
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
    fn config_accepts_multiple_rpc_urls() {
        let config = AltConfig {
            rpc: vec!["http://primary".into(), "http://secondary".into()],
            yellowstone_grpc: vec![YellowstoneGrpcConfig {
                url: "https://yellowstone".into(),
                token: Some("token".into()),
            }],
        };
        assert_eq!(config.rpc, vec!["http://primary", "http://secondary"]);
        assert_eq!(config.yellowstone_grpc[0].url, "https://yellowstone");
        assert_eq!(config.yellowstone_grpc[0].token.as_deref(), Some("token"));
    }

    #[test]
    fn alt_config_deserializes() {
        let config: AltConfig = toml::from_str(
            r#"
                rpc = ["http://primary", "http://secondary"]

                [[yellowstone_grpc]]
                url = "https://yellowstone-primary"
                token = "secret"

                [[yellowstone_grpc]]
                url = "https://yellowstone-secondary"
            "#,
        )
        .unwrap();

        assert_eq!(config.rpc.len(), 2);
        assert_eq!(config.yellowstone_grpc.len(), 2);
        assert_eq!(config.yellowstone_grpc[0].token.as_deref(), Some("secret"));
        assert_eq!(config.yellowstone_grpc[1].token, None);
    }

    #[tokio::test]
    async fn snapshot_falls_back_to_next_bootstrap_url() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for (page, next) in [Some("next"), None].into_iter().enumerate() {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 4096];
                let bytes_read = socket.read(&mut request).await.unwrap();
                assert!(bytes_read > 0);
                let request = String::from_utf8_lossy(&request[..bytes_read]);
                assert!(request.contains("getProgramAccountsV2"));
                assert!(request.contains("base64+zstd"));
                assert!(request.contains("100000"));
                if page == 1 {
                    assert!(request.contains(r#""paginationKey":"next""#));
                }
                let body = json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 42},
                        "value": {"accounts": [], "paginationKey": next},
                    },
                })
                .to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let urls = vec!["not a URL".to_owned(), format!("http://{address}")];
        let http = reqwest::Client::new();

        let (slot, tables) = fetch_snapshot_from_any(&http, &urls).await.unwrap();

        assert_eq!(slot, 42);
        assert!(tables.is_empty());
        server.await.unwrap();
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
