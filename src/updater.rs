use crate::{
    alt_state_fetcher::fetch_full_alt_state,
    config::Config,
    logging,
    state_updater::{FetchedAltState, StateUpdater},
    store::{Account, Key, Store},
    yellowstone::Event,
};
use anyhow::{Context, Result, ensure};
use solana_address_lookup_table_interface::program;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use yellowstone_grpc_proto::prelude::SubscribeUpdateAccount;

fn next_connected(active: usize, connected: &HashSet<usize>, count: usize) -> Option<usize> {
    (1..=count)
        .map(|offset| (active + offset) % count)
        .find(|source| connected.contains(source))
}

#[derive(Clone)]
enum StateEvent {
    Account {
        key: Key,
        slot: u64,
        write_version: u64,
        account: Option<Arc<Account>>,
    },
    ConfirmedSlot(u64),
}

// Events seen while a full fetch runs. Each write carries the full account, and replay ignores
// older versions anyway, so only the newest write per account is kept; confirmed slots collapse to
// the highest one. Capacity counts accounts touched, not events.
#[derive(Default)]
struct BufferedEvents {
    accounts: HashMap<Key, StateEvent>,
    confirmed_slot: Option<u64>,
}

impl BufferedEvents {
    fn push(&mut self, event: StateEvent, capacity: usize) -> Result<()> {
        let (key, version) = match &event {
            StateEvent::ConfirmedSlot(slot) => {
                self.confirmed_slot = self.confirmed_slot.max(Some(*slot));
                return Ok(());
            }
            StateEvent::Account {
                key,
                slot,
                write_version,
                ..
            } => (*key, (*slot, *write_version)),
        };
        match self.accounts.get(&key) {
            Some(StateEvent::Account {
                slot,
                write_version,
                ..
            }) if (*slot, *write_version) >= version => {}
            None => {
                ensure!(self.accounts.len() < capacity, "update buffer full");
                self.accounts.insert(key, event);
            }
            Some(_) => {
                self.accounts.insert(key, event);
            }
        }
        Ok(())
    }

    fn apply(self, updater: &mut StateUpdater) {
        for event in self.accounts.into_values() {
            apply_state_event(event, updater);
        }
        if let Some(slot) = self.confirmed_slot {
            updater.confirm(slot);
        }
    }
}

struct RefreshInProgress {
    fetch_task: JoinHandle<Result<FetchedAltState>>,
    buffered_events: BufferedEvents,
}

impl RefreshInProgress {
    fn start(client: reqwest::Client, rpc_url: String, minimum_context_slot: u64) -> Self {
        Self {
            fetch_task: tokio::spawn(async move {
                fetch_full_alt_state(&client, &rpc_url, Some(minimum_context_slot)).await
            }),
            buffered_events: BufferedEvents::default(),
        }
    }
}

impl Drop for RefreshInProgress {
    fn drop(&mut self) {
        self.fetch_task.abort();
    }
}

async fn wait_for_refresh(refresh: &mut Option<RefreshInProgress>) -> Result<FetchedAltState> {
    match refresh {
        Some(refresh) => (&mut refresh.fetch_task)
            .await
            .context("background refresh task failed")?,
        None => std::future::pending().await,
    }
}

enum UpdaterEvent {
    Shutdown,
    RefreshDue,
    RefreshFinished(Result<FetchedAltState>),
    Yellowstone(Event),
}

enum ServeOutcome {
    Restart,
    Shutdown,
}

fn apply_state_event(event: StateEvent, updater: &mut StateUpdater) {
    match event {
        StateEvent::Account {
            key,
            slot,
            write_version,
            account,
        } => {
            updater.queue(key, slot, write_version, account);
        }
        StateEvent::ConfirmedSlot(slot) => {
            updater.confirm(slot);
        }
    }
}

fn buffer_refresh_event(
    refresh: &mut Option<RefreshInProgress>,
    event: StateEvent,
    capacity: usize,
) {
    let Some(active_refresh) = refresh.as_mut() else {
        return;
    };
    if let Err(error) = active_refresh.buffered_events.push(event, capacity) {
        tracing::warn!(%error, "background ALT state refresh aborted");
        refresh.take();
    }
}

pub async fn run(
    config: Arc<Config>,
    store: Arc<Store>,
    events: mpsc::Receiver<Event>,
    stop: CancellationToken,
) -> Result<()> {
    UpdaterService::new(config, store, events, stop)?
        .run()
        .await
}

struct UpdaterService {
    config: Arc<Config>,
    store: Arc<Store>,
    events: mpsc::Receiver<Event>,
    stop: CancellationToken,
    client: reqwest::Client,
    bootstrap_rpc_url: String,
    yellowstone_urls: Vec<String>,
    connected_sources: HashSet<usize>,
    active_source: usize,
}

impl UpdaterService {
    fn new(
        config: Arc<Config>,
        store: Arc<Store>,
        events: mpsc::Receiver<Event>,
        stop: CancellationToken,
    ) -> Result<Self> {
        let bootstrap_rpc_url = config.bootstrap_rpc.resolve_url()?;
        let yellowstone_urls = config
            .grpc_sources
            .iter()
            .map(|source| source.resolve_url())
            .collect::<Result<_>>()?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?;

        Ok(Self {
            config,
            store,
            events,
            stop,
            client,
            bootstrap_rpc_url,
            yellowstone_urls,
            connected_sources: HashSet::new(),
            active_source: 0,
        })
    }

    async fn run(mut self) -> Result<()> {
        let mut updater = StateUpdater::new(self.store.clone());

        loop {
            updater.invalidate();
            if !self.wait_for_active_yellowstone().await? {
                break;
            }
            let Some(ready_updater) = self.bootstrap_alt_state().await? else {
                if self.stop.is_cancelled() {
                    break;
                }
                continue;
            };
            updater = ready_updater;

            match self.serve_alt_state(&mut updater).await? {
                ServeOutcome::Restart => continue,
                ServeOutcome::Shutdown => break,
            }
        }

        updater.invalidate();
        Ok(())
    }

    async fn wait_for_active_yellowstone(&mut self) -> Result<bool> {
        if !self.connected_sources.contains(&self.active_source) {
            self.select_next_yellowstone();
        }
        while !self.connected_sources.contains(&self.active_source) {
            let event = tokio::select! {
                _ = self.stop.cancelled() => return Ok(false),
                event = self.events.recv() => {
                    event.context("all Yellowstone sources stopped")?
                }
            };
            match event {
                Event::Connected { source } => {
                    self.connected_sources.insert(source);
                    self.active_source = source;
                }
                Event::Disconnected { source } => {
                    self.connected_sources.remove(&source);
                }
                Event::Account { .. } | Event::ConfirmedSlot { .. } => {}
            }
        }
        Ok(true)
    }

    async fn bootstrap_alt_state(&mut self) -> Result<Option<StateUpdater>> {
        let yellowstone_url = self.yellowstone_urls[self.active_source].clone();
        tracing::info!(yellowstone_url, "starting ALT state bootstrap");
        let started = Instant::now();
        let client = self.client.clone();
        let bootstrap_rpc_url = self.bootstrap_rpc_url.clone();
        let fetch_fut = fetch_full_alt_state(&client, &bootstrap_rpc_url, None);
        tokio::pin!(fetch_fut);
        let mut buffered = BufferedEvents::default();
        let fetch_result = loop {
            tokio::select! {
                _ = self.stop.cancelled() => return Ok(None),
                result = &mut fetch_fut => break result,
                event = self.events.recv() => {
                    let event = match event.context("all Yellowstone sources stopped")? {
                        Event::Connected { source } => {
                            self.connected_sources.insert(source);
                            continue;
                        }
                        Event::Disconnected { source: failed } => {
                            self.connected_sources.remove(&failed);
                            if failed == self.active_source {
                                self.select_next_yellowstone();
                                return Ok(None);
                            }
                            continue;
                        }
                        Event::Account { source, update } if source == self.active_source => {
                            match decode_account_update(*update) {
                                Ok(event) => event,
                                Err(error) => {
                                    tracing::warn!(
                                        error = %format!("{error:#}"),
                                        "active Yellowstone update failed"
                                    );
                                    self.select_next_yellowstone();
                                    return Ok(None);
                                }
                            }
                        }
                        Event::ConfirmedSlot { source, slot } if source == self.active_source => {
                            StateEvent::ConfirmedSlot(slot)
                        }
                        Event::Account { .. } | Event::ConfirmedSlot { .. } => continue,
                    };
                    // A slow fetch must retry, not exit the process: the panic hook turns an error here into exit(1).
                    if let Err(error) = buffered.push(event, self.config.stream_capacity) {
                        tracing::warn!(
                            error = %format!("{error:#}"),
                            elapsed = ?started.elapsed(),
                            "ALT cache bootstrap outran its update buffer; retrying"
                        );
                        logging::alert("ALT cache bootstrap update buffer full; retrying. Raise stream_capacity if this repeats");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        return Ok(None);
                    }
                }
            }
        };
        let fetched_alt_state = match fetch_result {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    error = %format!("{error:#}"),
                    "ALT cache bootstrap RPC failed; retrying"
                );
                logging::alert("ALT cache bootstrap RPC failed; readiness is false");
                tokio::time::sleep(Duration::from_secs(2)).await;
                return Ok(None);
            }
        };
        let context_slot = fetched_alt_state.context_slot;
        let mut updater = StateUpdater::from_fetched_alt_state(
            self.store.clone(),
            yellowstone_url.clone(),
            fetched_alt_state,
        );

        buffered.apply(&mut updater);
        updater.finish_recovery();
        tracing::info!(yellowstone_url, context_slot, elapsed = ?started.elapsed(), "cache ready");
        Ok(Some(updater))
    }

    async fn serve_alt_state(&mut self, updater: &mut StateUpdater) -> Result<ServeOutcome> {
        let yellowstone_url = self.yellowstone_urls[self.active_source].clone();
        let mut refresh_timer =
            tokio::time::interval(Duration::from_secs(self.config.full_refresh_interval_secs));
        refresh_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        refresh_timer.tick().await;
        let mut refresh = None;

        loop {
            let action = tokio::select! {
                _ = self.stop.cancelled() => UpdaterEvent::Shutdown,
                _ = refresh_timer.tick() => UpdaterEvent::RefreshDue,
                result = wait_for_refresh(&mut refresh) => {
                    UpdaterEvent::RefreshFinished(result)
                }
                event = self.events.recv() => UpdaterEvent::Yellowstone(
                    event.context("all Yellowstone sources stopped")?
                ),
            };

            match action {
                UpdaterEvent::Shutdown => return Ok(ServeOutcome::Shutdown),
                UpdaterEvent::RefreshDue => {
                    if refresh.is_none() {
                        tracing::info!(yellowstone_url, "starting background ALT state refresh");
                        refresh = Some(RefreshInProgress::start(
                            self.client.clone(),
                            self.bootstrap_rpc_url.clone(),
                            updater.confirmed_slot(),
                        ));
                    }
                }
                UpdaterEvent::RefreshFinished(result) => {
                    self.install_refreshed_alt_state(
                        result,
                        &mut refresh,
                        updater,
                        &yellowstone_url,
                    );
                }
                UpdaterEvent::Yellowstone(Event::Connected { source }) => {
                    self.connected_sources.insert(source);
                }
                UpdaterEvent::Yellowstone(Event::Disconnected { source: failed }) => {
                    self.connected_sources.remove(&failed);
                    if failed == self.active_source {
                        refresh.take();
                        self.select_next_yellowstone();
                        logging::alert(format!(
                            "ALT cache source {yellowstone_url} failed; readiness is false"
                        ));
                        return Ok(ServeOutcome::Restart);
                    }
                }
                UpdaterEvent::Yellowstone(Event::Account {
                    source: event_source,
                    update,
                }) => {
                    if event_source == self.active_source {
                        let event = match decode_account_update(*update) {
                            Ok(event) => event,
                            Err(error) => {
                                refresh.take();
                                tracing::warn!(
                                    error = %format!("{error:#}"),
                                    "active Yellowstone update failed"
                                );
                                self.select_next_yellowstone();
                                return Ok(ServeOutcome::Restart);
                            }
                        };
                        apply_state_event(event.clone(), updater);
                        buffer_refresh_event(&mut refresh, event, self.config.stream_capacity);
                    }
                }
                UpdaterEvent::Yellowstone(Event::ConfirmedSlot {
                    source: event_source,
                    slot,
                }) => {
                    if event_source == self.active_source {
                        let event = StateEvent::ConfirmedSlot(slot);
                        apply_state_event(event.clone(), updater);
                        buffer_refresh_event(&mut refresh, event, self.config.stream_capacity);
                    }
                }
            }
        }
    }

    fn install_refreshed_alt_state(
        &self,
        result: Result<FetchedAltState>,
        refresh: &mut Option<RefreshInProgress>,
        updater: &mut StateUpdater,
        yellowstone_url: &str,
    ) {
        let Some(mut completed) = refresh.take() else {
            return;
        };
        let fetched_alt_state = match result {
            Ok(fetched) => fetched,
            Err(error) => {
                tracing::warn!(
                    error = %format!("{error:#}"),
                    "background ALT state refresh failed"
                );
                return;
            }
        };
        let context_slot = fetched_alt_state.context_slot;
        let mut refreshed = StateUpdater::from_fetched_alt_state(
            self.store.clone(),
            yellowstone_url.to_owned(),
            fetched_alt_state,
        );
        std::mem::take(&mut completed.buffered_events).apply(&mut refreshed);
        refreshed.finish_recovery();
        *updater = refreshed;
        tracing::info!(
            yellowstone_url,
            context_slot,
            "background ALT state refresh installed"
        );
    }

    fn select_next_yellowstone(&mut self) {
        if let Some(source) = next_connected(
            self.active_source,
            &self.connected_sources,
            self.yellowstone_urls.len(),
        ) {
            self.active_source = source;
        }
    }
}

fn decode_account_update(update: SubscribeUpdateAccount) -> Result<StateEvent> {
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
        match Account::from_yellowstone(account) {
            Ok(account) => Some(Arc::new(account)),
            Err(error) => {
                let pubkey = bs58::encode(key).into_string();
                tracing::warn!(%pubkey, %error, "removing invalid ALT account");
                None
            }
        }
    };
    Ok(StateEvent::Account {
        key,
        slot: update.slot,
        write_version,
        account: value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo;

    fn account_event(key: u8, slot: u64, write_version: u64) -> StateEvent {
        StateEvent::Account {
            key: [key; 32],
            slot,
            write_version,
            account: Some(Arc::new(Account {
                value: SubscribeUpdateAccountInfo {
                    pubkey: vec![key; 32],
                    data: vec![key, slot as u8],
                    ..Default::default()
                },
            })),
        }
    }

    fn recovered(events: impl FnOnce(&mut StateUpdater)) -> (u64, Vec<(Key, Vec<u8>)>) {
        let store = Arc::new(Store::new());
        let mut updater = StateUpdater::from_fetched_alt_state(
            store.clone(),
            "test".into(),
            FetchedAltState {
                context_slot: 10,
                accounts: BTreeMap::new(),
            },
        );
        events(&mut updater);
        updater.finish_recovery();
        let snapshot = store.read_snapshot().unwrap();
        let accounts = snapshot
            .accounts
            .iter()
            .map(|(key, account)| (*key, account.value().data.clone()))
            .collect();
        (snapshot.slot, accounts)
    }

    #[test]
    fn collapsed_confirmations_replay_to_the_same_state() {
        let events = || {
            vec![
                account_event(1, 11, 1),
                StateEvent::ConfirmedSlot(11),
                // Arrives after its slot was confirmed.
                account_event(2, 11, 1),
                StateEvent::ConfirmedSlot(12),
                account_event(1, 12, 2),
                // Its slot is never confirmed, but the write is already confirmed-commitment data.
                account_event(3, 14, 1),
                StateEvent::ConfirmedSlot(13),
                StateEvent::ConfirmedSlot(9),
            ]
        };
        let sequential = recovered(|updater| {
            for event in events() {
                apply_state_event(event, updater);
            }
        });
        let collapsed = recovered(|updater| {
            let mut buffered = BufferedEvents::default();
            for event in events() {
                buffered.push(event, 16).unwrap();
            }
            buffered.apply(updater);
        });

        assert_eq!(collapsed, sequential);
        assert_eq!(collapsed.0, 13);
        assert_eq!(collapsed.1.len(), 3);
    }

    #[test]
    fn only_distinct_accounts_use_buffer_capacity() {
        let mut buffered = BufferedEvents::default();
        for slot in 0..10_000 {
            buffered.push(StateEvent::ConfirmedSlot(slot), 2).unwrap();
        }
        buffered.push(account_event(1, 11, 1), 2).unwrap();
        buffered.push(account_event(2, 11, 1), 2).unwrap();
        // More writes to accounts already held replace them, or are ignored if older.
        buffered.push(account_event(1, 12, 1), 2).unwrap();
        buffered.push(account_event(1, 11, 9), 2).unwrap();

        assert!(buffered.push(account_event(3, 11, 1), 2).is_err());
        assert_eq!(buffered.confirmed_slot, Some(9_999));
        assert!(matches!(
            buffered.accounts[&[1; 32]],
            StateEvent::Account { slot: 12, .. }
        ));
    }

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
