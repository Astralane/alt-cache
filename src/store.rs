use anyhow::{Context, Result, ensure};
use base64::{Engine, prelude::BASE64_STANDARD};
use serde::{Deserialize, Serialize};
use solana_account_decoder_client_types::{UiAccount, UiAccountData, UiAccountEncoding};
use solana_address_lookup_table_interface::{program, state::AddressLookupTable};
use std::{
    collections::{BTreeMap, HashMap},
    ops::Bound::{Excluded, Unbounded},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio::sync::broadcast;
use yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo;

pub type Key = [u8; 32];

#[derive(Clone, Deserialize, Serialize)]
pub struct KeyedUiAccount {
    pub pubkey: String,
    pub account: UiAccount,
}

pub fn program_id() -> String {
    program::id().to_string()
}
pub fn parse_key(s: &str) -> Result<Key> {
    bs58::decode(s)
        .into_vec()?
        .try_into()
        .map_err(|_| anyhow::anyhow!("expected a 32-byte key"))
}

#[derive(Clone)]
pub struct Account {
    pub slot: u64,
    pub value: SubscribeUpdateAccountInfo,
}
impl Account {
    pub fn from_yellowstone(slot: u64, value: SubscribeUpdateAccountInfo) -> Result<Self> {
        ensure!(
            value.owner.as_slice() == program::id().to_bytes(),
            "account owner is not ALT program"
        );
        ensure!(value.pubkey.len() == 32, "invalid account pubkey");
        AddressLookupTable::deserialize(&value.data)
            .map_err(|_| anyhow::anyhow!("invalid ALT data"))?;
        Ok(Self { slot, value })
    }

    pub fn from_rpc(pubkey: String, account: UiAccount, slot: u64) -> Result<Self> {
        let data = account.data.decode().context("invalid RPC account data")?;
        Self::from_yellowstone(
            slot,
            SubscribeUpdateAccountInfo {
                pubkey: parse_key(&pubkey)?.to_vec(),
                lamports: account.lamports,
                owner: parse_key(&account.owner)?.to_vec(),
                executable: account.executable,
                rent_epoch: account.rent_epoch,
                data,
                write_version: 0,
                txn_signature: None,
            },
        )
    }

    pub fn to_keyed_ui_account(&self) -> Result<KeyedUiAccount> {
        let data = zstd::stream::encode_all(self.value.data.as_slice(), 0)?;
        Ok(KeyedUiAccount {
            pubkey: bs58::encode(&self.value.pubkey).into_string(),
            account: UiAccount {
                lamports: self.value.lamports,
                data: UiAccountData::Binary(
                    BASE64_STANDARD.encode(data),
                    UiAccountEncoding::Base64Zstd,
                ),
                owner: bs58::encode(&self.value.owner).into_string(),
                executable: self.value.executable,
                rent_epoch: self.value.rent_epoch,
                space: Some(self.value.data.len() as u64),
            },
        })
    }
}

#[derive(Clone)]
pub enum Event {
    Write {
        sequence: u64,
        key: Key,
        account: Option<Arc<Account>>,
        slot: u64,
    },
    Reset,
}

pub struct Snapshot {
    pub epoch: String,
    pub sequence: u64,
    pub slot: u64,
    pub accounts: Vec<Arc<Account>>,
    pub receiver: broadcast::Receiver<Event>,
}
pub struct SnapshotPage {
    pub epoch: String,
    pub sequence: u64,
    pub slot: u64,
    pub accounts: Vec<Arc<Account>>,
    pub next: Option<Key>,
}
struct State {
    epoch: String,
    sequence: u64,
    source: Option<String>,
    ready: bool,
    slot: u64,
    baseline: u64,
    last_progress: Instant,
    accounts: BTreeMap<Key, Arc<Account>>,
    versions: HashMap<Key, (u64, u64)>,
}
pub struct Store {
    state: RwLock<State>,
    events: broadcast::Sender<Event>,
    stale_after: Duration,
    sources: RwLock<HashMap<String, (u64, Instant)>>,
}
#[derive(Clone, Serialize)]
pub struct Health {
    pub ready: bool,
    pub epoch: String,
    pub source: Option<String>,
    pub processed_slot: u64,
    pub accounts: usize,
    pub progress_age_ms: u128,
    pub sources: HashMap<String, SourceHealth>,
}
#[derive(Clone, Serialize)]
pub struct SourceHealth {
    pub slot: u64,
    pub fresh: bool,
}
impl Store {
    pub fn new(capacity: usize, stale_after: Duration) -> Self {
        Self {
            events: broadcast::channel(capacity).0,
            stale_after,
            sources: RwLock::new(HashMap::new()),
            state: RwLock::new(State {
                epoch: uuid::Uuid::new_v4().to_string(),
                sequence: 0,
                source: None,
                ready: false,
                slot: 0,
                baseline: 0,
                last_progress: Instant::now(),
                accounts: BTreeMap::new(),
                versions: HashMap::new(),
            }),
        }
    }
    pub fn invalidate(&self) {
        let mut s = self.state.write().unwrap();
        s.ready = false;
        let _ = self.events.send(Event::Reset);
    }
    pub fn install(&self, source: String, slot: u64, accounts: BTreeMap<Key, Arc<Account>>) {
        let mut s = self.state.write().unwrap();
        let _ = self.events.send(Event::Reset);
        *s = State {
            epoch: uuid::Uuid::new_v4().to_string(),
            sequence: 0,
            source: Some(source),
            ready: false,
            slot,
            baseline: slot,
            last_progress: Instant::now(),
            accounts,
            versions: HashMap::new(),
        };
    }
    pub fn progress(&self, slot: u64, target: u64) -> bool {
        let mut s = self.state.write().unwrap();
        if slot > s.slot {
            s.last_progress = Instant::now();
            s.slot = slot;
        }
        let became_ready = !s.ready && slot >= target;
        s.ready |= became_ready;
        became_ready
    }
    pub fn apply(&self, key: Key, slot: u64, version: u64, account: Option<Account>) -> bool {
        let mut s = self.state.write().unwrap();
        if slot <= s.baseline
            || s.versions
                .get(&key)
                .is_some_and(|old| *old >= (slot, version))
        {
            return false;
        }
        s.versions.insert(key, (slot, version));
        let account = account.map(Arc::new);
        if let Some(account) = &account {
            s.accounts.insert(key, account.clone());
        } else {
            s.accounts.remove(&key);
        }
        s.sequence += 1;
        let _ = self.events.send(Event::Write {
            sequence: s.sequence,
            key,
            account,
            slot,
        });
        true
    }
    fn available(&self, s: &State) -> Result<()> {
        ensure!(
            s.ready && s.last_progress.elapsed() < self.stale_after,
            "cache is not ready"
        );
        Ok(())
    }
    pub fn get(&self, key: &Key) -> Result<(String, u64, u64, Option<Arc<Account>>)> {
        let s = self.state.read().unwrap();
        self.available(&s)?;
        Ok((
            s.epoch.clone(),
            s.sequence,
            s.slot,
            s.accounts.get(key).cloned(),
        ))
    }
    pub fn snapshot(&self) -> Result<Snapshot> {
        let s = self.state.read().unwrap();
        self.available(&s)?;
        Ok(Snapshot {
            epoch: s.epoch.clone(),
            sequence: s.sequence,
            slot: s.slot,
            accounts: s.accounts.values().cloned().collect(),
            receiver: self.events.subscribe(),
        })
    }
    pub fn snapshot_page(&self, after: Option<Key>, limit: usize) -> Result<SnapshotPage> {
        ensure!(limit > 0, "snapshot page limit must be positive");
        let s = self.state.read().unwrap();
        self.available(&s)?;
        let lower = after.map_or(Unbounded, Excluded);
        let range = s.accounts.range((lower, Unbounded));
        let mut accounts: Vec<_> = range
            .take(limit + 1)
            .map(|(key, value)| (*key, value.clone()))
            .collect();
        let next = (accounts.len() > limit).then(|| accounts[limit - 1].0);
        accounts.truncate(limit);
        Ok(SnapshotPage {
            epoch: s.epoch.clone(),
            sequence: s.sequence,
            slot: s.slot,
            accounts: accounts.into_iter().map(|(_, account)| account).collect(),
            next,
        })
    }
    pub fn health(&self) -> Health {
        let s = self.state.read().unwrap();
        Health {
            ready: self.available(&s).is_ok(),
            epoch: s.epoch.clone(),
            source: s.source.clone(),
            processed_slot: s.slot,
            accounts: s.accounts.len(),
            progress_age_ms: s.last_progress.elapsed().as_millis(),
            sources: self
                .sources
                .read()
                .unwrap()
                .iter()
                .map(|(name, (slot, at))| {
                    (
                        name.clone(),
                        SourceHealth {
                            slot: *slot,
                            fresh: at.elapsed() < self.stale_after,
                        },
                    )
                })
                .collect(),
        }
    }
    pub fn source_progress(&self, source: &str, slot: u64) {
        let mut sources = self.sources.write().unwrap();
        if sources
            .get(source)
            .is_none_or(|(previous, _)| slot > *previous)
        {
            sources.insert(source.into(), (slot, Instant::now()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn account(key: Key) -> Account {
        Account {
            slot: 13,
            value: SubscribeUpdateAccountInfo {
                pubkey: key.to_vec(),
                lamports: 1,
                owner: program::id().to_bytes().to_vec(),
                executable: false,
                rent_epoch: 0,
                data: Vec::new(),
                write_version: 1,
                txn_signature: None,
            },
        }
    }
    fn store() -> Store {
        let s = Store::new(2, Duration::from_secs(10));
        s.install("one".into(), 10, BTreeMap::new());
        s.progress(12, 11);
        s
    }
    #[test]
    fn yellowstone_account_preserves_binary_data() {
        use solana_address_lookup_table_interface::state::LookupTableMeta;
        let table = AddressLookupTable {
            meta: LookupTableMeta {
                last_extended_slot: 42,
                ..Default::default()
            },
            addresses: std::borrow::Cow::Owned(vec![program::id()]),
        };
        let data = table.serialize_for_tests().unwrap();
        let account = Account::from_yellowstone(
            43,
            SubscribeUpdateAccountInfo {
                pubkey: vec![1; 32],
                lamports: 1234,
                owner: program::id().to_bytes().to_vec(),
                executable: false,
                rent_epoch: 9,
                data: data.clone(),
                write_version: 5,
                txn_signature: None,
            },
        )
        .unwrap();
        assert_eq!(account.value.data, data);
        assert_eq!(account.value.lamports, 1234);
        assert_eq!(account.value.write_version, 5);
        let rpc = account.to_keyed_ui_account().unwrap();
        assert_eq!(rpc.account.data.decode().unwrap(), account.value.data);
    }
    #[test]
    fn invalid_alt_is_rejected() {
        assert!(
            Account::from_yellowstone(
                1,
                SubscribeUpdateAccountInfo {
                    pubkey: vec![1; 32],
                    lamports: 1,
                    owner: program::id().to_bytes().to_vec(),
                    executable: false,
                    rent_epoch: 0,
                    data: Vec::new(),
                    write_version: 0,
                    txn_signature: None,
                },
            )
            .is_err()
        );
    }
    #[test]
    fn repeated_slot_does_not_extend_readiness() {
        let s = store();
        s.state.write().unwrap().last_progress = Instant::now() - Duration::from_secs(11);
        assert!(!s.progress(12, 11));
        assert!(!s.health().ready);
        assert!(s.get(&[1; 32]).is_err());
    }
    #[test]
    fn tombstones_reject_old_writes_and_baseline_replay() {
        let s = store();
        assert!(!s.apply([1; 32], 10, 100, None));
        assert!(s.apply([1; 32], 12, 2, None));
        assert!(!s.apply([1; 32], 12, 1, None));
        assert!(!s.apply([1; 32], 11, 999, None));
    }
    #[test]
    fn snapshot_and_updates_have_no_gap() {
        let s = store();
        let mut snapshot = s.snapshot().unwrap();
        assert!(s.apply([1; 32], 13, 1, None));
        assert!(matches!(
            snapshot.receiver.try_recv().unwrap(),
            Event::Write { sequence: 1, .. }
        ));
        s.invalidate();
        assert!(matches!(
            snapshot.receiver.try_recv().unwrap(),
            Event::Reset
        ));
        assert!(s.snapshot().is_err());
    }
    #[test]
    fn source_change_resets_version_domain_and_epoch() {
        let s = store();
        let epoch = s.health().epoch;
        s.apply([1; 32], 13, 999, None);
        s.install("two".into(), 10, BTreeMap::new());
        assert_ne!(epoch, s.health().epoch);
        assert!(!s.health().ready);
        assert!(s.apply([1; 32], 13, 1, None));
    }
    #[test]
    fn slow_subscribers_get_a_gap_error() {
        let s = store();
        let mut snapshot = s.snapshot().unwrap();
        for version in 0..4 {
            s.apply([1; 32], 13, version, None);
        }
        assert!(matches!(
            snapshot.receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(_))
        ));
    }
    #[test]
    fn snapshot_pages_use_exclusive_ordered_cursors() {
        let s = store();
        for value in [3, 1, 2] {
            let key = [value; 32];
            assert!(s.apply(key, 13, 1, Some(account(key))));
        }
        let first = s.snapshot_page(None, 2).unwrap();
        assert_eq!(
            first
                .accounts
                .iter()
                .map(|account| account.value.pubkey.as_slice())
                .collect::<Vec<_>>(),
            vec![[1; 32].as_slice(), [2; 32].as_slice()]
        );
        let second = s.snapshot_page(first.next, 2).unwrap();
        assert_eq!(second.sequence, first.sequence);
        assert_eq!(second.accounts.len(), 1);
        assert_eq!(second.accounts[0].value.pubkey, vec![3; 32]);
        assert!(second.next.is_none());
        assert!(!first.epoch.is_empty());
    }
}
