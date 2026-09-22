use anyhow::{Context, Result, ensure};
use arc_swap::ArcSwap;
use base64::{Engine, prelude::BASE64_STANDARD};
use im::OrdMap;
use serde::{Deserialize, Serialize};
use solana_account_decoder_client_types::{
    UiAccount, UiAccountData, UiAccountEncoding, UiDataSliceConfig,
};
use solana_address_lookup_table_interface::{program, state::AddressLookupTable};
use std::{
    collections::{BTreeMap, HashMap},
    ops::Bound::{Excluded, Unbounded},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
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
    pub(crate) value: SubscribeUpdateAccountInfo,
}
impl Account {
    pub fn value(&self) -> &SubscribeUpdateAccountInfo {
        &self.value
    }

    pub fn from_yellowstone(value: SubscribeUpdateAccountInfo) -> Result<Self> {
        ensure!(
            value.owner.as_slice() == program::id().to_bytes(),
            "account owner is not ALT program"
        );
        ensure!(value.pubkey.len() == 32, "invalid account pubkey");
        AddressLookupTable::deserialize(&value.data)
            .map_err(|_| anyhow::anyhow!("invalid ALT data"))?;
        Ok(Self { value })
    }

    pub fn from_rpc(pubkey: String, account: UiAccount) -> Result<Self> {
        let data = account.data.decode().context("invalid RPC account data")?;
        Self::from_yellowstone(SubscribeUpdateAccountInfo {
            pubkey: parse_key(&pubkey)?.to_vec(),
            lamports: account.lamports,
            owner: parse_key(&account.owner)?.to_vec(),
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data,
            write_version: 0,
            txn_signature: None,
        })
    }

    pub fn to_keyed_ui_account(&self) -> Result<KeyedUiAccount> {
        self.to_keyed_ui_account_with_config(UiAccountEncoding::Base64Zstd, None)
    }

    pub fn to_keyed_ui_account_with_config(
        &self,
        encoding: UiAccountEncoding,
        data_slice: Option<UiDataSliceConfig>,
    ) -> Result<KeyedUiAccount> {
        let data = if let Some(slice) = data_slice {
            let start = slice.offset.min(self.value.data.len());
            let end = start
                .saturating_add(slice.length)
                .min(self.value.data.len());
            &self.value.data[start..end]
        } else {
            &self.value.data
        };
        let data = match encoding {
            UiAccountEncoding::Binary => {
                UiAccountData::LegacyBinary(bs58::encode(data).into_string())
            }
            UiAccountEncoding::Base58 => {
                UiAccountData::Binary(bs58::encode(data).into_string(), encoding)
            }
            UiAccountEncoding::Base64 => {
                UiAccountData::Binary(BASE64_STANDARD.encode(data), encoding)
            }
            UiAccountEncoding::Base64Zstd => UiAccountData::Binary(
                BASE64_STANDARD.encode(zstd::stream::encode_all(data, 0)?),
                encoding,
            ),
            UiAccountEncoding::JsonParsed => {
                anyhow::bail!("jsonParsed account encoding is unsupported")
            }
        };
        Ok(KeyedUiAccount {
            pubkey: bs58::encode(&self.value.pubkey).into_string(),
            account: UiAccount {
                lamports: self.value.lamports,
                data,
                owner: bs58::encode(&self.value.owner).into_string(),
                executable: self.value.executable,
                rent_epoch: self.value.rent_epoch,
                space: Some(self.value.data.len() as u64),
            },
        })
    }
}

pub(crate) struct ReadSnapshot {
    pub slot: u64,
    pub accounts: OrdMap<Key, Arc<Account>>,
}
pub(crate) struct SnapshotPage {
    pub slot: u64,
    pub accounts: Vec<Arc<Account>>,
    pub next: Option<Key>,
}
struct ServerSnapshot {
    slot: u64,
    accounts: OrdMap<Key, Arc<Account>>,
}
struct RetainedSnapshot {
    snapshot: Arc<ServerSnapshot>,
    touched: Instant,
}
pub struct Store {
    current: ArcSwap<ServerSnapshot>,
    ready: AtomicBool,
    source: RwLock<Option<String>>,
    retained: Mutex<HashMap<u64, RetainedSnapshot>>,
}
#[derive(Clone, Serialize)]
pub struct Health {
    pub ready: bool,
    pub source: Option<String>,
    pub confirmed_slot: u64,
    pub accounts: usize,
}
impl Store {
    pub fn new() -> Self {
        Self {
            current: ArcSwap::from_pointee(ServerSnapshot {
                slot: 0,
                accounts: OrdMap::new(),
            }),
            ready: AtomicBool::new(false),
            source: RwLock::new(None),
            retained: Mutex::new(HashMap::new()),
        }
    }
    pub(crate) fn read_snapshot(&self) -> Result<ReadSnapshot> {
        ensure!(self.ready.load(Ordering::Acquire), "cache is not ready");
        let snapshot = self.current.load_full();
        Ok(ReadSnapshot {
            slot: snapshot.slot,
            accounts: snapshot.accounts.clone(),
        })
    }
    pub(crate) fn snapshot_page(
        &self,
        snapshot_slot: Option<u64>,
        after: Option<Key>,
        limit: usize,
    ) -> Result<SnapshotPage> {
        ensure!(limit > 0, "snapshot page limit must be positive");
        ensure!(self.ready.load(Ordering::Acquire), "cache is not ready");
        let snapshot = if let Some(slot) = snapshot_slot {
            let mut retained = self.retained.lock().unwrap();
            retained.retain(|_, value| value.touched.elapsed() < Duration::from_secs(300));
            let retained = retained
                .get_mut(&slot)
                .context("snapshot expired; restart pagination")?;
            retained.touched = Instant::now();
            retained.snapshot.clone()
        } else {
            self.current.load_full()
        };
        let lower = after.map_or(Unbounded, Excluded);
        let range = snapshot.accounts.range((lower, Unbounded));
        let mut accounts: Vec<_> = range
            .take(limit + 1)
            .map(|(key, value)| (*key, value.clone()))
            .collect();
        let next = (accounts.len() > limit).then(|| accounts[limit - 1].0);
        accounts.truncate(limit);
        if next.is_some() {
            self.retained
                .lock()
                .unwrap()
                .entry(snapshot.slot)
                .or_insert(RetainedSnapshot {
                    snapshot: snapshot.clone(),
                    touched: Instant::now(),
                });
        }
        Ok(SnapshotPage {
            slot: snapshot.slot,
            accounts: accounts.into_iter().map(|(_, account)| account).collect(),
            next,
        })
    }
    pub fn health(&self) -> Health {
        let snapshot = self.current.load();
        Health {
            ready: self.ready.load(Ordering::Acquire),
            source: self.source.read().unwrap().clone(),
            confirmed_slot: snapshot.slot,
            accounts: snapshot.accounts.len(),
        }
    }

    fn invalidate(&self) {
        self.ready.store(false, Ordering::Release);
    }

    fn publish(&self, source: &str, slot: u64, accounts: OrdMap<Key, Arc<Account>>) {
        let current = self.current.load();
        if current.slot != slot || current.slot == 0 {
            self.current
                .store(Arc::new(ServerSnapshot { slot, accounts }));
        }
        *self.source.write().unwrap() = Some(source.to_owned());
        self.ready.store(true, Ordering::Release);
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct StateUpdater {
    store: Arc<Store>,
    bootstrap_slot: u64,
    confirmed_slot: u64,
    source: String,
    accounts: OrdMap<Key, Arc<Account>>,
    pending: BTreeMap<u64, Vec<Mutation>>,
    versions: HashMap<Key, (u64, u64)>,
    recovering: bool,
}

struct Mutation {
    key: Key,
    account: Option<Account>,
}

impl StateUpdater {
    pub(crate) fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            bootstrap_slot: 0,
            confirmed_slot: 0,
            source: String::new(),
            accounts: OrdMap::new(),
            pending: BTreeMap::new(),
            versions: HashMap::new(),
            recovering: true,
        }
    }

    pub(crate) fn invalidate(&mut self) {
        self.store.invalidate();
    }

    pub(crate) fn install(
        &mut self,
        source: String,
        slot: u64,
        accounts: BTreeMap<Key, Arc<Account>>,
    ) {
        self.store.invalidate();
        self.source = source;
        self.bootstrap_slot = slot;
        self.confirmed_slot = slot;
        self.accounts = accounts.into_iter().collect();
        self.pending.clear();
        self.versions.clear();
        self.recovering = true;
    }

    pub(crate) fn finish_recovery(&mut self) {
        self.apply_pending_through(self.confirmed_slot);
        self.store
            .publish(&self.source, self.confirmed_slot, self.accounts.clone());
        self.recovering = false;
    }

    pub(crate) fn queue(
        &mut self,
        key: Key,
        slot: u64,
        version: u64,
        account: Option<Account>,
    ) -> Result<bool> {
        ensure!(
            self.recovering || slot > self.confirmed_slot,
            "account update arrived after its slot was confirmed"
        );
        if slot < self.bootstrap_slot
            || self
                .versions
                .get(&key)
                .is_some_and(|old| *old >= (slot, version))
        {
            return Ok(false);
        }
        self.versions.insert(key, (slot, version));
        self.pending
            .entry(slot)
            .or_default()
            .push(Mutation { key, account });
        Ok(true)
    }

    pub(crate) fn confirm(&mut self, slot: u64) -> bool {
        if slot <= self.confirmed_slot {
            return false;
        }
        self.apply_pending_through(slot);
        self.confirmed_slot = slot;
        if !self.recovering {
            self.store
                .publish(&self.source, slot, self.accounts.clone());
        }
        true
    }

    fn apply_pending_through(&mut self, slot: u64) {
        let slots: Vec<_> = self.pending.range(..=slot).map(|(slot, _)| *slot).collect();
        for slot in slots {
            for mutation in self.pending.remove(&slot).unwrap() {
                if let Some(account) = mutation.account {
                    self.accounts.insert(mutation.key, Arc::new(account));
                } else {
                    self.accounts.remove(&mutation.key);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn account(key: Key) -> Account {
        Account {
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
    fn store() -> (Arc<Store>, StateUpdater) {
        let store = Arc::new(Store::new());
        let mut updater = StateUpdater::new(store.clone());
        updater.install("one".into(), 10, BTreeMap::new());
        updater.finish_recovery();
        (store, updater)
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
        let account = Account::from_yellowstone(SubscribeUpdateAccountInfo {
            pubkey: vec![1; 32],
            lamports: 1234,
            owner: program::id().to_bytes().to_vec(),
            executable: false,
            rent_epoch: 9,
            data: data.clone(),
            write_version: 5,
            txn_signature: None,
        })
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
            Account::from_yellowstone(SubscribeUpdateAccountInfo {
                pubkey: vec![1; 32],
                lamports: 1,
                owner: program::id().to_bytes().to_vec(),
                executable: false,
                rent_epoch: 0,
                data: Vec::new(),
                write_version: 0,
                txn_signature: None,
            })
            .is_err()
        );
    }
    #[test]
    fn write_order_accepts_bootstrap_slot_and_rejects_older_versions() {
        let store = Arc::new(Store::new());
        let mut updater = StateUpdater::new(store);
        updater.install("one".into(), 10, BTreeMap::new());
        assert!(updater.queue([1; 32], 10, 100, None).unwrap());
        assert!(!updater.queue([1; 32], 10, 99, None).unwrap());
        assert!(updater.queue([1; 32], 12, 2, None).unwrap());
        assert!(!updater.queue([1; 32], 12, 1, None).unwrap());
        assert!(!updater.queue([1; 32], 11, 999, None).unwrap());
    }
    #[test]
    fn source_change_resets_version_domain() {
        let (s, mut updater) = store();
        updater.queue([1; 32], 13, 999, None).unwrap();
        updater.install("two".into(), 10, BTreeMap::new());
        assert!(!s.health().ready);
        assert!(updater.queue([1; 32], 13, 1, None).unwrap());
    }
    #[test]
    fn snapshot_pages_use_exclusive_ordered_cursors() {
        let (s, mut updater) = store();
        for value in [3, 1, 2] {
            let key = [value; 32];
            assert!(updater.queue(key, 13, 1, Some(account(key))).unwrap());
        }
        updater.confirm(13);
        let first = s.snapshot_page(None, None, 2).unwrap();
        assert_eq!(
            first
                .accounts
                .iter()
                .map(|account| account.value.pubkey.as_slice())
                .collect::<Vec<_>>(),
            vec![[1; 32].as_slice(), [2; 32].as_slice()]
        );
        let first_slot = first.slot;
        let first_cursor = first.next;
        let extra = [4; 32];
        updater.queue(extra, 14, 1, Some(account(extra))).unwrap();
        updater.confirm(14);
        let second = s.snapshot_page(Some(first_slot), first_cursor, 2).unwrap();
        assert_eq!(second.accounts.len(), 1);
        assert_eq!(second.accounts[0].value.pubkey, vec![3; 32]);
        assert!(second.next.is_none());
    }
}
