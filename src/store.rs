use anyhow::{Context, Result, ensure};
use arc_swap::ArcSwap;
use base64::{Engine, prelude::BASE64_STANDARD};
use imbl::OrdMap;
use serde::{Deserialize, Serialize};
use solana_account_decoder_client_types::{
    UiAccount, UiAccountData, UiAccountEncoding, UiDataSliceConfig,
};
use solana_address_lookup_table_interface::{program, state::AddressLookupTable};
use std::{
    collections::HashMap,
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
    active_yellowstone_url: RwLock<Option<String>>,
    retained: Mutex<HashMap<u64, RetainedSnapshot>>,
}
#[derive(Clone, Serialize)]
pub struct Health {
    pub ready: bool,
    pub active_yellowstone_url: Option<String>,
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
            active_yellowstone_url: RwLock::new(None),
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
            active_yellowstone_url: self.active_yellowstone_url.read().unwrap().clone(),
            confirmed_slot: snapshot.slot,
            accounts: snapshot.accounts.len(),
        }
    }

    pub(crate) fn invalidate(&self) {
        self.ready.store(false, Ordering::Release);
    }

    pub(crate) fn publish(
        &self,
        active_yellowstone_url: &str,
        slot: u64,
        accounts: OrdMap<Key, Arc<Account>>,
    ) {
        let current = self.current.load();
        if current.slot != slot || current.slot == 0 {
            self.current
                .store(Arc::new(ServerSnapshot { slot, accounts }));
        }
        *self.active_yellowstone_url.write().unwrap() = Some(active_yellowstone_url.to_owned());
        self.ready.store(true, Ordering::Release);
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
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
    fn snapshot_pages_use_exclusive_ordered_cursors() {
        let store = Store::new();
        let mut accounts = OrdMap::new();
        for value in [3, 1, 2] {
            let key = [value; 32];
            accounts.insert(key, Arc::new(account(key)));
        }
        store.publish("one", 13, accounts.clone());
        let first = store.snapshot_page(None, None, 2).unwrap();
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
        accounts.insert(extra, Arc::new(account(extra)));
        store.publish("one", 14, accounts);
        let second = store
            .snapshot_page(Some(first_slot), first_cursor, 2)
            .unwrap();
        assert_eq!(second.accounts.len(), 1);
        assert_eq!(second.accounts[0].value.pubkey, vec![3; 32]);
        assert!(second.next.is_none());
    }
}
