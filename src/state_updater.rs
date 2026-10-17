use crate::store::{Account, Key, Store};
use imbl::OrdMap;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

pub(crate) struct FetchedAltState {
    pub context_slot: u64,
    pub accounts: BTreeMap<Key, Arc<Account>>,
}

pub(crate) struct StateUpdater {
    store: Arc<Store>,
    bootstrap_slot: u64,
    confirmed_slot: u64,
    active_yellowstone_url: String,
    accounts: OrdMap<Key, Arc<Account>>,
    pending: BTreeMap<u64, Vec<Mutation>>,
    versions: HashMap<Key, (u64, u64)>,
    recovering: bool,
}

struct Mutation {
    key: Key,
    account: Option<Arc<Account>>,
}

impl StateUpdater {
    pub(crate) fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            bootstrap_slot: 0,
            confirmed_slot: 0,
            active_yellowstone_url: String::new(),
            accounts: OrdMap::new(),
            pending: BTreeMap::new(),
            versions: HashMap::new(),
            recovering: true,
        }
    }

    pub(crate) fn invalidate(&mut self) {
        self.store.invalidate();
    }

    pub(crate) fn confirmed_slot(&self) -> u64 {
        self.confirmed_slot
    }

    pub(crate) fn from_fetched_alt_state(
        store: Arc<Store>,
        active_yellowstone_url: String,
        fetched: FetchedAltState,
    ) -> Self {
        Self {
            store,
            bootstrap_slot: fetched.context_slot,
            confirmed_slot: fetched.context_slot,
            active_yellowstone_url,
            accounts: fetched.accounts.into_iter().collect(),
            pending: BTreeMap::new(),
            versions: HashMap::new(),
            recovering: true,
        }
    }

    pub(crate) fn finish_recovery(&mut self) {
        self.apply_pending_through(self.confirmed_slot);
        self.store.publish(
            &self.active_yellowstone_url,
            self.confirmed_slot,
            self.accounts.clone(),
        );
        self.recovering = false;
    }

    pub(crate) fn queue(
        &mut self,
        key: Key,
        slot: u64,
        version: u64,
        account: Option<Arc<Account>>,
    ) -> bool {
        if slot < self.bootstrap_slot
            || self
                .versions
                .get(&key)
                .is_some_and(|old| *old >= (slot, version))
        {
            return false;
        }
        self.versions.insert(key, (slot, version));
        self.pending
            .entry(slot)
            .or_default()
            .push(Mutation { key, account });
        true
    }

    pub(crate) fn confirm(&mut self, slot: u64) -> bool {
        if slot <= self.confirmed_slot {
            return false;
        }
        self.apply_pending_through(slot);
        self.confirmed_slot = slot;
        if !self.recovering {
            self.store
                .publish(&self.active_yellowstone_url, slot, self.accounts.clone());
        }
        true
    }

    fn apply_pending_through(&mut self, slot: u64) {
        let slots: Vec<_> = self.pending.range(..=slot).map(|(slot, _)| *slot).collect();
        for slot in slots {
            for mutation in self.pending.remove(&slot).unwrap() {
                if let Some(account) = mutation.account {
                    self.accounts.insert(mutation.key, account);
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
    use solana_address_lookup_table_interface::program;
    use yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo;

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

    fn updater(
        store: Arc<Store>,
        active_yellowstone_url: &str,
        context_slot: u64,
        accounts: BTreeMap<Key, Arc<Account>>,
    ) -> StateUpdater {
        StateUpdater::from_fetched_alt_state(
            store,
            active_yellowstone_url.into(),
            FetchedAltState {
                context_slot,
                accounts,
            },
        )
    }

    fn store() -> (Arc<Store>, StateUpdater) {
        let store = Arc::new(Store::new());
        let mut updater = updater(store.clone(), "one", 10, BTreeMap::new());
        updater.finish_recovery();
        (store, updater)
    }

    #[test]
    fn write_order_accepts_bootstrap_slot_and_rejects_older_versions() {
        let store = Arc::new(Store::new());
        let mut updater = updater(store, "one", 10, BTreeMap::new());
        assert!(updater.queue([1; 32], 10, 100, None));
        assert!(!updater.queue([1; 32], 10, 99, None));
        assert!(updater.queue([1; 32], 12, 2, None));
        assert!(!updater.queue([1; 32], 12, 1, None));
        assert!(!updater.queue([1; 32], 11, 999, None));
    }

    #[test]
    fn source_change_resets_version_domain() {
        let (store, mut updater) = store();
        updater.queue([1; 32], 13, 999, None);
        updater.invalidate();
        updater = self::updater(store.clone(), "two", 10, BTreeMap::new());
        assert!(!store.health().ready);
        assert!(updater.queue([1; 32], 13, 1, None));
    }

    #[test]
    fn fetched_state_does_not_replace_the_served_state_until_published() {
        let (store, _updater) = store();
        let mut refreshed = updater(store.clone(), "one", 20, BTreeMap::new());
        let key = [8; 32];

        refreshed.queue(key, 21, 1, Some(Arc::new(account(key))));
        refreshed.confirm(21);

        assert!(store.health().ready);
        assert_eq!(store.health().confirmed_slot, 10);
        assert_eq!(store.health().accounts, 0);

        refreshed.finish_recovery();

        assert!(store.health().ready);
        assert_eq!(store.health().confirmed_slot, 21);
        assert_eq!(store.health().accounts, 1);
    }

    #[test]
    fn late_update_is_published_at_the_next_confirmed_slot() {
        let (store, mut updater) = store();
        let key = [8; 32];

        assert!(updater.queue(key, 10, 1, Some(Arc::new(account(key)))));
        assert!(store.health().ready);
        assert_eq!(store.health().confirmed_slot, 10);
        assert_eq!(store.health().accounts, 0);

        assert!(updater.confirm(11));
        assert!(store.health().ready);
        assert_eq!(store.health().confirmed_slot, 11);
        assert_eq!(store.health().accounts, 1);
    }
}
