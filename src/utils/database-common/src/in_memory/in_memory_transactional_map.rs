// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Mutex, MutexGuard};

use crate::{
    InMemoryPreparedCommit,
    InMemoryTransactionParticipant,
    InMemoryWriteConflictError,
    TransactionId,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A map whose changes inside a transaction opened by
/// `InMemoryTransactionManager` form a working set: the transaction reads its
/// own changes over the committed entries, other transactions see them only
/// after the commit, and a rollback drops them. A commit fails when another
/// transaction committed a key this one changed since reading it.
///
/// Unlike a database, a write never waits for another transaction: two
/// transactions changing one key both succeed at write time, each checked
/// against its own view, and the later commit fails with a conflict. In
/// Postgres the later writer would wait for the first and then fail its own
/// check, such as a compare-and-set, at write time.
///
/// Changes made outside such a transaction are committed at once
pub struct InMemoryTransactionalMap<K, V> {
    state: Mutex<MapState<K, V>>,
}

impl<K: Eq + Hash, V> InMemoryTransactionalMap<K, V> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(MapState {
                committed: HashMap::new(),
                next_version: 1,
                working_sets: HashMap::new(),
            }),
        }
    }

    pub fn read<R>(
        &self,
        transaction_id: Option<TransactionId>,
        read: impl FnOnce(&InMemoryMapView<'_, K, V>) -> R,
    ) -> R {
        let state = self.state.lock().unwrap();
        read(&state.view(transaction_id))
    }

    /// Reads and changes the map under one lock, so a check made through
    /// [`InMemoryMapWrite::view`] still holds when the change is made
    pub fn write<R>(
        &self,
        transaction_id: Option<TransactionId>,
        write: impl FnOnce(&mut InMemoryMapWrite<'_, K, V>) -> R,
    ) -> R {
        let mut state = self.state.lock().unwrap();
        let transaction_id =
            transaction_id.filter(|transaction_id| state.working_sets.contains_key(transaction_id));
        write(&mut InMemoryMapWrite {
            state: &mut state,
            transaction_id,
        })
    }
}

impl<K: Eq + Hash, V> Default for InMemoryTransactionalMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> InMemoryTransactionParticipant for InMemoryTransactionalMap<K, V>
where
    K: Eq + Hash + Debug + Send,
    V: Send,
{
    fn begin(&self, transaction_id: TransactionId) {
        self.state
            .lock()
            .unwrap()
            .working_sets
            .insert(transaction_id, WorkingSet::default());
    }

    fn prepare_commit(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Box<dyn InMemoryPreparedCommit + '_>, InMemoryWriteConflictError> {
        let state = self.state.lock().unwrap();
        if let Some(working_set) = state.working_sets.get(&transaction_id) {
            for (key, change) in &working_set.changes {
                if state.committed_version(key) != change.base_version {
                    return Err(InMemoryWriteConflictError {
                        key: format!("{key:?}"),
                    });
                }
            }
        }
        Ok(Box::new(PreparedMapCommit {
            state,
            transaction_id,
        }))
    }

    fn rollback(&self, transaction_id: TransactionId) {
        self.state
            .lock()
            .unwrap()
            .working_sets
            .remove(&transaction_id);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PreparedMapCommit<'a, K, V> {
    state: MutexGuard<'a, MapState<K, V>>,
    transaction_id: TransactionId,
}

impl<K: Eq + Hash, V> InMemoryPreparedCommit for PreparedMapCommit<'_, K, V> {
    fn apply(mut self: Box<Self>) {
        let Some(working_set) = self.state.working_sets.remove(&self.transaction_id) else {
            return;
        };
        for (key, change) in working_set.changes {
            match change.value {
                ChangedValue::Put(value) => self.state.commit_put(key, value),
                ChangedValue::Delete => {
                    self.state.committed.remove(&key);
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The map as one transaction sees it: its working set over the committed
/// entries
pub struct InMemoryMapView<'a, K, V> {
    committed: &'a HashMap<K, Versioned<V>>,
    working_set: Option<&'a WorkingSet<K, V>>,
}

impl<'a, K: Eq + Hash, V> InMemoryMapView<'a, K, V> {
    pub fn get(&self, key: &K) -> Option<&'a V> {
        if let Some(working_set) = self.working_set
            && let Some(change) = working_set.changes.get(key)
        {
            return change.value.as_put();
        }
        self.committed.get(key).map(|versioned| &versioned.value)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    /// Every entry the transaction sees, in no particular order
    pub fn iter(&self) -> impl Iterator<Item = (&'a K, &'a V)> + 'a {
        let working_set = self.working_set;
        let unchanged = self
            .committed
            .iter()
            .filter(move |(key, _)| {
                working_set.is_none_or(|working_set| !working_set.changes.contains_key(*key))
            })
            .map(|(key, versioned)| (key, &versioned.value));
        let put = working_set
            .into_iter()
            .flat_map(|working_set| working_set.changes.iter())
            .filter_map(|(key, change)| change.value.as_put().map(|value| (key, value)));
        unchanged.chain(put)
    }

    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Changes the map: into the working set of a tracked transaction, otherwise
/// straight into the committed entries
pub struct InMemoryMapWrite<'a, K, V> {
    state: &'a mut MapState<K, V>,
    transaction_id: Option<TransactionId>,
}

impl<K: Eq + Hash, V> InMemoryMapWrite<'_, K, V> {
    pub fn view(&self) -> InMemoryMapView<'_, K, V> {
        self.state.view(self.transaction_id)
    }

    pub fn put(&mut self, key: K, value: V) {
        self.change(key, ChangedValue::Put(value));
    }

    pub fn delete(&mut self, key: K) {
        self.change(key, ChangedValue::Delete);
    }

    /// Deletes every key the transaction sees that matches `predicate`
    pub fn delete_where(&mut self, predicate: impl Fn(&K) -> bool)
    where
        K: Clone,
    {
        let keys: Vec<K> = self
            .view()
            .iter()
            .filter(|(key, _)| predicate(key))
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            self.delete(key);
        }
    }

    fn change(&mut self, key: K, value: ChangedValue<V>) {
        let Some(transaction_id) = self.transaction_id else {
            match value {
                ChangedValue::Put(value) => self.state.commit_put(key, value),
                ChangedValue::Delete => {
                    self.state.committed.remove(&key);
                }
            }
            return;
        };

        let base_version = self.state.committed_version(&key);
        let working_set = self.state.working_sets.get_mut(&transaction_id).unwrap();
        // A key changed twice keeps the version its first change was based on
        let base_version = working_set
            .changes
            .get(&key)
            .map_or(base_version, |change| change.base_version);
        working_set.changes.insert(
            key,
            Change {
                base_version,
                value,
            },
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MapState<K, V> {
    committed: HashMap<K, Versioned<V>>,
    next_version: u64,
    working_sets: HashMap<TransactionId, WorkingSet<K, V>>,
}

impl<K: Eq + Hash, V> MapState<K, V> {
    fn view(&self, transaction_id: Option<TransactionId>) -> InMemoryMapView<'_, K, V> {
        InMemoryMapView {
            committed: &self.committed,
            working_set: transaction_id
                .and_then(|transaction_id| self.working_sets.get(&transaction_id)),
        }
    }

    fn committed_version(&self, key: &K) -> Option<u64> {
        self.committed.get(key).map(|versioned| versioned.version)
    }

    fn commit_put(&mut self, key: K, value: V) {
        let version = self.next_version;
        self.next_version += 1;
        self.committed.insert(key, Versioned { value, version });
    }
}

struct Versioned<V> {
    value: V,
    version: u64,
}

struct WorkingSet<K, V> {
    changes: HashMap<K, Change<V>>,
}

impl<K, V> Default for WorkingSet<K, V> {
    fn default() -> Self {
        Self {
            changes: HashMap::new(),
        }
    }
}

struct Change<V> {
    /// Version of the committed key when the transaction first changed it;
    /// `None` when the key was absent
    base_version: Option<u64>,
    value: ChangedValue<V>,
}

enum ChangedValue<V> {
    Put(V),
    Delete,
}

impl<V> ChangedValue<V> {
    fn as_put(&self) -> Option<&V> {
        match self {
            Self::Put(value) => Some(value),
            Self::Delete => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;

    #[test]
    fn test_transaction_reads_its_own_changes() {
        let harness = MapHarness::with_committed(&[("a", 1), ("b", 2)]);
        let transaction = harness.begin();

        harness.map.write(Some(transaction), |write| {
            write.put("a", 10);
            write.delete("b");
            write.put("c", 3);
        });

        pretty_assertions::assert_eq!(harness.get(Some(transaction), "a"), Some(10));
        pretty_assertions::assert_eq!(harness.get(Some(transaction), "b"), None);
        pretty_assertions::assert_eq!(harness.entries(Some(transaction)), [("a", 10), ("c", 3)]);
    }

    #[test]
    fn test_other_transactions_see_changes_only_after_commit() {
        let harness = MapHarness::with_committed(&[("a", 1)]);
        let writer = harness.begin();
        let reader = harness.begin();
        harness.map.write(Some(writer), |write| write.put("a", 10));

        pretty_assertions::assert_eq!(harness.entries(Some(reader)), [("a", 1)]);
        pretty_assertions::assert_eq!(harness.entries(None), [("a", 1)]);

        harness.commit(writer);

        pretty_assertions::assert_eq!(harness.entries(Some(reader)), [("a", 10)]);
        pretty_assertions::assert_eq!(harness.entries(None), [("a", 10)]);
    }

    #[test]
    fn test_rollback_drops_the_working_set() {
        let harness = MapHarness::with_committed(&[("a", 1)]);
        let transaction = harness.begin();
        harness.map.write(Some(transaction), |write| {
            write.delete("a");
            write.put("b", 2);
        });

        harness.map.rollback(transaction);

        pretty_assertions::assert_eq!(harness.entries(None), [("a", 1)]);
    }

    #[test]
    fn test_commit_fails_on_a_key_another_transaction_committed() {
        let harness = MapHarness::with_committed(&[("a", 1)]);
        let first = harness.begin();
        let second = harness.begin();
        harness.map.write(Some(first), |write| write.put("a", 10));
        harness.map.write(Some(second), |write| write.put("a", 20));
        harness.commit(first);

        assert_matches!(
            harness.check_commit(second),
            Err(InMemoryWriteConflictError { key }) if key == "\"a\""
        );
    }

    #[test]
    fn test_commit_fails_on_a_key_another_transaction_created() {
        let harness = MapHarness::with_committed(&[]);
        let first = harness.begin();
        let second = harness.begin();
        harness.map.write(Some(first), |write| write.put("a", 10));
        harness.map.write(Some(second), |write| write.put("a", 20));
        harness.commit(first);

        assert_matches!(harness.check_commit(second), Err(_));
    }

    #[test]
    fn test_commits_changing_different_keys_both_apply() {
        let harness = MapHarness::with_committed(&[("a", 1), ("b", 2)]);
        let first = harness.begin();
        let second = harness.begin();
        harness.map.write(Some(first), |write| write.put("a", 10));
        harness.map.write(Some(second), |write| write.put("b", 20));

        harness.commit(first);
        harness.commit(second);

        pretty_assertions::assert_eq!(harness.entries(None), [("a", 10), ("b", 20)]);
    }

    #[test]
    fn test_changes_outside_a_tracked_transaction_are_committed_at_once() {
        let harness = MapHarness::with_committed(&[("a", 1)]);
        let untracked = TransactionId::next();

        harness
            .map
            .write(Some(untracked), |write| write.put("b", 2));
        harness.map.write(None, |write| write.delete("a"));

        pretty_assertions::assert_eq!(harness.entries(None), [("b", 2)]);
    }

    struct MapHarness {
        map: InMemoryTransactionalMap<&'static str, i32>,
    }

    impl MapHarness {
        fn with_committed(entries: &[(&'static str, i32)]) -> Self {
            let map = InMemoryTransactionalMap::new();
            map.write(None, |write| {
                for (key, value) in entries {
                    write.put(*key, *value);
                }
            });
            Self { map }
        }

        fn begin(&self) -> TransactionId {
            let transaction = TransactionId::next();
            self.map.begin(transaction);
            transaction
        }

        fn commit(&self, transaction: TransactionId) {
            self.map.prepare_commit(transaction).unwrap().apply();
        }

        fn check_commit(
            &self,
            transaction: TransactionId,
        ) -> Result<(), InMemoryWriteConflictError> {
            self.map.prepare_commit(transaction).map(|_| ())
        }

        fn get(&self, transaction: Option<TransactionId>, key: &'static str) -> Option<i32> {
            self.map.read(transaction, |view| view.get(&key).copied())
        }

        fn entries(&self, transaction: Option<TransactionId>) -> Vec<(&'static str, i32)> {
            let mut entries = self.map.read(transaction, |view| {
                view.iter()
                    .map(|(key, value)| (*key, *value))
                    .collect::<Vec<_>>()
            });
            entries.sort_unstable();
            entries
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
