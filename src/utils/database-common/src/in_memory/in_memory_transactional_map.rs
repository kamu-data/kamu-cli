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
/// after the commit, and a rollback drops them. A commit fails if another
/// transaction committed a key after this one first changed it with
/// [`InMemoryMapWrite::put`] or [`InMemoryMapWrite::delete`].
///
/// Unlike a database, a write never waits for another transaction: two
/// transactions changing one key both succeed at write time, and the later
/// commit fails with a conflict. In Postgres the later writer would wait for
/// the first and fail its own check, such as a compare-and-set. Where Postgres
/// lets the later writer apply its change after the first instead, the change
/// is made with [`InMemoryMapWrite::overwrite`] (an upsert replacing the
/// value), [`InMemoryMapWrite::upsert`] (an upsert updating it) or
/// [`InMemoryMapWrite::update`] (an `UPDATE` of an existing row).
///
/// Conflicts are found per key only, so unlike a database:
/// - a transaction deleting the keys that match a condition does not stop another from adding a key
///   that matches it;
/// - a uniqueness rule over other keys holds only if the repository keeps it as keys of its own,
///   taken with [`InMemoryMapWrite::claim`].
///
/// Changes made outside such a transaction, or under the ID of one that has
/// ended, are committed at once
pub struct InMemoryTransactionalMap<K, V> {
    state: Mutex<MapState<K, V>>,
}

impl<K: Eq + Hash, V> InMemoryTransactionalMap<K, V> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(MapState {
                committed: HashMap::new(),
                deleted_versions: HashMap::new(),
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
                let moved = state.committed_version(key) != change.base_version;
                let conflicts = match change.on_concurrent_commit {
                    OnConcurrentCommit::Fail => moved,
                    OnConcurrentCommit::Overwrite | OnConcurrentCommit::Upsert { .. } => false,
                    OnConcurrentCommit::Reapply(_) => moved && !state.committed.contains_key(key),
                    OnConcurrentCommit::Claim => state.committed.contains_key(key),
                };
                if conflicts {
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
            let moved = self.state.committed_version(&key) != change.base_version;
            let committed = self.state.committed.get(&key).map(|v| &v.value);
            match change.resolve(moved, committed) {
                ChangedValue::Put(value) => self.state.commit_put(key, value),
                ChangedValue::Delete => self.state.commit_delete(key),
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
        self.change(key, ChangedValue::Put(value), OnConcurrentCommit::Fail);
    }

    /// Puts the value under a key that must be free when the transaction
    /// commits, as an insert into a unique index does: the commit fails if
    /// the key is committed then, whatever the transaction saw at write time.
    /// Together with [`Self::is_deleted_by_another_transaction`], it takes
    /// over a key that another open transaction is freeing
    pub fn claim(&mut self, key: K, value: V) {
        self.change(key, ChangedValue::Put(value), OnConcurrentCommit::Claim);
    }

    /// Whether another open transaction has deleted the key, as a unique index
    /// sees an entry that an uncommitted transaction removes. Always `false`
    /// for a key this transaction has changed itself, whose own change is what
    /// it sees, and when this write is not in a tracked transaction
    pub fn is_deleted_by_another_transaction(&self, key: &K) -> bool {
        let Some(transaction_id) = self.transaction_id else {
            return false;
        };
        if self.state.working_sets[&transaction_id]
            .changes
            .contains_key(key)
        {
            return false;
        }
        self.state
            .working_sets
            .iter()
            .filter(|(other_id, _)| **other_id != transaction_id)
            .any(|(_, working_set)| {
                working_set
                    .changes
                    .get(key)
                    .is_some_and(|change| change.value.as_put().is_none())
            })
    }

    /// Puts the value as a database upsert replacing the row does: a key
    /// another transaction commits meanwhile is overwritten at commit instead
    /// of failing it
    pub fn overwrite(&mut self, key: K, value: V) {
        self.change(key, ChangedValue::Put(value), OnConcurrentCommit::Overwrite);
    }

    /// Replaces the value the transaction sees with `update` of it, as a
    /// database `UPDATE` does: if another transaction commits the key
    /// meanwhile, `update` is applied to that transaction's value at commit, and
    /// the commit fails only if that transaction deleted the key. Returns
    /// `false` when the transaction sees no value for the key
    pub fn update(&mut self, key: K, update: impl Fn(&V) -> V + Send + 'static) -> bool {
        let Some(value) = self.view().get(&key).map(&update) else {
            return false;
        };
        self.change(
            key,
            ChangedValue::Put(value),
            OnConcurrentCommit::Reapply(vec![Box::new(update)]),
        );
        true
    }

    /// Inserts `value`, or replaces the value the transaction sees with
    /// `update` of it, as a database `INSERT ... ON CONFLICT DO UPDATE` does:
    /// if another transaction commits the key meanwhile, `update` is applied
    /// to that transaction's value at commit, and if it deleted the key,
    /// `value` is inserted. The commit never fails on this key
    pub fn upsert(&mut self, key: K, value: V, update: impl Fn(&V) -> V + Send + 'static)
    where
        V: Clone,
    {
        let seen = match self.view().get(&key) {
            Some(current) => update(current),
            None => value.clone(),
        };
        self.change(
            key,
            ChangedValue::Put(seen),
            OnConcurrentCommit::Upsert {
                insert: value,
                update: Box::new(update),
                later_updates: Vec::new(),
            },
        );
    }

    pub fn delete(&mut self, key: K) {
        self.change(key, ChangedValue::Delete, OnConcurrentCommit::Fail);
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

    fn change(
        &mut self,
        key: K,
        value: ChangedValue<V>,
        on_concurrent_commit: OnConcurrentCommit<V>,
    ) {
        let Some(transaction_id) = self.transaction_id else {
            match value {
                ChangedValue::Put(value) => self.state.commit_put(key, value),
                ChangedValue::Delete => self.state.commit_delete(key),
            }
            return;
        };

        let base_version = self.state.committed_version(&key);
        let working_set = self.state.working_sets.get_mut(&transaction_id).unwrap();
        // A key changed twice keeps the version its first change was based on
        let change = match working_set.changes.remove(&key) {
            None => Change {
                base_version,
                on_concurrent_commit,
                value,
            },
            Some(earlier) => Change {
                base_version: earlier.base_version,
                on_concurrent_commit: earlier.on_concurrent_commit.then(on_concurrent_commit),
                value,
            },
        };
        working_set.changes.insert(key, change);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MapState<K, V> {
    committed: HashMap<K, Versioned<V>>,
    /// The version of each committed delete, so a key deleted and created
    /// again does not look unchanged
    deleted_versions: HashMap<K, u64>,
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
        self.committed
            .get(key)
            .map(|versioned| versioned.version)
            .or_else(|| self.deleted_versions.get(key).copied())
    }

    fn commit_put(&mut self, key: K, value: V) {
        let version = self.take_next_version();
        self.deleted_versions.remove(&key);
        self.committed.insert(key, Versioned { value, version });
    }

    fn commit_delete(&mut self, key: K) {
        if self.committed.remove(&key).is_some() {
            let version = self.take_next_version();
            self.deleted_versions.insert(key, version);
        }
    }

    fn take_next_version(&mut self) -> u64 {
        let version = self.next_version;
        self.next_version += 1;
        version
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
    /// Version of the committed key, or of its last delete, when the
    /// transaction first changed it; `None` when it never existed
    base_version: Option<u64>,
    on_concurrent_commit: OnConcurrentCommit<V>,
    value: ChangedValue<V>,
}

impl<V> Change<V> {
    /// The change to commit when the key's committed value is `committed`;
    /// `moved` tells whether another transaction committed the key since
    fn resolve(self, moved: bool, committed: Option<&V>) -> ChangedValue<V> {
        match self.on_concurrent_commit {
            OnConcurrentCommit::Reapply(updates) if moved => {
                let Some(committed) = committed else {
                    return self.value;
                };
                let mut updated = None;
                for update in &updates {
                    updated = Some(update(updated.as_ref().unwrap_or(committed)));
                }
                updated.map_or(self.value, ChangedValue::Put)
            }
            OnConcurrentCommit::Upsert {
                insert,
                update,
                later_updates,
            } if moved => {
                let base = match committed {
                    Some(committed) => update(committed),
                    None => insert,
                };
                ChangedValue::Put(
                    later_updates
                        .iter()
                        .fold(base, |value, later_update| later_update(&value)),
                )
            }
            OnConcurrentCommit::Fail
            | OnConcurrentCommit::Overwrite
            | OnConcurrentCommit::Reapply(_)
            | OnConcurrentCommit::Upsert { .. }
            | OnConcurrentCommit::Claim => self.value,
        }
    }
}

/// What the commit does when another transaction committed the key after this
/// one first changed it
enum OnConcurrentCommit<V> {
    Fail,
    /// Commits this transaction's value over the other's
    Overwrite,
    /// Applies the updates, in order, to the other transaction's value; fails
    /// if that transaction deleted the key
    Reapply(Vec<ValueUpdate<V>>),
    /// Applies `update` to the other transaction's value, or inserts `insert`
    /// if that transaction deleted the key; then applies the later updates
    Upsert {
        insert: V,
        update: ValueUpdate<V>,
        later_updates: Vec<ValueUpdate<V>>,
    },
    /// Fails if the key is committed at all, even if it did not move
    Claim,
}

/// A change of a value computed from the value it replaces
type ValueUpdate<V> = Box<dyn Fn(&V) -> V + Send>;

impl<V> OnConcurrentCommit<V> {
    /// The rule for a key changed by `self` and then by `later` in one
    /// transaction: a change that must fail on a conflict keeps failing, and a
    /// later overwrite discards the updates before it
    fn then(self, later: Self) -> Self {
        match (self, later) {
            // The key must still be free at commit, whatever came after
            (Self::Claim, _) => Self::Claim,
            (Self::Fail, _) | (_, Self::Fail | Self::Claim) => Self::Fail,
            (Self::Reapply(mut updates), Self::Reapply(later_updates)) => {
                updates.extend(later_updates);
                Self::Reapply(updates)
            }
            // The key exists after the first upsert, so a later upsert only
            // updates it
            (
                Self::Upsert {
                    insert,
                    update,
                    mut later_updates,
                },
                Self::Upsert {
                    update: later_update,
                    ..
                },
            ) => {
                later_updates.push(later_update);
                Self::Upsert {
                    insert,
                    update,
                    later_updates,
                }
            }
            (
                Self::Upsert {
                    insert,
                    update,
                    mut later_updates,
                },
                Self::Reapply(updates),
            ) => {
                later_updates.extend(updates);
                Self::Upsert {
                    insert,
                    update,
                    later_updates,
                }
            }
            // The key existed at the first update, which fails on a delete
            (
                Self::Reapply(mut updates),
                Self::Upsert {
                    update: later_update,
                    ..
                },
            ) => {
                updates.push(later_update);
                Self::Reapply(updates)
            }
            (Self::Overwrite | Self::Reapply(_) | Self::Upsert { .. }, Self::Overwrite)
            | (Self::Overwrite, Self::Reapply(_) | Self::Upsert { .. }) => Self::Overwrite,
        }
    }
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
        for (starting_state, committed) in [("existing key", &[("a", 1)][..]), ("new key", &[])] {
            let harness = MapHarness::with_committed(committed);
            let first = harness.begin();
            let second = harness.begin();
            harness.map.write(Some(first), |write| write.put("a", 10));
            harness.map.write(Some(second), |write| write.put("a", 20));
            harness.commit(first);

            assert_matches!(
                harness.check_commit(second),
                Err(InMemoryWriteConflictError { key }) if key == "\"a\"",
                "{starting_state}"
            );
        }
    }

    #[test]
    fn test_commit_fails_on_a_key_created_and_deleted_since_it_changed_it() {
        let harness = MapHarness::with_committed(&[]);
        let first = harness.begin();
        harness.map.write(Some(first), |write| write.put("a", 10));
        harness.map.write(None, |write| write.put("a", 20));
        harness.map.write(None, |write| write.delete("a"));

        assert_matches!(harness.check_commit(first), Err(_));
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
    fn test_overwrite_wins_over_a_key_another_transaction_committed() {
        let harness = MapHarness::with_committed(&[("a", 1)]);
        let first = harness.begin();
        let second = harness.begin();
        harness
            .map
            .write(Some(first), |write| write.overwrite("a", 10));
        harness
            .map
            .write(Some(second), |write| write.overwrite("a", 20));

        harness.commit(first);
        harness.commit(second);

        pretty_assertions::assert_eq!(harness.entries(None), [("a", 20)]);
    }

    #[test]
    fn test_update_is_applied_again_to_a_value_another_transaction_committed() {
        let harness = MapHarness::with_committed(&[("a", 1)]);
        let first = harness.begin();
        let second = harness.begin();
        harness.map.write(Some(first), |write| write.put("a", 10));
        harness.map.write(Some(second), |write| {
            assert!(write.update("a", |value| value + 5));
            assert!(write.update("a", |value| value * 2));
        });

        pretty_assertions::assert_eq!(harness.get(Some(second), "a"), Some(12));

        harness.commit(first);
        harness.commit(second);

        pretty_assertions::assert_eq!(harness.entries(None), [("a", 30)]);
    }

    #[test]
    fn test_update_fails_the_commit_on_a_key_another_transaction_deleted() {
        let harness = MapHarness::with_committed(&[("a", 1)]);
        let first = harness.begin();
        let second = harness.begin();
        harness.map.write(Some(first), |write| write.delete("a"));
        harness.map.write(Some(second), |write| {
            assert!(write.update("a", |value| value + 1));
        });
        harness.commit(first);

        assert_matches!(harness.check_commit(second), Err(_));
    }

    #[test]
    fn test_update_of_a_missing_key_changes_nothing() {
        let harness = MapHarness::with_committed(&[]);
        let transaction = harness.begin();

        let updated = harness.map.write(Some(transaction), |write| {
            write.update("a", |value| value + 1)
        });

        assert!(!updated);
        pretty_assertions::assert_eq!(harness.entries(Some(transaction)), []);
    }

    #[test]
    fn test_put_after_update_fails_on_a_key_another_transaction_committed() {
        let harness = MapHarness::with_committed(&[("a", 1)]);
        let first = harness.begin();
        let second = harness.begin();
        harness.map.write(Some(first), |write| write.put("a", 10));
        harness.map.write(Some(second), |write| {
            write.update("a", |value| value + 1);
            write.put("a", 20);
        });
        harness.commit(first);

        assert_matches!(harness.check_commit(second), Err(_));
    }

    #[test]
    fn test_upserts_of_a_missing_key_in_concurrent_transactions_both_apply() {
        let harness = MapHarness::with_committed(&[]);
        let first = harness.begin();
        let second = harness.begin();
        harness.map.write(Some(first), |write| {
            write.upsert("a", 20, |value| *value.max(&20));
        });
        harness.map.write(Some(second), |write| {
            write.upsert("a", 10, |value| *value.max(&10));
        });

        harness.commit(first);
        harness.commit(second);

        pretty_assertions::assert_eq!(harness.entries(None), [("a", 20)]);
    }

    #[test]
    fn test_upsert_updates_a_value_the_transaction_sees() {
        let harness = MapHarness::with_committed(&[("a", 1)]);
        let transaction = harness.begin();

        harness.map.write(Some(transaction), |write| {
            write.upsert("a", 10, |value| value + 5);
        });

        pretty_assertions::assert_eq!(harness.get(Some(transaction), "a"), Some(6));
    }

    #[test]
    fn test_upsert_inserts_its_value_over_a_key_another_transaction_deleted() {
        let harness = MapHarness::with_committed(&[("a", 1)]);
        let first = harness.begin();
        let second = harness.begin();
        harness.map.write(Some(first), |write| write.delete("a"));
        harness.map.write(Some(second), |write| {
            write.upsert("a", 10, |value| value + 5);
        });

        harness.commit(first);
        harness.commit(second);

        pretty_assertions::assert_eq!(harness.entries(None), [("a", 10)]);
    }

    #[test]
    fn test_claim_takes_over_a_key_another_transaction_freed() {
        let harness = MapHarness::with_committed(&[("a", 1)]);
        let releasing = harness.begin();
        let claiming = harness.begin();
        harness
            .map
            .write(Some(releasing), |write| write.delete("a"));
        let seen_freed = harness.map.write(Some(claiming), |write| {
            let freed = write.is_deleted_by_another_transaction(&"a");
            write.claim("a", 2);
            freed
        });

        harness.commit(releasing);
        harness.commit(claiming);

        assert!(seen_freed);
        pretty_assertions::assert_eq!(harness.entries(None), [("a", 2)]);
    }

    #[test]
    fn test_own_claim_hides_another_transaction_freeing_the_key() {
        let harness = MapHarness::with_committed(&[("a", 1)]);
        let releasing = harness.begin();
        let claiming = harness.begin();
        harness
            .map
            .write(Some(releasing), |write| write.delete("a"));

        let seen_freed_after_claim = harness.map.write(Some(claiming), |write| {
            write.claim("a", 2);
            write.is_deleted_by_another_transaction(&"a")
        });

        assert!(!seen_freed_after_claim);
    }

    #[test]
    fn test_claim_fails_the_commit_while_the_key_is_committed() {
        for (case, roll_back_release) in
            [("release still open", false), ("release rolled back", true)]
        {
            let harness = MapHarness::with_committed(&[("a", 1)]);
            let releasing = harness.begin();
            let claiming = harness.begin();
            harness
                .map
                .write(Some(releasing), |write| write.delete("a"));
            harness
                .map
                .write(Some(claiming), |write| write.claim("a", 2));
            if roll_back_release {
                harness.map.rollback(releasing);
            }

            assert_matches!(
                harness.check_commit(claiming),
                Err(InMemoryWriteConflictError { key }) if key == "\"a\"",
                "{case}"
            );
        }
    }

    #[test]
    fn test_claims_of_a_free_key_in_concurrent_transactions_fail_the_second_commit() {
        let harness = MapHarness::with_committed(&[]);
        let first = harness.begin();
        let second = harness.begin();
        harness.map.write(Some(first), |write| write.claim("a", 1));
        harness.map.write(Some(second), |write| write.claim("a", 2));

        harness.commit(first);

        assert_matches!(harness.check_commit(second), Err(_));
        pretty_assertions::assert_eq!(harness.entries(None), [("a", 1)]);
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
