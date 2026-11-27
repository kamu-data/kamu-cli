// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{InMemoryTransactionalMap, TransactionId};
use kamu_configuration::ReplaceProjectionEntriesError;
use kamu_resources::ResourceID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) type ProjectionEntriesMap<E> = InMemoryTransactionalMap<(ResourceID, u64), Vec<E>>;

/// The entries of a configuration projection, keyed by resource and generation
/// as the SQL backends key them, seen from one transaction
pub(crate) struct ProjectionEntries<'a, E> {
    map: &'a ProjectionEntriesMap<E>,
    transaction_id: Option<TransactionId>,
}

impl<'a, E> ProjectionEntries<'a, E>
where
    E: Clone + Send + Sync + 'static,
{
    pub(crate) fn new(
        map: &'a ProjectionEntriesMap<E>,
        transaction_id: Option<TransactionId>,
    ) -> Self {
        Self {
            map,
            transaction_id,
        }
    }

    pub(crate) fn find_entry(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
        predicate: impl Fn(&E) -> bool,
    ) -> Option<E> {
        self.map.read(self.transaction_id, |entries| {
            entries
                .get(&(*resource_id, resource_generation))
                .and_then(|entries| entries.iter().find(|entry| predicate(entry)))
                .cloned()
        })
    }

    pub(crate) fn get_entries(&self, resource_id: &ResourceID, resource_generation: u64) -> Vec<E> {
        self.map.read(self.transaction_id, |entries| {
            entries
                .get(&(*resource_id, resource_generation))
                .cloned()
                .unwrap_or_default()
        })
    }

    /// The entries of the latest generation, or of the latest one before
    /// `before_generation` when given
    pub(crate) fn get_latest_entries(
        &self,
        resource_id: &ResourceID,
        before_generation: Option<u64>,
    ) -> Vec<E> {
        self.map.read(self.transaction_id, |entries| {
            entries
                .iter()
                .filter(|((stored_resource_id, stored_generation), _)| {
                    stored_resource_id == resource_id
                        && before_generation.is_none_or(|before| *stored_generation < before)
                })
                .max_by_key(|((_, stored_generation), _)| *stored_generation)
                .map(|(_, entries)| entries.clone())
                .unwrap_or_default()
        })
    }

    pub(crate) fn replace_entries(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
        new_entries: &[E],
    ) -> Result<(), ReplaceProjectionEntriesError> {
        self.map.write(self.transaction_id, |entries| {
            let key = (*resource_id, resource_generation);
            if entries.view().contains_key(&key) {
                return Err(ReplaceProjectionEntriesError::concurrent_modification());
            }
            // A generation is written once, as the unique constraint of the SQL
            // backends enforces
            entries.claim(key, new_entries.to_vec());
            Ok(())
        })
    }

    pub(crate) fn cleanup_entries_before_generation(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
    ) {
        self.map.write(self.transaction_id, |entries| {
            entries.delete_where(|(stored_resource_id, stored_generation)| {
                stored_resource_id == resource_id && *stored_generation < resource_generation
            });
        });
    }

    pub(crate) fn delete_all_entries(&self, resource_ids: &[ResourceID]) {
        self.map.write(self.transaction_id, |entries| {
            entries
                .delete_where(|(stored_resource_id, _)| resource_ids.contains(stored_resource_id));
        });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
