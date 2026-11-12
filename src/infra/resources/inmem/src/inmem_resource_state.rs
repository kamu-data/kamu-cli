// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{
    InMemoryMapView,
    InMemoryMapWrite,
    InMemoryTransactionParticipant,
    InMemoryTransactionalMap,
    TransactionId,
    in_memory_transaction_participant,
};
use dill::*;
use kamu_resources::{ResourceID, ResourceName, ResourceSnapshot, TypeUri};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The name a live resource holds within its account and schema
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ResourceLookupKey {
    pub(crate) account_id: odf::AccountID,
    pub(crate) schema: TypeUri,
    pub(crate) name: ResourceName,
}

impl ResourceLookupKey {
    pub(crate) fn of(snapshot: &ResourceSnapshot) -> Self {
        Self {
            account_id: snapshot.headers.account.did.clone(),
            schema: snapshot.schema.clone(),
            name: snapshot.headers.name.clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resource snapshots and the names they hold. A name is a key of its own, as
/// the partial unique index of the SQL backends makes it a row of its own, so
/// two transactions taking one name conflict at commit
pub struct InMemoryResourceState {
    snapshots_by_id: InMemoryTransactionalMap<ResourceID, ResourceSnapshot>,
    ids_by_lookup_key: InMemoryTransactionalMap<ResourceLookupKey, ResourceID>,
}

#[component(pub)]
#[interface(dyn InMemoryTransactionParticipant)]
#[scope(Singleton)]
impl InMemoryResourceState {
    pub fn new() -> Self {
        Self {
            snapshots_by_id: InMemoryTransactionalMap::new(),
            ids_by_lookup_key: InMemoryTransactionalMap::new(),
        }
    }

    /// Reads both maps as one transaction sees them
    pub(crate) fn read<R>(
        &self,
        transaction_id: Option<TransactionId>,
        read: impl FnOnce(&ResourceStateView<'_, '_>) -> R,
    ) -> R {
        self.snapshots_by_id
            .read(transaction_id, |snapshots_by_id| {
                self.ids_by_lookup_key
                    .read(transaction_id, |ids_by_lookup_key| {
                        read(&ResourceStateView {
                            snapshots_by_id,
                            ids_by_lookup_key,
                        })
                    })
            })
    }

    /// Changes both maps under their locks, taken in the same order as a commit
    /// takes them
    pub(crate) fn write<R>(
        &self,
        transaction_id: Option<TransactionId>,
        write: impl FnOnce(
            &mut InMemoryMapWrite<'_, ResourceID, ResourceSnapshot>,
            &mut InMemoryMapWrite<'_, ResourceLookupKey, ResourceID>,
        ) -> R,
    ) -> R {
        self.snapshots_by_id
            .write(transaction_id, |snapshots_by_id| {
                self.ids_by_lookup_key
                    .write(transaction_id, |ids_by_lookup_key| {
                        write(snapshots_by_id, ids_by_lookup_key)
                    })
            })
    }
}

in_memory_transaction_participant!(InMemoryResourceState, snapshots_by_id, ids_by_lookup_key);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The resources one transaction sees
pub(crate) struct ResourceStateView<'v, 'a> {
    snapshots_by_id: &'v InMemoryMapView<'a, ResourceID, ResourceSnapshot>,
    ids_by_lookup_key: &'v InMemoryMapView<'a, ResourceLookupKey, ResourceID>,
}

impl<'a> ResourceStateView<'_, 'a> {
    pub(crate) fn snapshot(&self, id: &ResourceID) -> Option<&'a ResourceSnapshot> {
        self.snapshots_by_id.get(id)
    }

    pub(crate) fn snapshots(&self) -> impl Iterator<Item = &'a ResourceSnapshot> + 'a {
        self.snapshots_by_id.iter().map(|(_, snapshot)| snapshot)
    }

    /// The live resource holding the name
    pub(crate) fn live_snapshot_by_name(
        &self,
        lookup_key: &ResourceLookupKey,
    ) -> Option<&'a ResourceSnapshot> {
        self.ids_by_lookup_key
            .get(lookup_key)
            .and_then(|id| self.snapshots_by_id.get(id))
            .filter(|snapshot| snapshot.headers.deletion_requested_at.is_none())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
