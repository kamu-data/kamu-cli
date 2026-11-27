// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::{
    InMemoryTransactionParticipant,
    InMemoryTransactionalMap,
    TransactionId,
    TransactionRef,
    in_memory_transaction_participant,
};
use dill::*;
use internal_error::InternalError;
use kamu_resources::{ResourceID, ResourceOwnerProjectionRepository, ResourceRemovalListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryResourceOwnerProjectionState {
    owners_by_resource_id: InMemoryTransactionalMap<ResourceID, Vec<ResourceID>>,
}

#[component(pub)]
#[interface(dyn InMemoryTransactionParticipant)]
#[scope(Singleton)]
impl InMemoryResourceOwnerProjectionState {
    pub fn new() -> Self {
        Self {
            owners_by_resource_id: InMemoryTransactionalMap::new(),
        }
    }
}

in_memory_transaction_participant!(InMemoryResourceOwnerProjectionState, owners_by_resource_id);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryResourceOwnerProjectionRepository {
    state: Arc<InMemoryResourceOwnerProjectionState>,
    transaction_id: Option<TransactionId>,
}

#[component(pub)]
#[interface(dyn ResourceOwnerProjectionRepository)]
#[interface(dyn ResourceRemovalListener)]
impl InMemoryResourceOwnerProjectionRepository {
    pub fn new(
        state: Arc<InMemoryResourceOwnerProjectionState>,
        transaction_ref: Option<TransactionRef>,
    ) -> Self {
        Self {
            state,
            transaction_id: transaction_ref.map(|transaction_ref| transaction_ref.id()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceOwnerProjectionRepository for InMemoryResourceOwnerProjectionRepository {
    async fn replace_owners(
        &self,
        resource_id: &ResourceID,
        owner_ids: &[ResourceID],
    ) -> Result<(), InternalError> {
        self.state
            .owners_by_resource_id
            .write(self.transaction_id, |owners_by_resource_id| {
                if owner_ids.is_empty() {
                    owners_by_resource_id.delete(*resource_id);
                } else {
                    let mut sorted_ids = owner_ids.to_vec();
                    sorted_ids.sort();
                    owners_by_resource_id.put(*resource_id, sorted_ids);
                }
            });

        Ok(())
    }

    async fn find_owners(
        &self,
        resource_id: &ResourceID,
    ) -> Result<Vec<ResourceID>, InternalError> {
        Ok(self
            .state
            .owners_by_resource_id
            .read(self.transaction_id, |owners_by_resource_id| {
                owners_by_resource_id
                    .get(resource_id)
                    .cloned()
                    .unwrap_or_default()
            }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Drops the owners of purged resources, as the cascade of the SQL backends does
#[async_trait::async_trait]
impl ResourceRemovalListener for InMemoryResourceOwnerProjectionRepository {
    async fn on_resources_removed(&self, ids: &[ResourceID]) -> Result<(), InternalError> {
        self.state
            .owners_by_resource_id
            .write(self.transaction_id, |owners_by_resource_id| {
                owners_by_resource_id.delete_where(|id| ids.contains(id));
            });
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
