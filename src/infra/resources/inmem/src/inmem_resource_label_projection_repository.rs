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
use kamu_resources::{ResourceID, ResourceLabelProjectionRepository};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryResourceLabelProjectionState {
    entries_by_resource_id: InMemoryTransactionalMap<ResourceID, Vec<(String, String)>>,
}

#[component(pub)]
#[interface(dyn InMemoryTransactionParticipant)]
#[scope(Singleton)]
impl InMemoryResourceLabelProjectionState {
    pub fn new() -> Self {
        Self {
            entries_by_resource_id: InMemoryTransactionalMap::new(),
        }
    }
}

in_memory_transaction_participant!(InMemoryResourceLabelProjectionState, entries_by_resource_id);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryResourceLabelProjectionRepository {
    state: Arc<InMemoryResourceLabelProjectionState>,
    transaction_id: Option<TransactionId>,
}

#[component(pub)]
#[interface(dyn ResourceLabelProjectionRepository)]
impl InMemoryResourceLabelProjectionRepository {
    pub fn new(
        state: Arc<InMemoryResourceLabelProjectionState>,
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
impl ResourceLabelProjectionRepository for InMemoryResourceLabelProjectionRepository {
    async fn replace_entries(
        &self,
        resource_id: &ResourceID,
        entries: &[(String, String)],
    ) -> Result<(), InternalError> {
        self.state
            .entries_by_resource_id
            .write(self.transaction_id, |entries_by_resource_id| {
                if entries.is_empty() {
                    entries_by_resource_id.delete(*resource_id);
                } else {
                    let mut sorted_entries = entries.to_vec();
                    sorted_entries.sort();
                    entries_by_resource_id.put(*resource_id, sorted_entries);
                }
            });

        Ok(())
    }

    async fn find_entries(
        &self,
        resource_id: &ResourceID,
    ) -> Result<Vec<(String, String)>, InternalError> {
        Ok(self
            .state
            .entries_by_resource_id
            .read(self.transaction_id, |entries_by_resource_id| {
                entries_by_resource_id
                    .get(resource_id)
                    .cloned()
                    .unwrap_or_default()
            }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
