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
use dill::{Singleton, component, interface, scope};
use internal_error::InternalError;
use kamu_configuration::{
    ReplaceProjectionEntriesError,
    VariableSetEntry,
    VariableSetProjectionRepository,
};
use kamu_resources::{ResourceID, ResourceRemovalListener};

use crate::{ProjectionEntries, ProjectionEntriesMap};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryVariableSetProjectionState {
    entries_by_resource_id_generation: ProjectionEntriesMap<VariableSetEntry>,
}

#[component(pub)]
#[interface(dyn InMemoryTransactionParticipant)]
#[scope(Singleton)]
impl InMemoryVariableSetProjectionState {
    pub fn new() -> Self {
        Self {
            entries_by_resource_id_generation: InMemoryTransactionalMap::new(),
        }
    }
}

in_memory_transaction_participant!(
    InMemoryVariableSetProjectionState,
    entries_by_resource_id_generation
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryVariableSetProjectionRepository {
    state: Arc<InMemoryVariableSetProjectionState>,
    transaction_id: Option<TransactionId>,
}

#[component(pub)]
#[interface(dyn VariableSetProjectionRepository)]
#[interface(dyn ResourceRemovalListener)]
impl InMemoryVariableSetProjectionRepository {
    pub fn new(
        state: Arc<InMemoryVariableSetProjectionState>,
        transaction_ref: Option<TransactionRef>,
    ) -> Self {
        Self {
            state,
            transaction_id: transaction_ref.map(|transaction_ref| transaction_ref.id()),
        }
    }

    fn entries(&self) -> ProjectionEntries<'_, VariableSetEntry> {
        ProjectionEntries::new(
            &self.state.entries_by_resource_id_generation,
            self.transaction_id,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl VariableSetProjectionRepository for InMemoryVariableSetProjectionRepository {
    async fn find_entry(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
        key: &str,
    ) -> Result<Option<VariableSetEntry>, InternalError> {
        Ok(self
            .entries()
            .find_entry(resource_id, resource_generation, |entry| entry.key == key))
    }

    async fn get_entries(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
    ) -> Result<Vec<VariableSetEntry>, InternalError> {
        Ok(self.entries().get_entries(resource_id, resource_generation))
    }

    async fn get_latest_entries(
        &self,
        resource_id: &ResourceID,
    ) -> Result<Vec<VariableSetEntry>, InternalError> {
        Ok(self.entries().get_latest_entries(resource_id, None))
    }

    async fn get_latest_entries_before_generation(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
    ) -> Result<Vec<VariableSetEntry>, InternalError> {
        Ok(self
            .entries()
            .get_latest_entries(resource_id, Some(resource_generation)))
    }

    async fn replace_entries(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
        entries: &[VariableSetEntry],
    ) -> Result<(), ReplaceProjectionEntriesError> {
        self.entries()
            .replace_entries(resource_id, resource_generation, entries)
    }

    async fn cleanup_entries_before_generation(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
    ) -> Result<(), InternalError> {
        self.entries()
            .cleanup_entries_before_generation(resource_id, resource_generation);
        Ok(())
    }

    async fn delete_all_entries(&self, resource_ids: &[ResourceID]) -> Result<(), InternalError> {
        self.entries().delete_all_entries(resource_ids);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Drops the entries of purged resources, as the cascade of the SQL backends does
#[async_trait::async_trait]
impl ResourceRemovalListener for InMemoryVariableSetProjectionRepository {
    async fn on_resources_removed(&self, ids: &[ResourceID]) -> Result<(), InternalError> {
        self.delete_all_entries(ids).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
