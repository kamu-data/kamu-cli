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
    SecretSetEntry,
    SecretSetProjectionRepository,
};
use kamu_resources::{ResourceID, ResourceRemovalListener};

use crate::{ProjectionEntries, ProjectionEntriesMap};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemorySecretSetProjectionState {
    entries_by_resource_id_generation: ProjectionEntriesMap<SecretSetEntry>,
}

#[component(pub)]
#[interface(dyn InMemoryTransactionParticipant)]
#[scope(Singleton)]
impl InMemorySecretSetProjectionState {
    pub fn new() -> Self {
        Self {
            entries_by_resource_id_generation: InMemoryTransactionalMap::new(),
        }
    }
}

in_memory_transaction_participant!(
    InMemorySecretSetProjectionState,
    entries_by_resource_id_generation
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemorySecretSetProjectionRepository {
    state: Arc<InMemorySecretSetProjectionState>,
    transaction_id: Option<TransactionId>,
}

#[component(pub)]
#[interface(dyn SecretSetProjectionRepository)]
#[interface(dyn ResourceRemovalListener)]
impl InMemorySecretSetProjectionRepository {
    pub fn new(
        state: Arc<InMemorySecretSetProjectionState>,
        transaction_ref: Option<TransactionRef>,
    ) -> Self {
        Self {
            state,
            transaction_id: transaction_ref.map(|transaction_ref| transaction_ref.id()),
        }
    }

    fn entries(&self) -> ProjectionEntries<'_, SecretSetEntry> {
        ProjectionEntries::new(
            &self.state.entries_by_resource_id_generation,
            self.transaction_id,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SecretSetProjectionRepository for InMemorySecretSetProjectionRepository {
    async fn find_entry(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
        key: &str,
    ) -> Result<Option<SecretSetEntry>, InternalError> {
        Ok(self
            .entries()
            .find_entry(resource_id, resource_generation, |entry| entry.key == key))
    }

    async fn get_entries(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
    ) -> Result<Vec<SecretSetEntry>, InternalError> {
        Ok(self.entries().get_entries(resource_id, resource_generation))
    }

    async fn get_latest_entries(
        &self,
        resource_id: &ResourceID,
    ) -> Result<Vec<SecretSetEntry>, InternalError> {
        Ok(self.entries().get_latest_entries(resource_id, None))
    }

    async fn get_latest_entries_before_generation(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
    ) -> Result<Vec<SecretSetEntry>, InternalError> {
        Ok(self
            .entries()
            .get_latest_entries(resource_id, Some(resource_generation)))
    }

    async fn replace_entries(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
        entries: &[SecretSetEntry],
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
impl ResourceRemovalListener for InMemorySecretSetProjectionRepository {
    async fn on_resources_removed(&self, ids: &[ResourceID]) -> Result<(), InternalError> {
        self.delete_all_entries(ids).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
