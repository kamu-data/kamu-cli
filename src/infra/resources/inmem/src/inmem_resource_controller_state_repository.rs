// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use database_common::{
    InMemoryTransactionParticipant,
    InMemoryTransactionalMap,
    TransactionId,
    TransactionRef,
    in_memory_transaction_participant,
};
use dill::*;
use internal_error::InternalError;
use kamu_resources::{
    ActionableResourcesQuery,
    ResourceControllerName,
    ResourceControllerState,
    ResourceControllerStateRepository,
    ResourceID,
    ResourceSnapshot,
};

use crate::InMemoryResourceState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type ResourceControllerStateKey = (ResourceControllerName, ResourceID);

pub struct InMemoryResourceControllerStateStore {
    states: InMemoryTransactionalMap<ResourceControllerStateKey, ResourceControllerState>,
}

#[component(pub)]
#[interface(dyn InMemoryTransactionParticipant)]
#[scope(Singleton)]
impl InMemoryResourceControllerStateStore {
    pub fn new() -> Self {
        Self {
            states: InMemoryTransactionalMap::new(),
        }
    }
}

in_memory_transaction_participant!(InMemoryResourceControllerStateStore, states);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryResourceControllerStateRepository {
    store: Arc<InMemoryResourceControllerStateStore>,
    resource_state: Arc<InMemoryResourceState>,
    transaction_id: Option<TransactionId>,
}

#[component(pub)]
#[interface(dyn ResourceControllerStateRepository)]
impl InMemoryResourceControllerStateRepository {
    pub fn new(
        store: Arc<InMemoryResourceControllerStateStore>,
        resource_state: Arc<InMemoryResourceState>,
        transaction_ref: Option<TransactionRef>,
    ) -> Self {
        Self {
            store,
            resource_state,
            transaction_id: transaction_ref.map(|transaction_ref| transaction_ref.id()),
        }
    }

    /// The resources of the queried schema that match a predicate, each with
    /// the controller's row, if any
    fn matching_resources(&self, query: &ActionableResourcesQuery) -> Vec<MatchingResource> {
        // A commit holds every participant's lock, taken in registration order,
        // and `register_dependencies` registers the resource state before the controller state
        // store. Locking in another order deadlocks against a concurrent commit
        self.resource_state.read(self.transaction_id, |resources| {
            self.store.states.read(self.transaction_id, |states| {
                resources
                    .snapshots()
                    .filter(|snapshot| snapshot.schema == query.schema)
                    .filter_map(|snapshot| {
                        let state = states.get(&(query.controller.clone(), snapshot.id));
                        matches_predicate(snapshot, state, query).then(|| MatchingResource {
                            resource_id: snapshot.id,
                            generation: snapshot.headers.generation,
                            status_generation: snapshot.headers.status_generation,
                            state: state.cloned(),
                        })
                    })
                    .collect()
            })
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MatchingResource {
    resource_id: ResourceID,
    generation: u64,
    status_generation: u64,
    state: Option<ResourceControllerState>,
}

impl MatchingResource {
    fn is_suppressed(&self, query: &ActionableResourcesQuery, now: DateTime<Utc>) -> bool {
        let Some(state) = &self.state else {
            return false;
        };
        match state.not_before {
            Some(not_before) => not_before > now,
            None => {
                state.controller_revision == query.controller_revision
                    && state.handled_generation == Some(self.generation)
                    && state.handled_status_generation == Some(self.status_generation)
            }
        }
    }
}

fn matches_predicate(
    snapshot: &ResourceSnapshot,
    state: Option<&ResourceControllerState>,
    query: &ActionableResourcesQuery,
) -> bool {
    let headers = &snapshot.headers;
    if headers.deleted_at.is_some() {
        return false;
    }

    let reconciled_generation = snapshot
        .status
        .as_ref()
        .and_then(|status| status.reconciled_generation)
        .unwrap_or(0);
    let not_reconciled =
        headers.deletion_requested_at.is_none() && reconciled_generation < headers.generation;
    let deletion_pending = headers.deletion_requested_at.is_some();
    let status_changed = query.reacts_to_status
        && state.and_then(|state| state.handled_status_generation)
            != Some(headers.status_generation);

    not_reconciled || deletion_pending || status_changed
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceControllerStateRepository for InMemoryResourceControllerStateRepository {
    async fn get_state(
        &self,
        controller: &ResourceControllerName,
        resource_id: &ResourceID,
    ) -> Result<Option<ResourceControllerState>, InternalError> {
        Ok(self.store.states.read(self.transaction_id, |states| {
            states.get(&(controller.clone(), *resource_id)).cloned()
        }))
    }

    async fn upsert_state(&self, state: &ResourceControllerState) -> Result<(), InternalError> {
        // The foreign key of the database storages. Locks in the order of
        // `matching_resources`
        self.resource_state.read(self.transaction_id, |resources| {
            if resources.snapshot(&state.resource_id).is_none() {
                return Err(InternalError::new(format!(
                    "Resource {} not found for the state of controller {}",
                    state.resource_id,
                    state.controller.as_str()
                )));
            }
            self.store.states.write(self.transaction_id, |states| {
                states.overwrite((state.controller.clone(), state.resource_id), state.clone());
            });
            Ok(())
        })
    }

    async fn delete_states_for_resource(
        &self,
        resource_id: &ResourceID,
    ) -> Result<(), InternalError> {
        self.store.states.write(self.transaction_id, |states| {
            states.delete_where(|(_, state_resource_id)| state_resource_id == resource_id);
        });

        Ok(())
    }

    async fn find_actionable_resource_ids(
        &self,
        query: &ActionableResourcesQuery,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<ResourceID>, InternalError> {
        let mut resource_ids: Vec<ResourceID> = self
            .matching_resources(query)
            .into_iter()
            .filter(|matching| !matching.is_suppressed(query, now))
            .map(|matching| matching.resource_id)
            .collect();
        resource_ids.sort();
        resource_ids.truncate(limit);

        Ok(resource_ids)
    }

    async fn find_next_deadline(
        &self,
        query: &ActionableResourcesQuery,
    ) -> Result<Option<DateTime<Utc>>, InternalError> {
        Ok(self
            .matching_resources(query)
            .into_iter()
            .filter_map(|matching| matching.state.and_then(|state| state.not_before))
            .min())
    }

    async fn count_failing_resources(
        &self,
        controller: &ResourceControllerName,
    ) -> Result<usize, InternalError> {
        // Locks in the order of `matching_resources`
        Ok(self.resource_state.read(self.transaction_id, |resources| {
            self.store.states.read(self.transaction_id, |states| {
                resources
                    .snapshots()
                    .filter(|snapshot| snapshot.headers.deleted_at.is_none())
                    .filter(|snapshot| {
                        states
                            .get(&(controller.clone(), snapshot.id))
                            .is_some_and(|state| state.attempts > 0)
                    })
                    .count()
            })
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
