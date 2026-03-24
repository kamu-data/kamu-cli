// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use database_common::PaginationOpts;
use dill::*;
use event_sourcing::EventID;
use internal_error::InternalError;
use kamu_resources::{
    CreateResourceError,
    ResourceDuplicateError,
    ResourceID,
    ResourceIDStream,
    ResourceName,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceSnapshotStream,
    UpdateResourceError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ResourceLookupKey {
    account_id: odf::AccountID,
    kind: String,
    name: ResourceName,
}

#[derive(Default)]
struct State {
    snapshots_by_query: HashMap<ResourceRawEventQuery, ResourceSnapshot>,
    ids_by_lookup_key: HashMap<ResourceLookupKey, ResourceID>,
}

pub struct InMemoryResourceRepository {
    state: Arc<Mutex<State>>,
}

#[component(pub)]
#[interface(dyn ResourceRepository)]
#[scope(Singleton)]
impl InMemoryResourceRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceRepository for InMemoryResourceRepository {
    async fn new_resource_id(&self) -> Result<ResourceID, InternalError> {
        Ok(ResourceID::new_v4())
    }

    async fn create_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
    ) -> Result<(), CreateResourceError> {
        let mut guard = self.state.lock().unwrap();

        let query = ResourceRawEventQuery {
            kind: resource_snapshot.kind.clone(),
            id: resource_snapshot.resource_id,
        };
        let lookup_key = ResourceLookupKey {
            account_id: resource_snapshot.metadata.account.clone(),
            kind: resource_snapshot.kind.clone(),
            name: resource_snapshot.metadata.name.clone(),
        };

        if guard.snapshots_by_query.contains_key(&query)
            || guard.ids_by_lookup_key.contains_key(&lookup_key)
        {
            return Err(CreateResourceError::Duplicate(ResourceDuplicateError {
                account_id: resource_snapshot.metadata.account.clone(),
                kind: resource_snapshot.kind.clone(),
                name: resource_snapshot.metadata.name.clone(),
            }));
        }

        guard
            .ids_by_lookup_key
            .insert(lookup_key, resource_snapshot.resource_id);
        guard
            .snapshots_by_query
            .insert(query, resource_snapshot.clone());

        Ok(())
    }

    async fn update_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
        expected_last_event_id: Option<EventID>,
    ) -> Result<(), UpdateResourceError> {
        let mut guard = self.state.lock().unwrap();

        let query = ResourceRawEventQuery {
            kind: resource_snapshot.kind.clone(),
            id: resource_snapshot.resource_id,
        };

        let previous_snapshot = guard
            .snapshots_by_query
            .get(&query)
            .cloned()
            .ok_or_else(UpdateResourceError::concurrent_modification)?;

        if previous_snapshot.last_event_id != expected_last_event_id {
            return Err(UpdateResourceError::concurrent_modification());
        }

        let previous_lookup_key = ResourceLookupKey {
            account_id: previous_snapshot.metadata.account,
            kind: previous_snapshot.kind,
            name: previous_snapshot.metadata.name,
        };
        let next_lookup_key = ResourceLookupKey {
            account_id: resource_snapshot.metadata.account.clone(),
            kind: resource_snapshot.kind.clone(),
            name: resource_snapshot.metadata.name.clone(),
        };

        if let Some(existing_resource_id) = guard.ids_by_lookup_key.get(&next_lookup_key)
            && *existing_resource_id != resource_snapshot.resource_id
        {
            return Err(UpdateResourceError::Duplicate(ResourceDuplicateError {
                account_id: resource_snapshot.metadata.account.clone(),
                kind: resource_snapshot.kind.clone(),
                name: resource_snapshot.metadata.name.clone(),
            }));
        }

        guard.ids_by_lookup_key.remove(&previous_lookup_key);
        guard
            .ids_by_lookup_key
            .insert(next_lookup_key, resource_snapshot.resource_id);
        guard
            .snapshots_by_query
            .insert(query, resource_snapshot.clone());

        Ok(())
    }

    async fn get_resource_id_by_name(
        &self,
        account_id: odf::AccountID,
        kind: &str,
        name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError> {
        let guard = self.state.lock().unwrap();

        Ok(guard
            .ids_by_lookup_key
            .get(&ResourceLookupKey {
                account_id,
                kind: kind.to_owned(),
                name: name.clone(),
            })
            .and_then(|resource_id| {
                guard
                    .snapshots_by_query
                    .values()
                    .find(|snapshot| {
                        snapshot.resource_id == *resource_id
                            && snapshot.metadata.deleted_at.is_none()
                    })
                    .map(|snapshot| snapshot.resource_id)
            }))
    }

    async fn get_resource_snapshot(
        &self,
        query: &ResourceRawEventQuery,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        let guard = self.state.lock().unwrap();
        Ok(guard
            .snapshots_by_query
            .get(query)
            .filter(|snapshot| snapshot.metadata.deleted_at.is_none())
            .cloned())
    }

    fn list_resource_ids(
        &self,
        account_id: odf::AccountID,
        kind: &str,
        pagination: PaginationOpts,
    ) -> ResourceIDStream<'_> {
        let mut resource_ids_page: Vec<_> = {
            let guard = self.state.lock().unwrap();
            guard
                .snapshots_by_query
                .values()
                .filter(|snapshot| {
                    snapshot.metadata.account == account_id
                        && snapshot.kind == kind
                        && snapshot.metadata.deleted_at.is_none()
                })
                .cloned()
                .collect()
        };

        resource_ids_page.sort_by(|lhs, rhs| {
            rhs.metadata
                .updated_at
                .cmp(&lhs.metadata.updated_at)
                .then_with(|| rhs.resource_id.cmp(&lhs.resource_id))
        });

        let resource_ids_page: Vec<_> = resource_ids_page
            .into_iter()
            .skip(pagination.offset)
            .take(pagination.limit)
            .map(|snapshot| Ok(snapshot.resource_id))
            .collect();

        Box::pin(futures::stream::iter(resource_ids_page))
    }

    fn list_resource_snapshots_by_kind(
        &self,
        account_id: odf::AccountID,
        kind: &str,
        pagination: PaginationOpts,
    ) -> ResourceSnapshotStream<'_> {
        let mut snapshots_page: Vec<_> = {
            let guard = self.state.lock().unwrap();
            guard
                .snapshots_by_query
                .values()
                .filter(|snapshot| {
                    snapshot.metadata.account == account_id
                        && snapshot.kind == kind
                        && snapshot.metadata.deleted_at.is_none()
                })
                .cloned()
                .collect()
        };

        snapshots_page.sort_by(|lhs, rhs| {
            rhs.metadata
                .updated_at
                .cmp(&lhs.metadata.updated_at)
                .then_with(|| rhs.resource_id.cmp(&lhs.resource_id))
        });

        let snapshots_page: Vec<_> = snapshots_page
            .into_iter()
            .skip(pagination.offset)
            .take(pagination.limit)
            .map(Ok)
            .collect();

        Box::pin(futures::stream::iter(snapshots_page))
    }

    fn list_all_resource_snapshots(
        &self,
        account_id: odf::AccountID,
        pagination: PaginationOpts,
    ) -> ResourceSnapshotStream<'_> {
        let mut snapshots_page: Vec<_> = {
            let guard = self.state.lock().unwrap();
            guard
                .snapshots_by_query
                .values()
                .filter(|snapshot| {
                    snapshot.metadata.account == account_id
                        && snapshot.metadata.deleted_at.is_none()
                })
                .cloned()
                .collect()
        };

        snapshots_page.sort_by(|lhs, rhs| {
            rhs.metadata
                .updated_at
                .cmp(&lhs.metadata.updated_at)
                .then_with(|| rhs.resource_id.cmp(&lhs.resource_id))
        });

        let snapshots_page: Vec<_> = snapshots_page
            .into_iter()
            .skip(pagination.offset)
            .take(pagination.limit)
            .map(Ok)
            .collect();

        Box::pin(futures::stream::iter(snapshots_page))
    }

    async fn get_count_resources(
        &self,
        account_id: odf::AccountID,
        kind: &str,
    ) -> Result<usize, InternalError> {
        let guard = self.state.lock().unwrap();

        Ok(guard
            .snapshots_by_query
            .values()
            .filter(|snapshot| {
                snapshot.metadata.account == account_id
                    && snapshot.kind == kind
                    && snapshot.metadata.deleted_at.is_none()
            })
            .count())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
