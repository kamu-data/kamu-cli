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
    ResourceRow,
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
    rows_by_query: HashMap<ResourceRawEventQuery, ResourceRow>,
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

    async fn create_resource(&self, resource_row: &ResourceRow) -> Result<(), CreateResourceError> {
        let mut guard = self.state.lock().unwrap();

        let query = ResourceRawEventQuery {
            kind: resource_row.kind.clone(),
            id: resource_row.resource_id,
        };
        let lookup_key = ResourceLookupKey {
            account_id: resource_row.account_id.clone(),
            kind: resource_row.kind.clone(),
            name: resource_row.name.clone(),
        };

        if guard.rows_by_query.contains_key(&query)
            || guard.ids_by_lookup_key.contains_key(&lookup_key)
        {
            return Err(CreateResourceError::Duplicate(ResourceDuplicateError {
                account_id: resource_row.account_id.clone(),
                kind: resource_row.kind.clone(),
                name: resource_row.name.clone(),
            }));
        }

        guard
            .ids_by_lookup_key
            .insert(lookup_key, resource_row.resource_id);
        guard.rows_by_query.insert(query, resource_row.clone());

        Ok(())
    }

    async fn update_resource(
        &self,
        resource_row: &ResourceRow,
        expected_last_event_id: Option<EventID>,
    ) -> Result<(), UpdateResourceError> {
        let mut guard = self.state.lock().unwrap();

        let query = ResourceRawEventQuery {
            kind: resource_row.kind.clone(),
            id: resource_row.resource_id,
        };

        let previous_row = guard
            .rows_by_query
            .get(&query)
            .cloned()
            .ok_or_else(UpdateResourceError::concurrent_modification)?;

        if previous_row.last_event_id != expected_last_event_id {
            return Err(UpdateResourceError::concurrent_modification());
        }

        let previous_lookup_key = ResourceLookupKey {
            account_id: previous_row.account_id,
            kind: previous_row.kind,
            name: previous_row.name,
        };
        let next_lookup_key = ResourceLookupKey {
            account_id: resource_row.account_id.clone(),
            kind: resource_row.kind.clone(),
            name: resource_row.name.clone(),
        };

        if let Some(existing_resource_id) = guard.ids_by_lookup_key.get(&next_lookup_key)
            && *existing_resource_id != resource_row.resource_id
        {
            return Err(UpdateResourceError::Duplicate(ResourceDuplicateError {
                account_id: resource_row.account_id.clone(),
                kind: resource_row.kind.clone(),
                name: resource_row.name.clone(),
            }));
        }

        guard.ids_by_lookup_key.remove(&previous_lookup_key);
        guard
            .ids_by_lookup_key
            .insert(next_lookup_key, resource_row.resource_id);
        guard.rows_by_query.insert(query, resource_row.clone());

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
            .copied())
    }

    async fn get_resource_row(
        &self,
        query: &ResourceRawEventQuery,
    ) -> Result<Option<ResourceRow>, InternalError> {
        let guard = self.state.lock().unwrap();
        Ok(guard.rows_by_query.get(query).cloned())
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
                .rows_by_query
                .values()
                .filter(|row| row.account_id == account_id && row.kind == kind)
                .cloned()
                .collect()
        };

        resource_ids_page.sort_by(|lhs, rhs| {
            rhs.updated_at
                .cmp(&lhs.updated_at)
                .then_with(|| rhs.resource_id.cmp(&lhs.resource_id))
        });

        let resource_ids_page: Vec<_> = resource_ids_page
            .into_iter()
            .skip(pagination.offset)
            .take(pagination.limit)
            .map(|row| Ok(row.resource_id))
            .collect();

        Box::pin(futures::stream::iter(resource_ids_page))
    }

    async fn get_count_resources(
        &self,
        account_id: odf::AccountID,
        kind: &str,
    ) -> Result<usize, InternalError> {
        let guard = self.state.lock().unwrap();

        Ok(guard
            .rows_by_query
            .values()
            .filter(|row| row.account_id == account_id && row.kind == kind)
            .count())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
