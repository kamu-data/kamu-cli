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
    ResourceName,
    ResourcePhaseCounts,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceSnapshotStream,
    ResourceSummaryRow,
    ResourceUID,
    ResourceUIDStream,
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
    snapshots_by_id: HashMap<ResourceUID, ResourceSnapshot>,
    ids_by_lookup_key: HashMap<ResourceLookupKey, ResourceUID>,
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
    async fn new_resource_uid(&self) -> Result<ResourceUID, InternalError> {
        Ok(ResourceUID::new(uuid::Uuid::new_v4()))
    }

    async fn create_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
    ) -> Result<(), CreateResourceError> {
        let mut guard = self.state.lock().unwrap();

        let lookup_key = ResourceLookupKey {
            account_id: resource_snapshot.metadata.account.clone(),
            kind: resource_snapshot.kind.clone(),
            name: resource_snapshot.metadata.name.clone(),
        };

        if guard.snapshots_by_id.contains_key(&resource_snapshot.uid)
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
            .insert(lookup_key, resource_snapshot.uid);
        guard
            .snapshots_by_id
            .insert(resource_snapshot.uid, resource_snapshot.clone());

        Ok(())
    }

    async fn update_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
        expected_last_event_id: Option<EventID>,
    ) -> Result<(), UpdateResourceError> {
        let mut guard = self.state.lock().unwrap();

        let previous_snapshot = guard
            .snapshots_by_id
            .get(&resource_snapshot.uid)
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
            && *existing_resource_id != resource_snapshot.uid
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
            .insert(next_lookup_key, resource_snapshot.uid);
        guard
            .snapshots_by_id
            .insert(resource_snapshot.uid, resource_snapshot.clone());

        Ok(())
    }

    async fn find_resource_uid_by_name(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
        name: &ResourceName,
    ) -> Result<Option<ResourceUID>, InternalError> {
        let guard = self.state.lock().unwrap();

        Ok(guard
            .ids_by_lookup_key
            .get(&ResourceLookupKey {
                account_id: account_id.clone(),
                kind: kind.to_owned(),
                name: name.clone(),
            })
            .and_then(|uid| guard.snapshots_by_id.get(uid))
            .filter(|snapshot| snapshot.metadata.deleted_at.is_none())
            .map(|snapshot| snapshot.uid))
    }

    async fn find_resource_snapshot(
        &self,
        query: &ResourceRawEventQuery,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        let guard = self.state.lock().unwrap();
        Ok(guard
            .snapshots_by_id
            .get(&query.uid)
            .filter(|snapshot| snapshot.kind == query.kind)
            .filter(|snapshot| snapshot.metadata.deleted_at.is_none())
            .cloned())
    }

    async fn find_resource_snapshot_by_uid(
        &self,
        uid: &ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        let guard = self.state.lock().unwrap();

        Ok(guard
            .snapshots_by_id
            .get(uid)
            .filter(|snapshot| snapshot.metadata.deleted_at.is_none())
            .cloned())
    }

    fn list_resource_uids(
        &self,
        account_id: odf::AccountID,
        kind: &str,
        pagination: PaginationOpts,
    ) -> ResourceUIDStream<'_> {
        let mut resource_ids_page: Vec<_> = {
            let guard = self.state.lock().unwrap();
            guard
                .snapshots_by_id
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
                .then_with(|| rhs.uid.cmp(&lhs.uid))
        });

        let resource_ids_page: Vec<_> = resource_ids_page
            .into_iter()
            .skip(pagination.offset)
            .take(pagination.limit)
            .map(|snapshot| Ok(snapshot.uid))
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
                .snapshots_by_id
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
                .then_with(|| rhs.uid.cmp(&lhs.uid))
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
                .snapshots_by_id
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
                .then_with(|| rhs.uid.cmp(&lhs.uid))
        });

        let snapshots_page: Vec<_> = snapshots_page
            .into_iter()
            .skip(pagination.offset)
            .take(pagination.limit)
            .map(Ok)
            .collect();

        Box::pin(futures::stream::iter(snapshots_page))
    }

    async fn count_resources(
        &self,
        account_id: odf::AccountID,
        kind: &str,
    ) -> Result<usize, InternalError> {
        let guard = self.state.lock().unwrap();

        Ok(guard
            .snapshots_by_id
            .values()
            .filter(|snapshot| {
                snapshot.metadata.account == account_id
                    && snapshot.kind == kind
                    && snapshot.metadata.deleted_at.is_none()
            })
            .count())
    }

    async fn summarize_resources(
        &self,
        account_id: odf::AccountID,
    ) -> Result<Vec<ResourceSummaryRow>, InternalError> {
        let guard = self.state.lock().unwrap();

        let mut rows_by_key = HashMap::<(String, String), ResourceSummaryRow>::new();

        for snapshot in guard.snapshots_by_id.values().filter(|snapshot| {
            snapshot.metadata.account == account_id && snapshot.metadata.deleted_at.is_none()
        }) {
            let row = rows_by_key
                .entry((snapshot.kind.clone(), snapshot.api_version.clone()))
                .or_insert_with(|| ResourceSummaryRow {
                    kind: snapshot.kind.clone(),
                    api_version: snapshot.api_version.clone(),
                    total_count: 0,
                    phase_counts: ResourcePhaseCounts::default(),
                });

            row.total_count += 1;

            match snapshot
                .status
                .as_ref()
                .and_then(|status| status.get("phase"))
                .and_then(|phase| phase.as_str())
            {
                Some("Reconciling") => row.phase_counts.increment_reconciling(),
                Some("Ready") => row.phase_counts.increment_ready(),
                Some("Degraded") => row.phase_counts.increment_degraded(),
                Some("Failed") => row.phase_counts.increment_failed(),
                _ => row.phase_counts.increment_pending(),
            }
        }

        let mut rows = rows_by_key.into_values().collect::<Vec<_>>();
        rows.sort_by(|lhs, rhs| {
            lhs.kind
                .cmp(&rhs.kind)
                .then_with(|| lhs.api_version.cmp(&rhs.api_version))
        });

        Ok(rows)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
