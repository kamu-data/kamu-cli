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
use futures::StreamExt;
use internal_error::InternalError;
use kamu_accounts::AccountRepository;
use kamu_resources::{
    CreateResourceError,
    ResourceDuplicateError,
    ResourceHandleRow,
    ResourceID,
    ResourceIDStream,
    ResourceName,
    ResourcePhase,
    ResourcePhaseCounts,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceSnapshotStream,
    ResourceSnapshotUpdate,
    ResourceSummaryRow,
    TypeUri,
    UpdateResourceError,
    deleted_account_name_sentinel,
    deleted_account_resource_id_sentinel,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ResourceLookupKey {
    account_id: odf::AccountID,
    schema: TypeUri,
    name: ResourceName,
}

#[derive(Default)]
struct State {
    snapshots_by_id: HashMap<ResourceID, ResourceSnapshot>,
    ids_by_lookup_key: HashMap<ResourceLookupKey, ResourceID>,
}

pub struct InMemoryResourceRepository {
    state: Arc<Mutex<State>>,
    account_repository: Arc<dyn AccountRepository>,
}

#[component(pub)]
#[interface(dyn ResourceRepository)]
#[scope(Singleton)]
impl InMemoryResourceRepository {
    pub fn new(account_repository: Arc<dyn AccountRepository>) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            account_repository,
        }
    }

    /// Re-resolves the owning account's current resource id + name, live on
    /// every read — mirroring the postgres/sqlite backends' `LEFT JOIN
    /// accounts`. The stored snapshot only carries `account.did` as the durable
    /// identity; `account.id` (the account-resource id) and `account.name` are
    /// always refreshed here so account renames are reflected immediately and
    /// deletions never surface stale values. The account DID has no FK-cascade
    /// tie to the `accounts` table (different bounded context, async
    /// outbox-driven cleanup), so a missing account falls back to sentinels
    /// instead of failing the read.
    async fn resolve_snapshot(&self, mut snapshot: ResourceSnapshot) -> ResourceSnapshot {
        let (resource_id, name) = self
            .resolve_account_identity(&snapshot.headers.account.did)
            .await;
        snapshot.headers.account.id = resource_id;
        snapshot.headers.account.name = name;
        snapshot
    }

    /// Batched twin of [`Self::resolve_snapshot`] — resolves every snapshot's
    /// account name with a single `get_accounts_by_ids` call instead of one
    /// lookup per snapshot, mirroring the SQL backends' set-based `LEFT JOIN`
    /// (no N+1 across a multi-row read). An id missing from the batch result
    /// (deleted account) falls back to the sentinels per-snapshot, same as
    /// [`Self::resolve_account_identity`]. A hard `Internal` error from the
    /// lookup itself (no I/O in the inmem backend, so effectively
    /// unreachable in practice) is treated the same way rather than failing
    /// the whole read, to keep this method infallible like its single-item
    /// counterpart.
    async fn resolve_snapshots(&self, snapshots: Vec<ResourceSnapshot>) -> Vec<ResourceSnapshot> {
        let account_ids: Vec<&odf::AccountID> = snapshots
            .iter()
            .map(|snapshot| &snapshot.headers.account.did)
            .collect();

        let identity_by_did: HashMap<odf::AccountID, (odf::ResourceID, odf::AccountName)> = self
            .account_repository
            .get_accounts_by_ids(&account_ids)
            .await
            .map(|accounts| {
                accounts
                    .into_iter()
                    .map(|account| (account.id, (account.resource_id, account.account_name)))
                    .collect()
            })
            .unwrap_or_default();

        snapshots
            .into_iter()
            .map(|mut snapshot| {
                let (resource_id, name) = identity_by_did
                    .get(&snapshot.headers.account.did)
                    .cloned()
                    .unwrap_or_else(|| {
                        (
                            deleted_account_resource_id_sentinel(),
                            deleted_account_name_sentinel(),
                        )
                    });
                snapshot.headers.account.id = resource_id;
                snapshot.headers.account.name = name;
                snapshot
            })
            .collect()
    }

    /// Re-resolves the owning account's current resource id + name from the
    /// account repository, mirroring the SQL backends' `LEFT JOIN accounts`. A
    /// missing account (deleted, racing async cleanup) falls back to the
    /// sentinels rather than failing the read.
    async fn resolve_account_identity(
        &self,
        account_did: &odf::AccountID,
    ) -> (odf::ResourceID, odf::AccountName) {
        self.account_repository
            .get_account_by_id(account_did)
            .await
            .map(|account| (account.resource_id, account.account_name))
            .unwrap_or_else(|_| {
                (
                    deleted_account_resource_id_sentinel(),
                    deleted_account_name_sentinel(),
                )
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceRepository for InMemoryResourceRepository {
    async fn new_resource_id(&self) -> Result<ResourceID, InternalError> {
        Ok(ResourceID::new(uuid::Uuid::new_v4()))
    }

    async fn create_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
    ) -> Result<(), CreateResourceError> {
        let mut guard = self.state.lock().unwrap();

        let lookup_key = ResourceLookupKey {
            account_id: resource_snapshot.headers.account.did.clone(),
            schema: resource_snapshot.schema.clone(),
            name: resource_snapshot.headers.name.clone(),
        };

        if guard.snapshots_by_id.contains_key(&resource_snapshot.id)
            || guard.ids_by_lookup_key.contains_key(&lookup_key)
        {
            return Err(CreateResourceError::Duplicate(ResourceDuplicateError {
                account_id: resource_snapshot.headers.account.did.clone(),
                schema: resource_snapshot.schema.clone(),
                name: resource_snapshot.headers.name.clone(),
            }));
        }

        guard
            .ids_by_lookup_key
            .insert(lookup_key, resource_snapshot.id);
        guard
            .snapshots_by_id
            .insert(resource_snapshot.id, resource_snapshot.clone());

        Ok(())
    }

    async fn update_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
        expected_last_event_id: Option<EventID>,
    ) -> Result<(), UpdateResourceError> {
        let resource_update = ResourceSnapshotUpdate {
            snapshot: resource_snapshot.clone(),
            expected_last_event_id,
        };

        self.update_resources(std::slice::from_ref(&resource_update))
            .await
    }

    async fn update_resources(
        &self,
        resource_updates: &[ResourceSnapshotUpdate],
    ) -> Result<(), UpdateResourceError> {
        let mut guard = self.state.lock().unwrap();
        let mut prepared_updates = Vec::with_capacity(resource_updates.len());

        for resource_update in resource_updates {
            let resource_snapshot = &resource_update.snapshot;
            let previous_snapshot = guard
                .snapshots_by_id
                .get(&resource_snapshot.id)
                .cloned()
                .ok_or_else(UpdateResourceError::concurrent_modification)?;

            if previous_snapshot.last_event_id != resource_update.expected_last_event_id {
                return Err(UpdateResourceError::concurrent_modification());
            }

            let previous_lookup_key = ResourceLookupKey {
                account_id: previous_snapshot.headers.account.did,
                schema: previous_snapshot.schema,
                name: previous_snapshot.headers.name,
            };
            let next_lookup_key = ResourceLookupKey {
                account_id: resource_snapshot.headers.account.did.clone(),
                schema: resource_snapshot.schema.clone(),
                name: resource_snapshot.headers.name.clone(),
            };

            if let Some(existing_resource_id) = guard.ids_by_lookup_key.get(&next_lookup_key)
                && *existing_resource_id != resource_snapshot.id
            {
                return Err(UpdateResourceError::Duplicate(ResourceDuplicateError {
                    account_id: resource_snapshot.headers.account.did.clone(),
                    schema: resource_snapshot.schema.clone(),
                    name: resource_snapshot.headers.name.clone(),
                }));
            }

            prepared_updates.push((
                resource_snapshot.clone(),
                previous_lookup_key,
                next_lookup_key,
            ));
        }

        for (resource_snapshot, previous_lookup_key, next_lookup_key) in prepared_updates {
            guard.ids_by_lookup_key.remove(&previous_lookup_key);
            guard
                .ids_by_lookup_key
                .insert(next_lookup_key, resource_snapshot.id);
            guard
                .snapshots_by_id
                .insert(resource_snapshot.id, resource_snapshot);
        }

        Ok(())
    }

    async fn find_resource_id_by_name(
        &self,
        account_id: &odf::AccountID,
        schema: &TypeUri,
        name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError> {
        let guard = self.state.lock().unwrap();

        Ok(guard
            .ids_by_lookup_key
            .get(&ResourceLookupKey {
                account_id: account_id.clone(),
                schema: schema.clone(),
                name: name.clone(),
            })
            .and_then(|id| guard.snapshots_by_id.get(id))
            .filter(|snapshot| snapshot.headers.deleted_at.is_none())
            .map(|snapshot| snapshot.id))
    }

    async fn find_resource_handles_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceHandleRow>, InternalError> {
        let guard = self.state.lock().unwrap();

        Ok(ids
            .iter()
            .filter_map(|id| guard.snapshots_by_id.get(id))
            .filter(|snapshot| {
                snapshot.headers.account.did == *account_id && snapshot.headers.deleted_at.is_none()
            })
            .map(|snapshot| ResourceHandleRow {
                id: *snapshot.id.as_ref(),
                schema: snapshot.schema.to_string(),
                name: snapshot.headers.name.to_string(),
                account_id: snapshot.headers.account.did.clone(),
                account_resource_id: *snapshot.headers.account.id.as_ref(),
                account_name: snapshot.headers.account.name.to_string(),
            })
            .collect())
    }

    async fn find_resource_handles_by_names(
        &self,
        account_id: &odf::AccountID,
        schema: &TypeUri,
        names: &[ResourceName],
    ) -> Result<Vec<ResourceHandleRow>, InternalError> {
        let guard = self.state.lock().unwrap();

        Ok(names
            .iter()
            .filter_map(|name| {
                guard
                    .ids_by_lookup_key
                    .get(&ResourceLookupKey {
                        account_id: account_id.clone(),
                        schema: schema.clone(),
                        name: name.clone(),
                    })
                    .and_then(|id| guard.snapshots_by_id.get(id))
            })
            .filter(|snapshot| snapshot.headers.deleted_at.is_none())
            .map(|snapshot| ResourceHandleRow {
                id: *snapshot.id.as_ref(),
                schema: snapshot.schema.to_string(),
                name: snapshot.headers.name.to_string(),
                account_id: snapshot.headers.account.did.clone(),
                account_resource_id: *snapshot.headers.account.id.as_ref(),
                account_name: snapshot.headers.account.name.to_string(),
            })
            .collect())
    }

    async fn search_resource_handles(
        &self,
        account_id: &odf::AccountID,
        schemas: &[TypeUri],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceHandleRow>, InternalError> {
        if schemas.is_empty() || exact_names.is_some_and(<[ResourceName]>::is_empty) {
            return Ok(Vec::new());
        }

        let mut snapshots = {
            let guard = self.state.lock().unwrap();
            filter_search_snapshots(&guard, account_id, schemas, exact_names, name_pattern)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        };

        snapshots.sort_by(|lhs, rhs| {
            rhs.headers
                .updated_at
                .cmp(&lhs.headers.updated_at)
                .then_with(|| rhs.id.cmp(&lhs.id))
        });

        Ok(snapshots
            .into_iter()
            .skip(pagination.offset)
            .take(pagination.limit)
            .map(|snapshot| ResourceHandleRow {
                id: *snapshot.id.as_ref(),
                schema: snapshot.schema.to_string(),
                name: snapshot.headers.name.to_string(),
                account_id: snapshot.headers.account.did.clone(),
                account_resource_id: *snapshot.headers.account.id.as_ref(),
                account_name: snapshot.headers.account.name.to_string(),
            })
            .collect())
    }

    async fn count_search_resource_handles(
        &self,
        account_id: &odf::AccountID,
        schemas: &[TypeUri],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
    ) -> Result<usize, InternalError> {
        let guard = self.state.lock().unwrap();

        if schemas.is_empty() || exact_names.is_some_and(<[ResourceName]>::is_empty) {
            return Ok(0);
        }

        Ok(filter_search_snapshots(&guard, account_id, schemas, exact_names, name_pattern).len())
    }

    async fn find_resource_snapshot(
        &self,
        query: &ResourceRawEventQuery,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        let maybe_snapshot = {
            let guard = self.state.lock().unwrap();
            guard
                .snapshots_by_id
                .get(&query.id)
                .filter(|snapshot| snapshot.schema == query.schema)
                .filter(|snapshot| snapshot.headers.deleted_at.is_none())
                .cloned()
        };

        match maybe_snapshot {
            Some(snapshot) => Ok(Some(self.resolve_snapshot(snapshot).await)),
            None => Ok(None),
        }
    }

    async fn find_resource_snapshots_by_schema_and_ids(
        &self,
        schema: &TypeUri,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        let snapshots = {
            let guard = self.state.lock().unwrap();
            ids.iter()
                .filter_map(|id| guard.snapshots_by_id.get(id))
                .filter(|snapshot| snapshot.schema == *schema)
                .filter(|snapshot| snapshot.headers.deleted_at.is_none())
                .cloned()
                .collect::<Vec<_>>()
        };

        Ok(self.resolve_snapshots(snapshots).await)
    }

    async fn find_resource_snapshot_by_id(
        &self,
        id: &ResourceID,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        let maybe_snapshot = {
            let guard = self.state.lock().unwrap();
            guard
                .snapshots_by_id
                .get(id)
                .filter(|snapshot| snapshot.headers.deleted_at.is_none())
                .cloned()
        };

        match maybe_snapshot {
            Some(snapshot) => Ok(Some(self.resolve_snapshot(snapshot).await)),
            None => Ok(None),
        }
    }

    async fn find_resource_snapshots_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        let snapshots = {
            let guard = self.state.lock().unwrap();
            ids.iter()
                .filter_map(|id| guard.snapshots_by_id.get(id))
                .filter(|snapshot| {
                    snapshot.headers.account.did == *account_id
                        && snapshot.headers.deleted_at.is_none()
                })
                .cloned()
                .collect::<Vec<_>>()
        };

        Ok(self.resolve_snapshots(snapshots).await)
    }

    fn list_resource_ids(
        &self,
        account_id: odf::AccountID,
        schema: &TypeUri,
        pagination: PaginationOpts,
    ) -> ResourceIDStream<'_> {
        let mut resource_ids_page: Vec<_> = {
            let guard = self.state.lock().unwrap();
            guard
                .snapshots_by_id
                .values()
                .filter(|snapshot| {
                    snapshot.headers.account.did == account_id
                        && snapshot.schema == *schema
                        && snapshot.headers.deleted_at.is_none()
                })
                .cloned()
                .collect()
        };

        resource_ids_page.sort_by(|lhs, rhs| {
            rhs.headers
                .updated_at
                .cmp(&lhs.headers.updated_at)
                .then_with(|| rhs.id.cmp(&lhs.id))
        });

        let resource_ids_page: Vec<_> = resource_ids_page
            .into_iter()
            .skip(pagination.offset)
            .take(pagination.limit)
            .map(|snapshot| Ok(snapshot.id))
            .collect();

        Box::pin(futures::stream::iter(resource_ids_page))
    }

    fn list_resource_snapshots_by_schema(
        &self,
        account_id: odf::AccountID,
        schema: &TypeUri,
        pagination: PaginationOpts,
    ) -> ResourceSnapshotStream<'_> {
        let schema = schema.clone();

        let stream = futures::stream::once(async move {
            let mut snapshots_page: Vec<_> = {
                let guard = self.state.lock().unwrap();
                guard
                    .snapshots_by_id
                    .values()
                    .filter(|snapshot| {
                        snapshot.headers.account.did == account_id
                            && snapshot.schema == schema
                            && snapshot.headers.deleted_at.is_none()
                    })
                    .cloned()
                    .collect()
            };

            snapshots_page.sort_by(|lhs, rhs| {
                rhs.headers
                    .updated_at
                    .cmp(&lhs.headers.updated_at)
                    .then_with(|| rhs.id.cmp(&lhs.id))
            });

            let snapshots_page: Vec<_> = snapshots_page
                .into_iter()
                .skip(pagination.offset)
                .take(pagination.limit)
                .collect();

            self.resolve_snapshots(snapshots_page).await
        })
        .flat_map(|snapshots| futures::stream::iter(snapshots.into_iter().map(Ok)));

        Box::pin(stream)
    }

    fn list_all_resource_snapshots(
        &self,
        account_id: odf::AccountID,
        pagination: PaginationOpts,
    ) -> ResourceSnapshotStream<'_> {
        let stream = futures::stream::once(async move {
            let mut snapshots_page: Vec<_> = {
                let guard = self.state.lock().unwrap();
                guard
                    .snapshots_by_id
                    .values()
                    .filter(|snapshot| {
                        snapshot.headers.account.did == account_id
                            && snapshot.headers.deleted_at.is_none()
                    })
                    .cloned()
                    .collect()
            };

            snapshots_page.sort_by(|lhs, rhs| {
                rhs.headers
                    .updated_at
                    .cmp(&lhs.headers.updated_at)
                    .then_with(|| rhs.id.cmp(&lhs.id))
            });

            let snapshots_page: Vec<_> = snapshots_page
                .into_iter()
                .skip(pagination.offset)
                .take(pagination.limit)
                .collect();

            self.resolve_snapshots(snapshots_page).await
        })
        .flat_map(|snapshots| futures::stream::iter(snapshots.into_iter().map(Ok)));

        Box::pin(stream)
    }

    async fn count_resources(
        &self,
        account_id: odf::AccountID,
        schema: &TypeUri,
    ) -> Result<usize, InternalError> {
        let guard = self.state.lock().unwrap();

        Ok(guard
            .snapshots_by_id
            .values()
            .filter(|snapshot| {
                snapshot.headers.account.did == account_id
                    && snapshot.schema == *schema
                    && snapshot.headers.deleted_at.is_none()
            })
            .count())
    }

    async fn summarize_resources(
        &self,
        account_id: odf::AccountID,
    ) -> Result<Vec<ResourceSummaryRow>, InternalError> {
        let guard = self.state.lock().unwrap();

        let mut rows_by_key = HashMap::<TypeUri, ResourceSummaryRow>::new();

        for snapshot in guard.snapshots_by_id.values().filter(|snapshot| {
            snapshot.headers.account.did == account_id && snapshot.headers.deleted_at.is_none()
        }) {
            let row = rows_by_key
                .entry(snapshot.schema.clone())
                .or_insert_with(|| ResourceSummaryRow {
                    schema: snapshot.schema.to_string(),
                    total_count: 0,
                    phase_counts: ResourcePhaseCounts::default(),
                });

            row.total_count += 1;

            match snapshot.status.as_ref().map(|status| status.phase) {
                Some(ResourcePhase::Reconciling) => row.phase_counts.increment_reconciling(),
                Some(ResourcePhase::Ready) => row.phase_counts.increment_ready(),
                Some(ResourcePhase::Failed) => row.phase_counts.increment_failed(),
                Some(ResourcePhase::Pending) | None => row.phase_counts.increment_pending(),
            }
        }

        let mut rows = rows_by_key.into_values().collect::<Vec<_>>();
        rows.sort_by(|lhs, rhs| lhs.schema.cmp(&rhs.schema));

        Ok(rows)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn resource_name_matches_pattern(name: &str, pattern: &str) -> bool {
    // Resource names are compared case-insensitively, so we fold both sides to
    // lowercase before matching to mirror Postgres ILIKE / SQLite
    // LIKE ... COLLATE NOCASE behavior.
    let name_lc = name.to_ascii_lowercase();
    let pattern_lc = pattern.to_ascii_lowercase();
    let name = name_lc.as_str();
    let pattern = pattern_lc.as_str();

    let mut parts = pattern.split('%').peekable();
    let mut remaining_name = name;

    if !pattern.starts_with('%') {
        let Some(prefix) = parts.next() else {
            return name.is_empty();
        };

        if !remaining_name.starts_with(prefix) {
            return false;
        }

        remaining_name = &remaining_name[prefix.len()..];
    }

    while let Some(part) = parts.next() {
        if part.is_empty() {
            continue;
        }

        if parts.peek().is_none() && !pattern.ends_with('%') {
            return remaining_name.ends_with(part);
        }

        let Some(index) = remaining_name.find(part) else {
            return false;
        };
        remaining_name = &remaining_name[index + part.len()..];
    }

    pattern.ends_with('%') || remaining_name.is_empty()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn filter_search_snapshots<'a>(
    guard: &'a State,
    account_id: &odf::AccountID,
    schemas: &[TypeUri],
    exact_names: Option<&[ResourceName]>,
    name_pattern: Option<&str>,
) -> Vec<&'a ResourceSnapshot> {
    guard
        .snapshots_by_id
        .values()
        .filter(|snapshot| snapshot.headers.account.did == *account_id)
        .filter(|snapshot| schemas.contains(&snapshot.schema))
        .filter(|snapshot| snapshot.headers.deleted_at.is_none())
        .filter(|snapshot| exact_names.is_none_or(|names| names.contains(&snapshot.headers.name)))
        .filter(|snapshot| {
            name_pattern.is_none_or(|pattern| {
                resource_name_matches_pattern(&snapshot.headers.name, pattern)
            })
        })
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
