// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use event_sourcing::{ConcurrentModificationError, EventID};
use internal_error::InternalError;
use thiserror::Error;

use crate::{
    ResourceID,
    ResourceIDStream,
    ResourceName,
    ResourcePhaseCounts,
    ResourceRawEventQuery,
    ResourceSnapshot,
    ResourceSnapshotStream,
    TypeRef,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Storage for resources and the queries over them.
///
/// Label filters apply only to unknown result sets, not direct identity reads —
/// the scoped searches carry them per row inside [`ResourceScope`], while the
/// by-id/by-name reads take none at all.
#[async_trait::async_trait]
pub trait ResourceRepository: Send + Sync {
    async fn new_resource_id(&self) -> Result<ResourceID, InternalError>;

    async fn create_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
    ) -> Result<(), CreateResourceError>;

    async fn update_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
        expected_last_event_id: Option<EventID>,
    ) -> Result<(), UpdateResourceError>;

    async fn update_resources(
        &self,
        resource_updates: &[ResourceSnapshotUpdate],
    ) -> Result<(), UpdateResourceError> {
        for resource_update in resource_updates {
            self.update_resource(
                &resource_update.snapshot,
                resource_update.expected_last_event_id,
            )
            .await?;
        }

        Ok(())
    }

    async fn find_resource_id_by_name(
        &self,
        account_id: &odf::AccountID,
        schema: &TypeUri,
        name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError>;

    /// Returns rows in the order their IDs appear in `ids`. IDs that do not
    /// exist, are deleted, or belong to another account are absent, so the
    /// result may be shorter than the request but never reordered.
    async fn find_resource_handles_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceHandleRow>, InternalError>;

    /// Matches names case-insensitively. Names that do not exist are absent
    /// from the result.
    ///
    /// Returns pairs in the order their names appear in `names`.
    async fn resolve_resource_ids_by_names(
        &self,
        account_id: &odf::AccountID,
        schema: &TypeUri,
        names: &[ResourceName],
    ) -> Result<Vec<(ResourceName, ResourceID)>, InternalError>;

    /// Label filtering rides on `scope`: each row carries the pairs it must
    /// satisfy, so one call may filter differently per type.
    async fn search_resource_handles(
        &self,
        account_id: &odf::AccountID,
        scope: &ResourceScope,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceHandleRow>, InternalError>;

    async fn count_search_resource_handles(
        &self,
        account_id: &odf::AccountID,
        scope: &ResourceScope,
    ) -> Result<usize, InternalError>;

    async fn find_resource_snapshot(
        &self,
        query: &ResourceRawEventQuery,
    ) -> Result<Option<ResourceSnapshot>, InternalError>;

    /// Returns snapshots in the order their IDs appear in `ids`. This lookup is
    /// by schema, not by account, so it spans accounts.
    async fn find_resource_snapshots_by_schema_and_ids(
        &self,
        schema: &TypeUri,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError>;

    async fn find_resource_snapshot_by_id(
        &self,
        id: &ResourceID,
    ) -> Result<Option<ResourceSnapshot>, InternalError>;

    /// Returns snapshots in the order their IDs appear in `ids`. IDs that do
    /// not exist, are deleted, or belong to another account are absent, so the
    /// result may be shorter than the request but never reordered.
    async fn find_resource_snapshots_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError>;

    fn list_resource_ids(
        &self,
        account_id: odf::AccountID,
        schema: &TypeUri,
        pagination: PaginationOpts,
    ) -> ResourceIDStream<'_>;

    /// Spans the schemas named by `scope`, each with its own query and label
    /// pairs.
    fn list_resource_snapshots(
        &self,
        account_id: &odf::AccountID,
        scope: &ResourceScope,
        pagination: PaginationOpts,
    ) -> ResourceSnapshotStream<'_>;

    async fn count_resources(
        &self,
        account_id: odf::AccountID,
        schema: &TypeUri,
    ) -> Result<usize, InternalError>;

    async fn summarize_resources(
        &self,
        account_id: odf::AccountID,
    ) -> Result<Vec<ResourceSummaryRow>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One resolved label predicate: a canonical key and the string value it must
/// equal.
///
/// Flattened from a [`ResolvedResourceLabelFilter`] conjunction by the facade,
/// so a backend never has to interpret an expression tree.
///
/// [`ResolvedResourceLabelFilter`]: crate::ResolvedResourceLabelFilter
pub type ResourceLabelPair = (TypeRef, String);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Exactly one way to narrow a resource search or listing to specific
/// resources within a type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceQuery {
    ExactNames(Vec<ResourceName>),
    ExactIds(Vec<ResourceID>),
    NamePattern(String),
}

impl ResourceQuery {
    /// An empty `ExactNames`/`ExactIds` list can never match anything.
    pub fn is_vacuous(&self) -> bool {
        match self {
            Self::ExactNames(names) => names.is_empty(),
            Self::ExactIds(ids) => ids.is_empty(),
            Self::NamePattern(_) => false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One resource type in a scope, with the query that narrows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceTypeQuery {
    pub schema: TypeUri,
    pub query: Option<ResourceQuery>,
    /// Which account this row spans. `None` means the call-level account, which
    /// every row used to share — the scoped reads still take that as a scalar
    /// argument and it remains the default.
    ///
    /// Set only when a caller names an account per selector. Rows naming
    /// different accounts cannot merge, so the coalescer groups by
    /// `(schema, account, labels)` rather than by schema alone.
    pub account_id: Option<odf::AccountID>,
    /// Label pairs this row must satisfy, all of which must be present on a
    /// resource for it to match. Empty means unfiltered.
    ///
    /// Already resolved and flattened by the facade, so backends only ever see
    /// `(canonical key, string value)` pairs — `$not`/`$or` are rejected before
    /// reaching here. Part of the coalescer's grouping key: two rows differing
    /// only by labels describe different resources and must not merge.
    pub label_pairs: Vec<ResourceLabelPair>,
}

impl ResourceTypeQuery {
    /// The account this row applies to, falling back to the call-level one.
    pub fn effective_account_id<'a>(&'a self, default: &'a odf::AccountID) -> &'a odf::AccountID {
        self.account_id.as_ref().unwrap_or(default)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Which resources a search or listing spans: the types, plus the query and
/// label pairs narrowing each one.
///
/// A per-type query is what lets one call express `vs/a-% ss/b-%`, where each
/// type carries a different query. Per-type label pairs extend that to
/// filtering: one call may require different labels of each type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceScope {
    /// Every registered resource type, optionally narrowed by one query and one
    /// set of label pairs, both applying uniformly.
    AnyType(Option<ResourceQuery>, Vec<ResourceLabelPair>),
    /// Non-empty by construction: use `AnyType` instead of an empty list.
    Types(Vec<ResourceTypeQuery>),
}

impl ResourceScope {
    /// Panics if `types` is empty — callers must use `AnyType` for that.
    pub fn types(types: Vec<ResourceTypeQuery>) -> Self {
        assert!(
            !types.is_empty(),
            "ResourceScope::Types must not be empty; use AnyType instead"
        );
        Self::Types(types)
    }

    /// Every type, unnarrowed and unfiltered.
    pub fn any_type() -> Self {
        Self::AnyType(None, Vec::new())
    }

    /// The common single-type case, with or without a query, spanning the
    /// call-level account and carrying no label filter.
    pub fn one_type(schema: TypeUri, query: Option<ResourceQuery>) -> Self {
        Self::Types(vec![ResourceTypeQuery {
            schema,
            query,
            account_id: None,
            label_pairs: Vec::new(),
        }])
    }

    /// Every type, narrowed by one query applying to all of them and carrying
    /// no label filter.
    pub fn any_type_with_query(query: ResourceQuery) -> Self {
        Self::AnyType(Some(query), Vec::new())
    }

    /// Whether no resource can possibly match, so callers can skip the query
    /// entirely.
    ///
    /// Label pairs are deliberately not consulted: they narrow a row, but no
    /// set of pairs makes one unmatchable a priori the way an empty
    /// `ExactNames`/`ExactIds` list does.
    pub fn is_vacuous(&self) -> bool {
        match self {
            Self::AnyType(query, _) => query.as_ref().is_some_and(ResourceQuery::is_vacuous),
            Self::Types(types) => types
                .iter()
                .all(|entry| entry.query.as_ref().is_some_and(ResourceQuery::is_vacuous)),
        }
    }
}

impl Default for ResourceScope {
    fn default() -> Self {
        Self::any_type()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSummaryRow {
    pub schema: String,
    pub total_count: u64,
    pub phase_counts: ResourcePhaseCounts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CreateResourceError {
    #[error(transparent)]
    Duplicate(ResourceDuplicateError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UpdateResourceError {
    #[error(transparent)]
    Duplicate(ResourceDuplicateError),

    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl UpdateResourceError {
    pub fn concurrent_modification() -> Self {
        Self::ConcurrentModification(ConcurrentModificationError {})
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceSnapshotUpdate {
    pub snapshot: ResourceSnapshot,
    pub expected_last_event_id: Option<EventID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Resource already exists: account_id={account_id}, schema='{schema}', name='{name}'")]
pub struct ResourceDuplicateError {
    pub account_id: odf::AccountID,
    pub schema: TypeUri,
    pub name: ResourceName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
pub struct ResourceHandleRow {
    pub id: uuid::Uuid,
    pub schema: String,
    pub name: String,
    pub account_id: odf::AccountID,
    pub account_resource_id: uuid::Uuid,
    pub account_name: String,
}

impl ResourceHandleRow {
    pub fn account_handle(&self) -> odf::AccountHandle {
        odf::AccountHandle {
            id: ResourceID::new(self.account_resource_id),
            did: self.account_id.clone(),
            name: odf::AccountName::new_unchecked(&self.account_name),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
pub struct ResourceSnapshotRow {
    pub id: uuid::Uuid,
    pub account_id: odf::AccountID,
    pub account_resource_id: uuid::Uuid,
    pub account_name: String,
    pub resource_schema: String,
    pub resource_name: String,
    pub labels: serde_json::Value,
    pub annotations: serde_json::Value,
    pub spec: serde_json::Value,
    pub status: Option<serde_json::Value>,
    pub generation: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub last_event_id: Option<i64>,
}

impl ResourceSnapshotRow {
    pub fn into_snapshot(self) -> ResourceSnapshot {
        let id = ResourceID::new(self.id);

        ResourceSnapshot {
            id,
            schema: TypeUri::new_unchecked(self.resource_schema),
            headers: crate::ResourceHeaders {
                id,
                account: odf::AccountHandle {
                    id: ResourceID::new(self.account_resource_id),
                    did: self.account_id,
                    name: odf::AccountName::new_unchecked(&self.account_name),
                },
                name: ResourceName::new_unchecked(&self.resource_name),
                labels: crate::resource_labels_from_json(self.labels),
                annotations: crate::resource_annotations_from_json(self.annotations),
                generation: u64::try_from(self.generation).unwrap(),
                created_at: self.created_at,
                updated_at: self.updated_at,
                deleted_at: self.deleted_at,
            },
            spec: self.spec,
            status: self
                .status
                .as_ref()
                .and_then(ResourceSnapshot::basic_status_from_json),
            last_event_id: self.last_event_id.map(EventID::new),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
