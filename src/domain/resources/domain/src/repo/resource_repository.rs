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
    ResourceDueForPurge,
    ResourceHandle,
    ResourceHeaders,
    ResourceID,
    ResourceName,
    ResourcePhaseCounts,
    ResourcePurgeDeadline,
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

    /// A taken name is reported without failing the transaction, so the caller
    /// may read and write on in it.
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

    /// Finds the live resource holding the name; a resource gives it up when
    /// its deletion is requested.
    async fn find_resource_id_by_name(
        &self,
        account_id: &odf::AccountID,
        schema: &TypeUri,
        name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError>;

    /// Returns rows in the order their IDs appear in `ids`, in any state. IDs
    /// that do not exist or belong to another account are absent, so the result
    /// may be shorter than the request but never reordered.
    async fn find_resource_handles_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceHandleRow>, InternalError>;

    /// Writes the rows of the resources among `ids` of the account whose
    /// deletion is not requested, without changing them, and returns their
    /// IDs in no particular order. A concurrent write of such a row, such as a
    /// deletion request, then conflicts with this transaction: whichever
    /// commits second sees the first. The other IDs are absent.
    async fn touch_live_resources(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceID>, InternalError>;

    /// Matches names case-insensitively. Names that do not exist are absent
    /// from the result. Unless only live resources are visible, a name may
    /// resolve to several IDs.
    ///
    /// Returns pairs in the order their names appear in `names`. Within a name
    /// the live resource comes first, then those being deleted, oldest request
    /// first, then the deleted ones, newest deletion first.
    async fn resolve_resource_ids_by_names(
        &self,
        account_id: &odf::AccountID,
        schema: &TypeUri,
        names: &[ResourceName],
        visibility: ResourceVisibility,
    ) -> Result<Vec<(ResourceName, ResourceID)>, InternalError>;

    /// Returns IDs of live resources, not being deleted, of `schema` owned by
    /// `account_id` and carrying the exact label pair, oldest first.
    ///
    /// `account_id` is a security boundary, not an optimization: label values
    /// are caller-supplied and unvalidated, so matching across accounts would
    /// let one account's resource be picked up by another's lookup purely by
    /// carrying the same value.
    ///
    /// Ordering is by `created_at` rather than `updated_at` so that editing a
    /// resource never reshuffles precedence among equally-labelled peers.
    async fn find_resource_ids_by_schema_and_label(
        &self,
        account_id: &odf::AccountID,
        schema: &TypeUri,
        label_key: &str,
        label_value: &str,
    ) -> Result<Vec<ResourceID>, InternalError>;

    /// Cleanup counterpart of [`Self::find_resource_ids_by_schema_and_label`],
    /// spanning every account. Retracting a pointer to a deleted entity grants
    /// no access, so the scoped variant's privilege boundary does not apply.
    ///
    /// Account-free out of necessity, not preference: dataset deletion removes
    /// the `dataset_entries` row before posting its message, leaving the owner
    /// unresolvable by the time the cleanup consumer runs.
    async fn find_resource_ids_by_schema_and_label_any_account(
        &self,
        schema: &TypeUri,
        label_key: &str,
        label_value: &str,
    ) -> Result<Vec<ResourceID>, InternalError>;

    /// Label filtering rides on `scope`: each row carries the pairs it must
    /// satisfy, so one call may filter differently per type.
    ///
    /// An `owner_id` narrows every row to the resources it owns, whether or not
    /// the owner still exists.
    ///
    /// Orders as [`Self::list_resource_snapshots`] does.
    async fn search_resource_handles(
        &self,
        account_id: &odf::AccountID,
        scope: &ResourceScope,
        visibility: ResourceVisibility,
        owner_id: Option<ResourceID>,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceHandleRow>, InternalError>;

    async fn count_search_resource_handles(
        &self,
        account_id: &odf::AccountID,
        scope: &ResourceScope,
        visibility: ResourceVisibility,
        owner_id: Option<ResourceID>,
    ) -> Result<usize, InternalError>;

    /// Returns the resource in any state.
    async fn find_resource_snapshot(
        &self,
        query: &ResourceRawEventQuery,
    ) -> Result<Option<ResourceSnapshot>, InternalError>;

    /// Returns snapshots in the order their IDs appear in `ids`, in any state.
    /// This lookup is by schema, not by account, so it spans accounts.
    async fn find_resource_snapshots_by_schema_and_ids(
        &self,
        schema: &TypeUri,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError>;

    /// Returns the resource in any state.
    async fn find_resource_snapshot_by_id(
        &self,
        id: &ResourceID,
    ) -> Result<Option<ResourceSnapshot>, InternalError>;

    /// Returns snapshots in the order their IDs appear in `ids`, in any state.
    /// IDs that do not exist or belong to another account are absent, so the
    /// result may be shorter than the request but never reordered.
    async fn find_resource_snapshots_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError>;

    /// Spans the schemas named by `scope`, each with its own query and label
    /// pairs. Takes `owner_id` as [`Self::search_resource_handles`] does.
    ///
    /// Orders by last update, newest first. When deleted resources are visible,
    /// the live ones keep that order and come first; the others follow, newest
    /// first by the time their deletion completed, or was requested while it
    /// has not.
    fn list_resource_snapshots(
        &self,
        account_id: &odf::AccountID,
        scope: &ResourceScope,
        visibility: ResourceVisibility,
        owner_id: Option<ResourceID>,
        pagination: PaginationOpts,
    ) -> ResourceSnapshotStream<'_>;

    /// Counts resources in any state.
    async fn summarize_resources(
        &self,
        account_id: odf::AccountID,
    ) -> Result<Vec<ResourceSummaryRow>, InternalError>;

    /// Sets when deleted resources may be purged, ignoring IDs of no resource.
    /// The deadline is kept apart from the snapshot: updating a resource leaves
    /// it as it is.
    async fn set_purge_deadlines(
        &self,
        deadlines: &[ResourcePurgeDeadline],
    ) -> Result<(), InternalError>;

    /// Returns at most `limit` resources, of every account and type, whose
    /// purge deadline is not after `now`, earliest deadline first.
    async fn find_resources_due_for_purge(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<ResourceDueForPurge>, InternalError>;

    /// Counts the resources whose purge deadline is not after `now`.
    async fn count_resources_due_for_purge(&self, now: DateTime<Utc>)
    -> Result<u64, InternalError>;

    /// Claims the given resources whose purge deadline is not after `now` for
    /// a purge, and returns the IDs it claimed: a compare-and-set that clears
    /// the deadline, so a concurrent purger of the same resources claims none
    /// of them.
    async fn claim_resources_for_purge(
        &self,
        ids: &[ResourceID],
        now: DateTime<Utc>,
    ) -> Result<Vec<ResourceID>, InternalError>;

    /// Deletes the snapshot rows of resources claimed for a purge, with what
    /// the storage removes along with them by cascade (labels, owners,
    /// configuration projections), after every registered
    /// [`crate::ResourceRemovalListener`] removed what it keeps of them. The
    /// events and the controller bookkeeping of the resources are removed
    /// first, by their own repositories.
    async fn purge_resources(&self, ids: &[ResourceID]) -> Result<(), InternalError>;

    /// Returns the earliest purge deadline after `now`.
    async fn find_nearest_purge_at(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Which resources a lookup by name, search or listing returns. The reads by ID
/// (`find_resource_snapshot*`, `find_resource_handles_by_ids`) return resources
/// in any state; an ID query in a search scope follows the visibility like any
/// other.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ResourceVisibility {
    /// Resources whose deletion was not requested
    #[default]
    Active,
    /// Also resources whose deletion is in progress
    IncludeDeleting,
    /// Resources in any state
    IncludeDeleted,
    /// Only resources whose deletion is in progress or completed
    DeletedOnly,
}

impl ResourceVisibility {
    /// The visibility of resources addressed by ID: an ID finds a resource in
    /// any state, and only [`Self::DeletedOnly`] hides live ones from it
    pub fn of_ids(self) -> Self {
        match self {
            Self::Active | Self::IncludeDeleting | Self::IncludeDeleted => Self::IncludeDeleted,
            Self::DeletedOnly => Self::DeletedOnly,
        }
    }

    /// Whether resources whose deletion was not requested are visible
    pub fn includes_live(self) -> bool {
        match self {
            Self::Active | Self::IncludeDeleting | Self::IncludeDeleted => true,
            Self::DeletedOnly => false,
        }
    }

    /// Whether resources whose deletion is in progress are visible
    pub fn includes_deleting(self) -> bool {
        match self {
            Self::Active => false,
            Self::IncludeDeleting | Self::IncludeDeleted | Self::DeletedOnly => true,
        }
    }

    /// Whether resources whose deletion completed are visible
    pub fn includes_deleted(self) -> bool {
        match self {
            Self::Active | Self::IncludeDeleting => false,
            Self::IncludeDeleted | Self::DeletedOnly => true,
        }
    }

    /// Whether a resource in this state of deletion is visible
    pub fn includes(
        self,
        deletion_requested_at: Option<DateTime<Utc>>,
        deleted_at: Option<DateTime<Utc>>,
    ) -> bool {
        if deleted_at.is_some() {
            self.includes_deleted()
        } else if deletion_requested_at.is_some() {
            self.includes_deleting()
        } else {
            self.includes_live()
        }
    }
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
    /// Which account this row spans. `None` means the default account, which
    /// the scoped reads take as a scalar argument.
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
    /// The account this row applies to, falling back to the default one.
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
    AnyType {
        query: Option<ResourceQuery>,
        /// Which account this scope spans. `None` means the authenticated
        /// subject's own.
        account_id: Option<odf::AccountID>,
        label_pairs: Vec<ResourceLabelPair>,
    },
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

    /// Every type, unnarrowed and unfiltered, under the authenticated subject's
    /// own account.
    pub fn any_type() -> Self {
        Self::AnyType {
            query: None,
            account_id: None,
            label_pairs: Vec::new(),
        }
    }

    /// The common single-type case, with or without a query, spanning the
    /// default account and carrying no label filter.
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
        Self::AnyType {
            query: Some(query),
            account_id: None,
            label_pairs: Vec::new(),
        }
    }

    /// Whether no resource can possibly match, so callers can skip the query
    /// entirely.
    ///
    /// Label pairs are deliberately not consulted: they narrow a row, but no
    /// set of pairs makes one unmatchable a priori the way an empty
    /// `ExactNames`/`ExactIds` list does.
    pub fn is_vacuous(&self) -> bool {
        match self {
            Self::AnyType { query, .. } => query.as_ref().is_some_and(ResourceQuery::is_vacuous),
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

fn user_account_handle(
    did: odf::AccountID,
    resource_id: uuid::Uuid,
    name: &str,
) -> odf::ResourceOwnerHandle {
    odf::ResourceOwnerHandle {
        // TODO: Support Org accounts
        r#type: odf::metadata::auth::User::type_uri().cheap_clone(),
        id: ResourceID::new(resource_id),
        did,
        name: odf::AccountName::new_unchecked(name),
    }
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
    pub deletion_requested_at: Option<DateTime<Utc>>,
    pub deleted_at: Option<DateTime<Utc>>,
}

impl ResourceHandleRow {
    pub fn account_handle(&self) -> odf::ResourceOwnerHandle {
        user_account_handle(
            self.account_id.clone(),
            self.account_resource_id,
            &self.account_name,
        )
    }

    pub fn into_handle(self) -> ResourceHandle {
        ResourceHandle {
            account: self.account_handle(),
            r#type: TypeUri::new_unchecked(&self.schema),
            id: ResourceID::new(self.id),
            // TODO: temporary until we support DID-aware resource types; once we do,
            // this must be populated instead of always `None`.
            did: None,
            name: ResourceName::new_unchecked(&self.name),
        }
    }
}

impl From<&ResourceSnapshot> for ResourceHandleRow {
    fn from(snapshot: &ResourceSnapshot) -> Self {
        let headers = &snapshot.headers;
        Self {
            id: *snapshot.id.as_ref(),
            schema: snapshot.schema.to_string(),
            name: headers.name.to_string(),
            account_id: headers.account.did.clone(),
            account_resource_id: *headers.account.id.as_ref(),
            account_name: headers.account.name.to_string(),
            deletion_requested_at: headers.deletion_requested_at,
            deleted_at: headers.deleted_at,
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
    pub owner_references: Option<serde_json::Value>,
    pub spec: serde_json::Value,
    pub status: Option<serde_json::Value>,
    pub generation: i64,
    pub status_generation: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deletion_requested_at: Option<DateTime<Utc>>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub last_event_id: Option<i64>,
}

impl ResourceSnapshotRow {
    pub fn into_snapshot(self) -> ResourceSnapshot {
        let id = ResourceID::new(self.id);
        let account = user_account_handle(
            self.account_id,
            self.account_resource_id,
            &self.account_name,
        );

        ResourceSnapshot {
            id,
            schema: TypeUri::new_unchecked(&self.resource_schema),
            headers: ResourceHeaders {
                id,
                name: ResourceName::new_unchecked(&self.resource_name),
                labels: crate::resource_labels_from_json(self.labels),
                annotations: crate::resource_annotations_from_json(self.annotations),
                generation: u64::try_from(self.generation).unwrap(),
                status_generation: u64::try_from(self.status_generation).unwrap(),
                created_at: self.created_at,
                updated_at: self.updated_at,
                deletion_requested_at: self.deletion_requested_at,
                deleted_at: self.deleted_at,
                owner_references: crate::resource_owner_references_from_json(
                    self.owner_references,
                    &account,
                ),
                account,
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
