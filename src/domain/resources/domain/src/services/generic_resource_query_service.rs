// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use internal_error::InternalError;

use crate::{
    ResolvedResourceLabelFilter,
    ResourceHandleRow,
    ResourceID,
    ResourceName,
    ResourceSearchQuery,
    ResourceSnapshot,
    ResourceSummaryRow,
    ResourceTypeMismatchError,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Read-side access to resources for use cases and higher layers, which never
/// reach for [`crate::ResourceRepository`] directly.
///
/// See [`crate::ResourceRepository`] for the rule governing which methods take
/// a `label_filter`.
#[async_trait::async_trait]
pub trait GenericResourceQueryService: Send + Sync {
    async fn allocate_id(&self) -> Result<ResourceID, InternalError>;

    async fn find_resource_id_by_name(
        &self,
        account_id: &odf::AccountID,
        schema: &TypeUri,
        name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError>;

    async fn find_resource_handles_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceHandleRow>, InternalError>;

    /// Matches names case-insensitively. Names that do not exist are absent
    /// from the result.
    async fn resolve_resource_ids_by_names(
        &self,
        account_id: &odf::AccountID,
        schema: &TypeUri,
        names: &[ResourceName],
    ) -> Result<Vec<(ResourceName, ResourceID)>, InternalError>;

    async fn search_resource_handles(
        &self,
        account_id: &odf::AccountID,
        schemas: &[TypeUri],
        query: &ResourceSearchQuery,
        label_filter: &ResolvedResourceLabelFilter,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceHandleRow>, InternalError>;

    async fn count_search_resource_handles(
        &self,
        account_id: &odf::AccountID,
        schemas: &[TypeUri],
        query: &ResourceSearchQuery,
        label_filter: &ResolvedResourceLabelFilter,
    ) -> Result<usize, InternalError>;

    async fn find_owned_snapshot(
        &self,
        account_id: &odf::AccountID,
        schema: &'static TypeUri,
        id: ResourceID,
    ) -> Result<Option<ResourceSnapshot>, FindOwnedResourceError>;

    async fn find_owned_snapshots(
        &self,
        account_id: &odf::AccountID,
        schema: &'static TypeUri,
        ids: &[ResourceID],
    ) -> Result<FindOwnedSnapshotsOutcome, InternalError>;

    async fn get_snapshot_by_id(
        &self,
        id: &crate::ResourceID,
    ) -> Result<Option<ResourceSnapshot>, InternalError>;

    async fn find_snapshots_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError>;

    async fn list_snapshots_by_schema(
        &self,
        account_id: odf::AccountID,
        schema: &TypeUri,
        label_filter: &ResolvedResourceLabelFilter,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceSnapshot>, InternalError>;

    async fn list_all_snapshots(
        &self,
        account_id: odf::AccountID,
        label_filter: &ResolvedResourceLabelFilter,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceSnapshot>, InternalError>;

    async fn summarize_resources(
        &self,
        account_id: odf::AccountID,
    ) -> Result<Vec<ResourceSummaryRow>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FindOwnedSnapshotsOutcome {
    pub found: Vec<ResourceSnapshot>,
    pub not_found: Vec<ResourceID>,
    /// IDs owned by the account but with a different schema:
    /// (id, `actual_schema`)
    pub schema_mismatch: Vec<(ResourceID, TypeUri)>,
    pub access_denied: Vec<ResourceID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum FindOwnedResourceError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    TypeMismatch(#[from] ResourceTypeMismatchError),

    #[error(transparent)]
    Internal(#[from] internal_error::InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
