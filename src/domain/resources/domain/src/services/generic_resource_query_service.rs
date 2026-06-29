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
    ResourceID,
    ResourceIdentityRow,
    ResourceName,
    ResourceSnapshot,
    ResourceSummaryRow,
    ResourceTypeMismatchError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait GenericResourceQueryService: Send + Sync {
    async fn allocate_id(&self) -> Result<ResourceID, InternalError>;

    async fn find_resource_id_by_name(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
        name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError>;

    async fn find_resource_identities_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceIdentityRow>, InternalError>;

    async fn find_resource_identities_by_names(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
        names: &[ResourceName],
    ) -> Result<Vec<ResourceIdentityRow>, InternalError>;

    async fn search_resource_identities(
        &self,
        account_id: &odf::AccountID,
        kinds: &[String],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceIdentityRow>, InternalError>;

    async fn count_search_resource_identities(
        &self,
        account_id: &odf::AccountID,
        kinds: &[String],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
    ) -> Result<usize, InternalError>;

    async fn find_owned_snapshot(
        &self,
        account_id: &odf::AccountID,
        kind: &'static str,
        api_version: &'static str,
        id: ResourceID,
    ) -> Result<Option<ResourceSnapshot>, FindOwnedResourceError>;

    async fn find_owned_snapshots(
        &self,
        account_id: &odf::AccountID,
        kind: &'static str,
        api_version: &'static str,
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

    async fn list_snapshots_by_kind(
        &self,
        account_id: odf::AccountID,
        kind: &str,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceSnapshot>, InternalError>;

    async fn list_all_snapshots(
        &self,
        account_id: odf::AccountID,
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
    /// IDs owned by the account but with a different kind:
    /// (id, `actual_kind`)
    pub kind_mismatch: Vec<(ResourceID, String)>,
    /// IDs owned by the account, correct kind,
    /// but wrong `api_version`: (id, `actual_api_version`)
    pub api_version_mismatch: Vec<(ResourceID, String)>,
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
