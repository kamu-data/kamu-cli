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

use crate::{ResourceIdentityRow, ResourceName, ResourceSnapshot, ResourceSummaryRow, ResourceUID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait GenericResourceQueryService: Send + Sync {
    async fn allocate_uid(&self) -> Result<ResourceUID, InternalError>;

    async fn find_resource_uid_by_name(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
        name: &ResourceName,
    ) -> Result<Option<ResourceUID>, InternalError>;

    async fn find_resource_identities_by_uids(
        &self,
        account_id: &odf::AccountID,
        uids: &[ResourceUID],
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
        uid: ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, FindOwnedResourceError>;

    async fn find_owned_snapshots(
        &self,
        account_id: &odf::AccountID,
        kind: &'static str,
        uids: &[ResourceUID],
    ) -> Result<FindOwnedSnapshotsOutcome, InternalError>;

    async fn get_snapshot_by_uid(
        &self,
        uid: &crate::ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, InternalError>;

    async fn find_snapshots_by_uids(
        &self,
        account_id: &odf::AccountID,
        uids: &[ResourceUID],
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
    pub not_found: Vec<ResourceUID>,
    pub kind_mismatch: Vec<ResourceUID>,
    pub access_denied: Vec<ResourceUID>,
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
    Internal(#[from] internal_error::InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
