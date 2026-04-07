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

use crate::{ResourceSnapshot, ResourceSummaryRow, ResourceUID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait GenericResourceQueryService: Send + Sync {
    async fn allocate_uid(&self) -> Result<ResourceUID, InternalError>;

    async fn find_resource_uid_by_name(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
        name: &crate::ResourceName,
    ) -> Result<Option<crate::ResourceUID>, InternalError>;

    async fn find_owned_snapshot(
        &self,
        account_id: &odf::AccountID,
        kind: &'static str,
        uid: ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, FindOwnedResourceError>;

    async fn get_snapshot_by_uid(
        &self,
        uid: &crate::ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, InternalError>;

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
