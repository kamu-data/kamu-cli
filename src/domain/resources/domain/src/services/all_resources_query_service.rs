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

use crate::ResourceSnapshot;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AllResourcesQueryService: Send + Sync {
    async fn get_snapshot_by_uid(
        &self,
        uid: &crate::ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, InternalError>;

    async fn get_snapshots_by_uids(
        &self,
        uids: &[crate::ResourceUID],
    ) -> Result<Vec<Option<ResourceSnapshot>>, InternalError>;

    async fn list_all_snapshots(
        &self,
        account_id: odf::AccountID,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceSnapshot>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
