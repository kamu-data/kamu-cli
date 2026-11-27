// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::ResourceID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Maintains the rebuildable `resource_owners_projection` index.
/// Entries are updated in the same transactional scope as resource snapshots.
#[async_trait::async_trait]
pub trait ResourceOwnerProjectionRepository: Send + Sync {
    /// Replaces the owners recorded for `resource_id`, which may have none.
    async fn replace_owners(
        &self,
        resource_id: &ResourceID,
        owner_ids: &[ResourceID],
    ) -> Result<(), InternalError>;

    /// Returns the IDs of the recorded owners in order. Read by the repository
    /// tests, as the only view of the index that shows its cascade on a purge.
    async fn find_owners(&self, resource_id: &ResourceID)
    -> Result<Vec<ResourceID>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
