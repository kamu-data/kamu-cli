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

/// Maintains the rebuildable `resource_labels_projection` index.
/// Entries are updated in the same transactional scope as resource snapshots.
#[async_trait::async_trait]
pub trait ResourceLabelProjectionRepository: Send + Sync {
    /// Replaces all projected label entries for `resource_id` with `entries`.
    /// Idempotent: safe to call for a brand-new resource (no existing rows)
    /// or to re-sync an existing one (drops stale rows first).
    async fn replace_entries(
        &self,
        resource_id: &ResourceID,
        entries: &[(String, String)],
    ) -> Result<(), InternalError>;

    /// Returns the projected label entries for `resource_id`, sorted by key.
    async fn find_entries(
        &self,
        resource_id: &ResourceID,
    ) -> Result<Vec<(String, String)>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
