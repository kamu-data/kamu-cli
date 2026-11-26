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

/// Removes the rows a repository keys by resources being purged, where no
/// foreign key with `ON DELETE CASCADE` removes them: a table of another
/// read model, or an in-memory repository standing in for a cascade.
/// [`crate::ResourceRepository::purge_resources`] calls every registered
/// listener in its transaction, before it deletes the resources' rows.
///
/// A database-backed listener declares its tables in
/// [`crate::RESOURCE_KEYED_TABLES_WITHOUT_CASCADE`].
#[async_trait::async_trait]
pub trait ResourceRemovalListener: Send + Sync {
    async fn on_resources_removed(&self, ids: &[ResourceID]) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
