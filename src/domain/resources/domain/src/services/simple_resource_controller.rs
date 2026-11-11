// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{
    EventSourcedResource,
    ResourceID,
    SimpleResourceDeletionError,
    SimpleResourceReconciliationError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs the `SimpleResourceReconciler` of a resource type and records its
/// outcomes through the lifecycle primitives, owning the transactions
#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait SimpleResourceController<R: EventSourcedResource>: Send + Sync {
    /// Reconciles the resource when it needs it: one transaction records the
    /// start, a second runs `reconcile` and records the outcome. An outcome is
    /// discarded when the deletion of the resource was requested or its
    /// generation changed meanwhile
    async fn reconcile(&self, id: &ResourceID) -> Result<(), SimpleResourceReconciliationError<R>>;

    /// Runs the cleanup of each resource whose deletion was requested, in a
    /// transaction of its own, then completes their deletion. A failed
    /// cleanup, a database error included, is recorded in the status and
    /// leaves that deletion in progress. Resources that do not wait for a
    /// cleanup are skipped
    async fn complete_deletions(
        &self,
        ids: &[ResourceID],
    ) -> Result<(), SimpleResourceDeletionError<R>>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
