// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{ManagedResource, ResourceCleanupError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The one-shot callbacks of a resource type whose reconciliation and cleanup
/// each fit in a single step. The framework runs them and records their
/// outcomes.
#[async_trait::async_trait]
pub trait SimpleResourceReconciler<R: ManagedResource>: Send + Sync {
    /// Brings the entities the reconciler manages in line with the current
    /// generation of the resource.
    async fn reconcile(&self, resource: &R) -> Result<(), R::ReconcileError>;

    /// Removes the entities the reconciler created for a resource whose
    /// deletion was requested. Runs before the deletion completes and may run
    /// again after a failure, so it must be idempotent.
    async fn cleanup(&self, resource: &R) -> Result<(), ResourceCleanupError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
