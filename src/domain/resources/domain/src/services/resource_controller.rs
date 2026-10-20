// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::ManagedResource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The main controller of a resource type.
#[async_trait::async_trait]
pub trait ResourceController<R: ManagedResource>: Send + Sync {
    /// Brings the entities the controller manages in line with the current
    /// generation of the resource.
    async fn reconcile(&self, resource: &R) -> Result<(), R::ReconcileError>;

    /// Removes the entities the controller created for a resource whose
    /// deletion was requested. Runs before the deletion completes and may run
    /// again after a failure, so it must be idempotent.
    async fn cleanup(&self, resource: &R) -> Result<(), ResourceCleanupError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceReconcileError: std::error::Error + Send + Sync {
    fn reason_code(&self) -> &'static str;
    fn user_message(&self) -> String;
    fn is_transient(&self) -> bool;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ResourceCleanupError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
