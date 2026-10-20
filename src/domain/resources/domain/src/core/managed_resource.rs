// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::{
    DeclarativeResource,
    ResourceCleanupError,
    ResourceDeletionInProgressError,
    ResourceName,
    ResourceReconcileError,
    ResourceStatus,
    StatusConflictOf,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A resource whose desired state is reconciled by its main controller.
///
/// `generation` counts changes to headers and spec, and `status_generation`
/// counts status updates. Status updates take the `status_generation` the
/// caller read as a precondition and fail with a status conflict when it is
/// stale.
pub trait ManagedResource: DeclarativeResource {
    type ReconcileError: ResourceReconcileError;
    type LifecycleError: StatusConflictOf + From<ResourceDeletionInProgressError>;

    /// A resource being deleted never needs reconciliation.
    fn needs_reconciliation(&self) -> bool {
        use crate::ResourceStatusExt;

        if self.headers().deletion_requested_at.is_some() {
            return false;
        }

        let generation = self.headers().generation;

        !self.status().was_observed(generation) || !self.status().was_reconciled(generation)
    }

    /// True while the deletion waits for the controllers to clean up.
    fn needs_cleanup(&self) -> bool {
        self.headers().deletion_requested_at.is_some() && self.headers().deleted_at.is_none()
    }

    fn try_create(
        now: DateTime<Utc>,
        id: crate::ResourceID,
        headers: crate::ResourceHeadersInput,
        spec: Self::SpecInput,
    ) -> Result<Self, Self::LifecycleError>
    where
        Self: Sized;

    /// Sets the generation to `new_generation`, unless the headers are
    /// unchanged, in which case nothing is recorded. Fails once deletion was
    /// requested, as does a spec update.
    fn try_update_headers(
        &mut self,
        now: DateTime<Utc>,
        new_headers: crate::ResourceHeadersInput,
        new_generation: u64,
    ) -> Result<(), Self::LifecycleError>;

    fn try_update_spec(
        &mut self,
        now: DateTime<Utc>,
        new_spec: Self::SpecInput,
        new_generation: u64,
    ) -> Result<(), Self::LifecycleError>;

    /// Starts the deletion; the resource keeps its name until it completes.
    fn try_request_deletion(&mut self, now: DateTime<Utc>) -> Result<(), Self::LifecycleError>
    where
        Self: Sized;

    /// Completes the deletion and renames the resource to `tombstone_name`.
    fn try_complete_deletion(
        &mut self,
        now: DateTime<Utc>,
        tombstone_name: ResourceName,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: Sized;

    fn try_update_status(
        &mut self,
        now: DateTime<Utc>,
        expected_status_generation: u64,
        new_status: ResourceStatus,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: Sized;

    fn try_mark_reconciliation_started(
        &mut self,
        now: DateTime<Utc>,
        expected_status_generation: u64,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: Sized;

    /// Skipped when `expected_generation` is no longer the current generation.
    fn try_mark_reconciliation_succeeded(
        &mut self,
        now: DateTime<Utc>,
        expected_generation: u64,
        expected_status_generation: u64,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: Sized;

    /// Skipped when `expected_generation` is no longer the current generation.
    fn try_mark_reconciliation_failed(
        &mut self,
        now: DateTime<Utc>,
        expected_generation: u64,
        expected_status_generation: u64,
        error: &Self::ReconcileError,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: Sized;

    /// Skipped when the resource does not wait for a cleanup.
    fn try_mark_cleanup_failed(
        &mut self,
        now: DateTime<Utc>,
        expected_status_generation: u64,
        error: &ResourceCleanupError,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: Sized;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
