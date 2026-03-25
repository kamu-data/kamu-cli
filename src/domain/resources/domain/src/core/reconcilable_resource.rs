// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::{DeclarativeResource, ResourceReconcileError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ReconcilableResource: DeclarativeResource {
    type ReconcileSuccess;
    type ReconcileError: ResourceReconcileError;
    type ReconcileFailureDetails;
    type LifecycleError;

    fn needs_reconciliation(&self) -> bool {
        use crate::ResourceStatusLike;

        self.status()
            .resource_status()
            .needs_reconciliation(self.metadata().generation)
    }

    fn reconcile_failure_details(error: &Self::ReconcileError) -> Self::ReconcileFailureDetails;

    fn try_create(
        now: DateTime<Utc>,
        resource_id: crate::ResourceID,
        metadata: crate::ResourceMetadataInput,
        spec: Self::Spec,
    ) -> Result<Self, Self::LifecycleError>
    where
        Self: Sized;

    fn try_update_metadata(
        &mut self,
        now: DateTime<Utc>,
        new_metadata: crate::ResourceMetadataInput,
    ) -> Result<(), Self::LifecycleError>;

    fn try_update_spec(
        &mut self,
        now: DateTime<Utc>,
        new_spec: Self::Spec,
    ) -> Result<(), Self::LifecycleError>;

    fn try_delete(
        &mut self,
        now: DateTime<Utc>,
        tombstone_name: String,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: Sized;

    fn try_mark_reconciliation_started(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: Sized;

    fn try_mark_reconciliation_succeeded(
        &mut self,
        now: DateTime<Utc>,
        expected_generation: u64,
        success: Self::ReconcileSuccess,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: Sized;

    fn try_mark_reconciliation_failed(
        &mut self,
        now: DateTime<Utc>,
        expected_generation: u64,
        error: &Self::ReconcileError,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: Sized;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
