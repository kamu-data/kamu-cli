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
    ReconcilableEventSourcedResource,
    ReconcileFailureMapper,
    ResourceReconcileError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ReconcilableResource: DeclarativeResource {
    type ReconcileSuccess;
    type ReconcileError: ResourceReconcileError;
    type FailureDetails;
    type LifecycleError;

    fn needs_reconciliation(&self) -> bool {
        use crate::ResourceStatusLike;

        self.status()
            .resource_status()
            .needs_reconciliation(self.metadata().generation)
    }

    fn try_mark_reconciliation_started(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: ReconcilableEventSourcedResource + Sized,
    {
        crate::try_mark_resource_reconciliation_started(self, now)
    }

    fn try_mark_reconciliation_succeeded(
        &mut self,
        now: DateTime<Utc>,
        expected_generation: u64,
        success: Self::ReconcileSuccess,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: ReconcilableEventSourcedResource + Sized,
    {
        crate::try_mark_resource_reconciliation_succeeded(self, now, expected_generation, success)
    }

    fn try_mark_reconciliation_failed(
        &mut self,
        now: DateTime<Utc>,
        expected_generation: u64,
        error: &Self::ReconcileError,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: ReconcileFailureMapper + ReconcilableEventSourcedResource + Sized,
    {
        crate::try_mark_resource_reconciliation_failed(self, now, expected_generation, error)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
