// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::{
    EventSourcedResource,
    ResourceCleanupFailure,
    ResourceDeletionCompletion,
    ResourceID,
    ResourceLifecyclePrimitives,
    ResourceTransitionSkip,
    ResourceTransitionSkipReason,
    SimpleResourceDeletionError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Records the outcome of a cleanup through the lifecycle primitives, within
/// the transaction of its caller
pub struct SimpleResourceDeletionRecorder<'a, R>
where
    R: EventSourcedResource,
{
    lifecycle_primitives: &'a dyn ResourceLifecyclePrimitives<R>,
}

impl<'a, R> SimpleResourceDeletionRecorder<'a, R>
where
    R: EventSourcedResource,
{
    pub fn new(lifecycle_primitives: &'a dyn ResourceLifecyclePrimitives<R>) -> Self {
        Self {
            lifecycle_primitives,
        }
    }

    /// Completes the deletion of a cleaned-up resource. Returns it in its new
    /// state, or `None` when its deletion completed meanwhile.
    pub async fn complete_deletion(
        &self,
        id: ResourceID,
    ) -> Result<Option<R>, SimpleResourceDeletionError<R>> {
        let transition = self
            .lifecycle_primitives
            .complete_deletion(&[ResourceDeletionCompletion {
                id,
                expected_status_generation: None,
            }])
            .await?;
        log_skipped_outcomes(&transition.skipped);

        Ok(transition.transitioned.into_iter().next())
    }

    /// Records a failed cleanup in the status. Skipped when another writer
    /// updated the status since the cleanup started.
    pub async fn record_cleanup_failure(
        &self,
        failure: ResourceCleanupFailure,
    ) -> Result<(), SimpleResourceDeletionError<R>> {
        let transition = self
            .lifecycle_primitives
            .record_cleanup_failure(&[failure])
            .await?;
        log_skipped_outcomes(&transition.skipped);

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn log_skipped_outcomes(skipped: &[ResourceTransitionSkip]) {
    for skip in skipped {
        log_skipped_outcome(skip);
    }
}

fn log_skipped_outcome(skip: &ResourceTransitionSkip) {
    match skip.reason {
        ResourceTransitionSkipReason::NotAwaitingCleanup => {}
        ResourceTransitionSkipReason::StatusConflict(conflict) => {
            tracing::info!(
                resource_id = %skip.id,
                error = %conflict,
                "Skipping cleanup outcome: status was updated concurrently"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
