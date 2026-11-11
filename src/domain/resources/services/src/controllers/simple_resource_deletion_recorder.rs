// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use crate::ResourceCleanupAttempt;
use crate::domain::{
    EventSourcedResource,
    ResourceCleanupFailure,
    ResourceDeletionCompletion,
    ResourceLifecyclePrimitives,
    ResourceTransitionSkip,
    ResourceTransitionSkipReason,
    SimpleResourceDeletionError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Records the outcomes of cleanups through the lifecycle primitives, within
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

    /// Runs within one transaction: the outcomes and the `Deleted` message of
    /// the completed deletions commit together. A resource whose deletion
    /// completed meanwhile is skipped, and so is a failed cleanup of a resource
    /// whose status another writer updated since. When the same resource was
    /// cleaned up more than once, its last attempt counts.
    pub async fn record_outcomes(
        &self,
        attempts: &[ResourceCleanupAttempt],
    ) -> Result<(), SimpleResourceDeletionError<R>> {
        let mut completions = Vec::new();
        let mut failures = Vec::new();
        for attempt in last_attempt_per_resource(attempts) {
            match &attempt.failure {
                None => completions.push(ResourceDeletionCompletion {
                    id: attempt.id,
                    expected_status_generation: None,
                }),
                Some(error) => failures.push(ResourceCleanupFailure {
                    id: attempt.id,
                    expected_status_generation: attempt.status_generation,
                    error: Arc::clone(error),
                }),
            }
        }

        // Failures first, so the `Deleted` message is the last write
        let failed = self
            .lifecycle_primitives
            .record_cleanup_failure(&failures)
            .await?;
        let completed = self
            .lifecycle_primitives
            .complete_deletion(&completions)
            .await?;

        for skip in failed.skipped.iter().chain(&completed.skipped) {
            log_skipped_outcome(skip);
        }

        Ok(())
    }
}

/// The batch primitives take each resource at most once
fn last_attempt_per_resource(
    attempts: &[ResourceCleanupAttempt],
) -> impl Iterator<Item = &ResourceCleanupAttempt> {
    let last_index_by_id = attempts
        .iter()
        .enumerate()
        .map(|(index, attempt)| (attempt.id, index))
        .collect::<HashMap<_, _>>();

    attempts
        .iter()
        .enumerate()
        .filter(move |(index, attempt)| last_index_by_id[&attempt.id] == *index)
        .map(|(_, attempt)| attempt)
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
