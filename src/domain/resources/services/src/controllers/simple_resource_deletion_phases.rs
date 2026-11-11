// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;

use crate::domain::{
    EventSourcedResource,
    ResourceCleanupError,
    ResourceID,
    SimpleResourceDeletionError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A cleanup the reconciler ran, recorded once its own transaction has
/// ended
#[derive(Debug, Clone)]
pub struct ResourceCleanupAttempt {
    pub id: ResourceID,
    /// The status generation the cleanup started from
    pub status_generation: u64,
    pub failure: Option<Arc<ResourceCleanupError>>,
}

/// Ends the transaction of a cleanup. A failed cleanup rolls it back, which
/// discards whatever the reconciler wrote before failing; its failure is then
/// recorded in a transaction of its own.
pub enum ResourceCleanupTransactionError<R: EventSourcedResource> {
    CleanupFailed(ResourceCleanupAttempt),
    Deletion(SimpleResourceDeletionError<R>),
}

impl<R: EventSourcedResource> From<InternalError> for ResourceCleanupTransactionError<R> {
    fn from(err: InternalError) -> Self {
        Self::Deletion(SimpleResourceDeletionError::Internal(err))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The phases of completing deletions. The adapter of each type runs them in
/// transactions of their own; the provided methods orchestrate them.
#[async_trait::async_trait]
pub trait SimpleResourceDeletionPhases<R: EventSourcedResource>: Sync {
    /// Runs the cleanup of one resource within a transaction of its own.
    /// Returns `None` when the resource does not wait for a cleanup.
    async fn run_cleanup(
        &self,
        id: ResourceID,
    ) -> Result<Option<ResourceCleanupAttempt>, ResourceCleanupTransactionError<R>>;

    /// Records the outcomes within one transaction.
    async fn record_outcomes(
        &self,
        attempts: Vec<ResourceCleanupAttempt>,
    ) -> Result<(), SimpleResourceDeletionError<R>>;

    /// Runs each cleanup in a transaction of its own, so that a failing one,
    /// even a database error, holds back only its resource. Then records the
    /// outcomes as one batch, so that one `Deleted` message covers them.
    async fn run_deletion_phases(
        &self,
        ids: &[ResourceID],
    ) -> Result<(), SimpleResourceDeletionError<R>> {
        let mut attempts = Vec::with_capacity(ids.len());
        for &id in ids {
            match self.run_cleanup(id).await {
                Ok(Some(attempt))
                | Err(ResourceCleanupTransactionError::CleanupFailed(attempt)) => {
                    attempts.push(attempt);
                }
                Ok(None) => {}
                Err(ResourceCleanupTransactionError::Deletion(err)) => return Err(err),
            }
        }

        if attempts.is_empty() {
            return Ok(());
        }

        self.record_outcomes_in_batch(attempts).await
    }

    /// A concurrent modification of any resource rolls the batch back; the
    /// outcomes are then recorded one by one, so the conflict holds back only
    /// the resource it hit.
    async fn record_outcomes_in_batch(
        &self,
        attempts: Vec<ResourceCleanupAttempt>,
    ) -> Result<(), SimpleResourceDeletionError<R>> {
        use SimpleResourceDeletionError as E;

        match self.record_outcomes(attempts.clone()).await {
            Err(E::ConcurrentModification(err)) if attempts.len() > 1 => {
                tracing::info!(
                    error = %err,
                    "Recording cleanup outcomes one by one after a concurrent modification of the \
                     batch"
                );
            }
            result => return result,
        }

        for attempt in attempts {
            let id = attempt.id;
            match self.record_outcomes(vec![attempt]).await {
                Ok(()) => {}
                // The other writer completes the deletion or requests it again
                Err(E::ConcurrentModification(err)) => {
                    tracing::info!(
                        resource_id = %id,
                        error = %err,
                        "Skipping cleanup outcome: resource was modified concurrently"
                    );
                }
                Err(err @ (E::LoadFailed(_) | E::Internal(_) | E::Lifecycle(_))) => {
                    return Err(err);
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
