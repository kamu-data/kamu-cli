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

use internal_error::InternalError;

use crate::domain::{
    CompleteResourceDeletionUseCaseError,
    EventSourcedResource,
    ResourceAggregateLoader,
    ResourceCleanupError,
    ResourceCleanupFailure,
    ResourceDeletionCompletion,
    ResourceID,
    ResourceLifecyclePrimitives,
    ResourceLoadError,
    ResourceTransitionSkip,
    ResourceTransitionSkipReason,
    SimpleResourceReconciler,
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
    UseCase(CompleteResourceDeletionUseCaseError<R>),
}

impl<R: EventSourcedResource> From<InternalError> for ResourceCleanupTransactionError<R> {
    fn from(err: InternalError) -> Self {
        Self::UseCase(CompleteResourceDeletionUseCaseError::Internal(err))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CompleteResourceDeletionUseCaseHelper<'a, R>
where
    R: EventSourcedResource,
{
    resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
    lifecycle_primitives: &'a dyn ResourceLifecyclePrimitives<R>,
    reconciler: &'a dyn SimpleResourceReconciler<R>,
}

impl<'a, R> CompleteResourceDeletionUseCaseHelper<'a, R>
where
    R: EventSourcedResource,
{
    pub fn new(
        resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
        lifecycle_primitives: &'a dyn ResourceLifecyclePrimitives<R>,
        reconciler: &'a dyn SimpleResourceReconciler<R>,
    ) -> Self {
        Self {
            resource_aggregate_loader,
            lifecycle_primitives,
            reconciler,
        }
    }

    /// Runs the cleanup of one resource. A failed cleanup is returned as an
    /// error, so that the transaction of the caller rolls back. Returns `None`
    /// when the resource does not wait for a cleanup.
    pub async fn cleanup(
        &self,
        id: ResourceID,
    ) -> Result<Option<ResourceCleanupAttempt>, ResourceCleanupTransactionError<R>> {
        let resource = self
            .resource_aggregate_loader
            .load(&id)
            .await
            .map_err(ResourceLoadError)
            .map_err(CompleteResourceDeletionUseCaseError::LoadFailed)
            .map_err(ResourceCleanupTransactionError::UseCase)?;
        if !resource.needs_cleanup() {
            return Ok(None);
        }

        let status_generation = resource.headers().status_generation;
        match self.reconciler.cleanup(&resource).await {
            Ok(()) => Ok(Some(ResourceCleanupAttempt {
                id,
                status_generation,
                failure: None,
            })),
            Err(err) => {
                tracing::warn!(
                    resource_id = %id,
                    error = %err,
                    "Cleanup of a resource being deleted failed"
                );
                Err(ResourceCleanupTransactionError::CleanupFailed(
                    ResourceCleanupAttempt {
                        id,
                        status_generation,
                        failure: Some(Arc::new(err)),
                    },
                ))
            }
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
    ) -> Result<(), CompleteResourceDeletionUseCaseError<R>> {
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

/// The phases of completing deletions. Each generated use case runs them in
/// transactions of their own; the provided methods orchestrate them.
#[async_trait::async_trait]
pub trait CompleteResourceDeletionPhases<R: EventSourcedResource>: Sync {
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
    ) -> Result<(), CompleteResourceDeletionUseCaseError<R>>;

    /// Runs each cleanup in a transaction of its own, so that a failing one,
    /// even a database error, holds back only its resource. Then records the
    /// outcomes as one batch, so that one `Deleted` message covers them.
    async fn complete_deletions(
        &self,
        ids: &[ResourceID],
    ) -> Result<(), CompleteResourceDeletionUseCaseError<R>> {
        let mut attempts = Vec::with_capacity(ids.len());
        for &id in ids {
            match self.run_cleanup(id).await {
                Ok(Some(attempt))
                | Err(ResourceCleanupTransactionError::CleanupFailed(attempt)) => {
                    attempts.push(attempt);
                }
                Ok(None) => {}
                Err(ResourceCleanupTransactionError::UseCase(err)) => return Err(err),
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
    ) -> Result<(), CompleteResourceDeletionUseCaseError<R>> {
        use CompleteResourceDeletionUseCaseError as E;

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

#[macro_export]
macro_rules! declare_complete_resource_deletion_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::CompleteResourceDeletionUseCase<$resource>)]
        pub struct $use_case {
            catalog: dill::Catalog,
        }

        impl $use_case {
            #[::database_common_macros::transactional_method3(
                        resource_aggregate_loader:
                            std::sync::Arc<dyn kamu_resources::ResourceAggregateLoader<$resource>>,
                        lifecycle_primitives:
                            std::sync::Arc<dyn kamu_resources::ResourceLifecyclePrimitives<$resource>>,
                        reconciler:
                            std::sync::Arc<dyn kamu_resources::SimpleResourceReconciler<$resource>>
                    )]
            async fn cleanup_in_transaction(
                &self,
                id: kamu_resources::ResourceID,
            ) -> Result<
                Option<$crate::ResourceCleanupAttempt>,
                $crate::ResourceCleanupTransactionError<$resource>,
            > {
                let helper = $crate::CompleteResourceDeletionUseCaseHelper::<$resource>::new(
                    resource_aggregate_loader.as_ref(),
                    lifecycle_primitives.as_ref(),
                    reconciler.as_ref(),
                );

                helper.cleanup(id).await
            }

            #[::database_common_macros::transactional_method3(
                        resource_aggregate_loader:
                            std::sync::Arc<dyn kamu_resources::ResourceAggregateLoader<$resource>>,
                        lifecycle_primitives:
                            std::sync::Arc<dyn kamu_resources::ResourceLifecyclePrimitives<$resource>>,
                        reconciler:
                            std::sync::Arc<dyn kamu_resources::SimpleResourceReconciler<$resource>>
                    )]
            async fn record_in_transaction(
                &self,
                attempts: Vec<$crate::ResourceCleanupAttempt>,
            ) -> Result<(), kamu_resources::CompleteResourceDeletionUseCaseError<$resource>> {
                let helper = $crate::CompleteResourceDeletionUseCaseHelper::<$resource>::new(
                    resource_aggregate_loader.as_ref(),
                    lifecycle_primitives.as_ref(),
                    reconciler.as_ref(),
                );

                helper.record_outcomes(&attempts).await
            }
        }

        #[async_trait::async_trait]
        impl $crate::CompleteResourceDeletionPhases<$resource> for $use_case {
            async fn run_cleanup(
                &self,
                id: kamu_resources::ResourceID,
            ) -> Result<
                Option<$crate::ResourceCleanupAttempt>,
                $crate::ResourceCleanupTransactionError<$resource>,
            > {
                self.cleanup_in_transaction(id).await
            }

            async fn record_outcomes(
                &self,
                attempts: Vec<$crate::ResourceCleanupAttempt>,
            ) -> Result<(), kamu_resources::CompleteResourceDeletionUseCaseError<$resource>> {
                self.record_in_transaction(attempts).await
            }
        }

        #[async_trait::async_trait]
        impl kamu_resources::CompleteResourceDeletionUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                ids: &[kamu_resources::ResourceID],
            ) -> Result<(), kamu_resources::CompleteResourceDeletionUseCaseError<$resource>> {
                $crate::CompleteResourceDeletionPhases::complete_deletions(self, ids).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
