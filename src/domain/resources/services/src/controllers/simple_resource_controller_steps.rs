// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{ErrorIntoInternal, InternalError};

use crate::domain::{
    EventSourcedResource,
    ResourceCleanupFailure,
    ResourceID,
    ResourceLifecycleTransitionError,
    ResourceProcessOutcome,
    ResourceReconcileError,
    ResourceStaleExpectationError,
    ResourceVersion,
    SimpleResourceDeletionError,
    SimpleResourceReconciliationError,
};
use crate::{
    ResourceCleanupTransactionError,
    SimpleResourceCleanupOutcome,
    SimpleResourceReconciliationOutcome,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The next step for one resource of a type driven by a
/// `SimpleResourceReconciler`, as the runtime asks for it.
///
/// Each phase runs in a transaction of its own, which the implementor opens:
///
/// | State | Step | Outcome |
/// | --- | --- | --- |
/// | Deletion requested | `cleanup`, then `complete_deletion` | `Done` |
/// | Cleanup failed | `record_cleanup_failure` | an error, retried with backoff |
/// | Needs reconciliation | `begin_reconciliation`, `reconcile`, `complete_reconciliation` | `Done` |
/// | Reconcile failed | `fail_reconciliation` | `Done`, or an error when the failure is transient |
/// | Changed meanwhile by another writer | nothing | `RetryAt(now)` |
/// | Anything else | nothing | `Done` |
///
/// A resource that changes meanwhile is processed again in the next pass of
/// the drain, which reads its current state.
///
/// Public only for `declare_simple_resource_controller_adapter!`, which
/// implements it in the crate of each type.
#[doc(hidden)]
#[async_trait::async_trait]
pub trait SimpleResourceControllerSteps<R>: Sync
where
    R: EventSourcedResource + 'static,
    R::LifecycleError: std::error::Error + Send + Sync + 'static,
{
    fn now(&self) -> DateTime<Utc>;

    async fn load(&self, id: ResourceID) -> Result<R, InternalError>;

    /// Returns `None` when the resource needs no reconciliation
    async fn start_reconciliation(
        &self,
        id: ResourceID,
    ) -> Result<Option<R>, ResourceLifecycleTransitionError<R>>;

    /// Runs `reconcile` and records its outcome
    async fn finish_reconciliation(
        &self,
        started: R,
    ) -> Result<SimpleResourceReconciliationOutcome<R>, SimpleResourceReconciliationError<R>>;

    async fn run_cleanup(
        &self,
        id: ResourceID,
    ) -> Result<SimpleResourceCleanupOutcome, ResourceCleanupTransactionError<R>>;

    /// Returns the resource in its new state, or `None` when its deletion
    /// completed meanwhile
    async fn complete_deletion(
        &self,
        id: ResourceID,
    ) -> Result<Option<R>, SimpleResourceDeletionError<R>>;

    async fn record_cleanup_failure(
        &self,
        failure: ResourceCleanupFailure,
    ) -> Result<(), SimpleResourceDeletionError<R>>;

    async fn process_resource(
        &self,
        id: ResourceID,
    ) -> Result<ResourceProcessOutcome, InternalError> {
        let resource = self.load(id).await?;
        let version = ResourceVersion::from(resource.headers());

        if resource.needs_cleanup() {
            self.process_deletion(id).await
        } else if resource.needs_reconciliation() {
            self.process_reconciliation(id, version).await
        } else {
            Ok(ResourceProcessOutcome::Done(version))
        }
    }

    async fn process_reconciliation(
        &self,
        id: ResourceID,
        loaded: ResourceVersion,
    ) -> Result<ResourceProcessOutcome, InternalError> {
        use ResourceLifecycleTransitionError as E;
        use ResourceStaleExpectationError as Stale;

        let started = match self.start_reconciliation(id).await {
            Ok(Some(started)) => started,
            Ok(None) => return Ok(ResourceProcessOutcome::Done(loaded)),
            Err(
                E::Stale(
                    Stale::DeletionRequested(_)
                    | Stale::GenerationChanged(_)
                    | Stale::StatusGenerationChanged(_),
                )
                | E::ConcurrentModification(_),
            ) => return Ok(self.retry_now()),
            Err(err @ (E::LoadFailed(_) | E::Lifecycle(_) | E::Internal(_))) => {
                return Err(err.int_err());
            }
        };

        use SimpleResourceReconciliationError as Failure;
        use SimpleResourceReconciliationOutcome as Outcome;

        match self.finish_reconciliation(started).await {
            Ok(Outcome::Reconciled(reconciled)) => Ok(ResourceProcessOutcome::Done(
                ResourceVersion::from(reconciled.headers()),
            )),
            Ok(Outcome::Failed { recorded, error }) => {
                if error.is_transient() {
                    // Through the error path, so that the runtime backs off
                    // more with each attempt and keeps the error
                    Err(error.int_err())
                } else {
                    Ok(ResourceProcessOutcome::Done(ResourceVersion::from(
                        recorded.headers(),
                    )))
                }
            }
            Ok(Outcome::Superseded)
            | Err(Failure::StatusConflict(_) | Failure::ConcurrentModification(_)) => {
                Ok(self.retry_now())
            }
            Err(err @ (Failure::LoadFailed(_) | Failure::Internal(_) | Failure::Lifecycle(_))) => {
                Err(err.int_err())
            }
        }
    }

    async fn process_deletion(
        &self,
        id: ResourceID,
    ) -> Result<ResourceProcessOutcome, InternalError> {
        use ResourceCleanupTransactionError as Cleanup;
        use SimpleResourceDeletionError as E;

        match self.run_cleanup(id).await {
            Ok(SimpleResourceCleanupOutcome::CleanedUp) => {}
            Ok(SimpleResourceCleanupOutcome::NotAwaitingCleanup) => return Ok(self.retry_now()),
            Err(Cleanup::CleanupFailed(failure)) => {
                let error = failure.error.clone();
                match self.record_cleanup_failure(failure).await {
                    Ok(()) | Err(E::ConcurrentModification(_)) => {}
                    Err(err @ (E::LoadFailed(_) | E::Internal(_) | E::Lifecycle(_))) => {
                        return Err(err.int_err());
                    }
                }
                // Through the error path, so that the cleanup is retried
                // with a backoff
                return Err(error.int_err());
            }
            Err(Cleanup::Deletion(err)) => return Err(err.int_err()),
        }

        match self.complete_deletion(id).await {
            Ok(Some(deleted)) => Ok(ResourceProcessOutcome::Done(ResourceVersion::from(
                deleted.headers(),
            ))),
            // Skipped, because the deletion completed meanwhile, or another
            // writer saved the resource first
            Ok(None) | Err(E::ConcurrentModification(_)) => Ok(self.retry_now()),
            Err(err @ (E::LoadFailed(_) | E::Internal(_) | E::Lifecycle(_))) => Err(err.int_err()),
        }
    }

    fn retry_now(&self) -> ResourceProcessOutcome {
        ResourceProcessOutcome::RetryAt(self.now())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
