// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;
use std::sync::Arc;

use database_common::transaction;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use time_source::SystemTimeSource;

use crate::domain::{
    EventSourcedResource,
    ResourceAggregateLoader,
    ResourceID,
    ResourceLifecyclePrimitives,
    ResourceLifecycleTransitionError,
    ResourceProcessOutcome,
    ResourceReconcileError,
    ResourceStaleExpectationError,
    ResourceVersion,
    SimpleResourceDeletionError,
    SimpleResourceReconciler,
    SimpleResourceReconciliationError,
};
use crate::{
    ResourceCleanupTransactionError,
    SimpleResourceCleanupHelper,
    SimpleResourceCleanupOutcome,
    SimpleResourceDeletionRecorder,
    SimpleResourceReconciliationHelper,
    SimpleResourceReconciliationOutcome,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The next step for one resource of a type driven by a
/// `SimpleResourceReconciler`, as the runtime asks for it.
///
/// Each phase runs in a transaction of its own:
///
/// | State | Step | Outcome |
/// | --- | --- | --- |
/// | Deletion requested | `cleanup`, then `complete_deletion` | `Done` |
/// | Cleanup failed | `record_cleanup_failure` | an error, retried with backoff |
/// | Needs reconciliation | `begin_reconciliation`, `reconcile`, `complete_reconciliation` | `Done` |
/// | Reconcile failed | `fail_reconciliation` | `Done`, or an error when the failure is transient |
/// | Deleted meanwhile | nothing | `Done` |
/// | Changed meanwhile by another writer | nothing | `RetryAt(now)` |
/// | Anything else | nothing | `Done` |
///
/// A resource that changes meanwhile is processed again in the next pass of
/// the drain, which reads its current state.
///
/// Public only for `declare_simple_resource_controller_adapter!`, which
/// registers it for each type in the crate of that type.
#[doc(hidden)]
pub struct SimpleResourceController<'a, R> {
    catalog: &'a dill::Catalog,
    time_source: &'a dyn SystemTimeSource,
    _resource: PhantomData<fn() -> R>,
}

impl<'a, R> SimpleResourceController<'a, R>
where
    R: EventSourcedResource + 'static,
    R::LifecycleError: std::error::Error + Send + Sync + 'static,
{
    pub fn new(catalog: &'a dill::Catalog, time_source: &'a dyn SystemTimeSource) -> Self {
        Self {
            catalog,
            time_source,
            _resource: PhantomData,
        }
    }

    pub async fn process(&self, id: ResourceID) -> Result<ResourceProcessOutcome, InternalError> {
        let resource = transaction!(
            |loader: Arc<dyn ResourceAggregateLoader<R>>| -> Result<_, InternalError> {
                loader.load(&id).await.int_err()
            }
        )?;
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
        use SimpleResourceReconciliationError as Failure;
        use SimpleResourceReconciliationOutcome as Outcome;

        // 1: Start, committed before the reconciler runs, a stable hand-off
        // point for the second phase
        let started = transaction!(
            |primitives: Arc<dyn ResourceLifecyclePrimitives<R>>| -> Result<_, E<R>> {
                primitives.begin_reconciliation(&id, None).await
            }
        );
        let started = match started {
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

        // 2: Reconcile and record the outcome
        let finished = transaction!(|primitives: Arc<dyn ResourceLifecyclePrimitives<R>>,
                                     reconciler: Arc<dyn SimpleResourceReconciler<R>>|
         -> Result<_, Failure<R>> {
            SimpleResourceReconciliationHelper::new(primitives.as_ref(), reconciler.as_ref())
                .finish_reconciliation_phase(started)
                .await
        });
        match finished {
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

        // 1: Clean up
        let cleaned_up = transaction!(|loader: Arc<dyn ResourceAggregateLoader<R>>,
                                       reconciler: Arc<dyn SimpleResourceReconciler<R>>|
         -> Result<_, Cleanup<R>> {
            SimpleResourceCleanupHelper::new(loader.as_ref(), reconciler.as_ref())
                .cleanup(id)
                .await
        });
        match cleaned_up {
            Ok(SimpleResourceCleanupOutcome::CleanedUp) => {}
            Ok(SimpleResourceCleanupOutcome::NotAwaitingCleanup) => {
                // The deletion completed meanwhile
                let deleted = transaction!(
                    |loader: Arc<dyn ResourceAggregateLoader<R>>| -> Result<_, InternalError> {
                        loader.load(&id).await.int_err()
                    }
                )?;
                return Ok(ResourceProcessOutcome::Done(ResourceVersion::from(
                    deleted.headers(),
                )));
            }
            Err(Cleanup::CleanupFailed(failure)) => {
                let error = failure.error.clone();

                // 2a: Record the failure
                let recorded = transaction!(|primitives: Arc<
                    dyn ResourceLifecyclePrimitives<R>,
                >|
                 -> Result<_, E<R>> {
                    SimpleResourceDeletionRecorder::new(primitives.as_ref())
                        .record_cleanup_failure(failure)
                        .await
                });
                match recorded {
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

        // 2b: Complete the deletion
        let completed = transaction!(
            |primitives: Arc<dyn ResourceLifecyclePrimitives<R>>| -> Result<_, E<R>> {
                SimpleResourceDeletionRecorder::new(primitives.as_ref())
                    .complete_deletion(id)
                    .await
            }
        );
        match completed {
            Ok(Some(deleted)) => Ok(ResourceProcessOutcome::Done(ResourceVersion::from(
                deleted.headers(),
            ))),
            Ok(None) => {
                // The deletion completed meanwhile
                let deleted = transaction!(
                    |loader: Arc<dyn ResourceAggregateLoader<R>>| -> Result<_, InternalError> {
                        loader.load(&id).await.int_err()
                    }
                )?;
                Ok(ResourceProcessOutcome::Done(ResourceVersion::from(
                    deleted.headers(),
                )))
            }
            // Another writer saved the resource first
            Err(E::ConcurrentModification(_)) => Ok(self.retry_now()),
            Err(err @ (E::LoadFailed(_) | E::Internal(_) | E::Lifecycle(_))) => Err(err.int_err()),
        }
    }

    fn retry_now(&self) -> ResourceProcessOutcome {
        ResourceProcessOutcome::RetryAt(self.time_source.now())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
