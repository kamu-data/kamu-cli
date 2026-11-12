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
    ResourceLifecyclePrimitives,
    ResourceLifecycleTransitionError,
    ResourceStaleExpectationError,
    ResourceVersion,
    SimpleResourceReconciler,
    SimpleResourceReconciliationError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What the second phase of a reconciliation recorded
#[derive(Debug)]
pub enum SimpleResourceReconciliationOutcome<R: EventSourcedResource> {
    /// The resource as `complete_reconciliation` left it
    Reconciled(R),
    /// The resource as `fail_reconciliation` left it, with the error it recorded
    Failed {
        recorded: R,
        error: R::ReconcileError,
    },
    /// Nothing was recorded: the deletion of the resource was requested, or
    /// its generation changed, after the reconciliation started
    Superseded,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs a [`SimpleResourceReconciler`] on a resource whose reconciliation
/// started and records the outcome, within the transaction of its caller
pub struct SimpleResourceReconciliationHelper<'a, R>
where
    R: EventSourcedResource,
{
    lifecycle_primitives: &'a dyn ResourceLifecyclePrimitives<R>,
    reconciler: &'a dyn SimpleResourceReconciler<R>,
}

impl<'a, R> SimpleResourceReconciliationHelper<'a, R>
where
    R: EventSourcedResource,
{
    pub fn new(
        lifecycle_primitives: &'a dyn ResourceLifecyclePrimitives<R>,
        reconciler: &'a dyn SimpleResourceReconciler<R>,
    ) -> Self {
        Self {
            lifecycle_primitives,
            reconciler,
        }
    }

    /// Records the outcome of reconciling `started`, the resource as phase 1
    /// left it. The outcome is written to the current state of the resource,
    /// provided its deletion was not requested, its generation is still the
    /// reconciled one and no other writer updated the status since phase 1.
    pub async fn finish_reconciliation_phase(
        &self,
        started: R,
    ) -> Result<SimpleResourceReconciliationOutcome<R>, SimpleResourceReconciliationError<R>> {
        let id = *started.id();
        let expected = ResourceVersion::from(started.headers());

        // Phase 2 runs in a separate transaction after the started state was
        // committed, so the resource may have changed in between.
        let recorded = match self.reconciler.reconcile(&started).await {
            Ok(()) => self
                .lifecycle_primitives
                .complete_reconciliation(&id, expected)
                .await
                .map(SimpleResourceReconciliationOutcome::Reconciled),
            Err(error) => self
                .lifecycle_primitives
                .fail_reconciliation(&id, expected, &error)
                .await
                .map(|recorded| SimpleResourceReconciliationOutcome::Failed { recorded, error }),
        };

        use ResourceLifecycleTransitionError as E;
        use ResourceStaleExpectationError as Stale;

        match recorded {
            Ok(outcome) => Ok(outcome),
            Err(E::Stale(Stale::DeletionRequested(_))) => {
                tracing::info!(
                    resource_id = %id,
                    "Discarding reconciliation outcome of a resource being deleted"
                );
                Ok(SimpleResourceReconciliationOutcome::Superseded)
            }
            Err(E::Stale(Stale::GenerationChanged(conflict))) => {
                tracing::info!(
                    resource_id = %id,
                    reconciled_generation = conflict.expected,
                    current_generation = conflict.actual,
                    "Discarding reconciliation outcome of a superseded generation"
                );
                Ok(SimpleResourceReconciliationOutcome::Superseded)
            }
            Err(
                err @ (E::Stale(Stale::StatusGenerationChanged(_))
                | E::LoadFailed(_)
                | E::ConcurrentModification(_)
                | E::Lifecycle(_)
                | E::Internal(_)),
            ) => Err(err.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
