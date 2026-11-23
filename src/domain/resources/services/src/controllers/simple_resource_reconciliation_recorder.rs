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
    ResourceID,
    ResourceLifecyclePrimitives,
    ResourceLifecycleTransitionError,
    ResourceStaleExpectationError,
    ResourceVersion,
    SimpleResourceReconciliationError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What recording the outcome of a reconciliation did
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

/// Records the outcome of a reconciliation through the lifecycle primitives,
/// within the transaction of its caller.
///
/// `started` is the version the start of the reconciliation left. The outcome
/// is written to the current state of the resource, provided its deletion was
/// not requested, its generation is still the reconciled one and no other
/// writer updated the status since the start.
pub struct SimpleResourceReconciliationRecorder<'a, R>
where
    R: EventSourcedResource,
{
    lifecycle_primitives: &'a dyn ResourceLifecyclePrimitives<R>,
}

impl<'a, R> SimpleResourceReconciliationRecorder<'a, R>
where
    R: EventSourcedResource,
{
    pub fn new(lifecycle_primitives: &'a dyn ResourceLifecyclePrimitives<R>) -> Self {
        Self {
            lifecycle_primitives,
        }
    }

    /// Records a successful reconciliation
    pub async fn complete_reconciliation(
        &self,
        id: ResourceID,
        started: ResourceVersion,
    ) -> Result<SimpleResourceReconciliationOutcome<R>, SimpleResourceReconciliationError<R>> {
        let recorded = self
            .lifecycle_primitives
            .complete_reconciliation(&id, started)
            .await
            .map(SimpleResourceReconciliationOutcome::Reconciled);

        discard_superseded(id, recorded)
    }

    /// Records a failed reconciliation with its error
    pub async fn fail_reconciliation(
        &self,
        id: ResourceID,
        started: ResourceVersion,
        error: R::ReconcileError,
    ) -> Result<SimpleResourceReconciliationOutcome<R>, SimpleResourceReconciliationError<R>> {
        let recorded = self
            .lifecycle_primitives
            .fail_reconciliation(&id, started, &error)
            .await
            .map(|recorded| SimpleResourceReconciliationOutcome::Failed { recorded, error });

        discard_superseded(id, recorded)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn discard_superseded<R: EventSourcedResource>(
    id: ResourceID,
    recorded: Result<SimpleResourceReconciliationOutcome<R>, ResourceLifecycleTransitionError<R>>,
) -> Result<SimpleResourceReconciliationOutcome<R>, SimpleResourceReconciliationError<R>> {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
