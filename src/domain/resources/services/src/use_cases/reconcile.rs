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
    ReconcileResourceUseCaseError,
    ResourceExpectedVersion,
    ResourceID,
    ResourceLifecyclePrimitives,
    ResourceLifecycleTransitionError,
    ResourceStaleExpectationError,
    SimpleResourceReconciler,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ReconcileResourceUseCaseHelper<'a, R>
where
    R: EventSourcedResource,
{
    lifecycle_primitives: &'a dyn ResourceLifecyclePrimitives<R>,
    reconciler: &'a dyn SimpleResourceReconciler<R>,
}

impl<'a, R> ReconcileResourceUseCaseHelper<'a, R>
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

    pub async fn start_reconciliation_phase(
        &self,
        id: ResourceID,
    ) -> Result<Option<R>, ReconcileResourceUseCaseError<R>> {
        // Phase 1 commits the "started" transition before the actual reconciliation
        // work, giving the second phase a stable persisted hand-off point.
        let started = self
            .lifecycle_primitives
            .begin_reconciliation(&id, None)
            .await?;

        Ok(started)
    }

    /// Records the outcome of reconciling `started`, the resource as phase 1
    /// left it. The outcome is written to the current state of the resource,
    /// provided its deletion was not requested, its generation is still the
    /// reconciled one and no other writer updated the status since phase 1.
    pub async fn finish_reconciliation_phase(
        &self,
        started: R,
    ) -> Result<(), ReconcileResourceUseCaseError<R>> {
        let id = *started.id();
        let expected = ResourceExpectedVersion {
            generation: started.headers().generation,
            status_generation: started.headers().status_generation,
        };

        // Phase 2 runs in a separate transaction after the started state was
        // committed, so the resource may have changed in between.
        let recorded = match self.reconciler.reconcile(&started).await {
            Ok(()) => {
                self.lifecycle_primitives
                    .complete_reconciliation(&id, expected)
                    .await
            }
            Err(err) => {
                self.lifecycle_primitives
                    .fail_reconciliation(&id, expected, &err)
                    .await
            }
        };

        use ResourceLifecycleTransitionError as E;
        use ResourceStaleExpectationError as Stale;

        match recorded {
            Ok(_) => Ok(()),
            Err(E::Stale(Stale::DeletionRequested(_))) => {
                tracing::info!(
                    resource_id = %id,
                    "Discarding reconciliation outcome of a resource being deleted"
                );
                Ok(())
            }
            Err(E::Stale(Stale::GenerationChanged(conflict))) => {
                tracing::info!(
                    resource_id = %id,
                    reconciled_generation = conflict.expected,
                    current_generation = conflict.actual,
                    "Discarding reconciliation outcome of a superseded generation"
                );
                Ok(())
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

#[macro_export]
macro_rules! declare_reconcile_resource_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ReconcileResourceUseCase<$resource>)]
        pub struct $use_case {
            catalog: dill::Catalog,
        }

        impl $use_case {
            #[::database_common_macros::transactional_method2(
                        lifecycle_primitives:
                            std::sync::Arc<dyn kamu_resources::ResourceLifecyclePrimitives<$resource>>,
                        reconciler:
                            std::sync::Arc<dyn kamu_resources::SimpleResourceReconciler<$resource>>
                    )]
            async fn start_reconciliation_phase(
                &self,
                id: kamu_resources::ResourceID,
            ) -> Result<Option<$resource>, kamu_resources::ReconcileResourceUseCaseError<$resource>>
            {
                let helper = $crate::ReconcileResourceUseCaseHelper::<$resource>::new(
                    lifecycle_primitives.as_ref(),
                    reconciler.as_ref(),
                );

                helper.start_reconciliation_phase(id).await
            }

            #[::database_common_macros::transactional_method2(
                        lifecycle_primitives:
                            std::sync::Arc<dyn kamu_resources::ResourceLifecyclePrimitives<$resource>>,
                        reconciler:
                            std::sync::Arc<dyn kamu_resources::SimpleResourceReconciler<$resource>>
                    )]
            async fn finish_reconciliation_phase(
                &self,
                resource: $resource,
            ) -> Result<(), kamu_resources::ReconcileResourceUseCaseError<$resource>> {
                // Reconcile the aggregate returned by phase 1 and record the
                // outcome in a new transaction.
                let helper = $crate::ReconcileResourceUseCaseHelper::<$resource>::new(
                    lifecycle_primitives.as_ref(),
                    reconciler.as_ref(),
                );

                helper.finish_reconciliation_phase(resource).await
            }
        }

        #[async_trait::async_trait]
        impl kamu_resources::ReconcileResourceUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                id: &kamu_resources::ResourceID,
            ) -> Result<(), kamu_resources::ReconcileResourceUseCaseError<$resource>> {
                // The public use case only orchestrates the two committed
                // phases; transaction-scoped dependencies stay inside helpers.
                let Some(resource) = self.start_reconciliation_phase(id.clone()).await? else {
                    return Ok(());
                };

                self.finish_reconciliation_phase(resource).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
