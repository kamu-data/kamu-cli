// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::domain::{
    DeclarativeResource,
    InvariantViolationOf,
    ReconcilableEventSourcedResource,
    ReconcileResourceUseCaseError,
    Reconciler,
    ResourceDescriptorProvider,
    ResourcePersistenceService,
    ResourceStatusLike,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn persist_resource_state<R>(
    resource_persistence_service: &dyn ResourcePersistenceService<R>,
    resource: &mut R,
) -> Result<(), ReconcileResourceUseCaseError<R>>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    resource_persistence_service
        .save(resource)
        .await
        .map_err(ReconcileResourceUseCaseError::from)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn start_reconciliation_phase<R>(
    resource_persistence_service: &dyn ResourcePersistenceService<R>,
    mut resource: R,
    now: DateTime<Utc>,
) -> Result<R, ReconcileResourceUseCaseError<R>>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    // Phase 1 commits the "started" transition before the actual reconciliation
    // work, giving the second phase a stable persisted hand-off point.
    resource
        .try_mark_reconciliation_started(now)
        .map_err(ReconcileResourceUseCaseError::Lifecycle)?;

    persist_resource_state(resource_persistence_service, &mut resource).await?;

    Ok(resource)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn finish_reconciliation_phase<R>(
    resource_persistence_service: &dyn ResourcePersistenceService<R>,
    reconciler: &dyn Reconciler<R>,
    mut resource: R,
    now: DateTime<Utc>,
) -> Result<(), ReconcileResourceUseCaseError<R>>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    // Phase 2 runs in a separate transaction after the started state was
    // committed, so concurrent changes between the two phases are expected.
    match reconciler.reconcile(&resource).await {
        Ok(success) => {
            resource
                .try_mark_reconciliation_succeeded(now, resource.metadata().generation, success)
                .map_err(ReconcileResourceUseCaseError::Lifecycle)?;
        }
        Err(err) => {
            resource
                .try_mark_reconciliation_failed(now, resource.metadata().generation, &err)
                .map_err(ReconcileResourceUseCaseError::Lifecycle)?;
        }
    }
    persist_resource_state(resource_persistence_service, &mut resource).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! declare_reconcile_resource_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty,
        store = $store_trait:ident
    ) => {
        #[dill::component]
        #[dill::interface(dyn $crate::domain::ReconcileResourceUseCase<$resource>)]
        pub struct $use_case {
            catalog: dill::Catalog,
            reconciler: std::sync::Arc<dyn $crate::domain::Reconciler<$resource>>,
            time_source: std::sync::Arc<dyn time_source::SystemTimeSource>,
        }

        impl $use_case {
            #[::database_common_macros::transactional_method2(
                        resource_aggregate_loader:
                            std::sync::Arc<dyn $crate::domain::ResourceAggregateLoader<$resource>>,
                        resource_persistence_service:
                            std::sync::Arc<dyn $crate::domain::ResourcePersistenceService<$resource>>
                    )]
            async fn start_reconciliation_phase(
                &self,
                id: $crate::domain::ResourceID,
            ) -> Result<Option<$resource>, $crate::domain::ReconcileResourceUseCaseError<$resource>>
            {
                use $crate::domain::ReconcilableResource;

                // Load and persist the first transition inside a single
                // transaction, then return the committed aggregate state.
                let resource = resource_aggregate_loader
                    .load(&id)
                    .await
                    .map_err($crate::domain::ResourceLoadError)
                    .map_err($crate::domain::ReconcileResourceUseCaseError::LoadFailed)?;
                if !resource.needs_reconciliation() {
                    return Ok(None);
                }

                let resource = $crate::use_cases::reconcile::shared::start_reconciliation_phase(
                    resource_persistence_service.as_ref(),
                    resource,
                    self.time_source.now(),
                )
                .await?;

                Ok(Some(resource))
            }

            #[::database_common_macros::transactional_method1(
                        resource_persistence_service:
                            std::sync::Arc<dyn $crate::domain::ResourcePersistenceService<$resource>>
                    )]
            async fn finish_reconciliation_phase(
                &self,
                resource: $resource,
            ) -> Result<(), $crate::domain::ReconcileResourceUseCaseError<$resource>> {
                // Resume from the aggregate returned by phase 1 and record the
                // reconciler outcome in a new transaction.
                $crate::use_cases::reconcile::shared::finish_reconciliation_phase(
                    resource_persistence_service.as_ref(),
                    self.reconciler.as_ref(),
                    resource,
                    self.time_source.now(),
                )
                .await
            }
        }

        #[async_trait::async_trait]
        impl $crate::domain::ReconcileResourceUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                id: &$crate::domain::ResourceID,
            ) -> Result<(), $crate::domain::ReconcileResourceUseCaseError<$resource>> {
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

pub(crate) use declare_reconcile_resource_use_case;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
