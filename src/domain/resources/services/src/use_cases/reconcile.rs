// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Serialize;
use time_source::SystemTimeSource;

use crate::domain::{
    DeclarativeResource,
    InvariantViolationOf,
    ReconcilableEventSourcedResource,
    ReconcileResourceUseCaseError,
    Reconciler,
    ResourceAggregateLoader,
    ResourceDescriptorProvider,
    ResourceID,
    ResourceLoadError,
    ResourcePersistenceService,
    ResourceStatusLike,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ReconcileResourceUseCaseHelper<'a, R>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
{
    resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
    resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
    reconciler: &'a dyn Reconciler<R>,
    time_source: &'a dyn SystemTimeSource,
}

impl<'a, R> ReconcileResourceUseCaseHelper<'a, R>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    pub fn new(
        resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
        resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
        reconciler: &'a dyn Reconciler<R>,
        time_source: &'a dyn SystemTimeSource,
    ) -> Self {
        Self {
            resource_aggregate_loader,
            resource_persistence_service,
            reconciler,
            time_source,
        }
    }

    pub async fn start_reconciliation_phase(
        &self,
        id: ResourceID,
    ) -> Result<Option<R>, ReconcileResourceUseCaseError<R>> {
        let resource = self
            .resource_aggregate_loader
            .load(&id)
            .await
            .map_err(ResourceLoadError)
            .map_err(ReconcileResourceUseCaseError::LoadFailed)?;
        if !resource.needs_reconciliation() {
            return Ok(None);
        }

        // Phase 1 commits the "started" transition before the actual reconciliation
        // work, giving the second phase a stable persisted hand-off point.
        let mut resource = resource;
        resource
            .try_mark_reconciliation_started(self.time_source.now())
            .map_err(ReconcileResourceUseCaseError::Lifecycle)?;

        self.persist_resource_state(&mut resource).await?;

        Ok(Some(resource))
    }

    pub async fn finish_reconciliation_phase(
        &self,
        mut resource: R,
    ) -> Result<(), ReconcileResourceUseCaseError<R>> {
        let now = self.time_source.now();

        // Phase 2 runs in a separate transaction after the started state was
        // committed, so concurrent changes between the two phases are expected.
        match self.reconciler.reconcile(&resource).await {
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
        self.persist_resource_state(&mut resource).await
    }

    async fn persist_resource_state(
        &self,
        resource: &mut R,
    ) -> Result<(), ReconcileResourceUseCaseError<R>> {
        self.resource_persistence_service
            .save(resource)
            .await
            .map_err(ReconcileResourceUseCaseError::from)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
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
                let helper = $crate::ReconcileResourceUseCaseHelper::<$resource>::new(
                    resource_aggregate_loader.as_ref(),
                    resource_persistence_service.as_ref(),
                    self.reconciler.as_ref(),
                    self.time_source.as_ref(),
                );

                helper.start_reconciliation_phase(id).await
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
                let resource_aggregate_loader: std::sync::Arc<
                    dyn $crate::domain::ResourceAggregateLoader<$resource>,
                > = self.catalog.get_one().unwrap();
                let helper = $crate::ReconcileResourceUseCaseHelper::<$resource>::new(
                    resource_aggregate_loader.as_ref(),
                    resource_persistence_service.as_ref(),
                    self.reconciler.as_ref(),
                    self.time_source.as_ref(),
                );

                helper.finish_reconciliation_phase(resource).await
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
