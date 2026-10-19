// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use messaging_outbox::{Outbox, OutboxExt};
use serde::Serialize;
use time_source::SystemTimeSource;

use crate::domain::{
    DeclarativeResource,
    EventSourcedResource,
    InvariantViolationOf,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ReconcileResourceUseCaseError,
    Reconciler,
    ResourceAggregateLoader,
    ResourceID,
    ResourceLifecycleMessage,
    ResourceLoadError,
    ResourcePersistenceService,
    ResourceSchemaProvider,
    StatusConflictOf,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ReconcileResourceUseCaseHelper<'a, R>
where
    R: EventSourcedResource + ResourceSchemaProvider,
{
    resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
    resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
    outbox: &'a dyn Outbox,
    reconciler: &'a dyn Reconciler<R>,
    time_source: &'a dyn SystemTimeSource,
}

impl<'a, R> ReconcileResourceUseCaseHelper<'a, R>
where
    R: EventSourcedResource + ResourceSchemaProvider,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>,
    R::Spec: Serialize,
{
    pub fn new(
        resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
        resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
        outbox: &'a dyn Outbox,
        reconciler: &'a dyn Reconciler<R>,
        time_source: &'a dyn SystemTimeSource,
    ) -> Self {
        Self {
            resource_aggregate_loader,
            resource_persistence_service,
            outbox,
            reconciler,
            time_source,
        }
    }

    pub async fn start_reconciliation_phase(
        &self,
        id: ResourceID,
    ) -> Result<Option<R>, ReconcileResourceUseCaseError<R>> {
        let mut resource = self.load(&id).await?;
        if !resource.needs_reconciliation() {
            return Ok(None);
        }

        // Phase 1 commits the "started" transition before the actual reconciliation
        // work, giving the second phase a stable persisted hand-off point.
        let status_generation = resource.headers().status_generation;
        resource
            .try_mark_reconciliation_started(self.time_source.now(), status_generation)
            .map_err(Self::lifecycle_error)?;

        self.persist_resource_state(&mut resource).await?;

        Ok(Some(resource))
    }

    /// Records the outcome of reconciling `started`, the resource as phase 1
    /// left it. The outcome is written to the current state of the resource,
    /// provided its generation is still the reconciled one and no other writer
    /// updated the status since phase 1.
    pub async fn finish_reconciliation_phase(
        &self,
        started: R,
    ) -> Result<(), ReconcileResourceUseCaseError<R>> {
        let generation = started.headers().generation;
        let status_generation = started.headers().status_generation;

        let outcome = self.reconciler.reconcile(&started).await;

        // Phase 2 runs in a separate transaction after the started state was
        // committed, so the resource may have changed in between.
        let mut resource = self.load(started.id()).await?;
        if resource.headers().generation != generation {
            tracing::info!(
                resource_id = %resource.id(),
                reconciled_generation = generation,
                current_generation = resource.headers().generation,
                "Discarding reconciliation outcome of a superseded generation"
            );
            return Ok(());
        }

        let now = self.time_source.now();
        let lifecycle_message = match outcome {
            Ok(()) => {
                resource
                    .try_mark_reconciliation_succeeded(now, generation, status_generation)
                    .map_err(Self::lifecycle_error)?;
                self.persist_resource_state(&mut resource).await?;

                ResourceLifecycleMessage::reconciliation_succeeded(
                    now,
                    resource
                        .make_resource_snapshot()
                        .map_err(ReconcileResourceUseCaseError::Internal)?,
                )
            }
            Err(err) => {
                resource
                    .try_mark_reconciliation_failed(now, generation, status_generation, &err)
                    .map_err(Self::lifecycle_error)?;
                self.persist_resource_state(&mut resource).await?;

                ResourceLifecycleMessage::reconciliation_failed(
                    now,
                    resource
                        .make_resource_snapshot()
                        .map_err(ReconcileResourceUseCaseError::Internal)?,
                )
            }
        };

        self.outbox
            .post_message(MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE, lifecycle_message)
            .await
            .map_err(ReconcileResourceUseCaseError::Internal)
    }

    async fn load(&self, id: &ResourceID) -> Result<R, ReconcileResourceUseCaseError<R>> {
        self.resource_aggregate_loader
            .load(id)
            .await
            .map_err(ResourceLoadError)
            .map_err(ReconcileResourceUseCaseError::LoadFailed)
    }

    fn lifecycle_error(err: R::LifecycleError) -> ReconcileResourceUseCaseError<R> {
        match err.as_status_conflict() {
            Some(conflict) => ReconcileResourceUseCaseError::StatusConflict(*conflict),
            None => ReconcileResourceUseCaseError::Lifecycle(err),
        }
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
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ReconcileResourceUseCase<$resource>)]
        pub struct $use_case {
            catalog: dill::Catalog,
            time_source: std::sync::Arc<dyn time_source::SystemTimeSource>,
        }

        impl $use_case {
            #[::database_common_macros::transactional_method4(
                        resource_aggregate_loader:
                            std::sync::Arc<dyn kamu_resources::ResourceAggregateLoader<$resource>>,
                        resource_persistence_service:
                            std::sync::Arc<dyn kamu_resources::ResourcePersistenceService<$resource>>,
                        reconciler:
                            std::sync::Arc<dyn kamu_resources::Reconciler<$resource>>,
                        outbox:
                            std::sync::Arc<dyn $crate::messaging_outbox::Outbox>
                    )]
            async fn start_reconciliation_phase(
                &self,
                id: kamu_resources::ResourceID,
            ) -> Result<Option<$resource>, kamu_resources::ReconcileResourceUseCaseError<$resource>>
            {
                let helper = $crate::ReconcileResourceUseCaseHelper::<$resource>::new(
                    resource_aggregate_loader.as_ref(),
                    resource_persistence_service.as_ref(),
                    outbox.as_ref(),
                    reconciler.as_ref(),
                    self.time_source.as_ref(),
                );

                helper.start_reconciliation_phase(id).await
            }

            #[::database_common_macros::transactional_method4(
                        resource_aggregate_loader:
                            std::sync::Arc<dyn kamu_resources::ResourceAggregateLoader<$resource>>,
                        resource_persistence_service:
                            std::sync::Arc<dyn kamu_resources::ResourcePersistenceService<$resource>>,
                        reconciler:
                            std::sync::Arc<dyn kamu_resources::Reconciler<$resource>>,
                        outbox:
                            std::sync::Arc<dyn $crate::messaging_outbox::Outbox>
                    )]
            async fn finish_reconciliation_phase(
                &self,
                resource: $resource,
            ) -> Result<(), kamu_resources::ReconcileResourceUseCaseError<$resource>> {
                // Reconcile the aggregate returned by phase 1 and record the
                // outcome in a new transaction.
                let helper = $crate::ReconcileResourceUseCaseHelper::<$resource>::new(
                    resource_aggregate_loader.as_ref(),
                    resource_persistence_service.as_ref(),
                    outbox.as_ref(),
                    reconciler.as_ref(),
                    self.time_source.as_ref(),
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
