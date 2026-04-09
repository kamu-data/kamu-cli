// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use messaging_outbox::{Outbox, OutboxExt};
use serde::Serialize;
use time_source::SystemTimeSource;

use crate::domain::{
    DeclarativeResource,
    InvariantViolationOf,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ReconcilableEventSourcedResource,
    ReconcileResourceUseCaseError,
    Reconciler,
    ResourceAggregateLoader,
    ResourceDescriptorProvider,
    ResourceLifecycleMessage,
    ResourceLoadError,
    ResourcePersistenceService,
    ResourceStatusLike,
    ResourceUID,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ReconcileResourceUseCaseHelper<'a, R>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
{
    resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
    resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
    outbox: &'a dyn Outbox,
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
        id: ResourceUID,
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
        let lifecycle_message = self
            .apply_reconciliation_outcome(&mut resource, now)
            .await?;

        self.persist_resource_state(&mut resource).await?;
        self.notify_reconciliation_finished(&resource, lifecycle_message)
            .await
    }

    async fn apply_reconciliation_outcome(
        &self,
        resource: &mut R,
        now: DateTime<Utc>,
    ) -> Result<ResourceLifecycleMessage, ReconcileResourceUseCaseError<R>> {
        // Phase 2 runs in a separate transaction after the started state was
        // committed, so concurrent changes between the two phases are expected.
        match self.reconciler.reconcile(&*resource).await {
            Ok(success) => {
                resource
                    .try_mark_reconciliation_succeeded(now, resource.metadata().generation, success)
                    .map_err(ReconcileResourceUseCaseError::Lifecycle)?;

                Ok(ResourceLifecycleMessage::reconciliation_succeeded(
                    now,
                    resource
                        .make_resource_snapshot()
                        .map_err(ReconcileResourceUseCaseError::Internal)?,
                ))
            }
            Err(err) => {
                resource
                    .try_mark_reconciliation_failed(now, resource.metadata().generation, &err)
                    .map_err(ReconcileResourceUseCaseError::Lifecycle)?;

                Ok(ResourceLifecycleMessage::reconciliation_failed(
                    now,
                    resource
                        .make_resource_snapshot()
                        .map_err(ReconcileResourceUseCaseError::Internal)?,
                ))
            }
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

    async fn notify_reconciliation_finished(
        &self,
        resource: &R,
        lifecycle_message: ResourceLifecycleMessage,
    ) -> Result<(), ReconcileResourceUseCaseError<R>> {
        let snapshot = resource
            .make_resource_snapshot()
            .map_err(ReconcileResourceUseCaseError::Internal)?;
        let lifecycle_message = match lifecycle_message {
            ResourceLifecycleMessage::ReconciliationSucceeded(succeeded_message) => {
                ResourceLifecycleMessage::reconciliation_succeeded(
                    succeeded_message.event_time,
                    snapshot,
                )
            }
            ResourceLifecycleMessage::ReconciliationFailed(failed_message) => {
                ResourceLifecycleMessage::reconciliation_failed(failed_message.event_time, snapshot)
            }
            ResourceLifecycleMessage::Applied(_) => {
                unreachable!("reconcile use case must only emit reconciliation outcome messages")
            }
        };

        self.outbox
            .post_message(MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE, lifecycle_message)
            .await
            .map_err(ReconcileResourceUseCaseError::Internal)
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
                id: kamu_resources::ResourceUID,
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
                // Resume from the aggregate returned by phase 1 and record the
                // reconciler outcome in a new transaction.
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
                id: &kamu_resources::ResourceUID,
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
