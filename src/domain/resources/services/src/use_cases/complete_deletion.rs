// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use internal_error::ErrorIntoInternal;
use messaging_outbox::{Outbox, OutboxExt};
use serde::Serialize;
use time_source::SystemTimeSource;

use crate::domain::{
    CompleteResourceDeletionUseCaseError,
    DeclarativeResource,
    EventSourcedResource,
    InvariantViolationOf,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceAggregateLoader,
    ResourceController,
    ResourceID,
    ResourceLifecycleMessage,
    ResourceLoadError,
    ResourcePersistenceService,
    ResourceSchemaProvider,
    StatusConflictOf,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CompleteResourceDeletionUseCaseHelper<'a, R>
where
    R: EventSourcedResource + ResourceSchemaProvider,
{
    resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
    resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
    outbox: &'a dyn Outbox,
    controller: &'a dyn ResourceController<R>,
    time_source: &'a dyn SystemTimeSource,
}

impl<'a, R> CompleteResourceDeletionUseCaseHelper<'a, R>
where
    R: EventSourcedResource + ResourceSchemaProvider,
    R::LifecycleError:
        InvariantViolationOf<<R as DeclarativeResource>::ResourceState> + std::fmt::Display,
    R::Spec: Serialize,
{
    pub fn new(
        resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
        resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
        outbox: &'a dyn Outbox,
        controller: &'a dyn ResourceController<R>,
        time_source: &'a dyn SystemTimeSource,
    ) -> Self {
        Self {
            resource_aggregate_loader,
            resource_persistence_service,
            outbox,
            controller,
            time_source,
        }
    }

    /// Runs within one transaction: the entities the controllers removed, the
    /// outcomes and the `Deleted` message of the completed deletions commit
    /// together.
    pub async fn execute(
        &self,
        ids: &[ResourceID],
    ) -> Result<(), CompleteResourceDeletionUseCaseError<R>> {
        let now = self.time_source.now();

        let mut changed = Vec::new();
        for mut resource in self.load_many(ids).await? {
            if !resource.needs_cleanup() {
                continue;
            }

            let status_generation = resource.headers().status_generation;
            match self.controller.cleanup(&resource).await {
                Ok(()) => resource
                    .try_complete_deletion(now)
                    .map_err(Self::lifecycle_error)?,
                Err(err) => {
                    tracing::warn!(
                        resource_id = %resource.id(),
                        error = %err,
                        "Cleanup of a resource being deleted failed"
                    );
                    resource
                        .try_mark_cleanup_failed(now, status_generation, &err)
                        .map_err(Self::lifecycle_error)?;
                }
            }
            changed.push(resource);
        }

        if changed.is_empty() {
            return Ok(());
        }

        self.resource_persistence_service
            .save_many(&mut changed)
            .await?;

        let deleted = changed
            .iter()
            .filter(|resource| resource.headers().deleted_at.is_some())
            .map(EventSourcedResource::make_resource_snapshot)
            .collect::<Result<Vec<_>, _>>()?;

        if deleted.is_empty() {
            return Ok(());
        }

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
                ResourceLifecycleMessage::deleted(now, deleted),
            )
            .await
            .map_err(CompleteResourceDeletionUseCaseError::Internal)
    }

    async fn load_many(
        &self,
        ids: &[ResourceID],
    ) -> Result<Vec<R>, CompleteResourceDeletionUseCaseError<R>> {
        self.resource_aggregate_loader
            .load_many(ids)
            .await
            .map_err(|err| CompleteResourceDeletionUseCaseError::Internal(err.int_err()))?
            .into_iter()
            .map(|result| {
                result
                    .map_err(ResourceLoadError)
                    .map_err(CompleteResourceDeletionUseCaseError::LoadFailed)
            })
            .collect()
    }

    fn lifecycle_error(err: R::LifecycleError) -> CompleteResourceDeletionUseCaseError<R> {
        match err.as_status_conflict() {
            Some(conflict) => CompleteResourceDeletionUseCaseError::StatusConflict(*conflict),
            None => CompleteResourceDeletionUseCaseError::Lifecycle(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Completes the deletions as one batch, so that one `Deleted` message covers
/// them. A concurrent modification of any resource rolls the batch back; the
/// deletions are then completed one by one, so the conflict holds back only
/// the resource it hit. `run_in_transaction` runs one batch in its own
/// transaction.
pub async fn complete_resource_deletions_in_batch<R, F, Fut>(
    ids: &[ResourceID],
    run_in_transaction: F,
) -> Result<(), CompleteResourceDeletionUseCaseError<R>>
where
    R: EventSourcedResource,
    F: Fn(Vec<ResourceID>) -> Fut,
    Fut: Future<Output = Result<(), CompleteResourceDeletionUseCaseError<R>>>,
{
    use CompleteResourceDeletionUseCaseError as E;

    match run_in_transaction(ids.to_vec()).await {
        Err(E::ConcurrentModification(err)) if ids.len() > 1 => {
            tracing::info!(
                error = %err,
                "Completing deletions one by one after a concurrent modification of the batch"
            );
        }
        result => return result,
    }

    for &id in ids {
        match run_in_transaction(vec![id]).await {
            Ok(()) => {}
            // The other writer completes the deletion or requests it again
            Err(E::ConcurrentModification(err)) => {
                tracing::info!(
                    resource_id = %id,
                    error = %err,
                    "Skipping deletion completion: resource was modified concurrently"
                );
            }
            Err(E::StatusConflict(err)) => {
                tracing::info!(
                    resource_id = %id,
                    error = %err,
                    "Skipping deletion completion: status was updated concurrently"
                );
            }
            Err(err @ (E::LoadFailed(_) | E::Internal(_) | E::Lifecycle(_))) => return Err(err),
        }
    }

    Ok(())
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
            time_source: std::sync::Arc<dyn time_source::SystemTimeSource>,
        }

        impl $use_case {
            #[::database_common_macros::transactional_method4(
                        resource_aggregate_loader:
                            std::sync::Arc<dyn kamu_resources::ResourceAggregateLoader<$resource>>,
                        resource_persistence_service:
                            std::sync::Arc<dyn kamu_resources::ResourcePersistenceService<$resource>>,
                        controller:
                            std::sync::Arc<dyn kamu_resources::ResourceController<$resource>>,
                        outbox:
                            std::sync::Arc<dyn $crate::messaging_outbox::Outbox>
                    )]
            async fn execute_in_transaction(
                &self,
                ids: Vec<kamu_resources::ResourceID>,
            ) -> Result<(), kamu_resources::CompleteResourceDeletionUseCaseError<$resource>> {
                let helper = $crate::CompleteResourceDeletionUseCaseHelper::<$resource>::new(
                    resource_aggregate_loader.as_ref(),
                    resource_persistence_service.as_ref(),
                    outbox.as_ref(),
                    controller.as_ref(),
                    self.time_source.as_ref(),
                );

                helper.execute(&ids).await
            }
        }

        #[async_trait::async_trait]
        impl kamu_resources::CompleteResourceDeletionUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                ids: &[kamu_resources::ResourceID],
            ) -> Result<(), kamu_resources::CompleteResourceDeletionUseCaseError<$resource>> {
                $crate::complete_resource_deletions_in_batch(ids, |ids| {
                    self.execute_in_transaction(ids)
                })
                .await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
