// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use internal_error::ErrorIntoInternal;
use kamu_resources::{MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE, ResourceLifecycleMessage};
use messaging_outbox::{Outbox, OutboxExt};
use serde::Serialize;
use time_source::SystemTimeSource;

use crate::domain::{
    DeclarativeResource,
    DeleteResourcesError,
    GenericResourceQueryService,
    InvariantViolationOf,
    ReconcilableEventSourcedResource,
    ResourceAggregateLoader,
    ResourceDescriptorProvider,
    ResourceID,
    ResourceNotOwnedByAccountError,
    ResourcePersistenceError,
    ResourcePersistenceService,
    ResourceStatusLike,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DeleteResourcesUseCaseHelper<'a, R>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
{
    generic_resource_query_service: &'a dyn GenericResourceQueryService,
    resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
    resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
    outbox: &'a dyn Outbox,
    time_source: &'a dyn SystemTimeSource,
}

impl<'a, R> DeleteResourcesUseCaseHelper<'a, R>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::LifecycleError:
        InvariantViolationOf<<R as DeclarativeResource>::ResourceState> + std::fmt::Display,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    pub fn new(
        generic_resource_query_service: &'a dyn GenericResourceQueryService,
        resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
        resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
        outbox: &'a dyn Outbox,
        time_source: &'a dyn SystemTimeSource,
    ) -> Self {
        Self {
            generic_resource_query_service,
            resource_aggregate_loader,
            resource_persistence_service,
            outbox,
            time_source,
        }
    }

    pub async fn execute(
        &self,
        account_id: odf::AccountID,
        ids: Vec<ResourceID>,
    ) -> Result<(), DeleteResourcesError> {
        let now = self.time_source.now();

        let unique_ids = ids
            .into_iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let outcome = self
            .generic_resource_query_service
            .find_owned_snapshots(
                &account_id,
                R::DESCRIPTOR.resource_type,
                R::DESCRIPTOR.api_version,
                &unique_ids,
            )
            .await
            .map_err(DeleteResourcesError::Internal)?;

        if let Some(&id) = outcome.access_denied.first() {
            return Err(DeleteResourcesError::Access(
                odf::AccessError::Unauthorized(
                    ResourceNotOwnedByAccountError {
                        id,
                        resource_type: R::DESCRIPTOR.resource_type,
                    }
                    .into(),
                ),
            ));
        }

        let owned_resource_ids = outcome
            .found
            .into_iter()
            .map(|snapshot| snapshot.id)
            .collect::<Vec<_>>();

        let mut resources = self.load_resources(&owned_resource_ids).await?;

        match self
            .resource_persistence_service
            .delete_many(&mut resources, now)
            .await
        {
            Ok(()) => Ok(()),
            Err(ResourcePersistenceError::Duplicate(_)) => {
                unreachable!("delete_many() must not expose duplicate persistence errors")
            }
            Err(ResourcePersistenceError::ConcurrentModification(err)) => {
                Err(DeleteResourcesError::ConcurrentModification(err))
            }
            Err(ResourcePersistenceError::Internal(err)) => Err(DeleteResourcesError::Internal(
                err.with_context("Failed to persist deleted resources"),
            )),
        }?;

        if !resources.is_empty() {
            self.notify_resources_deleted(now, resources).await?;
        }

        Ok(())
    }

    async fn load_resources(&self, ids: &[ResourceID]) -> Result<Vec<R>, DeleteResourcesError> {
        self.resource_aggregate_loader
            .load_many(ids)
            .await
            .map_err(|err| {
                DeleteResourcesError::Internal(
                    format!("{err}")
                        .int_err()
                        .with_context("Failed to load resources"),
                )
            })?
            .into_iter()
            .zip(ids)
            .map(|(resource_result, id)| {
                resource_result.map_err(|err| {
                    DeleteResourcesError::Internal(
                        format!("{err}")
                            .int_err()
                            .with_context(format!("Failed to load resource {id}")),
                    )
                })
            })
            .collect()
    }

    async fn notify_resources_deleted(
        &self,
        now: DateTime<Utc>,
        resources: Vec<R>,
    ) -> Result<(), DeleteResourcesError> {
        let resource_snashots = resources
            .into_iter()
            .map(|resource| {
                resource
                    .make_resource_snapshot()
                    .map_err(DeleteResourcesError::Internal)
            })
            .collect::<Result<Vec<_>, _>>()?;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
                ResourceLifecycleMessage::deleted(now, resource_snashots),
            )
            .await
            .map_err(DeleteResourcesError::Internal)?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! declare_delete_resources_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::DeleteResourcesUseCase<$resource>)]
        pub struct $use_case {
            generic_resource_query_service:
                std::sync::Arc<dyn kamu_resources::GenericResourceQueryService>,
            resource_aggregate_loader:
                std::sync::Arc<dyn kamu_resources::ResourceAggregateLoader<$resource>>,
            resource_persistence_service:
                std::sync::Arc<dyn kamu_resources::ResourcePersistenceService<$resource>>,
            outbox: std::sync::Arc<dyn messaging_outbox::Outbox>,
            time_source: std::sync::Arc<dyn time_source::SystemTimeSource>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::DeleteResourcesUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                account_id: odf::AccountID,
                ids: Vec<kamu_resources::ResourceID>,
            ) -> Result<(), kamu_resources::DeleteResourcesError> {
                let helper = $crate::DeleteResourcesUseCaseHelper::<$resource>::new(
                    self.generic_resource_query_service.as_ref(),
                    self.resource_aggregate_loader.as_ref(),
                    self.resource_persistence_service.as_ref(),
                    self.outbox.as_ref(),
                    self.time_source.as_ref(),
                );

                helper.execute(account_id, ids).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
