// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::ErrorIntoInternal;
use serde::Serialize;
use time_source::SystemTimeSource;

use crate::domain::{
    DeclarativeResource,
    DeleteResourcesError,
    InvariantViolationOf,
    ReconcilableEventSourcedResource,
    ResourceAggregateLoader,
    ResourceDescriptorProvider,
    ResourcePersistenceError,
    ResourcePersistenceService,
    ResourceQueryService,
    ResourceSnapshot,
    ResourceStatusLike,
    ResourceUID,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DeleteResourcesUseCaseHelper<'a, R>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
{
    resource_query_service: &'a dyn ResourceQueryService<R>,
    resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
    resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
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
        resource_query_service: &'a dyn ResourceQueryService<R>,
        resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
        resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
        time_source: &'a dyn SystemTimeSource,
    ) -> Self {
        Self {
            resource_query_service,
            resource_aggregate_loader,
            resource_persistence_service,
            time_source,
        }
    }

    pub async fn execute(
        &self,
        account_id: odf::AccountID,
        uids: Vec<ResourceUID>,
    ) -> Result<(), DeleteResourcesError> {
        for uid in uids {
            let Some(_resource_snapshot) =
                self.find_owned_resource_snapshot(&account_id, uid).await?
            else {
                continue;
            };

            self.delete_resource(&uid).await?;
        }

        Ok(())
    }

    async fn delete_and_sync_resource(&self, mut resource: R) -> Result<(), DeleteResourcesError> {
        let uid = *resource.uid();
        match self
            .resource_persistence_service
            .delete(&mut resource, self.time_source.now())
            .await
        {
            Ok(()) => Ok(()),
            Err(ResourcePersistenceError::Duplicate(_)) => {
                unreachable!("delete() must not expose duplicate persistence errors")
            }
            Err(ResourcePersistenceError::ConcurrentModification(err)) => {
                Err(DeleteResourcesError::ConcurrentModification(err))
            }
            Err(ResourcePersistenceError::Internal(err)) => Err(DeleteResourcesError::Internal(
                err.with_context(format!("Failed to persist deleted resource {uid}")),
            )),
        }
    }

    async fn find_owned_resource_snapshot(
        &self,
        account_id: &odf::AccountID,
        uid: ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, DeleteResourcesError> {
        self.resource_query_service
            .find_owned_snapshot(account_id, uid)
            .await
            .map_err(DeleteResourcesError::from)
    }

    async fn delete_resource(&self, uid: &ResourceUID) -> Result<(), DeleteResourcesError> {
        let resource = self
            .resource_aggregate_loader
            .load(uid)
            .await
            .map_err(|err| {
                DeleteResourcesError::Internal(
                    format!("{err}")
                        .int_err()
                        .with_context(format!("Failed to load resource {uid}")),
                )
            })?;

        self.delete_and_sync_resource(resource).await
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
            resource_query_service:
                std::sync::Arc<dyn kamu_resources::ResourceQueryService<$resource>>,
            resource_aggregate_loader:
                std::sync::Arc<dyn kamu_resources::ResourceAggregateLoader<$resource>>,
            resource_persistence_service:
                std::sync::Arc<dyn kamu_resources::ResourcePersistenceService<$resource>>,
            time_source: std::sync::Arc<dyn time_source::SystemTimeSource>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::DeleteResourcesUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                account_id: odf::AccountID,
                uids: Vec<kamu_resources::ResourceUID>,
            ) -> Result<(), kamu_resources::DeleteResourcesError> {
                let helper = $crate::DeleteResourcesUseCaseHelper::<$resource>::new(
                    self.resource_query_service.as_ref(),
                    self.resource_aggregate_loader.as_ref(),
                    self.resource_persistence_service.as_ref(),
                    self.time_source.as_ref(),
                );

                helper.execute(account_id, uids).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
