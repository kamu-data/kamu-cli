// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::{EventID, SaveError};
use internal_error::ErrorIntoInternal;
use serde::Serialize;

use crate::domain::{
    CreateResourceError,
    DeclarativeResource,
    InvariantViolationOf,
    ReconcilableEventSourcedResource,
    ResourceDescriptorProvider,
    ResourcePersistenceError,
    ResourceRepository,
    ResourceStatusLike,
    UpdateResourceError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourcePersistenceServiceHelper<'a, R>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
{
    resource_repository: &'a dyn ResourceRepository,
    event_store: &'a R::Store,
}

impl<'a, R> ResourcePersistenceServiceHelper<'a, R>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    pub fn new(resource_repository: &'a dyn ResourceRepository, event_store: &'a R::Store) -> Self {
        Self {
            resource_repository,
            event_store,
        }
    }

    pub async fn create(&self, resource: &mut R) -> Result<(), ResourcePersistenceError> {
        let snapshot = resource.make_resource_snapshot()?;

        match self.resource_repository.create_resource(&snapshot).await {
            Ok(()) => {}
            Err(CreateResourceError::Duplicate(err)) => {
                return Err(ResourcePersistenceError::Duplicate(err));
            }
            Err(CreateResourceError::Internal(err)) => {
                return Err(ResourcePersistenceError::Internal(err));
            }
        }

        match resource.aggregate_mut().save(self.event_store).await {
            Ok(()) => {}
            Err(SaveError::ConcurrentModification(err)) => {
                return Err(ResourcePersistenceError::ConcurrentModification(err));
            }
            Err(err) => {
                return Err(ResourcePersistenceError::Internal(err.int_err()));
            }
        }

        self.sync_snapshot(resource, None).await
    }

    pub async fn save(&self, resource: &mut R) -> Result<(), ResourcePersistenceError> {
        let expected_last_event_id = resource.aggregate().last_stored_event_id();

        match resource.aggregate_mut().save(self.event_store).await {
            Ok(()) => {}
            Err(SaveError::ConcurrentModification(err)) => {
                return Err(ResourcePersistenceError::ConcurrentModification(err));
            }
            Err(err) => {
                return Err(ResourcePersistenceError::Internal(err.int_err()));
            }
        }

        self.sync_snapshot(resource, expected_last_event_id).await
    }
}

impl<R> ResourcePersistenceServiceHelper<'_, R>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::LifecycleError:
        InvariantViolationOf<<R as DeclarativeResource>::ResourceState> + std::fmt::Display,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    pub async fn delete(
        &self,
        resource: &mut R,
        now: DateTime<Utc>,
    ) -> Result<(), ResourcePersistenceError> {
        let tombstone_name = format!("deleted-{}", resource.resource_id());

        resource.try_delete(now, tombstone_name).map_err(|err| {
            ResourcePersistenceError::Internal(format!("{err}").int_err().with_context(format!(
                "Failed to delete resource {}",
                resource.resource_id()
            )))
        })?;

        match self.save(resource).await {
            Ok(()) => Ok(()),
            Err(ResourcePersistenceError::Duplicate(err)) => {
                Err(ResourcePersistenceError::Internal(
                    format!("{err}").int_err().with_context(format!(
                        "Unexpected duplicate resource state while deleting resource {}",
                        resource.resource_id()
                    )),
                ))
            }
            Err(err) => Err(err),
        }
    }
}

impl<R> ResourcePersistenceServiceHelper<'_, R>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    async fn sync_snapshot(
        &self,
        resource: &R,
        expected_last_event_id: Option<EventID>,
    ) -> Result<(), ResourcePersistenceError> {
        let snapshot = resource.make_resource_snapshot()?;

        match self
            .resource_repository
            .update_resource(&snapshot, expected_last_event_id)
            .await
        {
            Ok(()) => Ok(()),
            Err(UpdateResourceError::ConcurrentModification(err)) => {
                Err(ResourcePersistenceError::ConcurrentModification(err))
            }
            Err(err) => Err(ResourcePersistenceError::Internal(err.int_err())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! declare_resource_persistence_service {
    (
        service = $service:ident,
        resource = $resource:ty,
        store = $store_trait:ident
    ) => {
        #[dill::component]
        #[dill::interface(dyn $crate::domain::ResourcePersistenceService<$resource>)]
        pub struct $service {
            resource_repository: std::sync::Arc<dyn $crate::domain::ResourceRepository>,
            event_store: std::sync::Arc<dyn $crate::domain::$store_trait>,
        }

        #[async_trait::async_trait]
        impl $crate::domain::ResourcePersistenceService<$resource> for $service {
            async fn create(
                &self,
                resource: &mut $resource,
            ) -> Result<(), $crate::domain::ResourcePersistenceError> {
                let helper = $crate::ResourcePersistenceServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                    self.event_store.as_ref(),
                );

                helper.create(resource).await
            }

            async fn save(
                &self,
                resource: &mut $resource,
            ) -> Result<(), $crate::domain::ResourcePersistenceError> {
                let helper = $crate::ResourcePersistenceServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                    self.event_store.as_ref(),
                );

                helper.save(resource).await
            }

            async fn delete(
                &self,
                resource: &mut $resource,
                now: chrono::DateTime<chrono::Utc>,
            ) -> Result<(), $crate::domain::ResourcePersistenceError> {
                let helper = $crate::ResourcePersistenceServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                    self.event_store.as_ref(),
                );

                helper.delete(resource, now).await
            }
        }
    };
}

pub use declare_resource_persistence_service;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
