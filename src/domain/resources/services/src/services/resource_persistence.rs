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
    ResourceSnapshotUpdate,
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
        self.delete_many(std::slice::from_mut(resource), now).await
    }

    pub async fn delete_many(
        &self,
        resources: &mut [R],
        now: DateTime<Utc>,
    ) -> Result<(), ResourcePersistenceError> {
        for resource in resources.iter_mut() {
            Self::mark_deleted(resource, now)?;
        }

        let expected_last_event_ids = resources
            .iter()
            .map(|resource| resource.aggregate().last_stored_event_id())
            .collect::<Vec<_>>();

        // TODO: find a way to bulk it
        for resource in resources.iter_mut() {
            match resource.aggregate_mut().save(self.event_store).await {
                Ok(()) => {}
                Err(SaveError::ConcurrentModification(err)) => {
                    return Err(ResourcePersistenceError::ConcurrentModification(err));
                }
                Err(err) => {
                    return Err(ResourcePersistenceError::Internal(err.int_err()));
                }
            }
        }

        self.sync_snapshots(resources, expected_last_event_ids)
            .await
    }

    fn mark_deleted(resource: &mut R, now: DateTime<Utc>) -> Result<(), ResourcePersistenceError> {
        let tombstone_name = format!("deleted-{}", resource.uid());

        resource.try_delete(now, tombstone_name).map_err(|err| {
            ResourcePersistenceError::Internal(
                format!("{err}")
                    .int_err()
                    .with_context(format!("Failed to delete resource {}", resource.uid())),
            )
        })
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
        self.sync_snapshots(std::slice::from_ref(resource), vec![expected_last_event_id])
            .await
    }

    async fn sync_snapshots(
        &self,
        resources: &[R],
        expected_last_event_ids: Vec<Option<EventID>>,
    ) -> Result<(), ResourcePersistenceError> {
        let updates = resources
            .iter()
            .zip(expected_last_event_ids)
            .map(|(resource, expected_last_event_id)| {
                Ok(ResourceSnapshotUpdate {
                    snapshot: resource.make_resource_snapshot()?,
                    expected_last_event_id,
                })
            })
            .collect::<Result<Vec<_>, ResourcePersistenceError>>()?;

        match self.resource_repository.update_resources(&updates).await {
            Ok(()) => Ok(()),
            Err(UpdateResourceError::ConcurrentModification(err)) => {
                Err(ResourcePersistenceError::ConcurrentModification(err))
            }
            Err(UpdateResourceError::Duplicate(err)) => Err(ResourcePersistenceError::Internal(
                format!("{err}")
                    .int_err()
                    .with_context("Unexpected duplicate resource state while syncing resources"),
            )),
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
        store = $store:path
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ResourcePersistenceService<$resource>)]
        pub struct $service {
            resource_repository: std::sync::Arc<dyn kamu_resources::ResourceRepository>,
            event_store: std::sync::Arc<dyn $store>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::ResourcePersistenceService<$resource> for $service {
            async fn create(
                &self,
                resource: &mut $resource,
            ) -> Result<(), kamu_resources::ResourcePersistenceError> {
                let helper = $crate::ResourcePersistenceServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                    self.event_store.as_ref(),
                );

                helper.create(resource).await
            }

            async fn save(
                &self,
                resource: &mut $resource,
            ) -> Result<(), kamu_resources::ResourcePersistenceError> {
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
            ) -> Result<(), kamu_resources::ResourcePersistenceError> {
                let helper = $crate::ResourcePersistenceServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                    self.event_store.as_ref(),
                );

                helper.delete(resource, now).await
            }

            async fn delete_many(
                &self,
                resources: &mut [$resource],
                now: chrono::DateTime<chrono::Utc>,
            ) -> Result<(), kamu_resources::ResourcePersistenceError> {
                let helper = $crate::ResourcePersistenceServiceHelper::<$resource>::new(
                    self.resource_repository.as_ref(),
                    self.event_store.as_ref(),
                );

                helper.delete_many(resources, now).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
