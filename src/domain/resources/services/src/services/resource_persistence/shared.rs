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

async fn sync_snapshot<R>(
    resource_repository: &dyn ResourceRepository,
    resource: &R,
    expected_last_event_id: Option<EventID>,
) -> Result<(), ResourcePersistenceError>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    let snapshot = resource.make_resource_snapshot()?;

    match resource_repository
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn create_resource<R>(
    resource_repository: &dyn ResourceRepository,
    event_store: &R::Store,
    resource: &mut R,
) -> Result<(), ResourcePersistenceError>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    let snapshot = resource.make_resource_snapshot()?;

    match resource_repository.create_resource(&snapshot).await {
        Ok(()) => {}
        Err(CreateResourceError::Duplicate(err)) => {
            return Err(ResourcePersistenceError::Duplicate(err));
        }
        Err(CreateResourceError::Internal(err)) => {
            return Err(ResourcePersistenceError::Internal(err));
        }
    }

    match resource.aggregate_mut().save(event_store).await {
        Ok(()) => {}
        Err(SaveError::ConcurrentModification(err)) => {
            return Err(ResourcePersistenceError::ConcurrentModification(err));
        }
        Err(err) => {
            return Err(ResourcePersistenceError::Internal(err.int_err()));
        }
    }

    sync_snapshot(resource_repository, resource, None).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn save_resource<R>(
    resource_repository: &dyn ResourceRepository,
    event_store: &R::Store,
    resource: &mut R,
) -> Result<(), ResourcePersistenceError>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    let expected_last_event_id = resource.aggregate().last_stored_event_id();

    match resource.aggregate_mut().save(event_store).await {
        Ok(()) => {}
        Err(SaveError::ConcurrentModification(err)) => {
            return Err(ResourcePersistenceError::ConcurrentModification(err));
        }
        Err(err) => {
            return Err(ResourcePersistenceError::Internal(err.int_err()));
        }
    }

    sync_snapshot(resource_repository, resource, expected_last_event_id).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn delete_resource<R>(
    resource_repository: &dyn ResourceRepository,
    event_store: &R::Store,
    resource: &mut R,
    now: DateTime<Utc>,
) -> Result<(), ResourcePersistenceError>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::LifecycleError:
        InvariantViolationOf<<R as DeclarativeResource>::ResourceState> + std::fmt::Display,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    let tombstone_name = format!("deleted-{}", resource.resource_id());

    resource.try_delete(now, tombstone_name).map_err(|err| {
        ResourcePersistenceError::Internal(format!("{err}").int_err().with_context(format!(
            "Failed to delete resource {}",
            resource.resource_id()
        )))
    })?;

    match save_resource(resource_repository, event_store, resource).await {
        Ok(()) => Ok(()),
        Err(ResourcePersistenceError::Duplicate(err)) => Err(ResourcePersistenceError::Internal(
            format!("{err}").int_err().with_context(format!(
                "Unexpected duplicate resource state while deleting resource {}",
                resource.resource_id()
            )),
        )),
        Err(err) => Err(err),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
                super::shared::create_resource(
                    self.resource_repository.as_ref(),
                    self.event_store.as_ref(),
                    resource,
                )
                .await
            }

            async fn save(
                &self,
                resource: &mut $resource,
            ) -> Result<(), $crate::domain::ResourcePersistenceError> {
                super::shared::save_resource(
                    self.resource_repository.as_ref(),
                    self.event_store.as_ref(),
                    resource,
                )
                .await
            }

            async fn delete(
                &self,
                resource: &mut $resource,
                now: chrono::DateTime<chrono::Utc>,
            ) -> Result<(), $crate::domain::ResourcePersistenceError> {
                super::shared::delete_resource(
                    self.resource_repository.as_ref(),
                    self.event_store.as_ref(),
                    resource,
                    now,
                )
                .await
            }
        }
    };
}

pub(crate) use declare_resource_persistence_service;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
