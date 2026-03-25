// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::SaveError;
use internal_error::{ErrorIntoInternal, InternalError};
use serde::Serialize;
use time_source::SystemTimeSource;

use crate::domain::{
    DeclarativeResource,
    DeleteResourcesError,
    InvariantViolationOf,
    ReconcilableEventSourcedResource,
    ResourceDescriptorProvider,
    ResourceID,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceStatusLike,
};
use crate::resource_snapshot_sync::{SyncResourceSnapshotError, sync_resource_snapshot};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn delete_and_sync_resource<R>(
    resource_repository: &dyn ResourceRepository,
    event_store: &R::Store,
    time_source: &dyn SystemTimeSource,
    mut resource: R,
) -> Result<(), DeleteResourcesError>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::LifecycleError:
        InvariantViolationOf<<R as DeclarativeResource>::ResourceState> + std::fmt::Display,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    let resource_id = *resource.resource_id();
    let now = time_source.now();
    let tombstone_name = format!("deleted-{resource_id}");
    let expected_last_event_id = resource.aggregate().last_stored_event_id();

    resource.try_delete(now, tombstone_name).map_err(|err| {
        DeleteResourcesError::Internal(InternalError::new(format!(
            "Failed to delete resource {resource_id}: {err}"
        )))
    })?;

    resource
        .aggregate_mut()
        .save(event_store)
        .await
        .map_err(|err| match err {
            SaveError::ConcurrentModification(err) => {
                DeleteResourcesError::ConcurrentModification(err)
            }
            err => DeleteResourcesError::Internal(
                err.int_err()
                    .with_context(format!("Failed to save resource {resource_id}")),
            ),
        })?;

    match sync_resource_snapshot(resource_repository, &resource, expected_last_event_id).await {
        Ok(()) => Ok(()),
        Err(SyncResourceSnapshotError::ConcurrentModification(err)) => {
            Err(DeleteResourcesError::ConcurrentModification(err))
        }
        Err(SyncResourceSnapshotError::Internal(err)) => {
            Err(DeleteResourcesError::Internal(err.with_context(format!(
                "Failed to synchronize resource snapshot for resource {resource_id}"
            ))))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn find_owned_resource_snapshot<R>(
    resource_repository: &dyn ResourceRepository,
    account_id: &odf::AccountID,
    resource_id: ResourceID,
) -> Result<Option<ResourceSnapshot>, DeleteResourcesError>
where
    R: ResourceDescriptorProvider,
{
    let query = ResourceRawEventQuery {
        kind: R::DESCRIPTOR.resource_type.to_string(),
        id: resource_id,
    };

    let Some(resource_snapshot) = resource_repository.find_resource_snapshot(&query).await? else {
        return Ok(None);
    };

    if resource_snapshot.metadata.account != *account_id {
        return Err(DeleteResourcesError::not_enough_permissions(
            resource_snapshot.resource_id,
            R::DESCRIPTOR.resource_type,
        ));
    }

    Ok(Some(resource_snapshot))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! declare_delete_resources_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty,
        store = $store_trait:ident
    ) => {
        #[dill::component]
        #[dill::interface(dyn $crate::domain::DeleteResourcesUseCase<$resource>)]
        pub struct $use_case {
            resource_repository: std::sync::Arc<dyn $crate::domain::ResourceRepository>,
            event_store: std::sync::Arc<dyn $crate::domain::$store_trait>,
            time_source: std::sync::Arc<dyn time_source::SystemTimeSource>,
        }

        impl $use_case {
            async fn find_owned_resource_snapshot(
                &self,
                account_id: &odf::AccountID,
                resource_id: $crate::domain::ResourceID,
            ) -> Result<
                Option<$crate::domain::ResourceSnapshot>,
                $crate::domain::DeleteResourcesError,
            > {
                $crate::delete::shared::find_owned_resource_snapshot::<$resource>(
                    self.resource_repository.as_ref(),
                    account_id,
                    resource_id,
                )
                .await
            }

            async fn delete_resource(
                &self,
                resource_id: &$crate::domain::ResourceID,
            ) -> Result<(), $crate::domain::DeleteResourcesError> {
                // Load the concrete aggregate, then run the shared delete-and-
                // sync path over the already typed resource instance.
                let resource = <$resource>::load(resource_id, self.event_store.as_ref())
                    .await
                    .map_err(internal_error::ErrorIntoInternal::int_err)
                    .map_err($crate::domain::DeleteResourcesError::Internal)?;

                $crate::delete::shared::delete_and_sync_resource::<$resource>(
                    self.resource_repository.as_ref(),
                    self.event_store.as_ref(),
                    self.time_source.as_ref(),
                    resource,
                )
                .await
            }
        }

        #[async_trait::async_trait]
        impl $crate::domain::DeleteResourcesUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                account_id: odf::AccountID,
                resource_ids: Vec<$crate::domain::ResourceID>,
            ) -> Result<(), $crate::domain::DeleteResourcesError> {
                // This use case stays in one transaction boundary, but the
                // ownership check and the aggregate mutation are still split so
                // the control flow is readable.
                for resource_id in resource_ids {
                    let Some(_resource_snapshot) = self
                        .find_owned_resource_snapshot(&account_id, resource_id.clone())
                        .await?
                    else {
                        continue;
                    };

                    self.delete_resource(&resource_id).await?;
                }

                Ok(())
            }
        }
    };
}

pub(crate) use declare_delete_resources_use_case;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
