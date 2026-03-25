// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::SaveError;
use serde::Serialize;

use crate::domain::{
    ApplyResourceOutcome,
    ApplyResourceParams,
    ApplyResourceResult,
    ApplyResourceUseCaseError,
    CreateResourceError,
    DeclarativeResource,
    InvariantViolationOf,
    ReconcilableEventSourcedResource,
    ReconcilableResource,
    ResourceDescriptorProvider,
    ResourceID,
    ResourceMetadataInput,
    ResourceMetadataValidationError,
    ResourceRepository,
    ResourceStatusLike,
    ResourceValidateSpec,
};
use crate::resource_snapshot_sync::{SyncResourceSnapshotError, sync_resource_snapshot};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_result<R>(resource: R, outcome: ApplyResourceOutcome) -> ApplyResourceResult<R>
where
    R: DeclarativeResource,
{
    ApplyResourceResult {
        resource_id: *resource.resource_id(),
        state: resource.into_state(),
        outcome,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_resource_and_sync_snapshot<R>(
    resource_repository: &dyn ResourceRepository,
    event_store: &R::Store,
    resource: &mut R,
) -> Result<(), ApplyResourceUseCaseError<R>>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>,
    R::Spec: Serialize,
    R::Status: Serialize + ResourceStatusLike,
{
    let snapshot = resource
        .make_resource_snapshot()
        .map_err(ApplyResourceUseCaseError::SnapshotSyncFailed)?;

    match resource_repository.create_resource(&snapshot).await {
        Ok(()) => {}
        Err(CreateResourceError::Duplicate(err)) => {
            return Err(ApplyResourceUseCaseError::Duplicate(err));
        }
        Err(CreateResourceError::Internal(err)) => {
            return Err(ApplyResourceUseCaseError::SnapshotSyncFailed(err));
        }
    }

    match resource.aggregate_mut().save(event_store).await {
        Ok(()) => {}
        Err(SaveError::ConcurrentModification(err)) => {
            return Err(ApplyResourceUseCaseError::ConcurrentModification(err));
        }
        Err(err) => {
            return Err(ApplyResourceUseCaseError::SaveFailed(err));
        }
    }

    match sync_resource_snapshot(resource_repository, resource, None).await {
        Ok(()) => Ok(()),
        Err(SyncResourceSnapshotError::ConcurrentModification(err)) => {
            Err(ApplyResourceUseCaseError::ConcurrentModification(err))
        }
        Err(SyncResourceSnapshotError::Internal(err)) => {
            Err(ApplyResourceUseCaseError::SnapshotSyncFailed(err))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn update_resource_and_sync_snapshot<R>(
    resource_repository: &dyn ResourceRepository,
    event_store: &R::Store,
    resource: &mut R,
) -> Result<(), ApplyResourceUseCaseError<R>>
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
            return Err(ApplyResourceUseCaseError::ConcurrentModification(err));
        }
        Err(err) => {
            return Err(ApplyResourceUseCaseError::SaveFailed(err));
        }
    }

    match sync_resource_snapshot(resource_repository, resource, expected_last_event_id).await {
        Ok(()) => Ok(()),
        Err(SyncResourceSnapshotError::ConcurrentModification(err)) => {
            Err(ApplyResourceUseCaseError::ConcurrentModification(err))
        }
        Err(SyncResourceSnapshotError::Internal(err)) => {
            Err(ApplyResourceUseCaseError::SnapshotSyncFailed(err))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn resolve_existing_resource_id<R>(
    resource_repository: &dyn ResourceRepository,
    resource_id: Option<ResourceID>,
    metadata: &ResourceMetadataInput,
) -> Result<Option<ResourceID>, ApplyResourceUseCaseError<R>>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
{
    match resource_id {
        Some(resource_id) => Ok(Some(resource_id)),
        None => resource_repository
            .find_resource_id_by_name(
                metadata.account.clone(),
                R::DESCRIPTOR.resource_type,
                &metadata.name,
            )
            .await
            .map_err(ApplyResourceUseCaseError::SnapshotSyncFailed),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn ensure_resource_id_matches_type<R>(
    resource_repository: &dyn ResourceRepository,
    resource_id: &ResourceID,
) -> Result<(), ApplyResourceUseCaseError<R>>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
{
    let snapshot = resource_repository
        .find_resource_snapshot_by_id(resource_id)
        .await
        .map_err(ApplyResourceUseCaseError::SnapshotSyncFailed)?
        .ok_or(ApplyResourceUseCaseError::ResourceIdNotFound(*resource_id))?;

    if snapshot.kind != R::DESCRIPTOR.resource_type
        || snapshot.api_version != R::DESCRIPTOR.api_version
    {
        return Err(ApplyResourceUseCaseError::type_mismatch(
            *resource_id,
            &R::DESCRIPTOR,
            &snapshot,
        ));
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn apply_create_resource<R>(
    resource_repository: &dyn ResourceRepository,
    event_store: &R::Store,
    params: ApplyResourceParams<R>,
    now: DateTime<Utc>,
) -> Result<ApplyResourceResult<R>, ApplyResourceUseCaseError<R>>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::Spec: Serialize + PartialEq + Clone + ResourceValidateSpec,
    R::Status: Serialize + ResourceStatusLike,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>
        + From<ResourceMetadataValidationError>
        + From<<R::Spec as ResourceValidateSpec>::ValidationError>,
{
    let resource_id = resource_repository
        .new_resource_id()
        .await
        .map_err(ApplyResourceUseCaseError::SnapshotSyncFailed)?;
    let mut resource =
        <R as ReconcilableResource>::try_create(now, resource_id, params.metadata, params.spec)
            .map_err(ApplyResourceUseCaseError::Lifecycle)?;

    create_resource_and_sync_snapshot(resource_repository, event_store, &mut resource).await?;

    Ok(make_result(resource, ApplyResourceOutcome::Created))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn apply_update_resource<R>(
    resource_repository: &dyn ResourceRepository,
    event_store: &R::Store,
    mut resource: R,
    params: ApplyResourceParams<R>,
    now: DateTime<Utc>,
) -> Result<ApplyResourceResult<R>, ApplyResourceUseCaseError<R>>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::Spec: Serialize + PartialEq + Clone + ResourceValidateSpec,
    R::Status: Serialize + ResourceStatusLike,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>
        + From<ResourceMetadataValidationError>
        + From<<R::Spec as ResourceValidateSpec>::ValidationError>,
{
    <R as ReconcilableResource>::try_update_metadata(&mut resource, now, params.metadata)
        .map_err(ApplyResourceUseCaseError::Lifecycle)?;
    <R as ReconcilableResource>::try_update_spec(&mut resource, now, params.spec)
        .map_err(ApplyResourceUseCaseError::Lifecycle)?;

    if !resource.aggregate().has_updates() {
        return Ok(make_result(resource, ApplyResourceOutcome::Updated));
    }

    update_resource_and_sync_snapshot(resource_repository, event_store, &mut resource).await?;

    Ok(make_result(resource, ApplyResourceOutcome::Updated))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! declare_apply_resource_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty,
        store = $store_trait:ident
    ) => {
        #[dill::component]
        #[dill::interface(dyn $crate::domain::ApplyResourceUseCase<$resource>)]
        pub struct $use_case {
            resource_repository: std::sync::Arc<dyn $crate::domain::ResourceRepository>,
            event_store: std::sync::Arc<dyn $crate::domain::$store_trait>,
            time_source: std::sync::Arc<dyn time_source::SystemTimeSource>,
        }

        #[async_trait::async_trait]
        impl $crate::domain::ApplyResourceUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                params: $crate::domain::ApplyResourceParams<$resource>,
            ) -> Result<
                $crate::domain::ApplyResourceResult<$resource>,
                $crate::domain::ApplyResourceUseCaseError<$resource>,
            > {
                let maybe_existing_id =
                    $crate::apply::shared::resolve_existing_resource_id::<$resource>(
                        self.resource_repository.as_ref(),
                        params.resource_id,
                        &params.metadata,
                    )
                    .await?;

                let now = self.time_source.now();

                let Some(resource_id) = maybe_existing_id else {
                    return $crate::apply::shared::apply_create_resource::<$resource>(
                        self.resource_repository.as_ref(),
                        self.event_store.as_ref(),
                        params,
                        now,
                    )
                    .await;
                };

                $crate::apply::shared::ensure_resource_id_matches_type::<$resource>(
                    self.resource_repository.as_ref(),
                    &resource_id,
                )
                .await?;

                let resource = <$resource>::load(&resource_id, self.event_store.as_ref())
                    .await
                    .map_err($crate::domain::ApplyResourceUseCaseError::LoadFailed)?;

                $crate::apply::shared::apply_update_resource::<$resource>(
                    self.resource_repository.as_ref(),
                    self.event_store.as_ref(),
                    resource,
                    params,
                    now,
                )
                .await
            }
        }
    };
}

pub(crate) use declare_apply_resource_use_case;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
