// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::domain::{
    ApplyResourceOutcome,
    ApplyResourceParams,
    ApplyResourceResult,
    ApplyResourceUseCaseError,
    DeclarativeResource,
    InvariantViolationOf,
    ReconcilableEventSourcedResource,
    ReconcilableResource,
    ResourceAggregateLoader,
    ResourceDescriptorProvider,
    ResourceID,
    ResourceLoadError,
    ResourceMetadataInput,
    ResourceMetadataValidationError,
    ResourcePersistenceService,
    ResourceQueryService,
    ResourceStatusLike,
    ResourceValidateSpec,
};

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

pub(crate) async fn resolve_existing_resource_id<R>(
    resource_query_service: &dyn ResourceQueryService<R>,
    resource_id: Option<ResourceID>,
    metadata: &ResourceMetadataInput,
) -> Result<Option<ResourceID>, ApplyResourceUseCaseError<R>>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
{
    resource_query_service
        .find_existing_id_by_name(resource_id, metadata)
        .await
        .map_err(ApplyResourceUseCaseError::Internal)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn ensure_resource_id_matches_type<R>(
    resource_query_service: &dyn ResourceQueryService<R>,
    resource_id: &ResourceID,
) -> Result<(), ApplyResourceUseCaseError<R>>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
{
    resource_query_service
        .ensure_resource_id_matches_type(resource_id)
        .await
        .map_err(ApplyResourceUseCaseError::from)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn apply_create_resource<R>(
    resource_query_service: &dyn ResourceQueryService<R>,
    resource_persistence_service: &dyn ResourcePersistenceService<R>,
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
    let resource_id = resource_query_service
        .allocate_id()
        .await
        .map_err(ApplyResourceUseCaseError::Internal)?;
    let mut resource =
        <R as ReconcilableResource>::try_create(now, resource_id, params.metadata, params.spec)
            .map_err(ApplyResourceUseCaseError::Lifecycle)?;

    resource_persistence_service
        .create(&mut resource)
        .await
        .map_err(ApplyResourceUseCaseError::from)?;

    Ok(make_result(resource, ApplyResourceOutcome::Created))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn apply_update_resource<R>(
    resource_persistence_service: &dyn ResourcePersistenceService<R>,
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

    resource_persistence_service
        .save(&mut resource)
        .await
        .map_err(ApplyResourceUseCaseError::from)?;

    Ok(make_result(resource, ApplyResourceOutcome::Updated))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn execute_apply_resource<R>(
    resource_query_service: &dyn ResourceQueryService<R>,
    resource_aggregate_loader: &dyn ResourceAggregateLoader<R>,
    resource_persistence_service: &dyn ResourcePersistenceService<R>,
    time_source: &dyn time_source::SystemTimeSource,
    params: ApplyResourceParams<R>,
) -> Result<ApplyResourceResult<R>, ApplyResourceUseCaseError<R>>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::Spec: Serialize + PartialEq + Clone + ResourceValidateSpec,
    R::Status: Serialize + ResourceStatusLike,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>
        + From<ResourceMetadataValidationError>
        + From<<R::Spec as ResourceValidateSpec>::ValidationError>,
{
    let maybe_existing_id = resolve_existing_resource_id::<R>(
        resource_query_service,
        params.resource_id,
        &params.metadata,
    )
    .await?;

    let now = time_source.now();

    let Some(resource_id) = maybe_existing_id else {
        return apply_create_resource::<R>(
            resource_query_service,
            resource_persistence_service,
            params,
            now,
        )
        .await;
    };

    ensure_resource_id_matches_type::<R>(resource_query_service, &resource_id).await?;

    let resource = resource_aggregate_loader
        .load(&resource_id)
        .await
        .map_err(ResourceLoadError)
        .map_err(ApplyResourceUseCaseError::LoadFailed)?;

    apply_update_resource::<R>(resource_persistence_service, resource, params, now).await
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
            resource_query_service:
                std::sync::Arc<dyn $crate::domain::ResourceQueryService<$resource>>,
            resource_aggregate_loader:
                std::sync::Arc<dyn $crate::domain::ResourceAggregateLoader<$resource>>,
            resource_persistence_service:
                std::sync::Arc<dyn $crate::domain::ResourcePersistenceService<$resource>>,
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
                $crate::use_cases::apply::shared::execute_apply_resource::<$resource>(
                    self.resource_query_service.as_ref(),
                    self.resource_aggregate_loader.as_ref(),
                    self.resource_persistence_service.as_ref(),
                    self.time_source.as_ref(),
                    params,
                )
                .await
            }
        }
    };
}

pub(crate) use declare_apply_resource_use_case;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
