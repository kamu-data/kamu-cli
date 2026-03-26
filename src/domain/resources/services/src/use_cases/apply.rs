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
use time_source::SystemTimeSource;

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

pub struct ApplyResourceUseCaseHelper<'a, R>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
{
    resource_query_service: &'a dyn ResourceQueryService<R>,
    resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
    resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
    time_source: &'a dyn SystemTimeSource,
}

impl<'a, R> ApplyResourceUseCaseHelper<'a, R>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::Spec: Serialize + PartialEq + Clone + ResourceValidateSpec,
    R::Status: Serialize + ResourceStatusLike,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>
        + From<ResourceMetadataValidationError>
        + From<<R::Spec as ResourceValidateSpec>::ValidationError>,
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
        params: ApplyResourceParams<R>,
    ) -> Result<ApplyResourceResult<R>, ApplyResourceUseCaseError<R>> {
        let maybe_existing_id = self
            .resolve_existing_resource_id(params.resource_id, &params.metadata)
            .await?;

        let now = self.time_source.now();

        let Some(resource_id) = maybe_existing_id else {
            return self.apply_create_resource(params, now).await;
        };

        self.ensure_resource_id_matches_type(&resource_id).await?;

        let resource = self
            .resource_aggregate_loader
            .load(&resource_id)
            .await
            .map_err(ResourceLoadError)
            .map_err(ApplyResourceUseCaseError::LoadFailed)?;

        self.apply_update_resource(resource, params, now).await
    }

    fn make_result(resource: R, outcome: ApplyResourceOutcome) -> ApplyResourceResult<R> {
        ApplyResourceResult {
            resource_id: *resource.resource_id(),
            state: resource.into_state(),
            outcome,
        }
    }

    async fn resolve_existing_resource_id(
        &self,
        resource_id: Option<ResourceID>,
        metadata: &ResourceMetadataInput,
    ) -> Result<Option<ResourceID>, ApplyResourceUseCaseError<R>> {
        self.resource_query_service
            .find_existing_id_by_name(resource_id, metadata)
            .await
            .map_err(ApplyResourceUseCaseError::Internal)
    }

    async fn ensure_resource_id_matches_type(
        &self,
        resource_id: &ResourceID,
    ) -> Result<(), ApplyResourceUseCaseError<R>> {
        self.resource_query_service
            .ensure_resource_id_matches_type(resource_id)
            .await
            .map_err(ApplyResourceUseCaseError::from)
    }

    async fn apply_create_resource(
        &self,
        params: ApplyResourceParams<R>,
        now: DateTime<Utc>,
    ) -> Result<ApplyResourceResult<R>, ApplyResourceUseCaseError<R>> {
        let resource_id = self
            .resource_query_service
            .allocate_id()
            .await
            .map_err(ApplyResourceUseCaseError::Internal)?;
        let mut resource =
            <R as ReconcilableResource>::try_create(now, resource_id, params.metadata, params.spec)
                .map_err(ApplyResourceUseCaseError::Lifecycle)?;

        self.resource_persistence_service
            .create(&mut resource)
            .await
            .map_err(ApplyResourceUseCaseError::from)?;

        Ok(Self::make_result(resource, ApplyResourceOutcome::Created))
    }

    async fn apply_update_resource(
        &self,
        mut resource: R,
        params: ApplyResourceParams<R>,
        now: DateTime<Utc>,
    ) -> Result<ApplyResourceResult<R>, ApplyResourceUseCaseError<R>> {
        <R as ReconcilableResource>::try_update_metadata(&mut resource, now, params.metadata)
            .map_err(ApplyResourceUseCaseError::Lifecycle)?;
        <R as ReconcilableResource>::try_update_spec(&mut resource, now, params.spec)
            .map_err(ApplyResourceUseCaseError::Lifecycle)?;

        if !resource.aggregate().has_updates() {
            return Ok(Self::make_result(resource, ApplyResourceOutcome::Updated));
        }

        self.resource_persistence_service
            .save(&mut resource)
            .await
            .map_err(ApplyResourceUseCaseError::from)?;

        Ok(Self::make_result(resource, ApplyResourceOutcome::Updated))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
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
                let helper = $crate::ApplyResourceUseCaseHelper::<$resource>::new(
                    self.resource_query_service.as_ref(),
                    self.resource_aggregate_loader.as_ref(),
                    self.resource_persistence_service.as_ref(),
                    self.time_source.as_ref(),
                );

                helper.execute(params).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
