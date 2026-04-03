// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use messaging_outbox::{Outbox, OutboxExt};
use serde::Serialize;
use time_source::SystemTimeSource;

use crate::domain::{
    ApplyResourceOutcome,
    ApplyResourceParams,
    ApplyResourceResult,
    ApplyResourceUseCaseError,
    DeclarativeResource,
    GenericResourceQueryService,
    InvariantViolationOf,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ReconcilableEventSourcedResource,
    ReconcilableResource,
    ResourceAggregateLoader,
    ResourceDescriptorProvider,
    ResourceLifecycleMessage,
    ResourceLifecycleMessageOutcome,
    ResourceLoadError,
    ResourceMetadataInput,
    ResourceMetadataValidationError,
    ResourcePersistenceService,
    ResourceStatusLike,
    ResourceUID,
    ResourceValidateSpec,
    TypedResourceQueryService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ApplyResourceUseCaseHelper<'a, R>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
{
    generic_resource_query_service: &'a dyn GenericResourceQueryService,
    typed_resource_query_service: &'a dyn TypedResourceQueryService<R>,
    resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
    resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
    outbox: &'a dyn Outbox,
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
        generic_resource_query_service: &'a dyn GenericResourceQueryService,
        typed_resource_query_service: &'a dyn TypedResourceQueryService<R>,
        resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
        resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
        outbox: &'a dyn Outbox,
        time_source: &'a dyn SystemTimeSource,
    ) -> Self {
        Self {
            generic_resource_query_service,
            typed_resource_query_service,
            resource_aggregate_loader,
            resource_persistence_service,
            outbox,
            time_source,
        }
    }

    pub async fn execute(
        &self,
        params: ApplyResourceParams<R>,
    ) -> Result<ApplyResourceResult<R>, ApplyResourceUseCaseError<R>> {
        let maybe_existing_id = self
            .resolve_existing_resource_uid(params.uid, &params.metadata)
            .await?;

        let now = self.time_source.now();

        let Some(uid) = maybe_existing_id else {
            return self.apply_create_resource(params, now).await;
        };

        self.ensure_resource_uid_matches_type(&uid).await?;

        let resource = self
            .resource_aggregate_loader
            .load(&uid)
            .await
            .map_err(ResourceLoadError)
            .map_err(ApplyResourceUseCaseError::LoadFailed)?;

        self.apply_update_resource(resource, params, now).await
    }

    fn make_result(resource: R, outcome: ApplyResourceOutcome) -> ApplyResourceResult<R> {
        ApplyResourceResult {
            uid: *resource.uid(),
            state: resource.into(),
            outcome,
        }
    }

    async fn resolve_existing_resource_uid(
        &self,
        uid: Option<ResourceUID>,
        metadata: &ResourceMetadataInput,
    ) -> Result<Option<ResourceUID>, ApplyResourceUseCaseError<R>> {
        match uid {
            Some(uid) => Ok(Some(uid)),
            None => self
                .generic_resource_query_service
                .find_resource_uid_by_name(
                    &metadata.account,
                    R::DESCRIPTOR.resource_type,
                    &metadata.name,
                )
                .await
                .map_err(ApplyResourceUseCaseError::Internal),
        }
    }

    async fn ensure_resource_uid_matches_type(
        &self,
        uid: &ResourceUID,
    ) -> Result<(), ApplyResourceUseCaseError<R>> {
        self.typed_resource_query_service
            .ensure_resource_uid_matches_type(uid)
            .await
            .map_err(ApplyResourceUseCaseError::from)
    }

    async fn apply_create_resource(
        &self,
        params: ApplyResourceParams<R>,
        now: DateTime<Utc>,
    ) -> Result<ApplyResourceResult<R>, ApplyResourceUseCaseError<R>> {
        let uid = self
            .generic_resource_query_service
            .allocate_uid()
            .await
            .map_err(ApplyResourceUseCaseError::Internal)?;
        let mut resource =
            <R as ReconcilableResource>::try_create(now, uid, params.metadata, params.spec)
                .map_err(ApplyResourceUseCaseError::Lifecycle)?;

        self.resource_persistence_service
            .create(&mut resource)
            .await
            .map_err(ApplyResourceUseCaseError::from)?;

        self.notify_resource_applied(&resource, now, ApplyResourceOutcome::Created)
            .await?;

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
            return Ok(Self::make_result(resource, ApplyResourceOutcome::Untouched));
        }

        self.resource_persistence_service
            .save(&mut resource)
            .await
            .map_err(ApplyResourceUseCaseError::from)?;

        self.notify_resource_applied(&resource, now, ApplyResourceOutcome::Updated)
            .await?;

        Ok(Self::make_result(resource, ApplyResourceOutcome::Updated))
    }

    async fn notify_resource_applied(
        &self,
        resource: &R,
        event_time: DateTime<Utc>,
        outcome: ApplyResourceOutcome,
    ) -> Result<(), ApplyResourceUseCaseError<R>> {
        let snapshot = resource
            .make_resource_snapshot()
            .map_err(ApplyResourceUseCaseError::Internal)?;

        let outcome = match outcome {
            ApplyResourceOutcome::Created => ResourceLifecycleMessageOutcome::Created,
            ApplyResourceOutcome::Updated => ResourceLifecycleMessageOutcome::Updated,
            ApplyResourceOutcome::Untouched => {
                unreachable!("notify_resource_applied() must not be called for untouched resources")
            }
        };

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
                ResourceLifecycleMessage::applied(event_time, outcome, snapshot),
            )
            .await
            .map_err(ApplyResourceUseCaseError::Internal)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! declare_apply_resource_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ApplyResourceUseCase<$resource>)]
        pub struct $use_case {
            generic_resource_query_service:
                std::sync::Arc<dyn kamu_resources::GenericResourceQueryService>,
            typed_resource_query_service:
                std::sync::Arc<dyn kamu_resources::TypedResourceQueryService<$resource>>,
            resource_aggregate_loader:
                std::sync::Arc<dyn kamu_resources::ResourceAggregateLoader<$resource>>,
            resource_persistence_service:
                std::sync::Arc<dyn kamu_resources::ResourcePersistenceService<$resource>>,
            outbox: std::sync::Arc<dyn $crate::messaging_outbox::Outbox>,
            time_source: std::sync::Arc<dyn time_source::SystemTimeSource>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::ApplyResourceUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                params: kamu_resources::ApplyResourceParams<$resource>,
            ) -> Result<
                kamu_resources::ApplyResourceResult<$resource>,
                kamu_resources::ApplyResourceUseCaseError<$resource>,
            > {
                let helper = $crate::ApplyResourceUseCaseHelper::<$resource>::new(
                    self.generic_resource_query_service.as_ref(),
                    self.typed_resource_query_service.as_ref(),
                    self.resource_aggregate_loader.as_ref(),
                    self.resource_persistence_service.as_ref(),
                    self.outbox.as_ref(),
                    self.time_source.as_ref(),
                );

                helper.execute(params).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
