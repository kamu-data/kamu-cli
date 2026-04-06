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
    ApplyResourceAction,
    ApplyResourceParams,
    ApplyResourcePlan,
    ApplyResourceResult,
    ApplyResourceUseCaseError,
    DeclarativeResource,
    GenericResourceQueryService,
    InvariantViolationOf,
    ReconcilableEventSourcedResource,
    ReconcilableResource,
    ResourceAggregateLoader,
    ResourceDescriptorProvider,
    ResourceLoadError,
    ResourceMetadataInput,
    ResourceMetadataValidationError,
    ResourceStatusLike,
    ResourceUID,
    ResourceValidateSpec,
    TypedResourceQueryService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ApplyResourcePlanner<'a, R>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
{
    generic_resource_query_service: &'a dyn GenericResourceQueryService,
    typed_resource_query_service: &'a dyn TypedResourceQueryService<R>,
    resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
    time_source: &'a dyn SystemTimeSource,
}

impl<'a, R> ApplyResourcePlanner<'a, R>
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
        time_source: &'a dyn SystemTimeSource,
    ) -> Self {
        Self {
            generic_resource_query_service,
            typed_resource_query_service,
            resource_aggregate_loader,
            time_source,
        }
    }

    pub async fn plan(
        &self,
        params: ApplyResourceParams<R>,
    ) -> Result<ApplyResourcePlan<R>, ApplyResourceUseCaseError<R>> {
        let plan = self.plan_internal(params).await?;
        Ok(plan.into_public_plan())
    }

    pub async fn plan_internal(
        &self,
        params: ApplyResourceParams<R>,
    ) -> Result<PlannedApplyResource<R>, ApplyResourceUseCaseError<R>> {
        let now = self.time_source.now();

        let maybe_existing_uid = self
            .resolve_existing_resource_uid(params.uid, &params.metadata)
            .await?;

        let Some(uid) = maybe_existing_uid else {
            return self.plan_create_resource(params, now).await;
        };

        self.ensure_resource_uid_matches_type(&uid).await?;

        let resource = self
            .resource_aggregate_loader
            .load(&uid)
            .await
            .map_err(ResourceLoadError)
            .map_err(ApplyResourceUseCaseError::LoadFailed)?;

        self.plan_update_resource(resource, params, now)
    }

    pub(crate) async fn resolve_existing_resource_uid(
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

    pub(crate) async fn ensure_resource_uid_matches_type(
        &self,
        uid: &ResourceUID,
    ) -> Result<(), ApplyResourceUseCaseError<R>> {
        self.typed_resource_query_service
            .ensure_resource_uid_matches_type(uid)
            .await
            .map_err(ApplyResourceUseCaseError::from)
    }

    async fn plan_create_resource(
        &self,
        params: ApplyResourceParams<R>,
        now: DateTime<Utc>,
    ) -> Result<PlannedApplyResource<R>, ApplyResourceUseCaseError<R>> {
        let uid = self
            .generic_resource_query_service
            .allocate_uid()
            .await
            .map_err(ApplyResourceUseCaseError::Internal)?;

        let resource =
            <R as ReconcilableResource>::try_create(now, uid, params.metadata, params.spec)
                .map_err(ApplyResourceUseCaseError::Lifecycle)?;

        Ok(PlannedApplyResource {
            reconciliation_required: resource.needs_reconciliation(),
            resource,
            action: ApplyResourceAction::Create,
            executable: true,
            planned_at: now,
        })
    }

    pub(crate) fn plan_update_resource(
        &self,
        mut resource: R,
        params: ApplyResourceParams<R>,
        now: DateTime<Utc>,
    ) -> Result<PlannedApplyResource<R>, ApplyResourceUseCaseError<R>> {
        <R as ReconcilableResource>::try_update_metadata(&mut resource, now, params.metadata)
            .map_err(ApplyResourceUseCaseError::Lifecycle)?;
        <R as ReconcilableResource>::try_update_spec(&mut resource, now, params.spec)
            .map_err(ApplyResourceUseCaseError::Lifecycle)?;

        let action = if resource.aggregate().has_updates() {
            ApplyResourceAction::Update
        } else {
            ApplyResourceAction::Untouched
        };

        Ok(PlannedApplyResource {
            reconciliation_required: resource.needs_reconciliation(),
            resource,
            action,
            executable: true,
            planned_at: now,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PlannedApplyResource<R: ReconcilableEventSourcedResource> {
    pub(crate) resource: R,
    pub(crate) action: ApplyResourceAction,
    pub(crate) reconciliation_required: bool,
    pub(crate) executable: bool,
    pub(crate) planned_at: DateTime<Utc>,
}

impl<R> PlannedApplyResource<R>
where
    R: ReconcilableEventSourcedResource,
{
    pub fn into_public_plan(self) -> ApplyResourcePlan<R> {
        ApplyResourcePlan {
            uid: *self.resource.uid(),
            state: self.resource.into(),
            action: self.action,
            reconciliation_required: self.reconciliation_required,
            executable: self.executable,
        }
    }

    pub fn into_apply_result(self) -> ApplyResourceResult<R> {
        ApplyResourceResult {
            uid: *self.resource.uid(),
            state: self.resource.into(),
            outcome: self.action.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
