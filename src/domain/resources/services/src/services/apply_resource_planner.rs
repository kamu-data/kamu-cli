// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_resources::ResourceWarning;
use serde::Serialize;
use time_source::SystemTimeSource;

use crate::domain::{
    ApplyResourceAction,
    ApplyResourceLifecycleErrorHandling,
    ApplyResourceParams,
    ApplyResourcePlan,
    ApplyResourcePlanningDecision,
    ApplyResourceRejection,
    ApplyResourceUseCaseError,
    DeclarativeResource,
    GenericResourceQueryService,
    IntoApplyResourceRejection,
    InvariantViolationOf,
    ReconcilableEventSourcedResource,
    ReconcilableResource,
    ResourceAggregateLoader,
    ResourceDescriptorProvider,
    ResourceHeadersInput,
    ResourceHeadersValidationError,
    ResourceID,
    ResourceLinterSpec,
    ResourceLoadError,
    ResourceStatusLike,
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
    R::Spec: Serialize + PartialEq + Clone + ResourceValidateSpec + ResourceLinterSpec,
    R::Status: Serialize + ResourceStatusLike,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>
        + From<ResourceHeadersValidationError>
        + From<<R::Spec as ResourceValidateSpec>::ValidationError>
        + IntoApplyResourceRejection,
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
    ) -> Result<ApplyResourcePlanningDecision<R>, ApplyResourceUseCaseError<R>> {
        let plan = self.plan_internal(params).await?;
        Ok(match plan {
            PlannedApplyResourceDecision::Planned(plan) => {
                ApplyResourcePlanningDecision::Planned(plan.into_public_plan())
            }
            PlannedApplyResourceDecision::Rejected(rejection) => {
                ApplyResourcePlanningDecision::Rejected(rejection)
            }
        })
    }

    pub async fn plan_internal(
        &self,
        params: ApplyResourceParams<R>,
    ) -> Result<PlannedApplyResourceDecision<R>, ApplyResourceUseCaseError<R>> {
        let now = self.time_source.now();

        let maybe_existing_id = self
            .resolve_existing_resource_id(params.id, &params.headers)
            .await?;

        let Some(id) = maybe_existing_id else {
            return self.plan_create_resource(params, now).await;
        };

        self.ensure_resource_id_matches_type(&id).await?;

        let resource = self
            .resource_aggregate_loader
            .load(&id)
            .await
            .map_err(ResourceLoadError)
            .map_err(ApplyResourceUseCaseError::LoadFailed)?;

        self.plan_update_resource(resource, params, now)
    }

    pub(crate) async fn resolve_existing_resource_id(
        &self,
        id: Option<ResourceID>,
        headers: &ResourceHeadersInput,
    ) -> Result<Option<ResourceID>, ApplyResourceUseCaseError<R>> {
        match id {
            Some(id) => Ok(Some(id)),
            None => self
                .generic_resource_query_service
                .find_resource_id_by_name(&headers.account, R::DESCRIPTOR.schema, &headers.name)
                .await
                .map_err(ApplyResourceUseCaseError::Internal),
        }
    }

    pub(crate) async fn ensure_resource_id_matches_type(
        &self,
        id: &ResourceID,
    ) -> Result<(), ApplyResourceUseCaseError<R>> {
        self.typed_resource_query_service
            .ensure_resource_id_matches_type(id)
            .await
            .map_err(ApplyResourceUseCaseError::from)
    }

    async fn plan_create_resource(
        &self,
        params: ApplyResourceParams<R>,
        now: DateTime<Utc>,
    ) -> Result<PlannedApplyResourceDecision<R>, ApplyResourceUseCaseError<R>> {
        let id = self
            .generic_resource_query_service
            .allocate_id()
            .await
            .map_err(ApplyResourceUseCaseError::Internal)?;

        let resource =
            match <R as ReconcilableResource>::try_create(now, id, params.headers, params.spec) {
                Ok(resource) => resource,
                Err(err) => {
                    return Ok(PlannedApplyResourceDecision::Rejected(
                        Self::map_lifecycle_error(err)?,
                    ));
                }
            };

        let warnings = resource.spec().lint_warnings();

        Ok(PlannedApplyResourceDecision::Planned(
            PlannedApplyResource {
                reconciliation_required: resource.needs_reconciliation(),
                resource,
                action: ApplyResourceAction::Create,
                executable: true,
                planned_at: now,
                warnings,
            },
        ))
    }

    pub(crate) fn plan_update_resource(
        &self,
        mut resource: R,
        params: ApplyResourceParams<R>,
        now: DateTime<Utc>,
    ) -> Result<PlannedApplyResourceDecision<R>, ApplyResourceUseCaseError<R>> {
        if let Err(err) =
            <R as ReconcilableResource>::try_update_headers(&mut resource, now, params.headers)
        {
            return Ok(PlannedApplyResourceDecision::Rejected(
                Self::map_lifecycle_error(err)?,
            ));
        }
        if let Err(err) =
            <R as ReconcilableResource>::try_update_spec(&mut resource, now, params.spec)
        {
            return Ok(PlannedApplyResourceDecision::Rejected(
                Self::map_lifecycle_error(err)?,
            ));
        }

        let action = if resource.aggregate().has_updates() {
            ApplyResourceAction::Update
        } else {
            ApplyResourceAction::Untouched
        };

        let warnings = resource.spec().lint_warnings();

        Ok(PlannedApplyResourceDecision::Planned(
            PlannedApplyResource {
                reconciliation_required: resource.needs_reconciliation(),
                resource,
                action,
                executable: true,
                planned_at: now,
                warnings,
            },
        ))
    }

    fn map_lifecycle_error(
        err: R::LifecycleError,
    ) -> Result<ApplyResourceRejection, ApplyResourceUseCaseError<R>> {
        match err.into_apply_resource_rejection() {
            ApplyResourceLifecycleErrorHandling::Rejected(rejection) => Ok(rejection),
            ApplyResourceLifecycleErrorHandling::Technical(err) => {
                Err(ApplyResourceUseCaseError::Internal(err))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum PlannedApplyResourceDecision<R: ReconcilableEventSourcedResource> {
    Planned(PlannedApplyResource<R>),
    Rejected(ApplyResourceRejection),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PlannedApplyResource<R: ReconcilableEventSourcedResource> {
    pub(crate) resource: R,
    pub(crate) action: ApplyResourceAction,
    pub(crate) reconciliation_required: bool,
    pub(crate) executable: bool,
    pub(crate) planned_at: DateTime<Utc>,
    pub(crate) warnings: Vec<ResourceWarning>,
}

impl<R> PlannedApplyResource<R>
where
    R: ReconcilableEventSourcedResource,
{
    fn into_public_plan(mut self) -> ApplyResourcePlan<R> {
        let id = *self.resource.id();
        let state = self.resource.as_ref().clone();
        self.resource.revert();

        ApplyResourcePlan {
            id,
            state,
            action: self.action,
            reconciliation_required: self.reconciliation_required,
            executable: self.executable,
            warnings: self.warnings,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
