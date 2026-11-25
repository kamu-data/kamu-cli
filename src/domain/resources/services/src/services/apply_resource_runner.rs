// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, InternalError};
use messaging_outbox::Outbox;
use serde::Serialize;
use time_source::SystemTimeSource;

use crate::domain::{
    ApplyResourceApplicationDecision,
    ApplyResourceParams,
    ApplyResourcePlanningDecision,
    ApplyResourcePrecondition,
    ApplyResourceRejection,
    ApplyResourceUseCaseError,
    DeclarativeResource,
    EventSourcedResource,
    GenericResourceQueryService,
    IntoApplyResourceRejection,
    InvariantViolationOf,
    ResourceAggregateLoader,
    ResourceHeadersValidationError,
    ResourceLinterSpec,
    ResourceNameSuffixGenerator,
    ResourcePersistenceService,
    ResourceSchemaProvider,
    ResourceSpecFromInput,
    ResourceSpecPreApply,
    ResourceSpecPreApplyContext,
    ResourceSpecPreApplyOutcome,
    ResourceValidateSpec,
    TypedResourceQueryService,
};
use crate::{
    ApplyResourceExecution,
    ApplyResourcePlanExecutor,
    ApplyResourcePlanner,
    PlannedApplyResourceDecision,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One apply and one more after losing a race for the name
const MAX_APPLY_ATTEMPTS: usize = 2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Prepares, plans and executes the applies of a resource type
pub struct ApplyResourceRunner<'a, R>
where
    R: EventSourcedResource + ResourceSchemaProvider,
{
    pub generic_resource_query_service: &'a dyn GenericResourceQueryService,
    pub typed_resource_query_service: &'a dyn TypedResourceQueryService<R>,
    pub resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
    pub resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
    pub maybe_resource_spec_pre_apply: Option<&'a dyn ResourceSpecPreApply<R>>,
    pub name_suffix_generator: &'a dyn ResourceNameSuffixGenerator,
    pub outbox: &'a dyn Outbox,
    pub time_source: &'a dyn SystemTimeSource,
}

impl<R> ApplyResourceRunner<'_, R>
where
    R: EventSourcedResource + ResourceSchemaProvider,
    R::Spec: Serialize + PartialEq + Clone + ResourceSpecFromInput<R::SpecInput>,
    R::SpecInput: ResourceValidateSpec + ResourceLinterSpec + Clone,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>
        + From<ResourceHeadersValidationError>
        + From<<R::SpecInput as ResourceValidateSpec>::ValidationError>
        + IntoApplyResourceRejection,
{
    pub async fn plan(
        &self,
        params: ApplyResourceParams<R>,
    ) -> Result<ApplyResourcePlanningDecision<R>, ApplyResourceUseCaseError<R>> {
        Self::ensure_account_resolved(&params)?;

        let maybe_existing = self.planner().find_existing_resource(&params).await?;

        match self.prepare(&params, maybe_existing.as_ref()).await? {
            PreparedApplyResource::Prepared(params) => {
                self.planner().plan(params, maybe_existing).await
            }
            PreparedApplyResource::Rejected(rejection) => {
                Ok(ApplyResourcePlanningDecision::Rejected(rejection))
            }
        }
    }

    pub async fn apply(
        &self,
        params: ApplyResourceParams<R>,
        precondition: ApplyResourcePrecondition,
    ) -> Result<ApplyResourceApplicationDecision<R>, ApplyResourceUseCaseError<R>> {
        Self::ensure_account_resolved(&params)?;

        // Another writer may create the resource after it was looked for. The
        // next attempt does what it would have done coming second: the spec is
        // prepared anew, with the one stored by that writer as the current spec
        for _ in 0..MAX_APPLY_ATTEMPTS {
            match self.try_apply(&params, precondition).await? {
                ApplyResourceExecution::Decided(decision) => return Ok(decision),
                ApplyResourceExecution::NameTaken => {}
            }
        }

        Err(ApplyResourceUseCaseError::Internal(
            "Resource name is taken by other writers repeatedly"
                .int_err()
                .with_context(format!(
                    "schema='{}', name={:?}",
                    R::type_uri(),
                    params.headers.name
                )),
        ))
    }

    async fn try_apply(
        &self,
        params: &ApplyResourceParams<R>,
        precondition: ApplyResourcePrecondition,
    ) -> Result<ApplyResourceExecution<R>, ApplyResourceUseCaseError<R>> {
        // Preparation and planning must see the same resource, so it is looked
        // for once
        let maybe_existing = self.planner().find_existing_resource(params).await?;

        let params = match self.prepare(params, maybe_existing.as_ref()).await? {
            PreparedApplyResource::Prepared(params) => params,
            PreparedApplyResource::Rejected(rejection) => {
                return Ok(ApplyResourceExecution::rejected(rejection));
            }
        };

        let plan = match self
            .planner()
            .plan_internal(params, maybe_existing, precondition)
            .await?
        {
            PlannedApplyResourceDecision::Planned(plan) => plan,
            PlannedApplyResourceDecision::Rejected(rejection) => {
                return Ok(ApplyResourceExecution::rejected(rejection));
            }
        };

        self.executor().execute(plan).await
    }

    /// Runs the type's [`ResourceSpecPreApply`], if it has one
    async fn prepare(
        &self,
        params: &ApplyResourceParams<R>,
        maybe_existing: Option<&R>,
    ) -> Result<PreparedApplyResource<R>, ApplyResourceUseCaseError<R>> {
        let spec = match self.maybe_resource_spec_pre_apply {
            None => params.spec.clone(),
            Some(pre_apply) => {
                let outcome = pre_apply
                    .pre_apply(
                        params.spec.clone(),
                        ResourceSpecPreApplyContext {
                            account_id: params.headers.resolved_account_id(),
                            maybe_current_spec: maybe_existing.map(R::spec),
                        },
                    )
                    .await
                    .map_err(ApplyResourceUseCaseError::Internal)?;

                match outcome {
                    ResourceSpecPreApplyOutcome::Prepared(spec) => spec,
                    ResourceSpecPreApplyOutcome::Rejected(rejection) => {
                        return Ok(PreparedApplyResource::Rejected(rejection));
                    }
                }
            }
        };

        Ok(PreparedApplyResource::Prepared(ApplyResourceParams {
            id: params.id,
            headers: params.headers.clone(),
            spec,
        }))
    }

    /// `headers.account` must already be resolved by the time it gets here —
    /// resolving an id/name selector is the caller's responsibility (via the
    /// facade's `ResourceAccountResolver`, or by threading an already-known
    /// handle). This is enforced here rather than silently trusted, since the
    /// event-sourced projector downstream is pure/sync and cannot resolve
    /// accounts itself.
    fn ensure_account_resolved(params: &ApplyResourceParams<R>) -> Result<(), InternalError> {
        match &params.headers.account {
            Some(odf::metadata::auth::ResourceOwnerRef {
                r#type: Some(_),
                id: Some(_),
                did: Some(_),
                name: Some(_),
            }) => Ok(()),
            other => Err(InternalError::new(format!(
                "resource headers must carry a resolved account handle by the time it reaches the \
                 apply use case, got: {other:?}"
            ))),
        }
    }

    fn planner(&self) -> ApplyResourcePlanner<'_, R> {
        ApplyResourcePlanner::new(
            self.generic_resource_query_service,
            self.typed_resource_query_service,
            self.resource_aggregate_loader,
            self.name_suffix_generator,
            self.time_source,
        )
    }

    fn executor(&self) -> ApplyResourcePlanExecutor<'_, R> {
        ApplyResourcePlanExecutor::new(
            self.generic_resource_query_service,
            self.typed_resource_query_service,
            self.resource_aggregate_loader,
            self.resource_persistence_service,
            self.name_suffix_generator,
            self.outbox,
            self.time_source,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum PreparedApplyResource<R: DeclarativeResource> {
    Prepared(ApplyResourceParams<R>),
    Rejected(ApplyResourceRejection),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
