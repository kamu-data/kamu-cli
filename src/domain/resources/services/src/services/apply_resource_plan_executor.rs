// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use messaging_outbox::{Outbox, OutboxExt};
use serde::Serialize;

use crate::domain::{
    ApplyResourceAccessCheck,
    ApplyResourceAccountSource,
    ApplyResourceAction,
    ApplyResourceApplicationDecision,
    ApplyResourceHeaders,
    ApplyResourceParams,
    ApplyResourceRejection,
    ApplyResourceResult,
    ApplyResourceUseCaseError,
    CreationProvenance,
    DeclarativeResource,
    EventSourcedResource,
    IntoApplyResourceRejection,
    InvariantViolationOf,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceHeadersExt,
    ResourceHeadersValidationError,
    ResourceLifecycleMessage,
    ResourceLifecycleMessageOutcome,
    ResourceLinterSpec,
    ResourcePersistenceError,
    ResourcePersistenceService,
    ResourceSchemaProvider,
    ResourceSpecFromInput,
    ResourceValidateSpec,
};
use crate::{
    ApplyResourcePlanner,
    OmittedNameHandling,
    PlannedApplyResource,
    PlannedApplyResourceDecision,
    ResourceNaming,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ApplyResourcePlanExecutor<'a, R>
where
    R: EventSourcedResource + ResourceSchemaProvider,
{
    planner: ApplyResourcePlanner<'a, R>,
    resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
    outbox: &'a dyn Outbox,
}

impl<'a, R> ApplyResourcePlanExecutor<'a, R>
where
    R: EventSourcedResource + ResourceSchemaProvider,
    R::Spec: Serialize + PartialEq + Clone + ResourceSpecFromInput<R::SpecInput>,
    R::SpecInput: ResourceValidateSpec + ResourceLinterSpec + Clone,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>
        + From<ResourceHeadersValidationError>
        + From<<R::SpecInput as ResourceValidateSpec>::ValidationError>
        + IntoApplyResourceRejection,
{
    pub fn new(
        planner: ApplyResourcePlanner<'a, R>,
        resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
        outbox: &'a dyn Outbox,
    ) -> Self {
        Self {
            planner,
            resource_persistence_service,
            outbox,
        }
    }

    pub async fn execute(
        &self,
        plan: PlannedApplyResource<R>,
    ) -> Result<ApplyResourceExecution<R>, ApplyResourceUseCaseError<R>> {
        match plan.action {
            ApplyResourceAction::Create => self.execute_create(plan).await,
            ApplyResourceAction::Update => self.execute_update(plan).await,
            ApplyResourceAction::Untouched => Ok(ApplyResourceExecution::applied(
                self.make_apply_result(plan),
            )),
        }
    }

    async fn execute_create(
        &self,
        mut plan: PlannedApplyResource<R>,
    ) -> Result<ApplyResourceExecution<R>, ApplyResourceUseCaseError<R>> {
        while let Err(err) = self
            .resource_persistence_service
            .create(&mut plan.resource)
            .await
        {
            match err {
                ResourcePersistenceError::Duplicate(_) => {}
                err @ (ResourcePersistenceError::ConcurrentModification(_)
                | ResourcePersistenceError::InvalidDurableState(_)
                | ResourcePersistenceError::Internal(_)) => {
                    return Err(ApplyResourceUseCaseError::from(err));
                }
            }

            match plan.naming {
                // Nobody asked for this name, so whoever holds it is not the
                // resource to update
                ResourceNaming::Generated { attempts_left } => {
                    match self.replan_under_another_name(plan, attempts_left).await? {
                        PlannedApplyResourceDecision::Planned(replanned) => plan = replanned,
                        PlannedApplyResourceDecision::Rejected(rejection) => {
                            return Ok(ApplyResourceExecution::rejected(rejection));
                        }
                    }
                }
                // The plan's spec was prepared without that of the resource
                // holding the name, so it can't become an update here
                ResourceNaming::Given => return Ok(ApplyResourceExecution::NameTaken),
                ResourceNaming::Pending => {
                    unreachable!("a plan with a pending name is never executed")
                }
            }
        }

        self.notify_resource_applied(
            &plan.resource,
            plan.planned_at,
            ResourceLifecycleMessageOutcome::Created,
        )
        .await?;

        Ok(ApplyResourceExecution::applied(
            self.make_apply_result(plan),
        ))
    }

    async fn execute_update(
        &self,
        mut plan: PlannedApplyResource<R>,
    ) -> Result<ApplyResourceExecution<R>, ApplyResourceUseCaseError<R>> {
        self.resource_persistence_service
            .save(&mut plan.resource)
            .await
            .map_err(ApplyResourceUseCaseError::from)?;

        self.notify_resource_applied(
            &plan.resource,
            plan.planned_at,
            ResourceLifecycleMessageOutcome::Updated,
        )
        .await?;

        Ok(ApplyResourceExecution::applied(
            self.make_apply_result(plan),
        ))
    }

    async fn replan_under_another_name(
        &self,
        plan: PlannedApplyResource<R>,
        attempts_left: usize,
    ) -> Result<PlannedApplyResourceDecision<R>, ApplyResourceUseCaseError<R>> {
        let headers = ApplyResourceHeaders {
            name: None,
            ..plan.resource.headers().to_input().into()
        };
        let provenance = CreationProvenance::from_conditions(&plan.resource.status().conditions)?
            .ok_or_else(|| {
            InternalError::new("A resource planned for creation has no creation provenance")
        })?;

        self.planner
            .plan_create_resource(
                ApplyResourceParams {
                    id: None,
                    headers,
                    // Placed already
                    account_source: ApplyResourceAccountSource::Manifest,
                    access_check: ApplyResourceAccessCheck::ByGuard,
                    spec: plan.resource.spec().clone().into_input(),
                    provenance,
                },
                plan.planned_at,
                OmittedNameHandling::Generate {
                    max_attempts: attempts_left,
                },
            )
            .await
    }

    fn make_apply_result(&self, plan: PlannedApplyResource<R>) -> ApplyResourceResult<R> {
        ApplyResourceResult {
            id: *plan.resource.id(),
            state: plan.resource.into(),
            outcome: plan.action.into(),
            warnings: plan.warnings,
            previous_state: plan.previous_state,
        }
    }

    async fn notify_resource_applied(
        &self,
        resource: &R,
        event_time: DateTime<Utc>,
        outcome: ResourceLifecycleMessageOutcome,
    ) -> Result<(), ApplyResourceUseCaseError<R>> {
        let snapshot = resource
            .make_resource_snapshot()
            .map_err(ApplyResourceUseCaseError::Internal)?;

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

/// What became of a plan
#[derive(Debug)]
pub enum ApplyResourceExecution<R: DeclarativeResource> {
    Decided(ApplyResourceApplicationDecision<R>),
    /// Another writer took the name given to the resource to create; nothing is
    /// written
    NameTaken,
}

impl<R: DeclarativeResource> ApplyResourceExecution<R> {
    pub(crate) fn applied(result: ApplyResourceResult<R>) -> Self {
        Self::Decided(ApplyResourceApplicationDecision::Applied(result))
    }

    pub(crate) fn rejected(rejection: ApplyResourceRejection) -> Self {
        Self::Decided(ApplyResourceApplicationDecision::Rejected(rejection))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
