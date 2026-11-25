// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::ConcurrentModificationError;
use internal_error::ErrorIntoInternal;
use messaging_outbox::{Outbox, OutboxExt};
use serde::Serialize;
use time_source::SystemTimeSource;

use crate::domain::{
    ApplyResourceAction,
    ApplyResourceApplicationDecision,
    ApplyResourceHeaders,
    ApplyResourceParams,
    ApplyResourcePrecondition,
    ApplyResourceResult,
    ApplyResourceUseCaseError,
    DeclarativeResource,
    EventSourcedResource,
    GenericResourceQueryService,
    IntoApplyResourceRejection,
    InvariantViolationOf,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceAggregateLoader,
    ResourceHeadersExt,
    ResourceHeadersValidationError,
    ResourceLifecycleMessage,
    ResourceLifecycleMessageOutcome,
    ResourceLinterSpec,
    ResourceLoadError,
    ResourceNameSuffixGenerator,
    ResourcePersistenceError,
    ResourcePersistenceService,
    ResourceSchemaProvider,
    ResourceSpecFromInput,
    ResourceValidateSpec,
    TypedResourceQueryService,
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
    generic_resource_query_service: &'a dyn GenericResourceQueryService,
    typed_resource_query_service: &'a dyn TypedResourceQueryService<R>,
    resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
    resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
    name_suffix_generator: &'a dyn ResourceNameSuffixGenerator,
    outbox: &'a dyn Outbox,
    time_source: &'a dyn SystemTimeSource,
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
        generic_resource_query_service: &'a dyn GenericResourceQueryService,
        typed_resource_query_service: &'a dyn TypedResourceQueryService<R>,
        resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
        resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
        name_suffix_generator: &'a dyn ResourceNameSuffixGenerator,
        outbox: &'a dyn Outbox,
        time_source: &'a dyn SystemTimeSource,
    ) -> Self {
        Self {
            generic_resource_query_service,
            typed_resource_query_service,
            resource_aggregate_loader,
            resource_persistence_service,
            name_suffix_generator,
            outbox,
            time_source,
        }
    }

    pub async fn execute(
        &self,
        plan: PlannedApplyResource<R>,
        precondition: ApplyResourcePrecondition,
    ) -> Result<ApplyResourceApplicationDecision<R>, ApplyResourceUseCaseError<R>> {
        match plan.action {
            ApplyResourceAction::Create => self.execute_create(plan, precondition).await,
            ApplyResourceAction::Update => self.execute_update(plan).await,
            ApplyResourceAction::Untouched => Ok(ApplyResourceApplicationDecision::Applied(
                self.make_apply_result(plan),
            )),
        }
    }

    async fn execute_create(
        &self,
        mut plan: PlannedApplyResource<R>,
        precondition: ApplyResourcePrecondition,
    ) -> Result<ApplyResourceApplicationDecision<R>, ApplyResourceUseCaseError<R>> {
        while let Err(err) = self
            .resource_persistence_service
            .create(&mut plan.resource)
            .await
        {
            if !matches!(err, ResourcePersistenceError::Duplicate(_)) {
                return Err(ApplyResourceUseCaseError::from(err));
            }

            match plan.naming {
                // Nobody asked for this name, so whoever holds it is not the
                // resource to update
                ResourceNaming::Generated { attempts_left } => {
                    match self.replan_under_another_name(plan, attempts_left).await? {
                        PlannedApplyResourceDecision::Planned(replanned) => plan = replanned,
                        PlannedApplyResourceDecision::Rejected(rejection) => {
                            return Ok(ApplyResourceApplicationDecision::Rejected(rejection));
                        }
                    }
                }
                // Someone created the resource first; replanning as an update
                // would overwrite what they wrote
                ResourceNaming::Given if precondition == ApplyResourcePrecondition::Absent => {
                    return Err(ApplyResourceUseCaseError::ConcurrentModification(
                        ConcurrentModificationError {},
                    ));
                }
                ResourceNaming::Given => {
                    return self.retry_apply_after_duplicate_create(plan).await;
                }
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

        Ok(ApplyResourceApplicationDecision::Applied(
            self.make_apply_result(plan),
        ))
    }

    async fn execute_update(
        &self,
        mut plan: PlannedApplyResource<R>,
    ) -> Result<ApplyResourceApplicationDecision<R>, ApplyResourceUseCaseError<R>> {
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

        Ok(ApplyResourceApplicationDecision::Applied(
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

        self.planner()
            .plan_create_resource(
                ApplyResourceParams {
                    id: None,
                    headers,
                    spec: plan.resource.spec().clone().into_input(),
                },
                plan.planned_at,
                OmittedNameHandling::Generate {
                    max_attempts: attempts_left,
                },
            )
            .await
    }

    fn planner(&self) -> ApplyResourcePlanner<'a, R> {
        ApplyResourcePlanner::new(
            self.generic_resource_query_service,
            self.typed_resource_query_service,
            self.resource_aggregate_loader,
            self.name_suffix_generator,
            self.time_source,
        )
    }

    async fn retry_apply_after_duplicate_create(
        &self,
        plan: PlannedApplyResource<R>,
    ) -> Result<ApplyResourceApplicationDecision<R>, ApplyResourceUseCaseError<R>> {
        let headers = ApplyResourceHeaders::from(plan.resource.headers().to_input());

        let planner = self.planner();

        let Some(id) = planner.resolve_existing_resource_id(None, &headers).await? else {
            return Err(ApplyResourceUseCaseError::Internal(
                "Resource create hit duplicate constraint, but no existing resource was found"
                    .int_err()
                    .with_context(format!(
                        "account_id={:?}, schema='{}', name='{}'",
                        headers.account,
                        R::type_uri(),
                        plan.resource.headers().name
                    )),
            ));
        };

        planner.ensure_resource_id_matches_type(&id).await?;

        let existing_resource = self
            .resource_aggregate_loader
            .load(&id)
            .await
            .map_err(ResourceLoadError)
            .map_err(ApplyResourceUseCaseError::LoadFailed)?;

        let spec = plan.resource.spec().clone();

        let replanned = planner.plan_update_resource(
            existing_resource,
            ApplyResourceParams {
                id: Some(id),
                headers,
                spec: spec.into_input(),
            },
            plan.planned_at,
        )?;

        match replanned {
            PlannedApplyResourceDecision::Planned(replanned) => match replanned.action {
                ApplyResourceAction::Create => Err(ApplyResourceUseCaseError::Internal(
                    "Resource duplicate create retry unexpectedly produced a create plan"
                        .int_err()
                        .with_context(format!(
                            "account_id={:?}, schema='{}', name='{}'",
                            plan.resource.headers().account,
                            R::type_uri(),
                            plan.resource.headers().name
                        )),
                )),
                ApplyResourceAction::Update => self.execute_update(replanned).await,
                ApplyResourceAction::Untouched => Ok(ApplyResourceApplicationDecision::Applied(
                    self.make_apply_result(replanned),
                )),
            },
            PlannedApplyResourceDecision::Rejected(rejection) => {
                Ok(ApplyResourceApplicationDecision::Rejected(rejection))
            }
        }
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
