// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::ErrorIntoInternal;
use messaging_outbox::{Outbox, OutboxExt};
use serde::Serialize;
use time_source::SystemTimeSource;

use crate::domain::{
    ApplyResourceAction,
    ApplyResourceApplicationDecision,
    ApplyResourceParams,
    ApplyResourceResult,
    ApplyResourceUseCaseError,
    DeclarativeResource,
    GenericResourceQueryService,
    IntoApplyResourceRejection,
    InvariantViolationOf,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ReconcilableEventSourcedResource,
    ResourceAggregateLoader,
    ResourceDescriptorProvider,
    ResourceHeadersInput,
    ResourceHeadersValidationError,
    ResourceLifecycleMessage,
    ResourceLifecycleMessageOutcome,
    ResourceLinterSpec,
    ResourceLoadError,
    ResourcePersistenceError,
    ResourcePersistenceService,
    ResourceStatusLike,
    ResourceValidateSpec,
    TypedResourceQueryService,
};
use crate::{ApplyResourcePlanner, PlannedApplyResource, PlannedApplyResourceDecision};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ApplyResourcePlanExecutor<'a, R>
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

impl<'a, R> ApplyResourcePlanExecutor<'a, R>
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
        plan: PlannedApplyResource<R>,
    ) -> Result<ApplyResourceApplicationDecision<R>, ApplyResourceUseCaseError<R>> {
        match plan.action {
            ApplyResourceAction::Create => self.execute_create(plan).await,
            ApplyResourceAction::Update => self.execute_update(plan).await,
            ApplyResourceAction::Untouched => Ok(ApplyResourceApplicationDecision::Applied(
                self.make_apply_result(plan),
            )),
        }
    }

    async fn execute_create(
        &self,
        mut plan: PlannedApplyResource<R>,
    ) -> Result<ApplyResourceApplicationDecision<R>, ApplyResourceUseCaseError<R>> {
        match self
            .resource_persistence_service
            .create(&mut plan.resource)
            .await
        {
            Ok(()) => {}
            Err(ResourcePersistenceError::Duplicate(_)) => {
                return self.retry_apply_after_duplicate_create(plan).await;
            }
            Err(err) => return Err(ApplyResourceUseCaseError::from(err)),
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

    async fn retry_apply_after_duplicate_create(
        &self,
        plan: PlannedApplyResource<R>,
    ) -> Result<ApplyResourceApplicationDecision<R>, ApplyResourceUseCaseError<R>> {
        let headers = plan.resource.headers();

        let headers_input = ResourceHeadersInput {
            account: headers.account.clone(),
            name: headers.name.clone(),
            description: headers.description.clone(),
            labels: headers.labels.clone(),
            annotations: headers.annotations.clone(),
        };

        let planner = ApplyResourcePlanner::<R>::new(
            self.generic_resource_query_service,
            self.typed_resource_query_service,
            self.resource_aggregate_loader,
            self.time_source,
        );

        let Some(id) = planner
            .resolve_existing_resource_id(None, &headers_input)
            .await?
        else {
            return Err(ApplyResourceUseCaseError::Internal(
                "Resource create hit duplicate constraint, but no existing resource was found"
                    .int_err()
                    .with_context(format!(
                        "account_id={}, kind='{}', name='{}'",
                        headers_input.account,
                        R::DESCRIPTOR.resource_type,
                        headers_input.name
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

        let replanned = planner.plan_update_resource(
            existing_resource,
            ApplyResourceParams {
                id: Some(id),
                headers: headers_input,
                spec: plan.resource.spec().clone(),
            },
            plan.planned_at,
        )?;

        match replanned {
            PlannedApplyResourceDecision::Planned(replanned) => match replanned.action {
                ApplyResourceAction::Create => Err(ApplyResourceUseCaseError::Internal(
                    "Resource duplicate create retry unexpectedly produced a create plan"
                        .int_err()
                        .with_context(format!(
                            "account_id={}, kind='{}', name='{}'",
                            plan.resource.headers().account,
                            R::DESCRIPTOR.resource_type,
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
