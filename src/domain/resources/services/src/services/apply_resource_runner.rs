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
    ApplyResourceAccessCheck,
    ApplyResourceAccountSource,
    ApplyResourceApplicationDecision,
    ApplyResourceParams,
    ApplyResourcePlanningDecision,
    ApplyResourcePrecondition,
    ApplyResourceRejection,
    ApplyResourceRejectionCategory,
    ApplyResourceUseCaseError,
    DeclarativeResource,
    EventSourcedResource,
    GenericResourceQueryService,
    IntoApplyResourceRejection,
    InvariantViolationOf,
    ResourceAggregateLoader,
    ResourceHeadersValidationError,
    ResourceIDNotFoundError,
    ResourceLinterSpec,
    ResourceNameSuffixGenerator,
    ResourceOperation,
    ResourceOperationError,
    ResourceOperationGuard,
    ResourceOperationRequest,
    ResourcePersistenceService,
    ResourcePlacement,
    ResourcePlacementOutcome,
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
    DefaultResourceOperationGuard,
    PlannedApplyResourceDecision,
    ResourceDispatcherFactory,
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
    pub resource_dispatcher_factory: &'a ResourceDispatcherFactory,
    pub maybe_resource_spec_pre_apply: Option<&'a dyn ResourceSpecPreApply<R>>,
    pub maybe_resource_placement: Option<&'a dyn ResourcePlacement<R>>,
    pub maybe_resource_operation_guard: Option<&'a dyn ResourceOperationGuard<R>>,
    pub default_resource_operation_guard: &'a DefaultResourceOperationGuard,
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

        match self.prepare_with_access(params).await? {
            PreparedApplyResource::Prepared(prepared) => {
                self.planner()
                    .plan(prepared.params, prepared.maybe_existing)
                    .await
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
        let prepared = match self.prepare_with_access(params.clone()).await? {
            PreparedApplyResource::Prepared(prepared) => prepared,
            PreparedApplyResource::Rejected(rejection) => {
                return Ok(ApplyResourceExecution::rejected(rejection));
            }
        };

        let planner = self.planner();
        let plan = match planner
            .plan_internal(prepared.params, prepared.maybe_existing, precondition)
            .await?
        {
            PlannedApplyResourceDecision::Planned(plan) => plan,
            PlannedApplyResourceDecision::Rejected(rejection) => {
                return Ok(ApplyResourceExecution::rejected(rejection));
            }
        };

        ApplyResourcePlanExecutor::new(planner, self.resource_persistence_service, self.outbox)
            .execute(plan)
            .await
    }

    /// Places and prepares the resource, then checks that the principal may
    /// apply it there. Preparation and planning must see the same resource,
    /// so the one found here is planned against.
    ///
    /// A type with neither a placement nor a guard of its own is applied in
    /// the account the params name under the default rule, which needs no
    /// current resource: it is checked first, so a principal who may not use
    /// the account never has a resource there loaded or prepared for them
    async fn prepare_with_access(
        &self,
        params: ApplyResourceParams<R>,
    ) -> Result<PreparedApplyResource<R>, ApplyResourceUseCaseError<R>> {
        let checked_by_account_rule = match params.access_check {
            ApplyResourceAccessCheck::ByGuard => {
                self.maybe_resource_placement.is_none()
                    && self.maybe_resource_operation_guard.is_none()
            }
            ApplyResourceAccessCheck::CheckedByCaller => false,
        };
        if checked_by_account_rule {
            let account = params.headers.resolved_account_handle();
            self.default_resource_operation_guard
                .check_account_rule(&params.provenance.principal, &account)
                .await
                .map_err(|err| Self::apply_access_error(&account, err))?;
        }

        let (prepared, existing) = match self.place_and_prepare(params).await? {
            PlacedApplyResource::Prepared(prepared, existing) => (prepared, existing),
            PlacedApplyResource::Rejected(rejection) => {
                return Ok(PreparedApplyResource::Rejected(rejection));
            }
        };

        match prepared.access_check {
            ApplyResourceAccessCheck::ByGuard if checked_by_account_rule => {}
            ApplyResourceAccessCheck::ByGuard => {
                let account = prepared.headers.resolved_account_handle();
                let operation = match &existing {
                    ExistingResource::Found(current) => ResourceOperation::Update {
                        current,
                        new_spec: &prepared.spec,
                    },
                    ExistingResource::Absent | ExistingResource::Unavailable(_) => {
                        ResourceOperation::Create {
                            spec: &prepared.spec,
                        }
                    }
                };
                self.check_operation(
                    &prepared.provenance.principal,
                    &ResourceOperationRequest {
                        account: &account,
                        operation,
                    },
                )
                .await?;
            }
            ApplyResourceAccessCheck::CheckedByCaller => {}
        }

        let maybe_existing = match existing {
            ExistingResource::Absent => None,
            ExistingResource::Found(resource) => Some(resource),
            // Reported only to a principal who may apply in the account, so a
            // denial does not reveal what the ID names there
            ExistingResource::Unavailable(err) => return Err(err),
        };

        Ok(PreparedApplyResource::Prepared(PreparedApply {
            params: prepared,
            maybe_existing,
        }))
    }

    async fn check_operation(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        request: &ResourceOperationRequest<'_, R>,
    ) -> Result<(), ApplyResourceUseCaseError<R>> {
        let guard = DefaultResourceOperationGuard::or_default(
            self.maybe_resource_operation_guard,
            self.default_resource_operation_guard,
        );

        guard
            .check_operation(principal, request)
            .await
            .map_err(|err| Self::apply_access_error(request.account, err))
    }

    fn apply_access_error(
        account: &odf::ResourceOwnerHandle,
        err: ResourceOperationError,
    ) -> ApplyResourceUseCaseError<R> {
        match err {
            ResourceOperationError::Access(err) => err.into(),
            // Applying a resource the principal may not read is applying in an
            // account they may not use
            ResourceOperationError::NotFound(_) => odf::AccessError::Unauthorized(
                format!(
                    "Current subject is not allowed to apply resources of account '{}'",
                    account.name
                )
                .into(),
            )
            .into(),
            ResourceOperationError::Internal(err) => err.into(),
        }
    }

    /// Prepares the spec in the account of the apply, then places it. A
    /// manifest naming no account moves to the placed one, where the current
    /// spec is looked up and the spec is prepared again
    async fn place_and_prepare(
        &self,
        params: ApplyResourceParams<R>,
    ) -> Result<PlacedApplyResource<R>, ApplyResourceUseCaseError<R>> {
        let (prepared, existing) = match self.find_and_prepare(&params).await? {
            Ok(found) => found,
            Err(rejection) => return Ok(PlacedApplyResource::Rejected(rejection)),
        };

        let placed_account = match self.place(&prepared).await? {
            Ok(None) => return Ok(PlacedApplyResource::Prepared(prepared, existing)),
            Ok(Some(account)) if account.did == *prepared.headers.resolved_account_id() => {
                return Ok(PlacedApplyResource::Prepared(prepared, existing));
            }
            Ok(Some(account)) => account,
            Err(rejection) => return Ok(PlacedApplyResource::Rejected(rejection)),
        };

        match params.account_source {
            ApplyResourceAccountSource::Manifest => {
                return Ok(PlacedApplyResource::Rejected(ApplyResourceRejection {
                    category: ApplyResourceRejectionCategory::BusinessValidationFailed,
                    message: format!(
                        "A {} resource with this spec belongs to account '{}', not to account \
                         '{}' that the manifest names",
                        R::type_uri(),
                        placed_account.name,
                        prepared.headers.resolved_account_handle().name,
                    ),
                }));
            }
            ApplyResourceAccountSource::PrincipalDefault => {}
        }

        let mut params = params;
        params.headers.account = Some(placed_account.clone().into());

        let (prepared, existing) = match self.find_and_prepare(&params).await? {
            Ok(found) => found,
            Err(rejection) => return Ok(PlacedApplyResource::Rejected(rejection)),
        };

        match self.place(&prepared).await? {
            Ok(Some(account)) if account.did == placed_account.did => {
                Ok(PlacedApplyResource::Prepared(prepared, existing))
            }
            Ok(other) => Err(ApplyResourceUseCaseError::Internal(
                "Placing a prepared spec again gave another account"
                    .int_err()
                    .with_context(format!(
                        "schema='{}', first='{}', then={:?}",
                        R::type_uri(),
                        placed_account.name,
                        other.map(|account| account.name),
                    )),
            )),
            Err(rejection) => Ok(PlacedApplyResource::Rejected(rejection)),
        }
    }

    /// Prepares the spec against the resource the apply addresses, which is
    /// returned along with it
    async fn find_and_prepare(
        &self,
        params: &ApplyResourceParams<R>,
    ) -> Result<
        Result<(ApplyResourceParams<R>, ExistingResource<R>), ApplyResourceRejection>,
        ApplyResourceUseCaseError<R>,
    > {
        let existing = self.find_existing(params).await?;
        match self.prepare(params, existing.resource()).await? {
            Some(Ok(prepared)) => Ok(Ok((prepared, existing))),
            Some(Err(rejection)) => Ok(Err(rejection)),
            None => Ok(Ok((params.clone(), existing))),
        }
    }

    /// `Ok(None)` for the default placement
    async fn place(
        &self,
        prepared: &ApplyResourceParams<R>,
    ) -> Result<Result<Option<odf::ResourceOwnerHandle>, ApplyResourceRejection>, InternalError>
    {
        let Some(placement) = self.maybe_resource_placement else {
            return Ok(Ok(None));
        };

        Ok(
            match placement
                .place(&prepared.provenance.principal, &prepared.spec)
                .await?
            {
                ResourcePlacementOutcome::Default => Ok(None),
                ResourcePlacementOutcome::Account(account) => Ok(Some(account)),
                ResourcePlacementOutcome::Rejected(rejection) => Err(rejection),
            },
        )
    }

    /// The resource the apply addresses in its account. One addressed by an ID
    /// is looked for there only
    async fn find_existing(
        &self,
        params: &ApplyResourceParams<R>,
    ) -> Result<ExistingResource<R>, ApplyResourceUseCaseError<R>> {
        if let Some(id) = params.id {
            let in_account = self
                .generic_resource_query_service
                .find_resource_handles_by_ids(params.headers.resolved_account_id(), &[id])
                .await?
                .into_iter()
                .any(|row| row.deleted_at.is_none());
            if !in_account {
                return Ok(ExistingResource::Unavailable(
                    ResourceIDNotFoundError(id).into(),
                ));
            }
        }

        match self.planner().find_existing_resource(params).await {
            Ok(Some(resource)) => Ok(ExistingResource::Found(resource)),
            Ok(None) => Ok(ExistingResource::Absent),
            Err(err @ ApplyResourceUseCaseError::ResourceTypeMismatch(_)) => {
                Ok(ExistingResource::Unavailable(err))
            }
            Err(
                err @ (ApplyResourceUseCaseError::InvalidHeaders(_)
                | ApplyResourceUseCaseError::LoadFailed(_)
                | ApplyResourceUseCaseError::ResourceIDNotFound(_)
                | ApplyResourceUseCaseError::ConcurrentModification(_)
                | ApplyResourceUseCaseError::Access(_)
                | ApplyResourceUseCaseError::Internal(_)),
            ) => Err(err),
        }
    }

    /// Runs the type's [`ResourceSpecPreApply`]; `None` without one
    async fn prepare(
        &self,
        params: &ApplyResourceParams<R>,
        maybe_existing: Option<&R>,
    ) -> Result<
        Option<Result<ApplyResourceParams<R>, ApplyResourceRejection>>,
        ApplyResourceUseCaseError<R>,
    > {
        let Some(pre_apply) = self.maybe_resource_spec_pre_apply else {
            return Ok(None);
        };

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

        Ok(Some(match outcome {
            ResourceSpecPreApplyOutcome::Prepared(spec) => Ok(ApplyResourceParams {
                spec,
                ..params.clone()
            }),
            ResourceSpecPreApplyOutcome::Rejected(rejection) => Err(rejection),
        }))
    }

    /// Fails unless `headers.account` is a resolved handle: callers resolve
    /// selectors (the facade's `ResourceAccountResolver`), the projection cannot
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
            self.resource_dispatcher_factory,
            self.name_suffix_generator,
            self.time_source,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum PreparedApplyResource<R: DeclarativeResource> {
    Prepared(PreparedApply<R>),
    Rejected(ApplyResourceRejection),
}

struct PreparedApply<R: DeclarativeResource> {
    params: ApplyResourceParams<R>,
    maybe_existing: Option<R>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum PlacedApplyResource<R: EventSourcedResource> {
    Prepared(ApplyResourceParams<R>, ExistingResource<R>),
    Rejected(ApplyResourceRejection),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum ExistingResource<R: EventSourcedResource> {
    Absent,
    Found(R),
    /// The ID names no resource of the type in the account; reported once
    /// the principal may apply there
    Unavailable(ApplyResourceUseCaseError<R>),
}

impl<R: EventSourcedResource> ExistingResource<R> {
    fn resource(&self) -> Option<&R> {
        match self {
            Self::Found(resource) => Some(resource),
            Self::Absent | Self::Unavailable(_) => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
