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
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_resources::{
    ResourceVisibility,
    ResourceWarning,
    resource_replaces_resource_being_deleted_warning,
};
use serde::Serialize;
use time_source::SystemTimeSource;

use crate::domain::{
    ApplyResourceAction,
    ApplyResourceHeaders,
    ApplyResourceLifecycleErrorHandling,
    ApplyResourceParams,
    ApplyResourcePlan,
    ApplyResourcePlanningDecision,
    ApplyResourcePrecondition,
    ApplyResourceRejection,
    ApplyResourceUseCaseError,
    DeclarativeResource,
    EventSourcedResource,
    GenericResourceQueryService,
    IntoApplyResourceRejection,
    InvariantViolationOf,
    ManagedResource,
    ResourceAggregateLoader,
    ResourceHeadersInput,
    ResourceHeadersValidationError,
    ResourceID,
    ResourceLinterSpec,
    ResourceLoadError,
    ResourceName,
    ResourceNameSuffixGenerator,
    ResourceRef,
    ResourceSchemaProvider,
    ResourceSpecFromInput,
    ResourceValidateSpec,
    TypedResourceQueryService,
};
use crate::{GENERATED_NAME_SUFFIX_LENGTH, OwnerReferenceResolver, ResolveOwnerReferencesError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How many names are tried for a resource applied without one
pub(crate) const MAX_GENERATED_NAME_ATTEMPTS: usize = 5;

/// Fills the place of the random suffix in a plan
const PENDING_NAME_SUFFIX_CHAR: &str = "x";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ApplyResourcePlanner<'a, R>
where
    R: EventSourcedResource + ResourceSchemaProvider,
{
    generic_resource_query_service: &'a dyn GenericResourceQueryService,
    typed_resource_query_service: &'a dyn TypedResourceQueryService<R>,
    resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
    name_suffix_generator: &'a dyn ResourceNameSuffixGenerator,
    time_source: &'a dyn SystemTimeSource,
}

impl<'a, R> ApplyResourcePlanner<'a, R>
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
        name_suffix_generator: &'a dyn ResourceNameSuffixGenerator,
        time_source: &'a dyn SystemTimeSource,
    ) -> Self {
        Self {
            generic_resource_query_service,
            typed_resource_query_service,
            resource_aggregate_loader,
            name_suffix_generator,
            time_source,
        }
    }

    /// `maybe_existing` is the result of [`Self::find_existing_resource`]
    pub async fn plan(
        &self,
        params: ApplyResourceParams<R>,
        maybe_existing: Option<R>,
    ) -> Result<ApplyResourcePlanningDecision<R>, ApplyResourceUseCaseError<R>> {
        let plan = self
            .plan_with_naming(
                params,
                maybe_existing,
                ApplyResourcePrecondition::None,
                OmittedNameHandling::LeavePending,
            )
            .await?;
        Ok(match plan {
            PlannedApplyResourceDecision::Planned(plan) => {
                ApplyResourcePlanningDecision::Planned(plan.into_public_plan())
            }
            PlannedApplyResourceDecision::Rejected(rejection) => {
                ApplyResourcePlanningDecision::Rejected(rejection)
            }
        })
    }

    /// `maybe_existing` is the result of [`Self::find_existing_resource`]
    pub async fn plan_internal(
        &self,
        params: ApplyResourceParams<R>,
        maybe_existing: Option<R>,
        precondition: ApplyResourcePrecondition,
    ) -> Result<PlannedApplyResourceDecision<R>, ApplyResourceUseCaseError<R>> {
        self.plan_with_naming(
            params,
            maybe_existing,
            precondition,
            OmittedNameHandling::Generate {
                max_attempts: MAX_GENERATED_NAME_ATTEMPTS,
            },
        )
        .await
    }

    async fn plan_with_naming(
        &self,
        params: ApplyResourceParams<R>,
        maybe_existing: Option<R>,
        precondition: ApplyResourcePrecondition,
        omitted_name_handling: OmittedNameHandling,
    ) -> Result<PlannedApplyResourceDecision<R>, ApplyResourceUseCaseError<R>> {
        Self::ensure_name_given_or_generated(&params.headers)?;

        let now = self.time_source.now();

        let Some(resource) = maybe_existing else {
            return match precondition {
                ApplyResourcePrecondition::None | ApplyResourcePrecondition::Absent => {
                    match self.resolve_owner_references(params, None).await? {
                        OwnerReferencesResolution::Resolved(params) => {
                            self.plan_create_resource(params, now, omitted_name_handling)
                                .await
                        }
                        OwnerReferencesResolution::Rejected(rejection) => {
                            Ok(PlannedApplyResourceDecision::Rejected(rejection))
                        }
                    }
                }
                ApplyResourcePrecondition::Generation(_) => {
                    Err(ApplyResourceUseCaseError::ConcurrentModification(
                        ConcurrentModificationError {},
                    ))
                }
            };
        };

        let precondition_holds = match precondition {
            ApplyResourcePrecondition::None => true,
            ApplyResourcePrecondition::Absent => false,
            ApplyResourcePrecondition::Generation(generation) => {
                resource.headers().generation == generation
            }
        };
        if !precondition_holds {
            return Err(ApplyResourceUseCaseError::ConcurrentModification(
                ConcurrentModificationError {},
            ));
        }

        match self
            .resolve_owner_references(params, Some(&resource))
            .await?
        {
            OwnerReferencesResolution::Resolved(params) => {
                self.plan_update_resource(resource, params, now)
            }
            OwnerReferencesResolution::Rejected(rejection) => {
                Ok(PlannedApplyResourceDecision::Rejected(rejection))
            }
        }
    }

    async fn resolve_owner_references(
        &self,
        mut params: ApplyResourceParams<R>,
        maybe_existing: Option<&R>,
    ) -> Result<OwnerReferencesResolution<R>, ApplyResourceUseCaseError<R>> {
        // Events record an empty list as no list
        let Some(owner_references) = params
            .headers
            .owner_references
            .take()
            .filter(|owner_references| !owner_references.is_empty())
        else {
            return Ok(OwnerReferencesResolution::Resolved(params));
        };

        let resolver = OwnerReferenceResolver {
            generic_resource_query_service: self.generic_resource_query_service,
            account: params.headers.resolved_account(),
            owned_id: maybe_existing.map(|resource| *resource.id()),
            current_owners: maybe_existing
                .and_then(|resource| resource.headers().owner_references.as_deref())
                .unwrap_or_default(),
        };

        use ResolveOwnerReferencesError as E;
        match resolver.resolve(&owner_references).await {
            Ok(owners) => {
                params.headers.owner_references =
                    Some(owners.into_iter().map(ResourceRef::from).collect());
                Ok(OwnerReferencesResolution::Resolved(params))
            }
            Err(E::Rejected(err)) => Ok(OwnerReferencesResolution::Rejected(err.into())),
            Err(E::InvalidHeaders(err)) => Err(err.into()),
            Err(E::Internal(err)) => Err(err.into()),
        }
    }

    /// Fails for an omitted name that the type does not generate
    fn ensure_name_given_or_generated(
        headers: &ApplyResourceHeaders,
    ) -> Result<(), ResourceHeadersValidationError> {
        if headers.name.is_none() && R::GENERATED_NAME_PREFIX.is_none() {
            return Err(Self::missing_name_error());
        }
        Ok(())
    }

    fn missing_name_error() -> ResourceHeadersValidationError {
        ResourceHeadersValidationError::MissingName {
            schema: R::type_uri().cheap_clone(),
        }
    }

    /// The resource to update, `None` for one to create.
    ///
    /// An apply calls it once: whatever depends on the current state, the
    /// prepared spec included, is based on the returned resource, so that
    /// saving detects any change made since.
    pub async fn find_existing_resource(
        &self,
        params: &ApplyResourceParams<R>,
    ) -> Result<Option<R>, ApplyResourceUseCaseError<R>> {
        let Some(id) = self
            .resolve_existing_resource_id(params.id, &params.headers)
            .await?
        else {
            return Ok(None);
        };

        self.ensure_resource_id_matches_type(&id).await?;

        let resource = self
            .resource_aggregate_loader
            .load(&id)
            .await
            .map_err(ResourceLoadError)
            .map_err(ApplyResourceUseCaseError::LoadFailed)?;

        Ok(Some(resource))
    }

    /// `None` for a resource to create, which one without an ID and a name
    /// always is
    async fn resolve_existing_resource_id(
        &self,
        id: Option<ResourceID>,
        headers: &ApplyResourceHeaders,
    ) -> Result<Option<ResourceID>, ApplyResourceUseCaseError<R>> {
        if id.is_some() {
            return Ok(id);
        }

        let Some(name) = &headers.name else {
            return Ok(None);
        };

        self.generic_resource_query_service
            .find_resource_id_by_name(headers.resolved_account_id(), R::type_uri(), name)
            .await
            .map_err(ApplyResourceUseCaseError::Internal)
    }

    async fn ensure_resource_id_matches_type(
        &self,
        id: &ResourceID,
    ) -> Result<(), ApplyResourceUseCaseError<R>> {
        self.typed_resource_query_service
            .ensure_resource_id_matches_type(id)
            .await
            .map_err(ApplyResourceUseCaseError::from)
    }

    pub(crate) async fn plan_create_resource(
        &self,
        params: ApplyResourceParams<R>,
        now: DateTime<Utc>,
        omitted_name_handling: OmittedNameHandling,
    ) -> Result<PlannedApplyResourceDecision<R>, ApplyResourceUseCaseError<R>> {
        let id = self
            .generic_resource_query_service
            .allocate_id()
            .await
            .map_err(ApplyResourceUseCaseError::Internal)?;

        let mut warnings = params.spec.lint_warnings();

        let (headers, naming) = match params.headers.name.clone() {
            Some(name) => {
                warnings.extend(
                    self.replaced_resources_warnings(&params.headers, &name)
                        .await?,
                );
                (params.headers.into_named(name), ResourceNaming::Given)
            }
            None => {
                self.name_resource(params.headers, &params.spec, omitted_name_handling)
                    .await?
            }
        };

        let resource = match <R as ManagedResource>::try_create(now, id, headers, params.spec) {
            Ok(resource) => resource,
            Err(err) => {
                return Ok(PlannedApplyResourceDecision::Rejected(
                    Self::map_lifecycle_error(err)?,
                ));
            }
        };

        Ok(PlannedApplyResourceDecision::Planned(
            PlannedApplyResource {
                reconciliation_required: resource.needs_reconciliation(),
                resource,
                action: ApplyResourceAction::Create,
                naming,
                executable: true,
                planned_at: now,
                warnings,
                previous_state: None,
            },
        ))
    }

    /// Names a resource to create that was applied without a name
    async fn name_resource(
        &self,
        headers: ApplyResourceHeaders,
        spec: &R::SpecInput,
        omitted_name_handling: OmittedNameHandling,
    ) -> Result<(ResourceHeadersInput, ResourceNaming), ApplyResourceUseCaseError<R>> {
        let Some(make_prefix) = R::GENERATED_NAME_PREFIX else {
            return Err(Self::missing_name_error().into());
        };
        let prefix = make_prefix(spec);

        match omitted_name_handling {
            OmittedNameHandling::LeavePending => {
                let placeholder = PENDING_NAME_SUFFIX_CHAR.repeat(GENERATED_NAME_SUFFIX_LENGTH);
                let name = Self::make_name(&prefix, &placeholder)?;
                Ok((headers.into_named(name), ResourceNaming::Pending))
            }
            OmittedNameHandling::Generate { max_attempts } => {
                for attempt in 1..=max_attempts {
                    let name =
                        Self::make_name(&prefix, &self.name_suffix_generator.generate_suffix())?;
                    if self.is_name_unused(&headers, &name).await? {
                        return Ok((
                            headers.into_named(name),
                            ResourceNaming::Generated {
                                attempts_left: max_attempts - attempt,
                            },
                        ));
                    }
                }

                Err(ApplyResourceUseCaseError::Internal(
                    "Every generated resource name is taken"
                        .int_err()
                        .with_context(format!("schema='{}', prefix='{prefix}'", R::type_uri())),
                ))
            }
        }
    }

    fn make_name(prefix: &str, suffix: &str) -> Result<ResourceName, ApplyResourceUseCaseError<R>> {
        format!("{prefix}-{suffix}")
            .parse::<ResourceName>()
            .int_err()
            .map_err(ApplyResourceUseCaseError::Internal)
    }

    /// Counts deleted resources as well, which keep their names, so that a
    /// generated name finds one resource in the history too
    async fn is_name_unused(
        &self,
        headers: &ApplyResourceHeaders,
        name: &ResourceName,
    ) -> Result<bool, ApplyResourceUseCaseError<R>> {
        let holders = self
            .generic_resource_query_service
            .resolve_resource_ids_by_names(
                headers.resolved_account_id(),
                R::type_uri(),
                std::slice::from_ref(name),
                ResourceVisibility::IncludeDeleted,
            )
            .await
            .map_err(ApplyResourceUseCaseError::Internal)?;

        Ok(holders.is_empty())
    }

    /// Warns about each resource being deleted whose name the created resource
    /// takes.
    async fn replaced_resources_warnings(
        &self,
        headers: &ApplyResourceHeaders,
        name: &ResourceName,
    ) -> Result<Vec<ResourceWarning>, ApplyResourceUseCaseError<R>> {
        let replaced = self
            .generic_resource_query_service
            .resolve_resource_ids_by_names(
                headers.resolved_account_id(),
                R::type_uri(),
                std::slice::from_ref(name),
                ResourceVisibility::IncludeDeleting,
            )
            .await
            .map_err(ApplyResourceUseCaseError::Internal)?;

        Ok(replaced
            .iter()
            .map(|(_, id)| resource_replaces_resource_being_deleted_warning(id))
            .collect())
    }

    fn plan_update_resource(
        &self,
        mut resource: R,
        params: ApplyResourceParams<R>,
        now: DateTime<Utc>,
    ) -> Result<PlannedApplyResourceDecision<R>, ApplyResourceUseCaseError<R>> {
        // Snapshot the pre-apply state before any mutation. This is the `before`
        // side of the apply documents and it is free here — the aggregate is
        // already loaded, so nothing has to be re-read from storage.
        let previous_state = resource.as_ref().clone();

        let name = params
            .headers
            .name
            .clone()
            .unwrap_or_else(|| resource.headers().name.clone());

        // Headers and spec changes of one apply bump the generation once
        let new_generation = resource.headers().generation + 1;

        if let Err(err) = <R as ManagedResource>::try_update_headers(
            &mut resource,
            now,
            params.headers.into_named(name),
            new_generation,
        ) {
            return Ok(PlannedApplyResourceDecision::Rejected(
                Self::map_lifecycle_error(err)?,
            ));
        }

        let warnings = params.spec.lint_warnings();

        if let Err(err) =
            <R as ManagedResource>::try_update_spec(&mut resource, now, params.spec, new_generation)
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

        Ok(PlannedApplyResourceDecision::Planned(
            PlannedApplyResource {
                reconciliation_required: resource.needs_reconciliation(),
                resource,
                action,
                naming: ResourceNaming::Given,
                executable: true,
                planned_at: now,
                warnings,
                previous_state: Some(previous_state),
            },
        ))
    }

    fn map_lifecycle_error(
        err: R::LifecycleError,
    ) -> Result<ApplyResourceRejection, ApplyResourceUseCaseError<R>> {
        match err.into_apply_resource_rejection() {
            ApplyResourceLifecycleErrorHandling::Rejected(rejection) => Ok(rejection),
            ApplyResourceLifecycleErrorHandling::Internal(err) => {
                Err(ApplyResourceUseCaseError::Internal(err))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum OwnerReferencesResolution<R: DeclarativeResource> {
    Resolved(ApplyResourceParams<R>),
    Rejected(ApplyResourceRejection),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What planning does about the name of a new resource applied without one
#[derive(Debug, Clone, Copy)]
pub(crate) enum OmittedNameHandling {
    /// Puts a placeholder in its place
    LeavePending,
    Generate {
        max_attempts: usize,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Where the name of a planned resource comes from
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResourceNaming {
    /// The manifest, or the existing resource
    Given,
    /// A placeholder, as the apply generates the name
    Pending,
    Generated {
        /// How many more names may be tried if this one gets taken
        attempts_left: usize,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum PlannedApplyResourceDecision<R: EventSourcedResource> {
    Planned(PlannedApplyResource<R>),
    Rejected(ApplyResourceRejection),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PlannedApplyResource<R: EventSourcedResource> {
    pub(crate) resource: R,
    pub(crate) action: ApplyResourceAction,
    pub(crate) naming: ResourceNaming,
    pub(crate) reconciliation_required: bool,
    pub(crate) executable: bool,
    pub(crate) planned_at: DateTime<Utc>,
    pub(crate) warnings: Vec<ResourceWarning>,

    /// State captured before the plan mutated the aggregate, i.e. the `before`
    /// side of the apply documents. `None` for a create. Taken straight from
    /// the already-loaded aggregate, so it costs no additional read.
    pub(crate) previous_state: Option<R::ResourceState>,
}

impl<R> PlannedApplyResource<R>
where
    R: EventSourcedResource,
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
            previous_state: self.previous_state,
            name_pending: self.naming == ResourceNaming::Pending,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
