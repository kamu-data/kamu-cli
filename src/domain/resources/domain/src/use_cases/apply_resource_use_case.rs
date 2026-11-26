// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::ConcurrentModificationError;
use internal_error::{ErrorIntoInternal, InternalError};

use crate::{
    ApplyResourceHeaders,
    CreationProvenance,
    DeclarativeResource,
    EventSourcedResource,
    ResourceHeadersValidationError,
    ResourceID,
    ResourceIDNotFoundError,
    ResourceLoadError,
    ResourcePersistenceError,
    ResourceSnapshot,
    ResourceTypeMismatchError,
    ResourceWarning,
    TypeUri,
    TypedResourceQueryError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Precondition: `params.headers.account` must already be a fully resolved
/// `ResourceOwnerRef` — neither `plan` nor `apply` resolve an id/did/name selector
/// themselves. Resolution is the caller's responsibility (via the facade's
/// `ResourceAccountResolver`, or by threading an already-known handle).
#[async_trait::async_trait]
pub trait ApplyResourceUseCase<R: EventSourcedResource>: Send + Sync {
    async fn plan(
        &self,
        params: ApplyResourceParams<R>,
    ) -> Result<ApplyResourcePlanningDecision<R>, ApplyResourceUseCaseError<R>>;

    async fn apply(
        &self,
        params: ApplyResourceParams<R>,
    ) -> Result<ApplyResourceApplicationDecision<R>, ApplyResourceUseCaseError<R>>;

    /// Applies only while the stored resource still matches `precondition`,
    /// failing with `ConcurrentModification` otherwise. The check and the
    /// write are atomic: a change committed in between fails the save.
    async fn apply_with_precondition(
        &self,
        params: ApplyResourceParams<R>,
        precondition: ApplyResourcePrecondition,
    ) -> Result<ApplyResourceApplicationDecision<R>, ApplyResourceUseCaseError<R>>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The state of the stored resource an apply was computed from, for a
/// read-modify-write that must not overwrite a concurrent change
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyResourcePrecondition {
    None,
    /// No resource with the applied name exists
    Absent,
    /// The resource exists at this generation
    Generation(u64),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ApplyResourceParams<R: DeclarativeResource> {
    pub id: Option<ResourceID>,
    /// `headers.account` must be a fully resolved `ResourceOwnerRef` — see
    /// [`ApplyResourceUseCase`]. `headers.name` may be omitted if the type has
    /// a [`DeclarativeResource::GENERATED_NAME_PREFIX`].
    pub headers: ApplyResourceHeaders,
    pub account_source: ApplyResourceAccountSource,
    pub access_check: ApplyResourceAccessCheck,
    pub spec: R::SpecInput,
    /// Recorded when the apply creates the resource; ignored by an update.
    /// Its principal is the one whose access the apply is checked for
    pub provenance: CreationProvenance,
}

impl<R: DeclarativeResource> Clone for ApplyResourceParams<R>
where
    R::SpecInput: Clone,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            headers: self.headers.clone(),
            account_source: self.account_source,
            access_check: self.access_check,
            spec: self.spec.clone(),
            provenance: self.provenance.clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Who decides whether the principal may apply the resource
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyResourceAccessCheck {
    /// The type's `ResourceOperationGuard`, or the default rule
    ByGuard,
    /// The caller, by rules of its own, such as the dataset permission a
    /// compatibility API checks before it writes a dataset's resources. Never
    /// for an apply on behalf of a manifest
    CheckedByCaller,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Where `headers.account` of an apply comes from, which decides what a
/// [`ResourcePlacement`](crate::ResourcePlacement) elsewhere does
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyResourceAccountSource {
    /// The manifest names it; a placement elsewhere rejects the manifest
    Manifest,
    /// The principal's own, as the manifest names none; a placement elsewhere
    /// applies the resource there
    PrincipalDefault,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum ApplyResourcePlanningDecision<R: DeclarativeResource> {
    Planned(ApplyResourcePlan<R>),
    Rejected(ApplyResourceRejection),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum ApplyResourceApplicationDecision<R: DeclarativeResource> {
    Applied(ApplyResourceResult<R>),
    Rejected(ApplyResourceRejection),
}

impl<R: DeclarativeResource> ApplyResourceApplicationDecision<R> {
    pub fn expect_applied(self) -> ApplyResourceResult<R> {
        match self {
            Self::Applied(result) => result,
            Self::Rejected(rejection) => {
                panic!("Expected Applied decision, got Rejected: {rejection:?}")
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ApplyResourcePlan<R: DeclarativeResource> {
    pub id: ResourceID,
    pub state: R::ResourceState,
    pub action: ApplyResourceAction,
    pub reconciliation_required: bool,
    pub executable: bool,
    pub warnings: Vec<ResourceWarning>,

    /// State as it was before this apply, i.e. the `before` side of the apply
    /// documents. `None` for a create. Captured from the aggregate the planner
    /// already loaded, so it costs no additional read.
    pub previous_state: Option<R::ResourceState>,

    /// The name in `state` is a placeholder: the apply generates the real one
    pub name_pending: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyResourceAction {
    Create,
    Update,
    Untouched,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ApplyResourceResult<R: DeclarativeResource> {
    pub id: ResourceID,
    pub state: R::ResourceState,
    pub outcome: ApplyResourceOutcome,
    pub warnings: Vec<ResourceWarning>,

    /// See [`ApplyResourcePlan::previous_state`].
    pub previous_state: Option<R::ResourceState>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum ApplyResourceOutcome {
    #[strum(to_string = "Created")]
    Created,
    #[strum(to_string = "Updated")]
    Updated,
    #[strum(to_string = "Unchanged")]
    Untouched,
}

impl ApplyResourceOutcome {
    pub fn label(self, dry_run: bool) -> String {
        let label = self.to_string();

        if dry_run {
            format!("{label} (dry-run)")
        } else {
            label
        }
    }
}

impl From<ApplyResourceAction> for ApplyResourceOutcome {
    fn from(value: ApplyResourceAction) -> Self {
        match value {
            ApplyResourceAction::Create => Self::Created,
            ApplyResourceAction::Update => Self::Updated,
            ApplyResourceAction::Untouched => Self::Untouched,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ApplyResourceRejection {
    pub category: ApplyResourceRejectionCategory,
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ApplyResourceRejectionCategory {
    ImmutableFieldChanged,
    BusinessValidationFailed,
    ReferencedObjectMissing,
    LifecycleRuleConflict,
}

impl ApplyResourceRejectionCategory {
    pub fn label(self) -> &'static str {
        match self {
            Self::ImmutableFieldChanged => "immutable field changed",
            Self::BusinessValidationFailed => "business validation failed",
            Self::ReferencedObjectMissing => "referenced object missing",
            Self::LifecycleRuleConflict => "lifecycle rule conflict",
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum ApplyResourceLifecycleErrorHandling {
    Rejected(ApplyResourceRejection),
    Internal(InternalError),
}

impl ApplyResourceLifecycleErrorHandling {
    pub fn rejected(category: ApplyResourceRejectionCategory, err: impl std::fmt::Display) -> Self {
        Self::Rejected(ApplyResourceRejection {
            category,
            message: err.to_string(),
        })
    }

    pub fn internal(err: impl ErrorIntoInternal) -> Self {
        Self::Internal(err.int_err())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait IntoApplyResourceRejection {
    fn into_apply_resource_rejection(self) -> ApplyResourceLifecycleErrorHandling;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ApplyResourceUseCaseError<R: EventSourcedResource> {
    #[error(transparent)]
    InvalidHeaders(#[from] ResourceHeadersValidationError),

    #[error(transparent)]
    LoadFailed(#[from] ResourceLoadError<R::ResourceState>),

    #[error(transparent)]
    ResourceIDNotFound(#[from] ResourceIDNotFoundError),

    #[error(transparent)]
    ResourceTypeMismatch(#[from] ResourceTypeMismatchError),

    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    /// The principal may not apply the resource in its account
    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<R> ApplyResourceUseCaseError<R>
where
    R: EventSourcedResource,
{
    pub fn type_mismatch(id: ResourceID, expected: &TypeUri, actual: &ResourceSnapshot) -> Self {
        Self::ResourceTypeMismatch(ResourceTypeMismatchError::from_expected_and_actual(
            id, expected, actual,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<R> From<ResourcePersistenceError> for ApplyResourceUseCaseError<R>
where
    R: EventSourcedResource,
{
    fn from(err: ResourcePersistenceError) -> Self {
        match err {
            ResourcePersistenceError::ConcurrentModification(err) => {
                Self::ConcurrentModification(err)
            }
            ResourcePersistenceError::Duplicate(err) => Self::Internal(err.int_err()),
            ResourcePersistenceError::InvalidDurableState(err) => Self::Internal(err.int_err()),
            ResourcePersistenceError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<R> From<TypedResourceQueryError> for ApplyResourceUseCaseError<R>
where
    R: EventSourcedResource,
{
    fn from(err: TypedResourceQueryError) -> Self {
        match err {
            TypedResourceQueryError::NotFound(err) => Self::ResourceIDNotFound(err),
            TypedResourceQueryError::TypeMismatch(err) => Self::ResourceTypeMismatch(err),
            TypedResourceQueryError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
