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
    DeclarativeResource,
    ReconcilableEventSourcedResource,
    ResourceLoadError,
    ResourcePersistenceError,
    ResourceSnapshot,
    ResourceTypeMismatchError,
    ResourceUID,
    ResourceUIDNotFoundError,
    TypedResourceQueryError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ApplyResourceUseCase<R: ReconcilableEventSourcedResource>: Send + Sync {
    async fn plan(
        &self,
        params: ApplyResourceParams<R>,
    ) -> Result<ApplyResourcePlanningDecision<R>, ApplyResourceUseCaseError<R>>;

    async fn apply(
        &self,
        params: ApplyResourceParams<R>,
    ) -> Result<ApplyResourceApplicationDecision<R>, ApplyResourceUseCaseError<R>>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ApplyResourceParams<R: DeclarativeResource> {
    pub uid: Option<ResourceUID>,
    pub metadata: crate::ResourceMetadataInput,
    pub spec: R::Spec,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum ApplyResourcePlanningDecision<R: DeclarativeResource> {
    Planned(ApplyResourcePlan<R>),
    Rejected(ApplyResourceRejection),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum ApplyResourceApplicationDecision<R: DeclarativeResource> {
    Applied(ApplyResourceResult<R>),
    Rejected(ApplyResourceRejection),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ApplyResourcePlan<R: DeclarativeResource> {
    pub uid: ResourceUID,
    pub state: R::ResourceState,
    pub action: ApplyResourceAction,
    pub reconciliation_required: bool,
    pub executable: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyResourceAction {
    Create,
    Update,
    Untouched,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ApplyResourceResult<R: DeclarativeResource> {
    pub uid: ResourceUID,
    pub state: R::ResourceState,
    pub outcome: ApplyResourceOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyResourceOutcome {
    Created,
    Updated,
    Untouched,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyResourceRejectionCategory {
    ImmutableFieldChanged,
    BusinessValidationFailed,
    ReferencedObjectMissing,
    LifecycleRuleConflict,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum ApplyResourceLifecycleErrorHandling {
    Rejected(ApplyResourceRejection),
    Technical(InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait IntoApplyResourceRejection {
    fn into_apply_resource_rejection(self) -> ApplyResourceLifecycleErrorHandling;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ApplyResourceUseCaseError<R: ReconcilableEventSourcedResource> {
    #[error(transparent)]
    LoadFailed(#[from] ResourceLoadError<R::ResourceState>),

    #[error(transparent)]
    ResourceUIDNotFound(#[from] ResourceUIDNotFoundError),

    #[error(transparent)]
    ResourceTypeMismatch(#[from] ResourceTypeMismatchError),

    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<R> ApplyResourceUseCaseError<R>
where
    R: ReconcilableEventSourcedResource,
{
    pub fn type_mismatch(
        uid: ResourceUID,
        expected: &crate::ResourceDescriptor,
        actual: &ResourceSnapshot,
    ) -> Self {
        Self::ResourceTypeMismatch(ResourceTypeMismatchError::from_expected_and_actual(
            uid, expected, actual,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<R> From<ResourcePersistenceError> for ApplyResourceUseCaseError<R>
where
    R: ReconcilableEventSourcedResource,
{
    fn from(err: ResourcePersistenceError) -> Self {
        match err {
            ResourcePersistenceError::ConcurrentModification(err) => {
                Self::ConcurrentModification(err)
            }
            ResourcePersistenceError::Duplicate(err) => Self::Internal(err.int_err()),
            ResourcePersistenceError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<R> From<TypedResourceQueryError> for ApplyResourceUseCaseError<R>
where
    R: ReconcilableEventSourcedResource,
{
    fn from(err: TypedResourceQueryError) -> Self {
        match err {
            TypedResourceQueryError::NotFound(err) => Self::ResourceUIDNotFound(err),
            TypedResourceQueryError::TypeMismatch(err) => Self::ResourceTypeMismatch(err),
            TypedResourceQueryError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
