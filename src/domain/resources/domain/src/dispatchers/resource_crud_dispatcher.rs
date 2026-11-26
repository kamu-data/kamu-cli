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
use thiserror::Error;

use crate::{
    ApplyManifestApplicationDecision,
    ApplyManifestPlanningDecision,
    ApplyManifestRejection,
    ApplyResourceAccessCheck,
    ApplyResourceAccountSource,
    ApplyResourceHeaders,
    ApplyResourcePrecondition,
    ApplyResourceUseCaseError,
    CreationProvenance,
    RequestResourcesDeletionError,
    ResourceAccountAccess,
    ResourceAccountOperation,
    ResourceHeadersValidationError,
    ResourceID,
    ResourceIDNotFoundError,
    ResourceListScope,
    ResourceOperationError,
    ResourceTypeMismatchError,
    ResourceTypeSelectorRaw,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceCrudDispatcher: Send + Sync {
    fn schema(&self) -> &'static TypeUri;

    async fn plan_apply(
        &self,
        request: ResourceCrudDispatcherApplyRequest,
    ) -> Result<ApplyManifestPlanningDecision, ApplyResourceCrudDispatcherError>;

    async fn apply(
        &self,
        request: ResourceCrudDispatcherApplyRequest,
    ) -> Result<ApplyManifestApplicationDecision, ApplyResourceCrudDispatcherError> {
        self.apply_with_precondition(request, ApplyResourcePrecondition::None)
            .await
    }

    /// See [`crate::ApplyResourceUseCase::apply_with_precondition`].
    async fn apply_with_precondition(
        &self,
        request: ResourceCrudDispatcherApplyRequest,
        precondition: ApplyResourcePrecondition,
    ) -> Result<ApplyManifestApplicationDecision, ApplyResourceCrudDispatcherError>;

    async fn request_deletion(
        &self,
        request: ResourceCrudDispatcherDeletionRequest,
    ) -> Result<(), RequestResourcesDeletionCrudDispatcherError>;

    /// See [`crate::ResourceOperationGuard::check_account`]; the type's guard,
    /// or the default rule
    async fn check_account_access(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        account: &odf::ResourceOwnerHandle,
        operation: ResourceAccountOperation,
    ) -> Result<ResourceAccountAccess, ResourceOperationError>;

    /// Checks `operation` on each of the resources, all of the type and in
    /// `account`, each named once. One result per ID, in order
    async fn check_resource_operations(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        account: &odf::ResourceOwnerHandle,
        operation: ResourceAccountOperation,
        ids: &[ResourceID],
    ) -> Result<Vec<Result<(), ResourceOperationError>>, InternalError>;

    /// Checks a search of `account` by one selector of the type
    async fn check_list(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        account: &odf::ResourceOwnerHandle,
        scope: &ResourceListScope,
    ) -> Result<(), ResourceOperationError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceCrudDispatcherApplyRequest {
    pub id: Option<ResourceID>,
    pub headers: ApplyResourceHeaders,
    pub account_source: ApplyResourceAccountSource,
    pub access_check: ApplyResourceAccessCheck,
    pub spec: serde_json::Value,
    /// Recorded when the apply creates the resource; ignored by an update
    pub provenance: CreationProvenance,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceCrudDispatcherDeletionRequest {
    pub account_id: odf::AccountID,
    pub ids: Vec<ResourceID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ApplyResourceCrudDispatcherError {
    #[error(transparent)]
    InvalidHeaders(#[from] ResourceHeadersValidationError),

    #[error("Invalid spec for resource {schema}: {message}")]
    InvalidSpec { schema: TypeUri, message: String },

    #[error(transparent)]
    NotFound(#[from] ResourceIDNotFoundError),

    #[error(transparent)]
    TypeMismatch(#[from] ResourceTypeMismatchError),

    #[error(transparent)]
    ConcurrentModification(#[from] ConcurrentModificationError),

    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum RequestResourcesDeletionCrudDispatcherError {
    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    ConcurrentModification(#[from] ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A resource type was addressed by its canonical schema URI, but the registry
/// has no descriptor for it. Produced only where a schema URI is the lookup
/// key — e.g. apply, which reads `$schema` from the manifest. A duplicate
/// registration is a static wiring bug, so it is surfaced as an
/// [`InternalError`] instead of a variant here.
#[derive(Debug, Error)]
pub enum UnsupportedResourceDescriptorError {
    #[error("Unsupported resource descriptor {schema}")]
    NotFound { schema: TypeUri },
}

/// A resource type was addressed by a short selector (main name or alias, e.g.
/// `vs`), but the registry has no descriptor matching it. Produced only on
/// selector-based lookups; the selector is never a schema URI. A duplicate
/// registration is a static wiring bug, so it is surfaced as an
/// [`InternalError`] instead of a variant here.
#[derive(Debug, Error)]
pub enum UnsupportedResourceSelectorError {
    #[error("Unsupported resource selector {raw_selector}")]
    NotFound {
        raw_selector: ResourceTypeSelectorRaw,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<R> From<ApplyResourceUseCaseError<R>> for ApplyResourceCrudDispatcherError
where
    R: crate::EventSourcedResource,
{
    fn from(err: ApplyResourceUseCaseError<R>) -> Self {
        match err {
            ApplyResourceUseCaseError::InvalidHeaders(err) => Self::InvalidHeaders(err),
            ApplyResourceUseCaseError::LoadFailed(err) => {
                Self::Internal(format!("{err}").int_err())
            }
            ApplyResourceUseCaseError::ResourceIDNotFound(err) => Self::NotFound(err),
            ApplyResourceUseCaseError::ResourceTypeMismatch(err) => Self::TypeMismatch(err),
            ApplyResourceUseCaseError::ConcurrentModification(err) => {
                Self::ConcurrentModification(err)
            }
            ApplyResourceUseCaseError::Access(err) => Self::Access(err),
            ApplyResourceUseCaseError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<crate::ApplyResourceRejection> for ApplyManifestRejection {
    fn from(value: crate::ApplyResourceRejection) -> Self {
        Self {
            category: value.category,
            message: value.message,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<RequestResourcesDeletionError> for RequestResourcesDeletionCrudDispatcherError {
    fn from(err: RequestResourcesDeletionError) -> Self {
        match err {
            RequestResourcesDeletionError::Access(err) => Self::Access(err),
            RequestResourcesDeletionError::ConcurrentModification(err) => {
                Self::ConcurrentModification(err)
            }
            RequestResourcesDeletionError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
