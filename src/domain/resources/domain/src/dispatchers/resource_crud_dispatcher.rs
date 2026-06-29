// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use event_sourcing::ConcurrentModificationError;
use internal_error::{ErrorIntoInternal, InternalError};
use thiserror::Error;

use crate::{
    ApplyManifestApplicationDecision,
    ApplyManifestPlanningDecision,
    ApplyManifestRejection,
    ApplyResourceUseCaseError,
    DeleteResourcesError,
    GetResourceByIdError,
    ResourceHeadersInput,
    ResourceID,
    ResourceIDNotFoundError,
    ResourceSummaryView,
    ResourceTypeMismatchError,
    ResourceView,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceCrudDispatcher: Send + Sync {
    async fn plan_apply(
        &self,
        request: ResourceCrudDispatcherApplyRequest,
    ) -> Result<ApplyManifestPlanningDecision, ApplyResourceCrudDispatcherError>;

    async fn apply(
        &self,
        request: ResourceCrudDispatcherApplyRequest,
    ) -> Result<ApplyManifestApplicationDecision, ApplyResourceCrudDispatcherError>;

    async fn get(
        &self,
        request: ResourceCrudDispatcherGetRequest,
    ) -> Result<ResourceView, GetResourceCrudDispatcherError>;

    async fn list(
        &self,
        request: ResourceCrudDispatcherListRequest,
    ) -> Result<Vec<ResourceSummaryView>, InternalError>;

    async fn delete(
        &self,
        request: ResourceCrudDispatcherDeleteRequest,
    ) -> Result<(), DeleteResourcesCrudDispatcherError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceCrudDispatcherApplyRequest {
    pub id: Option<ResourceID>,
    pub headers: ResourceHeadersInput,
    pub spec: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceCrudDispatcherGetRequest {
    pub account_id: odf::AccountID,
    pub id: ResourceID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceCrudDispatcherListRequest {
    pub account_id: odf::AccountID,
    pub pagination: PaginationOpts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceCrudDispatcherDeleteRequest {
    pub account_id: odf::AccountID,
    pub ids: Vec<ResourceID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ApplyResourceCrudDispatcherError {
    #[error("Invalid spec for resource {kind}::{api_version}: {message}")]
    InvalidSpec {
        kind: String,
        api_version: String,
        message: String,
    },

    #[error(transparent)]
    NotFound(#[from] ResourceIDNotFoundError),

    #[error(transparent)]
    TypeMismatch(#[from] ResourceTypeMismatchError),

    #[error(transparent)]
    ConcurrentModification(#[from] ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetResourceCrudDispatcherError {
    #[error(transparent)]
    NotFound(#[from] ResourceIDNotFoundError),

    #[error(transparent)]
    TypeMismatch(#[from] ResourceTypeMismatchError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum DeleteResourcesCrudDispatcherError {
    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    ConcurrentModification(#[from] ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum UnsupportedResourceDescriptorError {
    #[error("Unsupported resource descriptor {kind}::{api_version}")]
    NotFound { kind: String, api_version: String },

    #[error("Duplicate resource CRUD dispatcher registered for {kind}::{api_version}")]
    Duplicate { kind: String, api_version: String },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<R> From<ApplyResourceUseCaseError<R>> for ApplyResourceCrudDispatcherError
where
    R: crate::ReconcilableEventSourcedResource,
{
    fn from(err: ApplyResourceUseCaseError<R>) -> Self {
        match err {
            ApplyResourceUseCaseError::LoadFailed(err) => {
                Self::Internal(format!("{err}").int_err())
            }
            ApplyResourceUseCaseError::ResourceIDNotFound(err) => Self::NotFound(err),
            ApplyResourceUseCaseError::ResourceTypeMismatch(err) => Self::TypeMismatch(err),
            ApplyResourceUseCaseError::ConcurrentModification(err) => {
                Self::ConcurrentModification(err)
            }
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

impl From<GetResourceByIdError> for GetResourceCrudDispatcherError {
    fn from(err: GetResourceByIdError) -> Self {
        match err {
            GetResourceByIdError::NotFound(err) => Self::NotFound(err),
            GetResourceByIdError::TypeMismatch(err) => Self::TypeMismatch(err),
            GetResourceByIdError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<DeleteResourcesError> for DeleteResourcesCrudDispatcherError {
    fn from(err: DeleteResourcesError) -> Self {
        match err {
            DeleteResourcesError::Access(err) => Self::Access(err),
            DeleteResourcesError::ConcurrentModification(err) => Self::ConcurrentModification(err),
            DeleteResourcesError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
