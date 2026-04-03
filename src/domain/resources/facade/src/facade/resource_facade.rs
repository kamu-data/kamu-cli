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
use kamu_resources::{
    ApplyManifestResult,
    ApplyResourceCrudDispatcherError,
    DeleteResourcesCrudDispatcherError,
    GetResourceCrudDispatcherError,
    ResourceAPIVersionMismatchError,
    ResourceDuplicateError,
    ResourceInvalidSpecError,
    ResourceManifestAccount,
    ResourceMetadataValidationError,
    ResourceName,
    ResourceNameNotFoundError,
    ResourceSummaryView,
    ResourceUID,
    ResourceUIDNotFoundError,
    ResourceView,
    UnsupportedResourceDescriptorError,
};
use thiserror::Error;

use crate::ResolveManifestAccountError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceFacade: Send + Sync {
    async fn apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<ApplyManifestResult, ApplyManifestError>;

    async fn get(&self, request: GetResourceRequest) -> Result<ResourceView, GetResourceError>;

    async fn list(
        &self,
        request: ListResourcesRequest,
    ) -> Result<Vec<ResourceSummaryView>, ListResourcesError>;

    async fn list_all(
        &self,
        request: ListAllResourcesRequest,
    ) -> Result<Vec<ResourceSummaryView>, ListAllResourcesError>;

    async fn delete(
        &self,
        request: DeleteResourceRequest,
    ) -> Result<ResourceUID, DeleteResourceError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ApplyManifestRequest {
    pub format: ResourceManifestFormat,
    pub manifest: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceManifestFormat {
    Json,
    Yaml,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct GetResourceRequest {
    pub kind: String,
    pub api_version: Option<String>,
    pub account: Option<ResourceManifestAccount>,
    pub resource_ref: GetResourceRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum GetResourceRef {
    ById(ResourceUID),
    ByName(ResourceName),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ListResourcesRequest {
    pub kind: String,
    pub account: Option<ResourceManifestAccount>,
    pub pagination: PaginationOpts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ListAllResourcesRequest {
    pub account: Option<ResourceManifestAccount>,
    pub pagination: PaginationOpts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DeleteResourceRequest {
    pub kind: String,
    pub account: Option<ResourceManifestAccount>,
    pub resource_ref: GetResourceRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ApplyManifestError {
    #[error(transparent)]
    ParseManifest(#[from] ParseResourceManifestError),

    #[error(transparent)]
    UnsupportedDescriptor(#[from] UnsupportedResourceDescriptorError),

    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    InvalidMetadata(#[from] ResourceMetadataValidationError),

    #[error(transparent)]
    InvalidSpec(#[from] ResourceInvalidSpecError),

    #[error(transparent)]
    UIDNotFound(#[from] ResourceUIDNotFoundError),

    #[error(transparent)]
    TypeMismatch(#[from] kamu_resources::ResourceTypeMismatchError),

    #[error(transparent)]
    Duplicate(#[from] ResourceDuplicateError),

    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<ApplyResourceCrudDispatcherError> for ApplyManifestError {
    fn from(err: ApplyResourceCrudDispatcherError) -> Self {
        use ApplyResourceCrudDispatcherError as E;
        match err {
            E::Internal(err) => Self::Internal(err),
            E::NotFound(err) => Self::UIDNotFound(err),
            E::TypeMismatch(err) => Self::TypeMismatch(err),
            E::Duplicate(err) => Self::Duplicate(err),
            E::ConcurrentModification(err) => Self::ConcurrentModification(err),
            E::Lifecycle(err) => Self::Internal(err.int_err()),
            E::InvalidSpec {
                kind,
                api_version,
                message,
            } => Self::InvalidSpec(ResourceInvalidSpecError {
                kind,
                api_version,
                message,
            }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetResourceError {
    #[error(transparent)]
    UnsupportedDescriptor(#[from] UnsupportedResourceDescriptorError),

    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    UIDNotFound(#[from] ResourceUIDNotFoundError),

    #[error(transparent)]
    NameNotFound(#[from] ResourceNameNotFoundError),

    #[error(transparent)]
    ApiVersionMismatch(#[from] ResourceAPIVersionMismatchError),

    #[error(transparent)]
    KindMismatch(#[from] ResourceKindMismatchError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<GetResourceCrudDispatcherError> for GetResourceError {
    fn from(err: GetResourceCrudDispatcherError) -> Self {
        use GetResourceCrudDispatcherError as E;
        match err {
            E::NotFound(err) => Self::UIDNotFound(err),
            E::TypeMismatch(err) => {
                if err.expected_kind != err.actual_kind {
                    Self::KindMismatch(ResourceKindMismatchError {
                        uid: err.uid,
                        expected_kind: err.expected_kind,
                        actual_kind: err.actual_kind,
                    })
                } else {
                    Self::ApiVersionMismatch(ResourceAPIVersionMismatchError {
                        expected_api_version: err.expected_api_version,
                        actual_api_version: err.actual_api_version,
                    })
                }
            }
            E::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ListResourcesError {
    #[error(transparent)]
    UnsupportedDescriptor(#[from] UnsupportedResourceDescriptorError),

    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ListAllResourcesError {
    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum DeleteResourceError {
    #[error(transparent)]
    UnsupportedDescriptor(#[from] UnsupportedResourceDescriptorError),

    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    UIDNotFound(#[from] ResourceUIDNotFoundError),

    #[error(transparent)]
    NameNotFound(#[from] ResourceNameNotFoundError),

    #[error(transparent)]
    KindMismatch(#[from] ResourceKindMismatchError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<DeleteResourcesCrudDispatcherError> for DeleteResourceError {
    fn from(err: DeleteResourcesCrudDispatcherError) -> Self {
        use DeleteResourcesCrudDispatcherError as E;
        match err {
            E::Access(err) => Self::Internal(err.int_err()),
            E::ConcurrentModification(err) => Self::Internal(err.int_err()),
            E::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Failed to parse resource manifest: {message}")]
pub struct ParseResourceManifestError {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Resource uid {uid} refers to kind '{actual_kind}', expected '{expected_kind}'")]
pub struct ResourceKindMismatchError {
    pub uid: ResourceUID,
    pub expected_kind: String,
    pub actual_kind: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
