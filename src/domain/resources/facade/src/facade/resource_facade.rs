// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use domain::{
    ApplyManifestApplicationDecision,
    ApplyManifestPlanningDecision,
    ApplyResourceCrudDispatcherError,
    DeleteResourcesCrudDispatcherError,
    GetResourceCrudDispatcherError,
    ResourceAPIVersionMismatchError,
    ResourceIdentityView,
    ResourceInvalidSpecError,
    ResourceKindDescriptor,
    ResourceManifestAccount,
    ResourceMetadataValidationError,
    ResourceName,
    ResourceNameNotFoundError,
    ResourceSummaryView,
    ResourceUID,
    ResourceUIDNotFoundError,
    ResourceView,
    ResourcesSummary,
    UnsupportedResourceDescriptorError,
};
use event_sourcing::ConcurrentModificationError;
use graphql_http::GraphqlHttpRequestError;
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_resources as domain;
use thiserror::Error;

use crate::ResolveManifestAccountError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceFacade: Send + Sync {
    async fn list_supported_kinds(
        &self,
    ) -> Result<Vec<ResourceKindDescriptor>, ListSupportedResourceKindsError>;

    async fn summary(
        &self,
        request: ResourcesSummaryRequest,
    ) -> Result<ResourcesSummary, ResourcesSummaryError>;

    async fn get(&self, request: GetResourceRequest) -> Result<ResourceView, GetResourceError>;

    async fn get_many(
        &self,
        request: BatchRequest<GetResourceRequest>,
    ) -> Result<BatchRequestResponse<ResourceView, GetResourceError>, GetResourceError>;

    async fn get_identity(
        &self,
        request: GetResourceRequest,
    ) -> Result<ResourceIdentityView, GetResourceError>;

    async fn get_identities(
        &self,
        request: BatchRequest<GetResourceRequest>,
    ) -> Result<BatchRequestResponse<ResourceIdentityView, GetResourceError>, GetResourceError>;

    async fn render_manifest(
        &self,
        request: RenderResourceManifestRequest,
    ) -> Result<RenderResourceManifestResult, RenderResourceManifestError>;

    async fn render_manifests(
        &self,
        request: BatchRequest<RenderResourceManifestRequest>,
    ) -> Result<
        BatchRequestResponse<RenderResourceManifestResult, RenderResourceManifestError>,
        RenderResourceManifestError,
    >;

    async fn list(
        &self,
        request: ListResourcesRequest,
    ) -> Result<Vec<ResourceSummaryView>, ListResourcesError>;

    async fn list_identities(
        &self,
        request: ListResourceIdentitiesRequest,
    ) -> Result<Vec<ResourceIdentityView>, ListResourcesError>;

    async fn list_all(
        &self,
        request: ListAllResourcesRequest,
    ) -> Result<Vec<ResourceSummaryView>, ListAllResourcesError>;

    async fn list_all_identities(
        &self,
        request: ListAllResourceIdentitiesRequest,
    ) -> Result<Vec<ResourceIdentityView>, ListAllResourcesError>;

    async fn plan_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<ApplyManifestPlanningDecision, ApplyManifestError>;

    async fn apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<ApplyManifestApplicationDecision, ApplyManifestError>;

    async fn delete(
        &self,
        request: DeleteResourceRequest,
    ) -> Result<ResourceUID, DeleteResourceError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct BatchRequest<R> {
    pub requests: Vec<R>,
}

#[derive(Debug)]
pub struct BatchRequestResponse<T, E> {
    pub items: Vec<T>,
    pub problems: Vec<BatchRequestProblem<E>>,
}

#[derive(Debug)]
pub struct BatchRequestProblem<E> {
    pub request_index: usize,
    pub error: E,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ApplyManifestRequest {
    pub format: ResourceManifestFormat,
    pub manifest: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "UPPERCASE")]
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
    pub resource_ref: ResourceRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct RenderResourceManifestRequest {
    pub kind: String,
    pub api_version: Option<String>,
    pub account: Option<ResourceManifestAccount>,
    pub resource_ref: ResourceRef,
    pub format: ResourceManifestFormat,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum ResourceRef {
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
pub struct ListResourceIdentitiesRequest {
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
pub struct ListAllResourceIdentitiesRequest {
    pub account: Option<ResourceManifestAccount>,
    pub pagination: PaginationOpts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DeleteResourceRequest {
    pub kind: String,
    pub account: Option<ResourceManifestAccount>,
    pub resource_ref: ResourceRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct ResourcesSummaryRequest {
    pub account: Option<ResourceManifestAccount>,
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
    ConcurrentModification(ConcurrentModificationError),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

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
            E::ConcurrentModification(err) => Self::ConcurrentModification(err),
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
    RemoteRequest(#[from] GraphqlHttpRequestError),

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

#[derive(Debug, Clone)]
pub struct RenderResourceManifestResult {
    pub manifest: String,
    pub format: ResourceManifestFormat,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum RenderResourceManifestError {
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
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<GetResourceError> for RenderResourceManifestError {
    fn from(err: GetResourceError) -> Self {
        match err {
            GetResourceError::UnsupportedDescriptor(err) => Self::UnsupportedDescriptor(err),
            GetResourceError::BadAccount(err) => Self::BadAccount(err),
            GetResourceError::UIDNotFound(err) => Self::UIDNotFound(err),
            GetResourceError::NameNotFound(err) => Self::NameNotFound(err),
            GetResourceError::ApiVersionMismatch(err) => Self::ApiVersionMismatch(err),
            GetResourceError::KindMismatch(err) => Self::KindMismatch(err),
            GetResourceError::RemoteRequest(err) => Self::RemoteRequest(err),
            GetResourceError::Internal(err) => Self::Internal(err),
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
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ListAllResourcesError {
    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResourcesSummaryError {
    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

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
    RemoteRequest(#[from] GraphqlHttpRequestError),

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
pub enum ListSupportedResourceKindsError {
    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
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
