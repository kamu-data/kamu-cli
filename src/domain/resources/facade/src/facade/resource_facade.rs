// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use internal_error::InternalError;
use kamu_resources::{
    ApplyManifestResult,
    ApplyResourceCrudDispatcherError,
    DeleteResourcesCrudDispatcherError,
    GetResourceCrudDispatcherError,
    ListResourcesCrudDispatcherError,
    ResourceManifestAccount,
    ResourceMetadataValidationError,
    ResourceNotFoundError,
    ResourceSummaryView,
    ResourceUID,
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

    async fn delete(&self, request: DeleteResourcesRequest) -> Result<(), DeleteResourcesError>;
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
    pub account: Option<ResourceManifestAccount>,
    pub uid: ResourceUID,
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
pub struct DeleteResourcesRequest {
    pub kind: String,
    pub account: Option<ResourceManifestAccount>,
    pub uids: Vec<ResourceUID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ApplyManifestError {
    #[error(transparent)]
    ParseManifest(#[from] ParseResourceManifestError),

    #[error(transparent)]
    UnsupportedDescriptor(#[from] UnsupportedResourceDescriptorError),

    #[error(transparent)]
    ResolveAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    InvalidMetadata(#[from] ResourceMetadataValidationError),

    #[error(transparent)]
    Dispatcher(#[from] ApplyResourceCrudDispatcherError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetResourceError {
    #[error(transparent)]
    UnsupportedDescriptor(#[from] UnsupportedResourceDescriptorError),

    #[error(transparent)]
    ResolveAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    NotFound(#[from] ResourceNotFoundError),

    #[error(transparent)]
    KindMismatch(#[from] ResourceKindMismatchError),

    #[error(transparent)]
    Dispatcher(#[from] GetResourceCrudDispatcherError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ListResourcesError {
    #[error(transparent)]
    UnsupportedDescriptor(#[from] UnsupportedResourceDescriptorError),

    #[error(transparent)]
    ResolveAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    Dispatcher(#[from] ListResourcesCrudDispatcherError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum DeleteResourcesError {
    #[error(transparent)]
    UnsupportedDescriptor(#[from] UnsupportedResourceDescriptorError),

    #[error(transparent)]
    ResolveAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    NotFound(#[from] ResourceNotFoundError),

    #[error(transparent)]
    KindMismatch(#[from] ResourceKindMismatchError),

    #[error(transparent)]
    Dispatcher(#[from] DeleteResourcesCrudDispatcherError),

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
