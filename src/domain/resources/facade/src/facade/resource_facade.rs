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
    Resource,
    ResourceAccountRef,
    ResourceHandle,
    ResourceID,
    ResourceName,
    ResourceSummaryView,
    ResourceTypeDescriptor,
    ResourceTypeSelectorRaw,
    ResourcesSummary,
};
use kamu_resources as domain;

use crate::{
    ApplyManifestError,
    BatchResourceError,
    DeleteResourceError,
    GetResourceError,
    ListAllResourcesError,
    ListResourcesError,
    ListSupportedResourceTypesError,
    RenderResourceManifestError,
    ResourceLookupProblem,
    ResourcesSummaryError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait ResourceFacade: Send + Sync {
    async fn list_supported_resource_types(
        &self,
    ) -> Result<Vec<ResourceTypeDescriptor>, ListSupportedResourceTypesError>;

    async fn summary(
        &self,
        request: ResourcesSummaryRequest,
    ) -> Result<ResourcesSummary, ResourcesSummaryError>;

    async fn get(
        &self,
        selector: ResourceSelector,
        spec_view_mode: SpecViewMode,
    ) -> Result<Resource, GetResourceError>;

    async fn get_many(
        &self,
        selector: ResourceBatchSelector,
        spec_view_mode: SpecViewMode,
    ) -> Result<BatchResourceResponse<Resource, ResourceLookupProblem>, BatchResourceError>;

    async fn get_handle(
        &self,
        selector: ResourceSelector,
    ) -> Result<ResourceHandle, GetResourceError>;

    async fn get_handles(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<BatchResourceResponse<ResourceHandle, ResourceLookupProblem>, BatchResourceError>;

    async fn render_manifest(
        &self,
        selector: ResourceSelector,
        format: ResourceManifestFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<RenderResourceManifestResult, RenderResourceManifestError>;

    async fn render_manifests(
        &self,
        selector: ResourceBatchSelector,
        format: ResourceManifestFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<
        BatchResourceResponse<RenderResourceManifestResult, ResourceLookupProblem>,
        BatchResourceError,
    >;

    async fn list(
        &self,
        request: ListResourcesRequest,
    ) -> Result<Vec<ResourceSummaryView>, ListResourcesError>;

    async fn list_handles(
        &self,
        request: ListResourceHandlesRequest,
    ) -> Result<Vec<ResourceHandle>, ListResourcesError>;

    async fn search_handles(
        &self,
        request: SearchResourceHandlesRequest,
    ) -> Result<SearchResourceHandlesResponse, ListResourcesError>;

    async fn list_all(
        &self,
        request: ListAllResourcesRequest,
    ) -> Result<Vec<ResourceSummaryView>, ListAllResourcesError>;

    async fn list_all_handles(
        &self,
        request: ListAllResourceHandlesRequest,
    ) -> Result<Vec<ResourceHandle>, ListAllResourcesError>;

    async fn plan_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<ApplyManifestPlanningDecision, ApplyManifestError>;

    async fn apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<ApplyManifestApplicationDecision, ApplyManifestError>;

    async fn plan_apply_manifests(
        &self,
        request: ApplyManifestBatchRequest,
    ) -> Result<ApplyManifestBatchResponse<ApplyManifestPlanningDecision>, BatchResourceError>;

    async fn apply_manifests(
        &self,
        request: ApplyManifestBatchRequest,
    ) -> Result<ApplyManifestBatchResponse<ApplyManifestApplicationDecision>, BatchResourceError>;

    async fn delete(&self, selector: ResourceSelector) -> Result<ResourceID, DeleteResourceError>;

    async fn delete_many(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<BatchResourceResponse<ResourceID, ResourceLookupProblem>, BatchResourceError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceSelector {
    pub account: Option<ResourceAccountRef>,
    pub resource_type: ResourceTypeSelectorRaw,
    pub resource_ref: ResourceRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceBatchSelector {
    pub account: Option<ResourceAccountRef>,
    pub resource_type: ResourceTypeSelectorRaw,
    pub resource_refs: Vec<ResourceRef>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct BatchResourceResponse<T, E> {
    pub successes: Vec<BatchResourceSuccess<T>>,
    pub problems: Vec<BatchResourceProblem<E>>,
}

impl<T, E> BatchResourceResponse<T, E> {
    pub fn empty() -> Self {
        Self {
            successes: Vec::new(),
            problems: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct BatchResourceSuccess<T> {
    pub request_index: usize,
    pub item: T,
}

#[derive(Debug)]
pub struct BatchResourceProblem<E> {
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

#[derive(Debug, Clone)]
pub struct ApplyManifestBatchRequest {
    pub items: Vec<ApplyManifestRequest>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ApplyManifestBatchResponse<D> {
    pub items: Vec<ApplyManifestBatchItemResult<D>>,
    /// Positional indexes of items that individually succeeded but whose
    /// accepted details could not be returned because the enclosing
    /// single-transaction batch rolled back.
    ///
    /// This is transport metadata, not a per-item error. Local facades can
    /// keep this empty because they report every attempted item's real
    /// pre-rollback outcome in `items`; remote transports such as GraphQL use
    /// this to make rolled-back successes explicit when rollback is forced
    /// through a transport-level error and the normal `data` payload is not
    /// available.
    pub rolled_back_successes: Vec<usize>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ApplyManifestBatchItemResult<D> {
    pub request_index: usize,
    pub outcome: Result<D, ApplyManifestError>,
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
pub enum ResourceRef {
    ById(ResourceID),
    ByName(ResourceName),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ListResourcesRequest {
    pub raw_type_selector: ResourceTypeSelectorRaw,
    pub account: Option<ResourceAccountRef>,
    pub pagination: PaginationOpts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ListResourceHandlesRequest {
    pub raw_type_selector: ResourceTypeSelectorRaw,
    pub account: Option<ResourceAccountRef>,
    pub pagination: PaginationOpts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SearchResourceHandlesRequest {
    pub raw_type_selectors: Vec<ResourceTypeSelectorRaw>,
    pub exact_names: Option<Vec<ResourceName>>,
    pub name_pattern: Option<String>,
    pub account: Option<ResourceAccountRef>,
    pub pagination: PaginationOpts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SearchResourceHandlesResponse {
    pub items: Vec<ResourceHandle>,
    pub total_count: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ListAllResourcesRequest {
    pub account: Option<ResourceAccountRef>,
    pub pagination: PaginationOpts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ListAllResourceHandlesRequest {
    pub account: Option<ResourceAccountRef>,
    pub pagination: PaginationOpts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct ResourcesSummaryRequest {
    pub account: Option<ResourceAccountRef>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct RenderResourceManifestResult {
    pub manifest: String,
    pub format: ResourceManifestFormat,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SpecViewMode {
    #[default]
    Encrypted,
    Revealed,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
